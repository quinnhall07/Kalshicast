# morning.py
from __future__ import annotations

import concurrent.futures
import json
import os
import random
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import STATIONS
from sources_registry import load_fetchers_safe
from etl_utils import compute_lead_hours
from db import (
    init_db,
    upsert_location,
    get_conn,
    get_or_create_forecast_run,
    bulk_upsert_forecasts_daily,
    bulk_upsert_forecast_extras_hourly,
)

# -------------------------
# Debug knobs (set via env)
# -------------------------
DEBUG_DUMP = os.getenv("DEBUG_DUMP", "0") == "1"
DEBUG_SOURCE = os.getenv("DEBUG_SOURCE", "").strip()      # exact source_id, e.g. "OME_BASE"
DEBUG_STATION = os.getenv("DEBUG_STATION", "").strip()    # exact station_id, e.g. "KNYC"

# -------------------------
# Retry / throttling
# -------------------------
MAX_ATTEMPTS = 4
BASE_SLEEP_SECONDS = 1.0

_PROVIDER_LIMITS = {
    "TOM": threading.Semaphore(1),
    "WAPI": threading.Semaphore(2),
    "VCR": threading.Semaphore(2),
    "NWS": threading.Semaphore(4),
    "OME": threading.Semaphore(3),
}

# -------------------------
# Strict payload shape
# -------------------------
# Collector MUST return:
# {
#   "issued_at": "2026-02-01T22:49:48Z",
#   "daily": [ {"target_date":"YYYY-MM-DD","high_f":float,"low_f":float}, ... ],
#   "hourly": { "time":[...], optional variable arrays ... }  # Open-Meteo style arrays
# }
#
# Notes:
# - "daily" is required (may be empty list).
# - "hourly" is optional.
# - No legacy list[dict] shape accepted.


def _provider_key(source_id: str) -> str:
    if source_id.startswith("TOM"):
        return "TOM"
    if source_id.startswith("WAPI"):
        return "WAPI"
    if source_id.startswith("VCR"):
        return "VCR"
    if source_id.startswith("NWS"):
        return "NWS"
    if source_id.startswith("OME"):
        return "OME"
    return "OTHER"


def _is_retryable_error(e: Exception) -> bool:
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(e, requests.HTTPError):
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        if code is None:
            return True
        return code == 429 or code >= 500

    msg = str(e).lower()
    return any(h in msg for h in [
        "timed out",
        "timeout",
        "temporarily",
        "try again",
        "connection reset",
        "service unavailable",
        "internal server error",
        "bad gateway",
        "gateway timeout",
        "too many requests",
        "rate limit",
    ])


def _debug_match(station_id: str, source_id: str) -> bool:
    return DEBUG_DUMP and (DEBUG_SOURCE == source_id) and (DEBUG_STATION == station_id)


def _call_fetcher_with_retry(fetcher, station: dict, source_id: str) -> Any:
    last_exc: Exception | None = None
    sem = _PROVIDER_LIMITS.get(_provider_key(source_id))

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"[morning] fetch start {station['station_id']} {source_id} attempt={attempt}", flush=True)
            if sem:
                with sem:
                    return fetcher(station)
            return fetcher(station)
        except Exception as e:
            last_exc = e
            if attempt >= MAX_ATTEMPTS or not _is_retryable_error(e):
                raise

            msg = str(e).lower()
            is_429 = ("429" in msg) or ("too many requests" in msg) or ("rate limit" in msg)
            sleep_s = (10.0 + random.random() * 5.0) if is_429 else min(
                5.0, (BASE_SLEEP_SECONDS * attempt) + random.random() * 0.5
            )

            print(
                f"[morning] RETRY {station['station_id']} {source_id} attempt {attempt}/{MAX_ATTEMPTS}: {e}",
                flush=True,
            )
            time.sleep(sleep_s)

    raise last_exc  # pragma: no cover


def _require_str(d: dict, k: str) -> str:
    v = d.get(k)
    if not isinstance(v, str) or not v.strip():
        raise ValueError(f"payload missing/invalid '{k}' (expected non-empty str)")
    return v.strip()


def _coerce_float(x: Any, *, field: str) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if s:
            return float(s)
    raise ValueError(f"invalid float for {field}: {x!r}")


def _normalize_daily(payload: dict) -> List[Dict[str, Any]]:
    daily = payload.get("daily")
    if not isinstance(daily, list):
        raise ValueError("payload missing/invalid 'daily' (expected list)")

    out: List[Dict[str, Any]] = []
    for r in daily:
        if not isinstance(r, dict):
            continue
        td = r.get("target_date")
        if not isinstance(td, str) or len(td) < 10:
            continue
        try:
            high_f = _coerce_float(r.get("high_f"), field="high_f")
            low_f = _coerce_float(r.get("low_f"), field="low_f")
        except Exception:
            continue

        out.append({"target_date": td[:10], "high_f": high_f, "low_f": low_f})

    return out


def _normalize_hourly_arrays(payload: dict) -> List[Dict[str, Any]]:
    """
    Hourly is stored unaggregated.
    Supported hourly payload: Open-Meteo style arrays:
      payload["hourly"] = { "time": [...], "<var>": [...], ... }

    We map provider keys -> standardized DB column keys.
    """
    hourly = payload.get("hourly")
    if hourly is None:
        return []

    if not isinstance(hourly, dict):
        raise ValueError("payload 'hourly' must be an object when present")

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        return []

    key_map = {
        "temperature_f": ["temperature_f", "temperature_2m"],
        "dewpoint_f": ["dewpoint_f", "dew_point_2m"],
        "humidity_pct": ["humidity_pct", "relative_humidity_2m"],
        "wind_speed_mph": ["wind_speed_mph", "wind_speed_10m"],
        "wind_dir_deg": ["wind_dir_deg", "wind_direction_10m"],
        "cloud_cover_pct": ["cloud_cover_pct", "cloud_cover"],
        "precip_prob_pct": ["precip_prob_pct", "precipitation_probability"],
    }

    series: Dict[str, List[Any]] = {}
    for out_k, candidates in key_map.items():
        for cand in candidates:
            v = hourly.get(cand)
            if isinstance(v, list):
                series[out_k] = v
                break

    m = len(times)
    for v in series.values():
        m = min(m, len(v))

    out: List[Dict[str, Any]] = []
    for i in range(m):
        vt = times[i]
        if not isinstance(vt, str) or not vt.strip():
            continue

        row: Dict[str, Any] = {"valid_time": vt.strip()}
        for k, arr in series.items():
            val = arr[i]
            if val is None:
                continue
            try:
                row[k] = float(val)
            except Exception:
                continue

        out.append(row)

    return out


def _normalize_payload_strict(raw: Any) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not isinstance(raw, dict):
        raise ValueError(f"collector returned {type(raw)}; expected dict payload")

    issued_at = _require_str(raw, "issued_at")
    daily_rows = _normalize_daily(raw)
    hourly_rows = _normalize_hourly_arrays(raw)

    return issued_at, daily_rows, hourly_rows


def _fetch_one(st: dict, source_id: str, fetcher):
    station_id = st["station_id"]
    try:
        raw = _call_fetcher_with_retry(fetcher, st, source_id)

        if _debug_match(station_id, source_id):
            print("[DEBUG raw type]", type(raw), flush=True)
            if isinstance(raw, dict):
                print("[DEBUG raw keys]", list(raw.keys()), flush=True)
                d = raw.get("daily")
                if isinstance(d, list) and d:
                    print("[DEBUG raw first daily row]", json.dumps(d[0], default=str)[:2000], flush=True)
                h = raw.get("hourly")
                if isinstance(h, dict):
                    print("[DEBUG raw hourly keys]", list(h.keys())[:50], flush=True)

        issued_at, daily_rows, hourly_rows = _normalize_payload_strict(raw)

        if _debug_match(station_id, source_id):
            if daily_rows:
                print("[DEBUG normalized daily first row]", json.dumps(daily_rows[0], default=str)[:2000], flush=True)
            if hourly_rows:
                print("[DEBUG normalized hourly first row]", json.dumps(hourly_rows[0], default=str)[:2000], flush=True)

        return (station_id, st, source_id, issued_at, daily_rows, hourly_rows, None)

    except Exception as e:
        return (station_id, st, source_id, None, [], [], e)


def main() -> None:
    init_db()

    print(
        "[DEBUG env]",
        "DEBUG_DUMP=", os.getenv("DEBUG_DUMP"),
        "DEBUG_SOURCE=", os.getenv("DEBUG_SOURCE"),
        "DEBUG_STATION=", os.getenv("DEBUG_STATION"),
        flush=True,
    )

    for st in STATIONS:
        upsert_location(st)

    fetchers = load_fetchers_safe()
    if not fetchers:
        print("[morning] ERROR: no enabled sources loaded (check config.SOURCES).", flush=True)
        return

    if DEBUG_DUMP:
        print("[DEBUG available sources]", sorted(fetchers.keys()), flush=True)
        print("[DEBUG available stations]", [s["station_id"] for s in STATIONS], flush=True)
        if not DEBUG_SOURCE or not DEBUG_STATION:
            print("[DEBUG] Set DEBUG_SOURCE and DEBUG_STATION to enable payload dumps.", flush=True)

    tasks: List[concurrent.futures.Future] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for st in STATIONS:
            for source_id, fetcher in fetchers.items():
                tasks.append(ex.submit(_fetch_one, st, source_id, fetcher))

        with get_conn() as conn:
            for fut in concurrent.futures.as_completed(tasks):
                station_id, st, source_id, issued_at, daily_rows, hourly_rows, err = fut.result()

                if err is not None:
                    print(f"[morning] FAIL {station_id} {source_id}: {err}", flush=True)
                    continue

                if not daily_rows and not hourly_rows:
                    print(f"[morning] WARN {station_id} {source_id}: no rows", flush=True)
                    continue

                run_id = get_or_create_forecast_run(source=source_id, issued_at=issued_at, conn=conn)

                # ---- DAILY ----
                if daily_rows:
                    daily_batch: List[Dict[str, Any]] = []
                    for r in daily_rows:
                        td = r["target_date"]

                        lead_high = compute_lead_hours(
                            station_tz=st["timezone"],
                            issued_at=issued_at,
                            target_date=td,
                            kind="high",
                        )
                        lead_low = compute_lead_hours(
                            station_tz=st["timezone"],
                            issued_at=issued_at,
                            target_date=td,
                            kind="low",
                        )

                        daily_batch.append({
                            "run_id": run_id,
                            "station_id": station_id,
                            "target_date": td,
                            "high_f": r["high_f"],
                            "low_f": r["low_f"],
                            "lead_high_hours": lead_high,
                            "lead_low_hours": lead_low,
                        })

                    wrote = bulk_upsert_forecasts_daily(conn, daily_batch)
                    conn.commit()
                    print(f"[morning] OK {station_id} {source_id}: wrote {wrote} daily rows issued_at={issued_at}", flush=True)

                # ---- HOURLY ----
                if hourly_rows:
                    hourly_batch: List[Dict[str, Any]] = []
                    for hr in hourly_rows:
                        vt = hr.get("valid_time")
                        if not isinstance(vt, str) or not vt.strip():
                            continue

                        hourly_batch.append({
                            "run_id": run_id,
                            "station_id": station_id,
                            "valid_time": vt.strip(),
                            "temperature_f": hr.get("temperature_f"),
                            "dewpoint_f": hr.get("dewpoint_f"),
                            "humidity_pct": hr.get("humidity_pct"),
                            "wind_speed_mph": hr.get("wind_speed_mph"),
                            "wind_dir_deg": hr.get("wind_dir_deg"),
                            "cloud_cover_pct": hr.get("cloud_cover_pct"),
                            "precip_prob_pct": hr.get("precip_prob_pct"),
                        })

                    if hourly_batch:
                        wrote = bulk_upsert_forecast_extras_hourly(conn, hourly_batch)
                        conn.commit()
                        print(f"[morning] OK {station_id} {source_id}: wrote {wrote} hourly rows issued_at={issued_at}", flush=True)


if __name__ == "__main__":
    main()
