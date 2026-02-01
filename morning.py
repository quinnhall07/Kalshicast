# morning.py
from __future__ import annotations

import concurrent.futures
import json
import os
import random
import time
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import STATIONS
from sources_registry import load_fetchers_safe
from etl_utils import compute_lead_hours
from db import (
    init_db,
    upsert_location,
    get_or_create_forecast_run,
    compute_revisions_for_run,
    get_conn,
    bulk_upsert_forecasts_daily,
    bulk_upsert_forecast_extras_hourly,
    bulk_upsert_forecast_values,  # legacy fallback
)

# -------------------------
# Debug knobs (set via env)
# -------------------------
DEBUG_DUMP = os.getenv("DEBUG_DUMP", "0") == "1"
DEBUG_SOURCE = os.getenv("DEBUG_SOURCE", "").strip()      # exact source_id, e.g. "OME_GFS"
DEBUG_STATION = os.getenv("DEBUG_STATION", "").strip()    # exact station_id, e.g. "KMDW"

# -------------------------
# Retry / throttling
# -------------------------
MAX_ATTEMPTS = 4
BASE_SLEEP_SECONDS = 1.0

_PROVIDER_LIMITS = {
    "TOM": threading.Semaphore(1),   # Tomorrow.io
    "WAPI": threading.Semaphore(2),
    "VCR": threading.Semaphore(2),
    "NWS": threading.Semaphore(4),
    "OME": threading.Semaphore(3),
}


def _coerce_float(x: Any) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        return float(x.strip())
    raise ValueError(f"not a number: {x!r}")


def _provider_key(source_id: str) -> str:
    if source_id.startswith("TOM"):
        return "TOM"
    if source_id.startswith("WAPI"):
        return "WAPI"
    if source_id.startswith("VCR"):
        return "VCR"
    if source_id.startswith("NWS"):
        return "NWS"
    if source_id.startswith("OME_") or source_id.startswith("OME"):
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
        "timed out", "timeout", "temporarily", "try again", "connection reset",
        "service unavailable", "internal server error", "bad gateway",
        "gateway timeout", "too many requests", "rate limit",
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


def _extract_daily_rows(raw_rows: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for r in raw_rows:
        if not isinstance(r, dict):
            continue
        if "target_date" not in r or "high" not in r or "low" not in r:
            continue

        td = str(r["target_date"])[:10]
        high = _coerce_float(r["high"])
        low = _coerce_float(r["low"])

        extras: Dict[str, Any] = {}

        # top-level
        for k in (
            "dewpoint_f", "humidity_pct", "wind_speed_mph", "wind_dir_deg",
            "cloud_cover_pct", "precip_prob_pct"
        ):
            if k in r and r[k] is not None:
                extras[k] = r[k]

        # nested extras
        nested = r.get("extras")
        if isinstance(nested, dict):
            for k in (
                "dewpoint_f", "humidity_pct", "wind_speed_mph", "wind_dir_deg",
                "cloud_cover_pct", "precip_prob_pct"
            ):
                if k in nested and nested[k] is not None:
                    extras.setdefault(k, nested[k])

        out.append({"target_date": td, "high": high, "low": low, "extras": extras})
    return out


def _extract_hourly_rows(payload: dict) -> List[Dict[str, Any]]:
    """
    Supports two shapes (no aggregation):
      A) payload["hourly_rows"] = [{valid_time, temperature_f, dewpoint_f, ... , extras?}, ...]
      B) payload["hourly"] = {"time": [...], "<var>": [...] ...}  (Open-Meteo style arrays)
         - We align by index and emit one row per time.
    """
    hr = payload.get("hourly_rows")
    if isinstance(hr, list):
        out: List[Dict[str, Any]] = []
        for r in hr:
            if not isinstance(r, dict):
                continue
            vt = r.get("valid_time") or r.get("time")
            if not isinstance(vt, str) or not vt.strip():
                continue
            out.append(dict(r, valid_time=vt))
        return out

    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        return []

    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        return []

    # Common keys we support (provider may supply a subset)
    # NOTE: values should already be in the correct unit if fetcher requests it that way.
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

        row: Dict[str, Any] = {"valid_time": vt}

        extras: Dict[str, Any] = {}
        for k, arr in series.items():
            val = arr[i]
            if val is None:
                continue
            try:
                row[k] = float(val)
                extras[k] = row[k]
            except Exception:
                continue

        # Keep any provider-specific payload in extras if present
        extra_obj = hourly.get("extras")
        if isinstance(extra_obj, dict):
            extras.update(extra_obj)

        row["extras"] = extras
        out.append(row)

    return out


def _normalize_payload(raw: Any, *, fallback_issued_at: str) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Returns:
      (issued_at_utc_iso, daily_rows, hourly_rows)

    Accepted:
      - list[dict] (legacy daily rows)
      - dict: {"issued_at": "...Z", "rows": [...], optional hourly_rows/hourly}
    """
    issued_at = fallback_issued_at

    if raw is None:
        return issued_at, [], []

    if isinstance(raw, dict):
        if isinstance(raw.get("issued_at"), str) and raw["issued_at"].strip():
            issued_at = raw["issued_at"]

        daily_rows = _extract_daily_rows(raw.get("rows"))
        hourly_rows = _extract_hourly_rows(raw)
        return issued_at, daily_rows, hourly_rows

    if isinstance(raw, list):
        daily_rows = _extract_daily_rows(raw)
        return issued_at, daily_rows, []

    raise ValueError(f"fetcher returned {type(raw)}; expected list[dict] or dict")


def _fetch_one(st: dict, source_id: str, fetcher, fallback_issued_at: str):
    station_id = st["station_id"]
    try:
        raw = _call_fetcher_with_retry(fetcher, st, source_id)

        if _debug_match(station_id, source_id):
            print("[DEBUG raw type]", type(raw), flush=True)
            if isinstance(raw, dict):
                print("[DEBUG raw keys]", list(raw.keys()), flush=True)
                rr = raw.get("rows")
                if isinstance(rr, list) and rr:
                    print("[DEBUG raw first daily row]", json.dumps(rr[0], default=str)[:2000], flush=True)
                hrr = raw.get("hourly_rows")
                if isinstance(hrr, list) and hrr:
                    print("[DEBUG raw first hourly row]", json.dumps(hrr[0], default=str)[:2000], flush=True)
                hourly = raw.get("hourly")
                if isinstance(hourly, dict):
                    print("[DEBUG raw hourly keys]", list(hourly.keys())[:50], flush=True)
            elif isinstance(raw, list) and raw:
                print("[DEBUG raw first daily row]", json.dumps(raw[0], default=str)[:2000], flush=True)

        issued_at, daily_rows, hourly_rows = _normalize_payload(raw, fallback_issued_at=fallback_issued_at)

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

    fallback_issued_at = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

    tasks: List[concurrent.futures.Future] = []
    touched_run_ids: set[Any] = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as ex:
        for st in STATIONS:
            for source_id, fetcher in fetchers.items():
                tasks.append(ex.submit(_fetch_one, st, source_id, fetcher, fallback_issued_at))

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
                touched_run_ids.add(run_id)

                # --- Write DAILY (preferred: forecasts_daily; fallback: legacy forecasts) ---
                if daily_rows:
                    daily_batch: List[Dict[str, Any]] = []
                    legacy_batch: List[Dict[str, Any]] = []

                    for r in daily_rows:
                        td = r["target_date"]
                        # extras are intentionally NOT stored in forecasts_daily (ML data lives in hourly table)
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
                            "high_f": r["high"],
                            "low_f": r["low"],
                            "lead_high_hours": lead_high,
                            "lead_low_hours": lead_low,
                        })

                        # legacy (optional fallback)
                        if DEBUG_DUMP and _debug_match(station_id, source_id):
                            extras = r.get("extras") or {}
                            print("[DEBUG daily extras (ignored for forecasts_daily)]", extras, flush=True)

                        # Keep legacy writes only if forecasts_daily isn't present yet
                        legacy_batch.append({
                            "run_id": run_id,
                            "station_id": station_id,
                            "target_date": td,
                            "kind": "high",
                            "value_f": r["high"],
                            "lead_hours": lead_high,
                            "dewpoint_f": None,
                            "humidity_pct": None,
                            "wind_speed_mph": None,
                            "wind_dir_deg": None,
                            "cloud_cover_pct": None,
                            "precip_prob_pct": None,
                            "extras": "{}",
                        })
                        legacy_batch.append({
                            "run_id": run_id,
                            "station_id": station_id,
                            "target_date": td,
                            "kind": "low",
                            "value_f": r["low"],
                            "lead_hours": lead_low,
                            "dewpoint_f": None,
                            "humidity_pct": None,
                            "wind_speed_mph": None,
                            "wind_dir_deg": None,
                            "cloud_cover_pct": None,
                            "precip_prob_pct": None,
                            "extras": "{}",
                        })

                    wrote_daily = 0
                    try:
                        wrote_daily = bulk_upsert_forecasts_daily(conn, daily_batch)
                        conn.commit()
                    except Exception:
                        # if forecasts_daily not deployed yet, write legacy instead
                        wrote_legacy = bulk_upsert_forecast_values(conn, legacy_batch)
                        conn.commit()
                        print(
                            f"[morning] OK {station_id} {source_id}: wrote {wrote_legacy} legacy rows issued_at={issued_at}",
                            flush=True,
                        )
                    else:
                        print(
                            f"[morning] OK {station_id} {source_id}: wrote {wrote_daily} daily rows issued_at={issued_at}",
                            flush=True,
                        )

                # --- Write HOURLY extras (forecast_extras_hourly) ---
                if hourly_rows:
                    hourly_batch: List[Dict[str, Any]] = []
                    for hr in hourly_rows:
                        vt = hr.get("valid_time")
                        if not isinstance(vt, str) or not vt.strip():
                            continue

                        extras = hr.get("extras")
                        if isinstance(extras, str):
                            try:
                                extras = json.loads(extras)
                            except Exception:
                                extras = {}

                        if not isinstance(extras, dict):
                            extras = {}

                        hourly_batch.append({
                            "run_id": run_id,
                            "station_id": station_id,
                            "valid_time": vt,
                            "temperature_f": hr.get("temperature_f"),
                            "dewpoint_f": hr.get("dewpoint_f"),
                            "humidity_pct": hr.get("humidity_pct"),
                            "wind_speed_mph": hr.get("wind_speed_mph"),
                            "wind_dir_deg": hr.get("wind_dir_deg"),
                            "cloud_cover_pct": hr.get("cloud_cover_pct"),
                            "precip_prob_pct": hr.get("precip_prob_pct"),
                            "extras": extras,
                        })

                    if hourly_batch:
                        try:
                            wrote_hr = bulk_upsert_forecast_extras_hourly(conn, hourly_batch)
                            conn.commit()
                            print(
                                f"[morning] OK {station_id} {source_id}: wrote {wrote_hr} hourly rows issued_at={issued_at}",
                                flush=True,
                            )
                        except Exception as e:
                            print(f"[morning] hourly FAIL {station_id} {source_id}: {e}", flush=True)

            # Revisions: compute ONCE per run_id
            if touched_run_ids:
                for run_id in sorted(touched_run_ids, key=lambda x: str(x)):
                    try:
                        wrote_rev = compute_revisions_for_run(run_id)
                        if wrote_rev:
                            conn.commit()
                            print(f"[morning] revisions run_id={run_id}: {wrote_rev}", flush=True)
                    except Exception as e:
                        print(f"[morning] revisions FAIL run_id={run_id}: {e}", flush=True)


if __name__ == "__main__":
    main()
