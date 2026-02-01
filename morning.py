# morning.py
from __future__ import annotations

import concurrent.futures
import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import threading

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
    bulk_upsert_forecast_values,
)

import os

DEBUG_SOURCE = os.getenv("DEBUG_SOURCE")   # e.g. "OME_GFS"
DEBUG_STATION = os.getenv("DEBUG_STATION") # e.g. "KMDW"
DEBUG_DUMP = os.getenv("DEBUG_DUMP", "0") == "1"

MAX_ATTEMPTS = 4
BASE_SLEEP_SECONDS = 1.0
FETCH_TIMEOUT_SECONDS = 30

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
    if source_id.startswith("OME_"):
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

            print(f"[morning] RETRY {station['station_id']} {source_id} attempt {attempt}/{MAX_ATTEMPTS}: {e}", flush=True)
            time.sleep(sleep_s)

    raise last_exc  # pragma: no cover

def _normalize_payload(raw: Any, *, fallback_issued_at: str) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Accept:
      - old: list[{target_date, high, low, ...extras}]
      - new: {issued_at: "...Z", rows: [...]}

    Returns (issued_at_utc_iso, rows)

    fallback_issued_at MUST be stable for the whole workflow run to avoid
    run fragmentation (multiple runs for the same provider in one workflow).
    """
    issued_at = fallback_issued_at

    if raw is None:
        return issued_at, []

    if isinstance(raw, dict):
        if isinstance(raw.get("issued_at"), str) and raw["issued_at"].strip():
            issued_at = raw["issued_at"]
        rows = raw.get("rows")
        if not isinstance(rows, list):
            raise ValueError("payload dict must include rows: list[dict]")
        raw = rows

    if not isinstance(raw, list):
        raise ValueError(f"fetcher returned {type(raw)}; expected list[dict] or dict")

    out: List[Dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        if "target_date" not in r or "high" not in r or "low" not in r:
            continue

        td = str(r["target_date"])[:10]
        high = _coerce_float(r["high"])
        low = _coerce_float(r["low"])

                extras: Dict[str, Any] = {}

        # 1) Pull from top-level (your current behavior)
        for k in (
            "dewpoint_f", "humidity_pct", "wind_speed_mph", "wind_dir_deg",
            "cloud_cover_pct", "precip_prob_pct"
        ):
            if k in r and r[k] is not None:
                extras[k] = r[k]

        # 2) Also merge nested extras if present
        nested = r.get("extras")
        if isinstance(nested, dict):
            for k in (
                "dewpoint_f", "humidity_pct", "wind_speed_mph", "wind_dir_deg",
                "cloud_cover_pct", "precip_prob_pct"
            ):
                if k in nested and nested[k] is not None:
                    extras.setdefault(k, nested[k])

        out.append({"target_date": td, "high": high, "low": low, "extras": extras})

    return issued_at, out

def _fetch_one(st: dict, source_id: str, fetcher, fallback_issued_at: str):
    station_id = st["station_id"]
    try:
        raw = _call_fetcher_with_retry(fetcher, st, source_id)
        if DEBUG_DUMP and (DEBUG_SOURCE == source_id) and (DEBUG_STATION == station_id):
            # Print only the keys + a small sample to avoid huge logs / secrets
            if isinstance(raw, dict):
                print("[debug] raw dict keys:", list(raw.keys()), flush=True)
                rows = raw.get("rows")
                if isinstance(rows, list) and rows:
                    print("[debug] raw first row:", json.dumps(rows[0], default=str)[:2000], flush=True)
            elif isinstance(raw, list) and raw:
                print("[debug] raw first row:", json.dumps(raw[0], default=str)[:2000], flush=True)
            else:
                print("[debug] raw type:", type(raw), flush=True)

        issued_at, rows = _normalize_payload(raw, fallback_issued_at=fallback_issued_at)
        return (station_id, st, source_id, issued_at, rows, None)
    except Exception as e:
        return (station_id, st, source_id, None, [], e)

def main() -> None:
    init_db()

    for st in STATIONS:
        upsert_location(st)

    fetchers = load_fetchers_safe()
    if not fetchers:
        print("[morning] ERROR: no enabled sources loaded (check config.SOURCES).", flush=True)
        return

    # Stable fallback issued_at for this entire workflow run (prevents run fragmentation)
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

        # ONE shared DB connection for the write phase (fast)
        with get_conn() as conn:
            for fut in concurrent.futures.as_completed(tasks):
                station_id, st, source_id, issued_at, rows, err = fut.result()

                if err is not None:
                    print(f"[morning] FAIL {station_id} {source_id}: {err}", flush=True)
                    continue

                if not rows:
                    print(f"[morning] WARN {station_id} {source_id}: no rows", flush=True)
                    continue

                # IMPORTANT: use shared conn here
                run_id = get_or_create_forecast_run(source=source_id, issued_at=issued_at, conn=conn)
                touched_run_ids.add(run_id)

                # Build a batch (2 rows per target_date: high + low)
                batch: List[Dict[str, Any]] = []
                for r in rows:
                    td = r["target_date"]
                    extras = r.get("extras") or {}

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

                    batch.append({
                        "run_id": run_id,
                        "station_id": station_id,
                        "target_date": td,
                        "kind": "high",
                        "value_f": r["high"],
                        "lead_hours": lead_high,
                        "dewpoint_f": extras.get("dewpoint_f"),
                        "humidity_pct": extras.get("humidity_pct"),
                        "wind_speed_mph": extras.get("wind_speed_mph"),
                        "wind_dir_deg": extras.get("wind_dir_deg"),
                        "cloud_cover_pct": extras.get("cloud_cover_pct"),
                        "precip_prob_pct": extras.get("precip_prob_pct"),
                        "extras": json.dumps(extras),
                    })

                    batch.append({
                        "run_id": run_id,
                        "station_id": station_id,
                        "target_date": td,
                        "kind": "low",
                        "value_f": r["low"],
                        "lead_hours": lead_low,
                        "dewpoint_f": extras.get("dewpoint_f"),
                        "humidity_pct": extras.get("humidity_pct"),
                        "wind_speed_mph": extras.get("wind_speed_mph"),
                        "wind_dir_deg": extras.get("wind_dir_deg"),
                        "cloud_cover_pct": extras.get("cloud_cover_pct"),
                        "precip_prob_pct": extras.get("precip_prob_pct"),
                        "extras": json.dumps(extras),
                    })

                wrote = bulk_upsert_forecast_values(conn, batch)
                conn.commit()
                print(f"[morning] OK {station_id} {source_id}: wrote {wrote} rows issued_at={issued_at}", flush=True)

            # Revisions: compute ONCE per run_id (not per station)
            if touched_run_ids:
                for run_id in sorted(touched_run_ids, key=lambda x: str(x)):
                    try:
                        wrote_rev = compute_revisions_for_run(run_id)
                        if wrote_rev:
                            conn.commit()
                            print(f"[morning] revisions run_id={run_id}: {wrote_rev}", flush=True)
                    except Exception as e:
                        print(f"[morning] revisions FAIL run_id={run_id}: {e}", flush=True)

    # Provider throttle isn't needed here anymore because we already throttle in fetch.
    # If you still want extra Open-Meteo pacing, implement it inside the fetcher itself.

if __name__ == "__main__":
    main()


