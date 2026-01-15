# morning.py
from __future__ import annotations
import concurrent.futures
import json
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import requests

from config import STATIONS
from sources_registry import load_fetchers_safe
from db import (
    init_db,
    upsert_location,
    get_or_create_forecast_run,
    upsert_forecast_value,
    compute_revisions_for_run
)


MAX_ATTEMPTS = 5
BASE_SLEEP_SECONDS = 2.0
FETCH_TIMEOUT_SECONDS = 30

def _coerce_float(x: Any) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        return float(x.strip())
    raise ValueError(f"not a number: {x!r}")


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


_executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)

def _call_fetcher_with_retry(fetcher, station: dict, source_id: str) -> Any:
    last_exc: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"[morning] fetch start {station['station_id']} {source_id} attempt={attempt}", flush=True)
            fut = _executor.submit(fetcher, station)
            return fut.result(timeout=FETCH_TIMEOUT_SECONDS)
        except concurrent.futures.TimeoutError as e:
            last_exc = e
            msg = f"fetch timeout after {FETCH_TIMEOUT_SECONDS}s"
            if attempt >= MAX_ATTEMPTS:
                raise RuntimeError(msg) from e
            sleep_s = min(5.0, (BASE_SLEEP_SECONDS * attempt) + random.random() * 0.5)
            print(f"[morning] RETRY {station['station_id']} {source_id} attempt {attempt}/{MAX_ATTEMPTS}: {msg}", flush=True)
            time.sleep(sleep_s)
        except Exception as e:
            last_exc = e
            if attempt >= MAX_ATTEMPTS or not _is_retryable_error(e):
                raise
            sleep_s = min(5.0, (BASE_SLEEP_SECONDS * attempt) + random.random() * 0.5)
            print(f"[morning] RETRY {station['station_id']} {source_id} attempt {attempt}/{MAX_ATTEMPTS}: {e}", flush=True)
            time.sleep(sleep_s)
    raise last_exc  # unreachable
    

def _normalize_payload(raw: Any) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Accept:
      - old: list[{target_date, high, low, ...extras}]
      - new: {issued_at: "...Z", rows: [...]}

    Returns (issued_at_utc_iso, rows)
    """
    issued_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if raw is None:
        return issued_at, []

    if isinstance(raw, dict):
        if isinstance(raw.get("issued_at"), str):
            issued_at = raw["issued_at"]
        rows = raw.get("rows")
        if not isinstance(rows, list):
            raise ValueError("payload dict must include rows: list[dict]")
        raw = rows

    if not isinstance(raw, list):
        raise ValueError(f"fetcher returned {type(raw)}; expected list[dict] or dict")

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(raw):
        if not isinstance(r, dict):
            continue
        if "target_date" not in r or "high" not in r or "low" not in r:
            continue
        td = str(r["target_date"])[:10]
        high = _coerce_float(r["high"])
        low = _coerce_float(r["low"])

        extras = {k: r[k] for k in (
            "dewpoint_f", "humidity_pct", "wind_speed_mph", "wind_dir_deg",
            "cloud_cover_pct", "precip_prob_pct"
        ) if k in r}

        out.append({"target_date": td, "high": high, "low": low, "extras": extras})

    return issued_at, out


def main() -> None:
    init_db()

    for st in STATIONS:
        upsert_location(st)

    fetchers = load_fetchers_safe()
    if not fetchers:
        print("[morning] ERROR: no enabled sources loaded (check config.SOURCES).")
        return

    for st in STATIONS:
        station_id = st["station_id"]

        for source_id, fetcher in fetchers.items():
            try:
                raw = _call_fetcher_with_retry(fetcher, st, source_id)
                issued_at, rows = _normalize_payload(raw)

                if not rows:
                    print(f"[morning] WARN {station_id} {source_id}: no rows")
                    continue

                run_id = get_or_create_forecast_run(source=source_id, issued_at=issued_at, meta_json=json.dumps({
                    "station_id": station_id
                }))

                for r in rows:
                    td = r["target_date"]
                    extras = r.get("extras") or {}

                    from etl_utils import compute_lead_hours
                    
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
                    
                    upsert_forecast_value(
                        run_id=run_id,
                        station_id=station_id,
                        target_date=td,
                        kind="high",
                        value_f=r["high"],
                        lead_hours=lead_high,
                        extras=extras,
                    )
                    
                    upsert_forecast_value(
                        run_id=run_id,
                        station_id=station_id,
                        target_date=td,
                        kind="low",
                        value_f=r["low"],
                        lead_hours=lead_low,
                        extras=extras,
                    )
                print(f"[morning] OK {station_id} {source_id}: saved {len(rows)} day(s) issued_at={issued_at}")
                try:
                    wrote_rev = compute_revisions_for_run(run_id)
                    if wrote_rev:
                        print(f"[morning] revisions {station_id} {source_id}: {wrote_rev}")
                except Exception as e:
                    print(f"[morning] revisions FAIL {station_id} {source_id}: {e}")
            except Exception as e:
                print(f"[morning] FAIL {station_id} {source_id}: {e}")

            if source_id.startswith("OME_"):
                time.sleep(0.4 + random.random() * 0.6)


if __name__ == "__main__":
    main()



