from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List

from config import STATIONS
from sources_registry import load_fetchers_safe
from db import upsert_forecast, upsert_source, upsert_station, init_db


def _coerce_float(x: Any) -> float:
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        return float(x.strip())
    raise ValueError(f"not a number: {x!r}")


def _normalize_rows(rows: Any) -> List[Dict[str, Any]]:
    """
    Enforce standardized output:
      [{"target_date":"YYYY-MM-DD","high":float,"low":float}, ...]
    Filters out malformed entries instead of crashing the whole run.
    """
    if rows is None:
        return []
    if not isinstance(rows, list):
        raise ValueError(f"fetcher returned {type(rows)}; expected list[dict]")

    out: List[Dict[str, Any]] = []
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            print(f"[morning] WARN: skipping non-dict row at index {i}: {r!r}")
            continue
        if "target_date" not in r or "high" not in r or "low" not in r:
            print(f"[morning] WARN: skipping incomplete row at index {i}: {r!r}")
            continue
        try:
            target_date = str(r["target_date"])[:10]
            high = _coerce_float(r["high"])
            low = _coerce_float(r["low"])
        except Exception as e:
            print(f"[morning] WARN: skipping bad row at index {i}: {r!r} ({e})")
            continue

        out.append({"target_date": target_date, "high": high, "low": low})
    return out


def main() -> None:
    init_db()

    # Ensure stations exist (FKs)
    for st in STATIONS:
        upsert_station(st["station_id"], st.get("name"), st.get("lat"), st.get("lon"))

    # Load enabled fetchers safely (won't crash if one import fails)
    fetchers = load_fetchers_safe()
    if not fetchers:
        print("[morning] ERROR: no enabled sources loaded (check config.SOURCES).")
        return

    # Ensure sources exist (FKs)
    for source_id in fetchers.keys():
        upsert_source(source_id)

    forecast_date = date.today().isoformat()

    for st in STATIONS:
        station_id = st["station_id"]

        for source_id, fetcher in fetchers.items():
            # Set fetched_at per source call (more accurate than one timestamp for everything)
            fetched_at = datetime.now().isoformat(timespec="seconds")

            try:
                raw_rows = fetcher(st)
                rows = _normalize_rows(raw_rows)

                if not rows:
                    print(f"[morning] WARN {station_id} {source_id}: no rows returned")
                    continue

                for f in rows:
                    upsert_forecast(
                        station_id=station_id,
                        source_id=source_id,
                        forecast_date=forecast_date,
                        target_date=f["target_date"],
                        high=f["high"],
                        low=f["low"],
                        fetched_at=fetched_at,
                    )

                print(f"[morning] OK {station_id} {source_id}: saved {len(rows)} row(s)")
            except Exception as e:
                # Never break the whole run for a single source
                print(f"[morning] FAIL {station_id} {source_id}: {e}")


if __name__ == "__main__":
    main()
