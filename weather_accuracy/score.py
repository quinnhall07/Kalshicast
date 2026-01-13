# score.py
from __future__ import annotations

from datetime import date as ddate, timedelta
from typing import Optional, Tuple

from config import STATIONS
from sources_registry import enabled_source_ids
from db import get_conn, upsert_score, _placeholder  # <-- add _placeholder


def _get_observation(cur, station_id: str, target_date: str) -> Optional[Tuple[float, float]]:
    ph = _placeholder()
    cur.execute(
        f"SELECT observed_high, observed_low FROM observations WHERE station_id={ph} AND date={ph}",
        (station_id, target_date),
    )
    row = cur.fetchone()
    if not row:
        return None
    return float(row[0]), float(row[1])


def _get_forecast(cur, station_id: str, source_id: str, forecast_date: str, target_date: str) -> Optional[Tuple[float, float]]:
    ph = _placeholder()
    cur.execute(
        f"""
        SELECT high, low
        FROM forecasts
        WHERE station_id={ph} AND source_id={ph} AND forecast_date={ph} AND target_date={ph}
        """,
        (station_id, source_id, forecast_date, target_date),
    )
    row = cur.fetchone()
    if not row:
        return None
    return float(row[0]), float(row[1])


def score_day(target_date: str) -> None:
    """
    Score forecasts for a given completed calendar date (target_date = YYYY-MM-DD),
    for all configured stations and enabled sources.

    Writes scores keyed by:
      (station_id, source_id, forecast_date, target_date)

    Produces BOTH:
      - same-day score: forecast_date == target_date
      - next-day score: forecast_date == target_date - 1
    """
    enabled_sources = enabled_source_ids()
    if not enabled_sources:
        print("[score] No enabled sources in config.")
        return

    td = ddate.fromisoformat(target_date)
    prev_date = (td - timedelta(days=1)).isoformat()

    conn = get_conn()
    cur = conn.cursor()

    for st in STATIONS:
        station_id = st["station_id"]

        obs = _get_observation(cur, station_id, target_date)
        if not obs:
            print(f"[score] SKIP {station_id} {target_date}: no observation")
            continue

        oh, ol = obs
        wrote = 0
        misses = 0

        for source_id in enabled_sources:
            # same-day
            f_same = _get_forecast(cur, station_id, source_id, target_date, target_date)
            if f_same:
                fh, fl = f_same
                upsert_score(
                    station_id=station_id,
                    source_id=source_id,
                    forecast_date=target_date,
                    target_date=target_date,
                    high_error=abs(fh - oh),
                    low_error=abs(fl - ol),
                )
                wrote += 1
            else:
                misses += 1
                print(f"[score] MISS same-day {station_id} {source_id} forecast_date={target_date} target_date={target_date}")

            # next-day
            f_next = _get_forecast(cur, station_id, source_id, prev_date, target_date)
            if f_next:
                fh, fl = f_next
                upsert_score(
                    station_id=station_id,
                    source_id=source_id,
                    forecast_date=prev_date,
                    target_date=target_date,
                    high_error=abs(fh - oh),
                    low_error=abs(fl - ol),
                )
                wrote += 1
            else:
                misses += 1
                print(f"[score] MISS next-day {station_id} {source_id} forecast_date={prev_date} target_date={target_date}")

        if wrote == 0:
            print(
                f"[score] SKIP {station_id} {target_date}: no matching forecasts found "
                f"(expected after reset; need forecasts dated {prev_date} and/or {target_date})"
            )
        else:
            print(f"[score] OK {station_id} {target_date}: wrote {wrote} score row(s), {misses} miss(es)")

    conn.close()
