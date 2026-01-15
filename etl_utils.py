# etl_utils.py
from __future__ import annotations

from datetime import datetime, date, time, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from config import TARGET_LOCAL_HOUR_HIGH, TARGET_LOCAL_HOUR_LOW


def utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_dt(s: str) -> datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_local_date(target_date: str) -> date:
    return date.fromisoformat(target_date[:10])


def compute_lead_hours(*, station_tz: str, issued_at: str, target_date: str, kind: str) -> float:
    """
    Lead time = (target local anchor time) - issued_at, in hours.
    Anchors:
      high -> TARGET_LOCAL_HOUR_HIGH (default 16:00 local)
      low  -> TARGET_LOCAL_HOUR_LOW  (default 07:00 local)
    """
    tz = ZoneInfo(station_tz)
    issued = parse_iso_dt(issued_at)

    d = to_local_date(target_date)
    anchor_hour = TARGET_LOCAL_HOUR_HIGH if kind == "high" else TARGET_LOCAL_HOUR_LOW
    target_local = datetime.combine(d, time(anchor_hour, 0), tzinfo=tz)
    target_utc = target_local.astimezone(timezone.utc)

    return (target_utc - issued).total_seconds() / 3600.0
