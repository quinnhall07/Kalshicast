# collectors/time_axis.py
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import List, Set, Tuple


def _utc_now_trunc_hour() -> datetime:
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _dt_to_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def build_hourly_axis_z(days: int, *, start_utc: datetime | None = None) -> List[str]:
    """
    Build a forward-looking hourly UTC axis of length days*24.

    - If start_utc is not provided, uses current UTC time truncated to the hour.
    - Returned timestamps are strings like "YYYY-MM-DDTHH:00:00Z".
    - days is clamped to >=1.
    """
    d = int(days) if isinstance(days, int) else 1
    if d < 1:
        d = 1

    t0 = start_utc
    if t0 is None:
        t0 = _utc_now_trunc_hour()
    if t0.tzinfo is None:
        t0 = t0.replace(tzinfo=timezone.utc)
    t0 = t0.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    n = d * 24
    return [_dt_to_z(t0 + timedelta(hours=i)) for i in range(n)]


def hourly_axis_set(axis: List[str]) -> Set[str]:
    """
    Convenience: build a set for O(1) membership checks.
    """
    return set(axis or [])


def axis_start_end(axis: List[str]) -> Tuple[datetime, datetime]:
    """
    Convert an axis list to inclusive UTC datetime bounds.

    Returns (start_utc, end_utc_inclusive).
    """
    if not axis:
        t0 = _utc_now_trunc_hour()
        return t0, t0

    def _parse_z(s: str) -> datetime:
        # Expect "...Z"
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)

    start = _parse_z(axis[0])
    end = _parse_z(axis[-1])
    return start, end


def daily_targets_from_axis(axis: List[str]) -> List[str]:
    """
    Produce daily target dates (YYYY-MM-DD) covered by the hourly axis,
    in chronological order, without duplicates.

    This is intended for building exactly FORECAST_DAYS daily rows when
    axis was built from build_hourly_axis_z(FORECAST_DAYS).
    """
    if not axis:
        return []

    seen = set()
    out: List[str] = []
    for t in axis:
        d = t[:10]
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def build_daily_targets(days: int, *, start_utc: datetime | None = None) -> List[str]:
    """
    Build exactly `days` daily target dates based on the same forward-looking
    policy as the hourly axis.

    Uses UTC dates derived from the hourly axis start time.
    """
    axis = build_hourly_axis_z(days, start_utc=start_utc)
    targets = daily_targets_from_axis(axis)
    # Enforce exactly `days` targets when possible.
    if len(targets) > days:
        targets = targets[:days]
    return targets


def truncate_issued_at_to_hour_z(ts: str | datetime | None) -> str | None:
    """
    Normalize any reasonable timestamp into UTC hour-truncated Z format:
      "YYYY-MM-DDTHH:00:00Z"

    Accepts:
      - datetime
      - ISO string with Z or offset
      - naive ISO string (assumed UTC)

    Returns None if unparsable.
    """
    if ts is None:
        return None

    dt: datetime | None = None

    if isinstance(ts, datetime):
        dt = ts
    elif isinstance(ts, str):
        s = ts.strip()
        if not s:
            return None
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return _dt_to_z(dt)
