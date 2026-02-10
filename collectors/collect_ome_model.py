# collect_ome_model.py
from __future__ import annotations

import math
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

from config import HEADERS

OME_URL = "https://api.open-meteo.com/v1/forecast"

"""
Collector contract REQUIRED by morning.py:

{
  "issued_at": "2026-02-01T06:00:00Z",
  "daily": [ {"target_date":"YYYY-MM-DD","high_f":float,"low_f":float}, ... ],
  "hourly": { "time":[...], optional variable arrays ... }  # Open-Meteo style arrays
}

For Open-Meteo, there is no reliable provider-issued timestamp for a forecast “run”.
Permanent decision: issued_at = fetch time truncated to the hour (UTC).
"""

# Lower timeouts to reduce tail latency + retry storms (morning.py handles retries).
# Use a (connect, read) timeout tuple for better control.
DEFAULT_TIMEOUT: Tuple[float, float] = (5.0, 15.0)


def _utc_now_trunc_hour_z() -> str:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def _ensure_time_z(ts: Any) -> Optional[str]:
    """
    Open-Meteo hourly time is typically "YYYY-MM-DDTHH:MM" in UTC (no tz).
    Convert to "YYYY-MM-DDTHH:MM:00Z" so Postgres ::timestamptz is unambiguous.
    """
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s[:-1] + "+00:00")
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        if len(s) >= 16 and s[10] == "T":
            return s[:16] + ":00Z"
        return None


def fetch_ome_model_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Open-Meteo multi-model collector (non-base models).

    Expects params to include model selection, e.g.:
      {"model": "gfs_seamless"} or {"models": "icon_seamless"} depending on Open-Meteo API.

    Also supports:
      - days_ahead: int (default 3; clamped 1..7) meaning today..today+days_ahead inclusive
        NOTE: your overall project wants a 4-day horizon for WAPI, but Open-Meteo models can remain as configured
        via params / config. (morning.py does not enforce horizon.)

    Returns:
      - daily highs/lows in F: high_f/low_f
      - hourly arrays object (Open-Meteo native arrays), with time normalized to "...Z"
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    p: Dict[str, Any] = dict(params or {})

    days_ahead = 3
    if p.get("days_ahead") is not None:
        try:
            days_ahead = int(p.pop("days_ahead"))
        except Exception:
            pass
    days_ahead = max(1, min(7, days_ahead))

    start = date.today()
    end = start + timedelta(days=days_ahead)

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),
        "timezone": "UTC",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "daily": "temperature_2m_max,temperature_2m_min",
        "hourly": ",".join(
            [
                "temperature_2m",
                "dew_point_2m",
                "relative_humidity_2m",
                "wind_speed_10m",
                "wind_direction_10m",
                "cloud_cover",
                "precipitation_probability",
            ]
        ),
    }
    q.update(p)

    # Allow overriding timeout via env if needed (e.g., in CI).
    # Format: "connect,read" or single float for both.
    timeout = DEFAULT_TIMEOUT
    env_t = os.getenv("OME_TIMEOUT", "").strip()
    if env_t:
        try:
            if "," in env_t:
                a, b = env_t.split(",", 1)
                timeout = (float(a.strip()), float(b.strip()))
            else:
                v = float(env_t)
                timeout = (v, v)
        except Exception:
            timeout = DEFAULT_TIMEOUT

    r = requests.get(OME_URL, params=q, headers=dict(HEADERS), timeout=timeout)
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(f"Open-Meteo error: {data.get('reason') or data.get('message') or data}")

    issued_at = _utc_now_trunc_hour_z()

    # -------- Daily --------
    daily_rows: List[Dict[str, Any]] = []
    daily = data.get("daily") or {}
    d_time = daily.get("time") or []
    d_hi = daily.get("temperature_2m_max") or []
    d_lo = daily.get("temperature_2m_min") or []

    if isinstance(d_time, list) and isinstance(d_hi, list) and isinstance(d_lo, list):
        n = min(len(d_time), len(d_hi), len(d_lo))
        for i in range(n):
            td = str(d_time[i])[:10]
            hi = _to_float(d_hi[i])
            lo = _to_float(d_lo[i])
            if hi is None or lo is None:
                continue
            daily_rows.append({"target_date": td, "high_f": float(hi), "low_f": float(lo)})

    # If daily missing, derive from hourly temperature_2m
    if not daily_rows:
        hourly = data.get("hourly") or {}
        h_time = hourly.get("time") or []
        h_temp = hourly.get("temperature_2m") or []
        if isinstance(h_time, list) and isinstance(h_temp, list) and h_time and h_temp:
            by_day: Dict[str, List[float]] = {}
            for t, v in zip(h_time, h_temp):
                td = str(t)[:10]
                fv = _to_float(v)
                if fv is None:
                    continue
                by_day.setdefault(td, []).append(float(fv))
            for td, vals in sorted(by_day.items()):
                if not vals:
                    continue
                daily_rows.append({"target_date": td, "high_f": max(vals), "low_f": min(vals)})

    # -------- Hourly (arrays object) --------
    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily_rows}

    hourly = data.get("hourly")
    if isinstance(hourly, dict):
        times = hourly.get("time")
        if isinstance(times, list) and times:
            tz_times: List[str] = []
            for t in times:
                tz = _ensure_time_z(t)
                if tz is None:
                    tz_times = []
                    break
                tz_times.append(tz)

            if tz_times:
                hourly_norm: Dict[str, Any] = dict(hourly)
                hourly_norm["time"] = tz_times
                out["hourly"] = hourly_norm

    return out
