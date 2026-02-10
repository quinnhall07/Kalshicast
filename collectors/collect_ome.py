# collectors/collect_ome.py  (OME_BASE)
from __future__ import annotations

import requests
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from config import HEADERS

OME_URL = "https://api.open-meteo.com/v1/forecast"

# 4-day horizon: today + next 3 days = 4 target dates
HORIZON_DAYS = 4

_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
]

_HOURLY_VARS = [
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
    "precipitation_probability",
]


def _utc_now_trunc_hour_z() -> str:
    # Canonical issued_at for OME_BASE: fetch-time snapshot truncated to hour (UTC)
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def fetch_ome_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    STRICT payload shape (consumed by morning.py):
      {
        "issued_at": "...Z",
        "daily": [ {"target_date":"YYYY-MM-DD","high_f":float,"low_f":float}, ... ],
        "hourly": { "time":[...], "<var>":[...], ... }   # Open-Meteo arrays (unaggregated)
      }
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    today = date.today()
    end_date = today + timedelta(days=HORIZON_DAYS - 1)  # inclusive

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),

        # daily highs/lows
        "daily": ",".join(_DAILY_VARS),
        "start_date": today.isoformat(),
        "end_date": end_date.isoformat(),

        # hourly, unaggregated features
        "hourly": ",".join(_HOURLY_VARS),

        # consistent units
        "timezone": "UTC",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
    }

    if params:
        q.update(params)

    r = requests.get(OME_URL, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(f"Open-Meteo error: {data.get('reason') or data.get('message') or data}")

    # FIX: issued_at must be truncated-to-hour UTC (not "now with seconds")
    issued_at = _utc_now_trunc_hour_z()

    # ---- daily list ----
    daily = data.get("daily") or {}
    dates = daily.get("time") or []
    tmax = daily.get("temperature_2m_max") or []
    tmin = daily.get("temperature_2m_min") or []

    out_daily: List[dict] = []
    if isinstance(dates, list) and dates:
        n = min(len(dates), len(tmax), len(tmin))
        for i in range(n):
            td = str(dates[i])[:10]
            try:
                high_f = float(tmax[i])
                low_f = float(tmin[i])
            except Exception:
                continue
            out_daily.append({"target_date": td, "high_f": high_f, "low_f": low_f})

    # ---- hourly arrays ----
    hourly = data.get("hourly")
    out: Dict[str, Any] = {"issued_at": issued_at, "daily": out_daily}

    if isinstance(hourly, dict) and isinstance(hourly.get("time"), list) and hourly["time"]:
        out["hourly"] = hourly

    return out
