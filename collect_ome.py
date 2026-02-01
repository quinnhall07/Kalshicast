# collect_ome.py
from __future__ import annotations

import requests
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from config import HEADERS

OME_URL = "https://api.open-meteo.com/v1/forecast"

# -------------------------
# Daily vars (kept minimal; ML-grade features come from hourly)
# -------------------------
_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
]

# -------------------------
# Hourly vars (for forecast_extras_hourly; NO aggregation)
# Use Open-Meteo's standard names.
# -------------------------
_HOURLY_VARS = [
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
    "precipitation_probability",
]


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_ome_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Returns payload compatible with morning.py:
      {
        "issued_at": "...Z",
        "rows": [ {target_date, high, low, extras?}, ... ],          # daily
        "hourly": { "time": [...], "<var>": [...], ... } OR
        "hourly_rows": [ {valid_time, temperature_f, dewpoint_f, ...}, ... ]  # optional
      }

    We use Open-Meteo arrays under "hourly" to avoid inflating payload size in Python.
    morning.py normalizes + stores hourly rows without aggregating.
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    today = date.today()
    tomorrow = date.fromordinal(today.toordinal() + 1)

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),

        # daily highs/lows
        "daily": ",".join(_DAILY_VARS),
        "start_date": today.isoformat(),
        "end_date": tomorrow.isoformat(),

        # hourly, unaggregated features for ML later
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

    issued_at = _utc_now_z()

    # -------------------------
    # Daily rows
    # -------------------------
    daily = data.get("daily") or {}
    dates = daily.get("time") or []
    tmax = daily.get("temperature_2m_max") or []
    tmin = daily.get("temperature_2m_min") or []

    rows: List[dict] = []
    if isinstance(dates, list) and dates:
        n = min(len(dates), len(tmax), len(tmin))
        for i in range(n):
            td = str(dates[i])[:10]
            try:
                high = float(tmax[i])
                low = float(tmin[i])
            except Exception:
                continue
            rows.append({"target_date": td, "high": high, "low": low})

    # -------------------------
    # Hourly arrays (preferred shape)
    # -------------------------
    hourly = data.get("hourly")
    hourly_out: Optional[dict] = None
    if isinstance(hourly, dict) and isinstance(hourly.get("time"), list) and hourly["time"]:
        # Pass through as-is; morning.py handles aligning by index + mapping to standardized columns.
        hourly_out = hourly

    out: Dict[str, Any] = {"issued_at": issued_at, "rows": rows}
    if hourly_out is not None:
        out["hourly"] = hourly_out

    return out
