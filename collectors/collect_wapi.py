# collect_wapi.py
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from config import HEADERS

WAPI_URL = "https://api.weatherapi.com/v1/forecast.json"

"""
STRICT payload shape required by sources_registry + morning.py:

{
  "issued_at": "...Z",
  "daily": [
    {"target_date": "YYYY-MM-DD", "high_f": float, "low_f": float},
    ...
  ],
  "hourly": {                     # optional
    "time": ["YYYY-MM-DDTHH:MM:00Z", ...],
    "temperature_f": [float|None, ...],
    "dewpoint_f": [float|None, ...],
    "humidity_pct": [float|None, ...],
    "wind_speed_mph": [float|None, ...],
    "wind_dir_deg": [float|None, ...],
    "cloud_cover_pct": [float|None, ...],
    "precip_prob_pct": [float|None, ...],
  }
}
"""

# WeatherAPI hourly fields available in forecastday.hour:
#   time_epoch, time, temp_f, dewpoint_f?, humidity, wind_mph, wind_degree, cloud, chance_of_rain, chance_of_snow
# dewpoint_f is not always present on all plans/versions; treat as optional.


def _get_key() -> str:
    key = os.getenv("WEATHERAPI_KEY")
    if not key:
        raise RuntimeError("Missing WEATHERAPI_KEY env var")
    return key


def _utc_now_trunc_hour_z() -> str:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _ensure_time_z(ts: Any) -> Optional[str]:
    """
    WeatherAPI 'time' usually looks like: "2026-02-10 01:00" (local time).
    We cannot trust its timezone without location tz metadata, and we standardize everything to UTC.
    WeatherAPI also returns time_epoch (seconds since epoch, UTC). Use that when available.

    This helper accepts either:
      - int/float epoch seconds
      - ISO-ish string (best-effort; assumed UTC if tz missing)
    """
    if ts is None:
        return None

    # Prefer epoch seconds
    if isinstance(ts, (int, float)):
        try:
            dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
            dt = dt.replace(second=0, microsecond=0)
            return dt.isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    if isinstance(ts, str) and ts.strip():
        s = ts.strip()
        # WeatherAPI sometimes uses "YYYY-MM-DD HH:MM"
        # Treat as UTC-naive (not perfect), but we should prefer epoch.
        try:
            s2 = s.replace(" ", "T")
            dt = datetime.fromisoformat(s2)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
            return dt.isoformat().replace("+00:00", "Z")
        except Exception:
            return None

    return None


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def fetch_wapi_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    WeatherAPI collector -> STRICT payload.

    Params:
      - days_ahead: int (default 3)  meaning today..today+days_ahead inclusive (so 3 -> 4 days)
        WeatherAPI uses "days" = number of forecast days starting today.

      - include_hourly: bool (default True)

    Notes:
      - issued_at: WeatherAPI does not provide a clean "model run" timestamp; use fetch time truncated to hour UTC.
      - hourly: use time_epoch for UTC-safe timestamps.
    """
    params = params or {}

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("WeatherAPI fetch requires station['lat'] and station['lon'].")

    key = _get_key()

    days_ahead = 3
    if params.get("days_ahead") is not None:
        try:
            days_ahead = int(params["days_ahead"])
        except Exception:
            pass
    days_ahead = max(0, min(7, days_ahead))

    # WeatherAPI "days" is count starting today
    ndays = days_ahead + 1

    include_hourly = True
    if params.get("include_hourly") is not None:
        include_hourly = bool(params["include_hourly"])

    base = date.today()
    want = {date.fromordinal(base.toordinal() + i).isoformat() for i in range(ndays)}

    q = {
        "key": key,
        "q": f"{float(lat)},{float(lon)}",
        "days": ndays,
        "aqi": "no",
        "alerts": "no",
    }

    r = requests.get(WAPI_URL, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    issued_at = _utc_now_trunc_hour_z()

    forecast_days = (data.get("forecast") or {}).get("forecastday") or []
    daily: List[Dict[str, Any]] = []

    # Hourly arrays
    hourly_out: Dict[str, List[Any]] = {
        "time": [],
        "temperature_f": [],
        "dewpoint_f": [],
        "humidity_pct": [],
        "wind_speed_mph": [],
        "wind_dir_deg": [],
        "cloud_cover_pct": [],
        "precip_prob_pct": [],
    }

    for day in forecast_days:
        d = str(day.get("date") or "")[:10]
        if not d or d not in want:
            continue

        daydata = day.get("day") or {}
        hi = _to_float(daydata.get("maxtemp_f"))
        lo = _to_float(daydata.get("mintemp_f"))
        if hi is not None and lo is not None:
            daily.append({"target_date": d, "high_f": float(hi), "low_f": float(lo)})

        if not include_hourly:
            continue

        hours = day.get("hour") or []
        if not isinstance(hours, list):
            continue

        for h in hours:
            if not isinstance(h, dict):
                continue

            # Use epoch for robust UTC
            t = _ensure_time_z(h.get("time_epoch"))
            if t is None:
                # fallback (best effort)
                t = _ensure_time_z(h.get("time"))
            if t is None:
                continue

            hourly_out["time"].append(t)

            hourly_out["temperature_f"].append(_to_float(h.get("temp_f")))
            # dewpoint not guaranteed
            hourly_out["dewpoint_f"].append(_to_float(h.get("dewpoint_f")))
            hourly_out["humidity_pct"].append(_to_float(h.get("humidity")))
            hourly_out["wind_speed_mph"].append(_to_float(h.get("wind_mph")))
            hourly_out["wind_dir_deg"].append(_to_float(h.get("wind_degree")))
            hourly_out["cloud_cover_pct"].append(_to_float(h.get("cloud")))

            # Prefer chance_of_rain (already percent)
            pop = _to_float(h.get("chance_of_rain"))
            if pop is None:
                # sometimes nested "condition" doesn't have prob; keep None
                pop = None
            hourly_out["precip_prob_pct"].append(pop)

    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily}

    # Only include hourly if we actually populated it and it's index-aligned
    if hourly_out["time"]:
        n = len(hourly_out["time"])
        # Hard align: truncate all arrays to min length
        min_len = min(len(v) for v in hourly_out.values())
        if min_len > 0 and min_len != n:
            for k in list(hourly_out.keys()):
                hourly_out[k] = hourly_out[k][:min_len]
        out["hourly"] = hourly_out

    return out
