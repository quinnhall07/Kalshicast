# collect_wapi.py
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from config import HEADERS

WAPI_URL = "https://api.weatherapi.com/v1/forecast.json"

# Fixed horizon for this source:
# 4-day horizon => today..today+3 (4 target dates) and 96 hourly points
HORIZON_DAYS = 4
HORIZON_HOURS = 96

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


def _get_key() -> str:
    key = os.getenv("WEATHERAPI_KEY")
    if not key:
        raise RuntimeError("Missing WEATHERAPI_KEY env var")
    return key


def _utc_now_trunc_hour_z() -> str:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _epoch_to_time_z(epoch: Any) -> Optional[str]:
    if epoch is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(epoch), tz=timezone.utc).replace(second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
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
    WeatherAPI collector -> STRICT payload with normalized horizon:
      - daily: up to 4 target dates (today..today+3)
      - hourly: up to 96 hours starting at 00:00 UTC today

    Params:
      - include_hourly: bool (default True)
    """
    params = params or {}

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("WeatherAPI fetch requires station['lat'] and station['lon'].")

    include_hourly = True
    if params.get("include_hourly") is not None:
        include_hourly = bool(params["include_hourly"])

    key = _get_key()

    # WeatherAPI "days" is count starting today
    ndays = HORIZON_DAYS

    base = date.today()
    want_dates = {date.fromordinal(base.toordinal() + i).isoformat() for i in range(HORIZON_DAYS)}

    # Hourly normalization window: [today 00:00 UTC, +96h)
    hour0 = datetime.combine(base, datetime.min.time(), tzinfo=timezone.utc)
    hour_end = hour0 + timedelta(hours=HORIZON_HOURS)

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
        if not d or d not in want_dates:
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

            # Use time_epoch for UTC-safe timestamps
            t = _epoch_to_time_z(h.get("time_epoch"))
            if t is None:
                continue

            try:
                dt = datetime.fromisoformat(t[:-1] + "+00:00")  # convert "Z" -> "+00:00"
            except Exception:
                continue

            if dt < hour0 or dt >= hour_end:
                continue

            hourly_out["time"].append(t)
            hourly_out["temperature_f"].append(_to_float(h.get("temp_f")))
            hourly_out["dewpoint_f"].append(_to_float(h.get("dewpoint_f")))
            hourly_out["humidity_pct"].append(_to_float(h.get("humidity")))
            hourly_out["wind_speed_mph"].append(_to_float(h.get("wind_mph")))
            hourly_out["wind_dir_deg"].append(_to_float(h.get("wind_degree")))
            hourly_out["cloud_cover_pct"].append(_to_float(h.get("cloud")))
            hourly_out["precip_prob_pct"].append(_to_float(h.get("chance_of_rain")))

    # Sort + de-dup hourly by time (WeatherAPI can occasionally repeat at day boundaries)
    if hourly_out["time"]:
        idx = sorted(range(len(hourly_out["time"])), key=lambda i: hourly_out["time"][i])
        for k in list(hourly_out.keys()):
            hourly_out[k] = [hourly_out[k][i] for i in idx]

        seen = set()
        keep = []
        for i, t in enumerate(hourly_out["time"]):
            if t in seen:
                continue
            seen.add(t)
            keep.append(i)

        for k in list(hourly_out.keys()):
            hourly_out[k] = [hourly_out[k][i] for i in keep]

        # Enforce max 96 points
        for k in list(hourly_out.keys()):
            hourly_out[k] = hourly_out[k][:HORIZON_HOURS]

    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily}

    if include_hourly and hourly_out["time"]:
        # Hard align: truncate all arrays to min length
        min_len = min(len(v) for v in hourly_out.values())
        if min_len > 0:
            for k in list(hourly_out.keys()):
                hourly_out[k] = hourly_out[k][:min_len]
            out["hourly"] = hourly_out

    return out
