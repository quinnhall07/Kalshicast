# collectors/collect_vcr.py
from __future__ import annotations

import os
from datetime import date, datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import requests

from config import HEADERS, FORECAST_DAYS

VCR_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

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

Horizon policy:
- Uses config.FORECAST_DAYS (today..today+FORECAST_DAYS-1 inclusive).
- Source params no longer control days; only include_hourly remains optional.
"""


def _utc_now_trunc_hour_z() -> str:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _get_key() -> str:
    key = os.getenv("VISUALCROSSING_KEY")
    if not key:
        raise RuntimeError("Missing VISUALCROSSING_KEY env var")
    return key


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _epoch_to_time_z(epoch: Any) -> Optional[str]:
    """
    Visual Crossing 'hours' can provide datetimeEpoch (seconds since epoch).
    Prefer that to avoid timezone ambiguity.
    """
    if epoch is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(epoch), tz=timezone.utc)
        dt = dt.replace(second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def fetch_vcr_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Visual Crossing collector -> STRICT payload.

    Params:
      - include_hourly: bool (default True)

    Notes:
      - unitGroup=us returns Fahrenheit, mph, etc.
      - issued_at: no reliable model-run timestamp; use fetch time truncated to hour UTC.
      - hourly: uses datetimeEpoch for UTC-safe timestamps.
      - horizon: config.FORECAST_DAYS (centralized).
    """
    params = params or {}

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Visual Crossing fetch requires station['lat'] and station['lon'].")

    key = _get_key()

    include_hourly = True
    if params.get("include_hourly") is not None:
        include_hourly = bool(params["include_hourly"])

    # Centralized horizon
    ndays = max(1, min(14, int(FORECAST_DAYS)))
    base = date.today()
    end_day = base + timedelta(days=ndays - 1)

    start = base.isoformat()
    end = end_day.isoformat()
    want = {(base + timedelta(days=i)).isoformat() for i in range(ndays)}

    q = {
        "unitGroup": "us",
        "key": key,
        "contentType": "json",
        # If hourly desired, ask for both; otherwise days only.
        "include": "days,hours" if include_hourly else "days",
    }

    url = f"{VCR_URL}/{float(lat)},{float(lon)}/{start}/{end}"
    r = requests.get(url, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    issued_at = _utc_now_trunc_hour_z()

    days = data.get("days") or []
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

    if isinstance(days, list):
        for drec in days:
            if not isinstance(drec, dict):
                continue
            d = str(drec.get("datetime") or "")[:10]
            if not d or d not in want:
                continue

            hi = _to_float(drec.get("tempmax"))
            lo = _to_float(drec.get("tempmin"))
            if hi is not None and lo is not None:
                daily.append({"target_date": d, "high_f": float(hi), "low_f": float(lo)})

            if not include_hourly:
                continue

            hours = drec.get("hours") or []
            if not isinstance(hours, list):
                continue

            for h in hours:
                if not isinstance(h, dict):
                    continue

                t = _epoch_to_time_z(h.get("datetimeEpoch"))
                if t is None:
                    continue

                # Restrict to desired days
                if t[:10] not in want:
                    continue

                hourly_out["time"].append(t)
                hourly_out["temperature_f"].append(_to_float(h.get("temp")))
                hourly_out["dewpoint_f"].append(_to_float(h.get("dew")))
                hourly_out["humidity_pct"].append(_to_float(h.get("humidity")))
                hourly_out["wind_speed_mph"].append(_to_float(h.get("windspeed")))
                hourly_out["wind_dir_deg"].append(_to_float(h.get("winddir")))
                hourly_out["cloud_cover_pct"].append(_to_float(h.get("cloudcover")))
                hourly_out["precip_prob_pct"].append(_to_float(h.get("precipprob")))

    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily}

    if hourly_out["time"]:
        # Truncate all arrays to min length to guarantee alignment
        min_len = min(len(v) for v in hourly_out.values())
        if min_len > 0:
            for k in list(hourly_out.keys()):
                hourly_out[k] = hourly_out[k][:min_len]
            out["hourly"] = hourly_out

    return out
