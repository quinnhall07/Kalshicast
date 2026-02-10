# collect_tom.py
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from config import HEADERS

TOM_URL = "https://api.tomorrow.io/v4/timelines"

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
    "dewpoint_f": [float|None, ...],          # may be unavailable
    "humidity_pct": [float|None, ...],
    "wind_speed_mph": [float|None, ...],
    "wind_dir_deg": [float|None, ...],
    "cloud_cover_pct": [float|None, ...],
    "precip_prob_pct": [float|None, ...],
  }
}

Notes:
- Tomorrow.io does not provide a stable "model run" timestamp here; we use fetch time truncated to hour UTC.
- We request 1d and (optionally) 1h timesteps in a single call.
"""


def _utc_now_trunc_hour_z() -> str:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _ensure_time_z(s: Any) -> Optional[str]:
    """
    Tomorrow.io returns ISO timestamps with 'Z' already.
    Normalize to minute precision with ':00Z' seconds if needed.
    """
    if not isinstance(s, str) or not s.strip():
        return None
    t = s.strip()
    try:
        if t.endswith("Z"):
            dt = datetime.fromisoformat(t[:-1] + "+00:00")
        else:
            dt = datetime.fromisoformat(t)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        # Fallback: just return yyyy-mm-ddThh:mm:00Z when possible
        if len(t) >= 16 and "T" in t:
            return t[:16] + ":00Z"
        return None


def fetch_tom_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Tomorrow.io collector -> STRICT payload.

    Params:
      - days_ahead: int (default 3) meaning today..today+days_ahead inclusive (so 3 -> 4 days)
      - include_hourly: bool (default True)

    Units:
      - units="imperial" => Fahrenheit, mph
    """
    params = params or {}

    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Tomorrow.io fetch requires station['lat'] and station['lon'].")

    key = os.getenv("TOMORROW_API_KEY")
    if not key:
        raise RuntimeError("Missing TOMORROW_API_KEY env var")

    days_ahead = 3
    if params.get("days_ahead") is not None:
        try:
            days_ahead = int(params["days_ahead"])
        except Exception:
            pass
    days_ahead = max(0, min(10, days_ahead))

    include_hourly = True
    if params.get("include_hourly") is not None:
        include_hourly = bool(params["include_hourly"])

    today = date.today()
    end_day = date.fromordinal(today.toordinal() + days_ahead)
    ndays = days_ahead + 1
    want = {date.fromordinal(today.toordinal() + i).isoformat() for i in range(ndays)}

    # Timeline span (UTC)
    start_time = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    end_time = datetime.combine(end_day, datetime.max.time(), tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    # Daily and hourly fields
    daily_fields = [
        "temperatureMax",
        "temperatureMin",
        "humidityAvg",
        "windSpeedAvg",
        "windDirectionAvg",
        "cloudCoverAvg",
        "precipitationProbabilityAvg",
    ]

    hourly_fields = [
        "temperature",
        "humidity",
        "windSpeed",
        "windDirection",
        "cloudCover",
        "precipitationProbability",
        # dewPoint is not always available on all plans; request anyway if present.
        "dewPoint",
    ]

    timesteps = ["1d"]
    fields = list(daily_fields)
    if include_hourly:
        timesteps.append("1h")
        # Tomorrow.io allows one unified "fields" list; it will return what’s relevant per timestep.
        fields.extend([f for f in hourly_fields if f not in fields])

    payload = {
        "location": f"{float(lat)},{float(lon)}",
        "fields": fields,
        "timesteps": timesteps,
        "units": "imperial",
        "startTime": start_time,
        "endTime": end_time,
        "timezone": "UTC",
    }

    r = requests.post(
        TOM_URL,
        params={"apikey": key},
        json=payload,
        headers=dict(HEADERS),
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()

    issued_at = _utc_now_trunc_hour_z()

    timelines = (data.get("data") or {}).get("timelines") or []
    if not timelines:
        return {"issued_at": issued_at, "daily": []}

    # Tomorrow.io returns separate timelines per timestep. Identify them.
    t_by_step: Dict[str, dict] = {}
    for tl in timelines:
        step = (tl.get("timestep") or "").strip()
        if step:
            t_by_step[step] = tl

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

    # ----- Daily -----
    d_tl = t_by_step.get("1d") or (timelines[0] if timelines else None)
    if isinstance(d_tl, dict):
        intervals = d_tl.get("intervals") or []
        if isinstance(intervals, list):
            for it in intervals:
                if not isinstance(it, dict):
                    continue
                start = it.get("startTime")
                vals = it.get("values") or {}
                if not isinstance(start, str) or not isinstance(vals, dict):
                    continue
                d = start[:10]
                if d not in want:
                    continue
                hi = _to_float(vals.get("temperatureMax"))
                lo = _to_float(vals.get("temperatureMin"))
                if hi is None or lo is None:
                    continue
                daily.append({"target_date": d, "high_f": float(hi), "low_f": float(lo)})

    # ----- Hourly -----
    if include_hourly:
        h_tl = t_by_step.get("1h")
        if isinstance(h_tl, dict):
            intervals = h_tl.get("intervals") or []
            if isinstance(intervals, list):
                for it in intervals:
                    if not isinstance(it, dict):
                        continue
                    start = _ensure_time_z(it.get("startTime"))
                    vals = it.get("values") or {}
                    if start is None or not isinstance(vals, dict):
                        continue

                    # Restrict to desired days (based on startTime date)
                    d = start[:10]
                    if d not in want:
                        continue

                    hourly_out["time"].append(start)

                    # Tomorrow.io uses Fahrenheit already (imperial)
                    hourly_out["temperature_f"].append(_to_float(vals.get("temperature")))
                    hourly_out["dewpoint_f"].append(_to_float(vals.get("dewPoint")))
                    hourly_out["humidity_pct"].append(_to_float(vals.get("humidity")))
                    hourly_out["wind_speed_mph"].append(_to_float(vals.get("windSpeed")))
                    hourly_out["wind_dir_deg"].append(_to_float(vals.get("windDirection")))
                    hourly_out["cloud_cover_pct"].append(_to_float(vals.get("cloudCover")))
                    hourly_out["precip_prob_pct"].append(_to_float(vals.get("precipitationProbability")))

    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily}

    if hourly_out["time"]:
        # Truncate all arrays to min length to guarantee alignment
        min_len = min(len(v) for v in hourly_out.values())
        if min_len > 0:
            for k in list(hourly_out.keys()):
                hourly_out[k] = hourly_out[k][:min_len]
            out["hourly"] = hourly_out

    return out
