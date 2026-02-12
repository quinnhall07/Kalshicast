# collectors/collect_vcr.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from config import HEADERS, FORECAST_DAYS
from collectors.time_axis import (
    axis_start_end,
    build_hourly_axis_z,
    daily_targets_from_axis,
    hourly_axis_set,
    truncate_issued_at_to_hour_z,
)

VCR_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

"""
STRICT payload shape required by sources_registry + morning.py:

{
  "issued_at": "...Z",
  "daily": [
    {"target_date": "YYYY-MM-DD", "high_f": float, "low_f": float},
    ...
  ],
  "hourly": {
    "time": ["YYYY-MM-DDTHH:00:00Z", ...],        # ALWAYS axis length (FORECAST_DAYS*24)
    "temperature_f": [float|None, ...],
    "dewpoint_f": [float|None, ...],
    "humidity_pct": [float|None, ...],
    "wind_speed_mph": [float|None, ...],
    "wind_dir_deg": [float|None, ...],
    "cloud_cover_pct": [float|None, ...],
    "precip_prob_pct": [float|None, ...],
  }
}

Policy:
- Horizon is centralized: config.FORECAST_DAYS.
- Hourly axis is forward-looking UTC (shared collectors.time_axis).
- We request a wide-enough VCR date window, then filter/reindex onto the shared axis.
- Daily comes from VCR "days" (tempmax/tempmin). If a date is missing, fallback from hourly temps.
"""


def _get_key() -> str:
    key = os.getenv("VISUALCROSSING_KEY")
    if not key:
        raise RuntimeError("Missing VISUALCROSSING_KEY env var")
    return key


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        return v
    except Exception:
        return None


def _epoch_to_time_z_hour(epoch: Any) -> Optional[str]:
    """
    Visual Crossing hours include datetimeEpoch (seconds since epoch).
    Convert to UTC and truncate to the hour (":00:00Z").
    """
    if epoch is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(epoch), tz=timezone.utc)
        dt = dt.replace(minute=0, second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _backfill_daily_from_hourly_temps(
    target_dates: List[str],
    axis: List[str],
    temps: List[Optional[float]],
    daily_by_date: Dict[str, Dict[str, Optional[float]]],
) -> None:
    if not axis or not temps or len(axis) != len(temps):
        return

    per: Dict[str, List[float]] = {}
    for t, v in zip(axis, temps):
        if v is None:
            continue
        d = t[:10]
        if d in target_dates:
            per.setdefault(d, []).append(float(v))

    for d in target_dates:
        rec = daily_by_date.setdefault(d, {"high_f": None, "low_f": None})
        if rec.get("high_f") is not None and rec.get("low_f") is not None:
            continue
        vals = per.get(d) or []
        if not vals:
            continue
        if rec.get("high_f") is None:
            rec["high_f"] = max(vals)
        if rec.get("low_f") is None:
            rec["low_f"] = min(vals)


def fetch_vcr_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
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
    ndays = int(FORECAST_DAYS)
    if ndays < 1:
        ndays = 1
    if ndays > 14:
        ndays = 14

    # Shared forward-looking axis
    axis = build_hourly_axis_z(ndays)
    axis_s = hourly_axis_set(axis)
    start_utc, end_utc = axis_start_end(axis)
    target_dates = daily_targets_from_axis(axis)[:ndays]

    # Request a wide-enough date window to safely cover the UTC axis.
    # VCR date ranges are interpreted in the location timezone; requesting one extra day
    # avoids edge clipping when we later filter by UTC axis timestamps.
    start_date = start_utc.date().isoformat()
    end_date = (end_utc.date() + timedelta(days=1)).isoformat()

    q = {
        "unitGroup": "us",
        "key": key,
        "contentType": "json",
        "include": "days,hours" if include_hourly else "days",
    }

    url = f"{VCR_URL}/{float(lat)},{float(lon)}/{start_date}/{end_date}"
    r = requests.get(url, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    issued_at = truncate_issued_at_to_hour_z(datetime.now(timezone.utc))
    if not issued_at:
        # should never happen, but keep contract stable
        issued_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )

    days = data.get("days") or []

    # --- Daily from VCR days (primary) ---
    daily_by_date: Dict[str, Dict[str, Optional[float]]] = {d: {"high_f": None, "low_f": None} for d in target_dates}

    # --- Hourly maps keyed by axis timestamps ---
    temp_map: Dict[str, float] = {}
    dew_map: Dict[str, float] = {}
    hum_map: Dict[str, float] = {}
    wsp_map: Dict[str, float] = {}
    wdir_map: Dict[str, float] = {}
    cc_map: Dict[str, float] = {}
    pop_map: Dict[str, float] = {}

    if isinstance(days, list):
        for drec in days:
            if not isinstance(drec, dict):
                continue

            d = str(drec.get("datetime") or "")[:10]
            if d and d in daily_by_date:
                hi = _to_float(drec.get("tempmax"))
                lo = _to_float(drec.get("tempmin"))
                if hi is not None:
                    daily_by_date[d]["high_f"] = float(hi)
                if lo is not None:
                    daily_by_date[d]["low_f"] = float(lo)

            if not include_hourly:
                continue

            hours = drec.get("hours") or []
            if not isinstance(hours, list):
                continue

            for h in hours:
                if not isinstance(h, dict):
                    continue

                t = _epoch_to_time_z_hour(h.get("datetimeEpoch"))
                if t is None or t not in axis_s:
                    continue

                # Fill maps (last write wins)
                tv = _to_float(h.get("temp"))
                dv = _to_float(h.get("dew"))
                hv = _to_float(h.get("humidity"))
                wsv = _to_float(h.get("windspeed"))
                wdv = _to_float(h.get("winddir"))
                ccv = _to_float(h.get("cloudcover"))
                ppv = _to_float(h.get("precipprob"))

                if tv is not None:
                    temp_map[t] = float(tv)
                if dv is not None:
                    dew_map[t] = float(dv)
                if hv is not None:
                    hum_map[t] = float(hv)
                if wsv is not None:
                    wsp_map[t] = float(wsv)
                if wdv is not None:
                    wdir_map[t] = float(wdv)
                if ccv is not None:
                    cc_map[t] = float(ccv)
                if ppv is not None:
                    pop_map[t] = float(ppv)

    # --- Build hourly arrays exactly on axis ---
    hourly_out: Dict[str, List[Any]] = {
        "time": axis,
        "temperature_f": [temp_map.get(t) for t in axis],
        "dewpoint_f": [dew_map.get(t) for t in axis],
        "humidity_pct": [hum_map.get(t) for t in axis],
        "wind_speed_mph": [wsp_map.get(t) for t in axis],
        "wind_dir_deg": [wdir_map.get(t) for t in axis],
        "cloud_cover_pct": [cc_map.get(t) for t in axis],
        "precip_prob_pct": [pop_map.get(t) for t in axis],
    }

    # --- Daily fallback from hourly temperatures only if needed ---
    if any(
        (daily_by_date[d].get("high_f") is None or daily_by_date[d].get("low_f") is None) for d in target_dates
    ):
        _backfill_daily_from_hourly_temps(target_dates, axis, hourly_out["temperature_f"], daily_by_date)

    daily: List[Dict[str, Any]] = []
    for d in target_dates:
        rec = daily_by_date.get(d) or {}
        hi = rec.get("high_f")
        lo = rec.get("low_f")
        if hi is None or lo is None:
            continue
        daily.append({"target_date": d, "high_f": float(hi), "low_f": float(lo)})

    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily}
    if include_hourly:
        out["hourly"] = hourly_out

    return out
