# collectors/collect_wapi.py
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from config import HEADERS, FORECAST_DAYS
from collectors.time_axis import (
    build_hourly_axis_z,
    daily_targets_from_axis,
    hourly_axis_set,
    truncate_issued_at_to_hour_z,
)

WAPI_URL = "https://api.weatherapi.com/v1/forecast.json"

"""
STRICT payload shape required by sources_registry + morning.py:

{
  "issued_at": "...Z",
  "daily": [
    {"target_date": "YYYY-MM-DD", "high_f": float, "low_f": float},
    ...
  ],
  "hourly": {
    "time": ["YYYY-MM-DDTHH:00:00Z", ...],      # ALWAYS axis length (FORECAST_DAYS*24)
    "temperature_f": [float|None, ...],
    "dewpoint_f": [float|None, ...],
    "humidity_pct": [float|None, ...],
    "wind_speed_mph": [float|None, ...],
    "wind_dir_deg": [float|None, ...],
    "cloud_cover_pct": [float|None, ...],
    "precip_prob_pct": [float|None, ...],
  }
}

Uses collectors.time_axis to enforce a shared forward-looking UTC axis.
WeatherAPI may return fewer forecast days depending on plan; we still output a full axis,
with missing points as None.
"""


def _get_key() -> str:
    key = os.getenv("WEATHERAPI_KEY")
    if not key:
        raise RuntimeError("Missing WEATHERAPI_KEY env var")
    return key


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _epoch_to_time_hour_z(epoch: Any) -> Optional[str]:
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


def fetch_wapi_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    WeatherAPI collector -> STRICT payload, axis-aligned.

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

    # Shared axis
    ndays = max(1, int(FORECAST_DAYS))
    axis = build_hourly_axis_z(ndays)
    axis_s = hourly_axis_set(axis)
    target_dates = daily_targets_from_axis(axis)[:ndays]

    # WeatherAPI wants an integer day count starting today; clamp to plan/API limits.
    # If the plan returns fewer days, we keep axis and fill missing with None.
    req_days = max(1, min(10, ndays))

    key = _get_key()
    q = {
        "key": key,
        "q": f"{float(lat)},{float(lon)}",
        "days": req_days,
        "aqi": "no",
        "alerts": "no",
    }

    r = requests.get(WAPI_URL, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    issued_at = truncate_issued_at_to_hour_z(datetime.now(timezone.utc))
    if not issued_at:
        issued_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )

    # ---- Prepare output containers (axis-aligned) ----
    hourly_out: Dict[str, List[Any]] = {
        "time": axis,
        "temperature_f": [None] * len(axis),
        "dewpoint_f": [None] * len(axis),
        "humidity_pct": [None] * len(axis),
        "wind_speed_mph": [None] * len(axis),
        "wind_dir_deg": [None] * len(axis),
        "cloud_cover_pct": [None] * len(axis),
        "precip_prob_pct": [None] * len(axis),
    }

    daily_by_date: Dict[str, Dict[str, Optional[float]]] = {d: {"high_f": None, "low_f": None} for d in target_dates}

    forecast_days = (data.get("forecast") or {}).get("forecastday") or []
    if not isinstance(forecast_days, list):
        forecast_days = []

    # ---- Fill from provider ----
    for day in forecast_days:
        if not isinstance(day, dict):
            continue

        d = str(day.get("date") or "")[:10]
        if d and d in daily_by_date:
            daydata = day.get("day") or {}
            if isinstance(daydata, dict):
                hi = _to_float(daydata.get("maxtemp_f"))
                lo = _to_float(daydata.get("mintemp_f"))
                if hi is not None:
                    daily_by_date[d]["high_f"] = float(hi)
                if lo is not None:
                    daily_by_date[d]["low_f"] = float(lo)

        if not include_hourly:
            continue

        hours = day.get("hour") or []
        if not isinstance(hours, list):
            continue

        for h in hours:
            if not isinstance(h, dict):
                continue

            t = _epoch_to_time_hour_z(h.get("time_epoch"))
            if t is None or t not in axis_s:
                continue

            # index into axis (fast enough at 96, but keep predictable)
            # build a map once if needed
            # (we avoid extra helper to keep file self-contained)
            # We'll compute position with a dict.
    # build index map once
    idx_map = {t: i for i, t in enumerate(axis)}

    if include_hourly:
        for day in forecast_days:
            if not isinstance(day, dict):
                continue
            hours = day.get("hour") or []
            if not isinstance(hours, list):
                continue
            for h in hours:
                if not isinstance(h, dict):
                    continue
                t = _epoch_to_time_hour_z(h.get("time_epoch"))
                if t is None:
                    continue
                i = idx_map.get(t)
                if i is None:
                    continue

                hourly_out["temperature_f"][i] = _to_float(h.get("temp_f"))
                hourly_out["dewpoint_f"][i] = _to_float(h.get("dewpoint_f"))
                hourly_out["humidity_pct"][i] = _to_float(h.get("humidity"))
                hourly_out["wind_speed_mph"][i] = _to_float(h.get("wind_mph"))
                hourly_out["wind_dir_deg"][i] = _to_float(h.get("wind_degree"))
                hourly_out["cloud_cover_pct"][i] = _to_float(h.get("cloud"))
                # chance_of_rain is a % (string/int) for rain; keep as float
                hourly_out["precip_prob_pct"][i] = _to_float(h.get("chance_of_rain"))

    # ---- Daily fallback from hourly temps if missing ----
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
