# collect_ome.py
from __future__ import annotations

import requests
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from config import HEADERS

OME_URL = "https://api.open-meteo.com/v1/forecast"

# Daily variables we want (Open-Meteo names)
# Note: Some variables may be unavailable depending on endpoint/model; those will come back missing/None.
_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",

    # Extras
    "relative_humidity_2m_max",
    "wind_speed_10m_max",
    "wind_direction_10m_dominant",
    "cloud_cover_mean",
    "precipitation_probability_max",
]

def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def fetch_ome_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    today = date.today()
    tomorrow = date.fromordinal(today.toordinal() + 1)
    want = {today.isoformat(), tomorrow.isoformat()}

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),

        # Ask for highs/lows + extras
        "daily": ",".join(_DAILY_VARS),

        # Force consistent units
        "timezone": "UTC",
        "start_date": today.isoformat(),
        "end_date": tomorrow.isoformat(),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",

        # Some Open-Meteo deployments support this; harmless if ignored.
        # Keeps precipitation_probability in percent semantics.
        "precipitation_unit": "inch",
    }

    # Allow caller to override/extend params safely
    if params:
        q.update(params)

    r = requests.get(OME_URL, params=q, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(f"Open-Meteo error: {data.get('reason') or data.get('message') or data}")

    daily = data.get("daily") or {}
    dates = daily.get("time") or []

    if not isinstance(dates, list) or not dates:
        return {"issued_at": _utc_now_z(), "rows": []}

    # Required temps
    tmax = daily.get("temperature_2m_max") or []
    tmin = daily.get("temperature_2m_min") or []

    # Optional extras (may be missing)
    rh_max = daily.get("relative_humidity_2m_max") or []
    ws_max = daily.get("wind_speed_10m_max") or []
    wd_dom = daily.get("wind_direction_10m_dominant") or []
    cc_mean = daily.get("cloud_cover_mean") or []
    pp_max = daily.get("precipitation_probability_max") or []

    n = min(len(dates), len(tmax), len(tmin))
    rows: List[dict] = []

    def _at(arr: list, i: int):
        return arr[i] if i < len(arr) else None

    for i in range(n):
        d = str(dates[i])[:10]
        if d not in want:
            continue
        try:
            high = float(tmax[i])
            low = float(tmin[i])
        except Exception:
            continue

        extras: Dict[str, Any] = {}

        v = _at(rh_max, i)
        if v is not None:
            try:
                extras["humidity_pct"] = float(v)
            except Exception:
                pass

        v = _at(ws_max, i)
        if v is not None:
            try:
                extras["wind_speed_mph"] = float(v)
            except Exception:
                pass

        v = _at(wd_dom, i)
        if v is not None:
            try:
                extras["wind_dir_deg"] = float(v)
            except Exception:
                pass

        v = _at(cc_mean, i)
        if v is not None:
            try:
                extras["cloud_cover_pct"] = float(v)
            except Exception:
                pass

        v = _at(pp_max, i)
        if v is not None:
            try:
                extras["precip_prob_pct"] = float(v)
            except Exception:
                pass

        rows.append({
            "target_date": d,
            "high": high,
            "low": low,
            "extras": extras,  # <-- morning.py will merge nested extras and store them
        })

    return {"issued_at": _utc_now_z(), "rows": rows}
