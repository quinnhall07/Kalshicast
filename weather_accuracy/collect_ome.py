# collect_ome.py
from __future__ import annotations

import requests
from datetime import date
from typing import Any, Dict, List

from config import HEADERS

OME_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_ome_forecast(station: dict, params: Dict[str, Any] | None = None) -> List[dict]:
    """
    Open-Meteo "forecast" endpoint (auto/blended models) -> standardized output.

    Note:
      - This endpoint is best treated as a single source (OME_BASE).
      - Per-model runs (GFS/ECMWF/GEM/ICON) should use model-specific endpoints
        via collect_omodel.py, not params on /v1/forecast.
    """
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
        "daily": "temperature_2m_max,temperature_2m_min",
        "timezone": "UTC",
        "start_date": today.isoformat(),
        "end_date": tomorrow.isoformat(),
        "temperature_unit": "fahrenheit",
    }

    r = requests.get(OME_URL, params=q, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(f"Open-Meteo error: {data.get('reason') or data.get('message') or data}")

    daily = data.get("daily") or {}
    dates = daily.get("time") or []
    tmax = daily.get("temperature_2m_max") or []
    tmin = daily.get("temperature_2m_min") or []

    if not dates:
        return []

    # Guard against weird partial payloads
    n = min(len(dates), len(tmax), len(tmin))
    if n == 0:
        return []

    out: List[dict] = []
    for i in range(n):
        d = str(dates[i])[:10]
        if d not in want:
            continue
        try:
            out.append({"target_date": d, "high": float(tmax[i]), "low": float(tmin[i])})
        except Exception:
            continue

    return out
