# collect_ome_model.py
from __future__ import annotations

import requests
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Tuple

from config import HEADERS

ENDPOINTS = {
    "gfs": "https://api.open-meteo.com/v1/gfs",
    "ecmwf": "https://api.open-meteo.com/v1/ecmwf",
    "gem": "https://api.open-meteo.com/v1/gem",
    "icon": "https://api.open-meteo.com/v1/dwd-icon",
}


def _summarize_high_low_by_date(times: List[str], temps: List[float]) -> Dict[str, Tuple[float, float]]:
    by_day: Dict[str, List[float]] = {}
    for t, temp in zip(times, temps):
        d = str(t)[:10]
        by_day.setdefault(d, []).append(float(temp))

    out: Dict[str, Tuple[float, float]] = {}
    for d, vals in by_day.items():
        out[d] = (max(vals), min(vals))
    return out


def fetch_openmeteo_model_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo model fetch requires station['lat'] and station['lon'].")

    params = params or {}
    model = str(params.get("model", "")).lower().strip()
    if model not in ENDPOINTS:
        raise ValueError(f"Unknown Open-Meteo model '{model}'. Expected one of {sorted(ENDPOINTS)}.")

    url = ENDPOINTS[model]

    today = date.today()
    tomorrow = date.fromordinal(today.toordinal() + 1)

    q = {
        "latitude": float(lat),
        "longitude": float(lon),
        "hourly": "temperature_2m",
        "timezone": "UTC",
        "start_date": today.isoformat(),
        "end_date": tomorrow.isoformat(),
        "temperature_unit": "fahrenheit",
    }

    r = requests.get(url, params=q, headers=dict(HEADERS), timeout=25)
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(f"Open-Meteo {model} error: {data.get('reason') or data.get('message') or data}")

    hourly = data.get("hourly") or {}
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    if not times or not temps:
        raise RuntimeError(f"Open-Meteo {model} returned no hourly temperature_2m")

    hl = _summarize_high_low_by_date(times, temps)

    want = {today.isoformat(), tomorrow.isoformat()}
    rows: List[dict] = []
    for d in sorted(want):
        if d in hl:
            hi, lo = hl[d]
            rows.append({"target_date": d, "high": float(hi), "low": float(lo)})

    issued_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {"issued_at": issued_at, "rows": rows}
