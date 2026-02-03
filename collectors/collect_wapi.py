# collect_wapi.py
from __future__ import annotations

import os
import requests
from datetime import date, datetime, timezone
from typing import List, Dict, Any

from config import HEADERS

WAPI_URL = "https://api.weatherapi.com/v1/forecast.json"


def _get_key() -> str:
    key = os.getenv("WEATHERAPI_KEY")
    if not key:
        raise RuntimeError(
            "Missing WEATHERAPI_KEY env var. Set it before running.\n"
            "PowerShell: $env:WEATHERAPI_KEY='...'\n"
            "macOS/Linux: export WEATHERAPI_KEY='...'"
        )
    return key


def fetch_wapi_forecast(station: dict) -> Dict[str, Any]:
    """
    WeatherAPI.com forecast -> standardized output
    Returns:
      {"issued_at":"...Z","rows":[{"target_date":"YYYY-MM-DD","high":..,"low":..}, ...]}
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("WeatherAPI fetch requires station['lat'] and station['lon'].")

    params = {
        "key": _get_key(),
        "q": f"{float(lat)},{float(lon)}",
        "days": 2,
        "aqi": "no",
        "alerts": "no",
    }

    r = requests.get(WAPI_URL, params=params, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    data = r.json()

    # Prefer provider's timestamp if present; else now()
    issued_at = (
        (data.get("current") or {}).get("last_updated_epoch")
        or (data.get("location") or {}).get("localtime_epoch")
    )
    if issued_at:
        issued_at = datetime.fromtimestamp(int(issued_at), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    else:
        issued_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    fc = (data.get("forecast") or {}).get("forecastday") or []
    if not fc:
        raise RuntimeError(f"WeatherAPI returned no forecastday data for {lat},{lon}")

    rows: List[dict] = []
    for day in fc:
        d = (day.get("date") or "")[:10]
        daypart = day.get("day") or {}
        hi = daypart.get("maxtemp_f")
        lo = daypart.get("mintemp_f")
        if not d or hi is None or lo is None:
            continue

        # Optional Tier-1 extras if present
        extras: Dict[str, Any] = {}
        avgh = daypart.get("avghumidity")
        if avgh is not None:
            extras["humidity_pct"] = float(avgh)

        precip = daypart.get("daily_chance_of_rain")
        if precip is not None:
            extras["precip_prob_pct"] = float(precip)

        rows.append({"target_date": d, "high": float(hi), "low": float(lo), **extras})

    today = date.today().isoformat()
    tomorrow = date.fromordinal(date.today().toordinal() + 1).isoformat()
    want = {today, tomorrow}
    rows = [x for x in rows if x["target_date"] in want]

    return {"issued_at": issued_at, "rows": rows}
