# collect_wapi.py
from __future__ import annotations

import os
import requests
from datetime import date
from typing import List

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


def fetch_wapi_forecast(station: dict) -> List[dict]:
    """
    WeatherAPI.com forecast -> standardized output
    Input: station dict with lat/lon
    Output: list of dicts for today + tomorrow:
      [{"target_date":"YYYY-MM-DD","high":..,"low":..}, ...]
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("WeatherAPI fetch requires station['lat'] and station['lon'].")

    params = {
        "key": _get_key(),
        "q": f"{float(lat)},{float(lon)}",  # WeatherAPI supports lat,lon query
        "days": 2,                          # today + tomorrow
        "aqi": "no",
        "alerts": "no",
    }

    r = requests.get(WAPI_URL, params=params, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    data = r.json()

    fc = (data.get("forecast") or {}).get("forecastday") or []
    if not fc:
        raise RuntimeError(f"WeatherAPI returned no forecastday data for {lat},{lon}")

    results: List[dict] = []
    for day in fc:
        d = (day.get("date") or "")[:10]
        daypart = day.get("day") or {}
        hi = daypart.get("maxtemp_f")
        lo = daypart.get("mintemp_f")
        if not d or hi is None or lo is None:
            continue
        results.append({"target_date": d, "high": float(hi), "low": float(lo)})

    # Keep only today + tomorrow if extra shows up
    today = date.today().isoformat()
    tomorrow = date.fromordinal(date.today().toordinal() + 1).isoformat()
    want = {today, tomorrow}
    results = [x for x in results if x["target_date"] in want]

    return results
