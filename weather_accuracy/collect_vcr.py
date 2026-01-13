# collect_vcr.py
from __future__ import annotations

import os
import requests
from datetime import date
from typing import Any, Dict, List, Optional

from config import HEADERS

BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"


def _get_key() -> str:
    key = os.getenv("VISUALCROSSING_KEY")
    if not key:
        raise RuntimeError(
            "Missing VISUALCROSSING_KEY env var.\n"
            "PowerShell: $env:VISUALCROSSING_KEY='...'\n"
            "macOS/Linux: export VISUALCROSSING_KEY='...'"
        )
    return key


def fetch_vcr_forecast(station: dict, params: Dict[str, Any] | None = None) -> List[dict]:
    """
    Visual Crossing Timeline API -> standardized output
    Input: station dict with lat/lon
    Output: list of dicts for today + tomorrow:
      [{"target_date":"YYYY-MM-DD","high":..,"low":..}, ...]
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Visual Crossing fetch requires station['lat'] and station['lon'].")

    params = params or {}
    unit_group = params.get("unitGroup", "us")  # "us" => Fahrenheit
    ndays = int(params.get("days", 2))

    # Ask for a short date range: today -> tomorrow
    today = date.today()
    end = date.fromordinal(today.toordinal() + (ndays - 1))

    location = f"{float(lat)},{float(lon)}"
    url = f"{BASE}{location}/{today.isoformat()}/{end.isoformat()}"

    q = {
        "key": _get_key(),
        "unitGroup": unit_group,      # "us" gives temp in F
        "contentType": "json",
        "include": "days",
        # keep payload small and stable:
        "elements": "datetime,tempmax,tempmin",
    }

    r = requests.get(url, params=q, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    data = r.json()

    days = data.get("days") or []
    if not days:
        raise RuntimeError(f"Visual Crossing returned no 'days' for {location}")

    out: List[dict] = []
    for d in days:
        dt = (d.get("datetime") or "")[:10]
        hi = d.get("tempmax")
        lo = d.get("tempmin")
        if not dt or hi is None or lo is None:
            continue
        out.append({"target_date": dt, "high": float(hi), "low": float(lo)})

    return out
