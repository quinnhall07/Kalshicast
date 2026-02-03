# collect_vcr.py
from __future__ import annotations

import os
import requests
from datetime import date, datetime, timezone
from typing import Any, Dict, List

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


def fetch_vcr_forecast(station: dict, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Visual Crossing Timeline API -> standardized output
    Returns:
      {"issued_at":"...Z","rows":[{"target_date":"YYYY-MM-DD","high":..,"low":..}, ...]}
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Visual Crossing fetch requires station['lat'] and station['lon'].")

    params = params or {}
    unit_group = params.get("unitGroup", "us")
    ndays = int(params.get("days", 2))

    today = date.today()
    end = date.fromordinal(today.toordinal() + (ndays - 1))

    location = f"{float(lat)},{float(lon)}"
    url = f"{BASE}{location}/{today.isoformat()}/{end.isoformat()}"

    q = {
        "key": _get_key(),
        "unitGroup": unit_group,
        "contentType": "json",
        "include": "days",
        "elements": "datetime,tempmax,tempmin,humidity,windspeed,winddir,cloudcover,precipprob",
    }

    r = requests.get(url, params=q, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    data = r.json()

    issued_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    days = data.get("days") or []
    if not days:
        raise RuntimeError(f"Visual Crossing returned no 'days' for {location}")

    rows: List[dict] = []
    for d in days:
        dt = (d.get("datetime") or "")[:10]
        hi = d.get("tempmax")
        lo = d.get("tempmin")
        if not dt or hi is None or lo is None:
            continue

        row: Dict[str, Any] = {"target_date": dt, "high": float(hi), "low": float(lo)}

        if d.get("humidity") is not None:
            row["humidity_pct"] = float(d["humidity"])
        if d.get("windspeed") is not None:
            row["wind_speed_mph"] = float(d["windspeed"])
        if d.get("winddir") is not None:
            row["wind_dir_deg"] = float(d["winddir"])
        if d.get("cloudcover") is not None:
            row["cloud_cover_pct"] = float(d["cloudcover"])
        if d.get("precipprob") is not None:
            row["precip_prob_pct"] = float(d["precipprob"])

        rows.append(row)

    return {"issued_at": issued_at, "rows": rows}
