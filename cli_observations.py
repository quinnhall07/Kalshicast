# cli_observations.py
from __future__ import annotations

import math
import time as time_mod
import requests
from datetime import datetime, date, time
from typing import Optional, List

from zoneinfo import ZoneInfo

from config import STATIONS, HEADERS
from db import upsert_observation, upsert_location


def c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def _extract_temps_f(features: List[dict]) -> List[float]:
    temps: List[float] = []
    for feat in features:
        v = feat.get("properties", {}).get("temperature", {}).get("value")
        if v is None:
            continue
        try:
            f = c_to_f(float(v))
            if math.isfinite(f):
                temps.append(f)
        except (TypeError, ValueError):
            continue
    return temps


def fetch_observations_for_station(station: dict, target_date: str) -> bool:
    station_id = station["station_id"]

    upsert_location({
        "station_id": station_id,
        "name": station.get("name"),
        "lat": station.get("lat"),
        "lon": station.get("lon"),
        "timezone": station.get("timezone"),
        "state": station.get("state"),
        "elevation_ft": station.get("elevation_ft"),
        "is_active": station.get("is_active"),
    })

    target = date.fromisoformat(target_date)
    tz = ZoneInfo(station.get("timezone") or "UTC")

    # Station-local day window -> UTC
    start_local = datetime.combine(target, time(0, 0), tzinfo=tz)
    end_local = datetime.combine(target, time(23, 59), tzinfo=tz)
    start_utc = start_local.astimezone(ZoneInfo("UTC")).isoformat()
    end_utc = end_local.astimezone(ZoneInfo("UTC")).isoformat()

    url = f"https://api.weather.gov/stations/{station_id}/observations"
    params = {"start": start_utc, "end": end_utc, "limit": 500}

    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"

    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=25)
            r.raise_for_status()
            payload = r.json()
            feats = payload.get("features", [])
            temps_f = _extract_temps_f(feats)

            if not temps_f:
                print(f"[obs] FAIL {station_id} {target_date}: no temps")
                return False

            high = round(max(temps_f), 1)
            low = round(min(temps_f), 1)

            upsert_observation(station_id, target_date, high, low)
            print(f"[obs] OK {station_id} {target_date}: high={high} low={low}")
            return True

        except requests.RequestException as e:
            last_err = e
            time_mod.sleep(1.5 * (attempt + 1))

    print(f"[obs] FAIL {station_id} {target_date}: {last_err}")
    return False


def fetch_observations(target_date: str) -> bool:
    any_ok = False
    for st in STATIONS:
        try:
            ok = fetch_observations_for_station(st, target_date)
            any_ok = any_ok or ok
        except Exception as e:
            print(f"[obs] FAIL {st.get('station_id')} {target_date}: {e}")
    return any_ok



