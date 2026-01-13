from __future__ import annotations

import math
import time as time_mod
import requests
from datetime import datetime, date, time, timezone
from typing import Optional, List

from config import STATIONS, HEADERS
from db import upsert_observation, upsert_station


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
    """
    Fetch observations for a single station and calendar date (YYYY-MM-DD).
    Computes observed high/low from all reported temps in that window and upserts into DB.
    """
    station_id = station["station_id"]
    upsert_station(station_id, station.get("name"), station.get("lat"), station.get("lon"))

    target = date.fromisoformat(target_date)

    # This uses "target_date midnight -> 23:59" in *your local time converted to UTC*.
    # That's acceptable for now; if you later want strict station-local time, we can adjust.
    start_utc = datetime.combine(target, time(0, 0)).astimezone(timezone.utc).isoformat()
    end_utc = datetime.combine(target, time(23, 59)).astimezone(timezone.utc).isoformat()

    url = f"https://api.weather.gov/stations/{station_id}/observations"
    params = {"start": start_utc, "end": end_utc, "limit": 500}

    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"

    # Light retry for transient DNS/timeouts
    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=25)
            r.raise_for_status()
            data = r.json()
            feats = data.get("features", [])
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
            # brief backoff
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

