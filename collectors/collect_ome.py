# collectors/collect_ome.py  (OME_BASE)
from __future__ import annotations

import os
import random
import threading
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter

from config import (
    HEADERS,
    FORECAST_DAYS,
    OME_TIMEOUT,
    OME_MAX_INFLIGHT,
    OME_MAX_ATTEMPTS,
    OME_BACKOFF_BASE_S,
)

OME_URL = "https://api.open-meteo.com/v1/forecast"

_DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
]

_HOURLY_VARS = [
    "temperature_2m",
    "dew_point_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "cloud_cover",
    "precipitation_probability",
]

# Shared per-process client resources
_OME_SEM = threading.BoundedSemaphore(value=int(OME_MAX_INFLIGHT))
_OME_SESSION = requests.Session()
_OME_SESSION.headers.update(dict(HEADERS))
_OME_SESSION.mount(
    "https://",
    HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0),
)


def _utc_now_trunc_hour_z() -> str:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _parse_timeout_from_env(default: Tuple[float, float]) -> Tuple[float, float]:
    env_t = (os.getenv("OME_TIMEOUT") or "").strip()
    if not env_t:
        return default
    try:
        if "," in env_t:
            a, b = env_t.split(",", 1)
            return (float(a.strip()), float(b.strip()))
        v = float(env_t)
        return (v, v)
    except Exception:
        return default


def _is_retryable_exc(e: Exception) -> bool:
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(e, requests.HTTPError):
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        return code == 429 or (isinstance(code, int) and code >= 500)
    return False


def _get_with_retries(url: str, *, params: Dict[str, Any]) -> dict:
    timeout = _parse_timeout_from_env(tuple(OME_TIMEOUT))

    last: Optional[Exception] = None

    for attempt in range(1, int(OME_MAX_ATTEMPTS) + 1):
        try:
            _OME_SEM.acquire()
            try:
                r = _OME_SESSION.get(url, params=params, timeout=timeout)
            finally:
                _OME_SEM.release()

            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(f"Open-Meteo error: {data.get('reason') or data.get('message') or data}")
            return data

        except Exception as e:
            last = e
            if attempt >= int(OME_MAX_ATTEMPTS) or not _is_retryable_exc(e):
                raise

            # jittered exponential backoff
            base = float(OME_BACKOFF_BASE_S) * (2 ** (attempt - 1))
            sleep_s = base * random.uniform(0.75, 1.25)
            time.sleep(sleep_s)

    raise last  # pragma: no cover


def fetch_ome_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    STRICT payload shape (consumed by morning.py):
      {
        "issued_at": "...Z",
        "daily": [ {"target_date":"YYYY-MM-DD","high_f":float,"low_f":float}, ... ],
        "hourly": { "time":[...], "<var>":[...], ... }   # Open-Meteo arrays (unaggregated)
      }
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    days = int(FORECAST_DAYS)
    days = max(1, min(14, days))  # safety clamp
    today = date.today()
    end_date = today + timedelta(days=days - 1)  # inclusive

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),

        # daily highs/lows
        "daily": ",".join(_DAILY_VARS),
        "start_date": today.isoformat(),
        "end_date": end_date.isoformat(),

        # hourly, unaggregated features
        "hourly": ",".join(_HOURLY_VARS),

        # consistent units
        "timezone": "UTC",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
    }

    if params:
        q.update(params)

    data = _get_with_retries(OME_URL, params=q)

    issued_at = _utc_now_trunc_hour_z()

    # ---- daily list ----
    daily = data.get("daily") or {}
    dates = daily.get("time") or []
    tmax = daily.get("temperature_2m_max") or []
    tmin = daily.get("temperature_2m_min") or []

    out_daily: List[dict] = []
    if isinstance(dates, list) and dates:
        n = min(len(dates), len(tmax), len(tmin))
        for i in range(n):
            td = str(dates[i])[:10]
            try:
                high_f = float(tmax[i])
                low_f = float(tmin[i])
            except Exception:
                continue
            out_daily.append({"target_date": td, "high_f": high_f, "low_f": low_f})

    # ---- hourly arrays ----
    hourly = data.get("hourly")
    out: Dict[str, Any] = {"issued_at": issued_at, "daily": out_daily}

    if isinstance(hourly, dict) and isinstance(hourly.get("time"), list) and hourly["time"]:
        out["hourly"] = hourly

    return out
