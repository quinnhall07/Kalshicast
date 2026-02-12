# collectors/collect_ome_model.py
from __future__ import annotations

import math
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

"""
Collector contract REQUIRED by morning.py:

{
  "issued_at": "2026-02-01T06:00:00Z",
  "daily": [ {"target_date":"YYYY-MM-DD","high_f":float,"low_f":float}, ... ],
  "hourly": { "time":[...], optional variable arrays ... }  # Open-Meteo style arrays
}

Permanent decision for Open-Meteo: issued_at = fetch time truncated to the hour (UTC).
"""

_DAILY = "temperature_2m_max,temperature_2m_min"
_HOURLY = ",".join(
    [
        "temperature_2m",
        "dew_point_2m",
        "relative_humidity_2m",
        "wind_speed_10m",
        "wind_direction_10m",
        "cloud_cover",
        "precipitation_probability",
    ]
)

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


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None


def _ensure_time_z(ts: Any) -> Optional[str]:
    """
    Open-Meteo hourly time is typically "YYYY-MM-DDTHH:MM" in UTC (no tz).
    Convert to "YYYY-MM-DDTHH:MM:00Z" so Postgres ::timestamptz is unambiguous.
    """
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s[:-1] + "+00:00")
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        if len(s) >= 16 and s[10] == "T":
            return s[:16] + ":00Z"
        return None


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

            base = float(OME_BACKOFF_BASE_S) * (2 ** (attempt - 1))
            sleep_s = base * random.uniform(0.75, 1.25)
            time.sleep(sleep_s)

    raise last  # pragma: no cover


def fetch_ome_model_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Open-Meteo multi-model collector (non-base models).

    params should include model selection, e.g.:
      {"model": "gfs"} / {"model": "ecmwf"} / {"model": "icon"} / {"model": "gem"}

    Horizon is centralized via config.FORECAST_DAYS.
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    p: Dict[str, Any] = dict(params or {})

    days = int(FORECAST_DAYS)
    days = max(1, min(14, days))
    start = date.today()
    end = start + timedelta(days=days - 1)

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),
        "timezone": "UTC",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "daily": _DAILY,
        "hourly": _HOURLY,
    }
    q.update(p)

    data = _get_with_retries(OME_URL, params=q)

    issued_at = _utc_now_trunc_hour_z()

    # -------- Daily --------
    daily_rows: List[Dict[str, Any]] = []
    daily = data.get("daily") or {}
    d_time = daily.get("time") or []
    d_hi = daily.get("temperature_2m_max") or []
    d_lo = daily.get("temperature_2m_min") or []

    if isinstance(d_time, list) and isinstance(d_hi, list) and isinstance(d_lo, list):
        n = min(len(d_time), len(d_hi), len(d_lo))
        for i in range(n):
            td = str(d_time[i])[:10]
            hi = _to_float(d_hi[i])
            lo = _to_float(d_lo[i])
            if hi is None or lo is None:
                continue
            daily_rows.append({"target_date": td, "high_f": float(hi), "low_f": float(lo)})

    # If daily missing, derive from hourly temperature_2m
    if not daily_rows:
        hourly0 = data.get("hourly") or {}
        h_time = hourly0.get("time") or []
        h_temp = hourly0.get("temperature_2m") or []
        if isinstance(h_time, list) and isinstance(h_temp, list) and h_time and h_temp:
            by_day: Dict[str, List[float]] = {}
            for t, v in zip(h_time, h_temp):
                td = str(t)[:10]
                fv = _to_float(v)
                if fv is None:
                    continue
                by_day.setdefault(td, []).append(float(fv))
            for td, vals in sorted(by_day.items()):
                if vals:
                    daily_rows.append({"target_date": td, "high_f": max(vals), "low_f": min(vals)})

    # -------- Hourly (arrays object) --------
    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily_rows}

    hourly = data.get("hourly")
    if isinstance(hourly, dict):
        times = hourly.get("time")
        if isinstance(times, list) and times:
            tz_times: List[str] = []
            for t in times:
                tz = _ensure_time_z(t)
                if tz is None:
                    tz_times = []
                    break
                tz_times.append(tz)

            if tz_times:
                hourly_norm: Dict[str, Any] = dict(hourly)
                hourly_norm["time"] = tz_times
                out["hourly"] = hourly_norm

    return out
