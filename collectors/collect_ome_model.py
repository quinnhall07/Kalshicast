# collectors/collect_ome_model.py
from __future__ import annotations

import math
import os
import random
import threading
import time
from datetime import datetime, timezone
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
from collectors.time_axis import (
    axis_start_end,
    build_hourly_axis_z,
    daily_targets_from_axis,
    hourly_axis_set,
    truncate_issued_at_to_hour_z,
)

OME_URL = "https://api.open-meteo.com/v1/forecast"

_DAILY = "temperature_2m_max,temperature_2m_min"
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


def _ensure_time_hour_z(ts: Any) -> Optional[str]:
    """
    Open-Meteo hourly time is typically "YYYY-MM-DDTHH:MM" (timezone=UTC).
    Normalize to hour-truncated "YYYY-MM-DDTHH:00:00Z".
    """
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        if len(s) >= 13 and s[10] == "T":
            return s[:13] + ":00:00Z"
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


def _reindex_axis(axis: List[str], m: Dict[str, float]) -> List[Optional[float]]:
    return [float(m[t]) if t in m else None for t in axis]


def _backfill_daily_from_hourly_temps(
    target_dates: List[str],
    axis: List[str],
    temps: List[Optional[float]],
    by_date: Dict[str, Dict[str, Optional[float]]],
) -> None:
    if not axis or not temps or len(axis) != len(temps):
        return

    per: Dict[str, List[float]] = {}
    for t, v in zip(axis, temps):
        if v is None:
            continue
        d = t[:10]
        if d in target_dates:
            per.setdefault(d, []).append(float(v))

    for d in target_dates:
        rec = by_date.setdefault(d, {"high_f": None, "low_f": None})
        vals = per.get(d) or []
        if not vals:
            continue
        if rec.get("high_f") is None:
            rec["high_f"] = max(vals)
        if rec.get("low_f") is None:
            rec["low_f"] = min(vals)


def fetch_ome_model_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Open-Meteo multi-model collector (non-base models).

    params should include model selection, e.g.:
      {"models": "gfs"} / {"models": "ecmwf"} / {"models": "icon"} / {"models": "gem"}

    Horizon is centralized via config.FORECAST_DAYS.
    Uses collectors.time_axis to enforce a forward-looking UTC axis.
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("Open-Meteo fetch requires station['lat'] and station['lon'].")

    p: Dict[str, Any] = dict(params or {})

    days = int(FORECAST_DAYS)
    days = max(1, min(14, days))

    axis = build_hourly_axis_z(days)
    axis_s = hourly_axis_set(axis)
    start_utc, end_utc = axis_start_end(axis)
    target_dates = daily_targets_from_axis(axis)[:days]

    start_date = start_utc.date().isoformat()
    end_date = end_utc.date().isoformat()

    q: Dict[str, Any] = {
        "latitude": float(lat),
        "longitude": float(lon),
        "timezone": "UTC",
        "start_date": start_date,
        "end_date": end_date,
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "daily": _DAILY,
        "hourly": ",".join(_HOURLY_VARS),
    }
    q.update(p)

    data = _get_with_retries(OME_URL, params=q)

    issued_at = truncate_issued_at_to_hour_z(datetime.now(timezone.utc))
    if not issued_at:
        issued_at = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0).isoformat().replace(
            "+00:00", "Z"
        )

    # ---- Hourly (maps -> axis arrays) ----
    hourly0 = data.get("hourly") or {}
    h_time = hourly0.get("time") or []
    if not isinstance(h_time, list):
        h_time = []

    provider_times: List[str] = []
    for t in h_time:
        tz = _ensure_time_hour_z(t)
        if tz is None:
            provider_times = []
            break
        provider_times.append(tz)

    var_maps: Dict[str, Dict[str, float]] = {}
    if provider_times:
        for var in _HOURLY_VARS:
            arr = hourly0.get(var)
            if not isinstance(arr, list):
                continue
            m: Dict[str, float] = {}
            for t, v in zip(provider_times, arr):
                if t not in axis_s:
                    continue
                fv = _to_float(v)
                if fv is None:
                    continue
                m[t] = float(fv)
            var_maps[var] = m

    hourly_out: Dict[str, Any] = {"time": axis}
    for var in _HOURLY_VARS:
        hourly_out[var] = _reindex_axis(axis, var_maps.get(var) or {})

    # ---- Daily (API first; align to target_dates; fallback if missing) ----
    daily = data.get("daily") or {}
    d_time = daily.get("time") or []
    d_hi = daily.get("temperature_2m_max") or []
    d_lo = daily.get("temperature_2m_min") or []

    by_date: Dict[str, Dict[str, Optional[float]]] = {d: {"high_f": None, "low_f": None} for d in target_dates}

    if isinstance(d_time, list) and isinstance(d_hi, list) and isinstance(d_lo, list):
        n = min(len(d_time), len(d_hi), len(d_lo))
        for i in range(n):
            td = str(d_time[i])[:10]
            if td not in by_date:
                continue
            hi = _to_float(d_hi[i])
            lo = _to_float(d_lo[i])
            if hi is None or lo is None:
                continue
            by_date[td]["high_f"] = float(hi)
            by_date[td]["low_f"] = float(lo)

    if any((by_date[d]["high_f"] is None or by_date[d]["low_f"] is None) for d in target_dates):
        _backfill_daily_from_hourly_temps(target_dates, axis, hourly_out["temperature_2m"], by_date)

    daily_rows: List[Dict[str, Any]] = []
    for td in target_dates:
        rec = by_date.get(td) or {}
        hi = rec.get("high_f")
        lo = rec.get("low_f")
        if hi is None or lo is None:
            continue
        daily_rows.append({"target_date": td, "high_f": float(hi), "low_f": float(lo)})

    return {"issued_at": issued_at, "daily": daily_rows, "hourly": hourly_out}
