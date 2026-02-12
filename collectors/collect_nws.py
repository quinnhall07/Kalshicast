# collectors/collect_nws.py
from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from config import HEADERS


"""
Collector contract REQUIRED by morning.py:

{
  "issued_at": "2026-02-01T06:00:00Z",
  "daily": [ {"target_date":"YYYY-MM-DD","high_f":float,"low_f":float}, ... ],
  "hourly": { "time":[...], optional variable arrays ... }  # Open-Meteo style arrays
}

Notes:
- "hourly" is optional, but if present must be arrays-object with "time".
- Times should be UTC and parseable as timestamptz. We emit "...Z".
- issued_at uses provider timestamp when available; otherwise fallback handled by caller/registry.
"""


# -------------------------
# Time / parsing helpers
# -------------------------


def _utc_now_trunc_hour_z() -> str:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _ensure_z_minute(ts: str) -> Optional[str]:
    """
    Normalize timestamps to a Postgres-friendly UTC timestamptz string.
    Accepts:
      - "YYYY-MM-DDTHH:MM"
      - "YYYY-MM-DDTHH:MMZ"
      - "YYYY-MM-DDTHH:MM:SSZ"
      - "...+00:00"
    Returns:
      - "YYYY-MM-DDTHH:MM:00Z"
    """
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()

    # If already has offset like +00:00 or -05:00, convert to Z via fromisoformat
    try:
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
        else:
            s2 = s
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        dt = dt.replace(second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        pass

    # If it's "YYYY-MM-DDTHH:MM" (no tz), assume UTC
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc).replace(second=0, microsecond=0)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


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


def _c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def _uom_to_mph(uom: str, v: float) -> float:
    u = (uom or "").lower()
    if "km_h" in u or "km/h" in u:
        return v * 0.621371
    if "m_s" in u or "m/s" in u:
        return v * 2.236936
    if "kn" in u or "knot" in u:
        return v * 1.150779
    return v


def _uom_to_f(uom: str, v: float) -> float:
    u = (uom or "").lower()
    if "degc" in u or "celsius" in u:
        return _c_to_f(v)
    if "degf" in u or "fahrenheit" in u:
        return v
    return _c_to_f(v)


def _parse_iso(dt_str: str) -> Optional[datetime]:
    if not isinstance(dt_str, str) or not dt_str:
        return None
    try:
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# -------------------------
# NWS endpoints
# -------------------------


def _extract_points_urls(lat: float, lon: float) -> Tuple[str, str, str]:
    """
    Returns (forecast_url, grid_url, hourly_url)
    from api.weather.gov/points/{lat},{lon}
    """
    url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    r = requests.get(url, headers=dict(HEADERS), timeout=20)
    r.raise_for_status()
    payload = r.json()
    props = (payload or {}).get("properties") or {}
    fc = props.get("forecast")
    grid = props.get("forecastGridData")
    hourly = props.get("forecastHourly")
    if not (isinstance(fc, str) and isinstance(grid, str) and isinstance(hourly, str)):
        raise ValueError("NWS points lookup missing forecast URLs")
    return fc, grid, hourly


# -------------------------
# Daily (high/low) from /forecast
# -------------------------


def _extract_daily_high_low(payload: dict, *, days_ahead: int) -> List[Dict[str, Any]]:
    """
    Uses /forecast endpoint periods and pairs Day/Night to get high/low per date.
    Keeps today..today+days_ahead inclusive.
    """
    props = (payload or {}).get("properties") or {}
    periods = props.get("periods") or []
    if not isinstance(periods, list):
        return []

    start = date.today()
    end = start + timedelta(days=days_ahead)

    by_date: Dict[str, Dict[str, Optional[float]]] = {}

    for p in periods:
        if not isinstance(p, dict):
            continue
        st = _parse_iso(p.get("startTime", ""))
        if not st:
            continue
        d = _to_utc(st).date()
        if d < start or d > end:
            continue

        is_day = bool(p.get("isDaytime", False))
        temp = _to_float(p.get("temperature"))
        if temp is None:
            continue

        key = d.isoformat()
        rec = by_date.setdefault(key, {"high_f": None, "low_f": None})
        if is_day:
            rec["high_f"] = temp if rec["high_f"] is None else max(rec["high_f"], temp)
        else:
            rec["low_f"] = temp if rec["low_f"] is None else min(rec["low_f"], temp)

    out: List[Dict[str, Any]] = []
    for i in range((end - start).days + 1):
        td = (start + timedelta(days=i)).isoformat()
        rec = by_date.get(td) or {}
        hi = rec.get("high_f")
        lo = rec.get("low_f")
        if hi is None or lo is None:
            continue
        out.append({"target_date": td, "high_f": float(hi), "low_f": float(lo)})

    return out


# -------------------------
# Hourly arrays from /forecastGridData
# -------------------------


_DURATION_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", re.IGNORECASE)


def _parse_duration_hours(dur: str) -> int:
    """
    Parse common ISO8601 durations like PT1H, PT2H, PT30M, PT1H30M.
    Expand into whole hours (>=1).
    """
    if not isinstance(dur, str) or not dur:
        return 1
    m = _DURATION_RE.match(dur.strip())
    if not m:
        return 1
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    total = h * 3600 + mi * 60 + s
    if total <= 0:
        return 1
    return max(1, int(round(total / 3600.0)))


def _expand_grid_values(
    values: Iterable[dict],
    *,
    uom: str,
    kind: str,
    start_utc: datetime,
    end_utc: datetime,
) -> Dict[str, float]:
    """
    Expand NWS grid data values into hourly UTC timestamps.

    Returns map:
      "YYYY-MM-DDTHH:MM:00Z" -> value (converted to desired units)

    Expansion repeats value for each hour in validTime duration.
    """
    out: Dict[str, float] = {}

    for it in values:
        if not isinstance(it, dict):
            continue
        vt = it.get("validTime")
        val = _to_float(it.get("value"))
        if not isinstance(vt, str) or val is None:
            continue

        try:
            start_s, dur_s = vt.split("/", 1)
        except ValueError:
            continue

        dt0 = _parse_iso(start_s)
        if not dt0:
            continue
        dt0 = _to_utc(dt0)

        hours = _parse_duration_hours(dur_s)

        if kind in ("temperature_f", "dewpoint_f"):
            val_conv = _uom_to_f(uom, float(val))
        elif kind == "wind_speed_mph":
            val_conv = _uom_to_mph(uom, float(val))
        else:
            val_conv = float(val)

        for h in range(hours):
            t = dt0 + timedelta(hours=h)
            if t < start_utc or t > end_utc:
                continue
            t = t.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            key = t.isoformat().replace("+00:00", "Z")
            out[key] = val_conv

    return out


def _extract_hourly_arrays_from_grid(payload: dict, *, days_ahead: int) -> Dict[str, Any]:
    """
    Build Open-Meteo style hourly arrays:

      {
        "time": [...],
        "temperature_f": [...],
        "dewpoint_f": [...],
        ...
      }

    Arrays are aligned by index to "time". Missing values are None.
    """
    props = (payload or {}).get("properties") or {}

    start_day = date.today()
    end_day = start_day + timedelta(days=days_ahead)

    start_utc = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end_utc = (
        datetime.combine(end_day, datetime.max.time(), tzinfo=timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
    )

    def _series(name: str) -> Tuple[str, List[dict]]:
        obj = props.get(name) or {}
        uom = str(obj.get("uom") or "")
        vals = obj.get("values") or []
        return uom, vals if isinstance(vals, list) else []

    temp_uom, temp_vals = _series("temperature")
    dew_uom, dew_vals = _series("dewpoint")
    rh_uom, rh_vals = _series("relativeHumidity")
    ws_uom, ws_vals = _series("windSpeed")
    wd_uom, wd_vals = _series("windDirection")
    sc_uom, sc_vals = _series("skyCover")
    pop_uom, pop_vals = _series("probabilityOfPrecipitation")

    temp_map = _expand_grid_values(temp_vals, uom=temp_uom, kind="temperature_f", start_utc=start_utc, end_utc=end_utc)
    dew_map = _expand_grid_values(dew_vals, uom=dew_uom, kind="dewpoint_f", start_utc=start_utc, end_utc=end_utc)
    rh_map = _expand_grid_values(rh_vals, uom=rh_uom, kind="humidity_pct", start_utc=start_utc, end_utc=end_utc)
    ws_map = _expand_grid_values(ws_vals, uom=ws_uom, kind="wind_speed_mph", start_utc=start_utc, end_utc=end_utc)
    wd_map = _expand_grid_values(wd_vals, uom=wd_uom, kind="wind_dir_deg", start_utc=start_utc, end_utc=end_utc)
    sc_map = _expand_grid_values(sc_vals, uom=sc_uom, kind="cloud_cover_pct", start_utc=start_utc, end_utc=end_utc)
    pop_map = _expand_grid_values(pop_vals, uom=pop_uom, kind="precip_prob_pct", start_utc=start_utc, end_utc=end_utc)

    keys = set()
    keys |= set(temp_map.keys())
    keys |= set(dew_map.keys())
    keys |= set(rh_map.keys())
    keys |= set(ws_map.keys())
    keys |= set(wd_map.keys())
    keys |= set(sc_map.keys())
    keys |= set(pop_map.keys())

    if not keys:
        return {}

    times = sorted(keys)

    def _arr(m: Dict[str, float]) -> List[Optional[float]]:
        out: List[Optional[float]] = []
        for t in times:
            v = m.get(t)
            out.append(float(v) if v is not None else None)
        return out

    return {
        "time": times,
        "temperature_f": _arr(temp_map),
        "dewpoint_f": _arr(dew_map),
        "humidity_pct": _arr(rh_map),
        "wind_speed_mph": _arr(ws_map),
        "wind_dir_deg": _arr(wd_map),
        "cloud_cover_pct": _arr(sc_map),
        "precip_prob_pct": _arr(pop_map),
    }


# -------------------------
# Public fetcher
# -------------------------


def fetch_nws_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    NWS collector:
      - daily highs/lows from /forecast
      - hourly features from /forecastGridData (expanded to hourly)

    params:
      - days_ahead: int (default 3; clamped 1..7)
    """
    lat = station.get("lat")
    lon = station.get("lon")
    if lat is None or lon is None:
        raise ValueError("NWS fetch requires station['lat'] and station['lon'].")

    days = 3
    if isinstance(params, dict) and params.get("days_ahead") is not None:
        try:
            days = int(params["days_ahead"])
        except Exception:
            pass
    days = max(1, min(7, days))

    forecast_url, grid_url, hourly_url = _extract_points_urls(float(lat), float(lon))

    r_fc = requests.get(forecast_url, headers=dict(HEADERS), timeout=25)
    r_fc.raise_for_status()
    payload_fc = r_fc.json()

    props_fc = (payload_fc.get("properties") or {})
    issued_at_raw = props_fc.get("generatedAt") or props_fc.get("updated")
    issued_at = _ensure_z_minute(str(issued_at_raw)) if issued_at_raw else None
    if not issued_at:
        issued_at = _utc_now_trunc_hour_z()

    daily = _extract_daily_high_low(payload_fc, days_ahead=days)

    r_grid = requests.get(grid_url, headers=dict(HEADERS), timeout=25)
    r_grid.raise_for_status()
    payload_grid = r_grid.json()

    # Prefer grid "generatedAt" if present
    grid_gen = (payload_grid.get("properties") or {}).get("generatedAt")
    issued2 = _ensure_z_minute(str(grid_gen)) if grid_gen else None
    if issued2:
        issued_at = issued2

    hourly = _extract_hourly_arrays_from_grid(payload_grid, days_ahead=days)

        # Build the same UTC window used by grid expansion
    start_day = date.today()
    end_day = start_day + timedelta(days=days)
    start_utc = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end_utc = (
        datetime.combine(end_day, datetime.max.time(), tzinfo=timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
    )

    # Pull /forecast/hourly and merge PoP into hourly arrays
    r_h = requests.get(hourly_url, headers=dict(HEADERS), timeout=25)
    r_h.raise_for_status()
    payload_h = r_h.json()

    pop_hourly_map = _expand_forecast_hourly_pop(payload_h, start_utc=start_utc, end_utc=end_utc)
    hourly = _merge_hourly_arrays(hourly, pop_map=pop_hourly_map)

    out: Dict[str, Any] = {"issued_at": issued_at, "daily": daily}
    if hourly and isinstance(hourly.get("time"), list) and hourly["time"]:
        out["hourly"] = hourly
    return out

#-------------------
# Helpers
#-------------------

def _expand_forecast_hourly_pop(payload: dict, *, start_utc: datetime, end_utc: datetime) -> Dict[str, float]:
    """
    Build hourly precip probability map from /forecast/hourly.
    Returns: {"YYYY-MM-DDTHH:00:00Z": pop_pct}
    """
    props = (payload or {}).get("properties") or {}
    periods = props.get("periods") or []
    if not isinstance(periods, list):
        return {}

    out: Dict[str, float] = {}

    for p in periods:
        if not isinstance(p, dict):
            continue

        st = _parse_iso(p.get("startTime", ""))
        et = _parse_iso(p.get("endTime", ""))

        if not st or not et:
            continue

        st = _to_utc(st).replace(minute=0, second=0, microsecond=0)
        et = _to_utc(et).replace(minute=0, second=0, microsecond=0)

        pop_obj = p.get("probabilityOfPrecipitation") or {}
        pop = _to_float(pop_obj.get("value") if isinstance(pop_obj, dict) else pop_obj)
        if pop is None:
            continue

        # fill each hour from startTime up to (but not including) endTime
        t = st
        while t < et:
            if start_utc <= t <= end_utc:
                key = t.isoformat().replace("+00:00", "Z")
                out[key] = float(pop)
            t += timedelta(hours=1)

    return out


def _merge_hourly_arrays(
    base: Dict[str, Any],
    *,
    pop_map: Dict[str, float],
) -> Dict[str, Any]:
    """
    Merge precip_prob_pct into an existing hourly arrays object.
    Fills missing values for existing timestamps; does not change time axis.
    """
    if not base or not isinstance(base.get("time"), list) or not base["time"]:
        # If no base time axis, create one from pop_map
        if not pop_map:
            return {}
        times = sorted(pop_map.keys())
        return {"time": times, "precip_prob_pct": [float(pop_map[t]) for t in times]}

    times: List[str] = base["time"]
    existing = base.get("precip_prob_pct")
    if not isinstance(existing, list) or len(existing) != len(times):
        existing = [None] * len(times)

    merged: List[Optional[float]] = []
    for i, t in enumerate(times):
        v = existing[i]
        if v is None:
            pv = pop_map.get(t)
            merged.append(float(pv) if pv is not None else None)
        else:
            merged.append(float(v))

    base["precip_prob_pct"] = merged
    return base

