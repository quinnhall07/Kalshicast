# collect_nws.py
from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from config import HEADERS


# Strict collector payload shape:
# {
#   "issued_at": "...Z",
#   "daily": [ {"target_date":"YYYY-MM-DD","high":float,"low":float}, ... ],
#   "hourly": [
#      {"valid_time":"YYYY-MM-DDTHH:MM","temperature_f":float|None,"dewpoint_f":float|None,
#       "humidity_pct":float|None,"wind_speed_mph":float|None,"wind_dir_deg":float|None,
#       "cloud_cover_pct":float|None,"precip_prob_pct":float|None},
#      ...
#   ]
# }


def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


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


def _parse_iso(dt_str: str) -> Optional[datetime]:
    if not isinstance(dt_str, str) or not dt_str:
        return None
    try:
        # Handles ...Z and offsets
        if dt_str.endswith("Z"):
            dt_str = dt_str[:-1] + "+00:00"
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def _strip_tz_to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


_DURATION_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")


def _parse_duration_hours(dur: str) -> int:
    """
    Parse very common ISO8601 durations like PT1H, PT2H, PT30M, PT1H30M.
    Returns hours as an integer count of whole hours for expansion purposes.
    If < 1 hour, returns 1 (store at least one sample).
    """
    if not isinstance(dur, str) or not dur:
        return 1
    m = _DURATION_RE.match(dur.strip().upper())
    if not m:
        return 1
    h = int(m.group(1) or 0)
    mi = int(m.group(2) or 0)
    s = int(m.group(3) or 0)
    total_seconds = h * 3600 + mi * 60 + s
    if total_seconds <= 0:
        return 1
    # Expand by whole hours; at least 1
    return max(1, int(round(total_seconds / 3600.0)))


def _extract_points_urls(lat: float, lon: float) -> Tuple[str, str, str]:
    """
    Returns (forecast_url, forecast_hourly_url, forecast_grid_url)
    from api.weather.gov/points/{lat},{lon}
    """
    url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"
    payload = requests.get(url, headers=dict(HEADERS), timeout=20).json()
    props = (payload or {}).get("properties") or {}
    fc = props.get("forecast")
    fch = props.get("forecastHourly")
    grid = props.get("forecastGridData")
    if not (isinstance(fc, str) and isinstance(fch, str) and isinstance(grid, str)):
        raise ValueError("NWS points lookup missing forecast URLs")
    return fc, fch, grid


def _extract_daily_high_low_from_forecast(payload: dict, *, days: int) -> List[Dict[str, Any]]:
    """
    Uses /forecast endpoint periods and pairs Day/Night to get high/low per date.
    Keeps today..today+days inclusive (days ahead).
    """
    props = (payload or {}).get("properties") or {}
    periods = props.get("periods") or []
    if not isinstance(periods, list):
        return []

    start = date.today()
    end = start + timedelta(days=days)

    by_date: Dict[str, Dict[str, Optional[float]]] = {}
    for p in periods:
        if not isinstance(p, dict):
            continue
        st = _parse_iso(p.get("startTime", ""))
        if not st:
            continue
        d = _strip_tz_to_utc(st).date()
        if d < start or d > end:
            continue

        is_day = bool(p.get("isDaytime", False))
        temp = _to_float(p.get("temperature"))
        if temp is None:
            continue

        key = d.isoformat()
        rec = by_date.setdefault(key, {"high": None, "low": None})
        if is_day:
            # Daytime temp is usually the high
            rec["high"] = temp if rec["high"] is None else max(rec["high"], temp)
        else:
            # Night temp is usually the low
            rec["low"] = temp if rec["low"] is None else min(rec["low"], temp)

    out: List[Dict[str, Any]] = []
    for i in range((end - start).days + 1):
        d = (start + timedelta(days=i)).isoformat()
        rec = by_date.get(d) or {}
        hi = rec.get("high")
        lo = rec.get("low")
        if hi is None or lo is None:
            continue
        out.append({"target_date": d, "high": float(hi), "low": float(lo)})
    return out


def _uom_to_mph(uom: str, v: float) -> float:
    u = (uom or "").lower()
    # Common in NWS grid data:
    # - unit:km_h-1
    # - unit:m_s-1
    # - unit:kn (knots)
    if "km_h" in u or "km/h" in u:
        return v * 0.621371
    if "m_s" in u or "m/s" in u:
        return v * 2.236936
    if "kn" in u or "knot" in u:
        return v * 1.150779
    # Fallback: assume already mph-ish
    return v


def _uom_to_f(uom: str, v: float) -> float:
    u = (uom or "").lower()
    # NWS temp/dewpoint usually in degC
    if "degc" in u or "celsius" in u:
        return _c_to_f(v)
    if "degf" in u or "fahrenheit" in u:
        return v
    # Fallback: assume C (most common)
    return _c_to_f(v)


def _expand_grid_values(
    values: Iterable[dict],
    *,
    uom: str,
    kind: str,
    start_utc: datetime,
    end_utc: datetime,
) -> Dict[str, float]:
    """
    Expand NWS grid data values into hourly timestamps (UTC), returning map:
      "YYYY-MM-DDTHH:MM" -> value (converted to desired units)
    Expansion repeats the same value for each hour covered by validTime duration.
    """
    out: Dict[str, float] = {}

    for it in values:
        if not isinstance(it, dict):
            continue
        vt = it.get("validTime")
        val = _to_float(it.get("value"))
        if not isinstance(vt, str) or val is None:
            continue

        # validTime looks like: "2026-02-01T00:00:00+00:00/PT1H"
        try:
            start_s, dur_s = vt.split("/")
        except ValueError:
            continue

        dt = _parse_iso(start_s)
        if not dt:
            continue
        dt = _strip_tz_to_utc(dt)

        hours = _parse_duration_hours(dur_s)

        # Convert units depending on field
        if kind in ("temperature_f", "dewpoint_f"):
            val_conv = _uom_to_f(uom, float(val))
        elif kind == "wind_speed_mph":
            val_conv = _uom_to_mph(uom, float(val))
        else:
            val_conv = float(val)

        # Expand by hour
        for h in range(hours):
            t = dt + timedelta(hours=h)
            if t < start_utc or t > end_utc:
                continue
            key = t.replace(minute=0, second=0, microsecond=0).isoformat(timespec="minutes")
            out[key] = val_conv

    return out


def _extract_hourly_from_grid(payload: dict, *, days: int) -> List[Dict[str, Any]]:
    """
    Uses /forecastGridData to derive hourly-ish variables with better coverage
    (dewpoint, humidity, skyCover, pop, etc.) than /forecastHourly.
    """
    props = (payload or {}).get("properties") or {}

    start_day = date.today()
    end_day = start_day + timedelta(days=days)
    start_utc = datetime.combine(start_day, datetime.min.time(), tzinfo=timezone.utc)
    end_utc = datetime.combine(end_day, datetime.max.time(), tzinfo=timezone.utc).replace(minute=0, second=0, microsecond=0)

    def _series(name: str) -> Tuple[str, List[dict]]:
        obj = props.get(name) or {}
        return (str(obj.get("uom") or ""), (obj.get("values") or []))  # type: ignore[return-value]

    # Attempt common NWS grid keys
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

    # Union timestamps across maps (prefer temperature timestamps as the backbone)
    keys = set(temp_map.keys()) | set(dew_map.keys()) | set(rh_map.keys()) | set(ws_map.keys()) | set(wd_map.keys()) | set(sc_map.keys()) | set(pop_map.keys())
    if not keys:
        return []

    def _maybe(m: Dict[str, float], k: str) -> Optional[float]:
        v = m.get(k)
        return float(v) if v is not None else None

    out: List[Dict[str, Any]] = []
    for k in sorted(keys):
        out.append(
            {
                "valid_time": k[:16],  # "YYYY-MM-DDTHH:MM"
                "temperature_f": _maybe(temp_map, k),
                "dewpoint_f": _maybe(dew_map, k),
                "humidity_pct": _maybe(rh_map, k),
                "wind_speed_mph": _maybe(ws_map, k),
                "wind_dir_deg": _maybe(wd_map, k),
                "cloud_cover_pct": _maybe(sc_map, k),
                "precip_prob_pct": _maybe(pop_map, k),
            }
        )
    return out


def fetch_nws_forecast(station: dict, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Collects:
      - daily highs/lows from NWS /forecast
      - hourly variable series from NWS /forecastGridData

    params (optional):
      - days_ahead: int (default 3)
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

    forecast_url, _forecast_hourly_url, grid_url = _extract_points_urls(float(lat), float(lon))

    # Daily forecast (high/low)
    r_fc = requests.get(forecast_url, headers=dict(HEADERS), timeout=25)
    r_fc.raise_for_status()
    payload_fc = r_fc.json()

    issued_at = (
        (payload_fc.get("properties") or {}).get("updated")
        or (payload_fc.get("properties") or {}).get("generatedAt")
        or _utc_now_z()
    )
    if not isinstance(issued_at, str) or not issued_at.strip():
        issued_at = _utc_now_z()

    daily = _extract_daily_high_low_from_forecast(payload_fc, days=days)

    # Grid (hourly-ish extras + temperature/dewpoint/humidity/etc.)
    r_grid = requests.get(grid_url, headers=dict(HEADERS), timeout=25)
    r_grid.raise_for_status()
    payload_grid = r_grid.json()

    # Prefer a better issued time if available
    grid_generated = (payload_grid.get("properties") or {}).get("generatedAt")
    if isinstance(grid_generated, str) and grid_generated.strip():
        issued_at = grid_generated

    hourly = _extract_hourly_from_grid(payload_grid, days=days)

    # Strict shape
    return {"issued_at": issued_at, "daily": daily, "hourly": hourly}
