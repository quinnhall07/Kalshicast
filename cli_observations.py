# cli_observations.py
from __future__ import annotations

import math
import re
import time as time_mod
import requests
from datetime import datetime, date, time
from typing import Optional, List, Tuple, Iterable

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


def _is_retryable_http(e: Exception) -> bool:
    if isinstance(e, (requests.Timeout, requests.ConnectionError)):
        return True
    if isinstance(e, requests.HTTPError):
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        return code is None or code == 429 or code >= 500
    return False


def _get_json(
    url: str,
    *,
    headers: dict,
    params: Optional[dict] = None,
    timeout: int = 25,
    attempts: int = 3,
) -> dict:
    last: Optional[Exception] = None
    for i in range(attempts):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            if i == attempts - 1 or not _is_retryable_http(e):
                raise
            time_mod.sleep(1.25 * (i + 1))
    raise last  # pragma: no cover

def _fetch_product(product_id_or_url: str) -> Tuple[str, Optional[str]]:
    url = (
        product_id_or_url
        if product_id_or_url.startswith("http")
        else f"https://api.weather.gov/products/{product_id_or_url}"
    )
    headers = dict(HEADERS)
    headers["Accept"] = "application/ld+json"
    payload = _get_json(url, headers=headers, timeout=25, attempts=3)

    text = payload.get("productText") or payload.get("text")
    if not isinstance(text, str) or not text.strip():
        raise ValueError("product missing productText")

    issued_at = payload.get("issuanceTime") or payload.get("issueTime") or payload.get("issuedAt")
    return text, issued_at if isinstance(issued_at, str) else None


def _parse_cli_max_min(text: str) -> Optional[Tuple[float, float]]:
    # 1) Explicit variants and dot-leader variants
    max_patterns = [
        r"\bMAXIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bMAX(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bHIGH(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bMAXIMUM(?:\s+TEMPERATURE)?(?:\s*\(.*?\))?\s*\.{2,}\s*([\-]?\d+(?:\.\d+)?)\b",
    ]
    min_patterns = [
        r"\bMINIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bMIN(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bLOW(?:\s+TEMPERATURE)?\s*[:\-]\s*([\-]?\d+(?:\.\d+)?)\b",
        r"\bMINIMUM(?:\s+TEMPERATURE)?(?:\s*\(.*?\))?\s*\.{2,}\s*([\-]?\d+(?:\.\d+)?)\b",
    ]

    hi: Optional[float] = None
    lo: Optional[float] = None

    for p in max_patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            try:
                hi = float(m.group(1))
                break
            except ValueError:
                pass

    for p in min_patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            try:
                lo = float(m.group(1))
                break
            except ValueError:
                pass

    # 2) Table format (MDW sample)
    if hi is None:
        m = re.search(r"^\s*MAXIMUM\s+([\-]?\d+(?:\.\d+)?)\b", text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            hi = float(m.group(1))

    if lo is None:
        m = re.search(r"^\s*MINIMUM\s+([\-]?\d+(?:\.\d+)?)\b", text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            lo = float(m.group(1))

    if hi is None or lo is None:
        return None
    return round(hi, 1), round(lo, 1)


def _cli_matches_site(text: str, cli_site: str) -> bool:
    t = text.upper()
    s = cli_site.upper().strip()
    # Strong match: CLIMDW style header token
    if f"CLI{s}" in t:
        return True
    # Reasonable match: whole-word station token
    if re.search(rf"\b{s}\b", t) is not None:
        return True
    # Weak match (last resort): substring (can false-positive, so keep last)
    return s in t


def _parse_cli_report_date(text: str) -> Optional[str]:
    # Works for MDW-style:
    # "...THE CHICAGO-MIDWAY CLIMATE SUMMARY FOR JANUARY 31 2026..."
    m = re.search(
        r"\bCLIMATE\s+SUMMARY\s+FOR\s+([A-Z]+\s+\d{1,2}\s+\d{4})\b",
        text.upper(),
        flags=re.DOTALL,
    )
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(1).title(), "%B %d %Y").date()
        return dt.isoformat()
    except Exception:
        return None


def _fallback_station_obs(station: dict, target_date: str) -> Optional[Tuple[float, float]]:
    station_id = station["station_id"]
    target = date.fromisoformat(target_date)
    tz = ZoneInfo(station.get("timezone") or "UTC")

    start_local = datetime.combine(target, time(0, 0), tzinfo=tz)
    end_local = datetime.combine(target, time(23, 59), tzinfo=tz)
    start_utc = start_local.astimezone(ZoneInfo("UTC")).isoformat()
    end_utc = end_local.astimezone(ZoneInfo("UTC")).isoformat()

    url = f"https://api.weather.gov/stations/{station_id}/observations"
    params = {"start": start_utc, "end": end_utc, "limit": 500}
    headers = dict(HEADERS)
    headers["Accept"] = "application/geo+json"

    payload = _get_json(url, headers=headers, params=params, timeout=25, attempts=3)
    feats = payload.get("features", [])
    temps_f = _extract_temps_f(feats)
    if not temps_f:
        return None
    return round(max(temps_f), 1), round(min(temps_f), 1)


def _candidate_location_ids(station: dict, cwa: str) -> List[str]:
    """
    NWS products endpoint uses 'locationId', which isn't always the same thing as CWA.
    We try a few plausible IDs. You can add explicit station['cli_location_id'] later.
    """
    station_id = str(station.get("station_id") or "").upper()
    cli_site = str(station.get("cli_site") or "").upper().strip()

    candidates: List[str] = []
    if cwa:
        candidates.append(cwa.upper().strip())
    if station.get("cli_location_id"):
        candidates.append(str(station["cli_location_id"]).upper().strip())
    if cli_site:
        candidates.append(cli_site)
    # Common heuristic: KMDW -> MDW
    if station_id.startswith("K") and len(station_id) == 4:
        candidates.append(station_id[1:])

    # de-dupe while preserving order
    out: List[str] = []
    seen = set()
    for x in candidates:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def _extract_products_list(payload: dict) -> List[dict]:
    """
    NWS sometimes returns LD+JSON with '@graph', but it can vary.
    We tolerate a few shapes to avoid false 'empty' results.
    """
    # Common LD+JSON
    for key in ("@graph", "graph", "products", "items"):
        v = payload.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    # Sometimes nested
    v = payload.get("data")
    if isinstance(v, dict):
        for key in ("@graph", "graph", "products", "items"):
            vv = v.get(key)
            if isinstance(vv, list):
                return [x for x in vv if isinstance(x, dict)]

    return []


def _list_cli_products(location_id: str, limit: int = 50) -> List[dict]:
    url = f"https://api.weather.gov/products/types/CLI/locations/{location_id}"
    headers = dict(HEADERS)
    headers["Accept"] = "application/ld+json"

    # NWS endpoint does NOT accept query.limit; pull all then slice locally.
    payload = _get_json(url, headers=headers, params=None, timeout=25, attempts=3)
    items = _extract_products_list(payload)

    return items[:limit]


def _issuance_sort_key(it: dict) -> str:
    # Prefer anything timestamp-ish; string sort is fine for ISO-ish values
    return str(it.get("issuanceTime") or it.get("issueTime") or it.get("issuedAt") or "")


def fetch_observations_for_station(station: dict, target_date: str) -> bool:
    station_id = station["station_id"]

    upsert_location(
        {
            "station_id": station_id,
            "name": station.get("name"),
            "lat": station.get("lat"),
            "lon": station.get("lon"),
            "timezone": station.get("timezone"),
            "state": station.get("state"),
            "elevation_ft": station.get("elevation_ft"),
            "is_active": station.get("is_active"),
        }
    )

    # What we expect to see in the CLI text (e.g., CLIMDW)
    cli_site = (
        station.get("cli_site")
        or (station_id[1:] if station_id.startswith("K") and len(station_id) == 4 else station_id)
    ).upper().strip()

    # Location IDs to try for the CLI endpoint.
    # Priority: explicit config -> heuristic from station_id.
    loc_ids: List[str] = []
    if cli_site:
        loc_ids.append(cli_site)

    # Optional overrides if you add them later
    if station.get("cli_location_id"):
        loc_ids.append(str(station["cli_location_id"]).upper().strip())

    # Common heuristic: KMDW -> MDW (already handled by cli_site default above, but keep as safety)
    if station_id.startswith("K") and len(station_id) == 4:
        loc_ids.append(station_id[1:])

    # de-dupe while preserving order
    seen = set()
    loc_ids = [x for x in loc_ids if x and not (x in seen or seen.add(x))]

    # CLI path (list -> product -> parse)
    try:
        last_cli_err: Optional[Exception] = None

        for loc in loc_ids:
            try:
                items = _list_cli_products(loc, limit=60)
                if not items:
                    raise ValueError(f"no CLI products for locationId={loc}")

                items_sorted = sorted(items, key=_issuance_sort_key, reverse=True)

                for it in items_sorted[:30]:
                    pid = it.get("id") or it.get("@id")
                    if not isinstance(pid, str) or not pid.strip():
                        continue

                    text, issued_at = _fetch_product(pid.strip())

                    if not _cli_matches_site(text, cli_site):
                        continue

                    report_date = _parse_cli_report_date(text)
                    if report_date and report_date != target_date:
                        continue

                    parsed = _parse_cli_max_min(text)
                    if not parsed:
                        continue

                    high, low = parsed
                    upsert_observation(
                        station_id=station_id,
                        obs_date=target_date,
                        observed_high=high,
                        observed_low=low,
                        issued_at=issued_at,
                        raw_text=text,
                        source="NWS_CLI",
                    )
                    print(f"[obs] OK {station_id} {target_date}: high={high} low={low} (CLI)")
                    return True

                raise ValueError(f"no matching/parseable CLI in newest products for locationId={loc}")

            except Exception as e:
                last_cli_err = e
                continue

        raise ValueError(str(last_cli_err) if last_cli_err else "CLI lookup failed")

    except Exception as e:
        # Fallback (still needed; CLI is not guaranteed for every site/day)
        fb = _fallback_station_obs(station, target_date)
        if not fb:
            print(f"[obs] FAIL {station_id} {target_date}: CLI failed ({e}); fallback failed")
            return False

        high, low = fb
        upsert_observation(
            station_id=station_id,
            obs_date=target_date,
            observed_high=high,
            observed_low=low,
            issued_at=None,
            raw_text=None,
            source="NWS_OBS_FALLBACK",
        )
        print(f"[obs] OK {station_id} {target_date}: high={high} low={low} (fallback; CLI failed: {e})")
        return True


def fetch_observations(target_date: str) -> bool:
    any_ok = False
    for st in STATIONS:
        try:
            ok = fetch_observations_for_station(st, target_date)
            any_ok = any_ok or ok
        except Exception as e:
            print(f"[obs] FAIL {st.get('station_id')} {target_date}: {e}")
    return any_ok


