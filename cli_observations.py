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

# -------------------------
# Retry knobs (station-level)
# -------------------------
OBS_MAX_ATTEMPTS = 3
OBS_RETRY_BASE_SLEEP = 1.5

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


# -------------------------
# Robust numeric parsing for CLI values
# -------------------------

_NUM_WITH_SUFFIX_RE = re.compile(r"([\-]?\d+(?:\.\d+)?)([A-Za-z]+)?")

def _parse_number_with_optional_letter(token: str) -> Optional[Tuple[float, bool]]:
    """
    Parses tokens like: '35', '35R', '-2A', '12.5X'
    Returns (value, had_letter_suffix)

    Safety:
      - Reject if the token contains any digit after the initial numeric portion
        (e.g. '35R2' or '35-2' should not be coerced).
    """
    t = (token or "").strip()
    if not t:
        return None

    # Quick reject if there are multiple numeric groups (e.g. '35R2', '35-2')
    nums = re.findall(r"[\-]?\d+(?:\.\d+)?", t)
    if len(nums) != 1:
        return None

    m = _NUM_WITH_SUFFIX_RE.search(t)
    if not m:
        return None

    num_s = m.group(1)
    suffix = m.group(2) or ""

    # If there's any digit outside the numeric portion, reject
    # (we already enforced single numeric group, but keep this as belt-and-suspenders)
    remainder = t.replace(num_s, "", 1)
    if any(ch.isdigit() for ch in remainder):
        return None

    try:
        v = float(num_s)
    except Exception:
        return None

    had_letter = bool(suffix)
    return v, had_letter


def _suspicious_temp_f(v: float) -> bool:
    # Hard bounds: keep wide but realistic
    return (v < -120.0) or (v > 140.0)


def _parse_cli_max_min(text: str) -> Optional[Tuple[float, float]]:
    # Capture numeric token plus optional letter suffix (e.g. 35R)
    token = r"([\-]?\d+(?:\.\d+)?[A-Za-z]?)"

    max_patterns = [
        rf"\bMAXIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*{token}\b",
        rf"\bMAX(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*{token}\b",
        rf"\bHIGH(?:\s+TEMPERATURE)?\s*[:\-]\s*{token}\b",
        rf"\bMAXIMUM(?:\s+TEMPERATURE)?(?:\s*\(.*?\))?\s*\.{{2,}}\s*{token}\b",
    ]
    min_patterns = [
        rf"\bMINIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*{token}\b",
        rf"\bMIN(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*{token}\b",
        rf"\bLOW(?:\s+TEMPERATURE)?\s*[:\-]\s*{token}\b",
        rf"\bMINIMUM(?:\s+TEMPERATURE)?(?:\s*\(.*?\))?\s*\.{{2,}}\s*{token}\b",
    ]

    hi: Optional[Tuple[float, bool]] = None
    lo: Optional[Tuple[float, bool]] = None

    for p in max_patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            parsed = _parse_number_with_optional_letter(m.group(1))
            if parsed:
                hi = parsed
                break

    for p in min_patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            parsed = _parse_number_with_optional_letter(m.group(1))
            if parsed:
                lo = parsed
                break

    # Table format (MDW sample)
    if hi is None:
        m = re.search(rf"^\s*MAXIMUM\s+{token}\b", text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            hi = _parse_number_with_optional_letter(m.group(1))

    if lo is None:
        m = re.search(rf"^\s*MINIMUM\s+{token}\b", text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            lo = _parse_number_with_optional_letter(m.group(1))

    if hi is None or lo is None:
        return None

    hi_v, hi_letter = hi
    lo_v, lo_letter = lo

    # Basic sanity check (still allow wide range)
    if _suspicious_temp_f(hi_v) or _suspicious_temp_f(lo_v):
        return None

    return round(hi_v, 1), round(lo_v, 1)


def _cli_matches_site(text: str, cli_site: str) -> bool:
    t = text.upper()
    s = cli_site.upper().strip()
    if f"CLI{s}" in t:
        return True
    if re.search(rf"\b{s}\b", t) is not None:
        return True
    return s in t


def _parse_cli_report_date(text: str) -> Optional[str]:
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


def _extract_products_list(payload: dict) -> List[dict]:
    for key in ("@graph", "graph", "products", "items"):
        v = payload.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

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

    payload = _get_json(url, headers=headers, params=None, timeout=25, attempts=3)
    items = _extract_products_list(payload)
    return items[:limit]


def _issuance_sort_key(it: dict) -> str:
    return str(it.get("issuanceTime") or it.get("issueTime") or it.get("issuedAt") or "")


def _try_parse_cli_with_guardrails(
    *,
    station: dict,
    target_date: str,
    cli_site: str,
    loc_ids: List[str],
) -> Optional[Tuple[float, float, str, Optional[str]]]:
    """
    Returns (high, low, raw_text, issued_at) or None
    Adds a guardrail: if CLI contains a letter-suffixed value like '35R', we accept the numeric
    part but sanity-check against fallback station observations (if available).
    """
    station_id = station["station_id"]

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

                # Detect letter-suffixed temps in the relevant lines by re-running the token match
                # and checking if either hi/lo had a suffix.
                # We do this by searching the same patterns and seeing whether the captured token
                # had trailing letters.
                def _had_letter(patterns: List[str]) -> bool:
                    tok_pat = r"([\-]?\d+(?:\.\d+)?[A-Za-z]?)"
                    for p in patterns:
                        m = re.search(p, text, flags=re.IGNORECASE)
                        if m:
                            parsed = _parse_number_with_optional_letter(m.group(1))
                            if parsed:
                                _, had = parsed
                                return had
                    return False

                token = r"([\-]?\d+(?:\.\d+)?[A-Za-z]?)"
                max_patterns = [
                    rf"\bMAXIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*{token}\b",
                    rf"\bMAX(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*{token}\b",
                    rf"\bHIGH(?:\s+TEMPERATURE)?\s*[:\-]\s*{token}\b",
                    rf"\bMAXIMUM(?:\s+TEMPERATURE)?(?:\s*\(.*?\))?\s*\.{{2,}}\s*{token}\b",
                    rf"^\s*MAXIMUM\s+{token}\b",
                ]
                min_patterns = [
                    rf"\bMINIMUM(?:\s+TEMPERATURE)?\s*[:\-]\s*{token}\b",
                    rf"\bMIN(?:IMUM)?\s+TEMP(?:ERATURE)?\s*[:\-]\s*{token}\b",
                    rf"\bLOW(?:\s+TEMPERATURE)?\s*[:\-]\s*{token}\b",
                    rf"\bMINIMUM(?:\s+TEMPERATURE)?(?:\s*\(.*?\))?\s*\.{{2,}}\s*{token}\b",
                    rf"^\s*MINIMUM\s+{token}\b",
                ]

                parsed = _parse_cli_max_min(text)
                if not parsed:
                    continue
                high, low = parsed

                had_letter = _had_letter(max_patterns) or _had_letter(min_patterns)

                # Guardrail: if letter suffix was present, cross-check with fallback obs
                # (If fallback fails, accept the CLI parse—better to keep data than drop it.)
                if had_letter:
                    fb = _fallback_station_obs(station, target_date)
                    if fb:
                        fb_high, fb_low = fb
                        # If CLI deviates wildly, treat as suspicious and keep searching products.
                        if (abs(high - fb_high) > 20.0) or (abs(low - fb_low) > 20.0):
                            raise ValueError(
                                f"suspicious CLI temps (letter suffix) high/low={high}/{low} "
                                f"vs fallback={fb_high}/{fb_low}"
                            )

                return high, low, text, issued_at

            raise ValueError(f"no matching/parseable CLI in newest products for locationId={loc}")

        except Exception as e:
            last_cli_err = e
            continue

    if last_cli_err is not None:
        raise ValueError(str(last_cli_err))
    return None


def fetch_observations_for_station(station: dict, target_date: str) -> bool:
    """
    Station-level retry wrapper:
      - Retries network-ish failures and transient API issues.
      - Does NOT blindly retry parse failures unless they are tied to a transient product fetch/list.
    """
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

    cli_site = (
        station.get("cli_site")
        or (station_id[1:] if station_id.startswith("K") and len(station_id) == 4 else station_id)
    ).upper().strip()

    loc_ids: List[str] = []
    if cli_site:
        loc_ids.append(cli_site)
    if station.get("cli_location_id"):
        loc_ids.append(str(station["cli_location_id"]).upper().strip())
    if station_id.startswith("K") and len(station_id) == 4:
        loc_ids.append(station_id[1:])

    seen = set()
    loc_ids = [x for x in loc_ids if x and not (x in seen or seen.add(x))]

    last_err: Optional[Exception] = None

    for attempt in range(1, OBS_MAX_ATTEMPTS + 1):
        try:
            # CLI first (with guardrails for letter suffixes like 35R)
            try:
                hi_lo = _try_parse_cli_with_guardrails(
                    station=station,
                    target_date=target_date,
                    cli_site=cli_site,
                    loc_ids=loc_ids,
                )
            except Exception as e:
                hi_lo = None
                last_cli_err = e

            if hi_lo is not None:
                high, low, text, issued_at = hi_lo
                upsert_observation(
                    station_id=station_id,
                    obs_date=target_date,
                    observed_high=high,
                    observed_low=low,
                    raw_text=text,
                    source="NWS_CLI",
                )
                print(f"[obs] OK {station_id} {target_date}: high={high} low={low} (CLI)")
                return True

            # If CLI path didn't yield a result, fallback
            fb = _fallback_station_obs(station, target_date)
            if not fb:
                err_msg = str(last_cli_err) if "last_cli_err" in locals() else "CLI lookup failed"
                raise ValueError(f"CLI failed ({err_msg}); fallback failed")

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
            print(f"[obs] OK {station_id} {target_date}: high={high} low={low} (fallback)")
            return True

        except Exception as e:
            last_err = e
            # Retry only if likely transient
            retryable = _is_retryable_http(e) or any(
                s in str(e).lower()
                for s in ("timed out", "timeout", "temporarily", "try again", "connection reset", "service unavailable")
            )
            if (attempt >= OBS_MAX_ATTEMPTS) or (not retryable):
                break

            sleep_s = OBS_RETRY_BASE_SLEEP * attempt
            print(f"[obs] RETRY {station_id} {target_date} attempt {attempt}/{OBS_MAX_ATTEMPTS}: {e}", flush=True)
            time_mod.sleep(sleep_s)

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

