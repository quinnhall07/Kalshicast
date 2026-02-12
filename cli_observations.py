# cli_observations.py
from __future__ import annotations

import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import requests

from config import STATIONS
from db import (
    get_or_create_observation_run,
    upsert_location,
    upsert_observation,
)

# -------------------------
# CLI (authoritative)
# -------------------------

CLI_CMD = os.getenv("OBS_CLI_CMD", "").strip()
# If you already have a working CLI command, set OBS_CLI_CMD in GitHub Actions, e.g.:
#   OBS_CLI_CMD="python -m your_cli_module"
# If blank, we won't attempt CLI and will go straight to METAR fallback.

CLI_TIMEOUT_S = int(os.getenv("OBS_CLI_TIMEOUT_S", "30"))

# -------------------------
# METAR fallback (non-authoritative)
# -------------------------

NWS_METAR_URL = "https://aviationweather.gov/api/data/metar"
HTTP_TIMEOUT_S = 25


def _utc_now_trunc_hour_z() -> str:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return now.isoformat().replace("+00:00", "Z")


def _to_float(x: object) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _extract_number(line: str) -> Optional[float]:
    # grabs first number like -12, 12, 12.3
    m = re.search(r"(-?\d+(?:\.\d+)?)", line)
    if not m:
        return None
    return _to_float(m.group(1))


def _parse_cli_high_low(text: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Try hard to parse CLI-style daily summary text.
    Handles common NWS daily climate report formats.
    """
    if not text:
        return None, None

    hi: Optional[float] = None
    lo: Optional[float] = None

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Pass 1: targeted keywords on same line
    hi_keys = ("MAXIMUM TEMPERATURE", "HIGH TEMPERATURE", "MAX TEMP", "HIGH TEMP", "DAILY MAX")
    lo_keys = ("MINIMUM TEMPERATURE", "LOW TEMPERATURE", "MIN TEMP", "LOW TEMP", "DAILY MIN")

    for ln in lines:
        u = ln.upper()
        if hi is None and any(k in u for k in hi_keys):
            v = _extract_number(ln)
            if v is not None:
                hi = v
                continue
        if lo is None and any(k in u for k in lo_keys):
            v = _extract_number(ln)
            if v is not None:
                lo = v
                continue

    # Pass 2: table-ish formats where "MAX" and "MIN" might be separate tokens
    if hi is None:
        for ln in lines:
            u = ln.upper()
            if u.startswith("MAX") or " MAX " in u:
                v = _extract_number(ln)
                if v is not None:
                    hi = v
                    break

    if lo is None:
        for ln in lines:
            u = ln.upper()
            if u.startswith("MIN") or " MIN " in u:
                v = _extract_number(ln)
                if v is not None:
                    lo = v
                    break

    return hi, lo


def _run_cli_for_station_date(station_id: str, target_date: str) -> str:
    """
    Calls your authoritative CLI. You must provide OBS_CLI_CMD in env.
    Must return raw stdout text.
    """
    if not CLI_CMD:
        return ""

    # We allow a generic CLI that accepts station/date flags; adjust if your CLI differs.
    # If your CLI uses different flags, update these two args in ONE place.
    cmd = CLI_CMD.split() + ["--station", station_id, "--date", target_date]

    p = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=CLI_TIMEOUT_S,
        check=False,
    )

    out = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
    return out.strip()


def _metar_daily_high_low(station_id: str, target_date: str) -> Tuple[Optional[float], Optional[float], str]:
    """
    Non-authoritative fallback:
    Pull METARs and compute max/min temp for the UTC day.
    Returns (hi, lo, raw_text_summary)
    """
    # target_date is YYYY-MM-DD
    start = f"{target_date}T00:00:00Z"
    end_dt = datetime.fromisoformat(target_date).replace(tzinfo=timezone.utc) + timedelta(days=1)
    end = end_dt.isoformat().replace("+00:00", "Z")

    params = {
        "ids": station_id,
        "format": "json",
        "date": start,
        "enddate": end,
    }

    r = requests.get(NWS_METAR_URL, params=params, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    data = r.json()

    temps = []
    # aviationweather.gov returns list of dict records (varies); try common fields
    for rec in data if isinstance(data, list) else []:
        # temperature can appear as 'temp' (C) or 'temp_f' depending on endpoint version; try both
        tf = rec.get("temp_f")
        if tf is None and rec.get("temp") is not None:
            # assume C -> F
            try:
                tf = float(rec["temp"]) * 9.0 / 5.0 + 32.0
            except Exception:
                tf = None
        v = _to_float(tf)
        if v is not None:
            temps.append(v)

    if not temps:
        return None, None, "metar: no temps"

    hi = max(temps)
    lo = min(temps)
    return hi, lo, f"metar: n={len(temps)} hi={hi:.1f} lo={lo:.1f} (UTC-day)"


def fetch_observations(target_date: str) -> bool:
    """
    Writes observations for all stations for target_date.
    Authoritative source: CLI parsed highs/lows.
    Fallback: METAR-derived highs/lows if CLI unparseable.

    Returns True if at least one station was successfully written.
    """
    # Ensure locations exist
    for st in STATIONS:
        upsert_location(st)

    run_id = get_or_create_observation_run(run_issued_at=_utc_now_trunc_hour_z())

    ok_any = False
    for st in STATIONS:
        station_id = st["station_id"]

        cli_text = ""
        cli_hi = cli_lo = None
        cli_err: Optional[str] = None

        # 1) CLI attempt (authoritative)
        try:
            cli_text = _run_cli_for_station_date(station_id, target_date)
            cli_hi, cli_lo = _parse_cli_high_low(cli_text)
            if cli_hi is None or cli_lo is None:
                raise ValueError("no parseable cli high/low")
        except Exception as e:
            cli_err = str(e)

        # 2) Fallback to METAR if CLI failed
        used_source = "CLI"
        hi = cli_hi
        lo = cli_lo
        flagged = None

        if hi is None or lo is None:
            try:
                m_hi, m_lo, m_note = _metar_daily_high_low(station_id, target_date)
                if m_hi is None or m_lo is None:
                    raise ValueError("metar fallback produced no high/low")

                used_source = "METAR"
                hi, lo = m_hi, m_lo
                flagged = f"CLI failed ({cli_err}); {m_note}"
            except Exception as e:
                print(f"[obs] FAIL {station_id} {target_date}: {cli_err or ''}{' | ' if cli_err else ''}{e}", flush=True)
                continue

        try:
            upsert_observation(
                run_id=run_id,
                station_id=station_id,
                date=target_date,
                observed_high=float(hi),
                observed_low=float(lo),
                source=used_source,
                flagged_raw_text=flagged,
                raw_text=(cli_text[:8000] if cli_text else None),
            )
            ok_any = True
            print(f"[obs] OK {station_id} {target_date}: high={hi} low={lo} ({used_source})", flush=True)
        except Exception as e:
            print(f"[obs] FAIL {station_id} {target_date}: {e}", flush=True)

    return ok_any
