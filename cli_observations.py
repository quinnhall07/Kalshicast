from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List

import requests

from config import STATIONS, HEADERS
from db import get_or_create_observation_run, upsert_observation, get_conn


# -------------------------
# Policy (single source)
# -------------------------
#
# - Authoritative observation is the CLI daily high/low.
# - If CLI cannot be parsed, fall back to METAR-derived daily (UTC-day).
#
# If CLI fails/unparseable and METAR succeeds:
#   - source="METAR"
#   - flagged_raw_text = raw CLI output (stdout+stderr) when available
#   - flagged_reason  = why CLI failed/unparseable
#
# If CLI succeeds:
#   - source="CLI"
#   - flagged_* are NULL
#
# IMPORTANT:
#   Set OBS_CLI_CMD in CI to your real CLI command.
#   Example: OBS_CLI_CMD="python -m your_cli_module"
#   This file will invoke: <OBS_CLI_CMD> <STATION_ID> <YYYY-MM-DD>
#

@dataclass(frozen=True)
class ObsPolicy:
    cli_cmd_env: str = "OBS_CLI_CMD"

    # METAR fallback controls
    metar_hours_back: int = 30  # request more, then filter by UTC day
    http_timeout_s: int = 25


POLICY = ObsPolicy()


def _split_cmd(s: str) -> List[str]:
    # Simple env-command splitter. If you need quoting, wrap in a script and set OBS_CLI_CMD to that script.
    return [p for p in s.strip().split() if p]


def _run_cli_daily(*, station_id: str, target_date: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (raw_text, err_reason).
    raw_text includes stdout+stderr (when available).
    err_reason is a short code like:
      - cli_unavailable
      - cli_not_found
      - cli_timeout
      - cli_nonzero:<code>
      - cli_error:<ExceptionName>
    """
    cmd = (os.getenv(POLICY.cli_cmd_env) or "").strip()
    if not cmd:
        return None, "cli_unavailable"

    args = _split_cmd(cmd)
    if not args:
        return None, "cli_unavailable"

    try:
        p = subprocess.run(
            args + [station_id, target_date],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except subprocess.TimeoutExpired as e:
        raw = ((e.stdout or "") + ("\n" + e.stderr if e.stderr else "")).strip() or None
        return raw, "cli_timeout"
    except FileNotFoundError:
        return None, "cli_not_found"
    except Exception as e:
        return None, f"cli_error:{type(e).__name__}"

    raw = ((p.stdout or "") + ("\n" + p.stderr if p.stderr else "")).strip() or None

    if p.returncode != 0:
        return raw, f"cli_nonzero:{p.returncode}"

    return raw, None


# Defensive CLI parsing:
_RE_HI = re.compile(r"\b(high|max)\b[^-\d]*(-?\d+(\.\d+)?)", re.IGNORECASE)
_RE_LO = re.compile(r"\b(low|min)\b[^-\d]*(-?\d+(\.\d+)?)", re.IGNORECASE)


def _parse_cli_high_low(raw: str) -> Optional[Tuple[float, float]]:
    if not raw or not raw.strip():
        return None

    m_hi = _RE_HI.search(raw)
    m_lo = _RE_LO.search(raw)
    if not m_hi or not m_lo:
        return None

    try:
        hi = float(m_hi.group(2))
        lo = float(m_lo.group(2))
    except Exception:
        return None

    return (hi, lo)


def _metar_url() -> str:
    # AviationWeather.gov API
    return "https://aviationweather.gov/api/data/metar"


def _fetch_metar_daily_utc_day(*, station_id: str, target_date: str) -> Tuple[Optional[float], Optional[float], str]:
    """
    METAR fallback: compute high/low using METAR temperature over the UTC day of target_date.
    Returns (high_f, low_f, meta_str).
    """
    hours = max(1, min(72, POLICY.metar_hours_back))
    q = {"ids": station_id, "format": "json", "hours": str(hours)}

    r = requests.get(_metar_url(), params=q, headers=dict(HEADERS), timeout=POLICY.http_timeout_s)
    r.raise_for_status()
    data = r.json()

    rows = data.get("data") if isinstance(data, dict) else data
    if not isinstance(rows, list):
        return None, None, "metar: bad response shape"

    day0 = datetime.fromisoformat(target_date).replace(tzinfo=timezone.utc)
    day1 = day0 + timedelta(days=1)

    temps: List[float] = []

    for rec in rows:
        if not isinstance(rec, dict):
            continue

        ts = rec.get("obsTime") or rec.get("reportTime") or rec.get("time")
        if not isinstance(ts, str) or not ts.strip():
            continue

        try:
            if ts.endswith("Z"):
                dt = datetime.fromisoformat(ts[:-1] + "+00:00")
            else:
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            dt = dt.astimezone(timezone.utc)
        except Exception:
            continue

        if not (day0 <= dt < day1):
            continue

        tf = None
        if rec.get("tempF") is not None:
            try:
                tf = float(rec["tempF"])
            except Exception:
                tf = None

        if tf is None and rec.get("tempC") is not None:
            try:
                tc = float(rec["tempC"])
                tf = tc * 9.0 / 5.0 + 32.0
            except Exception:
                tf = None

        if tf is None:
            continue

        temps.append(float(tf))

    if not temps:
        return None, None, "metar: n=0 (UTC-day)"

    hi = max(temps)
    lo = min(temps)
    return hi, lo, f"metar: n={len(temps)} hi={hi} lo={lo} (UTC-day)"


def fetch_observations(target_date: str) -> bool:
    """
    Fetch and store observations for all stations for target_date (YYYY-MM-DD).
    Returns True if any station wrote an observation.
    """
    # One observation_run per target_date, deterministic anchor.
    run_issued_at = f"{target_date}T12:00:00Z"
    run_id = get_or_create_observation_run(run_issued_at=run_issued_at)

    ok_any = False

    with get_conn() as conn:
        for st in STATIONS:
            sid = st["station_id"]

            # ---- 1) CLI ----
            raw_cli, cli_err = _run_cli_daily(station_id=sid, target_date=target_date)
            cli_parsed = _parse_cli_high_low(raw_cli or "") if raw_cli else None

            if cli_err is None and cli_parsed is not None:
                hi, lo = cli_parsed
                upsert_observation(
                    conn=conn,
                    run_id=run_id,
                    station_id=sid,
                    date=target_date,
                    observed_high=hi,
                    observed_low=lo,
                    source="CLI",
                    flagged_raw_text=None,
                    flagged_reason=None,
                )
                conn.commit()
                print(f"[obs] OK {sid} {target_date}: high={hi} low={lo} (CLI)", flush=True)
                ok_any = True
                continue

            # ---- 2) METAR fallback ----
            try:
                hi, lo, _meta = _fetch_metar_daily_utc_day(station_id=sid, target_date=target_date)
            except Exception as e:
                print(f"[obs] FAIL {sid} {target_date}: METAR error: {e}", flush=True)
                continue

            if hi is None or lo is None:
                reason = cli_err or "cli_unparseable"
                print(f"[obs] FAIL {sid} {target_date}: no parseable CLI and METAR empty ({reason})", flush=True)
                continue

            # flagged_raw_text MUST be the CLI text (when we have any).
            flagged_raw_text = raw_cli
            flagged_reason = f"CLI failed ({cli_err})" if cli_err is not None else "CLI failed (no parseable cli high/low)"

            upsert_observation(
                conn=conn,
                run_id=run_id,
                station_id=sid,
                date=target_date,
                observed_high=hi,
                observed_low=lo,
                source="METAR",
                flagged_raw_text=flagged_raw_text,
                flagged_reason=flagged_reason,
            )
            conn.commit()
            print(f"[obs] OK {sid} {target_date}: high={hi} low={lo} (METAR)", flush=True)
            ok_any = True

    return ok_any
