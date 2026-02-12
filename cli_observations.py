# cli_observations.py
from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import requests

from config import STATIONS
from db import (
    get_or_create_observation_run,
    upsert_location,
    upsert_observation,
    get_conn,
)

# -------------------------
# CLI (authoritative)
# -------------------------

CLI_CMD = os.getenv("OBS_CLI_CMD", "").strip()
# If blank, we won't attempt CLI and will go straight to METAR fallback.

CLI_TIMEOUT_S = int(os.getenv("OBS_CLI_TIMEOUT_S", "30"))

# -------------------------
# METAR fallback (non-authoritative)
# -------------------------

NWS_METAR_URL = "https://aviationweather.gov/api/data/metar"
HTTP_TIMEOUT_S = 25

@dataclass(frozen=True)
class CliResult:
    ok: bool
    raw: str
    reason: str  # why not ok (for flagged_reason prefix)


def _run_cli_daily(station_id: str, target_date: str) -> CliResult:
    """
    Run your CLI command that returns daily high/low for a station and date.

    IMPORTANT:
    - Keep this function aligned with whatever your actual CLI invocation is.
    - This wrapper just guarantees we always return raw text for flagging.
    """
    # TODO: adjust this command to your real CLI tool if needed.
    cmd = ["python", "cli_daily.py", "--station", station_id, "--date", target_date]

    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=25)
        raw = (p.stdout or "") + ("\n" + p.stderr if p.stderr else "")
        raw = raw.strip()
        if p.returncode != 0:
            return CliResult(ok=False, raw=raw or f"CLI returncode={p.returncode}", reason=f"CLI failed (returncode={p.returncode})")
        return CliResult(ok=True, raw=raw, reason="")
    except subprocess.TimeoutExpired as e:
        raw = (getattr(e, "stdout", "") or "") + ("\n" + getattr(e, "stderr", "") if getattr(e, "stderr", "") else "")
        raw = (raw or str(e)).strip()
        return CliResult(ok=False, raw=raw, reason="CLI failed (timeout)")
    except Exception as e:
        return CliResult(ok=False, raw=str(e).strip(), reason="CLI failed (exception)")

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
    m = re.search(r"(-?\d+(?:\.\d+)?)", line)
    if not m:
        return None
    return _to_float(m.group(1))


def _parse_cli_high_low(raw: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Parse CLI output -> (high, low, parse_fail_reason)

    You must match your CLI’s actual output format here.

    Current behavior:
    - returns (None,None,"no parseable cli high/low") if it can't parse.
    """
    if not raw or not raw.strip():
        return None, None, "empty cli output"

    s = raw.strip()

    # VERY SIMPLE parser example:
    # Expect lines containing "high=" and "low=" somewhere.
    try:
        hi = None
        lo = None
        for token in s.replace(",", " ").replace(";", " ").split():
            t = token.strip()
            if t.lower().startswith("high="):
                hi = float(t.split("=", 1)[1])
            elif t.lower().startswith("low="):
                lo = float(t.split("=", 1)[1])
        if hi is None or lo is None:
            return None, None, "no parseable cli high/low"
        return hi, lo, None
    except Exception:
        return None, None, "no parseable cli high/low"


def _run_cli_for_station_date(station_id: str, target_date: str) -> str:
    if not CLI_CMD:
        return ""

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
    for rec in data if isinstance(data, list) else []:
        tf = rec.get("temp_f")
        if tf is None and rec.get("temp") is not None:
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
    For each station:
      - authoritative source is CLI daily high/low
      - if CLI can't be parsed, fall back to METAR-derived daily

    Writes:
      observations.source = "CLI" or "METAR"
      observations.flagged_raw_text = raw CLI text if CLI failed/was unparseable
      observations.flagged_reason   = explanation if CLI failed/was unparseable
    """
    any_ok = False

    with get_conn() as conn:
        run_id = get_or_create_observation_run(run_issued_at=f"{target_date}T00:00:00Z", conn=conn)

        for st in STATIONS:
            station_id = st["station_id"]

            cli = _run_cli_daily(station_id, target_date)
            hi_cli, lo_cli, parse_fail = (None, None, None)

            if cli.ok:
                hi_cli, lo_cli, parse_fail = _parse_cli_high_low(cli.raw)
            else:
                parse_fail = cli.reason or "CLI failed"

            if hi_cli is not None and lo_cli is not None and parse_fail is None:
                # CLI wins (authoritative)
                upsert_observation(
                    run_id=run_id,
                    station_id=station_id,
                    date=target_date,
                    observed_high=hi_cli,
                    observed_low=lo_cli,
                    source="CLI",
                    flagged_raw_text=None,
                    flagged_reason=None,
                    conn=conn,
                )
                conn.commit()
                print(f"[obs] OK {station_id} {target_date}: high={hi_cli} low={lo_cli} (CLI)")
                any_ok = True
                continue

            # CLI failed/unparseable -> METAR fallback
            hi_m, lo_m, metar_summary = _metar_daily_high_low(station_id, target_date)

            flagged_raw_text = (cli.raw or "").strip() or None
            # flagged_reason should explain WHY the CLI couldn't be used (+ mention fallback)
            reason_core = parse_fail or "CLI parse failed"
            flagged_reason = f"{reason_core}; fell back to {metar_summary}"

            upsert_observation(
                run_id=run_id,
                station_id=station_id,
                date=target_date,
                observed_high=hi_m,
                observed_low=lo_m,
                source="METAR",
                flagged_raw_text=flagged_raw_text,
                flagged_reason=flagged_reason,
                conn=conn,
            )
            conn.commit()

            if hi_m is None and lo_m is None:
                print(f"[obs] FAIL {station_id} {target_date}: {reason_core} and METAR unavailable")
            else:
                print(f"[obs] OK {station_id} {target_date}: high={hi_m} low={lo_m} (METAR)")
                any_ok = True

    return any_ok

