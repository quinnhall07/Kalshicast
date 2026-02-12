# cli_observations.py
from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timezone
from typing import Optional, Tuple

from config import STATIONS

from db import (
    get_conn,
    get_or_create_observation_run,
    upsert_observation,
)

# -------------------------
# CLI command knobs
# -------------------------
CLI_BIN = os.getenv("OBS_CLI_BIN", "python")
CLI_SCRIPT = os.getenv("OBS_CLI_SCRIPT", "cli_observations.py")  # if you use a separate script, set this
CLI_TIMEOUT_S = int(os.getenv("OBS_CLI_TIMEOUT_S", "30"))

# -------------------------
# Parsing helpers
# -------------------------

_NUM = re.compile(r"(-?\d+(?:\.\d+)?)")

def _utc_now_z() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _extract_number(s: str) -> Optional[float]:
    if not s:
        return None
    m = _NUM.search(s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _parse_cli_payload(raw: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Accepts either JSON output or a loose text payload.
    Returns (high, low, flagged_raw_text_or_none).
    """
    raw = (raw or "").strip()
    if not raw:
        return None, None, "empty cli output"

    # Try JSON first
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            hi = _to_float(obj.get("high") or obj.get("observed_high") or obj.get("tmax"))
            lo = _to_float(obj.get("low") or obj.get("observed_low") or obj.get("tmin"))
            # Flag if missing or weird
            if hi is None or lo is None:
                return hi, lo, raw[:8000]
            return hi, lo, None
    except Exception:
        pass

    # Text fallback: find two numbers in the payload
    nums = _NUM.findall(raw)
    if len(nums) >= 2:
        hi = _extract_number(nums[0])
        lo = _extract_number(nums[1])
        if hi is None or lo is None:
            return None, None, raw[:8000]
        return hi, lo, None

    # Not parseable
    return None, None, raw[:8000]


def _fetch_cli_daily_high_low(station_id: str, target_date: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Calls your CLI and parses the result.
    This function assumes your existing CLI command prints either JSON or text containing highs/lows.
    Adjust the subprocess args to match your real CLI entrypoint if needed.
    """
    # If your CLI is a different command, replace this with the correct one.
    # Example expectation:
    #   python cli_daily.py --station KNYC --date 2026-02-11
    cmd = [CLI_BIN, CLI_SCRIPT, "--station", station_id, "--date", target_date]

    try:
        p = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=CLI_TIMEOUT_S,
            check=False,
        )
    except Exception as e:
        return None, None, f"cli call failed: {e}"

    out = (p.stdout or "").strip()
    err = (p.stderr or "").strip()

    # If CLI returned nonzero, still try to parse stdout, but flag it.
    hi, lo, flagged = _parse_cli_payload(out)
    if p.returncode != 0:
        # keep the most useful raw text
        flag = flagged or (err or out or f"cli returncode {p.returncode}")
        return hi, lo, (str(flag)[:8000] if flag else f"cli returncode {p.returncode}")

    # If stderr had anything suspicious, flag it (but don’t override a parse failure flag)
    if err and flagged is None:
        flagged = err[:8000]

    return hi, lo, flagged


def fetch_observations(target_date: str) -> bool:
    """
    Nightly observation ingest.

    Contract:
      - Creates ONE observation_runs row for this execution (run_issued_at = now UTC).
      - Inserts/upserts ONE observations row per station_id for (run_id, station_id, date).
      - Authoritative observation is CLI; if CLI can't be parsed, you should have your
        METAR fallback inside the CLI or inside this function (not implemented here).
    """
    ok_any = False
    run_issued_at = _utc_now_z()

    with get_conn() as conn:
        # Create a run id for this batch (same for all stations)
        run_id = get_or_create_observation_run(run_issued_at=run_issued_at, conn=conn)

        for st in STATIONS:
            station_id = st["station_id"]

            try:
                hi, lo, flagged = _fetch_cli_daily_high_low(station_id, target_date)

                # If you have a METAR fallback, it should run here when (hi, lo) are missing.
                # For now: store what we have, but flag when parse failed.
                if hi is None and lo is None:
                    # still write a row only if you want a record of failure; otherwise skip
                    raise ValueError("no parseable cli high/low")

                upsert_observation(
                    run_id=run_id,
                    station_id=station_id,
                    date=target_date,
                    observed_high=hi,
                    observed_low=lo,
                    source="cli",
                    flagged_raw_text=flagged,
                    conn=conn,
                )
                ok_any = True
                print(f"[obs] OK {station_id} {target_date}: high={hi} low={lo}", flush=True)

            except Exception as e:
                print(f"[obs] FAIL {station_id} {target_date}: {e}", flush=True)

        conn.commit()

    return ok_any
