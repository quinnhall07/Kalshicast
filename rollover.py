# rollover.py
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Tuple

from croniter import croniter

from db import get_conn

"""
Rolling deletion / retention policy + schedule gate.

Single source of truth: THIS FILE.

How it runs in GitHub Actions:
- Workflow triggers every N minutes (poll).
- This script checks whether the configured cron schedule is "due" within the poll window.
- If not due, it prints SKIP and exits 0.
- If due, it performs rolling deletion.

DB env var:
- db.py requires WEATHER_DB_URL (or DATABASE_URL).
- This script expects DATABASE_URL in CI and will export it into WEATHER_DB_URL automatically.
"""


# =========================
# EDIT KNOBS HERE (ONLY)
# =========================

@dataclass(frozen=True)
class Policy:
    # The real schedule (evaluated in UTC)
    cron_utc: str = "25 7 * * *"  # daily at 07:25 UTC

    # Must be >= the workflow poll interval (minutes)
    poll_window_minutes: int = 16

    # Retention windows (days)
    retain_hourly_days: int = 45          # forecast_extras_hourly (largest)
    retain_daily_days: int = 45           # forecasts_daily
    retain_errors_days: int = 365         # forecast_errors
    retain_observations_days: int = 3650  # observations (ground truth)
    retain_forecast_runs_days: int = 365
    retain_observation_runs_days: int = 365

    # Ops knobs
    dry_run: bool = False
    verbose: bool = True
    batch_limit: int = 50_000
    max_batches_per_table: int = 50

    # DB wiring
    # db.py expects WEATHER_DB_URL (or DATABASE_URL); we standardize on WEATHER_DB_URL
    db_env_var: str = "WEATHER_DB_URL"
    ci_db_secret_env: str = "DATABASE_URL"  # what workflow exports


POLICY = Policy()


# =========================
# Helpers
# =========================

def _print(msg: str) -> None:
    if POLICY.verbose:
        print(msg, flush=True)


def _bridge_db_env() -> None:
    """
    Ensure db.py sees the correct env var.

    Supports:
      - local: WEATHER_DB_URL or DATABASE_URL
      - CI: secrets exported as WEATHER_DB_URL or DATABASE_URL
    """
    # If db.py-required var already set, nothing to do.
    env_var = POLICY.db_env_var
    if (os.getenv(env_var) or "").strip():
        return

    # Accept either common env var name.
    candidates = [
        env_var,                     # WEATHER_DB_URL
        "WEATHER_DB_URL",            # explicit
        "DATABASE_URL",              # common in CI
        POLICY.ci_db_secret_env,     # DATABASE_URL (per Policy)
    ]

    for k in candidates:
        v = (os.getenv(k) or "").strip()
        if v:
            os.environ[env_var] = v
            return

    raise RuntimeError("Missing WEATHER_DB_URL (or DATABASE_URL) for Supabase Postgres.")



def _is_due(now_utc: datetime) -> Tuple[bool, datetime, timedelta]:
    """
    Stateless schedule gate:
    - compute previous scheduled time <= now
    - run only if it occurred within poll window
    """
    it = croniter(POLICY.cron_utc, now_utc)
    prev_sched = it.get_prev(datetime)
    delta = now_utc - prev_sched
    return delta < timedelta(minutes=POLICY.poll_window_minutes), prev_sched, delta


def _delete_batched(*, table: str, where_sql: str, params: tuple, label: str) -> int:
    """
    Batch delete via CTID so it works with any PK shape.
    Returns total deleted.
    """
    if POLICY.dry_run:
        count_sql = f"SELECT count(*) FROM {table} WHERE {where_sql}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                n = int(cur.fetchone()[0])
        _print(f"[rollover] DRY_RUN {label}: would delete {n}")
        return n

    delete_sql = f"""
    WITH doomed AS (
      SELECT ctid
      FROM {table}
      WHERE {where_sql}
      LIMIT %s
    )
    DELETE FROM {table}
    WHERE ctid IN (SELECT ctid FROM doomed)
    """

    total = 0
    with get_conn() as conn:
        with conn.cursor() as cur:
            for batch in range(1, POLICY.max_batches_per_table + 1):
                cur.execute(delete_sql, params + (POLICY.batch_limit,))
                n = cur.rowcount or 0
                conn.commit()
                total += n
                _print(f"[rollover] OK {label}: batch {batch} deleted {n} (total {total})")
                if n == 0:
                    break
    return total


def main() -> int:
    _bridge_db_env()

    now_utc = datetime.now(timezone.utc)
    due, prev, delta = _is_due(now_utc)

    if not due:
        _print(f"[rollover] SKIP now={now_utc.isoformat()} prev={prev.isoformat()} delta={delta}")
        return 0

    # Cutoffs
    today_utc = now_utc.date()
    hourly_cutoff = now_utc - timedelta(days=max(0, POLICY.retain_hourly_days))
    daily_cutoff_date = today_utc - timedelta(days=max(0, POLICY.retain_daily_days))
    errors_cutoff_date = today_utc - timedelta(days=max(0, POLICY.retain_errors_days))
    obs_cutoff_date = today_utc - timedelta(days=max(0, POLICY.retain_observations_days))
    fr_cutoff = now_utc - timedelta(days=max(0, POLICY.retain_forecast_runs_days))
    or_cutoff = now_utc - timedelta(days=max(0, POLICY.retain_observation_runs_days))

    _print(
        "[rollover] DUE "
        f"cron={POLICY.cron_utc} prev={prev.isoformat()} "
        f"hourly<{hourly_cutoff.isoformat()} daily<{daily_cutoff_date.isoformat()} "
        f"errors<{errors_cutoff_date.isoformat()} obs<{obs_cutoff_date.isoformat()} "
        f"forecast_runs<{fr_cutoff.isoformat()} observation_runs<{or_cutoff.isoformat()} "
        f"dry_run={POLICY.dry_run} batch_limit={POLICY.batch_limit} max_batches={POLICY.max_batches_per_table}"
    )

    # 1) Children first
    _delete_batched(
        table="forecast_extras_hourly",
        where_sql="valid_time < %s",
        params=(hourly_cutoff,),
        label="forecast_extras_hourly",
    )

    _delete_batched(
        table="forecasts_daily",
        where_sql="target_date < %s",
        params=(daily_cutoff_date,),
        label="forecasts_daily",
    )

    _delete_batched(
        table="forecast_errors",
        where_sql="target_date < %s",
        params=(errors_cutoff_date,),
        label="forecast_errors",
    )

    _delete_batched(
        table="observations",
        where_sql="date < %s",
        params=(obs_cutoff_date,),
        label="observations",
    )

    # 2) Parent run tables (old + orphaned)
    _delete_batched(
        table="forecast_runs fr",
        where_sql="""
          fr.issued_at < %s
          AND NOT EXISTS (SELECT 1 FROM forecasts_daily d WHERE d.run_id = fr.id)
          AND NOT EXISTS (SELECT 1 FROM forecast_extras_hourly h WHERE h.run_id = fr.id)
          AND NOT EXISTS (SELECT 1 FROM forecast_errors e WHERE e.run_id = fr.id)
        """,
        params=(fr_cutoff,),
        label="forecast_runs (orphaned)",
    )

    _delete_batched(
        table="observation_runs oru",
        where_sql="""
          oru.created_at < %s
          AND NOT EXISTS (SELECT 1 FROM observations o WHERE o.run_id = oru.id)
        """,
        params=(or_cutoff,),
        label="observation_runs (orphaned)",
    )

    _print("[rollover] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
