# rollover.py
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from db import get_conn

"""
Rolling deletion / retention policy.

Single source of truth:
  rollover_config.json

GitHub Actions should NOT repeat retention knobs. It should just run:
  python rollover.py --config rollover_config.json

Notes:
- Uses UTC for all cutoffs.
- Batched deletes use CTID (works even with composite PKs).
- Parent run tables are deleted only if OLD and ORPHANED.
"""


# -----------------------------
# Config loading
# -----------------------------
@dataclass(frozen=True)
class RolloverConfig:
    # Retention windows (days)
    retain_hourly_days: int
    retain_daily_days: int
    retain_errors_days: int
    retain_observations_days: int
    retain_forecast_runs_days: int
    retain_observation_runs_days: int

    # Ops knobs
    dry_run: bool
    verbose: bool
    batch_limit: int
    max_batches_per_table: int


DEFAULT_CONFIG = RolloverConfig(
    retain_hourly_days=180,
    retain_daily_days=365,
    retain_errors_days=3650,
    retain_observations_days=3650,
    retain_forecast_runs_days=3650,
    retain_observation_runs_days=3650,
    dry_run=False,
    verbose=True,
    batch_limit=50_000,
    max_batches_per_table=50,
)


def _load_config(path: str) -> RolloverConfig:
    p = Path(path)
    if not p.exists():
        return DEFAULT_CONFIG

    data = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return DEFAULT_CONFIG

    def gi(name: str, default: int) -> int:
        v = data.get(name, default)
        try:
            return int(v)
        except Exception:
            return default

    def gb(name: str, default: bool) -> bool:
        v = data.get(name, default)
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "y", "on")
        return bool(v)

    return RolloverConfig(
        retain_hourly_days=gi("retain_hourly_days", DEFAULT_CONFIG.retain_hourly_days),
        retain_daily_days=gi("retain_daily_days", DEFAULT_CONFIG.retain_daily_days),
        retain_errors_days=gi("retain_errors_days", DEFAULT_CONFIG.retain_errors_days),
        retain_observations_days=gi("retain_observations_days", DEFAULT_CONFIG.retain_observations_days),
        retain_forecast_runs_days=gi("retain_forecast_runs_days", DEFAULT_CONFIG.retain_forecast_runs_days),
        retain_observation_runs_days=gi("retain_observation_runs_days", DEFAULT_CONFIG.retain_observation_runs_days),
        dry_run=gb("dry_run", DEFAULT_CONFIG.dry_run),
        verbose=gb("verbose", DEFAULT_CONFIG.verbose),
        batch_limit=gi("batch_limit", DEFAULT_CONFIG.batch_limit),
        max_batches_per_table=gi("max_batches_per_table", DEFAULT_CONFIG.max_batches_per_table),
    )


def _print(msg: str, cfg: RolloverConfig) -> None:
    if cfg.verbose:
        print(msg, flush=True)


# -----------------------------
# Batched delete helper (CTID)
# -----------------------------
def _delete_batched(
    *,
    table: str,
    where_sql: str,
    params: tuple,
    cfg: RolloverConfig,
    label: str,
) -> int:
    """
    Deletes in batches using CTID selection so it works with composite keys.
    Returns total deleted.
    """
    total = 0

    # Dry run: count only (no batches needed)
    if cfg.dry_run:
        count_sql = f"SELECT count(*) FROM {table} WHERE {where_sql}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                n = int(cur.fetchone()[0])
        _print(f"[rollover] DRY_RUN {label}: would delete {n}", cfg)
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

    with get_conn() as conn:
        with conn.cursor() as cur:
            for batch in range(1, cfg.max_batches_per_table + 1):
                cur.execute(delete_sql, params + (cfg.batch_limit,))
                n = cur.rowcount or 0
                conn.commit()
                total += n
                _print(f"[rollover] OK {label}: batch {batch} deleted {n} (total {total})", cfg)
                if n == 0:
                    break

    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="rollover_config.json", help="Path to rollover JSON config")
    args = ap.parse_args()

    cfg = _load_config(args.config)

    now_utc = datetime.now(timezone.utc)
    today_utc = now_utc.date()

    hourly_cutoff = now_utc - timedelta(days=max(0, cfg.retain_hourly_days))
    daily_cutoff_date = today_utc - timedelta(days=max(0, cfg.retain_daily_days))
    errors_cutoff_date = today_utc - timedelta(days=max(0, cfg.retain_errors_days))
    obs_cutoff_date = today_utc - timedelta(days=max(0, cfg.retain_observations_days))
    fr_cutoff = now_utc - timedelta(days=max(0, cfg.retain_forecast_runs_days))
    or_cutoff = now_utc - timedelta(days=max(0, cfg.retain_observation_runs_days))

    _print(
        "[rollover] policy "
        f"hourly<{hourly_cutoff.isoformat()} "
        f"daily<{daily_cutoff_date.isoformat()} "
        f"errors<{errors_cutoff_date.isoformat()} "
        f"obs<{obs_cutoff_date.isoformat()} "
        f"forecast_runs<{fr_cutoff.isoformat()} "
        f"observation_runs<{or_cutoff.isoformat()} "
        f"dry_run={cfg.dry_run} batch_limit={cfg.batch_limit} max_batches={cfg.max_batches_per_table}",
        cfg,
    )

    # ----------------------------
    # 1) Children first
    # ----------------------------
    _delete_batched(
        table="forecast_extras_hourly",
        where_sql="valid_time < %s",
        params=(hourly_cutoff,),
        cfg=cfg,
        label="forecast_extras_hourly",
    )

    _delete_batched(
        table="forecasts_daily",
        where_sql="target_date < %s",
        params=(daily_cutoff_date,),
        cfg=cfg,
        label="forecasts_daily",
    )

    _delete_batched(
        table="forecast_errors",
        where_sql="target_date < %s",
        params=(errors_cutoff_date,),
        cfg=cfg,
        label="forecast_errors",
    )

    _delete_batched(
        table="observations",
        where_sql="date < %s",
        params=(obs_cutoff_date,),
        cfg=cfg,
        label="observations",
    )

    # ----------------------------
    # 2) Parent run tables (old + orphaned)
    # ----------------------------
    # forecast_runs: issued_at is canonical in your design
    _delete_batched(
        table="forecast_runs fr",
        where_sql="""
          fr.issued_at < %s
          AND NOT EXISTS (SELECT 1 FROM forecasts_daily d WHERE d.run_id = fr.id)
          AND NOT EXISTS (SELECT 1 FROM forecast_extras_hourly h WHERE h.run_id = fr.id)
          AND NOT EXISTS (SELECT 1 FROM forecast_errors e WHERE e.run_id = fr.id)
        """,
        params=(fr_cutoff,),
        cfg=cfg,
        label="forecast_runs (orphaned)",
    )

    # observation_runs: assumes created_at exists
    _delete_batched(
        table="observation_runs oru",
        where_sql="""
          oru.created_at < %s
          AND NOT EXISTS (SELECT 1 FROM observations o WHERE o.run_id = oru.id)
        """,
        params=(or_cutoff,),
        cfg=cfg,
        label="observation_runs (orphaned)",
    )

    _print("[rollover] done", cfg)


if __name__ == "__main__":
    main()
