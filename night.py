# night.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from cli_observations import fetch_observations
from db import (
    init_db,
    build_forecast_errors_for_date,
    update_dashboard_stats,
)


def _parse_windows(env_val: str | None) -> list[int]:
    # Default windows (days). Keep short + month-ish.
    if not env_val:
        return [2, 3, 7, 14, 30, 90]
    out: list[int] = []
    for part in env_val.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            n = int(part)
            if n > 0:
                out.append(n)
        except ValueError:
            continue
    return out or [2, 3, 7, 14, 30, 90]


def _compute_metrics_for_day(target_date: str) -> None:
    """
    Night pipeline metrics step.

    Primary path: call compute_metrics.compute_day/score_day if available.
    Fallback path: call db.build_forecast_errors_for_date + db.update_dashboard_stats.
    """
    # Prefer compute_metrics if it's wired correctly.
    try:
        import compute_metrics  # type: ignore

        if hasattr(compute_metrics, "compute_day"):
            compute_metrics.compute_day(target_date)
            return
        if hasattr(compute_metrics, "score_day"):
            compute_metrics.score_day(target_date)
            return
    except Exception:
        # Fall back to DB-native implementation below.
        pass

    wrote = build_forecast_errors_for_date(target_date=target_date)
    if wrote == 0:
        print(f"[night] SKIP {target_date}: no forecast errors written", flush=True)
        return

    windows = _parse_windows(os.getenv("STATS_WINDOWS_DAYS"))
    for w in windows:
        update_dashboard_stats(window_days=w)

    print(
        f"[night] OK {target_date}: wrote {wrote} forecast_errors and updated dashboard_stats windows={windows}",
        flush=True,
    )


def main() -> None:
    init_db()

    # Use UTC consistently; "yesterday" in UTC.
    target_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

    ok_any = fetch_observations(target_date)
    if not ok_any:
        print(f"[night] No observations fetched for any station for {target_date}; skipping metrics.", flush=True)
        return

    _compute_metrics_for_day(target_date)


if __name__ == "__main__":
    main()
