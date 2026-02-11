# night.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from db import init_db
from cli_observations import fetch_observations

import compute_metrics


def _run_metrics(target_date: str) -> None:
    """
    compute_metrics entrypoint compatibility shim.

    Tries common function names so night.py doesn't need to change
    every time compute_metrics.py evolves.
    """
    for fn_name in ("score_day", "compute_day", "compute_for_date", "run_for_date", "main"):
        fn = getattr(compute_metrics, fn_name, None)
        if callable(fn):
            # main() might be argumentless in some repos; try date first.
            try:
                fn(target_date)  # type: ignore[misc]
            except TypeError:
                fn()  # type: ignore[misc]
            return

    raise RuntimeError(
        "compute_metrics.py has no callable entrypoint found. "
        "Expected one of: score_day/compute_day/compute_for_date/run_for_date/main."
    )


def main() -> None:
    init_db()

    # Night job: ingest yesterday's observed high/low, then compute errors/stats.
    target_date = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()

    ok_any = fetch_observations(target_date)

    if not ok_any:
        print(f"[night] No observations fetched for any station for {target_d_
