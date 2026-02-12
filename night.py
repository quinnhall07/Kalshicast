# night.py
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from db import init_db
from cli_observations import fetch_observations
import compute_metrics


def _run_metrics(target_date: str) -> None:
    """
    Call compute_metrics entrypoint without tightly coupling to one function name.
    """
    for fn_name in (
        "score_day",
        "compute_day",
        "compute_for_date",
        "run_for_date",
        "main",
    ):
        fn = getattr(compute_metrics, fn_name, None)
        if callable(fn):
            try:
                fn(target_date)  # preferred signature
            except TypeError:
                fn()  # fallback if main() has no args
            return

    raise RuntimeError(
        "compute_metrics.py has no valid entrypoint "
        "(expected score_day/compute_day/compute_for_date/run_for_date/main)."
    )


def main() -> None:
    init_db()

    # Always operate in UTC for scheduling consistency
    target_date = (
        datetime.now(timezone.utc).date() - timedelta(days=1)
    ).isoformat()

    ok_any = fetch_observations(target_date)

    if not ok_any:
        print(
            f"[night] No observations fetched for any station for {target_date}; skipping metrics.",
            flush=True,
        )
        return

    try:
        _run_metrics(target_date)
        print(
            f"[night] OK {target_date}: observations ingested; metrics computed.",
            flush=True,
        )
    except Exception as e:
        print(
            f"[night] FAIL {target_date}: metrics computation error: {e}",
            flush=True,
        )
        raise


if __name__ == "__main__":
    main()
