# night.py
from __future__ import annotations

from datetime import date, timedelta

from db import init_db
from cli_observations import fetch_observations
from compute_metrics import score_day


def main() -> None:
    init_db()

    target_date = (date.today() - timedelta(days=1)).isoformat()
    ok_any = fetch_observations(target_date)

    if ok_any:
        score_day(target_date)
    else:
        print("[night] No observations fetched for any station; skipping scoring.")


if __name__ == "__main__":
    main()

