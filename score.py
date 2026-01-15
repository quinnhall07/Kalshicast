# score.py
from __future__ import annotations

from datetime import date as ddate, timedelta

from db import build_errors_for_date, update_error_stats


def score_day(target_date: str) -> None:
    wrote = build_errors_for_date(target_date)

    if wrote == 0:
        print(f"[score] SKIP {target_date}: no errors written")
        return

    for w in (2, 3, 7, 31):
        update_error_stats(window_days=w)

    print(f"[score] OK {target_date}: wrote {wrote} errors and updated stats")
