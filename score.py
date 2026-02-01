# score.py
from __future__ import annotations

import os
from typing import Iterable

from db import build_errors_for_date, update_error_stats


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


def score_day(target_date: str) -> None:
    wrote = build_errors_for_date(target_date)

    if wrote == 0:
        print(f"[score] SKIP {target_date}: no errors written")
        return

    windows = _parse_windows(os.getenv("STATS_WINDOWS_DAYS"))
    for w in windows:
        update_error_stats(window_days=w)

    print(f"[score] OK {target_date}: wrote {wrote} errors and updated stats windows={windows}")
