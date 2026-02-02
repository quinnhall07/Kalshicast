# reset_db.py
from __future__ import annotations

from db import get_conn


def reset_db() -> None:
    # Truncate only tables that actually exist (avoids failures when optional tables aren't migrated yet)
    candidates = [
        # Optional / newer
        "public.forecast_revisions",
        "public.observations_v2",
        "public.observation_runs",
        "public.observations_latest",
        "public.forecasts_hourly",
        "public.forecast_extras_hourly",
        # Core
        "public.forecast_errors",
        "public.error_stats",
        "public.forecasts",
        "public.forecast_runs",
        "public.observations",
        "public.locations",
    ]

    with get_conn() as conn:
        with conn.cursor() as cur:
            existing: list[str] = []
            for t in candidates:
                # to_regclass returns NULL if the relation doesn't exist
                cur.execute("select to_regclass(%s)", (t,))
                if cur.fetchone()[0] is not None:
                    existing.append(t)

            if not existing:
                print("No known tables exist; nothing to reset.")
                return

            sql = "TRUNCATE TABLE " + ", ".join(existing) + " RESTART IDENTITY CASCADE;"
            cur.execute(sql)

        conn.commit()

    print(f"Postgres reset complete. Truncated {len(existing)} tables.")


if __name__ == "__main__":
    reset_db()
