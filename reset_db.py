# reset_db.py
from __future__ import annotations

from db import get_conn, _is_postgres


def reset_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    if _is_postgres():
        cur.execute("""
            TRUNCATE TABLE
              forecast_errors,
              error_stats,
              forecasts,
              forecast_runs,
              observations,
              locations
            CASCADE;
        """)
        conn.commit()
        conn.close()
        print("Postgres reset complete.")
        return

    # SQLite
    cur.execute("DELETE FROM forecast_errors;")
    cur.execute("DELETE FROM error_stats;")
    cur.execute("DELETE FROM forecasts;")
    cur.execute("DELETE FROM forecast_runs;")
    cur.execute("DELETE FROM observations;")
    cur.execute("DELETE FROM locations;")
    conn.commit()
    conn.close()
    print("SQLite reset complete.")


if __name__ == "__main__":
    reset_db()
