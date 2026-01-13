# reset_db.py
from __future__ import annotations

from db import get_conn


def _is_postgres_conn(conn) -> bool:
    # psycopg/psycopg2 connections won't be sqlite3.Connection
    try:
        import sqlite3
        return not isinstance(conn, sqlite3.Connection)
    except Exception:
        return True


def reset_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    if _is_postgres_conn(conn):
        # Supabase/Postgres
        # TRUNCATE is fast and also clears dependent rows with CASCADE
        cur.execute("""
            TRUNCATE TABLE
              scores,
              forecasts,
              observations,
              sources,
              stations
            CASCADE;
        """)
        conn.commit()
        conn.close()
        print("Supabase/Postgres database cleared: scores, forecasts, observations, sources, stations")
        return

    # SQLite
    # Delete children first to satisfy FK constraints
    cur.execute("DELETE FROM scores;")
    cur.execute("DELETE FROM forecasts;")
    cur.execute("DELETE FROM observations;")
    cur.execute("DELETE FROM sources;")
    cur.execute("DELETE FROM stations;")
    conn.commit()
    conn.close()
    print("SQLite database cleared: scores, forecasts, observations, sources, stations")


if __name__ == "__main__":
    reset_db()
