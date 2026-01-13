# db.py
"""
SQLite schema + upsert helpers for multi-station, multi-source, multi-horizon scoring.

ADDED (non-breaking):
- If env var WEATHER_DB_URL or DATABASE_URL is set to a Postgres URL, use Postgres (Supabase).
- Otherwise, default to SQLite as before.

Tables:
- stations(station_id PK, name, lat, lon)
- sources(source_id PK)
- forecasts(station_id, source_id, forecast_date, target_date, high, low, fetched_at) PK(station_id,source_id,forecast_date,target_date)
- observations(station_id, date, observed_high, observed_low) PK(station_id,date)
- scores(station_id, source_id, forecast_date, target_date, high_error, low_error) PK(station_id,source_id,forecast_date,target_date)
"""

from __future__ import annotations

import os
import sqlite3
from typing import Optional, Any

from config import DB_PATH

# ----------------------------
# Backend selection
# ----------------------------

def _db_url() -> Optional[str]:
    """
    If set, routes DB operations to Postgres/Supabase.
    Prefer WEATHER_DB_URL, fall back to DATABASE_URL.
    """
    return os.getenv("WEATHER_DB_URL") or os.getenv("DATABASE_URL")


def _is_postgres() -> bool:
    url = _db_url()
    return bool(url) and url.startswith(("postgresql://", "postgres://"))


def _pg_connect():
    """
    Connect to Postgres using psycopg (v3) if available, else psycopg2.
    """
    url = _db_url()
    if not url:
        raise RuntimeError("Postgres selected but WEATHER_DB_URL/DATABASE_URL not set")

    try:
        import psycopg  # type: ignore
        # autocommit=False (default) so we can mimic sqlite commit behavior
        return psycopg.connect(url)
    except Exception:
        try:
            import psycopg2  # type: ignore
            return psycopg2.connect(url)
        except Exception as e:
            raise RuntimeError(
                "Postgres URL set, but neither psycopg nor psycopg2 is installed. "
                "Install one: pip install psycopg[binary]  (recommended)  OR  pip install psycopg2-binary"
            ) from e


def get_conn() -> Any:
    """
    Returns a DB-API connection:
    - sqlite3.Connection if using SQLite
    - psycopg/psycopg2 connection if using Postgres
    """
    if _is_postgres():
        return _pg_connect()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _placeholder() -> str:
    """
    SQL parameter placeholder:
    - SQLite uses '?'
    - Postgres (psycopg/psycopg2) uses '%s'
    """
    return "%s" if _is_postgres() else "?"


def _sql(sqlite_sql: str, pg_sql: Optional[str] = None) -> str:
    """
    Helper to choose SQL variant. If pg_sql not provided, reuse sqlite_sql.
    """
    return pg_sql if (_is_postgres() and pg_sql is not None) else sqlite_sql


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # We keep column types compatible with your existing code (dates stored as TEXT).
    # This avoids any changes to other modules.
    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS stations (
        station_id TEXT PRIMARY KEY,
        name TEXT,
        lat REAL,
        lon REAL
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.stations (
        station_id TEXT PRIMARY KEY,
        name TEXT,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION
    )
    """))

    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS sources (
        source_id TEXT PRIMARY KEY
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.sources (
        source_id TEXT PRIMARY KEY
    )
    """))

    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS forecasts (
        station_id TEXT NOT NULL,
        source_id  TEXT NOT NULL,
        forecast_date TEXT NOT NULL,
        target_date   TEXT NOT NULL,
        high REAL,
        low REAL,
        fetched_at TEXT NOT NULL,

        PRIMARY KEY (station_id, source_id, forecast_date, target_date),
        FOREIGN KEY (station_id) REFERENCES stations(station_id),
        FOREIGN KEY (source_id) REFERENCES sources(source_id)
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.forecasts (
        station_id TEXT NOT NULL REFERENCES public.stations(station_id),
        source_id  TEXT NOT NULL REFERENCES public.sources(source_id),
        forecast_date TEXT NOT NULL,
        target_date   TEXT NOT NULL,
        high DOUBLE PRECISION,
        low DOUBLE PRECISION,
        fetched_at TEXT NOT NULL,

        PRIMARY KEY (station_id, source_id, forecast_date, target_date)
    )
    """))

    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS observations (
        station_id TEXT NOT NULL,
        date TEXT NOT NULL,
        observed_high REAL,
        observed_low REAL,

        PRIMARY KEY (station_id, date),
        FOREIGN KEY (station_id) REFERENCES stations(station_id)
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.observations (
        station_id TEXT NOT NULL REFERENCES public.stations(station_id),
        date TEXT NOT NULL,
        observed_high DOUBLE PRECISION,
        observed_low DOUBLE PRECISION,

        PRIMARY KEY (station_id, date)
    )
    """))

    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS scores (
        station_id TEXT NOT NULL,
        source_id  TEXT NOT NULL,
        forecast_date TEXT NOT NULL,
        target_date   TEXT NOT NULL,
        high_error REAL NOT NULL,
        low_error  REAL NOT NULL,

        PRIMARY KEY (station_id, source_id, forecast_date, target_date),
        FOREIGN KEY (station_id) REFERENCES stations(station_id),
        FOREIGN KEY (source_id) REFERENCES sources(source_id)
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.scores (
        station_id TEXT NOT NULL REFERENCES public.stations(station_id),
        source_id  TEXT NOT NULL REFERENCES public.sources(source_id),
        forecast_date TEXT NOT NULL,
        target_date   TEXT NOT NULL,
        high_error DOUBLE PRECISION NOT NULL,
        low_error  DOUBLE PRECISION NOT NULL,

        PRIMARY KEY (station_id, source_id, forecast_date, target_date)
    )
    """))

    conn.commit()
    conn.close()


# ----------------------------
# Upsert helpers
# ----------------------------

def upsert_station(
    station_id: str,
    name: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None
) -> None:
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
            INSERT INTO public.stations (station_id, name, lat, lon)
            VALUES ({ph},{ph},{ph},{ph})
            ON CONFLICT (station_id) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, public.stations.name),
                lat  = COALESCE(EXCLUDED.lat,  public.stations.lat),
                lon  = COALESCE(EXCLUDED.lon,  public.stations.lon)
        """, (station_id, name, lat, lon))
    else:
        cur.execute(f"""
            INSERT INTO stations (station_id, name, lat, lon)
            VALUES ({ph},{ph},{ph},{ph})
            ON CONFLICT(station_id) DO UPDATE SET
                name=COALESCE(excluded.name, stations.name),
                lat=COALESCE(excluded.lat, stations.lat),
                lon=COALESCE(excluded.lon, stations.lon)
        """, (station_id, name, lat, lon))

    conn.commit()
    conn.close()


def upsert_source(source_id: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
            INSERT INTO public.sources (source_id)
            VALUES ({ph})
            ON CONFLICT (source_id) DO NOTHING
        """, (source_id,))
    else:
        cur.execute(f"""
            INSERT INTO sources (source_id)
            VALUES ({ph})
            ON CONFLICT(source_id) DO NOTHING
        """, (source_id,))

    conn.commit()
    conn.close()


def upsert_forecast(
    *,
    station_id: str,
    source_id: str,
    forecast_date: str,
    target_date: str,
    high: float,
    low: float,
    fetched_at: str
) -> None:
    """
    Writes one forecast row for a station+source, for a given forecast_date snapshot,
    targeting a specific target_date (today/tomorrow).
    """
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
            INSERT INTO public.forecasts
              (station_id, source_id, forecast_date, target_date, high, low, fetched_at)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT (station_id, source_id, forecast_date, target_date) DO UPDATE SET
              high=EXCLUDED.high,
              low=EXCLUDED.low,
              fetched_at=EXCLUDED.fetched_at
        """, (station_id, source_id, forecast_date, target_date, high, low, fetched_at))
    else:
        cur.execute(f"""
            INSERT INTO forecasts (station_id, source_id, forecast_date, target_date, high, low, fetched_at)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT(station_id, source_id, forecast_date, target_date) DO UPDATE SET
                high=excluded.high,
                low=excluded.low,
                fetched_at=excluded.fetched_at
        """, (station_id, source_id, forecast_date, target_date, high, low, fetched_at))

    conn.commit()
    conn.close()

from datetime import datetime

def upsert_observation(
    station_id: str,
    obs_date: str,
    observed_high: float,
    observed_low: float
) -> None:
    """
    Writes observed high/low for a station on obs_date (YYYY-MM-DD).
    Adds fetched_at automatically (ISO timestamp).
    """
    fetched_at = datetime.now().isoformat(timespec="seconds")

    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
            INSERT INTO public.observations (station_id, date, observed_high, observed_low, fetched_at)
            VALUES ({ph},{ph},{ph},{ph},{ph})
            ON CONFLICT (station_id, date) DO UPDATE SET
              observed_high=EXCLUDED.observed_high,
              observed_low=EXCLUDED.observed_low,
              fetched_at=EXCLUDED.fetched_at
        """, (station_id, obs_date, observed_high, observed_low, fetched_at))
    else:
        cur.execute(f"""
            INSERT INTO observations (station_id, date, observed_high, observed_low, fetched_at)
            VALUES ({ph},{ph},{ph},{ph},{ph})
            ON CONFLICT(station_id, date) DO UPDATE SET
                observed_high=excluded.observed_high,
                observed_low=excluded.observed_low,
                fetched_at=excluded.fetched_at
        """, (station_id, obs_date, observed_high, observed_low, fetched_at))

    conn.commit()
    conn.close()


def upsert_score(
    *,
    station_id: str,
    source_id: str,
    forecast_date: str,
    target_date: str,
    high_error: float,
    low_error: float
) -> None:
    """
    Writes an error row keyed by station+source+forecast_date+target_date.
    Supports separate evaluation of:
      - same-day (forecast_date == target_date)
      - next-day (forecast_date == target_date - 1)
    """
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
            INSERT INTO public.scores
              (station_id, source_id, forecast_date, target_date, high_error, low_error)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT (station_id, source_id, forecast_date, target_date) DO UPDATE SET
              high_error=EXCLUDED.high_error,
              low_error=EXCLUDED.low_error
        """, (station_id, source_id, forecast_date, target_date, high_error, low_error))
    else:
        cur.execute(f"""
            INSERT INTO scores (station_id, source_id, forecast_date, target_date, high_error, low_error)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT(station_id, source_id, forecast_date, target_date) DO UPDATE SET
                high_error=excluded.high_error,
                low_error=excluded.low_error
        """, (station_id, source_id, forecast_date, target_date, high_error, low_error))
    conn.commit()
    conn.close()
