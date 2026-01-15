# db.py
from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import Optional, Any, Dict

from config import DB_PATH

# ----------------------------
# Backend selection
# ----------------------------

def _db_url() -> Optional[str]:
    return os.getenv("WEATHER_DB_URL") or os.getenv("DATABASE_URL")


def _is_postgres() -> bool:
    url = _db_url()
    return bool(url) and url.startswith(("postgresql://", "postgres://"))


def _pg_connect():
    url = _db_url()
    if not url:
        raise RuntimeError("Postgres selected but WEATHER_DB_URL/DATABASE_URL not set")

    try:
        import psycopg  # type: ignore
        return psycopg.connect(url)
    except ImportError:
        import psycopg2  # type: ignore
        return psycopg2.connect(url)


def get_conn() -> Any:
    if _is_postgres():
        return _pg_connect()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _placeholder() -> str:
    return "%s" if _is_postgres() else "?"


def _sql(sqlite_sql: str, pg_sql: Optional[str] = None) -> str:
    return pg_sql if (_is_postgres() and pg_sql is not None) else sqlite_sql


# ----------------------------
# Schema (minimal foundation)
# ----------------------------

def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # Locations (stations)
    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS locations (
        station_id TEXT PRIMARY KEY,
        name TEXT,
        state TEXT,
        timezone TEXT,
        lat REAL,
        lon REAL,
        elevation_ft REAL,
        is_active INTEGER DEFAULT 1
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.locations (
        station_id TEXT PRIMARY KEY,
        name TEXT,
        state TEXT,
        timezone TEXT,
        lat DOUBLE PRECISION,
        lon DOUBLE PRECISION,
        elevation_ft DOUBLE PRECISION,
        is_active BOOLEAN DEFAULT true
    )
    """))

    # Sources registry
    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS sources (
        source_id TEXT PRIMARY KEY
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.sources (
        source_id TEXT PRIMARY KEY
    )
    """))

    # Forecast runs (issue times)
    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS forecast_runs (
        run_id TEXT PRIMARY KEY,
        source TEXT NOT NULL,
        issued_at TEXT NOT NULL,
        fetched_at TEXT NOT NULL,
        meta_json TEXT DEFAULT '{}',
        UNIQUE (source, issued_at)
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.forecast_runs (
        run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        source TEXT NOT NULL,
        issued_at TIMESTAMPTZ NOT NULL,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        meta JSONB NOT NULL DEFAULT '{}'::jsonb,
        UNIQUE (source, issued_at)
    )
    """))

    # Forecast values (normalized high/low)
    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS forecasts (
        run_id TEXT NOT NULL,
        station_id TEXT NOT NULL,
        target_date TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('high','low')),
        value_f REAL,
        lead_hours REAL,

        dewpoint_f REAL,
        humidity_pct REAL,
        wind_speed_mph REAL,
        wind_dir_deg REAL,
        cloud_cover_pct REAL,
        precip_prob_pct REAL,

        created_at TEXT NOT NULL,

        PRIMARY KEY (run_id, station_id, target_date, kind),
        FOREIGN KEY (station_id) REFERENCES locations(station_id)
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.forecasts (
        run_id UUID NOT NULL REFERENCES public.forecast_runs(run_id) ON DELETE CASCADE,
        station_id TEXT NOT NULL REFERENCES public.locations(station_id) ON DELETE CASCADE,
        target_date DATE NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('high','low')),
        value_f DOUBLE PRECISION,
        lead_hours DOUBLE PRECISION,

        dewpoint_f DOUBLE PRECISION,
        humidity_pct DOUBLE PRECISION,
        wind_speed_mph DOUBLE PRECISION,
        wind_dir_deg DOUBLE PRECISION,
        cloud_cover_pct DOUBLE PRECISION,
        precip_prob_pct DOUBLE PRECISION,

        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

        PRIMARY KEY (run_id, station_id, target_date, kind)
    )
    """))

    # Observations (daily truth) — includes fetched_at (your code depends on it)
    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS observations (
        station_id TEXT NOT NULL,
        date TEXT NOT NULL,
        observed_high REAL,
        observed_low REAL,
        issued_at TEXT,
        fetched_at TEXT NOT NULL,
        raw_text TEXT,
        source TEXT DEFAULT 'nws_station_obs',

        PRIMARY KEY (station_id, date),
        FOREIGN KEY (station_id) REFERENCES locations(station_id)
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.observations (
        station_id TEXT NOT NULL REFERENCES public.locations(station_id) ON DELETE CASCADE,
        date DATE NOT NULL,
        observed_high DOUBLE PRECISION,
        observed_low DOUBLE PRECISION,
        issued_at TIMESTAMPTZ,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        raw_text TEXT,
        source TEXT NOT NULL DEFAULT 'nws_station_obs',

        PRIMARY KEY (station_id, date)
    )
    """))

    conn.commit()
    conn.close()


# ----------------------------
# Upserts (backward-compatible names)
# ----------------------------

def upsert_station(
    station_id: str,
    name: Optional[str] = None,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    timezone: Optional[str] = None,
    state: Optional[str] = None,
    elevation_ft: Optional[float] = None,
    is_active: Optional[bool] = None,
) -> None:
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
            INSERT INTO public.locations
              (station_id, name, state, timezone, lat, lon, elevation_ft, is_active)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT (station_id) DO UPDATE SET
              name = COALESCE(EXCLUDED.name, public.locations.name),
              state = COALESCE(EXCLUDED.state, public.locations.state),
              timezone = COALESCE(EXCLUDED.timezone, public.locations.timezone),
              lat = COALESCE(EXCLUDED.lat, public.locations.lat),
              lon = COALESCE(EXCLUDED.lon, public.locations.lon),
              elevation_ft = COALESCE(EXCLUDED.elevation_ft, public.locations.elevation_ft),
              is_active = COALESCE(EXCLUDED.is_active, public.locations.is_active)
        """, (station_id, name, state, timezone, lat, lon, elevation_ft, is_active))
    else:
        cur.execute(f"""
            INSERT INTO locations
              (station_id, name, state, timezone, lat, lon, elevation_ft, is_active)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT(station_id) DO UPDATE SET
              name=COALESCE(excluded.name, locations.name),
              state=COALESCE(excluded.state, locations.state),
              timezone=COALESCE(excluded.timezone, locations.timezone),
              lat=COALESCE(excluded.lat, locations.lat),
              lon=COALESCE(excluded.lon, locations.lon),
              elevation_ft=COALESCE(excluded.elevation_ft, locations.elevation_ft),
              is_active=COALESCE(excluded.is_active, locations.is_active)
        """, (station_id, name, state, timezone, lat, lon, elevation_ft,
              None if is_active is None else int(is_active)))

    conn.commit()
    conn.close()


def upsert_source(source_id: str) -> None:
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"INSERT INTO public.sources (source_id) VALUES ({ph}) ON CONFLICT DO NOTHING", (source_id,))
    else:
        cur.execute(f"INSERT INTO sources (source_id) VALUES ({ph}) ON CONFLICT DO NOTHING", (source_id,))

    conn.commit()
    conn.close()


def upsert_observation(
    station_id: str,
    obs_date: str,
    observed_high: float,
    observed_low: float,
    issued_at: Optional[str] = None,
    raw_text: Optional[str] = None,
    source: str = "nws_station_obs",
) -> None:
    fetched_at = datetime.now().isoformat(timespec="seconds")

    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
            INSERT INTO public.observations
              (station_id, date, observed_high, observed_low, issued_at, fetched_at, raw_text, source)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT (station_id, date) DO UPDATE SET
              observed_high=EXCLUDED.observed_high,
              observed_low=EXCLUDED.observed_low,
              issued_at=COALESCE(EXCLUDED.issued_at, public.observations.issued_at),
              fetched_at=EXCLUDED.fetched_at,
              raw_text=COALESCE(EXCLUDED.raw_text, public.observations.raw_text),
              source=EXCLUDED.source
        """, (station_id, obs_date, observed_high, observed_low, issued_at, fetched_at, raw_text, source))
    else:
        cur.execute(f"""
            INSERT INTO observations
              (station_id, date, observed_high, observed_low, issued_at, fetched_at, raw_text, source)
            VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
            ON CONFLICT(station_id, date) DO UPDATE SET
              observed_high=excluded.observed_high,
              observed_low=excluded.observed_low,
              issued_at=COALESCE(excluded.issued_at, observations.issued_at),
              fetched_at=excluded.fetched_at,
              raw_text=COALESCE(excluded.raw_text, observations.raw_text),
              source=excluded.source
        """, (station_id, obs_date, observed_high, observed_low, issued_at, fetched_at, raw_text, source))

    conn.commit()
    conn.close()
