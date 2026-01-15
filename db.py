# db.py
from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import datetime
from typing import Optional, Any, Dict, Iterable, List, Tuple

from config import DB_PATH


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


def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

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

    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS forecast_errors (
      forecast_id TEXT PRIMARY KEY,                 -- run_id|station|date|kind
      station_id TEXT NOT NULL,
      source TEXT NOT NULL,
      target_date TEXT NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('high','low')),
      issued_at TEXT NOT NULL,
      lead_hours REAL,
      forecast_f REAL,
      observed_f REAL,
      error_f REAL,
      abs_error_f REAL,
      created_at TEXT NOT NULL
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.forecast_errors (
      forecast_id TEXT PRIMARY KEY,
      station_id TEXT NOT NULL REFERENCES public.locations(station_id) ON DELETE CASCADE,
      source TEXT NOT NULL,
      target_date DATE NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('high','low')),
      issued_at TIMESTAMPTZ NOT NULL,
      lead_hours DOUBLE PRECISION,
      forecast_f DOUBLE PRECISION,
      observed_f DOUBLE PRECISION,
      error_f DOUBLE PRECISION,
      abs_error_f DOUBLE PRECISION,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """))

    cur.execute(_sql("""
    CREATE TABLE IF NOT EXISTS error_stats (
      station_id TEXT,
      source TEXT NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('high','low','both')),
      window_days INTEGER NOT NULL,
      n INTEGER NOT NULL,
      bias REAL,
      mae REAL,
      rmse REAL,
      p10 REAL,
      p50 REAL,
      p90 REAL,
      last_updated TEXT NOT NULL,
      PRIMARY KEY (station_id, source, kind, window_days)
    )
    """, """
    CREATE TABLE IF NOT EXISTS public.error_stats (
      station_id TEXT REFERENCES public.locations(station_id) ON DELETE CASCADE,
      source TEXT NOT NULL,
      kind TEXT NOT NULL CHECK(kind IN ('high','low','both')),
      window_days INTEGER NOT NULL,
      n INTEGER NOT NULL,
      bias DOUBLE PRECISION,
      mae DOUBLE PRECISION,
      rmse DOUBLE PRECISION,
      p10 DOUBLE PRECISION,
      p50 DOUBLE PRECISION,
      p90 DOUBLE PRECISION,
      last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (station_id, source, kind, window_days)
    )
    """))

    if not _is_postgres():
        cur.execute("CREATE INDEX IF NOT EXISTS idx_forecasts_station_date ON forecasts(station_id, target_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_errors_station_date ON forecast_errors(station_id, target_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_errors_source ON forecast_errors(source)")

    conn.commit()
    conn.close()


def upsert_location(station: dict) -> None:
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    station_id = station["station_id"]
    name = station.get("name")
    state = station.get("state")
    tz = station.get("timezone")
    lat = station.get("lat")
    lon = station.get("lon")
    elevation_ft = station.get("elevation_ft")
    is_active = station.get("is_active")

    if _is_postgres():
        cur.execute(f"""
          INSERT INTO public.locations (station_id, name, state, timezone, lat, lon, elevation_ft, is_active)
          VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
          ON CONFLICT (station_id) DO UPDATE SET
            name=COALESCE(EXCLUDED.name, public.locations.name),
            state=COALESCE(EXCLUDED.state, public.locations.state),
            timezone=COALESCE(EXCLUDED.timezone, public.locations.timezone),
            lat=COALESCE(EXCLUDED.lat, public.locations.lat),
            lon=COALESCE(EXCLUDED.lon, public.locations.lon),
            elevation_ft=COALESCE(EXCLUDED.elevation_ft, public.locations.elevation_ft),
            is_active=COALESCE(EXCLUDED.is_active, public.locations.is_active)
        """, (station_id, name, state, tz, lat, lon, elevation_ft, is_active))
    else:
        cur.execute(f"""
          INSERT INTO locations (station_id, name, state, timezone, lat, lon, elevation_ft, is_active)
          VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
          ON CONFLICT(station_id) DO UPDATE SET
            name=COALESCE(excluded.name, locations.name),
            state=COALESCE(excluded.state, locations.state),
            timezone=COALESCE(excluded.timezone, locations.timezone),
            lat=COALESCE(excluded.lat, locations.lat),
            lon=COALESCE(excluded.lon, locations.lon),
            elevation_ft=COALESCE(excluded.elevation_ft, locations.elevation_ft),
            is_active=COALESCE(excluded.is_active, locations.is_active)
        """, (station_id, name, state, tz, lat, lon, elevation_ft,
              None if is_active is None else int(bool(is_active))))

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
    fetched_at = _now_iso()
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


def get_or_create_forecast_run(source: str, issued_at: str, meta_json: str = "{}") -> str:
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
          INSERT INTO public.forecast_runs (source, issued_at, meta)
          VALUES ({ph},{ph},{ph}::jsonb)
          ON CONFLICT (source, issued_at) DO UPDATE SET fetched_at=now()
          RETURNING run_id
        """, (source, issued_at, meta_json))
        run_id = str(cur.fetchone()[0])
        conn.commit()
        conn.close()
        return run_id

    run_id = str(uuid.uuid4())
    fetched_at = _now_iso()
    cur.execute(f"""
      INSERT OR IGNORE INTO forecast_runs (run_id, source, issued_at, fetched_at, meta_json)
      VALUES ({ph},{ph},{ph},{ph},{ph})
    """, (run_id, source, issued_at, fetched_at, meta_json))
    cur.execute(f"SELECT run_id FROM forecast_runs WHERE source={ph} AND issued_at={ph}", (source, issued_at))
    run_id2 = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return str(run_id2)


def upsert_forecast_value(
    *,
    run_id: str,
    station_id: str,
    target_date: str,
    kind: str,
    value_f: float,
    lead_hours: Optional[float] = None,
    extras: Optional[Dict[str, Any]] = None,
) -> None:
    extras = extras or {}
    created_at = _now_iso()

    dewpoint_f = extras.get("dewpoint_f")
    humidity_pct = extras.get("humidity_pct")
    wind_speed_mph = extras.get("wind_speed_mph")
    wind_dir_deg = extras.get("wind_dir_deg")
    cloud_cover_pct = extras.get("cloud_cover_pct")
    precip_prob_pct = extras.get("precip_prob_pct")

    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    if _is_postgres():
        cur.execute(f"""
          INSERT INTO public.forecasts
            (run_id, station_id, target_date, kind, value_f, lead_hours,
             dewpoint_f, humidity_pct, wind_speed_mph, wind_dir_deg, cloud_cover_pct, precip_prob_pct)
          VALUES
            ({ph}::uuid,{ph},{ph}::date,{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
          ON CONFLICT (run_id, station_id, target_date, kind) DO UPDATE SET
            value_f=EXCLUDED.value_f,
            lead_hours=COALESCE(EXCLUDED.lead_hours, public.forecasts.lead_hours),
            dewpoint_f=COALESCE(EXCLUDED.dewpoint_f, public.forecasts.dewpoint_f),
            humidity_pct=COALESCE(EXCLUDED.humidity_pct, public.forecasts.humidity_pct),
            wind_speed_mph=COALESCE(EXCLUDED.wind_speed_mph, public.forecasts.wind_speed_mph),
            wind_dir_deg=COALESCE(EXCLUDED.wind_dir_deg, public.forecasts.wind_dir_deg),
            cloud_cover_pct=COALESCE(EXCLUDED.cloud_cover_pct, public.forecasts.cloud_cover_pct),
            precip_prob_pct=COALESCE(EXCLUDED.precip_prob_pct, public.forecasts.precip_prob_pct)
        """, (
            run_id, station_id, target_date, kind, value_f, lead_hours,
            dewpoint_f, humidity_pct, wind_speed_mph, wind_dir_deg, cloud_cover_pct, precip_prob_pct
        ))
    else:
        cur.execute(f"""
          INSERT INTO forecasts
            (run_id, station_id, target_date, kind, value_f, lead_hours,
             dewpoint_f, humidity_pct, wind_speed_mph, wind_dir_deg, cloud_cover_pct, precip_prob_pct, created_at)
          VALUES
            ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
          ON CONFLICT(run_id, station_id, target_date, kind) DO UPDATE SET
            value_f=excluded.value_f,
            lead_hours=COALESCE(excluded.lead_hours, forecasts.lead_hours),
            dewpoint_f=COALESCE(excluded.dewpoint_f, forecasts.dewpoint_f),
            humidity_pct=COALESCE(excluded.humidity_pct, forecasts.humidity_pct),
            wind_speed_mph=COALESCE(excluded.wind_speed_mph, forecasts.wind_speed_mph),
            wind_dir_deg=COALESCE(excluded.wind_dir_deg, forecasts.wind_dir_deg),
            cloud_cover_pct=COALESCE(excluded.cloud_cover_pct, forecasts.cloud_cover_pct),
            precip_prob_pct=COALESCE(excluded.precip_prob_pct, forecasts.precip_prob_pct)
        """, (
            run_id, station_id, target_date, kind, value_f, lead_hours,
            dewpoint_f, humidity_pct, wind_speed_mph, wind_dir_deg, cloud_cover_pct, precip_prob_pct, created_at
        ))

    conn.commit()
    conn.close()


def build_errors_for_date(target_date: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    # load observations
    cur.execute(f"SELECT station_id, observed_high, observed_low FROM observations WHERE date={ph}", (target_date,))
    obs_rows = cur.fetchall()
    if not obs_rows:
        conn.close()
        return 0

    wrote = 0
    for station_id, oh, ol in obs_rows:
        # fetch all forecasts for that station/date
        cur.execute(f"""
          SELECT f.run_id, r.source, r.issued_at, f.kind, f.value_f, f.lead_hours
          FROM forecasts f
          JOIN forecast_runs r ON r.run_id = f.run_id
          WHERE f.station_id={ph} AND f.target_date={ph}
        """, (station_id, target_date))
        for run_id, source, issued_at, kind, forecast_f, lead_hours in cur.fetchall():
            if forecast_f is None:
                continue
            observed_f = float(oh) if kind == "high" else float(ol)
            error_f = float(forecast_f) - observed_f
            abs_error_f = abs(error_f)
            forecast_id = f"{run_id}|{station_id}|{target_date}|{kind}"
            created_at = _now_iso()

            if _is_postgres():
                cur.execute(f"""
                  INSERT INTO public.forecast_errors
                    (forecast_id, station_id, source, target_date, kind, issued_at, lead_hours,
                     forecast_f, observed_f, error_f, abs_error_f)
                  VALUES ({ph},{ph},{ph},{ph}::date,{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                  ON CONFLICT (forecast_id) DO NOTHING
                """, (forecast_id, station_id, source, target_date, kind, issued_at, lead_hours,
                      forecast_f, observed_f, error_f, abs_error_f))
            else:
                cur.execute(f"""
                  INSERT INTO forecast_errors
                    (forecast_id, station_id, source, target_date, kind, issued_at, lead_hours,
                     forecast_f, observed_f, error_f, abs_error_f, created_at)
                  VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
                  ON CONFLICT(forecast_id) DO NOTHING
                """, (forecast_id, station_id, source, target_date, kind, issued_at, lead_hours,
                      forecast_f, observed_f, error_f, abs_error_f, created_at))
            wrote += 1

    conn.commit()
    conn.close()
    return wrote


def _percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return float(d0 + d1)


def update_error_stats(*, window_days: int, station_id: Optional[str] = None) -> None:
    conn = get_conn()
    cur = conn.cursor()
    ph = _placeholder()

    where_station = ""
    params: List[Any] = []
    if station_id:
        where_station = f" AND station_id={ph}"
        params.append(station_id)

    cur.execute(_sql(
        f"""
        SELECT source, kind, error_f, abs_error_f
        FROM forecast_errors
        WHERE target_date >= date('now', '-' || {ph} || ' day'){where_station}
        """,
        f"""
        SELECT source, kind, error_f, abs_error_f
        FROM public.forecast_errors
        WHERE target_date >= (now()::date - ({ph}::int * interval '1 day')){where_station}
        """,
    ), [window_days] + params)

    rows = cur.fetchall()

    # group by source + kind
    by: Dict[Tuple[str, str], List[Tuple[float, float]]] = {}
    for source, kind, e, ae in rows:
        if e is None or ae is None:
            continue
        by.setdefault((str(source), str(kind)), []).append((float(e), float(ae)))

    now_ts = _now_iso()

    for (source, kind), vals in by.items():
        n = len(vals)
        errors = [v[0] for v in vals]
        abs_errors = [v[1] for v in vals]

        bias = sum(errors) / n
        mae = sum(abs_errors) / n
        rmse = (sum((x * x) for x in errors) / n) ** 0.5

        se = sorted(errors)
        p10 = _percentile(se, 0.10)
        p50 = _percentile(se, 0.50)
        p90 = _percentile(se, 0.90)

        if _is_postgres():
            cur.execute(f"""
              INSERT INTO public.error_stats
                (station_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, last_updated)
              VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
              ON CONFLICT (station_id, source, kind, window_days) DO UPDATE SET
                n=EXCLUDED.n, bias=EXCLUDED.bias, mae=EXCLUDED.mae, rmse=EXCLUDED.rmse,
                p10=EXCLUDED.p10, p50=EXCLUDED.p50, p90=EXCLUDED.p90, last_updated=EXCLUDED.last_updated
            """, (station_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, now_ts))
        else:
            cur.execute(f"""
              INSERT INTO error_stats
                (station_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, last_updated)
              VALUES ({ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph},{ph})
              ON CONFLICT(station_id, source, kind, window_days) DO UPDATE SET
                n=excluded.n, bias=excluded.bias, mae=excluded.mae, rmse=excluded.rmse,
                p10=excluded.p10, p50=excluded.p50, p90=excluded.p90, last_updated=excluded.last_updated
            """, (station_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, now_ts))

    # both = avg(high, low) per row (quick & consistent with your current combined MAE)
    for source in sorted(set(k[0] for k in by.keys())):
        highs = by.get((source, "high"), [])
        lows = by.get((source, "low"), [])
        if not highs or not lows:
            continue
        n = min(len(highs), len(lows))
        # combine by pairing most recent n rows by insertion order is not available here;
        # use aggregate combined MAE as mean of high+low MAE (cheap and stable).
        mae_high = sum(v[1] for v in highs) / len(highs)
        mae_low = sum(v[1] for v in lows) / len(lows)
        mae_both = (mae_high + mae_low) / 2.0

        if _is_postgres():
            cur.execute(f"""
              INSERT INTO public.error_stats
                (station_id, source, kind, window_days, n, mae, last_updated)
              VALUES ({ph},{ph},'both',{ph},{ph},{ph},{ph})
              ON CONFLICT (station_id, source, kind, window_days) DO UPDATE SET
                n=EXCLUDED.n, mae=EXCLUDED.mae, last_updated=EXCLUDED.last_updated
            """, (station_id, source, window_days, n, mae_both, now_ts))
        else:
            cur.execute(f"""
              INSERT INTO error_stats
                (station_id, source, kind, window_days, n, mae, last_updated)
              VALUES ({ph},{ph},'both',{ph},{ph},{ph},{ph})
              ON CONFLICT(station_id, source, kind, window_days) DO UPDATE SET
                n=excluded.n, mae=excluded.mae, last_updated=excluded.last_updated
            """, (station_id, source, window_days, n, mae_both, now_ts))

    conn.commit()
    conn.close()
