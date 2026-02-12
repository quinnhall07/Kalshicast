# db.py (Supabase-only) — REFAC (no legacy) :contentReference[oaicite:0]{index=0}
from __future__ import annotations
import math
import os
from typing import Any, Dict, List, Optional, Tuple


# -------------------------
# Connection / init
# -------------------------

def _db_url() -> str:
    url = os.getenv("WEATHER_DB_URL") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("Missing WEATHER_DB_URL (or DATABASE_URL) for Supabase Postgres.")
    return url


def get_conn():
    url = _db_url()
    try:
        import psycopg  # type: ignore
        return psycopg.connect(url)
    except ImportError:
        import psycopg2  # type: ignore
        return psycopg2.connect(url)


def init_db() -> None:
    # Schema created via migrations. This just validates connectivity.
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("select 1;")


# -------------------------
# Locations
# -------------------------

def upsert_location(station: dict) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.locations
                  (station_id, name, state, timezone, lat, lon, elevation_ft, is_active)
                values
                  (%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict (station_id) do update set
                  name=coalesce(excluded.name, public.locations.name),
                  state=coalesce(excluded.state, public.locations.state),
                  timezone=coalesce(excluded.timezone, public.locations.timezone),
                  lat=coalesce(excluded.lat, public.locations.lat),
                  lon=coalesce(excluded.lon, public.locations.lon),
                  elevation_ft=coalesce(excluded.elevation_ft, public.locations.elevation_ft),
                  is_active=coalesce(excluded.is_active, public.locations.is_active)
                """,
                (
                    station["station_id"],
                    station.get("name"),
                    station.get("state"),
                    station.get("timezone"),
                    station.get("lat"),
                    station.get("lon"),
                    station.get("elevation_ft"),
                    station.get("is_active", True),
                ),
            )
        conn.commit()


# -------------------------
# Forecast runs
# -------------------------

def get_or_create_forecast_run(*, source: str, issued_at: str, fetched_at: Optional[str] = None, conn=None) -> Any:
    """
    Requires: public.forecast_runs(run_id uuid pk, source text, issued_at timestamptz, fetched_at timestamptz)
    Recommended: UNIQUE (source, issued_at) for idempotency.
    """
    owns = False
    if conn is None:
        conn = get_conn()
        owns = True

    try:
        with conn.cursor() as cur:
            if fetched_at:
                cur.execute(
                    """
                    insert into public.forecast_runs (source, issued_at, fetched_at)
                    values (%s, %s::timestamptz, %s::timestamptz)
                    on conflict (source, issued_at) do update set
                      fetched_at = excluded.fetched_at
                    returning run_id
                    """,
                    (source, issued_at, fetched_at),
                )
            else:
                cur.execute(
                    """
                    insert into public.forecast_runs (source, issued_at)
                    values (%s, %s::timestamptz)
                    on conflict (source, issued_at) do update set
                      source = excluded.source
                    returning run_id
                    """,
                    (source, issued_at),
                )
            run_id = cur.fetchone()[0]
        if owns:
            conn.commit()
        return run_id
    finally:
        if owns:
            conn.close()


# -------------------------
# Daily forecasts (condensed)
# -------------------------

def bulk_upsert_forecasts_daily(conn, rows: list[dict]) -> int:
    """
    Upsert into forecasts_daily.

    Schema columns:
      lead_hours_high, lead_hours_low

    Backward-compatible input keys accepted:
      lead_hours_high OR lead_high_hours
      lead_hours_low  OR lead_low_hours
    """
    if not rows:
        return 0

    sql = """
    insert into public.forecasts_daily (
      run_id, station_id, target_date,
      high_f, low_f,
      lead_hours_high, lead_hours_low
    )
    values (%s,%s,%s,%s,%s,%s,%s)
    on conflict (run_id, station_id, target_date) do update set
      high_f = excluded.high_f,
      low_f  = excluded.low_f,
      lead_hours_high = excluded.lead_hours_high,
      lead_hours_low  = excluded.lead_hours_low
    ;
    """

    prepared = []
    for r in rows:
        # Accept either naming convention from upstream
        lead_hi = r.get("lead_hours_high", r.get("lead_high_hours"))
        lead_lo = r.get("lead_hours_low", r.get("lead_low_hours"))

        prepared.append((
            r["run_id"],
            r["station_id"],
            r["target_date"],
            r.get("high_f"),
            r.get("low_f"),
            lead_hi,
            lead_lo,
        ))

    with conn.cursor() as cur:
        cur.executemany(sql, prepared)

    conn.commit()
    return len(prepared)


# -------------------------
# Hourly forecast extras (ML) — no extras json, no created_at
# -------------------------

def bulk_upsert_forecast_extras_hourly(conn, rows: List[dict]) -> int:
    """
    Requires: public.forecast_extras_hourly
      (run_id uuid, station_id text, valid_time timestamptz,
       temperature_f double precision,
       dewpoint_f double precision,
       humidity_pct double precision,
       wind_speed_mph double precision,
       wind_dir_deg double precision,
       cloud_cover_pct double precision,
       precip_prob_pct double precision,
       primary key (run_id, station_id, valid_time))

    rows items:
      run_id, station_id, valid_time (ISO or datetime),
      temperature_f, dewpoint_f, humidity_pct,
      wind_speed_mph, wind_dir_deg, cloud_cover_pct, precip_prob_pct
    """
    if not rows:
        return 0

    sql = """
    insert into public.forecast_extras_hourly (
      run_id, station_id, valid_time,
      temperature_f, dewpoint_f, humidity_pct,
      wind_speed_mph, wind_dir_deg, cloud_cover_pct,
      precip_prob_pct
    ) values (
      %(run_id)s, %(station_id)s, %(valid_time)s::timestamptz,
      %(temperature_f)s, %(dewpoint_f)s, %(humidity_pct)s,
      %(wind_speed_mph)s, %(wind_dir_deg)s, %(cloud_cover_pct)s,
      %(precip_prob_pct)s
    )
    on conflict (run_id, station_id, valid_time)
    do update set
      temperature_f = excluded.temperature_f,
      dewpoint_f = excluded.dewpoint_f,
      humidity_pct = excluded.humidity_pct,
      wind_speed_mph = excluded.wind_speed_mph,
      wind_dir_deg = excluded.wind_dir_deg,
      cloud_cover_pct = excluded.cloud_cover_pct,
      precip_prob_pct = excluded.precip_prob_pct;
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    return len(rows)


# -------------------------
# Observations (new; renamed from observations_v2)
# -------------------------

def get_or_create_observation_run(*, run_issued_at: str, conn=None) -> Any:
    """
    Requires: public.observation_runs(run_id uuid pk, run_issued_at timestamptz unique)
    """
    owns = False
    if conn is None:
        conn = get_conn()
        owns = True

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.observation_runs (run_issued_at)
                values (%s::timestamptz)
                on conflict (run_issued_at) do update set
                  run_issued_at = excluded.run_issued_at
                returning run_id
                """,
                (run_issued_at,),
            )
            run_id = cur.fetchone()[0]
        if owns:
            conn.commit()
        return run_id
    finally:
        if owns:
            conn.close()


def upsert_observation(
    *,
    run_id: Any,
    station_id: str,
    date: str,
    observed_high: float | None,
    observed_low: float | None,
    source: str,
    flagged_raw_text: str | None = None,
    flagged_reason: str | None = None,
    raw_text: str | None = None,  # legacy alias -> flagged_raw_text
    conn=None,
    **_ignored,  # swallow stale kwargs safely
) -> None:
    """
    Upsert into observations.

    Backward compatible:
      - accepts raw_text=... (legacy) and maps it to flagged_raw_text
      - ignores unexpected kwargs to prevent pipeline breakage

    Schema (authoritative):
      observations(run_id, station_id, date, observed_high, observed_low, source, flagged_raw_text, flagged_reason)
    """
    frt = flagged_raw_text
    if frt is None and raw_text is not None:
        frt = raw_text

    if frt is not None and not str(frt).strip():
        frt = None
    if flagged_reason is not None and not str(flagged_reason).strip():
        flagged_reason = None

    sql = """
    INSERT INTO observations (
      run_id,
      station_id,
      date,
      observed_high,
      observed_low,
      source,
      flagged_raw_text,
      flagged_reason
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id, station_id, date)
    DO UPDATE SET
      observed_high     = EXCLUDED.observed_high,
      observed_low      = EXCLUDED.observed_low,
      source            = EXCLUDED.source,
      flagged_raw_text  = COALESCE(EXCLUDED.flagged_raw_text, observations.flagged_raw_text),
      flagged_reason    = COALESCE(EXCLUDED.flagged_reason, observations.flagged_reason)
    """

    if conn is None:
        from db import get_conn  # if get_conn is in this module, you can remove this import and call directly
        with get_conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    sql,
                    (run_id, station_id, date, observed_high, observed_low, source, frt, flagged_reason),
                )
            c.commit()
    else:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (run_id, station_id, date, observed_high, observed_low, source, frt, flagged_reason),
            )


def _latest_observation_run_id(conn) -> Optional[Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select run_id
            from public.observation_runs
            order by run_issued_at desc
            limit 1
            """
        )
        row = cur.fetchone()
        return row[0] if row else None


# -------------------------
# Forecast errors (compact; keyed by run ids)
# -------------------------

def build_forecast_errors_for_date(*, target_date: str, conn=None) -> int:
    """
    Populate forecast_errors for a single date using your schema:

      forecast_errors(
        forecast_run_id uuid,
        observation_run_id uuid,
        station_id text,
        target_date date,
        ae_high real,
        ae_low real,
        mae real,
        created_at timestamptz default now()
      )

    Notes:
    - Uses the *latest* observation per (station_id, date) by observation_runs.run_issued_at.
    - Writes one row per (forecast_run_id, observation_run_id, station_id, target_date) when both sides exist.
    """
    owns = conn is None
    if owns:
        from db import get_conn
        conn = get_conn()

    try:
        sql = """
        WITH obs_latest AS (
          SELECT DISTINCT ON (o.station_id, o.date)
            o.station_id,
            o.date,
            o.run_id AS observation_run_id,
            o.observed_high,
            o.observed_low
          FROM observations o
          JOIN observation_runs r ON r.run_id = o.run_id
          WHERE o.date = %s::date
          ORDER BY o.station_id, o.date, r.run_issued_at DESC
        ),
        pairs AS (
          SELECT
            fr.run_id AS forecast_run_id,
            ol.observation_run_id,
            d.station_id,
            d.target_date,
            d.high_f,
            d.low_f,
            ol.observed_high,
            ol.observed_low
          FROM forecasts_daily d
          JOIN forecast_runs fr ON fr.run_id = d.run_id
          JOIN obs_latest ol
            ON ol.station_id = d.station_id
           AND ol.date = d.target_date
          WHERE d.target_date = %s::date
        ),
        computed AS (
          SELECT
            forecast_run_id,
            observation_run_id,
            station_id,
            target_date,
            CASE
              WHEN high_f IS NULL OR observed_high IS NULL THEN NULL
              ELSE ABS(high_f - observed_high)
            END AS ae_high,
            CASE
              WHEN low_f IS NULL OR observed_low IS NULL THEN NULL
              ELSE ABS(low_f - observed_low)
            END AS ae_low
          FROM pairs
        ),
        computed2 AS (
          SELECT
            forecast_run_id,
            observation_run_id,
            station_id,
            target_date,
            ae_high,
            ae_low,
            CASE
              WHEN ae_high IS NOT NULL AND ae_low IS NOT NULL THEN (ae_high + ae_low) / 2.0
              WHEN ae_high IS NOT NULL THEN ae_high
              WHEN ae_low  IS NOT NULL THEN ae_low
              ELSE NULL
            END AS mae
          FROM computed
        )
        INSERT INTO forecast_errors (
          forecast_run_id,
          observation_run_id,
          station_id,
          target_date,
          ae_high,
          ae_low,
          mae
        )
        SELECT
          forecast_run_id,
          observation_run_id,
          station_id,
          target_date,
          ae_high,
          ae_low,
          mae
        FROM computed2
        WHERE ae_high IS NOT NULL OR ae_low IS NOT NULL
        ON CONFLICT (forecast_run_id, observation_run_id, station_id, target_date)
        DO UPDATE SET
          ae_high = EXCLUDED.ae_high,
          ae_low  = EXCLUDED.ae_low,
          mae     = EXCLUDED.mae,
          created_at = now()
        """
        with conn.cursor() as cur:
            cur.execute(sql, (target_date, target_date))
            n = cur.rowcount or 0
        if owns:
            conn.commit()
        return n
    finally:
        if owns:
            conn.close()

# -------------------------
# Dashboard stats (renamed from error_stats)
# -------------------------

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


def update_dashboard_stats(*, window_days: int, conn=None) -> None:
    """
    Recompute dashboard_stats directly from forecasts_daily + latest observations.
    This does NOT depend on forecast_errors shape beyond existence.

    dashboard_stats schema:
      (station_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, last_updated)

    kind in {'high','low','both'}.
    """
    if window_days <= 0:
        window_days = 1

    owns = conn is None
    if owns:
        from db import get_conn
        conn = get_conn()

    try:
        sql = """
        WITH obs_latest AS (
          SELECT DISTINCT ON (o.station_id, o.date)
            o.station_id,
            o.date,
            o.observed_high,
            o.observed_low
          FROM observations o
          JOIN observation_runs r ON r.run_id = o.run_id
          WHERE o.date >= (CURRENT_DATE - (%s::int * INTERVAL '1 day'))::date
          ORDER BY o.station_id, o.date, r.run_issued_at DESC
        ),
        joined AS (
          SELECT
            d.station_id,
            fr.source,
            d.target_date AS date,
            d.high_f,
            d.low_f,
            o.observed_high,
            o.observed_low
          FROM forecasts_daily d
          JOIN forecast_runs fr ON fr.run_id = d.run_id
          JOIN obs_latest o
            ON o.station_id = d.station_id
           AND o.date = d.target_date
          WHERE d.target_date >= (CURRENT_DATE - (%s::int * INTERVAL '1 day'))::date
        ),
        high_err AS (
          SELECT
            station_id,
            source,
            (high_f - observed_high) AS err
          FROM joined
          WHERE high_f IS NOT NULL AND observed_high IS NOT NULL
        ),
        low_err AS (
          SELECT
            station_id,
            source,
            (low_f - observed_low) AS err
          FROM joined
          WHERE low_f IS NOT NULL AND observed_low IS NOT NULL
        ),
        both_err AS (
          SELECT station_id, source, err FROM high_err
          UNION ALL
          SELECT station_id, source, err FROM low_err
        ),
        agg AS (
          SELECT
            station_id,
            source,
            'high'::text AS kind,
            COUNT(*)::int AS n,
            AVG(err)::float AS bias,
            AVG(ABS(err))::float AS mae,
            SQRT(AVG(err*err))::float AS rmse,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY err)::float AS p10,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY err)::float AS p50,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY err)::float AS p90
          FROM high_err
          GROUP BY station_id, source

          UNION ALL

          SELECT
            station_id,
            source,
            'low'::text AS kind,
            COUNT(*)::int AS n,
            AVG(err)::float AS bias,
            AVG(ABS(err))::float AS mae,
            SQRT(AVG(err*err))::float AS rmse,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY err)::float AS p10,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY err)::float AS p50,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY err)::float AS p90
          FROM low_err
          GROUP BY station_id, source

          UNION ALL

          SELECT
            station_id,
            source,
            'both'::text AS kind,
            COUNT(*)::int AS n,
            AVG(err)::float AS bias,
            AVG(ABS(err))::float AS mae,
            SQRT(AVG(err*err))::float AS rmse,
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY err)::float AS p10,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY err)::float AS p50,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY err)::float AS p90
          FROM both_err
          GROUP BY station_id, source
        )
        INSERT INTO dashboard_stats (
          station_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, last_updated
        )
        SELECT
          station_id,
          source,
          kind,
          %s::int AS window_days,
          n,
          bias,
          mae,
          rmse,
          p10,
          p50,
          p90,
          now()
        FROM agg
        ON CONFLICT (station_id, source, kind, window_days)
        DO UPDATE SET
          n = EXCLUDED.n,
          bias = EXCLUDED.bias,
          mae = EXCLUDED.mae,
          rmse = EXCLUDED.rmse,
          p10 = EXCLUDED.p10,
          p50 = EXCLUDED.p50,
          p90 = EXCLUDED.p90,
          last_updated = now()
        """
        with conn.cursor() as cur:
            cur.execute(sql, (window_days, window_days, window_days))
        if owns:
            conn.commit()
    finally:
        if owns:
            conn.close()





