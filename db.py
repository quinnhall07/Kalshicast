# db.py (Supabase-only) — REFAC (no legacy) :contentReference[oaicite:0]{index=0}
from __future__ import annotations

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
    run_id: int,
    station_id: str,
    date: str,
    observed_high: float | None,
    observed_low: float | None,
    source: str,
    flagged_raw_text: str | None = None,
    raw_text: str | None = None,   # legacy alias
    conn=None,
    **_ignored,                    # swallow stale kwargs safely
) -> None:
    """
    Upsert into observations.

    Backward compatible:
      - accepts raw_text=... (legacy) and maps it to flagged_raw_text
      - ignores unexpected kwargs to prevent pipeline breakage
    """
    frt = flagged_raw_text if (flagged_raw_text is not None and str(flagged_raw_text).strip()) else raw_text
    if frt is not None and not str(frt).strip():
        frt = None

    sql = """
    INSERT INTO observations (
      run_id,
      station_id,
      date,
      observed_high,
      observed_low,
      source,
      flagged_raw_text
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id, station_id, date)
    DO UPDATE SET
      observed_high = EXCLUDED.observed_high,
      observed_low  = EXCLUDED.observed_low,
      source        = EXCLUDED.source,
      flagged_raw_text = COALESCE(EXCLUDED.flagged_raw_text, observations.flagged_raw_text)
    """

    # Use provided conn if your db layer does that; otherwise use your existing pattern.
    if conn is None:
        from db import get_conn  # if get_conn exists in same module, remove this import and call directly
        with get_conn() as c:
            with c.cursor() as cur:
                cur.execute(sql, (run_id, station_id, date, observed_high, observed_low, source, frt))
            c.commit()
    else:
        with conn.cursor() as cur:
            cur.execute(sql, (run_id, station_id, date, observed_high, observed_low, source, frt))


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

def build_forecast_errors_for_date(*, target_date: str, observation_run_id: Optional[Any] = None) -> int:
    """
    Requires: public.forecast_errors (compact)
      (forecast_run_id uuid, observation_run_id uuid, station_id text, target_date date, kind text,
       forecast_f double precision, observed_f double precision,
       error_f double precision, abs_error_f double precision,
       lead_hours double precision,
       primary key (forecast_run_id, observation_run_id, station_id, target_date, kind))

    Reads:
      - public.observations (for selected observation_run_id)
      - public.forecasts_daily + public.forecast_runs
    """
    with get_conn() as conn:
        if observation_run_id is None:
            observation_run_id = _latest_observation_run_id(conn)
            if observation_run_id is None:
                return 0

        with conn.cursor() as cur:
            cur.execute(
                """
                select station_id, observed_high, observed_low
                from public.observations
                where run_id=%s and date=%s::date
                """,
                (observation_run_id, target_date),
            )
            obs_rows = cur.fetchall()

            if not obs_rows:
                return 0

            wrote = 0

            for station_id, oh, ol in obs_rows:
                cur.execute(
                    """
                    select d.run_id, r.source, r.issued_at,
                           d.high_f, d.low_f,
                           d.lead_hours_high, d.lead_hours_low
                    from public.forecasts_daily d
                    join public.forecast_runs r on r.run_id = d.run_id
                    where d.station_id=%s and d.target_date=%s::date
                    """,
                    (station_id, target_date),
                )

                for forecast_run_id, _source, _issued_at, high_f, low_f, lead_high, lead_low in cur.fetchall():
                    # high
                    if high_f is not None and oh is not None:
                        err = float(high_f) - float(oh)
                        cur.execute(
                            """
                            insert into public.forecast_errors
                              (forecast_run_id, observation_run_id, station_id, target_date, kind,
                               forecast_f, observed_f, error_f, abs_error_f, lead_hours)
                            values
                              (%s,%s,%s,%s::date,'high',%s,%s,%s,%s,%s)
                            on conflict (forecast_run_id, observation_run_id, station_id, target_date, kind)
                            do update set
                              forecast_f = excluded.forecast_f,
                              observed_f = excluded.observed_f,
                              error_f = excluded.error_f,
                              abs_error_f = excluded.abs_error_f,
                              lead_hours = excluded.lead_hours
                            """,
                            (
                                forecast_run_id,
                                observation_run_id,
                                station_id,
                                target_date,
                                float(high_f),
                                float(oh),
                                err,
                                abs(err),
                                lead_high,
                            ),
                        )
                        wrote += 1

                    # low
                    if low_f is not None and ol is not None:
                        err = float(low_f) - float(ol)
                        cur.execute(
                            """
                            insert into public.forecast_errors
                              (forecast_run_id, observation_run_id, station_id, target_date, kind,
                               forecast_f, observed_f, error_f, abs_error_f, lead_hours)
                            values
                              (%s,%s,%s,%s::date,'low',%s,%s,%s,%s,%s)
                            on conflict (forecast_run_id, observation_run_id, station_id, target_date, kind)
                            do update set
                              forecast_f = excluded.forecast_f,
                              observed_f = excluded.observed_f,
                              error_f = excluded.error_f,
                              abs_error_f = excluded.abs_error_f,
                              lead_hours = excluded.lead_hours
                            """,
                            (
                                forecast_run_id,
                                observation_run_id,
                                station_id,
                                target_date,
                                float(low_f),
                                float(ol),
                                err,
                                abs(err),
                                lead_low,
                            ),
                        )
                        wrote += 1

        conn.commit()
        return wrote


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


def update_dashboard_stats(*, window_days: int, station_id: Optional[str] = None) -> None:
    """
    Requires: public.dashboard_stats
      (station_id text, source text, kind text ('high','low','both'),
       window_days int, n int, bias double, mae double, rmse double, p10 double, p50 double, p90 double,
       last_updated timestamptz,
       primary key (station_id, source, kind, window_days))

    Reads:
      - public.forecast_errors
      - public.forecast_runs (for source)
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            params: List[Any] = [window_days]
            station_clause = ""
            if station_id:
                station_clause = "and e.station_id=%s"
                params.append(station_id)

            cur.execute(
                f"""
                select
                  e.station_id,
                  r.source,
                  e.kind,
                  e.error_f,
                  e.abs_error_f
                from public.forecast_errors e
                join public.forecast_runs r on r.run_id = e.forecast_run_id
                where e.target_date >= (now()::date - (%s::int * interval '1 day'))
                {station_clause}
                """,
                params,
            )
            rows = cur.fetchall()

            by: Dict[Tuple[str, str, str], List[Tuple[float, float]]] = {}
            for st_id, source, kind, e, ae in rows:
                if st_id is None or source is None or kind is None:
                    continue
                if e is None or ae is None:
                    continue
                by.setdefault((str(st_id), str(source), str(kind)), []).append((float(e), float(ae)))

            # per (station, source, kind)
            for (st_id, source, kind), vals in by.items():
                n = len(vals)
                if n == 0:
                    continue

                errors = [v[0] for v in vals]
                abs_errors = [v[1] for v in vals]

                bias = sum(errors) / n
                mae = sum(abs_errors) / n
                rmse = (sum((x * x) for x in errors) / n) ** 0.5

                se = sorted(errors)
                p10 = _percentile(se, 0.10)
                p50 = _percentile(se, 0.50)
                p90 = _percentile(se, 0.90)

                cur.execute(
                    """
                    insert into public.dashboard_stats
                      (station_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, last_updated)
                    values
                      (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, now())
                    on conflict (station_id, source, kind, window_days) do update set
                      n=excluded.n,
                      bias=excluded.bias,
                      mae=excluded.mae,
                      rmse=excluded.rmse,
                      p10=excluded.p10,
                      p50=excluded.p50,
                      p90=excluded.p90,
                      last_updated=now()
                    """,
                    (st_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90),
                )

            # 'both' rollup
            stations_sources = sorted({(k[0], k[1]) for k in by.keys()})
            for st_id, source in stations_sources:
                highs = by.get((st_id, source, "high"), [])
                lows = by.get((st_id, source, "low"), [])
                if not highs or not lows:
                    continue

                mae_high = sum(v[1] for v in highs) / len(highs)
                mae_low = sum(v[1] for v in lows) / len(lows)
                mae_both = (mae_high + mae_low) / 2.0
                n = min(len(highs), len(lows))

                cur.execute(
                    """
                    insert into public.dashboard_stats
                      (station_id, source, kind, window_days, n, mae, last_updated)
                    values
                      (%s,%s,'both',%s,%s,%s, now())
                    on conflict (station_id, source, kind, window_days) do update set
                      n=excluded.n,
                      mae=excluded.mae,
                      last_updated=now()
                    """,
                    (st_id, source, window_days, n, mae_both),
                )

        conn.commit()




