# db.py (Supabase-only) — UPDATED
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from etl_utils import utc_now_z

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
    # Supabase schema should be created via migrations.
    conn = get_conn()
    conn.close()


# -------------------------
# Locations
# -------------------------

def upsert_location(station: dict) -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        insert into public.locations (station_id, name, state, timezone, lat, lon, elevation_ft, is_active)
        values (%s,%s,%s,%s,%s,%s,%s,%s)
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
    conn.close()


# -------------------------
# Observations (supports 2x/day snapshots IF schema exists)
# -------------------------

# Authority ordering: prevent fallback overwriting CLI
_OBS_SOURCE_PRIORITY = {
    "NWS_CLI": 100,
    "nws_cli": 100,
    "NWS_OBS_FALLBACK": 10,
    "nws_station_obs": 10,
}

def _obs_priority(source: str) -> int:
    return _OBS_SOURCE_PRIORITY.get(source, 0)

# Per-process snapshot timestamp for observations (so a single night run uses one run_issued_at)
_OBS_RUN_ISSUED_AT: Optional[str] = None

def _get_obs_run_issued_at() -> str:
    global _OBS_RUN_ISSUED_AT
    if _OBS_RUN_ISSUED_AT is None:
        _OBS_RUN_ISSUED_AT = utc_now_z()
    return _OBS_RUN_ISSUED_AT


def get_or_create_observation_run(run_issued_at: str, conn=None) -> Any:
    """
    Requires migration:
      public.observation_runs(run_id PK, run_issued_at unique)
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
                on conflict (run_issued_at) do update set run_issued_at = excluded.run_issued_at
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
    station_id: str,
    obs_date: str,
    observed_high: float,
    observed_low: float,
    issued_at: Optional[str] = None,
    raw_text: Optional[str] = None,
    source: str = "nws_station_obs",
    *,
    run_issued_at: Optional[str] = None,
) -> None:
    """
    Behavior:
      - If (observation_runs + observations_v2) exist: store MULTI-RUN snapshots keyed by (run_id, station_id, date).
      - Else: store legacy single row keyed by (station_id, date), but enforce "no downgrade":
          fallback cannot overwrite CLI.
    """
    conn = get_conn()
    cur = conn.cursor()

    # Use a stable snapshot timestamp within one process run (night job)
    run_issued_at = run_issued_at or _get_obs_run_issued_at()

    try:
        # detect whether new schema exists
        cur.execute("""
            select
              to_regclass('public.observation_runs') is not null as has_runs,
              to_regclass('public.observations_v2') is not null as has_v2
        """)
        has_runs, has_v2 = cur.fetchone()
        has_new = bool(has_runs) and bool(has_v2)

        if has_new:
            run_id = get_or_create_observation_run(run_issued_at, conn=conn)

            cur.execute(
                """
                insert into public.observations_v2
                  (run_id, station_id, date, observed_high, observed_low, issued_at, fetched_at, raw_text, source)
                values
                  (%s, %s, %s::date, %s, %s, %s::timestamptz, now(), %s, %s)
                on conflict (run_id, station_id, date) do update set
                  observed_high = excluded.observed_high,
                  observed_low  = excluded.observed_low,
                  issued_at     = coalesce(excluded.issued_at, public.observations_v2.issued_at),
                  fetched_at    = now(),
                  raw_text      = coalesce(excluded.raw_text, public.observations_v2.raw_text),
                  source        = excluded.source
                """,
                (run_id, station_id, obs_date, observed_high, observed_low, issued_at, raw_text, source),
            )

            conn.commit()
            return

        # --- legacy single-row behavior with no-downgrade ---
        cur.execute(
            """
            select source
            from public.observations
            where station_id=%s and date=%s::date
            """,
            (station_id, obs_date),
        )
        row = cur.fetchone()
        existing_source = row[0] if row else None

        # Do not allow fallback (or any lower authority) to overwrite CLI
        if existing_source is not None and _obs_priority(source) < _obs_priority(str(existing_source)):
            # Optional: still bump fetched_at so you can see it ran
            cur.execute(
                """
                update public.observations
                set fetched_at = now()
                where station_id=%s and date=%s::date
                """,
                (station_id, obs_date),
            )
            conn.commit()
            return

        cur.execute(
            """
            insert into public.observations
              (station_id, date, observed_high, observed_low, issued_at, fetched_at, raw_text, source)
            values (%s,%s::date,%s,%s,%s::timestamptz, now(), %s, %s)
            on conflict (station_id, date) do update set
              observed_high=excluded.observed_high,
              observed_low=excluded.observed_low,
              issued_at=coalesce(excluded.issued_at, public.observations.issued_at),
              fetched_at=now(),
              raw_text=coalesce(excluded.raw_text, public.observations.raw_text),
              source=excluded.source
            """,
            (station_id, obs_date, observed_high, observed_low, issued_at, raw_text, source),
        )

        conn.commit()

    finally:
        conn.close()


# -------------------------
# Forecast runs / forecasts
# -------------------------

def get_or_create_forecast_run(source: str, issued_at: str, conn=None) -> Any:
    owns = False
    if conn is None:
        conn = get_conn()
        owns = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.forecast_runs (source, issued_at)
                values (%s, %s)
                on conflict (source, issued_at) do update set source = excluded.source
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


def bulk_upsert_forecast_values(conn, rows: list[dict]) -> int:
    if not rows:
        return 0

    sql = """
    insert into public.forecasts (
        run_id, station_id, target_date, kind, value_f, lead_hours,
        dewpoint_f, humidity_pct, wind_speed_mph, wind_dir_deg,
        cloud_cover_pct, precip_prob_pct, extras
    ) values (
        %(run_id)s, %(station_id)s, %(target_date)s, %(kind)s, %(value_f)s, %(lead_hours)s,
        %(dewpoint_f)s, %(humidity_pct)s, %(wind_speed_mph)s, %(wind_dir_deg)s,
        %(cloud_cover_pct)s, %(precip_prob_pct)s, %(extras)s::jsonb
    )
    on conflict (run_id, station_id, target_date, kind)
    do update set
        value_f = excluded.value_f,
        lead_hours = excluded.lead_hours,
        dewpoint_f = excluded.dewpoint_f,
        humidity_pct = excluded.humidity_pct,
        wind_speed_mph = excluded.wind_speed_mph,
        wind_dir_deg = excluded.wind_dir_deg,
        cloud_cover_pct = excluded.cloud_cover_pct,
        precip_prob_pct = excluded.precip_prob_pct,
        extras = excluded.extras;
    """

    with conn.cursor() as cur:
        cur.executemany(sql, rows)

    return len(rows)


def upsert_forecast_value(
    *,
    run_id: Any,
    station_id: str,
    target_date: str,
    kind: str,
    value_f: float,
    lead_hours: Optional[float],
    extras: Optional[Dict[str, Any]] = None,
) -> None:
    extras = extras or {}

    cur_vals = (
        run_id,
        station_id,
        target_date,
        kind,
        value_f,
        lead_hours,
        extras.get("dewpoint_f"),
        extras.get("humidity_pct"),
        extras.get("wind_speed_mph"),
        extras.get("wind_dir_deg"),
        extras.get("cloud_cover_pct"),
        extras.get("precip_prob_pct"),
    )

    conn = get_conn()
    cur = conn.cursor()

    # NOTE: removed ::uuid casts to avoid run_id type mismatch
    cur.execute(
        """
        insert into public.forecasts
          (run_id, station_id, target_date, kind, value_f, lead_hours,
           dewpoint_f, humidity_pct, wind_speed_mph, wind_dir_deg, cloud_cover_pct, precip_prob_pct)
        values
          (%s,%s,%s::date,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        on conflict (run_id, station_id, target_date, kind) do update set
          value_f=excluded.value_f,
          lead_hours=coalesce(excluded.lead_hours, public.forecasts.lead_hours),
          dewpoint_f=coalesce(excluded.dewpoint_f, public.forecasts.dewpoint_f),
          humidity_pct=coalesce(excluded.humidity_pct, public.forecasts.humidity_pct),
          wind_speed_mph=coalesce(excluded.wind_speed_mph, public.forecasts.wind_speed_mph),
          wind_dir_deg=coalesce(excluded.wind_dir_deg, public.forecasts.wind_dir_deg),
          cloud_cover_pct=coalesce(excluded.cloud_cover_pct, public.forecasts.cloud_cover_pct),
          precip_prob_pct=coalesce(excluded.precip_prob_pct, public.forecasts.precip_prob_pct)
        """,
        cur_vals,
    )

    conn.commit()
    conn.close()


# -------------------------
# Errors + stats
# -------------------------

def build_errors_for_date(target_date: str) -> int:
    conn = get_conn()
    cur = conn.cursor()

    # Prefer observations_latest (multi-run) if it exists
    cur.execute("select to_regclass('public.observations_latest') is not null")
    has_latest = bool(cur.fetchone()[0])

    if has_latest:
        cur.execute(
            """
            select station_id, observed_high, observed_low
            from public.observations_latest
            where date=%s::date
            """,
            (target_date,),
        )
    else:
        cur.execute(
            """
            select station_id, observed_high, observed_low
            from public.observations
            where date=%s::date
            """,
            (target_date,),
        )

    obs_rows = cur.fetchall()
    if not obs_rows:
        conn.close()
        return 0

    wrote = 0
    for station_id, oh, ol in obs_rows:
        cur.execute(
            """
            select f.run_id, r.source, r.issued_at, f.kind, f.value_f, f.lead_hours
            from public.forecasts f
            join public.forecast_runs r on r.run_id = f.run_id
            where f.station_id=%s and f.target_date=%s::date
            """,
            (station_id, target_date),
        )

        for run_id, source, issued_at, kind, forecast_f, lead_hours in cur.fetchall():
            if forecast_f is None:
                continue
            observed_f = float(oh) if kind == "high" else float(ol)
            error_f = float(forecast_f) - observed_f
            abs_error_f = abs(error_f)
            forecast_id = f"{run_id}|{station_id}|{target_date}|{kind}"

            cur.execute(
                """
                insert into public.forecast_errors
                  (forecast_id, station_id, source, target_date, kind, issued_at, lead_hours,
                   forecast_f, observed_f, error_f, abs_error_f)
                values
                  (%s,%s,%s,%s::date,%s,%s,%s,%s,%s,%s,%s)
                on conflict (forecast_id) do nothing
                """,
                (
                    forecast_id,
                    station_id,
                    source,
                    target_date,
                    kind,
                    issued_at,
                    lead_hours,
                    forecast_f,
                    observed_f,
                    error_f,
                    abs_error_f,
                ),
            )
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


def compute_revisions_for_run(run_id: Any) -> int:
    """
    For all forecast rows in a run, compute delta vs previous issued_at for the same
    (station_id, source, kind, target_date). Idempotent via PK.
    """
    conn = get_conn()
    cur = conn.cursor()

    # NOTE: removed ::uuid cast to avoid run_id type mismatch
    cur.execute(
        """
        select r.source, r.issued_at, f.station_id, f.target_date, f.kind, f.value_f
        from public.forecasts f
        join public.forecast_runs r on r.run_id = f.run_id
        where f.run_id = %s
        """,
        (run_id,),
    )
    rows = cur.fetchall()
    if not rows:
        conn.close()
        return 0

    wrote = 0
    for source, issued_at, station_id, target_date, kind, forecast_f in rows:
        if forecast_f is None:
            continue

        cur.execute(
            """
            select r2.issued_at, f2.value_f
            from public.forecasts f2
            join public.forecast_runs r2 on r2.run_id = f2.run_id
            where f2.station_id=%s
              and r2.source=%s
              and f2.kind=%s
              and f2.target_date=%s::date
              and r2.issued_at < %s::timestamptz
            order by r2.issued_at desc
            limit 1
            """,
            (station_id, source, kind, target_date, issued_at),
        )
        prev = cur.fetchone()
        prev_issued_at = prev[0] if prev else None
        prev_forecast_f = float(prev[1]) if (prev and prev[1] is not None) else None
        delta_f = (float(forecast_f) - prev_forecast_f) if prev_forecast_f is not None else None

        cur.execute(
            """
            insert into public.forecast_revisions
              (station_id, source, kind, target_date, issued_at, forecast_f,
               prev_issued_at, prev_forecast_f, delta_f)
            values
              (%s,%s,%s,%s::date,%s::timestamptz,%s,%s::timestamptz,%s,%s)
            on conflict (station_id, source, kind, target_date, issued_at) do nothing
            """,
            (
                station_id,
                source,
                kind,
                target_date,
                issued_at,
                float(forecast_f),
                prev_issued_at,
                prev_forecast_f,
                delta_f,
            ),
        )
        wrote += 1

    conn.commit()
    conn.close()
    return wrote


def update_error_stats(*, window_days: int, station_id: Optional[str] = None) -> None:
    conn = get_conn()
    cur = conn.cursor()

    params: List[Any] = [window_days]
    station_clause = ""
    if station_id:
        station_clause = "and station_id=%s"
        params.append(station_id)

    cur.execute(
        f"""
        select station_id, source, kind, error_f, abs_error_f
        from public.forecast_errors
        where target_date >= (now()::date - (%s::int * interval '1 day'))
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

    now_ts = utc_now_z()

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
            insert into public.error_stats
              (station_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, last_updated)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::timestamptz)
            on conflict (station_id, source, kind, window_days) do update set
              n=excluded.n, bias=excluded.bias, mae=excluded.mae, rmse=excluded.rmse,
              p10=excluded.p10, p50=excluded.p50, p90=excluded.p90, last_updated=excluded.last_updated
            """,
            (st_id, source, kind, window_days, n, bias, mae, rmse, p10, p50, p90, now_ts),
        )

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
            insert into public.error_stats
              (station_id, source, kind, window_days, n, mae, last_updated)
            values (%s,%s,'both',%s,%s,%s,%s::timestamptz)
            on conflict (station_id, source, kind, window_days) do update set
              n=excluded.n, mae=excluded.mae, last_updated=excluded.last_updated
            """,
            (st_id, source, window_days, n, mae_both, now_ts),
        )

    conn.commit()
    conn.close()
