-- migrations/001_init.sql
create extension if not exists pgcrypto;

-- 1) New ML-grade hourly feature table (no aggregation)
create table if not exists public.forecast_extras_hourly (
  run_id uuid not null references public.forecast_runs(run_id) on delete cascade,
  station_id text not null references public.locations(station_id) on delete cascade,
  valid_time timestamptz not null,

  temperature_f double precision,
  dewpoint_f double precision,
  humidity_pct double precision,
  wind_speed_mph double precision,
  wind_dir_deg double precision,
  cloud_cover_pct double precision,
  precip_prob_pct double precision,

  extras jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),

  primary key (run_id, station_id, valid_time)
);

create index if not exists idx_extras_hourly_station_time
  on public.forecast_extras_hourly (station_id, valid_time);

create index if not exists idx_extras_hourly_run
  on public.forecast_extras_hourly (run_id);

-- 2) New compact daily forecast table (one row per station/day/run)
-- (This replaces the "kind = high/low" two-row shape going forward.)
create table if not exists public.forecasts_daily (
  run_id uuid not null references public.forecast_runs(run_id) on delete cascade,
  station_id text not null references public.locations(station_id) on delete cascade,
  target_date date not null,

  high_f double precision,
  low_f double precision,

  lead_high_hours double precision,
  lead_low_hours double precision,

  created_at timestamptz not null default now(),
  primary key (run_id, station_id, target_date)
);

create index if not exists idx_forecasts_daily_station_date
  on public.forecasts_daily (station_id, target_date);

-- 3) (Optional) Compatibility view so existing queries can still read the old shape.
-- NOTE: This is READ-ONLY. Writes should go to forecasts_daily.
create or replace view public.forecasts_v1 as
select
  run_id,
  station_id,
  target_date,
  'high'::text as kind,
  high_f as value_f,
  lead_high_hours as lead_hours,
  null::double precision as dewpoint_f,
  null::double precision as humidity_pct,
  null::double precision as wind_speed_mph,
  null::double precision as wind_dir_deg,
  null::double precision as cloud_cover_pct,
  null::double precision as precip_prob_pct,
  created_at
from public.forecasts_daily
union all
select
  run_id,
  station_id,
  target_date,
  'low'::text as kind,
  low_f as value_f,
  lead_low_hours as lead_hours,
  null::double precision as dewpoint_f,
  null::double precision as humidity_pct,
  null::double precision as wind_speed_mph,
  null::double precision as wind_dir_deg,
  null::double precision as cloud_cover_pct,
  null::double precision as precip_prob_pct,
  created_at
from public.forecasts_daily;

-- 4) (Optional) Backfill forecasts_daily from existing forecasts (if you already have data).
-- Safe/idempotent: will not overwrite non-null high/low if already populated.
insert into public.forecasts_daily (run_id, station_id, target_date, high_f, low_f, lead_high_hours, lead_low_hours)
select
  run_id,
  station_id,
  target_date,
  max(value_f) filter (where kind = 'high') as high_f,
  max(value_f) filter (where kind = 'low')  as low_f,
  max(lead_hours) filter (where kind = 'high') as lead_high_hours,
  max(lead_hours) filter (where kind = 'low')  as lead_low_hours
from public.forecasts
group by run_id, station_id, target_date
on conflict (run_id, station_id, target_date) do update set
  high_f = coalesce(public.forecasts_daily.high_f, excluded.high_f),
  low_f  = coalesce(public.forecasts_daily.low_f,  excluded.low_f),
  lead_high_hours = coalesce(public.forecasts_daily.lead_high_hours, excluded.lead_high_hours),
  lead_low_hours  = coalesce(public.forecasts_daily.lead_low_hours,  excluded.lead_low_hours);

-- 5) (Optional) If you want to stop writing to public.forecasts entirely, you can later:
--   - update code to use forecasts_daily
--   - update scoring to use forecasts_daily
--   - then drop extras columns / drop table forecasts
