-- 1) create run table
create table if not exists public.observation_runs (
  run_id bigserial primary key,
  run_issued_at timestamptz not null,
  created_at timestamptz not null default now(),
  note text
);

create unique index if not exists observation_runs_unique
on public.observation_runs (run_issued_at);

-- 2) create new observations table keyed by run
create table if not exists public.observations_v2 (
  run_id bigint not null references public.observation_runs(run_id) on delete cascade,
  station_id text not null references public.locations(station_id),
  date date not null,
  observed_high double precision not null,
  observed_low double precision not null,
  issued_at timestamptz null,
  fetched_at timestamptz not null default now(),
  raw_text text null,
  source text not null,
  primary key (run_id, station_id, date)
);

create index if not exists observations_v2_station_date
on public.observations_v2 (station_id, date);

-- 3) convenience view for "latest observation per station/date"
create or replace view public.observations_latest as
select distinct on (station_id, date)
  station_id, date,
  observed_high, observed_low,
  issued_at, fetched_at, raw_text, source,
  run_id
from public.observations_v2
order by station_id, date, fetched_at desc;
