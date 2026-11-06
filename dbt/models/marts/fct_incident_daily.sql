{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='snapshot_date',
        on_schema_change='fail'
    )
}}

-- Grain: one row per incident per snapshot date (periodic snapshot fact).
--
-- acres_burned and percent_contained are state at a point in time. Never sum them across dates;
-- use acres_change or mart_statewide_daily for totals.
--
-- A run replaces only its logical date, so reruns are idempotent. A backfill must reprocess from
-- the earliest changed date through the latest date, because carried-forward values and
-- acres_change depend on earlier days.

with daily as (

    select * from {{ ref('int_incident_daily_filled') }}
    {% if var('run_date') %}
        where
            snapshot_date
            {{ '=' if is_incremental() else '<=' }}
            cast('{{ var("run_date") }}' as date)
    {% endif %}

)

select
    md5(concat_ws('|', daily.unique_id, daily.snapshot_date)) as incident_daily_sk,
    daily.unique_id,
    daily.snapshot_date,
    cast(strftime(daily.snapshot_date, '%Y%m%d') as integer) as date_sk,
    versions.incident_version_sk,

    daily.acres_burned,
    daily.percent_contained,
    daily.acres_change,
    daily.acres_change_is_single_day,
    daily.days_since_previous_snapshot,
    daily.personnel,
    daily.structures_destroyed,
    daily.structures_threatened,

    daily.acres_is_carried_forward,
    daily.containment_is_carried_forward,
    daily.acres_reported,
    daily.percent_contained_reported,
    daily.started_at,
    daily.source_updated_at
from daily
inner join {{ ref('dim_incident') }} as versions
    on
        daily.unique_id = versions.unique_id
        and daily.snapshot_ts >= versions.valid_from
        and daily.snapshot_ts < versions.valid_to
