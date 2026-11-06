-- Grain: one row per incident, summarizing everything the feed reported during the season.

with facts as (

    select * from {{ ref('fct_incident_daily') }}

),

peak_growth as (

    -- Only true one-day changes count, so catch-up jumps cannot pose as a daily peak.
    select
        unique_id,
        snapshot_date as peak_growth_date,
        acres_change as peak_daily_growth_acres
    from facts
    where acres_change > 0 and acres_change_is_single_day
    qualify row_number() over (
        partition by unique_id order by acres_change desc, snapshot_date asc
    ) = 1

),

first_half_contained as (

    select
        unique_id,
        min(snapshot_date) as first_50_percent_contained_date
    from facts
    where percent_contained >= 50
    group by unique_id

),

summary as (

    select
        unique_id,
        min(snapshot_date) as first_seen_date,
        max(snapshot_date) as last_seen_date,
        count(*) as days_in_feed,
        max(started_at) as started_at,
        max(acres_burned) as peak_acres,
        arg_max(acres_burned, snapshot_date) as latest_acres,
        arg_max(percent_contained, snapshot_date) as latest_percent_contained,
        max(structures_destroyed) as peak_structures_destroyed,
        avg(case when acres_is_carried_forward then 1.0 else 0.0 end) as share_of_days_carried_forward
    from facts
    group by unique_id

)

select
    summary.unique_id,
    incidents.incident_name,
    incidents.county_names,
    incidents.admin_unit,
    incidents.is_major,
    summary.first_seen_date,
    summary.last_seen_date,
    summary.days_in_feed,
    summary.started_at,
    summary.peak_acres,
    summary.latest_acres,
    summary.latest_percent_contained,
    summary.peak_structures_destroyed,
    summary.share_of_days_carried_forward,
    peak_growth.peak_growth_date,
    peak_growth.peak_daily_growth_acres,
    first_half_contained.first_50_percent_contained_date,
    date_diff(
        'day', cast(summary.started_at as date), first_half_contained.first_50_percent_contained_date
    ) as days_from_start_to_50_percent_contained
from summary
inner join {{ ref('dim_incident') }} as incidents
    on summary.unique_id = incidents.unique_id and incidents.is_current
left join peak_growth on summary.unique_id = peak_growth.unique_id
left join first_half_contained on summary.unique_id = first_half_contained.unique_id
