-- Grain: one row per calendar date.
--
-- This is the correct way to total a semi-additive measure. Each fire contributes its latest known
-- acreage as of each date, even after leaving the feed, and once per date. Summing the fact table
-- across dates would count a fire's whole acreage again every day it stayed active.

with dates as (

    select * from {{ ref('dim_date') }}

),

facts as (

    select * from {{ ref('fct_incident_daily') }}
    where acres_burned is not null

),

latest_known as (

    select
        dates.calendar_date,
        facts.unique_id,
        arg_max(facts.acres_burned, facts.snapshot_date) as acres_burned
    from dates
    inner join facts on dates.calendar_date >= facts.snapshot_date
    group by dates.calendar_date, facts.unique_id

),

season_totals as (

    select
        calendar_date,
        sum(acres_burned) as tracked_acres_to_date,
        count(*) as fires_with_known_acreage
    from latest_known
    group by calendar_date

),

active as (

    select
        snapshot_date,
        count(*) as incidents_in_feed,
        sum(personnel) as personnel_reported,
        avg(case when acres_is_carried_forward then 1.0 else 0.0 end) as share_carried_forward
    from {{ ref('fct_incident_daily') }}
    group by snapshot_date

)

select
    dates.calendar_date,
    dates.has_feed_snapshot,
    active.incidents_in_feed,
    active.personnel_reported,
    active.share_carried_forward,
    season_totals.tracked_acres_to_date,
    season_totals.fires_with_known_acreage,
    season_totals.tracked_acres_to_date - lag(season_totals.tracked_acres_to_date) over (
        order by dates.calendar_date
    ) as acres_added
from dates
left join active on dates.calendar_date = active.snapshot_date
left join season_totals on dates.calendar_date = season_totals.calendar_date
