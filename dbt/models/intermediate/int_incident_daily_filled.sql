-- Grain: one row per incident per snapshot date.
--
-- The feed often publishes a null acreage or containment for a fire that reported a value the
-- day before. Those nulls mean "not stated today", not "zero acres", so each measure carries the
-- last reported value forward and flags that it did. Growth is computed on the filled series so
-- a missing day cannot look like a sudden burn of the fire's whole acreage.
--
-- After a run of nulls, the next reported value contains several days of growth at once.
-- acres_change_is_single_day separates true one-day figures from those catch-up jumps.

with snapshots as (

    select * from {{ ref('stg_incidents__snapshots') }}

),

filled as (

    select
        *,
        last_value(acres_reported ignore nulls) over incident_history as acres_burned,
        last_value(percent_contained_reported ignore nulls) over incident_history
            as percent_contained,
        lag(snapshot_date) over incident_order as previous_snapshot_date,
        lag(acres_reported) over incident_order as previous_acres_reported
    from snapshots
    window
        incident_history as (
            partition by unique_id order by snapshot_date
            rows between unbounded preceding and current row
        ),
        incident_order as (partition by unique_id order by snapshot_date)

)

select
    *,
    acres_reported is null and acres_burned is not null as acres_is_carried_forward,
    percent_contained_reported is null and percent_contained is not null
        as containment_is_carried_forward,
    date_diff('day', previous_snapshot_date, snapshot_date) as days_since_previous_snapshot,
    acres_burned - lag(acres_burned) over (
        partition by unique_id order by snapshot_date
    ) as acres_change,
    coalesce(
        date_diff('day', previous_snapshot_date, snapshot_date) = 1
        and acres_reported is not null
        and previous_acres_reported is not null,
        false
    ) as acres_change_is_single_day
from filled
