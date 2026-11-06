-- Grain: one row per calendar date in the season.
-- Includes dates with no feed snapshot, so an outage shows up as a gap rather than disappearing.

with spine as (

    select unnest(generate_series(
        cast('{{ var("season_start") }}' as date),
        cast('{{ var("season_end") }}' as date),
        interval 1 day
    ))::date as calendar_date

),

published as (

    select distinct snapshot_date from {{ ref('stg_incidents__snapshots') }}

)

select
    cast(strftime(spine.calendar_date, '%Y%m%d') as integer) as date_sk,
    spine.calendar_date,
    extract(year from spine.calendar_date) as calendar_year,
    extract(month from spine.calendar_date) as calendar_month,
    strftime(spine.calendar_date, '%A') as day_name,
    published.snapshot_date is not null as has_feed_snapshot
from spine
left join published on spine.calendar_date = published.snapshot_date
