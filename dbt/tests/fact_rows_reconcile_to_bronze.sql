-- Every bronze row through the latest processed date must appear exactly once in the fact.
-- This catches rows lost or multiplied by the SCD2 range join, and days never processed.

with fact as (

    select snapshot_date, count(*) as row_count
    from {{ ref('fct_incident_daily') }}
    group by snapshot_date

),

bronze as (

    select snapshot_date, count(*) as row_count
    from {{ ref('stg_incidents__snapshots') }}
    where snapshot_date <= (select max(snapshot_date) from fact)
    group by snapshot_date

)

select
    coalesce(bronze.snapshot_date, fact.snapshot_date) as snapshot_date,
    bronze.row_count as bronze_rows,
    fact.row_count as fact_rows
from bronze
full outer join fact on bronze.snapshot_date = fact.snapshot_date
where bronze.row_count is distinct from fact.row_count
