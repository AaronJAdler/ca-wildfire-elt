-- The stateful dbt snapshot must agree with history derived from immutable bronze, for every date
-- the warehouse has processed. A failure means a day was skipped, so replay the snapshot in order.

with processed as (

    select cast(max(snapshot_date) as timestamp) as through_ts
    from {{ ref('fct_incident_daily') }}

),

from_snapshot as (

    select unique_id, incident_name, county_ids, is_final, valid_from
    from {{ ref('dim_incident') }}

),

from_bronze as (

    select unique_id, incident_name, county_ids, is_final, valid_from
    from {{ ref('int_incident_versions') }}
    where valid_from <= (select through_ts from processed)

),

only_in_snapshot as (

    select * from from_snapshot
    except
    select * from from_bronze

),

only_in_bronze as (

    select * from from_bronze
    except
    select * from from_snapshot

)

select 'only_in_snapshot' as side, * from only_in_snapshot
union all
select 'only_in_bronze' as side, * from only_in_bronze
