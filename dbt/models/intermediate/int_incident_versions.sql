-- Grain: one row per consecutive run of identical incident attributes.
--
-- Derives the same history as the dbt snapshot, but from immutable bronze, using gaps and islands.
-- A test compares the two, which catches a snapshot that drifted or was replayed out of order.

with attributes as (

    select
        unique_id,
        snapshot_ts,
        incident_name,
        admin_unit,
        county_ids,
        county_names,
        is_major,
        is_calfire,
        is_final,
        md5(concat_ws(
            '|', incident_name, coalesce(admin_unit, ''), coalesce(county_ids, ''),
            coalesce(county_names, ''), is_major, is_calfire, is_final
        )) as attributes_hash
    from {{ ref('stg_incidents__snapshots') }}

),

flagged as (

    select
        *,
        case
            when
                lag(attributes_hash) over (partition by unique_id order by snapshot_ts)
                = attributes_hash
                then 0
            else 1
        end as starts_new_version
    from attributes

),

numbered as (

    select
        *,
        sum(starts_new_version) over (
            partition by unique_id order by snapshot_ts rows unbounded preceding
        ) as version_number
    from flagged

)

select
    unique_id,
    version_number,
    any_value(incident_name) as incident_name,
    any_value(admin_unit) as admin_unit,
    any_value(county_ids) as county_ids,
    any_value(county_names) as county_names,
    any_value(is_major) as is_major,
    any_value(is_calfire) as is_calfire,
    any_value(is_final) as is_final,
    min(snapshot_ts) as valid_from
from numbered
group by unique_id, version_number
