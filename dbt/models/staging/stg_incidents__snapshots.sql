-- Grain: one row per incident per snapshot date.
-- Parses and types the raw record. Business rules belong in the intermediate layer.

with source as (

    select
        *,
        try_cast(raw_json ->> '$.Latitude' as double) as latitude_raw,
        try_cast(raw_json ->> '$.Longitude' as double) as longitude_raw
    from {{ source('bronze', 'incidents') }}

)

select
    unique_id,
    snapshot_date,
    cast(snapshot_date as timestamp) as snapshot_ts,

    -- Peak Fire published a longitude of -11846461 on 2021-07-20, a dropped decimal point. Bad
    -- coordinates become null and are flagged. Bronze keeps the original, and nothing is guessed.
    case when latitude_raw between 32 and 42.1 then latitude_raw end as latitude,
    case when longitude_raw between -124.6 and -114 then longitude_raw end as longitude,
    (latitude_raw is not null and latitude_raw not between 32 and 42.1)
    or (longitude_raw is not null and longitude_raw not between -124.6 and -114)
        as has_invalid_coordinates,

    trim(raw_json ->> '$.Name') as incident_name,
    nullif(trim(raw_json ->> '$.AdminUnit'), '') as admin_unit,
    nullif(trim(raw_json ->> '$.CountyIds'), '') as county_ids,
    array_to_string(
        from_json(raw_json -> '$.Counties', '["VARCHAR"]'), ', '
    ) as county_names,

    try_cast(raw_json ->> '$.AcresBurned' as double) as acres_reported,
    try_cast(raw_json ->> '$.PercentContained' as double) as percent_contained_reported,
    try_cast(raw_json ->> '$.PersonnelInvolved' as integer) as personnel,
    try_cast(raw_json ->> '$.StructuresDestroyed' as integer) as structures_destroyed,
    try_cast(raw_json ->> '$.StructuresThreatened' as integer) as structures_threatened,

    coalesce(try_cast(raw_json ->> '$.MajorIncident' as boolean), false) as is_major,
    coalesce(try_cast(raw_json ->> '$.CalFireIncident' as boolean), false) as is_calfire,
    coalesce(try_cast(raw_json ->> '$.Final' as boolean), false) as is_final,

    -- The feed publishes UTC timestamps with a Z suffix.
    try_cast(replace(raw_json ->> '$.Started', 'Z', '') as timestamp) as started_at,
    try_cast(replace(raw_json ->> '$.Updated', 'Z', '') as timestamp) as source_updated_at,

    source_commit,
    source_committed_at

from source
