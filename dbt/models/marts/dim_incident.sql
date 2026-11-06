-- Grain: one row per version of an incident's descriptive attributes (SCD type 2).

select
    md5(concat_ws('|', unique_id, dbt_valid_from)) as incident_version_sk,
    unique_id,
    incident_name,
    admin_unit,
    county_ids,
    county_names,
    is_major,
    is_calfire,
    is_final,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31' as timestamp)) as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('snap_incident') }}
