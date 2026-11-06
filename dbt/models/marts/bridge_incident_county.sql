-- Grain: one row per incident per county, from each incident's current attributes.
--
-- Fires can span counties and the feed gives no per-county acreage. This bridge supports counting
-- incidents by county. Summing a fire's acres through it would count them once per county.

with exploded as (

    select
        unique_id,
        cast(trim(unnest(string_split(county_ids, ','))) as integer) as county_id
    from {{ ref('dim_incident') }}
    where is_current and county_ids is not null

)

select distinct
    unique_id,
    county_id
from exploded
