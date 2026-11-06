-- Grain: one row per California county.

select
    county_id,
    county_name
from {{ ref('ca_counties') }}
