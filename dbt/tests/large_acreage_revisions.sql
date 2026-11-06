{{ config(severity='warn') }}

-- Warning only. Published perimeters are sometimes remapped downward, and those revisions are real.
-- A drop over 5% is still worth a person's attention, so it is surfaced rather than hidden.

select unique_id, snapshot_date, acres_burned, acres_change
from {{ ref('fct_incident_daily') }}
where acres_change < 0
    and acres_burned - acres_change > 0
    and -acres_change / (acres_burned - acres_change) > 0.05
