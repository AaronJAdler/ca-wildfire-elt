-- Guards the semi-additive trap. A date's total can never exceed the sum of each fire's season peak.

with ceiling as (

    select sum(peak_acres) as maximum_possible
    from {{ ref('mart_incident_season') }}

)

select daily.calendar_date, daily.tracked_acres_to_date, ceiling.maximum_possible
from {{ ref('mart_statewide_daily') }} as daily
cross join ceiling
where daily.tracked_acres_to_date > ceiling.maximum_possible
