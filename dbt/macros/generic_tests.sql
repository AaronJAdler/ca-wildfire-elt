{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as row_count
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}


{% test in_range(model, column_name, min_value, max_value) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}


{% test no_overlapping_versions(model, key, valid_from, valid_to) %}
-- SCD2 rule: a key's validity ranges must not overlap, or a range join would fan out.
select earlier.{{ key }}
from {{ model }} as earlier
inner join {{ model }} as later
    on earlier.{{ key }} = later.{{ key }}
    and earlier.{{ valid_from }} < later.{{ valid_from }}
    and earlier.{{ valid_to }} > later.{{ valid_from }}
{% endtest %}


{% test one_current_version(model, key, flag) %}
select {{ key }}
from {{ model }}
group by {{ key }}
having sum(case when {{ flag }} then 1 else 0 end) <> 1
{% endtest %}
