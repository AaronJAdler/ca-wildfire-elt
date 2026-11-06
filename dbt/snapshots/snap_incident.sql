{% snapshot snap_incident %}

{#
    SCD type 2 history of descriptive attributes. Measures such as acres belong to the fact table.

    Each run reads only its logical date and stamps versions with that date rather than the wall
    clock, so replaying history produces the dates the changes really happened.

    A snapshot cannot insert a version into its past. Rerunning an old day would otherwise compare
    old attributes with the current version and record a false change, so rows older than an
    incident's current version are ignored. That makes reruns safe. A day that was skipped outright
    is caught by the snapshot_matches_bronze_history test.
#}

{{
    config(
        target_schema='snapshots',
        unique_key='unique_id',
        strategy='check',
        updated_at='snapshot_ts',
        check_cols=[
            'incident_name', 'admin_unit', 'county_ids', 'county_names',
            'is_major', 'is_calfire', 'is_final'
        ],
        hard_deletes='ignore'
    )
}}

{% set existing = load_relation(this) if execute else none %}

select
    source.unique_id,
    source.snapshot_ts,
    source.incident_name,
    source.admin_unit,
    source.county_ids,
    source.county_names,
    source.is_major,
    source.is_calfire,
    source.is_final
from {{ ref('stg_incidents__snapshots') }} as source
{% if var('run_date') %}
where source.snapshot_date = cast('{{ var("run_date") }}' as date)
{% else %}
where source.snapshot_date = (select max(snapshot_date) from {{ ref('stg_incidents__snapshots') }})
{% endif %}
{% if existing is not none %}
    and not exists (
        select 1
        from {{ this }} as current_version
        where current_version.unique_id = source.unique_id
            and current_version.dbt_valid_to is null
            and current_version.dbt_valid_from > source.snapshot_ts
    )
{% endif %}

{% endsnapshot %}
