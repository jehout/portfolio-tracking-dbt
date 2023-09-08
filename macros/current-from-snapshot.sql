{% macro current_from_snapshot(snsh_ref, output_snsh_load_ts=false) %}

    select
        * exclude (dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to)
        {% if output_snsh_load_ts %}, dbt_updated_at as snsh_loaded_ts_utc {% endif %}
    from {{ snsh_ref }}
    where dbt_valid_to is null

{% endmacro %}
