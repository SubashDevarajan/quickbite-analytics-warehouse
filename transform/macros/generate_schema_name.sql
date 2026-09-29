{#
  Use the configured schema name as-is (raw, staging, snapshots, marts)
  instead of dbt's default "<target_schema>_<custom_schema>". The same names
  are created as BigQuery datasets by infra/terraform.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
