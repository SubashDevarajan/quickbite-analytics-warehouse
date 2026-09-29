{# Small helpers where DuckDB and BigQuery SQL differ. #}

{# Timestamp minus N days. #}
{% macro lookback(ts_expr, days) -%}
    {{ return(adapter.dispatch('lookback')(ts_expr, days)) }}
{%- endmacro %}

{% macro default__lookback(ts_expr, days) -%}
    ({{ ts_expr }} - interval '{{ days }} days')
{%- endmacro %}

{% macro bigquery__lookback(ts_expr, days) -%}
    timestamp_sub({{ ts_expr }}, interval {{ days }} day)
{%- endmacro %}


{# ISO day of week: Monday = 1 ... Sunday = 7. #}
{% macro iso_day_of_week(date_expr) -%}
    {{ return(adapter.dispatch('iso_day_of_week')(date_expr)) }}
{%- endmacro %}

{% macro default__iso_day_of_week(date_expr) -%}
    isodow({{ date_expr }})
{%- endmacro %}

{% macro bigquery__iso_day_of_week(date_expr) -%}
    (mod(extract(dayofweek from {{ date_expr }}) + 5, 7) + 1)
{%- endmacro %}


{# Integer surrogate key for a date: 2026-09-30 -> 20260930. #}
{% macro date_key(date_expr) -%}
    cast(extract(year from {{ date_expr }}) * 10000
         + extract(month from {{ date_expr }}) * 100
         + extract(day from {{ date_expr }}) as integer)
{%- endmacro %}
