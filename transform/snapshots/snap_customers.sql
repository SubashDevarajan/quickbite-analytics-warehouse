{#
  SCD Type 2 history of customers. Each daily run compares the day's full
  extract with the snapshot: when updated_at moved forward, the old version
  is closed (dbt_valid_to) and a new one opened (dbt_valid_from = updated_at).
  Runs must be processed in date order; the Airflow task sets depends_on_past.
#}
{% snapshot snap_customers %}
{{ config(
    target_schema='snapshots',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

select
    customer_id,
    zone,
    loyalty_tier,
    cast(signup_ts as timestamp) as signup_ts,
    cast(updated_at as timestamp) as updated_at
from {{ source('raw', 'customers') }}
where _extract_date = cast('{{ var("run_date") }}' as date)

{% endsnapshot %}
