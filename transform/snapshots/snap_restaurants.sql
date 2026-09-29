{#
  SCD Type 2 history of restaurants: commission rate and cuisine change over
  time, and revenue must use the rate that applied when the order was placed.
#}
{% snapshot snap_restaurants %}
{{ config(
    target_schema='snapshots',
    unique_key='restaurant_id',
    strategy='timestamp',
    updated_at='updated_at'
) }}

select
    restaurant_id,
    name,
    zone,
    cuisine,
    commission_pct,
    is_active,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at
from {{ source('raw', 'restaurants') }}
where _extract_date = cast('{{ var("run_date") }}' as date)

{% endsnapshot %}
