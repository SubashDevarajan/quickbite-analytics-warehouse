{#
  Order fact, one row per order, incremental.

  * Point-in-time joins: each order gets the dimension *version* valid at
    order_ts, so revenue uses the commission rate that applied that day.
  * Late data: each run re-processes orders updated in the last
    `late_arrival_days`, so late status changes (refunds) land, and orders
    whose restaurant arrived late in the catalogue get re-pointed from the
    unknown member (-1) to the real restaurant.
  * Idempotent: MERGE / delete+insert on order_id.
  * BigQuery: partitioned by order_date and clustered, so date-filtered
    queries scan only the partitions they need.
#}
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy=('merge' if target.type == 'bigquery' else 'delete+insert'),
    partition_by=({'field': 'order_date', 'data_type': 'date'} if target.type == 'bigquery' else none),
    cluster_by=(['restaurant_id', 'customer_id'] if target.type == 'bigquery' else none),
    on_schema_change='fail'
) }}

with orders as (
    select *
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where updated_at >= (
        select {{ lookback('max(updated_at)', var('late_arrival_days')) }} from {{ this }}
    )
    {% endif %}
),

customers as (
    select * from {{ ref('dim_customer') }} where customer_key != '-1'
),

restaurants as (
    select * from {{ ref('dim_restaurant') }} where restaurant_key != '-1'
)

select
    o.order_id,
    o.order_date,
    {{ date_key('o.order_date') }} as date_key,
    coalesce(c.customer_key, '-1') as customer_key,
    coalesce(r.restaurant_key, '-1') as restaurant_key,
    o.customer_id,
    o.restaurant_id,
    r.zone as restaurant_zone,
    c.loyalty_tier as customer_tier,
    o.order_ts,
    o.status,
    o.is_final,
    o.payment_method,
    o.items_count,
    o.gross_amount,
    o.discount_amount,
    o.delivery_fee,
    o.net_amount,
    r.commission_pct,
    round(o.net_amount * r.commission_pct / 100, 2) as commission_amount,
    o.delivered_ts,
    {{ dbt.datediff('o.order_ts', 'o.delivered_ts', 'minute') }} as delivery_minutes,
    o.updated_at
from orders as o
left join customers as c
    on o.customer_id = c.customer_id
    and o.order_ts >= c.valid_from
    and o.order_ts < c.valid_to
left join restaurants as r
    on o.restaurant_id = r.restaurant_id
    and o.order_ts >= r.valid_from
    and o.order_ts < r.valid_to
