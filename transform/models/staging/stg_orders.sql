-- One row per order: its latest known state.
-- The same order appears in several daily extracts (placed -> delivered ->
-- refunded), and flaky exports sometimes duplicate rows, so keep the most
-- recent version by updated_at.
with ranked as (
    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc, _loaded_at desc
        ) as version_rank
    from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    restaurant_id,
    cast(order_ts as timestamp) as order_ts,
    cast(cast(order_ts as timestamp) as date) as order_date,
    status,
    status in ('DELIVERED', 'CANCELLED', 'REFUNDED') as is_final,
    items_count,
    gross_amount,
    discount_amount,
    delivery_fee,
    gross_amount - discount_amount + delivery_fee as net_amount,
    coalesce(payment_method, 'UNKNOWN') as payment_method,
    cast(delivered_ts as timestamp) as delivered_ts,
    cast(updated_at as timestamp) as updated_at,
    _extract_date as last_extract_date
from ranked
where version_rank = 1
