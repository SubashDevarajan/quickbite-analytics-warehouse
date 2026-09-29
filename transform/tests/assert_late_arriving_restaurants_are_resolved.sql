-- Orders can reference a restaurant that is not in the catalogue yet
-- (restaurant_key = '-1'). The incremental look-back must re-point them once
-- the restaurant arrives, so no order older than the look-back window may
-- still point at the unknown member.
select order_id, order_date, restaurant_id
from {{ ref('fct_orders') }}
where restaurant_key = '-1'
    and order_date < (
        select cast({{ dbt.dateadd('day', -1 * var('late_arrival_days'), 'max(order_date)') }} as date)
        from {{ ref('fct_orders') }}
    )
