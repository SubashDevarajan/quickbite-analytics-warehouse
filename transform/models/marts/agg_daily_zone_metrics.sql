-- Daily operating metrics per delivery zone (zone of the restaurant at order time).
select
    order_date,
    coalesce(restaurant_zone, 'UNKNOWN') as zone,
    count(*) as orders,
    sum(case when status = 'DELIVERED' then 1 else 0 end) as delivered_orders,
    sum(case when status = 'CANCELLED' then 1 else 0 end) as cancelled_orders,
    sum(case when status = 'REFUNDED' then 1 else 0 end) as refunded_orders,
    sum(case when status = 'DELIVERED' then net_amount else 0 end) as gmv,
    sum(case when status = 'DELIVERED' then coalesce(commission_amount, 0) else 0 end) as commission_revenue,
    avg(case when status in ('DELIVERED', 'REFUNDED') then delivery_minutes end) as avg_delivery_minutes,
    avg(
        case
            when status in ('DELIVERED', 'REFUNDED')
                then case when delivery_minutes <= 40 then 1.0 else 0.0 end
        end
    ) as sla_hit_rate
from {{ ref('fct_orders') }}
where is_final
group by 1, 2
