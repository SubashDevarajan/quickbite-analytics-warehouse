-- Revenue by the loyalty tier the customer had *when they ordered*.
-- Using the current tier instead would move past revenue into GOLD/PLATINUM
-- every time a customer is upgraded: the classic reason for SCD Type 2.
select
    cast({{ dbt.date_trunc('month', 'order_date') }} as date) as month,
    coalesce(customer_tier, 'UNKNOWN') as loyalty_tier,
    count(*) as delivered_orders,
    sum(net_amount) as gmv,
    count(distinct customer_id) as active_customers
from {{ ref('fct_orders') }}
where status = 'DELIVERED'
group by 1, 2
