-- Customer dimension, SCD Type 2: one row per customer *version*.
-- Facts join on customer_id AND order_ts within [valid_from, valid_to), so an
-- order is attributed to the loyalty tier and zone the customer had at the time.
with versions as (
    select
        customer_id,
        zone,
        loyalty_tier,
        signup_ts,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (partition by customer_id order by dbt_valid_from) as version_number
    from {{ ref('snap_customers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_key,
    customer_id,
    zone,
    loyalty_tier,
    signup_ts,
    version_number,
    -- The first version we ever captured also covers the time before it:
    -- history older than the first snapshot is unknown, not absent.
    case
        when version_number = 1 then cast('1900-01-01 00:00:00' as timestamp)
        else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31 23:59:59' as timestamp)) as valid_to,
    dbt_valid_to is null as is_current
from versions

union all

-- Unknown member: facts that cannot be matched point here instead of being dropped.
select
    '-1',
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    cast(null as timestamp),
    0,
    cast('1900-01-01 00:00:00' as timestamp),
    cast('9999-12-31 23:59:59' as timestamp),
    true
