-- Restaurant dimension, SCD Type 2 (commission rate, cuisine, zone, status over time).
with versions as (
    select
        restaurant_id,
        name,
        zone,
        cuisine,
        commission_pct,
        is_active,
        created_at,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (partition by restaurant_id order by dbt_valid_from) as version_number
    from {{ ref('snap_restaurants') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['restaurant_id', 'dbt_valid_from']) }} as restaurant_key,
    restaurant_id,
    name,
    zone,
    cuisine,
    commission_pct,
    is_active,
    created_at,
    version_number,
    case
        when version_number = 1 then cast('1900-01-01 00:00:00' as timestamp)
        else dbt_valid_from
    end as valid_from,
    coalesce(dbt_valid_to, cast('9999-12-31 23:59:59' as timestamp)) as valid_to,
    dbt_valid_to is null as is_current
from versions

union all

select
    '-1',
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    'UNKNOWN',
    cast(null as {{ dbt.type_numeric() }}),
    cast(null as boolean),
    cast(null as timestamp),
    0,
    cast('1900-01-01 00:00:00' as timestamp),
    cast('9999-12-31 23:59:59' as timestamp),
    true
