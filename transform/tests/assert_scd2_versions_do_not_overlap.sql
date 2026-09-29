-- A customer or restaurant can have only one valid version at any instant.
-- Overlaps would make the point-in-time join return two rows per order.
with versions as (
    select 'customer' as dim, customer_id as natural_key, valid_from, valid_to
    from {{ ref('dim_customer') }}
    where customer_key != '-1'

    union all

    select 'restaurant' as dim, restaurant_id as natural_key, valid_from, valid_to
    from {{ ref('dim_restaurant') }}
    where restaurant_key != '-1'
)

select a.dim, a.natural_key, a.valid_from, b.valid_from as overlapping_from
from versions as a
inner join versions as b
    on a.dim = b.dim
    and a.natural_key = b.natural_key
    and a.valid_from < b.valid_from
    and a.valid_to > b.valid_from
