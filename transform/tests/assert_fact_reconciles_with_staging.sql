-- Nothing lost or duplicated between staging and the incremental fact:
-- row count and total gross amount must match exactly.
with stg as (
    select count(*) as stg_rows, sum(gross_amount) as stg_gross from {{ ref('stg_orders') }}
),

fct as (
    select count(*) as fct_rows, sum(gross_amount) as fct_gross from {{ ref('fct_orders') }}
)

select stg_rows, fct_rows, stg_gross, fct_gross
from stg
cross join fct
where stg_rows != fct_rows or stg_gross != fct_gross
