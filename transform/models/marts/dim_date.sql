with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2028-01-01' as date)"
    ) }}
),

days as (
    select cast(date_day as date) as date_day from spine
)

select
    {{ date_key('date_day') }} as date_key,
    date_day,
    cast(extract(year from date_day) as integer) as year,
    cast(extract(month from date_day) as integer) as month,
    cast(extract(day from date_day) as integer) as day_of_month,
    {{ iso_day_of_week('date_day') }} as iso_day_of_week,
    {{ iso_day_of_week('date_day') }} >= 6 as is_weekend
from days
