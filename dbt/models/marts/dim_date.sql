select
    row_number() over (order by date) as date_sk,
    date,
    cast(extract(day from date) as bigint) as day,
    year,
    month,
    quarter,
    iso_week,
    weekday,
    weekday_name,
    is_weekend,
    is_holiday,
    holiday_name,
    is_ramadan,
    is_eid_al_fitr,
    is_eid_al_adha,
    season
from {{ ref('stg_calendar') }}
