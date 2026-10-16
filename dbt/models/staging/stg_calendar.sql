select
    cast(date as date) as date,
    cast(year as bigint) as year,
    cast(month as bigint) as month,
    cast(quarter as bigint) as quarter,
    cast(iso_week as bigint) as iso_week,
    cast(weekday as bigint) as weekday,
    nullif(trim(cast(weekday_name as text)), '') as weekday_name,
    cast(is_weekend as boolean) as is_weekend,
    cast(is_holiday as boolean) as is_holiday,
    nullif(trim(cast(holiday_name as text)), '') as holiday_name,
    cast(is_ramadan as boolean) as is_ramadan,
    cast(is_eid_al_fitr as boolean) as is_eid_al_fitr,
    cast(is_eid_al_adha as boolean) as is_eid_al_adha,
    nullif(trim(cast(season as text)), '') as season,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'calendar') }}
