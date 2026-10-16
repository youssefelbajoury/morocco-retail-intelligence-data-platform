select
    date,
    weekday,
    weekday_name,
    is_weekend,
    day
from {{ ref('dim_date') }}
where weekday < 0
    or weekday > 6
    or is_weekend is distinct from (weekday >= 5)
    or weekday_name is distinct from (
        array['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    )[weekday + 1]
    or day is distinct from cast(extract(day from date) as bigint)
    or is_weekend is null
    or is_holiday is null
    or is_ramadan is null
