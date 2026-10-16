select
    customer_id,
    signup_date,
    region_id,
    birth_year,
    gender,
    segment,
    loyalty_tier,
    preferred_channel_id,
    is_active
from {{ ref('stg_customers') }}
