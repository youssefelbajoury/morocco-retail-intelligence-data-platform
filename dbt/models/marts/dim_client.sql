select
    row_number() over (order by c.customer_id) as client_sk,
    c.customer_id,
    c.signup_date,
    c.region_id,
    r.region_sk,
    c.birth_year,
    c.gender,
    c.segment,
    c.loyalty_tier,
    c.preferred_channel_id,
    ch.canal_sk,
    c.is_active
from {{ ref('int_customers') }} c
join {{ ref('dim_region') }} r on r.region_id = c.region_id
join {{ ref('dim_canal') }} ch on ch.channel_id = c.preferred_channel_id
