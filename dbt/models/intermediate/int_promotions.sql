select
    promotion_id,
    product_id,
    region_id,
    channel_id,
    promotion_type,
    discount_pct,
    start_date,
    end_date
from {{ ref('stg_promotions') }}
