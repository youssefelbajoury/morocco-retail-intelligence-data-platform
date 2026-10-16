select
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_cost_mad,
    unit_price_mad,
    margin_pct,
    launch_date,
    is_active
from {{ ref('stg_products') }}
