select
    nullif(trim(cast(product_id as text)), '') as product_id,
    nullif(trim(cast(product_name as text)), '') as product_name,
    nullif(trim(cast(category as text)), '') as category,
    nullif(trim(cast(subcategory as text)), '') as subcategory,
    nullif(trim(cast(brand as text)), '') as brand,
    cast(unit_cost_mad as double precision) as unit_cost_mad,
    cast(unit_price_mad as double precision) as unit_price_mad,
    cast(margin_pct as double precision) as margin_pct,
    cast(launch_date as date) as launch_date,
    cast(is_active as boolean) as is_active,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'products') }}
