select
    nullif(trim(cast(promotion_id as text)), '') as promotion_id,
    nullif(trim(cast(product_id as text)), '') as product_id,
    nullif(trim(cast(region_id as text)), '') as region_id,
    nullif(trim(cast(channel_id as text)), '') as channel_id,
    nullif(trim(cast(promotion_type as text)), '') as promotion_type,
    cast(discount_pct as double precision) as discount_pct,
    cast(start_date as date) as start_date,
    cast(end_date as date) as end_date,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'promotions') }}
