select
    cast(snapshot_date as date) as snapshot_date,
    nullif(trim(cast(store_id as text)), '') as store_id,
    nullif(trim(cast(product_id as text)), '') as product_id,
    cast(quantity_on_hand as bigint) as quantity_on_hand,
    cast(stock_value_mad as double precision) as stock_value_mad,
    nullif(trim(cast(stock_status as text)), '') as stock_status,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'inventory') }}
