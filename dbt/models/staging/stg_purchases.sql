select
    nullif(trim(cast(purchase_id as text)), '') as purchase_id,
    cast(purchase_date as date) as purchase_date,
    nullif(trim(cast(store_id as text)), '') as store_id,
    nullif(trim(cast(product_id as text)), '') as product_id,
    cast(quantity as bigint) as quantity,
    cast(unit_cost_mad as double precision) as unit_cost_mad,
    cast(total_cost_mad as double precision) as total_cost_mad,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'purchases') }}
