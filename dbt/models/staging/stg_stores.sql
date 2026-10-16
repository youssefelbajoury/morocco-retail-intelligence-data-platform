select
    nullif(trim(cast(store_id as text)), '') as store_id,
    nullif(trim(cast(store_name as text)), '') as store_name,
    nullif(trim(cast(city as text)), '') as city,
    nullif(trim(cast(region_id as text)), '') as region_id,
    nullif(trim(cast(store_format as text)), '') as store_format,
    cast(size_sqm as bigint) as size_sqm,
    cast(open_date as date) as open_date,
    cast(is_active as boolean) as is_active,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'stores') }}
