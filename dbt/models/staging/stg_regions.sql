select
    nullif(trim(cast(region_id as text)), '') as region_id,
    nullif(trim(cast(region_name as text)), '') as region_name,
    cast(demand_weight as double precision) as demand_weight,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'regions') }}
