select
    nullif(trim(cast(channel_id as text)), '') as channel_id,
    nullif(trim(cast(channel_name as text)), '') as channel_name,
    nullif(trim(cast(channel_type as text)), '') as channel_type,
    cast(demand_weight as double precision) as demand_weight,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'channels') }}
