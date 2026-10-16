select
    nullif(trim(cast(customer_id as text)), '') as customer_id,
    cast(signup_date as date) as signup_date,
    nullif(trim(cast(region_id as text)), '') as region_id,
    cast(birth_year as bigint) as birth_year,
    nullif(trim(cast(gender as text)), '') as gender,
    nullif(trim(cast(segment as text)), '') as segment,
    nullif(trim(cast(loyalty_tier as text)), '') as loyalty_tier,
    nullif(trim(cast(preferred_channel_id as text)), '') as preferred_channel_id,
    cast(is_active as boolean) as is_active,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'customers') }}
