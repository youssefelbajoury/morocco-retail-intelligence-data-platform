select
    nullif(trim(cast(sale_id as text)), '') as sale_id,
    nullif(trim(cast(transaction_id as text)), '') as transaction_id,
    cast(sale_date as date) as sale_date,
    cast(hour_of_day as bigint) as hour_of_day,
    nullif(trim(cast(store_id as text)), '') as store_id,
    nullif(trim(cast(channel_id as text)), '') as channel_id,
    nullif(trim(cast(customer_id as text)), '') as customer_id,
    nullif(trim(cast(product_id as text)), '') as product_id,
    nullif(trim(cast(promotion_id as text)), '') as promotion_id,
    cast(quantity as bigint) as quantity,
    cast(unit_price_mad as double precision) as unit_price_mad,
    cast(discount_pct as double precision) as discount_pct,
    cast(net_amount_mad as double precision) as net_amount_mad,
    cast(cost_amount_mad as double precision) as cost_amount_mad,
    cast(margin_mad as double precision) as margin_mad,
    cast(batch_id as text) as batch_id,
    cast(ingestion_timestamp as timestamp with time zone) as ingestion_timestamp
from {{ source('silver', 'sales') }}
