select
    sale_id,
    transaction_id,
    sale_date,
    event_date,
    hour_of_day,
    store_id,
    channel_id,
    customer_id,
    product_id,
    promotion_id,
    quantity,
    unit_price_mad,
    discount_pct,
    net_amount_mad,
    cost_amount_mad,
    margin_mad,
    ingestion_timestamp
from (
    select
        sale_id,
        transaction_id,
        sale_date,
        cast(sale_date as date) as event_date,
        hour_of_day,
        store_id,
        channel_id,
        customer_id,
        product_id,
        promotion_id,
        quantity,
        unit_price_mad,
        discount_pct,
        net_amount_mad,
        cost_amount_mad,
        margin_mad,
        ingestion_timestamp
    from {{ ref('stg_sales') }}
) base
