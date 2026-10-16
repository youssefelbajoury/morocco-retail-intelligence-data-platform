select
    purchase_id,
    purchase_date,
    event_date,
    store_id,
    product_id,
    quantity,
    unit_cost_mad,
    total_cost_mad,
    ingestion_timestamp
from (
    select
        purchase_id,
        purchase_date,
        cast(purchase_date as date) as event_date,
        store_id,
        product_id,
        quantity,
        unit_cost_mad,
        total_cost_mad,
        ingestion_timestamp
    from {{ ref('stg_purchases') }}
) base
