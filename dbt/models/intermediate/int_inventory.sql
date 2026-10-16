select
    event_date,
    snapshot_date,
    store_id,
    product_id,
    quantity_on_hand,
    stock_value_mad,
    stock_status,
    ingestion_timestamp
from (
    select
        cast(snapshot_date as date) as event_date,
        snapshot_date,
        store_id,
        product_id,
        quantity_on_hand,
        stock_value_mad,
        stock_status,
        ingestion_timestamp
    from {{ ref('stg_inventory') }}
) base
