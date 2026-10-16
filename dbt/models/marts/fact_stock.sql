{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['snapshot_date', 'store_id', 'product_id'],
) }}

select
    d.date_sk,
    m.magasin_sk,
    p.produit_sk,
    s.snapshot_date,
    s.store_id,
    s.product_id,
    s.quantity_on_hand,
    s.stock_value_mad,
    s.stock_status,
    s.ingestion_timestamp
from {{ ref('int_inventory') }} s
join {{ ref('dim_date') }} d on d.date = s.event_date
join {{ ref('dim_magasin') }} m on m.store_id = s.store_id
join {{ ref('dim_produit') }} p
    on p.product_id = s.product_id
    and s.event_date >= p.valid_from
    and (p.valid_to is null or s.event_date < p.valid_to)
{% if is_incremental() %}
where s.ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
