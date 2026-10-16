{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='purchase_id',
) }}

select
    d.date_sk,
    m.magasin_sk,
    p.produit_sk,
    s.purchase_id,
    s.quantity,
    s.unit_cost_mad,
    s.total_cost_mad,
    s.ingestion_timestamp
from {{ ref('int_purchases') }} s
join {{ ref('dim_date') }} d on d.date = s.event_date
join {{ ref('dim_magasin') }} m on m.store_id = s.store_id
join {{ ref('dim_produit') }} p
    on p.product_id = s.product_id
    and s.event_date >= p.valid_from
    and (p.valid_to is null or s.event_date < p.valid_to)
{% if is_incremental() %}
where s.ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
