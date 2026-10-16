{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='sale_id',
) }}

select
    d.date_sk,
    m.magasin_sk,
    ca.canal_sk,
    cl.client_sk,
    p.produit_sk,
    pm.promotion_sk,
    s.sale_id,
    s.transaction_id,
    s.hour_of_day,
    s.quantity,
    s.unit_price_mad,
    s.discount_pct,
    s.net_amount_mad,
    s.cost_amount_mad,
    s.margin_mad,
    s.ingestion_timestamp
from {{ ref('int_sales') }} s
join {{ ref('dim_date') }} d on d.date = s.event_date
join {{ ref('dim_magasin') }} m on m.store_id = s.store_id
join {{ ref('dim_canal') }} ca on ca.channel_id = s.channel_id
join {{ ref('dim_produit') }} p
    on p.product_id = s.product_id
    and s.event_date >= p.valid_from
    and (p.valid_to is null or s.event_date < p.valid_to)
left join {{ ref('dim_client') }} cl on cl.customer_id = s.customer_id
left join {{ ref('dim_promotion') }} pm on pm.promotion_id = s.promotion_id
{% if is_incremental() %}
where s.ingestion_timestamp > (select max(ingestion_timestamp) from {{ this }})
{% endif %}
