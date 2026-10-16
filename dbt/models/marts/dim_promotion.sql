select
    row_number() over (order by p.promotion_id) as promotion_sk,
    p.promotion_id,
    p.product_id,
    pr.produit_sk,
    p.region_id,
    r.region_sk,
    p.channel_id,
    c.canal_sk,
    p.promotion_type,
    p.discount_pct,
    p.start_date,
    p.end_date
from {{ ref('int_promotions') }} p
join {{ ref('dim_produit') }} pr on pr.product_id = p.product_id and pr.is_current
left join {{ ref('dim_region') }} r on r.region_id = p.region_id
left join {{ ref('dim_canal') }} c on c.channel_id = p.channel_id
