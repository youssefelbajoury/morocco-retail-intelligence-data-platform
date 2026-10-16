select
    'fact_ventes.quantity' as metric,
    sale_id as row_key,
    cast(quantity as text) as value
from {{ ref('fact_ventes') }}
where quantity < 1
union all
select
    'fact_ventes.discount_pct',
    sale_id,
    cast(discount_pct as text)
from {{ ref('fact_ventes') }}
where discount_pct < 0 or discount_pct > 1
union all
select
    'fact_ventes.net_amount_mad',
    sale_id,
    cast(net_amount_mad as text)
from {{ ref('fact_ventes') }}
where net_amount_mad < 0
union all
select
    'fact_ventes.margin_mad',
    sale_id,
    cast(margin_mad as text)
from {{ ref('fact_ventes') }}
where margin_mad < 0
union all
select
    'fact_achats.quantity',
    purchase_id,
    cast(quantity as text)
from {{ ref('fact_achats') }}
where quantity < 1
union all
select
    'fact_achats.total_cost_mad',
    purchase_id,
    cast(total_cost_mad as text)
from {{ ref('fact_achats') }}
where total_cost_mad < 0
union all
select
    'fact_stock.quantity_on_hand',
    cast(magasin_sk as text) || '/' || cast(produit_sk as text) || '/'
        || cast(date_sk as text),
    cast(quantity_on_hand as text)
from {{ ref('fact_stock') }}
where quantity_on_hand < 0
union all
select
    'fact_stock.stock_value_mad',
    cast(magasin_sk as text) || '/' || cast(produit_sk as text) || '/'
        || cast(date_sk as text),
    cast(stock_value_mad as text)
from {{ ref('fact_stock') }}
where stock_value_mad < 0
