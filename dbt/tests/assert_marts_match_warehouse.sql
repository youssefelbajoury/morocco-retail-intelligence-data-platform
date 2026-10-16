-- depends_on: {{ ref('dim_canal') }}
-- depends_on: {{ ref('dim_client') }}
-- depends_on: {{ ref('dim_date') }}
-- depends_on: {{ ref('dim_magasin') }}
-- depends_on: {{ ref('dim_promotion') }}
-- depends_on: {{ ref('dim_produit') }}
-- depends_on: {{ ref('dim_region') }}
-- depends_on: {{ ref('fact_achats') }}
-- depends_on: {{ ref('fact_stock') }}
-- depends_on: {{ ref('fact_ventes') }}
{% set wh_schema = var('warehouse_schema') %}
{% set wh_tables = [
    'dim_region',
    'dim_canal',
    'dim_date',
    'dim_magasin',
    'dim_client',
    'dim_promotion',
    'dim_produit',
    'fact_ventes',
    'fact_achats',
    'fact_stock',
] %}
{% set ns = namespace(missing = []) %}
{% for table in wh_tables %}
    {% set relation = adapter.get_relation(
        database=target.database, schema=wh_schema, identifier=table
    ) %}
    {% if relation is none %}
        {% do ns.missing.append(table) %}
    {% endif %}
{% endfor %}
{% if ns.missing | length > 0 %}
select
    cast(null as text) as issue,
    cast(null as text) as key,
    cast(null as text) as detail
where false
{% else %}
with dbt_ventes as (
    select
        f.sale_id,
        f.transaction_id,
        f.date_sk,
        f.magasin_sk,
        f.canal_sk,
        f.client_sk,
        f.promotion_sk,
        f.hour_of_day,
        f.quantity,
        f.unit_price_mad,
        f.discount_pct,
        f.net_amount_mad,
        f.cost_amount_mad,
        f.margin_mad,
        p.product_id
    from {{ ref('fact_ventes') }} f
    join {{ ref('dim_produit') }} p on p.produit_sk = f.produit_sk
),
warehouse_ventes as (
    select
        f.sale_id,
        f.transaction_id,
        f.date_sk,
        f.magasin_sk,
        f.canal_sk,
        f.client_sk,
        f.promotion_sk,
        f.hour_of_day,
        f.quantity,
        f.unit_price_mad,
        f.discount_pct,
        f.net_amount_mad,
        f.cost_amount_mad,
        f.margin_mad,
        p.product_id
    from {{ adapter.get_relation(
        database=target.database, schema=wh_schema, identifier='fact_ventes'
    ) }} f
    join {{ adapter.get_relation(
        database=target.database, schema=wh_schema, identifier='dim_produit'
    ) }} p on p.produit_sk = f.produit_sk
),
count_checks as (
    select
        'dim_region' as table_name,
        (select count(*) from {{ ref('dim_region') }}) as dbt_rows,
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='dim_region'
        ) }}) as warehouse_rows
    union all
    select
        'dim_canal',
        (select count(*) from {{ ref('dim_canal') }}),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='dim_canal'
        ) }})
    union all
    select
        'dim_date',
        (select count(*) from {{ ref('dim_date') }}),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='dim_date'
        ) }})
    union all
    select
        'dim_magasin',
        (select count(*) from {{ ref('dim_magasin') }}),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='dim_magasin'
        ) }})
    union all
    select
        'dim_client',
        (select count(*) from {{ ref('dim_client') }}),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='dim_client'
        ) }})
    union all
    select
        'dim_promotion',
        (select count(*) from {{ ref('dim_promotion') }}),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='dim_promotion'
        ) }})
    union all
    select
        'dim_produit (current versions)',
        (select count(*) from {{ ref('dim_produit') }} where is_current),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='dim_produit'
        ) }})
    union all
    select
        'fact_ventes',
        (select count(*) from {{ ref('fact_ventes') }}),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='fact_ventes'
        ) }})
    union all
    select
        'fact_achats',
        (select count(*) from {{ ref('fact_achats') }}),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='fact_achats'
        ) }})
    union all
    select
        'fact_stock',
        (select count(*) from {{ ref('fact_stock') }}),
        (select count(*) from {{ adapter.get_relation(
            database=target.database, schema=wh_schema, identifier='fact_stock'
        ) }})
),
count_mismatches as (
    select
        'row_count_mismatch' as issue,
        table_name as key,
        cast(dbt_rows as text) || ' vs ' || cast(warehouse_rows as text) as detail
    from count_checks
    where dbt_rows <> warehouse_rows
),
fact_mismatches as (
    select
        'missing_in_warehouse' as issue,
        d.sale_id as key,
        'row absent from warehouse fact_ventes' as detail
    from dbt_ventes d
    left join warehouse_ventes w on w.sale_id = d.sale_id
    where w.sale_id is null
    union all
    select
        'missing_in_dbt',
        w.sale_id,
        'row absent from dbt fact_ventes'
    from warehouse_ventes w
    left join dbt_ventes d on d.sale_id = w.sale_id
    where d.sale_id is null
    union all
    select
        'column_mismatch',
        d.sale_id,
        concat_ws(
            ', ',
            case when d.transaction_id is distinct from w.transaction_id
                then 'transaction_id' end,
            case when d.date_sk is distinct from w.date_sk then 'date_sk' end,
            case when d.magasin_sk is distinct from w.magasin_sk then 'magasin_sk' end,
            case when d.canal_sk is distinct from w.canal_sk then 'canal_sk' end,
            case when d.client_sk is distinct from w.client_sk then 'client_sk' end,
            case when d.promotion_sk is distinct from w.promotion_sk
                then 'promotion_sk' end,
            case when d.hour_of_day is distinct from w.hour_of_day
                then 'hour_of_day' end,
            case when d.quantity is distinct from w.quantity then 'quantity' end,
            case when d.unit_price_mad is distinct from w.unit_price_mad
                then 'unit_price_mad' end,
            case when d.discount_pct is distinct from w.discount_pct
                then 'discount_pct' end,
            case when d.net_amount_mad is distinct from w.net_amount_mad
                then 'net_amount_mad' end,
            case when d.cost_amount_mad is distinct from w.cost_amount_mad
                then 'cost_amount_mad' end,
            case when d.margin_mad is distinct from w.margin_mad
                then 'margin_mad' end,
            case when d.product_id is distinct from w.product_id then 'product_id' end
        )
    from dbt_ventes d
    join warehouse_ventes w on w.sale_id = d.sale_id
    where d.transaction_id is distinct from w.transaction_id
        or d.date_sk is distinct from w.date_sk
        or d.magasin_sk is distinct from w.magasin_sk
        or d.canal_sk is distinct from w.canal_sk
        or d.client_sk is distinct from w.client_sk
        or d.promotion_sk is distinct from w.promotion_sk
        or d.hour_of_day is distinct from w.hour_of_day
        or d.quantity is distinct from w.quantity
        or d.unit_price_mad is distinct from w.unit_price_mad
        or d.discount_pct is distinct from w.discount_pct
        or d.net_amount_mad is distinct from w.net_amount_mad
        or d.cost_amount_mad is distinct from w.cost_amount_mad
        or d.margin_mad is distinct from w.margin_mad
        or d.product_id is distinct from w.product_id
)
select issue, key, detail from count_mismatches
union all
select issue, key, detail from fact_mismatches
{% endif %}
