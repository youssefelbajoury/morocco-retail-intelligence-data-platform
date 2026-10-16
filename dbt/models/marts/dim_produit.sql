with versions as (
    select
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        unit_cost_mad,
        unit_price_mad,
        margin_pct,
        launch_date,
        is_active,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (
            partition by product_id
            order by dbt_valid_from
        ) as version_no
    from {{ ref('snapshot_dim_produit') }}
),
effective as (
    select
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        unit_cost_mad,
        unit_price_mad,
        margin_pct,
        launch_date,
        is_active,
        case
            when version_no = 1 then launch_date
            else cast(dbt_valid_from as date)
        end as valid_from,
        cast(dbt_valid_to as date) as valid_to,
        version_no
    from versions
)
select
    {{ text_hash_sk("product_id || '|' || version_no || '|' || valid_from::text") }} as produit_sk,
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_cost_mad,
    unit_price_mad,
    margin_pct,
    launch_date,
    is_active,
    valid_from,
    valid_to,
    valid_to is null as is_current
from effective
