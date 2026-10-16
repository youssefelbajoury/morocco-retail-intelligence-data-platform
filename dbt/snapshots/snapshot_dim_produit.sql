{% snapshot snapshot_dim_produit %}

{{
    config(
        unique_key="product_id",
        strategy="check",
        check_cols=[
            "product_name",
            "category",
            "subcategory",
            "brand",
            "unit_cost_mad",
            "unit_price_mad",
            "margin_pct",
            "launch_date",
            "is_active",
        ],
    )
}}

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
    is_active
from {{ ref('int_products') }}

{% endsnapshot %}
