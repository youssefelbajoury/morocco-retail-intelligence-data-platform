"""Output schema contract: ordered columns and primary keys per entity."""

from __future__ import annotations

from datetime import date

ENTITY_COLUMNS: dict[str, tuple[str, ...]] = {
    "regions": (
        "region_id",
        "region_name",
        "demand_weight",
    ),
    "channels": (
        "channel_id",
        "channel_name",
        "channel_type",
        "demand_weight",
    ),
    "stores": (
        "store_id",
        "store_name",
        "city",
        "region_id",
        "store_format",
        "size_sqm",
        "open_date",
        "is_active",
    ),
    "products": (
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "unit_cost_mad",
        "unit_price_mad",
        "margin_pct",
        "launch_date",
        "is_active",
    ),
    "customers": (
        "customer_id",
        "signup_date",
        "region_id",
        "birth_year",
        "gender",
        "segment",
        "loyalty_tier",
        "preferred_channel_id",
        "is_active",
    ),
    "promotions": (
        "promotion_id",
        "product_id",
        "region_id",
        "channel_id",
        "promotion_type",
        "discount_pct",
        "start_date",
        "end_date",
    ),
    "sales": (
        "sale_id",
        "transaction_id",
        "sale_date",
        "hour_of_day",
        "store_id",
        "channel_id",
        "customer_id",
        "product_id",
        "promotion_id",
        "quantity",
        "unit_price_mad",
        "discount_pct",
        "net_amount_mad",
        "cost_amount_mad",
        "margin_mad",
    ),
    "purchases": (
        "purchase_id",
        "purchase_date",
        "store_id",
        "product_id",
        "quantity",
        "unit_cost_mad",
        "total_cost_mad",
    ),
    "inventory": (
        "snapshot_date",
        "store_id",
        "product_id",
        "quantity_on_hand",
        "stock_value_mad",
        "stock_status",
    ),
    "calendar": (
        "date",
        "year",
        "month",
        "quarter",
        "iso_week",
        "weekday",
        "weekday_name",
        "is_weekend",
        "is_holiday",
        "holiday_name",
        "is_ramadan",
        "is_eid_al_fitr",
        "is_eid_al_adha",
        "season",
    ),
}

PRIMARY_KEYS: dict[str, str] = {
    "regions": "region_id",
    "channels": "channel_id",
    "stores": "store_id",
    "products": "product_id",
    "customers": "customer_id",
    "promotions": "promotion_id",
    "sales": "sale_id",
    "purchases": "purchase_id",
    "inventory": "snapshot_date",  # composite: snapshot_date + store_id + product_id
    "calendar": "date",
}

# Columns that are legitimately nullable in sales / dimension scope columns.
NULLABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "sales": ("customer_id", "promotion_id"),
    "promotions": ("region_id", "channel_id"),
    "inventory": ("stock_status",),
    "calendar": ("holiday_name",),
}

PERIOD_START = date(2021, 1, 1)
PERIOD_END = date(2024, 12, 31)

ENTITIES: tuple[str, ...] = tuple(ENTITY_COLUMNS)
