"""Behavioral tests for the controlled synthetic data generator."""

from __future__ import annotations

import numpy as np
import pandas as pd

from mri.synthetic import generate, get_profile
from mri.synthetic.morocco import CITIES, REGIONS
from mri.synthetic.schema import (
    ENTITY_COLUMNS,
    NULLABLE_COLUMNS,
    PERIOD_END,
    PERIOD_START,
    PRIMARY_KEYS,
)

TINY = get_profile("tiny")
SMALL = get_profile("small")

MONEY_COLUMNS = (
    "unit_cost_mad",
    "unit_price_mad",
    "price_mad",
    "net_amount_mad",
    "cost_amount_mad",
    "margin_mad",
    "total_cost_mad",
    "stock_value_mad",
)


def _assert_subset(child: pd.Series, parent: pd.Series) -> None:
    values = set(child.dropna().unique())
    assert values <= set(parent.unique())


def test_all_entities_present(tiny_tables):
    assert set(tiny_tables) == set(ENTITY_COLUMNS)


def test_column_contract(tiny_tables):
    for entity, frame in tiny_tables.items():
        assert list(frame.columns) == list(ENTITY_COLUMNS[entity])


def test_generate_is_deterministic_for_same_seed(tiny_tables):
    second = generate(42, TINY)
    for entity, frame in tiny_tables.items():
        pd.testing.assert_frame_equal(frame, second[entity])


def test_different_seed_produces_different_data(tiny_tables):
    other = generate(43, TINY)
    assert not tiny_tables["sales"].equals(other["sales"])
    assert not tiny_tables["stores"].equals(other["stores"])


def test_actual_sales_row_count_matches_profile(tiny_tables, small_tables):
    assert len(tiny_tables["sales"]) == TINY.sales_rows
    assert len(small_tables["sales"]) == SMALL.sales_rows


def test_primary_keys_are_unique_and_not_null(tiny_tables):
    for entity, key in PRIMARY_KEYS.items():
        frame = tiny_tables[entity]
        assert frame[key].notna().all()
        if entity == "inventory":
            assert not frame.duplicated(["snapshot_date", "store_id", "product_id"]).any()
        else:
            assert not frame[key].duplicated().any()


def test_referential_integrity(tiny_tables):
    tables = tiny_tables
    _assert_subset(tables["stores"]["region_id"], tables["regions"]["region_id"])
    _assert_subset(tables["customers"]["region_id"], tables["regions"]["region_id"])
    _assert_subset(tables["customers"]["preferred_channel_id"], tables["channels"]["channel_id"])
    _assert_subset(tables["promotions"]["product_id"], tables["products"]["product_id"])
    _assert_subset(tables["promotions"]["region_id"], tables["regions"]["region_id"])
    _assert_subset(tables["promotions"]["channel_id"], tables["channels"]["channel_id"])

    sales = tables["sales"]
    _assert_subset(sales["store_id"], tables["stores"]["store_id"])
    _assert_subset(sales["product_id"], tables["products"]["product_id"])
    _assert_subset(sales["channel_id"], tables["channels"]["channel_id"])
    _assert_subset(sales["customer_id"], tables["customers"]["customer_id"])
    _assert_subset(sales["promotion_id"], tables["promotions"]["promotion_id"])

    _assert_subset(tables["purchases"]["store_id"], tables["stores"]["store_id"])
    _assert_subset(tables["purchases"]["product_id"], tables["products"]["product_id"])
    _assert_subset(tables["inventory"]["store_id"], tables["stores"]["store_id"])
    _assert_subset(tables["inventory"]["product_id"], tables["products"]["product_id"])


def test_no_nulls_in_required_columns(tiny_tables):
    for entity, frame in tiny_tables.items():
        nullable = set(NULLABLE_COLUMNS.get(entity, ()))
        required = [c for c in frame.columns if c not in nullable]
        assert frame[required].notna().all().all(), entity


def test_products_have_positive_price_above_cost(tiny_tables):
    products = tiny_tables["products"]
    assert (products["unit_cost_mad"] > 0).all()
    assert (products["unit_price_mad"] > products["unit_cost_mad"]).all()
    assert products["margin_pct"].between(0.0, 0.6).all()


def test_sales_numeric_ranges(tiny_tables):
    sales = tiny_tables["sales"]
    assert sales["quantity"].between(1, 20).all()
    assert (sales["unit_price_mad"] > 0).all()
    assert sales["discount_pct"].between(0.0, 0.45).all()
    assert (sales["net_amount_mad"] > 0).all()
    assert (sales["cost_amount_mad"] > 0).all()
    assert (sales["margin_mad"] >= 0).all()
    assert sales["hour_of_day"].between(8, 23).all()


def test_sales_amounts_are_consistent(tiny_tables):
    sales = tiny_tables["sales"].merge(
        tiny_tables["products"][["product_id", "unit_cost_mad"]], on="product_id", how="left"
    )
    expected_net = (
        sales["quantity"] * sales["unit_price_mad"] * (1 - sales["discount_pct"])
    ).round(2)
    assert np.allclose(sales["net_amount_mad"], expected_net, atol=0.011)
    expected_cost = (sales["quantity"] * sales["unit_cost_mad"]).round(2)
    assert np.allclose(sales["cost_amount_mad"], expected_cost, atol=0.011)
    assert np.allclose(
        sales["margin_mad"],
        (sales["net_amount_mad"] - sales["cost_amount_mad"]).clip(lower=0),
        atol=0.011,
    )


def test_sales_discount_never_exceeds_margin(tiny_tables):
    merged = tiny_tables["sales"].merge(
        tiny_tables["products"][["product_id", "margin_pct"]], on="product_id", how="left"
    )
    assert (merged["discount_pct"] <= merged["margin_pct"] * 0.9 + 1e-6).all()


def test_supply_numeric_ranges(tiny_tables):
    inventory = tiny_tables["inventory"]
    assert (inventory["quantity_on_hand"] >= 0).all()
    assert (inventory["stock_value_mad"] >= 0).all()
    assert set(inventory["stock_status"]) <= {"in_stock", "low", "out_of_stock"}
    assert (
        (inventory["stock_status"] == "out_of_stock") == (inventory["quantity_on_hand"] == 0)
    ).all()

    purchases = tiny_tables["purchases"]
    assert (purchases["quantity"] >= 1).all()
    assert (purchases["unit_cost_mad"] > 0).all()
    expected_total = (purchases["quantity"] * purchases["unit_cost_mad"]).round(2)
    assert np.allclose(purchases["total_cost_mad"], expected_total, atol=0.011)


def test_supply_values_track_product_cost(tiny_tables):
    inventory = tiny_tables["inventory"].merge(
        tiny_tables["products"][["product_id", "unit_cost_mad"]], on="product_id", how="left"
    )
    expected = (inventory["quantity_on_hand"] * inventory["unit_cost_mad"]).round(2)
    assert np.allclose(inventory["stock_value_mad"], expected, atol=0.02)


def test_date_ranges(tiny_tables):
    sales = tiny_tables["sales"]
    assert sales["sale_date"].between(PERIOD_START.isoformat(), PERIOD_END.isoformat()).all()
    purchases = tiny_tables["purchases"]
    assert (
        purchases["purchase_date"].between(PERIOD_START.isoformat(), PERIOD_END.isoformat()).all()
    )
    inventory = tiny_tables["inventory"]
    assert (
        inventory["snapshot_date"].between(PERIOD_START.isoformat(), PERIOD_END.isoformat()).all()
    )

    promotions = tiny_tables["promotions"]
    assert (promotions["start_date"] <= promotions["end_date"]).all()
    assert promotions["start_date"].between(PERIOD_START.isoformat(), PERIOD_END.isoformat()).all()
    assert promotions["end_date"].between(PERIOD_START.isoformat(), PERIOD_END.isoformat()).all()

    assert tiny_tables["customers"]["signup_date"].between("2017-01-01", "2024-06-30").all()
    assert tiny_tables["products"]["launch_date"].between("2015-01-01", "2020-12-31").all()
    assert tiny_tables["stores"]["open_date"].between("2015-01-01", "2020-12-31").all()


def test_sales_never_precede_customer_signup(tiny_tables):
    merged = (
        tiny_tables["sales"]
        .dropna(subset=["customer_id"])
        .merge(
            tiny_tables["customers"][["customer_id", "signup_date"]], on="customer_id", how="left"
        )
    )
    assert (merged["sale_date"] >= merged["signup_date"]).all()


def test_sales_never_precede_store_opening(tiny_tables):
    merged = tiny_tables["sales"].merge(
        tiny_tables["stores"][["store_id", "open_date"]], on="store_id", how="left"
    )
    assert (merged["sale_date"] >= merged["open_date"]).all()


def test_transactions_are_internally_consistent(tiny_tables):
    sales = tiny_tables["sales"]
    grouped = sales.groupby("transaction_id")
    assert (grouped["sale_date"].nunique() == 1).all()
    assert (grouped["store_id"].nunique() == 1).all()
    assert (grouped["channel_id"].nunique() == 1).all()
    assert (grouped["hour_of_day"].nunique() == 1).all()
    assert grouped.ngroups < len(sales)


def test_promotions_are_applied_to_sales(tiny_tables):
    sales = tiny_tables["sales"]
    promo_lines = sales.dropna(subset=["promotion_id"])
    assert not promo_lines.empty
    assert (promo_lines["discount_pct"] > 0).all()

    promos = tiny_tables["promotions"][
        ["promotion_id", "start_date", "end_date", "region_id", "channel_id"]
    ].rename(columns={"region_id": "promo_region", "channel_id": "promo_channel"})
    stores = tiny_tables["stores"][["store_id", "region_id"]].rename(
        columns={"region_id": "store_region"}
    )
    merged = promo_lines.merge(promos, on="promotion_id", how="left").merge(
        stores, on="store_id", how="left"
    )
    assert (merged["sale_date"] >= merged["start_date"]).all()
    assert (merged["sale_date"] <= merged["end_date"]).all()
    assert (
        merged["promo_region"].isna() | (merged["promo_region"] == merged["store_region"])
    ).all()
    assert (
        merged["promo_channel"].isna() | (merged["promo_channel"] == merged["channel_id"])
    ).all()


def test_promotions_have_realistic_discounts(tiny_tables):
    promotions = tiny_tables["promotions"]
    assert promotions["discount_pct"].between(0.04, 0.45).all()
    assert set(promotions["promotion_type"]) <= {
        "remise",
        "prix_promo",
        "bogo",
        "fidelite",
        "bundle",
    }


def test_product_popularity_is_concentrated(tiny_tables):
    counts = tiny_tables["sales"]["product_id"].value_counts()
    top_share = counts.head(max(1, len(counts) // 10)).sum() / counts.sum()
    assert top_share > 0.30


def test_store_demand_varies(tiny_tables):
    sales = tiny_tables["sales"]
    per_store = sales["store_id"].value_counts()
    assert per_store.size >= 2
    assert per_store.max() > per_store.min()


def test_regional_demand_varies(tiny_tables):
    merged = tiny_tables["sales"].merge(
        tiny_tables["stores"][["store_id", "region_id"]], on="store_id", how="left"
    )
    per_region = merged["region_id"].value_counts()
    assert per_region.size >= 2
    assert per_region.max() > per_region.min()


def test_seasonality_markers_present_in_sales(tiny_tables):
    sales = tiny_tables["sales"].merge(
        tiny_tables["calendar"], left_on="sale_date", right_on="date", how="left"
    )
    assert sales["is_ramadan"].any()
    assert sales["is_holiday"].any()
    assert sales["is_weekend"].any()
    weekend = sales[sales["is_weekend"]]
    weekday = sales[~sales["is_weekend"]]
    assert not weekend.empty and not weekday.empty


def test_anonymous_and_loyal_customer_behaviour(tiny_tables):
    sales = tiny_tables["sales"]
    anonymous_rate = sales["customer_id"].isna().mean()
    assert 0.05 < anonymous_rate < 0.15
    purchases = sales.dropna(subset=["customer_id"])
    per_customer = purchases["customer_id"].value_counts()
    assert per_customer.max() > per_customer.median()


def test_morocco_context(tiny_tables):
    regions = tiny_tables["regions"]
    assert set(regions["region_id"]) == {r[0] for r in REGIONS}
    assert len(regions) == 12
    assert len(tiny_tables["channels"]) == 3

    all_cities = {city for cities in CITIES.values() for city in cities}
    assert set(tiny_tables["stores"]["city"]) <= all_cities

    assert set(regions["region_name"]).issuperset(
        {"Casablanca-Settat", "Rabat-Sale-Kenitra", "Marrakech-Safi", "Dakhla-Oued Ed-Dahab"}
    )


def test_currency_columns_are_mad(tiny_tables):
    for entity, frame in tiny_tables.items():
        for column in frame.columns:
            if column in MONEY_COLUMNS:
                assert column.endswith("_mad"), (entity, column)


def test_weights_are_normalised(tiny_tables):
    assert abs(tiny_tables["regions"]["demand_weight"].sum() - 1.0) < 1e-4
    assert abs(tiny_tables["channels"]["demand_weight"].sum() - 1.0) < 1e-4
