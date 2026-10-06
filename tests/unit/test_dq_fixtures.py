"""Controlled data-quality fixtures: isolated from the normal dataset."""

from __future__ import annotations

import pandas as pd
import pytest

from mri.synthetic.dq_fixtures import SCENARIOS, build_fixture, write_fixture


def test_expected_scenarios_are_declared():
    assert SCENARIOS == (
        "null_required",
        "out_of_range",
        "bad_dates",
        "duplicate_keys",
        "schema_drift",
    )


def test_unknown_scenario_raises():
    with pytest.raises(ValueError, match="unknown data-quality scenario"):
        build_fixture("meltdown")


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_fixture_writes_under_its_own_root(tmp_path, scenario):
    paths = write_fixture(scenario, tmp_path)
    assert paths
    for path in paths:
        assert path.exists()
        assert tmp_path in path.parents


def test_null_required_fixture_has_nulls():
    tables = build_fixture("null_required")
    assert tables["sales"]["store_id"].isna().any()
    assert tables["sales"]["sale_date"].isna().any()
    assert tables["products"]["category"].isna().any()
    assert tables["stores"]["region_id"].isna().any()


def test_out_of_range_fixture_has_invalid_values():
    tables = build_fixture("out_of_range")
    assert (tables["sales"]["quantity"] < 0).any()
    assert (tables["sales"]["discount_pct"] > 1).any()
    assert (tables["sales"]["margin_mad"] < 0).any()
    assert (tables["customers"]["birth_year"] > 2026).any()


def test_bad_dates_fixture_has_invalid_dates():
    tables = build_fixture("bad_dates")
    assert (tables["sales"]["sale_date"] == "2024-13-45").any()
    assert (tables["sales"]["sale_date"] == "1999-01-01").any()
    assert (tables["products"]["launch_date"] == "not-a-date").any()


def test_duplicate_keys_fixture_has_duplicates():
    tables = build_fixture("duplicate_keys")
    assert tables["sales"]["sale_id"].duplicated().any()
    assert tables["stores"]["store_id"].duplicated().any()


def test_schema_drift_fixture_changes_schema():
    tables = build_fixture("schema_drift")
    assert "margin_mad" not in tables["sales"].columns
    assert "unexpected_col" in tables["sales"].columns
    assert "price_mad" in tables["products"].columns
    assert "unit_price_mad" not in tables["products"].columns


def test_fixture_is_based_on_a_valid_baseline(tiny_tables):
    baseline_sales = len(tiny_tables["sales"])
    assert len(build_fixture("out_of_range")["sales"]) == baseline_sales
    assert not tiny_tables["sales"]["store_id"].isna().any()
    assert not pd.Series(tiny_tables["sales"]["sale_id"]).duplicated().any()
