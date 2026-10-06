"""Scale profile definitions."""

from __future__ import annotations

import pytest

from mri.synthetic.profiles import PROFILE_NAMES, SCALE_PROFILES, get_profile


def test_required_profiles_exist():
    for name in ("small", "medium", "large", "xl"):
        assert name in SCALE_PROFILES


def test_profile_names_are_stable():
    assert PROFILE_NAMES == ("tiny", "small", "medium", "large", "xl")


def test_profiles_have_positive_configuration():
    for profile in SCALE_PROFILES.values():
        assert profile.sales_rows > 0
        assert profile.product_count > 0
        assert profile.store_count > 0
        assert profile.customer_count > 0
        assert profile.promotion_count > 0
        assert 0 < profile.assortment_ratio <= 1
        assert profile.snapshot_days > 0
        assert profile.chunk_rows > 0


def test_profiles_scale_up_monotonically():
    sales = [SCALE_PROFILES[n].sales_rows for n in PROFILE_NAMES]
    assert sales == sorted(sales)
    assert len(set(sales)) == len(sales)


def test_get_profile_returns_named_profile():
    assert get_profile("medium").name == "medium"


def test_get_profile_unknown_name_raises():
    with pytest.raises(ValueError, match="unknown scale profile"):
        get_profile("huge")
