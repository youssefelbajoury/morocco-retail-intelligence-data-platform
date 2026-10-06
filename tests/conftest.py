"""Shared pytest fixtures for the synthetic data generator test-suite."""

from __future__ import annotations

import pandas as pd
import pytest

from mri.synthetic import generate, get_profile


@pytest.fixture(scope="session")
def tiny_tables() -> dict[str, pd.DataFrame]:
    """One deterministic ``tiny`` dataset (seed 42) shared across the session."""
    return generate(42, get_profile("tiny"))


@pytest.fixture(scope="session")
def small_tables() -> dict[str, pd.DataFrame]:
    """One deterministic ``small`` dataset (seed 42) for scale checks."""
    return generate(42, get_profile("small"))
