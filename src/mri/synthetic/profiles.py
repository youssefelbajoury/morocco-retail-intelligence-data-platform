"""Scale profiles for the synthetic data generator.

Record counts below are **targets documented here**; the generator reports the
*actual* counts in its manifest and console output for every run.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ScaleProfile:
    """Configuration of one dataset scale."""

    name: str
    sales_rows: int
    product_count: int
    store_count: int
    customer_count: int
    promotion_count: int
    assortment_ratio: float  # fraction of products stocked per store
    snapshot_days: int  # inventory snapshot + replenishment cadence (days)
    chunk_rows: int  # sales rows generated per chunk (memory bound)

    @property
    def sales_label(self) -> str:
        return f"{self.sales_rows:,}"


SCALE_PROFILES: dict[str, ScaleProfile] = {
    p.name: p
    for p in (
        ScaleProfile(
            name="tiny",
            sales_rows=2_000,
            product_count=60,
            store_count=4,
            customer_count=200,
            promotion_count=20,
            assortment_ratio=0.5,
            snapshot_days=30,
            chunk_rows=1_000_000,
        ),
        ScaleProfile(
            name="small",
            sales_rows=60_000,
            product_count=200,
            store_count=10,
            customer_count=3_000,
            promotion_count=150,
            assortment_ratio=0.5,
            snapshot_days=30,
            chunk_rows=1_000_000,
        ),
        ScaleProfile(
            name="medium",
            sales_rows=600_000,
            product_count=600,
            store_count=24,
            customer_count=25_000,
            promotion_count=800,
            assortment_ratio=0.4,
            snapshot_days=14,
            chunk_rows=1_000_000,
        ),
        ScaleProfile(
            name="large",
            sales_rows=3_000_000,
            product_count=1_200,
            store_count=40,
            customer_count=120_000,
            promotion_count=3_000,
            assortment_ratio=0.4,
            snapshot_days=14,
            chunk_rows=1_000_000,
        ),
        ScaleProfile(
            name="xl",
            sales_rows=8_000_000,
            product_count=2_500,
            store_count=60,
            customer_count=400_000,
            promotion_count=8_000,
            assortment_ratio=0.35,
            snapshot_days=14,
            chunk_rows=1_000_000,
        ),
    )
}

PROFILE_NAMES: tuple[str, ...] = tuple(SCALE_PROFILES)


def get_profile(name: str) -> ScaleProfile:
    """Return a profile by name or raise with the list of valid names."""
    try:
        return SCALE_PROFILES[name]
    except KeyError as exc:
        raise ValueError(
            f"unknown scale profile {name!r}; expected one of {', '.join(PROFILE_NAMES)}"
        ) from exc
