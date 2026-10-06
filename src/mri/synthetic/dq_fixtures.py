"""Optional, controlled data-quality test fixtures — kept out of data/generated.

Each scenario starts from a clean ``tiny`` baseline and applies ONE documented
corruption class. Fixtures are written to ``<dq_output>/<scenario>/`` and are
never mixed with the production-like generated dataset.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from mri.synthetic.generator import generate
from mri.synthetic.profiles import get_profile

SCENARIOS: tuple[str, ...] = (
    "null_required",
    "out_of_range",
    "bad_dates",
    "duplicate_keys",
    "schema_drift",
)


def build_fixture(scenario: str, seed: int = 42) -> dict[str, pd.DataFrame]:
    """Return a corrupted copy of the clean ``tiny`` baseline for ``scenario``."""
    if scenario not in SCENARIOS:
        raise ValueError(
            f"unknown data-quality scenario {scenario!r}; expected one of {', '.join(SCENARIOS)}"
        )
    tables = {name: frame.copy() for name, frame in generate(seed, get_profile("tiny")).items()}

    if scenario == "null_required":
        sales = tables["sales"]
        sales.loc[sales.index[:5], "store_id"] = None
        sales.loc[sales.index[5:10], "product_id"] = None
        sales.loc[sales.index[10:15], "sale_date"] = None
        products = tables["products"]
        products.loc[products.index[:3], "category"] = None
        products.loc[products.index[3:6], "unit_price_mad"] = None
        tables["stores"].loc[tables["stores"].index[:2], "region_id"] = None

    elif scenario == "out_of_range":
        sales = tables["sales"]
        sales.loc[sales.index[:3], "quantity"] = -3
        sales.loc[sales.index[3:6], "quantity"] = 999
        sales.loc[sales.index[6:9], "unit_price_mad"] = -5.0
        sales.loc[sales.index[9:12], "discount_pct"] = 1.5
        sales.loc[sales.index[12:15], "margin_mad"] = -50.0
        products = tables["products"]
        products.loc[products.index[:3], "margin_pct"] = 1.5
        products.loc[products.index[3:6], "unit_price_mad"] = -10.0
        customers = tables["customers"]
        customers.loc[customers.index[:2], "birth_year"] = 1850
        customers.loc[customers.index[2:4], "birth_year"] = 3000

    elif scenario == "bad_dates":
        sales = tables["sales"]
        sales.loc[sales.index[:5], "sale_date"] = "2024-13-45"
        sales.loc[sales.index[5:10], "sale_date"] = "1999-01-01"
        products = tables["products"]
        products.loc[products.index[:3], "launch_date"] = "not-a-date"
        promotions = tables["promotions"]
        promotions.loc[promotions.index[:2], "start_date"] = "not-a-date"

    elif scenario == "duplicate_keys":
        sales = tables["sales"]
        tables["sales"] = pd.concat([sales, sales.head(10)], ignore_index=True)
        stores = tables["stores"]
        tables["stores"] = pd.concat([stores, stores.head(2)], ignore_index=True)

    elif scenario == "schema_drift":
        sales = tables["sales"].drop(columns=["margin_mad"])
        sales["unexpected_col"] = 1
        tables["sales"] = sales
        products = tables["products"].rename(columns={"unit_price_mad": "price_mad"})
        tables["products"] = products

    return tables


def write_fixture(scenario: str, output_root: Path | str, seed: int = 42) -> list[Path]:
    """Write the corrupted fixture dataset to ``<output_root>/<scenario>/``."""
    tables = build_fixture(scenario, seed)
    base = Path(output_root) / scenario
    paths: list[Path] = []
    for entity, frame in tables.items():
        entity_dir = base / entity
        entity_dir.mkdir(parents=True, exist_ok=True)
        path = entity_dir / f"{entity}.csv"
        frame.to_csv(path, index=False)
        paths.append(path)
    return paths
