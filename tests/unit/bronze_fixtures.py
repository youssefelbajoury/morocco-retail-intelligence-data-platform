"""Shared fixture builders for the Bronze unit tests.

Contract-shaped CSV batches and object-store seeding helpers so the tests can
inject exact valid/invalid rows (including values the synthetic generator would
never produce) directly at the raw-object level.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

from mri.ingestion.bronze_schema import StandardizedBatch, parse_rows, standardize
from mri.ingestion.config import Settings
from mri.ingestion.contracts import load_all_contracts

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = load_all_contracts()
RAW_BUCKET = "retail-raw"
BRONZE_BUCKET = "retail-bronze"
DEFAULT_TAG = "0123456789abcdef"


def make_settings(tmp_path: Path) -> Settings:
    return Settings(
        minio_endpoint="localhost:9000",
        minio_access_key="unit-test-key",
        minio_secret_key="unit-test-secret",
        minio_secure=False,
        bucket_raw=RAW_BUCKET,
        bucket_bronze=BRONZE_BUCKET,
        contracts_dir=REPO_ROOT / "data_contracts",
        manifest_dir=tmp_path / "manifests",
        bronze_manifest_dir=tmp_path / "bronze" / "manifests",
    )


def csv_bytes(source: str, rows: list[dict[str, str]]) -> bytes:
    """Serialize rows under the contract's exact header (missing cells become ``""``)."""
    contract = CONTRACTS[source]
    header = [column.name for column in contract.columns]
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=header, extrasaction="ignore", restval="")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def raw_key(source: str, tag: str = DEFAULT_TAG) -> str:
    return f"raw/{source}/{source}__{tag}.csv"


def standardized(source: str, rows: list[dict[str, str]]) -> StandardizedBatch:
    contract = CONTRACTS[source]
    raw = parse_rows(csv_bytes(source, rows), contract, "csv")
    return standardize(raw, contract)


def region_row(**overrides: str) -> dict[str, str]:
    row = {
        "region_id": "R01",
        "region_name": "Casablanca-Settat",
        "demand_weight": "0.25",
    }
    row.update(overrides)
    return row


def channel_row(**overrides: str) -> dict[str, str]:
    row = {
        "channel_id": "CH01",
        "channel_name": "Flagship Store",
        "channel_type": "physical",
        "demand_weight": "0.5",
    }
    row.update(overrides)
    return row


def store_row(**overrides: str) -> dict[str, str]:
    row = {
        "store_id": "ST01",
        "store_name": "Gueliz Flagship",
        "city": "Marrakech",
        "region_id": "R01",
        "store_format": "hypermarket",
        "size_sqm": "3500",
        "open_date": "2019-06-01",
        "is_active": "true",
    }
    row.update(overrides)
    return row


def product_row(**overrides: str) -> dict[str, str]:
    row = {
        "product_id": "P000001",
        "product_name": "Atlas Argan Oil 500ml",
        "category": "grocery",
        "subcategory": "oils",
        "brand": "Atlas",
        "unit_cost_mad": "42.00",
        "unit_price_mad": "59.90",
        "margin_pct": "0.30",
        "launch_date": "2021-03-15",
        "is_active": "true",
    }
    row.update(overrides)
    return row


def sales_row(**overrides: str) -> dict[str, str]:
    row = {
        "sale_id": "S000000001",
        "transaction_id": "T00000001",
        "sale_date": "2024-06-15",
        "hour_of_day": "12",
        "store_id": "ST01",
        "channel_id": "CH01",
        "customer_id": "CU0001",
        "product_id": "P000001",
        "promotion_id": "",
        "quantity": "2",
        "unit_price_mad": "25.50",
        "discount_pct": "0.10",
        "net_amount_mad": "45.90",
        "cost_amount_mad": "30.00",
        "margin_mad": "15.90",
    }
    row.update(overrides)
    return row


def seed_raw(
    store: Any,
    settings: Settings,
    source: str,
    rows: list[dict[str, str]],
    tag: str = DEFAULT_TAG,
) -> str:
    key = raw_key(source, tag)
    store.put_bytes(settings.bucket_raw, key, csv_bytes(source, rows))
    return key


def seed_dimensions(store: Any, settings: Settings) -> dict[str, str]:
    """Seed the four dimension sources every fact reference resolves against."""
    return {
        "regions": seed_raw(store, settings, "regions", [region_row()]),
        "channels": seed_raw(store, settings, "channels", [channel_row()]),
        "stores": seed_raw(store, settings, "stores", [store_row()]),
        "products": seed_raw(store, settings, "products", [product_row()]),
    }
