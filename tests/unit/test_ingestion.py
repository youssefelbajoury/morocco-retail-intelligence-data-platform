"""Unit tests for contracts, discovery, validation and the ingestion pipeline."""

from __future__ import annotations

from pathlib import Path

import pytest

from mri.ingestion.config import Settings
from mri.ingestion.contracts import ContractError, load_all_contracts, load_contract, parse_version
from mri.ingestion.discovery import detect_format, infer_source, sha256_file
from mri.ingestion.exceptions import ContractNotFoundError, SourceFileError
from mri.ingestion.pipeline import IngestionPipeline, content_addressed_key
from mri.ingestion.storage import InMemoryObjectStore
from mri.ingestion.validation import validate_file

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = REPO_ROOT / "data_contracts"
BUCKET = "retail-raw"


def _pipeline(
    tmp_path: Path, *, dry_run: bool = False
) -> tuple[IngestionPipeline, InMemoryObjectStore]:
    settings = Settings(
        minio_endpoint="localhost:9000",
        minio_access_key="k",
        minio_secret_key="s",
        minio_secure=False,
        bucket_raw="retail-raw",
        bucket_bronze="retail-bronze",
        contracts_dir=REPO_ROOT / "data_contracts",
        manifest_dir=tmp_path / "manifests",
    )
    store = InMemoryObjectStore()
    pipeline = IngestionPipeline(store, settings, load_all_contracts(), dry_run=dry_run)
    return pipeline, store


def _write(path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_all_contracts_load() -> None:
    contracts = load_all_contracts()
    assert set(contracts) == {
        "regions",
        "channels",
        "stores",
        "products",
        "customers",
        "promotions",
        "sales",
        "purchases",
        "inventory",
        "calendar",
        "open_meteo_history",
        "open_meteo_forecast",
        "frankfurter_fx",
        "openfoodfacts_products",
    }
    sales = contracts["sales"]
    assert sales.version == "1.0.0"
    assert sales.primary_key == ("sale_id",)
    assert sales.column_map()["customer_id"].nullable is True
    assert sales.column_map()["sale_id"].nullable is False
    assert "sale_date" in sales.required_columns
    fx = contracts["frankfurter_fx"]
    assert fx.format_types == ("json",)
    assert fx.primary_key == ("date", "base", "quote")
    assert fx.column_map()["rate"].type == "number"


def test_unknown_contract_raises(tmp_path) -> None:
    empty = tmp_path / "contracts"
    empty.mkdir()
    with pytest.raises(ContractNotFoundError):
        load_contract("does_not_exist", empty)


def test_contract_version_parses() -> None:
    assert load_all_contracts()["regions"].version_tuple == (1, 0, 0)


def test_bad_version_raises() -> None:
    with pytest.raises(ContractError):
        parse_version("v1")


def test_infer_source() -> None:
    known = ("sales", "stores", "calendar")
    assert infer_source(Path("sales.csv"), known) == "sales"
    assert infer_source(Path("sales_2021.csv"), known) == "sales"
    assert infer_source(Path("manifest.json"), known) is None


def test_detect_format() -> None:
    assert detect_format(Path("a.csv")) == "csv"
    assert detect_format(Path("a.JSON")) == "json"
    with pytest.raises(SourceFileError):
        detect_format(Path("a.txt"))


def test_sha256_file_is_stable(tmp_path) -> None:
    path = tmp_path / "x.txt"
    _write(path, "hello")
    assert sha256_file(path) == sha256_file(path)


def test_validate_valid(tmp_path) -> None:
    path = tmp_path / "regions.csv"
    _write(path, "region_id,region_name,demand_weight\nR01,Alpha,0.5\nR02,Beta,0.4\n")
    result = validate_file(path, load_contract("regions"), "csv")
    assert result.ok
    assert result.row_count == 2
    assert result.issues == ()


def test_validate_missing_column(tmp_path) -> None:
    path = tmp_path / "regions.csv"
    _write(path, "region_id,region_name\nR01,Alpha\n")
    result = validate_file(path, load_contract("regions"), "csv")
    assert not result.ok
    assert "MISSING_COLUMN" in {issue.code for issue in result.issues}


def test_validate_type_mismatch(tmp_path) -> None:
    path = tmp_path / "regions.csv"
    _write(path, "region_id,region_name,demand_weight\nR01,Alpha,not-a-number\n")
    result = validate_file(path, load_contract("regions"), "csv")
    assert not result.ok
    assert any(issue.code == "TYPE_MISMATCH" for issue in result.issues)


def test_validate_nullable_customer_id(tmp_path) -> None:
    path = tmp_path / "sales.csv"
    header = (
        "sale_id,transaction_id,sale_date,hour_of_day,store_id,channel_id,customer_id,"
        "product_id,promotion_id,quantity,unit_price_mad,discount_pct,net_amount_mad,"
        "cost_amount_mad,margin_mad"
    )
    row = "S1,T1,2021-01-01,10,ST001,CH01,,P000001,,2,10.0,0.0,20.0,15.0,5.0"
    _write(path, f"{header}\n{row}\n")
    result = validate_file(path, load_contract("sales"), "csv")
    assert result.ok


def test_validate_json_array(tmp_path) -> None:
    path = tmp_path / "regions.json"
    _write(path, '[{"region_id":"R01","region_name":"A","demand_weight":0.5}]')
    result = validate_file(path, load_contract("regions"), "json")
    assert result.ok
    assert result.row_count == 1


def test_pipeline_ingest_and_idempotence(tmp_path) -> None:
    pipeline, store = _pipeline(tmp_path)
    path = tmp_path / "regions.csv"
    _write(path, "region_id,region_name,demand_weight\nR01,Alpha,0.5\n")

    first = pipeline.ingest_path(path, source="regions")
    assert len(first.ingested) == 1
    key = first.records[0].object_key
    assert key is not None and store.object_exists(BUCKET, key)
    assert first.manifest_path is not None and first.manifest_path.is_file()

    second = pipeline.ingest_path(path, source="regions")
    assert second.records[0].outcome == "already_ingested"


def test_pipeline_rejects_and_preserves(tmp_path) -> None:
    pipeline, store = _pipeline(tmp_path)
    path = tmp_path / "regions.csv"
    _write(path, "region_id,region_name\nR01,Alpha\n")

    result = pipeline.ingest_path(path, source="regions")
    assert len(result.rejected) == 1
    assert result.manifest["counts"]["rejected"] == 1
    rejected_key = result.records[0].rejected_key
    assert rejected_key is not None
    assert store.object_exists(BUCKET, rejected_key)
    assert store.object_exists(BUCKET, f"{rejected_key}.reason.json")
    assert "MISSING_COLUMN" in (result.records[0].reason or "")


def test_pipeline_dry_run_writes_nothing(tmp_path) -> None:
    pipeline, store = _pipeline(tmp_path, dry_run=True)
    path = tmp_path / "regions.csv"
    _write(path, "region_id,region_name,demand_weight\nR01,Alpha,0.5\n")
    result = pipeline.ingest_path(path, source="regions")
    assert result.records[0].outcome == "ingested"
    assert store.list_keys(BUCKET) == []
    assert result.manifest_path is None


def test_content_addressed_key_is_stable() -> None:
    digest = "0123456789abcdef" + "0" * 48
    assert (
        content_addressed_key("raw", "sales", "sales.csv", digest)
        == "raw/sales/sales__0123456789abcdef.csv"
    )
