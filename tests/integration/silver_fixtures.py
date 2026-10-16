"""Shared session fixtures for the integration suite.

``silver_env`` generates the deterministic ``tiny`` dataset, promotes it once
through Bronze from an in-memory object store and exposes uniquely suffixed
``raw_staging_it*``, ``silver_it*`` and ``audit_it*`` PostgreSQL schemas which
are dropped again afterwards.  Every integration module (Silver, warehouse)
builds on top of this single session so the suite never touches the
development schemas and can run repeatedly.

The fixture only promotes Bronze; the actual Silver load is performed by the
first consumer (the Silver tests or the warehouse fixture), both of which are
idempotent, so module execution order does not matter.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path

import pandas as pd
import psycopg
import pytest

from mri.ingestion.bronze import BronzePipeline
from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract, load_all_contracts
from mri.ingestion.storage import InMemoryObjectStore
from mri.silver import conninfo_for
from mri.synthetic import generate, get_profile
from mri.synthetic.writer import write_dataset

RAW_TAG = "silverit"


@dataclass(frozen=True)
class SilverEnv:
    """Session-wide fixture state: settings, bronze store and row expectations."""

    settings: Settings
    store: InMemoryObjectStore
    contracts: dict[str, Contract]
    tables: dict[str, pd.DataFrame]
    expected: dict[str, int]
    root: Path


def drop_schemas(settings: Settings, *extra: str) -> None:
    schemas = (
        settings.staging_schema,
        settings.silver_schema,
        settings.audit_schema,
        *extra,
    )
    with psycopg.connect(conninfo_for(settings)) as conn:
        for schema in schemas:
            conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture(scope="session")
def silver_env(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SilverEnv]:
    base = Settings.from_env()
    if not base.postgres_password:
        pytest.skip("POSTGRES_PASSWORD not configured")
    tag = uuid.uuid4().hex[:8]
    root = tmp_path_factory.mktemp("silver-it")
    settings = replace(
        base,
        staging_schema=f"raw_staging_it{tag}",
        silver_schema=f"silver_it{tag}",
        audit_schema=f"audit_it{tag}",
        manifest_dir=root / "manifests",
        bronze_manifest_dir=root / "bronze-manifests",
        silver_manifest_dir=root / "silver-manifests",
    )
    try:
        with psycopg.connect(conninfo_for(settings)):
            pass
    except psycopg.Error as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    tables = generate(42, get_profile("tiny"))
    write_dataset(tables, root / "data", get_profile("tiny"), 42)
    store = InMemoryObjectStore()
    for entity in tables:
        csv_path = root / "data" / "tiny" / entity / f"{entity}.csv"
        store.put_bytes(
            settings.bucket_raw, f"raw/{entity}/{entity}__{RAW_TAG}.csv", csv_path.read_bytes()
        )
    contracts = load_all_contracts()
    bronze_result = BronzePipeline(store, settings, contracts).run()
    assert bronze_result.manifest.get("promoted") == len(tables), bronze_result.counts

    env = SilverEnv(
        settings=settings,
        store=store,
        contracts=contracts,
        tables=tables,
        expected={entity: int(len(frame)) for entity, frame in tables.items()},
        root=root,
    )
    try:
        yield env
    finally:
        drop_schemas(settings)


__all__ = ["RAW_TAG", "SilverEnv", "drop_schemas", "silver_env"]
