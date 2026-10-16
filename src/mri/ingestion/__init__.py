"""File and external-API ingestion layer for the Morocco Retail Intelligence platform.

Public surface:

- :class:`mri.ingestion.contracts.Contract` / :func:`load_all_contracts`
- :class:`mri.ingestion.pipeline.IngestionPipeline`
- :class:`mri.ingestion.external.ExternalPipeline`
- :class:`mri.ingestion.clients.WeatherClient` / :class:`mri.ingestion.clients.FxClient`
  / :class:`mri.ingestion.clients.ProductCatalogClient`
- :func:`mri.ingestion.cli.main`
"""

from __future__ import annotations

from mri.ingestion.clients import FxClient, ProductCatalogClient, WeatherClient
from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract, load_all_contracts, load_contract
from mri.ingestion.external import ExternalPipeline, ExternalRunResult
from mri.ingestion.pipeline import IngestionPipeline, RunResult

__all__ = [
    "Contract",
    "ExternalPipeline",
    "ExternalRunResult",
    "FxClient",
    "IngestionPipeline",
    "ProductCatalogClient",
    "RunResult",
    "Settings",
    "WeatherClient",
    "load_all_contracts",
    "load_contract",
]
