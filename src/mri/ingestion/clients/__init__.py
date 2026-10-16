"""External API clients for the Morocco Retail Intelligence platform.

Each client is keyless, rate-aware and returns raw payloads untouched; the
shared transport lives in :mod:`mri.ingestion.clients.http`.
"""

from __future__ import annotations

from mri.ingestion.clients.base import (
    ExternalClient,
    FetchOptions,
    FetchOutcome,
    FetchRequest,
)
from mri.ingestion.clients.fx_client import FxClient
from mri.ingestion.clients.http import HttpClient, RateLimiter, RetryPolicy
from mri.ingestion.clients.locations import LOCATIONS, Location, resolve_locations
from mri.ingestion.clients.product_catalog_client import (
    ProductCatalogClient,
    build_mapping,
    validate_barcode,
)
from mri.ingestion.clients.weather_client import WeatherClient

__all__ = [
    "ExternalClient",
    "FetchOptions",
    "FetchOutcome",
    "FetchRequest",
    "FxClient",
    "HttpClient",
    "LOCATIONS",
    "Location",
    "ProductCatalogClient",
    "RateLimiter",
    "RetryPolicy",
    "WeatherClient",
    "build_mapping",
    "resolve_locations",
    "validate_barcode",
]
