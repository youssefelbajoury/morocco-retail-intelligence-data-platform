"""Open Food Facts client: keyless product-catalog enrichment by barcode.

Official constraints (verified in the v3 API documentation):

* reads need no API key, but a identifying User-Agent is mandatory —
  the shared :class:`~mri.ingestion.clients.http.HttpClient` sends the
  configured ``EXTERNAL_USER_AGENT`` for every request;
* reads are limited to 15 requests/minute/IP, which the default pacing of
  ``OPEN_FOOD_FACTS_MIN_INTERVAL`` (4.1 s) respects;
* production is ``world.openfoodfacts.org``; staging needs basic auth and is
  deliberately not used here.

Each fetched product produces a non-destructive mapping record
(``external_product_id`` / ``internal_product_id`` / ``source`` /
``mapping_status``). Matching is **exact barcode equality only** against the
optional internal index — no fuzzy merging, and internal synthetic products
are never modified.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from mri.ingestion.clients.base import FetchOptions, FetchOutcome, FetchRequest, selected
from mri.ingestion.clients.http import HttpClient, RateLimiter, RetryPolicy
from mri.ingestion.exceptions import ApiResponseError, UsageError

SOURCE = "openfoodfacts_products"
SOURCES: tuple[str, ...] = (SOURCE,)

PROVIDER = "Open Food Facts"
API_DOCS = "https://openfoodfacts.github.io/openfoodfacts-server/api/"
TERMS = "https://world.openfoodfacts.org/terms-of-use"
DATA_TYPE = "product_catalog"
API_VERSION_PATH = "api/v3"
MIN_BARCODE_LENGTH = 8
MAX_BARCODE_LENGTH = 14


def validate_barcode(barcode: str) -> str:
    """Accept plain numeric GTIN/EAN barcodes (8-14 digits), else fail as usage."""
    cleaned = barcode.strip()
    if not cleaned.isdigit() or not MIN_BARCODE_LENGTH <= len(cleaned) <= MAX_BARCODE_LENGTH:
        raise UsageError(
            f"invalid barcode {barcode!r}: expected {MIN_BARCODE_LENGTH}-"
            f"{MAX_BARCODE_LENGTH} digits (EAN-8/UPC-A/EAN-13/ITF-14)"
        )
    return cleaned


def build_mapping(
    barcode: str,
    *,
    internal_index: Mapping[str, str],
    found: bool,
) -> dict[str, str | None]:
    """Exact-barcode mapping record; ``MATCHED`` needs both sides present."""
    internal_id = internal_index.get(barcode)
    if found:
        status = "MATCHED" if internal_id else "NEW"
    else:
        status = "UNMATCHED"
    return {
        "external_product_id": barcode,
        "internal_product_id": internal_id,
        "source": SOURCE,
        "mapping_status": status,
    }


class ProductCatalogClient:
    """Builds and executes Open Food Facts product reads."""

    sources = SOURCES

    def __init__(
        self,
        http: HttpClient,
        *,
        base_url: str,
        internal_index: Mapping[str, str] | None = None,
    ) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._internal_index: dict[str, str] = dict(internal_index or {})

    @property
    def internal_index(self) -> Mapping[str, str]:
        return dict(self._internal_index)

    @staticmethod
    def default_http(
        *, user_agent: str, timeout: float, max_attempts: int, min_interval: float
    ) -> HttpClient:
        return HttpClient(
            user_agent=user_agent,
            timeout=timeout,
            retry=RetryPolicy(max_attempts=max_attempts),
            rate_limiter=RateLimiter(min_interval),
        )

    def build_requests(self, options: FetchOptions) -> list[FetchRequest]:
        if not selected(options, self.sources):
            return []
        if not options.barcodes:
            raise UsageError(
                "the products source needs at least one barcode (--barcode or --barcode-file)"
            )

        seen: dict[str, None] = {}
        for raw in options.barcodes:
            seen[validate_barcode(raw)] = None
        barcodes = list(seen)
        if options.limit is not None:
            barcodes = barcodes[: options.limit]

        requests: list[FetchRequest] = []
        for barcode in barcodes:
            context: dict[str, Any] = {
                "provider": PROVIDER,
                "api_docs": API_DOCS,
                "terms": TERMS,
                "license": "ODbL (database), DBCL (contents), CC BY-SA (images)",
                "api_key_required": False,
                "data_type": DATA_TYPE,
                "barcode": barcode,
                "min_interval_seconds": self._http.rate_limiter.min_interval
                if self._http.rate_limiter
                else None,
            }
            requests.append(
                FetchRequest(
                    source=SOURCE,
                    data_type=DATA_TYPE,
                    url=f"{self._base_url}/{API_VERSION_PATH}/product/{barcode}.json",
                    context=context,
                )
            )
        return requests

    def fetch(self, request: FetchRequest) -> FetchOutcome:
        barcode = str(request.context.get("barcode", ""))
        result = self._http.get_json(request.url, request.params)
        payload = result.payload
        if not isinstance(payload, dict) or not isinstance(payload.get("code"), (str, int)):
            raise ApiResponseError(
                f"unexpected Open Food Facts payload from {result.url}",
                reason="unexpected_payload",
                status_code=result.status_code,
            )
        product = payload.get("product")
        found = isinstance(product, dict) and bool(product)
        mapping = build_mapping(barcode, internal_index=self._internal_index, found=found)
        return FetchOutcome(
            request=request,
            payload=payload,
            raw=result.text.encode("utf-8"),
            status_code=result.status_code,
            attempts=result.attempts,
            elapsed_ms=result.elapsed_ms,
            found=found,
            mapping=mapping,
        )
