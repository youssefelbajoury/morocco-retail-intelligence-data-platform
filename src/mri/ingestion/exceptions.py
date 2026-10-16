"""Structured exceptions for the ingestion pipeline."""

from __future__ import annotations

from typing import Any


class IngestionError(Exception):
    """Base class for every error raised by the ingestion package."""


class ContractError(IngestionError):
    """A contract file is missing, malformed, or declares an unsupported version."""


class ContractNotFoundError(ContractError):
    """No contract could be resolved for the requested source."""


class SourceFileError(IngestionError):
    """A source file is missing or cannot be read."""


class ObjectStoreError(IngestionError):
    """The object store rejected an operation."""


class ManifestError(IngestionError):
    """A run manifest could not be written or read."""


class ValidationFailure(IngestionError):
    """Raised only for programmer errors; data problems use ``ValidationResult``."""

    def __init__(self, message: str, details: Any | None = None) -> None:
        super().__init__(message)
        self.details = details


class ExternalApiError(IngestionError):
    """An external API request failed permanently (never silently swallowed)."""


class ApiRetryError(ExternalApiError):
    """A retryable condition (timeout / network / 429 / 5xx) survived every attempt."""

    def __init__(
        self,
        message: str,
        *,
        attempts: int,
        status_code: int | None = None,
        reason: str = "retries_exhausted",
    ) -> None:
        super().__init__(message)
        self.attempts = attempts
        self.status_code = status_code
        self.reason = reason


class ApiResponseError(ExternalApiError):
    """A response was received but cannot be used as-is (permanent, not retried)."""

    def __init__(
        self,
        message: str,
        *,
        reason: str = "http_error",
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.reason = reason
        self.status_code = status_code


class UsageError(IngestionError):
    """The caller supplied invalid arguments (maps to CLI exit code 2)."""


class SilverLoadError(IngestionError):
    """The Silver loader rejected a batch or could not complete a load."""


class WarehouseError(IngestionError):
    """The warehouse build could not complete or PostgreSQL rejected it."""
