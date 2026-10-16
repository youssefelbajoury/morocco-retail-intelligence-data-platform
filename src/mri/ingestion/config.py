"""Runtime settings for the ingestion pipeline, resolved from the environment.

Credentials are never logged. Values are read from (in order of precedence):
the process environment, the repository ``.env`` file, and built-in defaults.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


def find_repo_root(start: Path | None = None) -> Path:
    """Walk upwards from ``start`` until a directory containing pyproject.toml."""
    current = (start or Path(__file__)).resolve()
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").is_file():
            return candidate
    return Path.cwd().resolve()


def read_dotenv(path: Path) -> dict[str, str]:
    """Minimal ``.env`` reader (``KEY=VALUE`` lines, ``#`` comments)."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        values[key.strip()] = value
    return values


@dataclass(frozen=True)
class Settings:
    """Resolved settings for one ingestion process."""

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    bucket_raw: str
    bucket_bronze: str
    contracts_dir: Path
    manifest_dir: Path
    raw_prefix: str = "raw"
    rejected_prefix: str = "rejected/contracts"
    manifest_prefix: str = "_manifests"
    external_prefix: str = "raw_external"
    bronze_prefix: str = "bronze"
    quality_prefix: str = "_quality"
    promoted_prefix: str = "_promoted"
    rejected_bronze_prefix: str = "_rejected"
    bronze_manifest_prefix: str = "_manifests"
    bronze_manifest_dir: Path = Path("reports/bronze")
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "retail"
    postgres_user: str = "retail_app"
    postgres_password: str = ""
    postgres_sslmode: str = "prefer"
    staging_schema: str = "raw_staging"
    silver_schema: str = "silver"
    audit_schema: str = "audit"
    warehouse_schema: str = "warehouse"
    silver_manifest_dir: Path = Path("reports/silver")
    observability_dir: Path = Path("reports/observability")
    open_meteo_base_url: str = "https://api.open-meteo.com"
    open_meteo_archive_base_url: str = "https://archive-api.open-meteo.com"
    frankfurter_base_url: str = "https://api.frankfurter.dev"
    open_food_facts_base_url: str = "https://world.openfoodfacts.org"
    external_user_agent: str = "MoroccoRetailIntelligence/0.1.0 (noreply@example.invalid)"
    external_timeout_seconds: float = 20.0
    external_max_attempts: int = 4
    open_food_facts_min_interval: float = 4.1

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        repo_root: Path | None = None,
        contracts_dir: Path | None = None,
        manifest_dir: Path | None = None,
        bronze_manifest_dir: Path | None = None,
        silver_manifest_dir: Path | None = None,
        observability_dir: Path | None = None,
    ) -> Settings:
        root = repo_root or find_repo_root()
        dotenv = read_dotenv(root / ".env")
        merged: dict[str, str] = {**dotenv, **os.environ}
        if env:
            merged.update(env)

        endpoint_raw = merged.get("MINIO_ENDPOINT", "http://localhost:9000").strip()
        secure = endpoint_raw.lower().startswith("https")
        endpoint = endpoint_raw.split("://", 1)[-1].rstrip("/")

        contact = merged.get("EXTERNAL_CONTACT_EMAIL", "").strip()
        default_ua = f"MoroccoRetailIntelligence/0.1.0 ({contact or 'noreply@example.invalid'})"

        return cls(
            minio_endpoint=endpoint,
            minio_access_key=merged.get("MINIO_ACCESS_KEY", ""),
            minio_secret_key=merged.get("MINIO_SECRET_KEY", ""),
            minio_secure=secure,
            bucket_raw=merged.get("MINIO_BUCKET_RAW", "retail-raw"),
            bucket_bronze=merged.get("MINIO_BUCKET_BRONZE", "retail-bronze"),
            contracts_dir=contracts_dir or (root / "data_contracts"),
            manifest_dir=manifest_dir or (root / "reports" / "ingestion"),
            bronze_manifest_dir=bronze_manifest_dir or (root / "reports" / "bronze"),
            silver_manifest_dir=silver_manifest_dir or (root / "reports" / "silver"),
            observability_dir=observability_dir or (root / "reports" / "observability"),
            postgres_host=merged.get("POSTGRES_HOST", "localhost").strip() or "localhost",
            postgres_port=_int_env(merged, "POSTGRES_PORT", 5432),
            postgres_db=merged.get("POSTGRES_DB", "retail").strip() or "retail",
            postgres_user=merged.get("POSTGRES_USER", "retail_app").strip() or "retail_app",
            postgres_password=merged.get("POSTGRES_PASSWORD", ""),
            postgres_sslmode=merged.get("POSTGRES_SSLMODE", "prefer").strip() or "prefer",
            staging_schema=_schema_env(merged, "PG_STAGING_SCHEMA", "raw_staging"),
            silver_schema=_schema_env(merged, "PG_SILVER_SCHEMA", "silver"),
            audit_schema=_schema_env(merged, "PG_AUDIT_SCHEMA", "audit"),
            warehouse_schema=_schema_env(merged, "PG_WAREHOUSE_SCHEMA", "warehouse"),
            external_prefix=(
                merged.get("EXTERNAL_RAW_PREFIX", "raw_external").strip("/") or "raw_external"
            ),
            open_meteo_base_url=merged.get("OPEN_METEO_BASE_URL", "").rstrip("/")
            or "https://api.open-meteo.com",
            open_meteo_archive_base_url=merged.get("OPEN_METEO_ARCHIVE_BASE_URL", "").rstrip("/")
            or "https://archive-api.open-meteo.com",
            frankfurter_base_url=merged.get("FRANKFURTER_BASE_URL", "").rstrip("/")
            or "https://api.frankfurter.dev",
            open_food_facts_base_url=merged.get("OPEN_FOOD_FACTS_BASE_URL", "").rstrip("/")
            or "https://world.openfoodfacts.org",
            external_user_agent=merged.get("EXTERNAL_USER_AGENT", "").strip() or default_ua,
            external_timeout_seconds=_float_env(merged, "EXTERNAL_TIMEOUT_SECONDS", 20.0),
            external_max_attempts=_int_env(merged, "EXTERNAL_MAX_ATTEMPTS", 4),
            open_food_facts_min_interval=_float_env(merged, "OPEN_FOOD_FACTS_MIN_INTERVAL", 4.1),
        )


def _float_env(values: Mapping[str, str], key: str, default: float) -> float:
    raw = values.get(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"{key} must be a number, got {raw!r}") from None


def _int_env(values: Mapping[str, str], key: str, default: int) -> int:
    raw = values.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{key} must be an integer, got {raw!r}") from None


def _schema_env(values: Mapping[str, str], key: str, default: str) -> str:
    return values.get(key, "").strip().strip("/") or default
