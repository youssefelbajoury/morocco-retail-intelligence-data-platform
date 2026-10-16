"""Environment-driven settings for the metrics exporter.

Deliberately tiny and dependency-light: it reads only what the exporter needs
(PostgreSQL destinations and the HTTP bind address) from the process
environment, falling back to the repository ``.env`` when run on the host. It
does not import the heavier ingestion ``Settings`` so the exporter image can
ship nothing but ``psycopg``.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from psycopg.conninfo import make_conninfo

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9245

try:  # pragma: no cover - version import is trivial
    from mri import __version__ as _VERSION
except Exception:  # noqa: BLE001 - never let import errors break the exporter
    _VERSION = "0.0.0"


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
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


@dataclass(frozen=True)
class MetricsSettings:
    """Resolved exporter settings (never contains the log-file paths)."""

    host: str
    port: int
    retail_dsn: str
    airflow_dsn: str
    version: str

    @property
    def airflow_configured(self) -> bool:
        return bool(self.airflow_dsn)

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        repo_root: Path | None = None,
    ) -> MetricsSettings:
        root = repo_root or find_repo_root()
        merged: dict[str, str] = {**read_dotenv(root / ".env"), **os.environ}
        if env:
            merged.update(env)

        retail_dsn = _dsn(
            merged,
            host_key="POSTGRES_HOST",
            port_key="POSTGRES_PORT",
            db_key="POSTGRES_DB",
            user_key="POSTGRES_USER",
            password_key="POSTGRES_PASSWORD",
            sslmode_key="POSTGRES_SSLMODE",
            defaults=("localhost", "5432", "retail", "retail_app", "", "prefer"),
        )
        airflow_dsn = ""
        if merged.get("AIRFLOW_POSTGRES_DB", "").strip():
            airflow_dsn = _dsn(
                merged,
                host_key="AIRFLOW_POSTGRES_HOST",
                port_key="AIRFLOW_POSTGRES_PORT",
                db_key="AIRFLOW_POSTGRES_DB",
                user_key="AIRFLOW_POSTGRES_USER",
                password_key="AIRFLOW_POSTGRES_PASSWORD",
                sslmode_key="AIRFLOW_POSTGRES_SSLMODE",
                defaults=("localhost", "5434", "airflow", "airflow", "", "prefer"),
            )
        return cls(
            host=merged.get("MRI_METRICS_HOST", "").strip() or DEFAULT_HOST,
            port=_int_env(merged, "MRI_METRICS_PORT", DEFAULT_PORT),
            retail_dsn=retail_dsn,
            airflow_dsn=airflow_dsn,
            version=str(_VERSION),
        )


def _dsn(
    values: Mapping[str, str],
    *,
    host_key: str,
    port_key: str,
    db_key: str,
    user_key: str,
    password_key: str,
    sslmode_key: str,
    defaults: tuple[str, str, str, str, str, str],
) -> str:
    default_host, default_port, default_db, default_user, default_pw, default_ssl = defaults
    host = values.get(host_key, "").strip() or default_host
    port = values.get(port_key, "").strip() or default_port
    dbname = values.get(db_key, "").strip() or default_db
    user = values.get(user_key, "").strip() or default_user
    password = values.get(password_key, "") or default_pw
    sslmode = values.get(sslmode_key, "").strip() or default_ssl
    params = {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "sslmode": sslmode,
        "connect_timeout": "5",
    }
    if password:
        params["password"] = password
    return make_conninfo(**params)


def _int_env(values: Mapping[str, str], key: str, default: int) -> int:
    raw = values.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{key} must be an integer, got {raw!r}") from None


__all__ = [
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "MetricsSettings",
    "find_repo_root",
    "read_dotenv",
]
