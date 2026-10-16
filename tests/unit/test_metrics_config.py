"""Phase 19: unit tests for the exporter settings resolution."""

from __future__ import annotations

from pathlib import Path

import pytest
from psycopg.conninfo import conninfo_to_dict

from mri.metrics.config import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    MetricsSettings,
    find_repo_root,
    read_dotenv,
)


def test_defaults_when_nothing_is_configured(tmp_path: Path) -> None:
    settings = MetricsSettings.from_env(env={}, repo_root=tmp_path)
    assert settings.host == DEFAULT_HOST
    assert settings.port == DEFAULT_PORT
    assert settings.airflow_configured is False
    assert settings.airflow_dsn == ""
    parsed = conninfo_to_dict(settings.retail_dsn)
    assert parsed["host"] == "localhost"
    assert parsed["dbname"] == "retail"
    assert parsed["user"] == "retail_app"


def test_retail_dsn_is_built_from_postgres_keys(tmp_path: Path) -> None:
    env = {
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "retail",
        "POSTGRES_USER": "retail_app",
        "POSTGRES_PASSWORD": "secret-value",
        "POSTGRES_SSLMODE": "prefer",
    }
    parsed = conninfo_to_dict(MetricsSettings.from_env(env=env, repo_root=tmp_path).retail_dsn)
    assert parsed["host"] == "postgres"
    assert parsed["port"] == "5432"
    assert parsed["password"] == "secret-value"


def test_airflow_configured_only_when_db_is_present(tmp_path: Path) -> None:
    without = MetricsSettings.from_env(env={"AIRFLOW_POSTGRES_USER": "airflow"}, repo_root=tmp_path)
    assert without.airflow_configured is False

    with_db = MetricsSettings.from_env(
        env={
            "AIRFLOW_POSTGRES_DB": "airflow",
            "AIRFLOW_POSTGRES_USER": "airflow",
            "AIRFLOW_POSTGRES_PASSWORD": "pw",
        },
        repo_root=tmp_path,
    )
    assert with_db.airflow_configured is True
    parsed = conninfo_to_dict(with_db.airflow_dsn)
    assert parsed["host"] == "localhost"
    assert parsed["port"] == "5434"
    assert parsed["dbname"] == "airflow"


def test_bind_address_overrides_and_port_validation(tmp_path: Path) -> None:
    settings = MetricsSettings.from_env(
        env={"MRI_METRICS_HOST": "0.0.0.0", "MRI_METRICS_PORT": "9999"}, repo_root=tmp_path
    )
    assert settings.host == "0.0.0.0"
    assert settings.port == 9999

    with pytest.raises(ValueError, match="MRI_METRICS_PORT"):
        MetricsSettings.from_env(env={"MRI_METRICS_PORT": "not-a-number"}, repo_root=tmp_path)


def test_explicit_env_argument_beats_dotenv(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("POSTGRES_DB=from_dotenv\n", encoding="utf-8")
    settings = MetricsSettings.from_env(env={"POSTGRES_DB": "from_env"}, repo_root=tmp_path)
    assert conninfo_to_dict(settings.retail_dsn)["dbname"] == "from_env"


def test_process_env_beats_dotenv(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("POSTGRES_DB=from_dotenv\n", encoding="utf-8")
    monkeypatch.setenv("POSTGRES_DB", "from_process")
    settings = MetricsSettings.from_env(repo_root=tmp_path)
    assert conninfo_to_dict(settings.retail_dsn)["dbname"] == "from_process"


def test_read_dotenv_parses_quotes_comments_and_blank_lines(tmp_path: Path) -> None:
    dotenv = tmp_path / ".env"
    dotenv.write_text(
        "# comment\n\nA=1\nB=\"two\"\nC='three'\nnot-a-pair\n",
        encoding="utf-8",
    )
    assert read_dotenv(dotenv) == {"A": "1", "B": "two", "C": "three"}
    assert read_dotenv(tmp_path / "missing.env") == {}


def test_find_repo_root_locates_pyproject(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    nested = tmp_path / "src" / "pkg"
    nested.mkdir(parents=True)
    assert find_repo_root(nested) == tmp_path
