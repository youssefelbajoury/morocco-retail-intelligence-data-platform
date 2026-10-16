"""PHASE 24 — PostgreSQL least privilege (live retail PostgreSQL).

Asserts the deliberate split introduced in PHASE 24:

- the application/owner role (``POSTGRES_USER``) is a plain login role: it owns
  the retail database and issues all pipeline DDL, but has NO elevated role
  attributes (not superuser, no createdb/createrole/replication/bypassrls);
- the break-glass administrator (``POSTGRES_ADMIN_USER``) is the only superuser;
- the application role cannot run cluster-level DDL (``CREATE DATABASE`` /
  ``CREATE ROLE``);
- the application role can still read ``pg_stat_activity`` (the metrics
  exporter's non-superuser visibility is unchanged).

Skipped when the retail PostgreSQL instance is unreachable (matches the existing
integration-test convention).
"""

from __future__ import annotations

import re
from pathlib import Path

import psycopg
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _read_env() -> dict[str, str]:
    env: dict[str, str] = {}
    for line in (REPO_ROOT / ".env").read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Za-z0-9_]+)=(.*)$", line)
        if match:
            env[match.group(1)] = match.group(2)
    return env


def _connect(user: str, password: str, env: dict[str, str]) -> psycopg.Connection:
    return psycopg.connect(
        host=env.get("POSTGRES_HOST", "localhost"),
        port=int(env.get("POSTGRES_PORT", "5433")),
        dbname=env.get("POSTGRES_DB", "retail"),
        user=user,
        password=password,
        connect_timeout=5,
        autocommit=True,
    )


@pytest.fixture(scope="module")
def env() -> dict[str, str]:
    try:
        return _read_env()
    except FileNotFoundError:  # pragma: no cover - environment dependent
        pytest.skip("no .env to read PostgreSQL credentials from")


@pytest.fixture(scope="module")
def app(env: dict[str, str]):
    try:
        connection = _connect(env["POSTGRES_USER"], env["POSTGRES_PASSWORD"], env)
    except psycopg.OperationalError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"retail PostgreSQL unreachable: {exc}")
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def admin(env: dict[str, str]):
    if not env.get("POSTGRES_ADMIN_USER") or not env.get("POSTGRES_ADMIN_PASSWORD"):
        pytest.skip("no POSTGRES_ADMIN_USER/PASSWORD configured")
    try:
        connection = _connect(env["POSTGRES_ADMIN_USER"], env["POSTGRES_ADMIN_PASSWORD"], env)
    except psycopg.OperationalError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"admin login failed: {exc}")
    yield connection
    connection.close()


def test_application_role_is_not_privileged(app, env: dict[str, str]) -> None:
    row = app.execute(
        "select rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls "
        "from pg_roles where rolname = %s",
        (env["POSTGRES_USER"],),
    ).fetchone()
    assert row is not None, f"role {env['POSTGRES_USER']} missing"
    assert tuple(row) == (False, False, False, False, False), tuple(row)


def test_application_role_owns_the_database(app, env: dict[str, str]) -> None:
    owner = app.execute(
        "select pg_get_userbyid(datdba) from pg_database where datname = %s",
        (env.get("POSTGRES_DB", "retail"),),
    ).fetchone()
    assert owner is not None
    assert owner[0] == env["POSTGRES_USER"]


def test_application_role_owns_analytics_schemas(app, env: dict[str, str]) -> None:
    row = app.execute(
        "select count(*) from pg_namespace where nspname = 'warehouse' "
        "and pg_get_userbyid(nspowner) = %s",
        (env["POSTGRES_USER"],),
    ).fetchone()
    assert row is not None
    assert row[0] == 1, "application role does not own the warehouse schema"


def test_application_role_cannot_create_database(app) -> None:
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        app.execute("create database mri_phase24_denied")


def test_application_role_cannot_create_role(app) -> None:
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        app.execute("create role mri_phase24_denied")


def test_application_role_can_read_pg_stat_activity(app) -> None:
    row = app.execute("select count(*) from pg_stat_activity").fetchone()
    assert row is not None
    assert row[0] >= 1


def test_administrator_role_is_superuser(admin, env: dict[str, str]) -> None:
    row = admin.execute(
        "select rolsuper from pg_roles where rolname = %s",
        (env["POSTGRES_ADMIN_USER"],),
    ).fetchone()
    assert row is not None, f"role {env['POSTGRES_ADMIN_USER']} missing"
    assert row[0] is True
