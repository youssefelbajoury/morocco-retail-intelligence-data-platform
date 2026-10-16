"""PHASE 20 — the BI role is strictly read-only (live retail PostgreSQL).

Connects to the retail warehouse exactly like Apache Superset does and asserts
both sides of the boundary:

- realm: the ``BI_READER_USER`` role has SELECT access to the analytics
  schemas (``warehouse``, ``dbt_marts``, ``audit``);
- and it cannot write: no DB-role privileges, no role memberships, no
  non-SELECT grants, and an actual DDL statement is refused.

Skipped when the retail PostgreSQL instance is unreachable (matches the
existing integration-test convention).
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
    )


@pytest.fixture(scope="module")
def env() -> dict[str, str]:
    try:
        return _read_env()
    except FileNotFoundError:  # pragma: no cover - environment dependent
        pytest.skip("no .env to read BI/POSTGRES credentials from")


@pytest.fixture(scope="module")
def bi_user(env: dict[str, str]) -> str:
    return env.get("BI_READER_USER", "superset_bi")


@pytest.fixture(scope="module")
def admin(env: dict[str, str]):
    try:
        connection = _connect(env["POSTGRES_USER"], env["POSTGRES_PASSWORD"], env)
    except psycopg.OperationalError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"retail PostgreSQL unreachable: {exc}")
    yield connection
    connection.close()


@pytest.fixture(scope="module")
def bi_reader(env: dict[str, str]):
    try:
        connection = _connect(env["BI_READER_USER"], env["BI_READER_PASSWORD"], env)
    except psycopg.OperationalError as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"BI role login failed: {exc}")
    yield connection
    connection.close()


def test_bi_role_has_no_privileged_roles(admin, bi_user: str) -> None:
    row = admin.execute(
        "select rolsuper, rolcreaterole, rolcreatedb from pg_roles where rolname = %s",
        (bi_user,),
    ).fetchone()
    assert row is not None, f"role {bi_user} missing"
    assert tuple(row) == (False, False, False)


def test_bi_role_has_no_memberships(admin, bi_user: str) -> None:
    count = admin.execute(
        "select count(*) from pg_auth_members "
        "where member = (select oid from pg_roles where rolname = %s)",
        (bi_user,),
    ).fetchone()
    assert count is not None
    assert count[0] == 0, f"role {bi_user} inherits from another role"


def test_bi_role_has_no_write_grants(admin, bi_user: str) -> None:
    row = admin.execute(
        "select count(*) from information_schema.role_table_grants "
        "where grantee = %s and privilege_type <> 'SELECT'",
        (bi_user,),
    ).fetchone()
    assert row is not None
    assert row[0] == 0, f"role {bi_user} holds non-SELECT table grants"


def test_bi_role_can_read_analytics_schemas(bi_reader) -> None:
    for schema, table in [
        ("warehouse", "dim_date"),
        ("dbt_marts", "fact_ventes"),
        ("audit", "load_audit"),
    ]:
        row = bi_reader.execute(f"select count(*) from {schema}.{table}").fetchone()
        assert row is not None, f"cannot read {schema}.{table}"


def test_bi_role_cannot_create_tables(bi_reader) -> None:
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        bi_reader.execute("create table mri_phase20_denied(id int)")
