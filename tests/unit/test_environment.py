"""PHASE 00 environment smoke tests — prove the toolchain imports and runs."""

import sys

import pandas
import psycopg
import pyarrow
import sqlalchemy


def test_python_is_312():
    assert sys.version_info[:2] == (3, 12)


def test_core_data_stack_imports():
    assert pandas.__version__.startswith("2.")
    assert pyarrow.__version__
    assert sqlalchemy.__version__.startswith("2.")
    assert psycopg.__version__.startswith("3.")


def test_great_expectations_runtime():
    import great_expectations as gx

    context = gx.get_context(mode="ephemeral")
    assert context is not None


def test_dbt_core_and_postgres_adapter():
    import dbt.adapters.postgres  # noqa: F401
    import dbt.version

    assert dbt.version.__version__.startswith("1.")
