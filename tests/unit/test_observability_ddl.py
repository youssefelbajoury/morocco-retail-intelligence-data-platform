"""Phase 16: unit tests for the PostgreSQL ``ops`` schema DDL."""

from __future__ import annotations

from mri.observability.ddl import (
    EVENT_TABLE,
    EVENT_TABLE_COLUMNS,
    OPS_SCHEMA,
    VIEWS,
    ident,
    qualified,
    render_ops_ddl,
)


def test_ident_quotes_and_escapes() -> None:
    assert ident("pipeline_event") == '"pipeline_event"'
    # embedded quotes are doubled, identifiers never leak out of the quotes
    assert ident('weird"name') == '"weird""name"'


def test_qualified_joins_schema_and_table() -> None:
    assert qualified("ops", "pipeline_event") == '"ops"."pipeline_event"'


def test_ddl_creates_schema_table_and_indexes() -> None:
    statements = render_ops_ddl()
    joined = ";\n".join(statements)
    assert 'CREATE SCHEMA IF NOT EXISTS "ops"' in joined
    assert 'CREATE TABLE IF NOT EXISTS "ops"."pipeline_event"' in joined
    assert EVENT_TABLE_COLUMNS[1].name == "event_hash"
    assert "JSONB" in EVENT_TABLE_COLUMNS[-1].definition
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in joined


def test_ddl_idempotent_wording_avoids_drops() -> None:
    joined = ";\n".join(render_ops_ddl())
    assert "DROP" not in joined
    assert joined.count("CREATE SCHEMA IF NOT EXISTS") == 1
    assert joined.count("CREATE OR REPLACE VIEW") == len(VIEWS)


def test_ddl_renders_the_three_metric_views() -> None:
    statements = render_ops_ddl()
    text = "\n".join(statements)
    for view in VIEWS:
        assert f'"{view}"' in text


def test_run_facts_skips_run_level_row_totals() -> None:
    """Rows are summed only from granular (batch_id) events, like offline metrics."""
    text = "\n".join(render_ops_ddl())
    assert "FILTER (WHERE batch_id <> '')" in text
    assert "FILTER (WHERE status = 'failed')" in text


def test_metrics_views_use_postgres_compatible_aggregates() -> None:
    """percentile_cont returns double precision; the 2-arg round() needs numeric."""
    text = "\n".join(render_ops_ddl())
    assert "percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::numeric" in text
    assert "round(avg(duration_ms), 1)" in text


def test_run_facts_uses_terminal_status_rule() -> None:
    """Mirror of the offline rule: a run whose last event isn't success is a failure."""
    text = "\n".join(render_ops_ddl())
    assert "cardinality(array_agg(status ORDER BY timestamp, event_id))" in text
    assert "count(*) FILTER (WHERE succeeded) AS succeeded" in text
    assert "count(*) FILTER (WHERE failed) AS failed" in text


def test_custom_schema_is_used_everywhere() -> None:
    statements = render_ops_ddl(schema="ops_it")
    joined = "; ".join(statements)
    assert '"ops_it"' in joined
    assert '"ops".' not in joined


def test_event_columns_cover_every_canonical_field() -> None:
    from mri.observability.events import FIELDS

    column_names = {column.name for column in EVENT_TABLE_COLUMNS}
    assert column_names == {"event_id", "event_hash", *FIELDS}
    assert OPS_SCHEMA == "ops"
    assert EVENT_TABLE == "pipeline_event"
