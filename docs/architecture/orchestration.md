# Orchestration, Testing & CI/CD Architecture

> **Status: PLANNED.** No Airflow DAG and no GitHub Actions workflow exists yet.

---

## 1. Orchestration architecture (Apache Airflow)

Airflow schedules and monitors the platform. It **coordinates only** — it never
contains business logic.

```mermaid
flowchart TB
    TRIG["schedule<br/>(daily batch, hourly weather)"]
    subgraph DAG["dag: retail_daily_pipeline (PLANNED)"]
        T1["ingest_external"]
        T2["ingest_synthetic"]
        T3["bronze_build"]
        T4["quality_gate"]
        T5["load_silver"]
        T6["dbt_run"]
        T7["dbt_test"]
        T8["superset_refresh?"]
    end
    ERR["on failure: retry → alert → stop chain"]
    UI["Airflow UI / run history"]

    TRIG --> T1 --> T2 --> T3 --> T4 --> T5 --> T6 --> T7 --> T8
    T4 -.->|failed| ERR
    T7 -.->|failed| ERR
    DAG --> UI
```

### Orchestration decisions (designed)

| Decision | Detail |
|----------|--------|
| Deployment | Official container image `apache/airflow:3.3.2` via Docker Compose (**PLANNED**) |
| Constraint files | If Airflow is ever installed with pip/uv, official constraints files are mandatory |
| Tasks are thin | Each task calls one Python entry point or one `dbt` command |
| Idempotent tasks | Re-running any task is safe |
| Dependencies are explicit | Data intervals and `depends_on_past` are avoided except where needed |
| Secrets | Delivered via environment variables, never hardcoded in DAGs |

---

## 2. Testing architecture

Three test levels, each with a clear owner:

```mermaid
flowchart LR
    subgraph Python["Python code — pytest (IMPLEMENTED skeleton)"]
        U["tests/unit<br/>pure logic"]
        I["tests/integration<br/>DB / MinIO interactions"]
    end
    subgraph Data["Data — Great Expectations (PLANNED)"]
        E["expectation suites<br/>row counts, types, nulls, ranges"]
    end
    subgraph SQL["Models — dbt test (PLANNED)"]
        T["schema tests<br/>unique, not_null, relationships"]
    end
    U --> CI
    I --> CI
    E --> CI
    T --> CI
    CI["GitHub Actions<br/>quality gate (PLANNED)"]
```

| Level | Tool | Examples |
|-------|------|----------|
| Unit | pytest | parsing helpers, contract validation, seed determinism |
| Integration | pytest | load into Postgres, MinIO read/write (containers via compose) |
| Data quality | Great Expectations | freshness, ranges, allowed values, referential presence |
| Model | dbt test | uniqueness of keys, not-null foreign keys, relationship integrity |
| Static | ruff + mypy | lint/format and type safety |

Commands available now: `uv run pytest`, `uv run ruff check .`, `uv run mypy`
(see `Makefile`).

---

## 3. CI/CD architecture (GitHub Actions)

**PLANNED** — `.github/workflows/` is reserved but empty on purpose.

Planned pipeline (not yet created):

```mermaid
flowchart LR
    PUSH["push / pull_request"] --> LINT["ruff check"]
    LINT --> TYPE["mypy"]
    TYPE --> TEST["pytest"]
    TEST --> BUILD["build package"]
    BUILD --> DOCKER["docker compose config<br/>(validate compose file)"]
```

| Stage | Tool | Blocking? |
|-------|------|-----------|
| Lint | ruff | yes |
| Type check | mypy | yes |
| Unit tests | pytest | yes |
| Data/dbt tests | GE + dbt test (when stack exists) | yes |
| Compose validation | `docker compose config` | yes |
