# Morocco Retail Intelligence Data Platform

An end-to-end, batch-oriented **Data Engineering platform** for a simulated Moroccan
retail/distribution business (stores, e-commerce, customers, products, promotions,
inventory, purchases). It ingests external public APIs and deterministic synthetic
data, validates it, stores it in object storage, transforms it into a PostgreSQL
dimensional warehouse with dbt, and serves BI dashboards with Apache Superset —
orchestrated by Airflow, tested with pytest + Great Expectations + dbt tests, and
delivered through GitHub Actions CI/CD.

> **Current phase: PHASE 03 — local container foundation.**
> The repository skeleton, Python tooling, the seeded synthetic data generator and
> the local **containers PostgreSQL 16 + MinIO** exist (health + persistence
> validated).
> **No pipeline, buckets, warehouse schema, orchestration or dashboard has been built yet.**

---

## Architecture

Full design: [`ARCHITECTURE.md`](ARCHITECTURE.md)

```
Public APIs + Synthetic Generator
        ↓
Python Ingestion
        ↓
MinIO  (RAW → BRONZE)
        ↓
Great Expectations  (quality gate)
        ↓
PostgreSQL  (SILVER / staging)
        ↓
dbt  (INTERMEDIATE → GOLD / MARTS)
        ↓
PostgreSQL Star Schema  (WAREHOUSE)
        ↓
Apache Superset
```

Airflow = orchestration · pytest = code tests · GitHub Actions = CI/CD ·
Data Contracts = source guarantees · Prometheus/Grafana = future observability.

---

## Technology Stack

| Layer | Technology | Status |
|-------|-----------|--------|
| Language / tooling | Python 3.12, `uv`, ruff, mypy, pytest | **IMPLEMENTED** |
| Object storage | MinIO (Docker, pinned release) | **IMPLEMENTED** (container; buckets PLANNED) |
| Data quality | Great Expectations | dependency installed, suites PLANNED |
| Warehouse | PostgreSQL 16 (Docker) | **IMPLEMENTED** (container; schema PLANNED) |
| Transformation | dbt-core + dbt-postgres | dependency installed, project PLANNED |
| Orchestration | Apache Airflow (official image `apache/airflow:3.3.2`) | PLANNED |
| BI | Apache Superset (`apache/superset:6.0.0`) | PLANNED |
| CI/CD | GitHub Actions | PLANNED (`.github/workflows/` reserved) |
| Observability | Prometheus + Grafana | FUTURE |
| Local stack | Docker Desktop + Docker Compose v2 | host READY |

---

## Data Sources

| Source | Type | Provides | Status |
|--------|------|----------|--------|
| Synthetic generator | Internal, seeded RNG | customers, products, stores, regions, sales, inventory, purchases, promotions, channels, calendar | **IMPLEMENTED** |
| Open-Meteo | Public REST API (no key) | historical weather for Moroccan cities | PLANNED |
| Frankfurter | Public REST API (no key) | EUR/MAD, USD/MAD, GBP/MAD exchange rates | PLANNED |
| Open Food Facts | Public REST API (no key) | product catalog enrichment | PLANNED |
| Calendar / holidays | Python `holidays` (planned) | Moroccan calendar, Ramadan/Aïd attributes | PLANNED |

All sources will be covered by versioned contracts — see
[`data_contracts/README.md`](data_contracts/README.md).

The internal synthetic generator is seeded and fully deterministic across 9
entities plus a calendar table, with seasonal trends, promotions, and referential
integrity; identical seeds yield byte-identical output. See
[`docs/data-model/synthetic-data.md`](docs/data-model/synthetic-data.md) and run:

```bash
uv run python -m apps.data_generator --scale tiny --seed 42 --out data/generated
```

> All generated content is **entirely synthetic** — no real customer, transaction,
> or proprietary data is used or implied. Output is git-ignored.

---

## Data Layers

| Layer | Store | Purpose | Status |
|-------|-------|---------|--------|
| RAW | MinIO `raw/` | immutable source landing zone | PLANNED |
| BRONZE | MinIO `bronze/` | typed, partitioned Parquet | PLANNED |
| SILVER | PostgreSQL `staging` | cleaned, conformed records (quality-gated) | PLANNED |
| INTERMEDIATE | dbt | reusable SQL transformations | PLANNED |
| GOLD / MARTS | dbt | business-level models per domain | PLANNED |
| WAREHOUSE | PostgreSQL star schema | facts + dimensions for Superset | PLANNED |

Details: [`docs/architecture/transformation.md`](docs/architecture/transformation.md)

---

## Local Container Stack

PostgreSQL 16 and MinIO run locally through Docker Compose (PHASE 03). Only these
two services exist today — Airflow, Superset and any warehouse schema are PLANNED.

| Service | Image (pinned) | Host address | Purpose |
|---------|----------------|--------------|---------|
| PostgreSQL 16 | `postgres:16-alpine` | `localhost:5433` | future Silver + warehouse |
| MinIO API | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | `localhost:9000` | future RAW/BRONZE object storage |
| MinIO console | (same image) | `localhost:9001` | bucket/object inspection |

Containers talk to each other by service name over the `mri-net` network
(`postgres:5432`, `minio:9000`) — never `localhost`.

```powershell
# from the repository root (points Compose at the root .env)
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml up -d
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml ps
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml down
powershell -NoProfile -ExecutionPolicy Bypass -File infrastructure/scripts/smoke-test.ps1
```

- `down` **keeps** the named volumes (data survives); `down -v` **deletes** them.
- Rationale, ports and the MinIO registry caveat:
  [`docs/decisions/ADR-001-local-container-foundation.md`](docs/decisions/ADR-001-local-container-foundation.md).
- `make` equivalents (`make docker-up`, `make docker-ps`, …) exist but `make` is not
  installed on this Windows host.

---

## Testing

| Level | Tool | Status |
|-------|------|--------|
| Python unit/integration | pytest (`tests/unit`, `tests/integration`) | **IMPLEMENTED** (environment smoke + synthetic generator suites passing) |
| Lint / format | ruff | **IMPLEMENTED** |
| Type checking | mypy | **IMPLEMENTED** |
| Data quality | Great Expectations suites | PLANNED |
| Model tests | dbt tests | PLANNED |

```bash
uv run pytest          # tests
uv run ruff check .    # lint
uv run mypy            # types
```

---

## Data Quality

Quality is a **gate, not a report**: data must pass Great Expectations suites before
it is trusted into PostgreSQL Silver, and dbt tests must pass before marts are
consumed. Failed records are counted and quarantined — never dropped silently.
Contract policy: [`data_contracts/README.md`](data_contracts/README.md).

---

## CI/CD

**PLANNED** — `.github/workflows/` is reserved and intentionally empty. The planned
pipeline runs `ruff → mypy → pytest → docker compose config` on every push/PR
([`docs/architecture/orchestration.md`](docs/architecture/orchestration.md)).

---

## Performance

Design choices aimed at local, reproducible performance (all PLANNED where noted):

- **Parquet + columnar reads** (PyArrow) instead of CSV for Bronze analytics.
- **Date partitioning** (`dt=`) in MinIO → incremental loads instead of full reloads.
- **Incremental dbt materialization** for large fact models.
- **Batch daily processing** — the workload is batch; no streaming complexity added.
- **Resource honesty**: the host has ~7.8 GiB RAM / ~37 GB free disk, so containers
  are started in groups rather than all at once (see
  [`docs/development-environment.md`](docs/development-environment.md)).

---

## Local Development

Requirements: Git, Docker Desktop (WSL2 backend), `uv` — audited in PHASE 00
([`docs/development-environment.md`](docs/development-environment.md)).

```bash
uv sync                          # create .venv from uv.lock (Python 3.12.13)
uv run pytest                    # run tests
uv run ruff check .              # lint
uv run ruff format .             # format
uv run mypy                      # type check
```

Equivalent `make` targets (if `make` is available, e.g. Linux/CI):

```bash
make help | make install | make test | make lint | make format | make typecheck | make check
```

Copy `.env.example` → `.env` and fill local values (the PHASE 03 `.env` holds the
generated PostgreSQL/MinIO credentials). **`.env` is git-ignored; never commit
secrets.**

Container commands are documented in **Local Container Stack** above. `make` is not
installed on this Windows host, so run the `docker compose --env-file .env -f
infrastructure/docker/docker-compose.yml ...` forms directly.

---

## Project Structure

```
├── ARCHITECTURE.md               # target architecture (design)
├── Makefile                      # developer commands that work TODAY
├── pyproject.toml / uv.lock      # pinned, reproducible Python project
├── .env.example                  # placeholder environment variables
├── apps/data_generator/          # IMPLEMENTED: `python -m apps.data_generator`
├── apps/ingestion/               # PLANNED
├── src/mri/                      # shared Python package (IMPLEMENTED skeleton)
├── airflow/                      # PLANNED: dags/, plugins/, config/
├── dbt/                          # PLANNED: models/, seeds/, macros/, tests/
├── warehouse/                    # PLANNED: migrations/, schemas/, seeds/
├── data_quality/                 # PLANNED: expectations/, checks/, reports/
├── data_contracts/               # contract policy (IMPLEMENTED) + contracts (PLANNED)
├── infrastructure/docker|scripts # IMPLEMENTED: compose (postgres+minio) + smoke test
├── tests/                        # unit/ (IMPLEMENTED), integration/, fixtures/
├── benchmarks/                   # PLANNED
├── dashboards/superset/          # PLANNED
├── monitoring/prometheus|grafana # FUTURE
├── scripts/                      # PLANNED
├── .github/workflows/            # PLANNED (reserved, empty)
└── docs/                         # architecture, development environment, decisions...
```

---

## Roadmap

| Phase | Content | Status |
|-------|---------|--------|
| 00 | Environment audit + controlled installation | **DONE** |
| 01 | Repository foundation + architecture documentation | **DONE** |
| 02 | Local container stack (PostgreSQL + MinIO via Docker Compose) | **DONE** |
| 03 | Synthetic data generator + data contracts v1 | generator **DONE**, contracts v1 PLANNED |
| 04 | Ingestion (APIs + files) → MinIO RAW/BRONZE | PLANNED |
| 05 | Great Expectations quality gate → PostgreSQL Silver | PLANNED |
| 06 | dbt project: intermediate, marts, star schema, tests | PLANNED |
| 07 | Airflow orchestration | PLANNED |
| 08 | Superset dashboards | PLANNED |
| 09 | GitHub Actions CI/CD | PLANNED |
| 10 | Prometheus/Grafana observability | FUTURE |

---

## License / purpose

Portfolio project — Master II-BDCC. Simulated business data only; no real personal
data is used.
