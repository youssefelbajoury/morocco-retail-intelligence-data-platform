# Development Environment — PHASE 00 Audit & Installation Record

> Project: **Morocco Retail Intelligence Data Platform**
> Phase: **00 — Environment audit + controlled installation**
> Audit date: **2026-10-06**
> Scope: inspect, verify, install only required host tooling, document. **No pipeline,
> schema, DAG, dbt model, dashboard or data generator was created.**

---

## 1. Selected architecture

```
┌────────────────────────────── Windows 11 host ──────────────────────────────┐
│  Git · uv · Python 3.12 (uv-managed) · project .venv                        │
│  Docker Desktop 4.85.0  ── WSL2 backend (docker-desktop distro, Engine 29)  │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │ docker compose (Phase 01+)
   ┌────────────────────────────────┼────────────────────────────────┐
   │ postgres:16-alpine │ minio │ apache/airflow:3.3.2 │ apache/superset:6.0.0
   └──────────────────────────────────────────────────────────────────┘
```

**Rule adopted:** the host carries only Git + Python + Python project tooling.
PostgreSQL, MinIO, Airflow and Superset are **containerized** (never installed
globally on the host).

---

## 2. Hardware / OS (inspected)

| Item | Value |
|------|-------|
| OS | Microsoft Windows 11 Enterprise, Version 10.0.26300 (build 26300), 64-bit |
| Device | HP EliteBook 840 G6 |
| CPU | Intel Core i5-8365U @ 1.60 GHz — 4 cores / 8 logical processors |
| RAM | 8 386 805 760 bytes ≈ **7.8 GiB total** |
| Disk (C:) | 237.2 GB total, **37.8 GB free** at audit time (36.7 GB after Docker started) |

---

## 3. Tools inspected (audit results)

### Already installed — verified by running the command

| Tool | Version | How verified |
|------|---------|--------------|
| Git | 2.54.0.windows.1 | `git --version` |
| Python (host default) | 3.14.3 | `python --version` |
| pip (host default) | 26.0.1 | `pip --version` |
| uv | 0.12.1 | `uv --version` |
| Docker Desktop | 4.85.0 (build 235549) | install dir + `docker version` server block |
| Docker Engine | 29.6.2 (API 1.55, linux/amd64) | `docker version` after starting Docker Desktop |
| Docker Compose | v5.3.1 (plugin) | `docker compose version` |
| Docker engine resources | 8 CPUs, 4 003 069 952 B ≈ 3.73 GiB RAM | `docker info` |
| WSL | 2.7.11.0, WSLg 1.0.73.2, kernel 6.18.33.2-2 | `wsl --version` |
| WSL distros | `docker-desktop` (default, v2, Stopped), `Ubuntu` (v2, Stopped) | `wsl -l -v` |
| WSL Ubuntu | Ubuntu 26.04.1 LTS, 8 CPUs, 3.7 GiB RAM, Python 3.14.4 | `wsl -d Ubuntu -e bash -lc ...` |
| PostgreSQL (native, Windows services) | 16.14-2 (`postgresql-x64-16`, **Running**, port **5432**) and 18.4-2 (`postgresql-x64-18`, **Running**, configured port **3000**) | `Get-Service`, `Get-NetTCPConnection`, `postgresql.conf` |
| psql (native) | present at `C:\Program Files\PostgreSQL\16\bin\psql.exe` and `...\18\bin\psql.exe` — **not on PATH** | file existence check |
| Node.js / npm | v25.6.0 / 11.18.0 (not required by this project) | `node --version`, `npm --version` |
| Java (JDK) | 26.0.1 (not required by this project) | `java -version` |
| VS Code | present on PATH | `Get-Command code` |

### Environment variables inspected (names/values only, no secrets)

- `PATH`: contains Git, Java (JDK 26), Node, host Python 3.14, Docker Desktop.
- `JAVA_HOME=C:\Program Files\Java\jdk-26.0.1`
- `DOCKER_HOST`, `DOCKER_CONTEXT`, `PYTHONHOME`, `PYTHONPATH`, `VIRTUAL_ENV`,
  `CONDA_PREFIX`, `UV_PYTHON`: **empty / unset** (healthy defaults).
- No `.env` file existed; none was read.

### Not installed on host

| Tool | Status |
|------|--------|
| `psql` on PATH | not on PATH (binaries exist under `C:\Program Files\PostgreSQL\<ver>\bin`) |
| gcc / make | missing (not required — all selected packages ship prebuilt wheels) |
| conda / poetry | not present — no package-manager mixing |
| Python 3.12 system install | missing → **installed as a uv-managed interpreter** (see §5) |

---

## 4. Existing Docker state (inspected, untouched)

Docker Desktop was **not running** at audit start; it was started only to validate the
engine (`docker version`, `docker compose version`). No platform stack was launched.

Pre-existing images/containers belong to **other projects** and were left untouched:

- Images: `minio/minio:latest`, `minio/mc:latest`, `postgres:16-alpine`, `redis:7-alpine`,
  `autoflow-api`, `autoflow-web`, `axllent/mailpit`, `node:22-alpine`, `hello-world`
- Containers: 8 present (0 running at audit time)
- Disk: images 4.38 GB, volumes 1.78 GB, **build cache 6.85 GB (3.31 GB reclaimable)**

No `docker system prune` or cleanup was executed (would risk other projects' data).

---

## 5. Version policy — chosen versions and why

Sources consulted: official docs only
(airflow.apache.org, docs.getdbt.com, docs.greatexpectations.io, superset.apache.org,
docs.docker.com, postgresql.org).

| Component | Chosen | Reason |
|-----------|--------|--------|
| Python | **3.12.13** (uv-managed) | Great Expectations 1.x requires `>=3.10,<3.14` → host Python **3.14.3 is incompatible**. 3.12 is supported by GX, dbt-core 1.12 (`3.10–3.14`), Airflow 3.3 (`3.10–3.14`), and is the project's stated preference. Pinned by `.python-version` + `requires-python = ">=3.12,<3.13"`. |
| Package manager | **uv 0.12.1** (Option B: uv project) | Already installed; resolves + locks (`uv.lock`) + manages the interpreter, so no system-wide Python 3.12 install and no admin rights needed. Single coherent strategy: **uv only** (no pip/conda/poetry mixing). |
| pandas | **2.3.3** (`>=2.2,<3`) | Current stable 2.x line. `pandas 3.0.x` exists but is a new major release; bounded to `<3` to stay conservative with the GX/dbt ecosystem (revisit in a later phase). |
| pyarrow | **25.0.1** (`>=14`) | Current stable; GX `arrow` extras require `pyarrow>=14` on Python 3.12. |
| SQLAlchemy | **2.1.3** (`>=2.0,<2.2`) | 2.x series with psycopg 3 support; upper bound keeps us on the tested 2.0/2.1 line. |
| psycopg | **3.3.6** (`psycopg[binary]>=3.1,<4`) | Modern PostgreSQL driver for SQLAlchemy 2 and direct use. (`dbt-postgres` separately brings `psycopg2-binary` 2.9.13 — both coexist.) |
| Great Expectations | **1.23.2** (`>=1.0,<2`) | Current stable GX Core, Python `>=3.10,<3.14` (main driver for choosing Python 3.12). |
| dbt-core | **1.12.5** (`>=1.12,<2`) | Latest stable line (1.12.x); official Python matrix lists 3.10–3.14 incl. 3.12. |
| dbt-postgres | **1.11.0** (`>=1.11,<2`) | Latest available postgres adapter; declares `dbt-core>=1.8,<2` → compatible with core 1.12.5 (verified by `uv lock` + `dbt --version`). |
| pytest | **9.1.1** (`>=8,<10`) | Current stable test runner. |
| ruff | **0.16.10** (`>=0.9,<1`) | Modern linter/formatter (replaces flake8/isort/black combo). |
| mypy | **2.4.0** (`>=1.8,<3`) | Type checker (`ignore_missing_imports = true` for untyped libs such as pyarrow/dbt). |
| pandas-stubs | **2.3.3.260113** (`>=2.2,<3`, dev) | Type stubs matching the selected pandas 2.3.x line. |
| PostgreSQL (container) | **postgres:16-alpine** (planned, Phase 01) | Matches the native PostgreSQL **16.14** already present/running, and `postgres:16-alpine` already exists in the local Docker cache. Supported by Airflow 3.3 (PG 14–18), dbt and GX. |
| MinIO (container) | **Chainguard rebuild, digest-pinned** `cgr.dev/chainguard/minio@sha256:dc455c…f751e56` (`RELEASE.2026-09-22T19-25-18Z`) | MinIO no longer publishes Docker images (source-only); Docker Hub and Quay tags are gone. The Chainguard distribution is pullable, security-maintained and post CVE-2025-62506 — see ADR-001. Adopted as the default image in Phase 20. |
| Apache Airflow (container) | **apache/airflow:3.3.2** (planned) | Latest **stable** release per official docs; supported Python 3.10–3.14, PostgreSQL 14–18. Containerized → official image + official constraints apply, **not** a host pip install. |
| Apache Superset (container) | **apache/superset:6.0.0** (planned) | Latest official release referenced by the official quickstart (`git checkout tags/6.0.0`). Note: `lean` images ship **without** DB drivers → a `-dev` tag or a derived image with `psycopg2-binary`/`psycopg` is required to reach PostgreSQL. |
| Docker Engine / Compose | 29.6.2 / v5.3.1 (existing) | Current Docker Desktop 4.85.0 bundled versions; `docker compose` (v2 plugin syntax) is the supported CLI. |

**Nothing was installed "as latest" blindly** — every version above is either pinned by
`pyproject.toml` ranges + `uv.lock`, or recorded as an exact planned container tag.

---

## 6. What was installed in PHASE 00

| Name | Version | Method | Reason |
|------|---------|--------|--------|
| Python 3.12.13 (interpreter) | 3.12.13 | `uv python install 3.12` → `%APPDATA%\uv\python\cpython-3.12-windows-x86_64-none\` | Required because host Python 3.14 is unsupported by Great Expectations; no admin rights needed. |
| Project virtualenv `.venv` | — | `uv sync` | Isolated environment for the project. |
| pandas, pyarrow, SQLAlchemy, psycopg, Great Expectations, dbt-core, dbt-postgres, pytest, ruff, mypy | see §5 | `uv sync` (from `pyproject.toml` + `uv.lock`) | Core inventory of the project's technology list. |
| Docker Desktop engine startup | 29.6.2 | started existing install | Validation only (`docker version` / `docker compose version`). |

**Not installed on purpose:** PostgreSQL/MinIO/Airflow/Superset host installs (containerized
by design), Airflow pip package (official image + constraints instead), GitHub Actions
config (later phase), Node/Java additions (not required).

---

## 7. Commands executed (audit + validation)

> Note: during PHASE 01 the smoke-test file was moved from `tests/test_environment.py`
> to `tests/unit/test_environment.py` to match the repository structure. The commands
> below are the original PHASE 00 record.

```text
# environment
Get-CimInstance Win32_OperatingSystem / Win32_ComputerSystem / Win32_Processor
Get-CimInstance Win32_LogicalDisk            → C: 237.2 GB / 37.8 GB free
git --version                                → 2.54.0.windows.1
python --version                             → 3.14.3 (host default)
pip --version                                → 26.0.1
uv --version                                 → 0.12.1
node --version / npm --version               → v25.6.0 / 11.18.0
java -version                                → 26.0.1
wsl --status ; wsl -l -v                     → WSL 2.7.11.0; docker-desktop + Ubuntu (v2)
wsl -d Ubuntu -e bash -lc "..."              → Ubuntu 26.04.1 LTS OK
Get-Service postgresql-x64-16, postgresql-x64-18   → both Running
Get-NetTCPConnection -State Listen           → 5432 = PG16

# docker (started Docker Desktop first)
docker version        → Client 29.6.2 / Server Docker Desktop 4.85.0, Engine 29.6.2
docker compose version→ v5.3.1
docker info           → 29.6.2 | Docker Desktop | ~3.73 GiB | 8 CPUs
docker images / docker system df  → existing images read only, nothing removed

# project
git init              → repository created inside the project folder
uv python install 3.12→ Installed Python 3.12.13
uv lock               → Resolved 89 packages
uv sync               → .venv created, all packages installed
uv run python -c "import ..." → python 3.12.13, pandas 2.3.3, pyarrow 25.0.1,
                      sqlalchemy 2.1.3, psycopg 3.3.6, great_expectations 1.23.2,
                      dbt-core 1.12.5, pytest 9.1.1
uv run ruff --version → ruff 0.16.10
uv run mypy --version → mypy 2.4.0
uv run pytest --version → pytest 9.1.1
uv run dbt --version  → core 1.12.5 (up to date), postgres adapter 1.11.0 (up to date)
uv run python -c "import dbt.adapters.postgres" → OK
uv run python -c "import great_expectations as gx; gx.get_context(mode='ephemeral')"
                      → EphemeralDataContext OK
uv run python -c "create_engine('postgresql+psycopg://...')" → dialect OK (no server needed)

# quality gates on the PHASE 00 smoke tests
uv run pytest          → tests/test_environment.py .... [100%]  4 passed in 8.86s
uv run ruff check --fix . → 1 fixable error fixed (import order)
uv run ruff check .    → All checks passed!
uv run mypy tests      → Success: no issues found in 1 source file
```

---

## 8. How to reproduce the environment

```bash
# 1. prerequisites: Git, Docker Desktop (WSL2 backend), uv
uv python install 3.12

# 2. install the locked environment
uv sync

# 3. sanity check
uv run python -c "import pandas, pyarrow, sqlalchemy, psycopg, great_expectations, dbt.version"
uv run ruff --version && uv run mypy --version && uv run pytest --version
uv run dbt --version

# 4. containers (Phase 01+) — do NOT run in Phase 00
# docker compose -f docker/docker-compose.yml up -d
```

Activation when needed outside `uv run`:

```powershell
.\.venv\Scripts\Activate.ps1
```

---

## 9. Known limitations / warnings

1. **RAM ≈ 7.8 GiB total, Docker VM ≈ 3.73 GiB.** PostgreSQL + MinIO + Airflow +
   Superset simultaneously will be tight (Superset alone can use 1–2 GiB). Consider
   raising Docker Desktop memory to 6–8 GiB *and* starting services in groups, or run
   Superset only when needed. Native PostgreSQL 16 **and** 18 services also consume RAM
   and are not required for this project.
2. **Disk: ~36.7 GB free.** Full stack images (Airflow ~1.5–2 GB, Superset ~1.5 GB,
   plus build cache) will consume several GB more. Existing build cache is 6.85 GB —
   safe to reclaim later *only after confirming it belongs to obsolete builds*.
3. **Port conflicts:** native PostgreSQL 16 owns **5432**, native PostgreSQL 18 is
   configured on **3000**. Containerized PostgreSQL must publish on a different host
   port (plan: **5433**), MinIO 9000/9001, Airflow 8080, Superset 8088.
4. **Host Python 3.14.3 remains the default `python` on PATH.** Project work must go
   through `uv run` / `.venv` (Python 3.12.13). Never `pip install` into the host 3.14.
5. `git config core.autocrlf = true` is set globally at the user level → line endings
   are normalized to LF in the repository; expected on Windows.
6. WSL Ubuntu 26.04 is available but **not required** — Docker Desktop's
   `docker-desktop` WSL distro provides the Linux engine. Do not use WSL Ubuntu as a
   second source of Python/tools for this project (avoid mixing).
7. Superset `lean` image lacks database drivers; a derived image (or `-dev` tag) with a
   PostgreSQL driver must be created in a later phase.
8. `gcc`/`make` are absent — irrelevant for the current dependency set (pure wheels),
   but would block any future source-build dependency.

---

## 10. Blockers for PHASE 01

**None.** No admin-rights action, no reboot and no system-configuration change is
pending. Docker engine validated, Python 3.12 environment locked and import-verified.

---

## 11. Security notes

- No secrets, API keys, passwords or tokens were printed, read or written.
- `.env` is git-ignored; only `.env.example` with placeholders exists.
- All placeholder passwords in `.env.example` are obviously fake (`change-me-local-only`).

---

## 12. PHASE 03 addendum — local container foundation

> Phase: **03 — Local container foundation (PostgreSQL + MinIO)**.
> Scope: the two local services only. No business schema, no buckets, no pipeline,
> no Airflow/Superset.

### 12.1 What was added

| Item | Path |
|------|------|
| Compose stack (postgres + minio) | `infrastructure/docker/docker-compose.yml` |
| Read-only smoke test | `infrastructure/scripts/smoke-test.ps1` |
| Decision record | `docs/decisions/ADR-001-local-container-foundation.md` |
| Make targets | `Makefile` (`docker-config`, `docker-up`, `docker-down`, `docker-down-v`, `docker-ps`, `docker-logs`, `docker-restart`, `docker-smoke`) |
| Local credentials | `.env` (git-ignored, generated for this host) |

### 12.2 Services, images, ports, volumes, network

| Service | Image (pinned) | Host → container | Volume |
|---------|----------------|------------------|--------|
| PostgreSQL | `postgres:16-alpine` | `localhost:5433` → `5432` | `mri-postgres-data` → `/var/lib/postgresql/data` |
| MinIO | `cgr.dev/chainguard/minio@sha256:dc455cdd4db6e390b1b26514f15ff37681a777bf8dab41aac445a0259f751e56` (`RELEASE.2026-09-22T19-25-18Z`) | `localhost:9000` → `9000`, `localhost:9001` → `9001` | `mri-minio-data` → `/data` |

- Network: `mri-net` (bridge). Containers reach each other as `postgres:5432` and
  `minio:9000` — **never** `localhost`.
- PostgreSQL healthcheck: `pg_isready -U $POSTGRES_USER -d $POSTGRES_DB` (real
  readiness, not just process-up).
- MinIO healthcheck: bash `/dev/tcp` connect probe against `:9000` (the Chainguard
  image has no `curl`/`wget`; the S3 port binds only once the server is up).
  HTTP `GET /minio/health/live` is additionally exercised by the integration tests.

### 12.3 Commands (run from the repository root)

```powershell
# Validate the Compose file
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml config

# Start / status / stop
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml up -d
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml ps
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml down

# Full read-only verification (health, SELECT 1, MinIO health, service DNS)
powershell -NoProfile -ExecutionPolicy Bypass -File infrastructure/scripts/smoke-test.ps1
```

`--env-file .env` is required: the Compose project directory is the compose file's
folder (`infrastructure/docker`), so the root `.env` is not auto-detected.

Equivalent `make` targets exist (`make docker-up`, `make docker-ps`, …) but `make`
is not installed on this Windows host.

### 12.4 `down` vs `down -v`

| Command | Containers / network | Named volumes (data) |
|---------|----------------------|----------------------|
| `docker compose down` | removed | **kept** — data survives |
| `docker compose down -v` | removed | **deleted** — data is lost |

Verified this phase: after `down`, `mri-postgres-data` and `mri-minio-data` still
exist and the PostgreSQL data directory (`pgdata/`, `PG_VERSION`, …) is intact;
after `up` again both containers return `healthy` and `SELECT 1` succeeds.

### 12.5 Validation results (this host)

| Check | Result |
|-------|--------|
| `docker compose config` | valid |
| `docker compose ps` | `mri-postgres` healthy, `mri-minio` healthy |
| PostgreSQL connect + `SELECT 1` | `1` (server version 16.14) |
| MinIO `/minio/health/live`, `/ready` | HTTP 200 (console HTTP 200) |
| Service-to-service DNS | `postgres` ⇄ `minio` resolve; `postgres` fetches `http://minio:9000/minio/health/live` |
| Restart | both return `healthy` |
| Persistence after `down`/`up` | volumes and `pgdata` intact, `SELECT 1` still works |

### 12.6 Known limitation — MinIO image registry withdrawal

MinIO removed its community images from Docker Hub (and Quay anonymous pulls now
fail); the upstream repo is archived and community releases are source-only. This
is why the stack default moved (Phase 20) to the maintained **Chainguard**
rebuild, digest-pinned to `RELEASE.2026-09-22T19-25-18Z` (post CVE-2025-62506) —
pullable on a clean machine. The previously used `minio/minio:RELEASE.2025-09-07`
image is no longer obtainable from any public registry and was affected by
CVE-2025-62506. See
[`docs/decisions/ADR-001-local-container-foundation.md`](decisions/ADR-001-local-container-foundation.md).
