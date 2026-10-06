# ADR-001 — Local container foundation (PostgreSQL + MinIO)

- **Status:** Accepted
- **Date:** 2026-10-06
- **Phase:** 03 — Local container foundation
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phases 00–02 delivered the host tooling audit, the repository/docs foundation and
the deterministic synthetic data generator. The next platform layers (ingestion,
object storage, validation, dbt, warehouse, orchestration, BI) all need two shared
services: a relational database and S3-compatible object storage.

The development host is a laptop (Windows 11 + WSL2 + Docker Desktop, ≈7.8 GiB RAM,
≈3.7 GiB assigned to the Docker VM, ≈36 GB free disk). It already runs **native
PostgreSQL 16 (port 5432)** and **PostgreSQL 18 (port 3000)** as Windows services.
Bringing up the whole target stack (PostgreSQL, MinIO, Airflow, Superset) at once
would exhaust the available RAM.

## Decision

1. **Run PostgreSQL 16 and MinIO as Docker Compose services, not host installs.**
2. **Publish PostgreSQL on host port `5433`** (container `5432`).
- **Expose MinIO on host `9000` (API) and `9001` (console).**
- **Use one dedicated bridge network (`mri-net`)**; services address each other by
  service name (`postgres`, `minio`), never `localhost`.
- **Persist data in named volumes** (`mri-postgres-data`, `mri-minio-data`).
- **Introduce services gradually:** only PostgreSQL and MinIO now. Airflow,
  Superset, dbt, Great Expectations and any business schema come in later phases.

---

## Why PostgreSQL is containerized

- **Reproducibility / infrastructure-as-code.** The exact server version
  (`postgres:16-alpine`) and its configuration live in the repository and are
  recreated identically on any machine, instead of depending on however a host
  service happens to be configured.
- **Isolation.** The project's database is separate from the two native Windows
  PostgreSQL services already running (16 on 5432, 18 on 3000). They are not
  touched, stopped or reconfigured.
- **Reproducibility of teardown.** `docker compose down -v` returns to a pristine
  state without uninstalling or altering host software.
- **Match with production-oriented tooling.** Airflow, dbt and Great Expectations
  treat the database as a service reached over the network — the container models
  the same boundary locally.

## Why MinIO

- The platform's RAW/BRONZE layers are file-based and must be S3-compatible so the
  same ingestion code could later target AWS S3, without rewrites.
- MinIO is a lightweight, single-binary S3-compatible server that runs comfortably
  beside PostgreSQL within the Docker memory budget.
- It provides a console for manual inspection of buckets and objects during
  development.

## Why host port 5433 (and not 5432)

- The host already runs **native PostgreSQL 16.14 on 5432**; binding the container
  there would fail or silently shadow the native server.
- Native **PostgreSQL 18** is configured on port 3000, so 3000 is also unavailable.
- **5433** is the conventional alternative and keeps the mapping explicit and
  collision-free.

| Context | Address |
|---------|---------|
| Host → PostgreSQL | `localhost:5433` |
| Container → PostgreSQL | `postgres:5432` |
| Host → MinIO API | `localhost:9000` |
| Host → MinIO console | `localhost:9001` |
- Containers never use `localhost` for each other; they use service names over the
  `mri-net` network.

## Why services are introduced gradually

- **Resource constraint.** The Docker VM has ≈3.7 GiB. Standing up PostgreSQL,
  MinIO, Airflow and Superset at once risks memory pressure and confusing failures.
  Introducing two services first keeps the footprint small and each layer
  debuggable.
- **Independent validation.** Each layer can be verified in isolation (this ADR's
  phase verifies only health, connectivity and persistence of PostgreSQL + MinIO),
  which matches the project's "data quality/verification before the next layer"
  principle.
- **Clear failure attribution.** When something breaks, only the newly added
  service is suspect.

---

## MinIO image availability — important caveat

MinIO has withdrawn its community Docker images from public registries:

- Docker Hub `minio/minio` no longer resolves (repository removed).
- `quay.io/minio/minio` returns HTTP 401 without authentication.
- The upstream GitHub repository was archived in 2026.

The stack therefore pins the **last official community release available locally**:

```
minio/minio:RELEASE.2025-09-07T16-13-09Z
```

Consequences and mitigations:

- `docker compose up` works because that image is present in the local Docker
  cache; Compose does not re-pull an image that already exists.
- On a **clean machine** the image must be obtained first (copy the cached image,
  or build the release from source, or substitute a maintained fork such as
  `cgr.dev/chainguard/minio@sha256:…`, verified pullable during this phase).
- The tag is pinned (not `latest`) so the running binary is reproducible where the
  image is available.

This is recorded as a known warning in the phase completion report and in
`docs/development-environment.md`.

---

## Alternatives considered

| Alternative | Why not chosen |
|-------------|----------------|
| Native PostgreSQL/MinIO installs | Pollutes the host, hard to reproduce/tear down, and would conflict with existing native PostgreSQL services. |
| Container PostgreSQL on host port 5432 | Conflicts with the running native PostgreSQL 16. |
| `minio/minio:latest` | Explicitly forbidden, non-reproducible, and the registry repo is gone anyway. |
| Maintained MinIO fork (Chainguard) | Verified pullable and security-maintained, but distroless (no `curl`), no console ergonomics, and only a moving `latest` tag; kept as a documented fallback rather than the default. |
| Object storage substituted by a plain filesystem mount | Loses the S3-compatible interface the ingestion layer is designed against. |
| Introducing Airflow/Superset now | Exceeds the available container memory for this phase and is out of scope. |

---

## Consequences

- **Positive:** reproducible local services, no host pollution, host-port conflict
  avoided, and a validated persistence/health baseline for later phases.
- **Negative:** the MinIO image must pre-exist locally (registry withdrawal); the
  stack depends on Docker Desktop being running.
- **Follow-up:** when ingestion needs MinIO on a fresh machine, decide between a
  maintained fork and a self-built image (revisit in a later phase).