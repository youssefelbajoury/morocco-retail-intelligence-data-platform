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

## MinIO image availability and supply chain (updated Phase 20)

MinIO has stopped distributing community **Docker images**:

- Docker Hub `minio/minio`: repository removed — pulling the previous pin returns
  `denied: requested access to the resource is denied`.
- `quay.io/minio/minio`: anonymous pulls now fail (`no such manifest` / 401); the
  historical tag is no longer served (verified 2026-10-12).
- The upstream GitHub repository was archived in 2026. MinIO's own release notes
  now instruct container users to build from source.

**Known vulnerability:** the previously pinned release
`minio/minio:RELEASE.2025-09-07T16-13-09Z` is affected by privilege escalation via
session-policy bypass in service accounts and STS —
**GHSA-jjjj-jwhf-8rgr / CVE-2025-62506** (CVSS 8.1). The fix landed only in
`RELEASE.2025-10-15T17-29-55Z`, which MinIO never published as a Docker image, so
no patched image was pullable from any registry for that build.

**Decision (adopted in Phase 20):** the default MinIO image is the maintained,
security-maintained Chainguard rebuild, **digest-pinned**:

```
cgr.dev/chainguard/minio@sha256:dc455cdd4db6e390b1b26514f15ff37681a777bf8dab41aac445a0259f751e56
# RELEASE.2026-09-22T19-25-18Z (post CVE-2025-62506), go 1.27.2
```

Validation (Phase 20, evidence):

- The tag pulls publicly on a clean engine.
- An **isolated** instance (temp ports `19000`/`19001`, test-only credentials) ran
  the four MinIO integration-test modules: `test_ingestion_minio.py`,
  `test_external_minio.py`, `test_bronze_minio.py`, `test_external_bronze_minio.py`
  → **6 passed** (real S3 put/get/stat/delete round-trips).
- The recreated `mri-minio` then passed `test_ingestion_minio.py` against the
  production volume, and `GET /minio/health/live` returns 200 (host port 9000).
- **Data preserved:** the same `mri-minio-data` named volume is mounted unchanged
  (MinIO on-disk format is forward-compatible; downgrading to the 2025 build is
  not supported). A safety snapshot lives at
  `%TEMP%\minio-data-backup.tar.gz` (86.6 MB, 2026-10-12).

Behaviour differences to expect vs the old image:

- The healthcheck is a bash `/dev/tcp` connect probe (the image has no
  `curl`/`wget`); HTTP `/minio/health/live` is exercised post-boot by the
  integration tests instead.
- The image's default user is non-root (UID 65532); the service runs with
  `user: "0:0"` so the pre-existing root-owned volume stays writable — identical
  runtime behaviour to the old image.
- Console is unchanged (`:9001`).

Rollback (operational): revert the `image:` line in
`infrastructure/docker/docker-compose.yml` to
`minio/minio:RELEASE.2025-09-07T16-13-09Z` (still present in the local cache) and,
if needed, restore the volume from the snapshot. Note that this reintroduces
CVE-2025-62506; rollback is only for emergency recovery, not a steady state.

---

## Alternatives considered

| Alternative | Why not chosen |
|-------------|----------------|
| Native PostgreSQL/MinIO installs | Pollutes the host, hard to reproduce/tear down, and would conflict with existing native PostgreSQL services. |
| Container PostgreSQL on host port 5432 | Conflicts with the running native PostgreSQL 16. |
| `minio/minio:latest` | Explicitly forbidden, non-reproducible, and the registry repo is gone anyway. |
| Maintained MinIO fork (Chainguard) | **Adopted as default since Phase 20** after isolated S3-compat testing and digest pinning; distroless-style (no `curl`) → `/dev/tcp` healthcheck; runs as `user: 0:0` to reuse the existing volume. |
| Self-built MinIO from source | Official recommendation; deterministic but heavy (Go toolchain build); kept as the clean-machine fallback if Chainguard's distribution is ever unavailable. |
| Object storage substituted by a plain filesystem mount | Loses the S3-compatible interface the ingestion layer is designed against. |
| Introducing Airflow/Superset now | Exceeds the available container memory for this phase and is out of scope. |

---

## Consequences

- **Positive:** reproducible local services, no host pollution, host-port conflict
  avoided, a validated persistence/health baseline for later phases, and an
  actively maintained, CVE-free MinIO distribution by default.
- **Negative:** MinIO upstream no longer publishes images (Chainguard is a third
  party rolling distribution, pinned by digest); the stack depends on Docker
  Desktop being running.
- **Follow-up:** revisit the Chainguard distribution if its upstream source base
  changes materially; otherwise treat the digest pin as the reproduction point.