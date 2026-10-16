# Morocco Retail Intelligence Data Platform — developer commands
#
# Every target below runs a command that EXISTS today.
# `make` is not installed on the Windows host used for development; the same
# commands can always be run directly, e.g. `uv run pytest` or the docker
# compose commands documented in README.md.

COMPOSE := docker compose --env-file .env -f infrastructure/docker/docker-compose.yml
AIRFLOW := $(COMPOSE) -f infrastructure/docker/docker-compose.airflow.yml
SUPERSET := $(COMPOSE) -f infrastructure/docker/docker-compose.superset.yml
OBSERVABILITY := $(COMPOSE) -f infrastructure/docker/docker-compose.observability.yml
OBS_PROFILE := --profile observability
INGEST  := uv run python -m apps.ingestion

.PHONY: help install test lint format typecheck check e2e \
        contracts validate ingest manifests \
        external-dry-run external smoke-external \
        bronze bronze-dry-run bench-bronze \
        docker-config docker-up docker-down docker-down-v docker-ps docker-logs docker-restart docker-smoke \
        airflow-up airflow-down airflow-ps airflow-trigger airflow-runs airflow-validate \
        superset-up superset-down superset-ps superset-seed superset-verify \
        obs-config obs-up obs-down obs-down-v obs-ps obs-logs obs-metrics obs-validate

help: ## Show available targets
	@echo "Morocco Retail Intelligence Data Platform"
	@echo ""
	@echo "  make install       - sync the locked Python environment (uv sync)"
	@echo "  make test          - run pytest"
	@echo "  make lint          - run ruff checks (no changes)"
	@echo "  make format        - format code with ruff + apply safe autofixes"
	@echo "  make typecheck     - run mypy on src/ and tests/"
	@echo "  make check         - lint + typecheck + test (local quality gate)"
	@echo "  make e2e           - browser e2e tests vs Superset UI (needs 'make superset-up')"
	@echo ""
	@echo "  make contracts     - list the versioned source contracts"
	@echo "  make validate      - validate generated CSV/JSON (no upload)"
	@echo "  make ingest        - ingest data/generated/tiny into MinIO raw/"
	@echo "  make manifests     - list local ingestion run manifests"
	@echo ""
	@echo "  make external-dry-run - plan external API requests (no network, no writes)"
	@echo "  make external      - fetch all three keyless APIs into raw_external/"
	@echo "  make smoke-external - one live call per external API (MRI_LIVE_SMOKE=1)"
	@echo ""
	@echo "  make bronze        - build + quality-gate every raw batch (RAW -> Parquet)"
	@echo "  make bronze-dry-run - plan the bronze run (no quality gate, no writes)"
	@echo "  make bench-bronze  - run the small-scale bronze benchmark"
	@echo ""
	@echo "  make docker-config - validate the Compose file"
	@echo "  make docker-up     - start PostgreSQL + MinIO in the background"
	@echo "  make docker-down   - stop containers, KEEP data volumes"
	@echo "  make docker-down-v - stop containers and DELETE data volumes"
	@echo "  make docker-ps     - show container status"
	@echo "  make docker-logs   - follow container logs"
	@echo "  make docker-restart- restart containers"
	@echo "  make docker-smoke  - verify the running stack (read-only)"
	@echo ""
	@echo "  make airflow-up    - start the Airflow stack (scheduler, UI on :8080)"
	@echo "  make airflow-down  - stop the Airflow containers (metadata kept)"
	@echo "  make airflow-ps    - show Airflow container status"
	@echo "  make airflow-trigger - trigger the morocco_retail_pipeline DAG"
	@echo "  make airflow-runs  - list DAG runs and their states"
	@echo "  make airflow-validate - validate the DAG with real Airflow DagBag"
	@echo ""
	@echo "  make superset-up   - start the Superset BI stack (UI on :8088)"
	@echo "  make superset-down - stop the Superset containers (metadata kept)"
	@echo "  make superset-ps   - show Superset container status"
	@echo "  make superset-seed - (re)seed database, datasets, 42 charts, 5 dashboards"
	@echo "  make superset-verify - assert the seeded BI layer (4/42/5, layouts, SQL)"
	@echo ""
	@echo "  make obs-config    - validate the optional observability overlay"
	@echo "  make obs-up        - start Prometheus + Grafana + exporter (profile)"
	@echo "  make obs-down      - stop the monitoring containers (data kept)"
	@echo "  make obs-down-v    - stop the monitoring containers and DELETE data"
	@echo "  make obs-ps        - show monitoring container status"
	@echo "  make obs-logs      - follow monitoring logs"
	@echo "  make obs-metrics   - print one exporter scrape to stdout"
	@echo "  make obs-validate  - validate Prometheus config + rules (promtool)"

install: ## Sync dependencies from uv.lock
	uv sync

test: ## Run the test suite
	uv run pytest

lint: ## Static checks without modifying files
	uv run ruff check .
	uv run ruff format --check .

format: ## Format code and apply safe fixes
	uv run ruff format .
	uv run ruff check --fix .

typecheck: ## Type-check src/ and tests/
	uv run mypy

check: lint typecheck test ## Full local quality gate

e2e: ## Run browser end-to-end tests against the live Superset UI (needs 'make superset-up')
	uv run pytest e2e

contracts: ## List loaded source contracts
	$(INGEST) contracts

validate: ## Validate the generated tiny dataset (no upload)
	$(INGEST) validate --path data/generated/tiny --recursive

ingest: ## Ingest the tiny dataset into MinIO raw/ (idempotent)
	$(INGEST) ingest --path data/generated/tiny --recursive

manifests: ## List locally written ingestion manifests
	$(INGEST) manifest --list

external-dry-run: ## Plan external API requests (no network, no writes)
	$(INGEST) external --source all --dry-run --json

external: ## Fetch weather, FX and product data into raw_external/
	$(INGEST) external --source all

smoke-external: ## One live call per external API (skipped unless MRI_LIVE_SMOKE=1)
	MRI_LIVE_SMOKE=1 uv run pytest tests/smoke -q

bronze: ## Build + quality-gate every raw batch (RAW -> partitioned Parquet)
	$(INGEST) bronze --source all

bronze-dry-run: ## Plan the bronze run (no quality gate, no writes)
	$(INGEST) --json bronze --source all --dry-run

bench-bronze: ## Run the small-scale bronze benchmark
	uv run python benchmarks/bronze_benchmark.py --scale small

docker-config: ## Validate the docker-compose configuration
	$(COMPOSE) config

docker-up: ## Start the local containers (PostgreSQL + MinIO)
	$(COMPOSE) up -d

docker-down: ## Stop containers and keep named volumes (data survives)
	$(COMPOSE) down

docker-down-v: ## Stop containers AND delete named volumes (data is lost)
	$(COMPOSE) down -v

docker-ps: ## Show container status
	$(COMPOSE) ps

docker-logs: ## Follow logs from all services
	$(COMPOSE) logs -f

docker-restart: ## Restart the containers
	$(COMPOSE) restart

docker-smoke: ## Verify the running stack without changing data
	powershell -NoProfile -ExecutionPolicy Bypass -File infrastructure/scripts/smoke-test.ps1

airflow-up: ## Start the Airflow stack (UI at http://localhost:8080)
	$(AIRFLOW) up -d

airflow-down: ## Stop the Airflow containers, keep the metadata database
	$(AIRFLOW) stop

airflow-ps: ## Show Airflow container status
	$(AIRFLOW) ps

airflow-trigger: ## Trigger the end-to-end pipeline DAG
	docker exec mri-airflow-scheduler airflow dags trigger morocco_retail_pipeline

airflow-runs: ## List DAG runs and task states
	docker exec mri-airflow-scheduler airflow dags list-runs morocco_retail_pipeline

airflow-validate: ## Validate the DAG with the real Airflow DagBag (in container)
	$(AIRFLOW) exec -T airflow-scheduler python /opt/airflow/mri/tests/airflow/validate_dag.py

superset-up: ## Start the Superset BI stack on top of the base stack
	$(SUPERSET) up -d

superset-down: ## Stop the Superset containers, keep the metadata volume
	$(SUPERSET) stop

superset-ps: ## Show Superset container status
	$(SUPERSET) ps

superset-seed: ## (Re)seed database, datasets, charts and dashboards (idempotent)
	uv run python dashboards/superset/seed.py all

superset-verify: ## Assert the seeded BI layer (objects, layouts, dataset SQL)
	uv run python dashboards/superset/verify.py

obs-config: ## Validate the optional observability overlay configuration
	$(OBSERVABILITY) $(OBS_PROFILE) config -q

obs-up: ## Start Prometheus + Grafana + exporter on top of the base stack
	$(OBSERVABILITY) $(OBS_PROFILE) up -d

obs-down: ## Stop the monitoring containers, keep the named volumes
	$(OBSERVABILITY) $(OBS_PROFILE) down

obs-down-v: ## Stop the monitoring containers AND delete their volumes
	$(OBSERVABILITY) $(OBS_PROFILE) down -v

obs-ps: ## Show monitoring container status
	$(OBSERVABILITY) $(OBS_PROFILE) ps

obs-logs: ## Follow the monitoring container logs
	$(OBSERVABILITY) $(OBS_PROFILE) logs -f

obs-metrics: ## Print a single metrics scrape to stdout (no server)
	uv run python -m apps.metrics --once

obs-validate: ## Validate the Prometheus config and alert rules with promtool
	docker run --rm --entrypoint promtool \
		-v "$(CURDIR)/monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro" \
		-v "$(CURDIR)/monitoring/prometheus/rules:/etc/prometheus/rules:ro" \
		prom/prometheus:v3.13.4 check config /etc/prometheus/prometheus.yml
