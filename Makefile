# Morocco Retail Intelligence Data Platform — developer commands
#
# Every target below runs a command that EXISTS today.
# `make` is not installed on the Windows host used for development; the same
# commands can always be run directly, e.g. `uv run pytest` or the docker
# compose commands documented in README.md.

COMPOSE := docker compose --env-file .env -f infrastructure/docker/docker-compose.yml

.PHONY: help install test lint format typecheck check \
        docker-config docker-up docker-down docker-down-v docker-ps docker-logs docker-restart docker-smoke

help: ## Show available targets
	@echo "Morocco Retail Intelligence Data Platform"
	@echo ""
	@echo "  make install       - sync the locked Python environment (uv sync)"
	@echo "  make test          - run pytest"
	@echo "  make lint          - run ruff checks (no changes)"
	@echo "  make format        - format code with ruff + apply safe autofixes"
	@echo "  make typecheck     - run mypy on src/ and tests/"
	@echo "  make check         - lint + typecheck + test (local quality gate)"
	@echo ""
	@echo "  make docker-config - validate the Compose file"
	@echo "  make docker-up     - start PostgreSQL + MinIO in the background"
	@echo "  make docker-down   - stop containers, KEEP data volumes"
	@echo "  make docker-down-v - stop containers and DELETE data volumes"
	@echo "  make docker-ps     - show container status"
	@echo "  make docker-logs   - follow container logs"
	@echo "  make docker-restart- restart containers"
	@echo "  make docker-smoke  - verify the running stack (read-only)"

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
