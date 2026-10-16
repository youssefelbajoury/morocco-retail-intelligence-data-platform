# Contributing

Thanks for your interest in the Morocco Retail Intelligence Data Platform. This
is primarily a **portfolio / learning project**, but issues and pull requests are
welcome. Please keep changes consistent with the project's principles:
reproducibility, idempotence, quality gates, and no secrets in Git.

## Prerequisites

- Python 3.12 via [`uv`](https://docs.astral.sh/uv/) (dependencies are locked in
  `uv.lock`; never edit it by hand).
- Docker Desktop (WSL2 backend on Windows) for the containerized stack.
- See [`docs/development-environment.md`](docs/development-environment.md) for the
  audited environment and version rationale.

## Local setup

```bash
uv python install 3.12
uv sync
Copy-Item .env.example .env      # PowerShell; fill with locally generated values
```

**Never commit `.env`** (it is git-ignored). Only placeholders belong in
`.env.example`.

## Quality gate (must pass before a PR)

```bash
uv run ruff check .            # lint
uv run ruff format --check .   # format
uv run mypy                    # types
uv run pytest                  # tests
```

Equivalent: `make check` (lint + typecheck + test). dbt changes should also pass
`uv run python -m apps.dbt build` against a running PostgreSQL container.

## Conventions

- **Style:** ruff (`line-length = 100`, rules `E,F,I,UP,B`) and mypy are
  authoritative. Do not add comments unless they clarify non-obvious intent.
- **Tests:** add or update tests for any behavior change. Integration tests that
  need PostgreSQL/MinIO create **unique disposable schemas/buckets** and clean up.
- **Contracts:** if you change a source, update the matching
  `data_contracts/*.yml` and re-run the drift detector
  (`uv run pytest tests/unit/test_contract_drift.py`).
- **Docs:** keep counts (models, tests, charts, tasks) accurate; the README and
  `ARCHITECTURE.md` must agree with the code.
- **Secrets:** never commit credentials, tokens or `.env`. Run the secret scan
  (`uv run python scripts/scan_secrets.py`) if you touched configuration.

## Commit messages

Use short, imperative, conventional-style subjects, e.g.:

```
feat(ingestion): add calendar contract validation
fix(dbt): correct SCD2 contiguity for mid-day attribute changes
docs(readme): clarify CI is configured but not yet run remotely
```

## Reporting issues

Include: what you ran, the expected vs actual result, and the relevant log slice.
Redact any credentials before pasting output.

## License

The project has **no license yet** (all rights reserved pending the owner's
decision). By contributing you agree your contribution may be distributed under
the license the owner eventually adopts.
