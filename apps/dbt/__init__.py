"""Repository wrapper around the dbt CLI.

Usage examples::

    python -m apps.dbt debug
    python -m apps.dbt build
    python -m apps.dbt --vars '{schema_suffix: _it1}' build
    python -m apps.dbt source freshness

The wrapper resolves ``DBT_TARGET_*`` connection variables from ``.env`` (the
same settings every other application entry point uses), locates the ``dbt``
executable installed next to the current Python interpreter and forwards the
process exit code untouched, so callers can rely on dbt's own exit codes.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path

from mri.ingestion.config import Settings, find_repo_root
from mri.ingestion.manifest import new_run_id
from mri.observability import runtime as observability

PROJECT_DIR = "dbt"
MISSING_PASSWORD_MESSAGE = (
    "error: DBT_TARGET_PASSWORD is not set; export it or add POSTGRES_PASSWORD to .env"
)

OBSERVABILITY_FLAGS = ("--dag-run-id", "--observability-dir")
TARGET_RUN_RESULTS = "dbt/target/run_results.json"


def dbt_executable() -> str:
    """Return the dbt executable shipped with the active Python environment."""
    suffix = ".exe" if os.name == "nt" else ""
    candidate = Path(sys.executable).resolve().parent / f"dbt{suffix}"
    if candidate.is_file():
        return str(candidate)
    found = shutil.which("dbt")
    if found:
        return found
    raise FileNotFoundError("dbt executable not found; install with `uv add --dev dbt-postgres`")


def build_env(settings: Settings, env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Environment for dbt: ``DBT_TARGET_*`` injected from settings, never overriding."""
    merged = dict(os.environ if env is None else env)
    defaults = {
        "DBT_TARGET_HOST": settings.postgres_host,
        "DBT_TARGET_PORT": str(settings.postgres_port),
        "DBT_TARGET_DB": settings.postgres_db,
        "DBT_TARGET_USER": settings.postgres_user,
        "DBT_TARGET_PASSWORD": settings.postgres_password,
        "DBT_TARGET_SSLMODE": settings.postgres_sslmode,
    }
    for key, value in defaults.items():
        merged.setdefault(key, value)
    return merged


def build_command(
    argv: Sequence[str],
    *,
    repo_root: Path | None = None,
    executable: str | None = None,
) -> list[str]:
    """Assemble the dbt command line for ``argv`` plus repository project flags."""
    root = repo_root or find_repo_root()
    project_dir = root / PROJECT_DIR
    command = [executable or dbt_executable(), *argv]
    joined = " ".join(argv)
    if "--project-dir" not in joined:
        command += ["--project-dir", str(project_dir)]
    if "--profiles-dir" not in joined:
        command += ["--profiles-dir", str(project_dir)]
    return command


def extract_observability_args(
    argv: Sequence[str],
) -> tuple[list[str], str, Path | None]:
    """Split ``--dag-run-id VALUE`` / ``--observability-dir VALUE`` out of argv.

    dbt flags are intercepted by the wrapper (never forwarded to the dbt
    executable); everything else is returned unchanged.
    """
    dag_run_id = ""
    observability_dir: Path | None = None
    forwarded: list[str] = []
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg in OBSERVABILITY_FLAGS:
            if index + 1 >= len(argv):
                raise ValueError(f"{arg} requires a value")
            value = argv[index + 1]
            if arg == "--dag-run-id":
                dag_run_id = value
            else:
                observability_dir = Path(value)
            index += 2
            continue
        forwarded.append(arg)
        index += 1
    return forwarded, dag_run_id, observability_dir


def _read_run_results(path: Path) -> dict:
    """Load ``dbt/target/run_results.json`` defensively; never raise on parse."""
    if not path.is_file():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError, TypeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    results = payload.get("results")
    if not isinstance(results, list):
        return {}
    return {
        "metadata": payload.get("metadata"),
        "results": [r for r in results if isinstance(r, dict) and r.get("unique_id")],
    }


def _emit_model_events(run_id: str, payload: dict) -> None:
    for result in payload.get("results", []):
        unique_id = str(result.get("unique_id", ""))
        parts = unique_id.split(".")
        project = parts[1] if len(parts) > 2 else ""
        model = parts[2] if len(parts) > 2 else unique_id
        status = str(result.get("status", "skipped"))
        failed_status = status in ("error", "fail")
        observability.emit(
            pipeline="dbt",
            stage="dbt",
            run_id=run_id,
            status=status,
            source=project,
            batch_id=model,
            error_code=("" if not failed_status else "DBT_MODEL_FAILED"),
            message=f"{status}: {unique_id}",
            payload={
                "node": unique_id,
                "execution_time_seconds": result.get("execution_time"),
                "error": result.get("message"),
            },
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Run dbt with repository defaults and return its exit code."""
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        forwarded, dag_run_id, observability_dir = extract_observability_args(arguments)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    settings = Settings.from_env(observability_dir=observability_dir)
    if not settings.postgres_password:
        print(MISSING_PASSWORD_MESSAGE, file=sys.stderr)
        return 1
    observability.set_context(dag_run_id=dag_run_id, events_dir=settings.observability_dir)
    enabled = bool(dag_run_id or observability_dir or os.environ.get("MRI_OBSERVABILITY_DIR"))
    if enabled:
        run_id = f"dbt-{new_run_id()}"
        observability.emit(
            pipeline="dbt",
            stage="dbt",
            run_id=run_id,
            status="started",
            message=f"dbt run {run_id} starting",
            payload={"command": " ".join(forwarded)},
        )
    try:
        command = build_command(forwarded)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    started = time.perf_counter()
    completed = subprocess.run(command, env=build_env(settings), check=False)
    duration_ms = observability.elapsed_ms(started)
    code = int(completed.returncode)
    if enabled:
        observability.emit(
            pipeline="dbt",
            stage="dbt",
            run_id=run_id,
            status=("success" if code == 0 else "failed"),
            duration_ms=duration_ms,
            error_code=("" if code == 0 else "DBT_FAILED"),
            message=f"dbt finished with exit code {code}",
        )
        payload = _read_run_results(find_repo_root() / TARGET_RUN_RESULTS)
        if payload.get("results"):
            _emit_model_events(run_id, payload)
    return code
