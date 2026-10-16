"""PHASE 20 — security-hardening regressions (offline, no services required).

These tests guard the deliberate controls from
docs/security/security-hardening.md:

- every container image is pinned (never ``latest``) and the MinIO image is the
  maintained digest-pinned Chainguard build (not the CVE-affected, unpullable
  official release);
- every published host port is bound to the loopback address only;
- ``.env`` (real credentials) is never tracked and never committed;
- ``.env.example`` declares placeholders only (all values empty);
- compose interpolations all resolve against ``.env.example`` keys;
- CI never references the removed MinIO image and uses placeholder creds only;
- tracked deliverable docs/reports contain no real credential-shaped strings.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILES = [
    REPO_ROOT / "infrastructure/docker/docker-compose.yml",
    REPO_ROOT / "infrastructure/docker/docker-compose.airflow.yml",
    REPO_ROOT / "infrastructure/docker/docker-compose.superset.yml",
    REPO_ROOT / "infrastructure/docker/docker-compose.observability.yml",
]
ENV_EXAMPLE = REPO_ROOT / ".env.example"
ENV_FILE = REPO_ROOT / ".env"
GITIGNORE = REPO_ROOT / ".gitignore"
CI_FILE = REPO_ROOT / ".github/workflows/ci.yml"

# Old MinIO community image: removed from registries and affected by
# CVE-2025-62506 (see ADR-001).
OLD_MINIO_IMAGE = "minio/minio:RELEASE.2025-09-07T16-13-09Z"

SECRET_PATTERNS = (
    re.compile(
        r"^[ \t]*(?P<k>(?:POSTGRES|MINIO|BI_READER|SUPERSET|GRAFANA)_"
        r"(?:PASSWORD|SECRET_KEY|SECRET|ACCESS_KEY)|"
        r"AIRFLOW__CORE__FERNET_KEY|AIRFLOW_JWT_SECRET)"
        r"\s*=\s*(?![A-Za-z_][\w.]*\()\S+"
    ),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"Authorization:\s*(?:Basic|Bearer)\s+[^\s]{12,}"),
)


def _read_key_values(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^([A-Za-z0-9_]+)=(.*)$", line)
        if match:
            values[match.group(1)] = match.group(2)
    return values


def _services() -> list[tuple[str, str, dict]]:
    """Return ``(label, service_name, definition)`` for every service."""
    import yaml

    services: list[tuple[str, str, dict]] = []
    for path in COMPOSE_FILES:
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        for name, definition in (doc or {}).get("services", {}).items():
            services.append((path.name, str(name), definition))
    return services


def _tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [REPO_ROOT / name for name in result.stdout.splitlines()]


def _secret_lines(text: str) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if any(pattern.search(line) for pattern in SECRET_PATTERNS):
            hits.append((line_number, line.strip()[:80]))
    return hits


def test_compose_images_are_pinned_not_latest() -> None:
    for label, name, definition in _services():
        image = definition.get("image")
        assert isinstance(image, str), f"{label}:{name} has no pinned image"
        assert not image.endswith(":latest"), f"{label}:{name} uses 'latest'"


def test_minio_image_is_digest_pinned_chainguard() -> None:
    for _label, name, definition in _services():
        if name != "minio":
            continue
        image = definition["image"]
        assert isinstance(image, str)
        assert image.startswith("cgr.dev/chainguard/minio@sha256:")
        assert image.count("@sha256:") == 1
        assert OLD_MINIO_IMAGE not in image


def test_published_ports_are_loopback_only() -> None:
    pattern = re.compile(r"^127\.0\.0\.1:.*:\d+$")
    for label, name, definition in _services():
        for port in definition.get("ports", []):
            assert isinstance(port, str)
            assert pattern.match(port), f"{label}:{name} binds {port} not on 127.0.0.1"


def test_local_env_is_git_ignored_and_untracked() -> None:
    ignore_rules = GITIGNORE.read_text(encoding="utf-8").splitlines()
    assert any(rule.strip() == ".env" for rule in ignore_rules), ".env not in .gitignore"
    if not (os.environ.get("CI") or os.environ.get("GITHUB_ACTIONS")):
        assert ENV_FILE.is_file(), ".env missing (expected an ignored local file)"
    ls_files = subprocess.run(
        ["git", "ls-files", "--cached", ".env"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    assert ls_files.stdout.strip() == "", "the real .env is tracked by git"


def test_env_example_holds_placeholders_only() -> None:
    for key, value in _read_key_values(ENV_EXAMPLE).items():
        assert value == "", f".env.example key {key} carries a non-empty value"


def test_compose_interpolations_are_declared_in_env_example() -> None:
    example_keys = set(_read_key_values(ENV_EXAMPLE))
    for path in COMPOSE_FILES:
        text = path.read_text(encoding="utf-8")
        # `${KEY}` without a `:-default` is mandatory and must be declared;
        # `${KEY:-default}` is an optional override and may stay undeclared.
        # `$${KEY}` is escaped for the container shell, not a Compose var.
        for key in re.findall(r"(?<!\$)\$\{([A-Z0-9_]+)\}", text):
            assert key in example_keys, f"{path.name} interpolates undeclared ${key}"


def test_ci_uses_chainguard_minio_and_placeholder_creds_only() -> None:
    text = CI_FILE.read_text(encoding="utf-8")
    assert OLD_MINIO_IMAGE not in text, "CI still references the removed MinIO image"
    assert "cgr.dev/chainguard/minio@sha256:" in text
    assert "ci-minio-access-key" in text
    assert "ci-minio-secret-key" in text


def test_tracked_deliverables_contain_no_secret_values() -> None:
    violations: list[str] = []
    for path in _tracked_files():
        relative = path.relative_to(REPO_ROOT)
        if path.suffix not in {
            ".md",
            ".py",
            ".sql",
            ".yml",
            ".yaml",
            ".txt",
            ".ps1",
            ".sh",
            ".json",
            ".toml",
        }:
            continue
        if path.name in {".env", ".env.example"}:
            continue
        content = path.read_text(encoding="utf-8", errors="replace")
        violations.extend(f"{relative}:{line}: {text}" for line, text in _secret_lines(content))
    assert not violations, "credential-shaped strings found in tracked files:\n" + "\n".join(
        violations
    )


def test_secret_scanner_flags_literals_but_not_env_lookups() -> None:
    ignored = (
        '            minio_access_key=merged.get("MINIO_ACCESS_KEY", ""),',
        '            postgres_password=merged.get("POSTGRES_PASSWORD", ""),',
        "        minio_access_key=base.minio_access_key,",
        '        minio_access_key="unit-test-key",',
    )
    for line in ignored:
        assert _secret_lines(line) == [], line

    flagged = (
        "MINIO_SECRET_KEY=2f7d1c9a4b6e8f0d",
        "POSTGRES_PASSWORD=hunter2",
        "AIRFLOW__CORE__FERNET_KEY=Zm9vYmFyYmF6cXV4",
        "aws_access_key_id = AKIA" + "IOSFODNN7EXAMPLE",
        "Authorization: Bearer abc" + "defghijklmnop",
    )
    for line in flagged:
        assert _secret_lines(line), line
