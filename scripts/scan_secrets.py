"""Local Phase 20 secret scanner.

Scans (a) all tracked files in HEAD and (b) the working tree (tracked +
untracked source/config), applying heuristic regex patterns. NEVER prints the
matched value — only the pattern id and the file path (+ blob path for history).

Usage: uv run python scripts-ignored/scan_secrets.py
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("aws-access-key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    (
        "generic-secret-eq",
        re.compile(
            r"(?im)^\s*(?:SECRET[\w_]*|.*PASSWORD|.*PASSWD|.*API[\w_]*KEY|.*ACCESS_KEY"
            r"|.*SECRET_KEY|.*TOKEN[^ ]*|AUTH[\w_]*)\s*=\s*[\"']?([^\"'\s;]{8,})"
        ),
    ),
    (
        "collon-secret",
        re.compile(r"(?i)\b(?:password|passwd|secret|token|api_?key)\b\s*[:=]\s*[^\"'\s,;]{8,}"),
    ),
    ("private-key", re.compile(r"-----BEGIN (?:RSA |OPENSSH |EC |DSA |PGP )?PRIVATE KEY-----")),
    ("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{6,}\b")),
    (
        "connstr-password",
        re.compile(
            r"(?i)\b(?:postgres(?:ql)?|mysql|mongodb(?:\+srv)?|redis|amqp|s3|https?)://[^\s:/]+:[^\s@/]{3,}@"
        ),
    ),
    ("github-token", re.compile(r"\b(?:ghp_|github_pat_|gho_|ghu_)[A-Za-z0-9_]{20,}\b")),
    ("slack-token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
    ("gcp-sa", re.compile(r"\"type\":\s*\"service_account\"")),
    ("fernet", re.compile(r"\b[0-9A-Za-z_-]{44}=\b")),
    (
        "hex-sha256-secret-looking",
        re.compile(r"(?i)\b(?:secret|token|key)[\w_-]*\s*=\s*[\"']?[0-9a-f]{32,}[\"']?"),
    ),
]

SKIP_DIRS = {
    ".git",
    ".venv",
    "node_modules",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "data",
    "generated",
    "datasets",
    "reports",
    "output",
    "dbt_packages",
    "dbt_target",
    "target",
    "logs/dbt",
    "dbt/logs",
    ".uv-cache",
    ".cache",
}
SKIP_DIR_PAIRS = {("airflow", "logs")}
SKIP_SUFFIX = {
    ".pyc",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".pdf",
    ".parquet",
    ".csv.gz",
    ".sqlite",
    ".db",
    ".egg-info",
    ".log",
    ".lock",
    ".whl",
    ".so",
    ".dll",
}


def iter_worktree() -> list[Path]:
    files: list[Path] = []
    for path in sorted(REPO.rglob("*")):
        parts = path.relative_to(REPO).parts
        if any(part in SKIP_DIRS for part in parts) or any(
            pair == parts[i : i + 2] for pair in SKIP_DIR_PAIRS for i in range(len(parts) - 1)
        ):
            continue
        if not path.is_file() or path.suffix in SKIP_SUFFIX:
            continue
        files.append(path)
    return files


def scan_bytes(data: bytes, on_hit) -> None:
    try:
        text = data.decode("utf-8", errors="replace")
    except Exception:
        return
    for pid, pattern in PATTERNS:
        if pattern.search(text):
            on_hit(pid)


def main() -> int:
    findings: list[str] = []
    counts: dict[str, int] = {}

    def hit(pid: str, where: str) -> None:
        counts[pid] = counts.get(pid, 0) + 1
        findings.append(f"  [{pid}] {where}")

    # 1) Working tree
    for path in iter_worktree():
        rel = str(path.relative_to(REPO)).replace("\\", "/")
        try:
            data = path.read_bytes()
        except OSError:
            continue
        scan_bytes(data, lambda pid, p=rel: hit(pid, p))

    # 2) Git history (only one commit exists)
    try:
        tree = subprocess.run(
            ["git", "ls-tree", "-r", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True
        ).stdout
        for line in tree.splitlines():
            parts = line.split("\t")
            if len(parts) != 2:
                continue
            blob = parts[0].split()[2]
            path = parts[1]
            if path.startswith(".git") or any(seg in path.split("/") for seg in ("dags", "logs")):
                pass
            try:
                data = subprocess.run(
                    ["git", "cat-file", "blob", blob], cwd=REPO, capture_output=True, check=True
                ).stdout
            except subprocess.CalledProcessError:
                continue
            scan_bytes(data, lambda pid, p=path: hit(pid, f"HEAD:{p}"))
    except subprocess.CalledProcessError:
        print("git tree listing unavailable", file=sys.stderr)

    print(f"pattern counts: {counts or 'no hits'}")
    for line in findings:
        print(line)
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
