"""Guard against re-introducing hazardous Polaris guidance.

The server-wide `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` feature flag is
not a supported STS-skip fix on FlashBlade. It drops endpoint and
path-style settings from Polaris's StorageAccessConfig, so server-side
S3FileIO falls back to `s3.amazonaws.com` and every request 404s.

The real fix is per-catalog `stsUnavailable: true` in the bootstrap
payload. This test asserts that the hazardous env-var name does not
appear anywhere in the tree, so no reader can grep it up and try it.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

FORBIDDEN = "SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION"

# Directories that either belong to another tool's cache or are not tracked
# in git. Skip them so a stale build artefact does not fail this test.
SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "target",
    "node_modules",
    ".venv",
    "venv",
    "dist",
    "build",
    "htmlcov",
    ".coverage",
    "lakebench-output",
    "dev-artifacts",
}

# Files that are non-text or would produce false positives from binary noise.
SKIP_SUFFIXES = {
    ".pyc",
    ".so",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".pdf",
    ".zip",
    ".gz",
    ".tar",
    ".whl",
    ".jar",
    ".class",
    ".woff",
    ".woff2",
    ".ttf",
    ".otf",
}


def _iter_text_files(root: Path):
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.relative_to(root).parts):
            continue
        if path.suffix.lower() in SKIP_SUFFIXES:
            continue
        yield path


def test_forbidden_env_var_absent_from_tree() -> None:
    """No file in the repo should mention the hazardous env var by name.

    This test file itself is excluded from the search because it documents
    the forbidden token in a string literal by design.
    """
    self_path = Path(__file__).resolve()
    offenders: list[str] = []
    for path in _iter_text_files(REPO_ROOT):
        if path.resolve() == self_path:
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        if FORBIDDEN in text:
            offenders.append(str(path.relative_to(REPO_ROOT)))

    assert not offenders, (
        "Hazardous Polaris guidance re-appeared. The server-wide "
        f"{FORBIDDEN} flag is not a supported STS-skip fix; use per-catalog "
        "stsUnavailable=true in the bootstrap payload instead.\n"
        "Files still mentioning the forbidden flag:\n  " + "\n  ".join(offenders)
    )
