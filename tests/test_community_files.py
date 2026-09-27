"""GOALS P8.4: the community files an outside contributor looks for exist."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_files_present():
    for name in (
        "SECURITY.md",
        "CODE_OF_CONDUCT.md",
        "CONTRIBUTING.md",
        "LICENSE",
        ".github/pull_request_template.md",
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/ISSUE_TEMPLATE/config.yml",
    ):
        assert (ROOT / name).is_file(), name


def test_code_of_conduct_references_covenant_2_1():
    text = (ROOT / "CODE_OF_CONDUCT.md").read_text()
    assert "contributor-covenant.org/version/2/1/code_of_conduct" in text


def test_issue_forms_parse():
    for path in (ROOT / ".github" / "ISSUE_TEMPLATE").glob("*.yml"):
        assert isinstance(yaml.safe_load(path.read_text()), dict), path.name


def test_contributing_points_only_at_tracked_paths():
    # dev-artifacts/ and CLAUDE.md are gitignored; an outside contributor
    # cannot follow a pointer to them.
    text = (ROOT / "CONTRIBUTING.md").read_text()
    assert "dev-artifacts/" not in text
    assert "CLAUDE.md" not in text
    assert "pytest tests/ -x --timeout" not in text  # pytest-timeout is not a dev dependency


# Maintainer material that exists only in the maintainers' checkout (excluded
# locally, not in this repository). A tracked file that points at it sends an
# outside reader to a path they do not have.
_LOCAL_ONLY_EVERYWHERE = ("dev-artifacts/", "CLAUDE.md")
# Cited from code comments as decision ids; public docs must not rely on it.
_LOCAL_ONLY_IN_DOCS = ("AML-GOALS",)
_PUBLIC_DOCS = ("docs", "README.md", "CHANGELOG.md", "CONTRIBUTING.md")


def _git_grep(patterns: tuple[str, ...], *pathspec: str) -> list[str]:
    if not (ROOT / ".git").exists():
        pytest.skip("not a git checkout")
    args = ["git", "grep", "-nIF"]
    for pattern in patterns:
        args += ["-e", pattern]
    args += ["--", *pathspec, ":!tests/test_community_files.py", ":!docs/internal/"]
    result = subprocess.run(args, cwd=ROOT, capture_output=True, text=True)
    assert result.returncode in (0, 1), result.stderr  # 1 means no match
    return result.stdout.splitlines()


def test_tracked_files_do_not_point_at_local_only_paths():
    hits = _git_grep(_LOCAL_ONLY_EVERYWHERE, ".")
    hits += _git_grep(_LOCAL_ONLY_IN_DOCS, *_PUBLIC_DOCS)
    assert not hits, "tracked files cite local-only maintainer material:\n" + "\n".join(hits)
