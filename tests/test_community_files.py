"""GOALS P8.4: the community files an outside contributor looks for exist."""

from __future__ import annotations

from pathlib import Path

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
