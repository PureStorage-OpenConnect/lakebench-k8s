"""Release workflows must not publish unless CI passed on the tagged commit
and the tag is on main (1.5.0 shipped to PyPI from an unmerged commit)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

WF = Path(__file__).resolve().parents[1] / ".github" / "workflows"


def _load(name: str) -> dict:
    return yaml.safe_load((WF / name).read_text())


def _on(wf: dict) -> dict:
    # PyYAML reads the bare key `on` as boolean True.
    return wf.get("on", wf.get(True))


def test_ci_is_reusable_and_runs_on_every_branch():
    ci = _load("ci.yml")
    triggers = _on(ci)
    assert "workflow_call" in triggers
    assert triggers["push"]["branches"] == ["**"]


@pytest.mark.parametrize(
    ("workflow", "publisher"), [("release.yml", "publish"), ("binary.yml", "release")]
)
def test_publish_waits_for_ci_and_tag_checks(workflow, publisher):
    wf = _load(workflow)
    jobs = wf["jobs"]
    assert jobs["ci"]["uses"] == "./.github/workflows/ci.yml"
    needs = jobs[publisher]["needs"]
    needs = [needs] if isinstance(needs, str) else needs
    assert {"ci", "verify-tag"} <= set(needs)
    steps = " ".join(str(s.get("run", "")) for s in jobs["verify-tag"]["steps"])
    assert "merge-base --is-ancestor" in steps and "origin/main" in steps
    assert "scripts/check_version.py --tag" in steps
