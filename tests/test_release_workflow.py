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
def test_publish_waits_for_ci_and_release_gate(workflow, publisher):
    jobs = _load(workflow)["jobs"]
    assert jobs["ci"]["uses"] == "./.github/workflows/ci.yml"
    assert jobs["release-gate"]["uses"] == "./.github/workflows/release-gate.yml"
    needs = jobs[publisher]["needs"]
    needs = [needs] if isinstance(needs, str) else needs
    assert {"ci", "release-gate"} <= set(needs)


def test_release_gate_workflow_checks_tag_and_runs_the_script():
    gate = _load("release-gate.yml")
    assert "workflow_call" in _on(gate)
    jobs = gate["jobs"]
    tag_steps = " ".join(str(s.get("run", "")) for s in jobs["verify-tag"]["steps"])
    assert "merge-base --is-ancestor" in tag_steps and "origin/main" in tag_steps
    assert "scripts/check_version.py --tag" in tag_steps
    gate_steps = " ".join(str(s.get("run", "")) for s in jobs["gate"]["steps"])
    assert "scripts/release_gate.py" in gate_steps and "--require-all" in gate_steps
