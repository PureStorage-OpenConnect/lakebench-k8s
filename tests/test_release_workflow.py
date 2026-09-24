"""The release workflow must not publish unless CI passed on the tagged commit,
the commit is on main, and the release gate passed; PyPI must never get a
version that has no GitHub Release (1.5.0 shipped to PyPI from an unmerged
commit)."""

from __future__ import annotations

from pathlib import Path

import yaml

WF = Path(__file__).resolve().parents[1] / ".github" / "workflows"


def _load(name: str) -> dict:
    return yaml.safe_load((WF / name).read_text())


def _on(wf: dict) -> dict:
    # PyYAML reads the bare key `on` as boolean True.
    return wf.get("on", wf.get(True))


def _needs(job: dict) -> set[str]:
    needs = job.get("needs", [])
    return {needs} if isinstance(needs, str) else set(needs)


def _upstream(jobs: dict, name: str) -> set[str]:
    seen: set[str] = set()
    todo = list(_needs(jobs[name]))
    while todo:
        n = todo.pop()
        if n not in seen:
            seen.add(n)
            todo.extend(_needs(jobs[n]))
    return seen


def test_ci_is_reusable_and_runs_on_every_branch():
    triggers = _on(_load("ci.yml"))
    assert "workflow_call" in triggers
    assert triggers["push"]["branches"] == ["**"]


def test_one_workflow_publishes_on_tags():
    tag_workflows = [
        p.name for p in WF.glob("*.yml") if "tags" in (_on(_load(p.name)).get("push") or {})
    ]
    assert tag_workflows == ["release.yml"]


def test_publish_order():
    jobs = _load("release.yml")["jobs"]
    assert jobs["ci"]["uses"] == "./.github/workflows/ci.yml"
    assert {"ci", "verify-tag", "gate", "build-dist", "build-binary", "github-release"} <= (
        _upstream(jobs, "publish")
    )
    assert "publish" not in _upstream(jobs, "github-release")


def test_only_the_release_job_can_write():
    wf = _load("release.yml")
    assert wf["permissions"] == {"contents": "read"}
    writers = [
        n for n, j in wf["jobs"].items() if (j.get("permissions") or {}).get("contents") == "write"
    ]
    assert writers == ["github-release"]


def test_verify_tag_checks_the_tested_sha_and_version():
    steps = " ".join(
        str(s.get("run", "")) for s in _load("release.yml")["jobs"]["verify-tag"]["steps"]
    )
    assert 'merge-base --is-ancestor "$GITHUB_SHA" origin/main' in steps
    assert "scripts/check_version.py --tag" in steps


def test_gate_runs_release_only_checks_strictly():
    steps = " ".join(str(s.get("run", "")) for s in _load("release.yml")["jobs"]["gate"]["steps"])
    assert "scripts/release_gate.py" in steps and "--require-all" in steps
    for check in ("examples", "version", "changelog", "em-dashes", "uat-results"):
        assert check in steps
