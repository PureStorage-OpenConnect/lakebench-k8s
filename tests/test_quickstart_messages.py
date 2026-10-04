"""What a user following the README quick start is told (outcome 1, Easy)."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.deploy import DeploymentResult, DeploymentStatus
from lakebench.exit_codes import ExitCode

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "v14user.yaml"


def _cfg(cycles: int = 1):
    return SimpleNamespace(architecture=SimpleNamespace(pipeline=SimpleNamespace(cycles=cycles)))


def test_deploy_inside_run_prints_no_next_steps(monkeypatch):
    from lakebench.cli import _deploy

    seen = []
    monkeypatch.setattr(_deploy, "deploy", lambda **kw: seen.append(_deploy._next_steps(_cfg())))
    _deploy.deploy_inside_run(None)
    # run carries on by itself: no "Next: lakebench generate" while it deploys.
    assert seen == [""]
    # A plain deploy still names the next steps.
    assert "lakebench generate" in _deploy._next_steps(_cfg())
    assert "lakebench run" in _deploy._next_steps(_cfg(cycles=3))


def _destroy(monkeypatch, *args, progress=()):
    engine = MagicMock()

    def destroy_all(progress_callback=None, **_kw):
        for component, status, message in progress:
            progress_callback(component, status, message)
        return [DeploymentResult(component="namespace", status=DeploymentStatus.SUCCESS)]

    engine.destroy_all.side_effect = destroy_all
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda *a, **k: engine)
    return CliRunner().invoke(app, ["destroy", str(FIXTURE), *args])


def test_non_interactive_destroy_names_the_flag_the_readme_uses(monkeypatch):
    res = _destroy(monkeypatch)
    assert res.exit_code == ExitCode.NOT_CONFIRMED, res.output
    assert "Pass --yes" in res.output
    assert "--force" not in res.output


def test_destroy_progress_has_no_internal_heading_or_negative_time(monkeypatch):
    res = _destroy(
        monkeypatch,
        "--yes",
        progress=[
            ("category1", DeploymentStatus.IN_PROGRESS, "Removing remaining namespaced objects..."),
            ("category1", DeploymentStatus.SUCCESS, "Removed the remaining namespaced objects"),
            # Reported done with no start of its own.
            ("watch-list", DeploymentStatus.SUCCESS, "Dropped the namespace from the watch list"),
        ],
    )
    assert "Removed the remaining namespaced objects" in res.output, res.output
    lines = [line.strip() for line in res.output.splitlines()]
    assert "category1" not in lines
    assert "-0.0s" not in res.output
