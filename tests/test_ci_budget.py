"""scripts/ci_budget.py: the wall-time budget for CI legs (the fast path)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ci_budget.py"


def _run(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args], capture_output=True, text=True, env=env
    )


def test_over_budget_fails():
    res = _run(
        "--seconds",
        "1",
        "--label",
        "unit",
        "--",
        sys.executable,
        "-c",
        "import time; time.sleep(2)",
    )
    assert res.returncode == 1, res.stdout + res.stderr
    assert "::error::unit took 2 s, budget 1 s" in res.stdout


def test_command_failure_wins():
    res = _run(
        "--seconds",
        "1",
        "--",
        sys.executable,
        "-c",
        "import sys, time; time.sleep(1.5); sys.exit(3)",
    )
    assert res.returncode == 3
    assert "::error::" not in res.stdout


def test_within_budget_passes_and_writes_the_summary(tmp_path):
    summary = tmp_path / "summary.md"
    import os

    env = {**os.environ, "GITHUB_STEP_SUMMARY": str(summary)}
    res = _run("--seconds", "60", "--label", "fast", "--", sys.executable, "-c", "pass", env=env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert summary.read_text().startswith("fast: ") and "(budget 60 s)" in summary.read_text()


def test_usage_errors():
    assert _run("--seconds", "5").returncode == 2  # no --
    assert _run("--seconds", "5", "--").returncode == 2  # no command
    assert _run("--seconds", "0", "--", "true").returncode == 2
    assert _run("--seconds", "5", "--", "/no/such/binary").returncode == 127
