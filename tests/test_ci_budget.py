"""scripts/ci_budget.py: the wall-time budget for CI legs (the fast path)."""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import time
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
    # The elapsed seconds depend on the host's load; the shape does not.
    assert re.search(r"::error::unit took \d+ s, budget 1 s", res.stdout), res.stdout


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
    env = {**os.environ, "GITHUB_STEP_SUMMARY": str(summary)}
    res = _run("--seconds", "60", "--label", "fast", "--", sys.executable, "-c", "pass", env=env)
    assert res.returncode == 0, res.stdout + res.stderr
    assert summary.read_text().startswith("fast: ") and "(budget 60 s)" in summary.read_text()


def test_usage_errors():
    assert _run("--seconds", "5").returncode == 2  # no --
    assert _run("--seconds", "5", "--").returncode == 2  # no command
    assert _run("--seconds", "0", "--", "true").returncode == 2
    assert _run("--seconds", "5", "--", "/no/such/binary").returncode == 127


def test_a_signal_stops_the_command_and_still_records_the_time(tmp_path):
    summary = tmp_path / "summary.md"
    env = {**os.environ, "GITHUB_STEP_SUMMARY": str(summary)}
    marker = tmp_path / "child-started"
    proc = subprocess.Popen(
        [
            sys.executable,
            str(SCRIPT),
            "--seconds",
            "60",
            "--label",
            "slow",
            "--",
            sys.executable,
            "-c",
            f"import pathlib, time; pathlib.Path({str(marker)!r}).touch(); time.sleep(60)",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    deadline = time.monotonic() + 30
    while not marker.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert marker.exists(), "the child never started"
    proc.send_signal(signal.SIGTERM)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == 128 + signal.SIGTERM, out
    assert "stopped by signal" in out
    assert "(stopped by a signal)" in summary.read_text()


def test_sigint_then_sigterm_stops_a_command_that_ignores_sigint(tmp_path):
    # The runner cancels with SIGINT, then SIGTERM; make does not pass SIGINT
    # on. The whole command group must stop, once, with the time recorded.
    summary = tmp_path / "summary.md"
    env = {**os.environ, "GITHUB_STEP_SUMMARY": str(summary)}
    pidfile = tmp_path / "child.pid"
    body = (
        "import os, pathlib, signal, time; signal.signal(signal.SIGINT, signal.SIG_IGN); "
        f"pathlib.Path({str(pidfile)!r}).write_text(str(os.getpid())); time.sleep(60)"
    )
    proc = subprocess.Popen(
        [sys.executable, str(SCRIPT), "--seconds", "60", "--label", "stub", "--"]
        + [sys.executable, "-c", body],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )
    deadline = time.monotonic() + 30
    while not pidfile.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert pidfile.exists(), "the child never started"
    proc.send_signal(signal.SIGINT)
    time.sleep(0.2)
    proc.send_signal(signal.SIGTERM)
    out, err = proc.communicate(timeout=60)
    assert proc.returncode == 128 + signal.SIGINT, out + err
    assert "Traceback" not in err
    assert summary.read_text().count("(stopped by a signal)") == 1
    child_pid = int(pidfile.read_text())
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            os.kill(child_pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    else:
        raise AssertionError(f"child {child_pid} still running")
