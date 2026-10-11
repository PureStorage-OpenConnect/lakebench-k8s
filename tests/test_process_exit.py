"""A finished command exits with its status even when a daemon thread holds
the logging lock at shutdown (a completed ``lakebench run`` once hung for
hours there, so a script's next step, destroy, never ran)."""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pytest

_SCRIPT = textwrap.dedent(
    """
    import logging, sys, threading, time
    from lakebench.cli._process_exit import run_and_exit

    logging.getLogger("lb-test").addHandler(logging.StreamHandler(sys.stderr))

    held = threading.Event()

    def hold():
        logging._lock.acquire()
        held.set()
        time.sleep(120)

    threading.Thread(target=hold, daemon=True).start()
    if not held.wait(10):
        sys.exit(99)  # the lock was never held: the test would prove nothing

    def app():
        print("command output")
        {body}

    run_and_exit(app)
    """
)


@pytest.mark.parametrize(
    ("body", "code"),
    [
        ("return None", 0),
        ("raise SystemExit(3)", 3),
        ("raise SystemExit(None)", 0),
        ("raise SystemExit('refused: reason')", 1),
    ],
)
def test_exit_status_survives_a_held_logging_lock(body, code):
    proc = subprocess.run(
        [sys.executable, "-c", _SCRIPT.format(body=body)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == code
    assert "command output" in proc.stdout
    if isinstance(code, int) and "refused" in body:
        assert "refused: reason" in proc.stderr
