#!/usr/bin/env python3
"""Run a command under a wall-time budget (the CI fast path).

Usage:
    python scripts/ci_budget.py --seconds N [--label L] -- <command> [args...]

Runs the command, measures its wall time and:

- exits with the command's own exit code when that is not zero (a failing
  command is never reported as a budget problem);
- otherwise exits 1 when the elapsed time exceeds N seconds, printing
  ``::error::<L> took <s> s, budget <N> s`` (a GitHub Actions annotation);
- otherwise exits 0.

With ``GITHUB_STEP_SUMMARY`` set it appends ``<L>: <s> s (budget <N> s)`` to
that file, pass or fail, so every run records its timing. A job's
``timeout-minutes`` is only a backstop above the budget.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from collections.abc import Sequence


class _Stopped(Exception):
    """SIGTERM or SIGINT arrived (a cancelled run or a job timeout)."""


def _stop(signum: int, _frame: object) -> None:
    raise _Stopped(signum)


def _stop_group(child: subprocess.Popen) -> None:
    """SIGTERM the command's process group, then SIGKILL it after 30 s."""
    for sig, wait in ((signal.SIGTERM, 30.0), (signal.SIGKILL, 10.0)):
        try:
            os.killpg(child.pid, sig)
        except ProcessLookupError:
            return
        try:
            child.wait(timeout=wait)
            return
        except subprocess.TimeoutExpired:
            continue


def _summary(label: str, elapsed: float, budget: float, note: str = "") -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"{label}: {elapsed:.0f} s (budget {budget:.0f} s){note}\n")


def main(argv: Sequence[str] | None = None) -> int:
    args_in = list(sys.argv[1:] if argv is None else argv)
    if "--" not in args_in:
        print("usage: ci_budget.py --seconds N [--label L] -- <command>", file=sys.stderr)
        return 2
    split = args_in.index("--")
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seconds", type=float, required=True)
    parser.add_argument("--label", default="step")
    opts = parser.parse_args(args_in[:split])
    cmd = args_in[split + 1 :]
    if not cmd:
        print("ci_budget.py: no command after --", file=sys.stderr)
        return 2
    if opts.seconds <= 0:
        print("ci_budget.py: --seconds must be positive", file=sys.stderr)
        return 2

    # A cancel or a timeout-minutes kill signals this process (the runner
    # sends SIGINT, then SIGTERM): the command, in its own process group so
    # every process it started is reached, gets SIGTERM, and the time it ran
    # is still recorded, since the slowest runs are the ones that get killed.
    # The handlers go in before the command starts, so no signal can orphan
    # it, and any later signal is ignored while the group is stopped.
    previous = {s: signal.signal(s, _stop) for s in (signal.SIGTERM, signal.SIGINT)}
    start = time.monotonic()
    child = None
    try:
        try:
            child = subprocess.Popen(cmd, start_new_session=True)
        except OSError as exc:
            print(f"::error::{opts.label}: cannot run {cmd[0]}: {exc}")
            return 127
        rc = child.wait()
    except _Stopped as stop:
        for s in previous:
            signal.signal(s, signal.SIG_IGN)
        elapsed = time.monotonic() - start
        _summary(opts.label, elapsed, opts.seconds, " (stopped by a signal)")
        print(f"::error::{opts.label} stopped by signal {stop.args[0]} after {elapsed:.0f} s")
        if child is not None:
            _stop_group(child)
        return 128 + int(stop.args[0])
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)
    elapsed = time.monotonic() - start

    _summary(opts.label, elapsed, opts.seconds)
    if rc != 0:
        return rc
    if elapsed > opts.seconds:
        print(f"::error::{opts.label} took {elapsed:.0f} s, budget {opts.seconds:.0f} s")
        return 1
    print(f"{opts.label}: {elapsed:.0f} s (budget {opts.seconds:.0f} s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
