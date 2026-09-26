"""The continuous loop's last minute: a due benchmark round that cannot fit
is skipped once, and the loop never busy-spins with 0 s sleeps."""

from __future__ import annotations

import ast
import inspect

from lakebench.cli._sustained import late_benchmark_round_skip, loop_sleep_seconds

RUN = 1800.0


def _simulate(start: float, next_round_at: float, min_remaining: float, skip: bool = True):
    """Passes, zero sleeps and journal lines from `start` to the run end."""
    t, passes, zero_sleeps, journal = start, 0, 0, []
    while t < RUN and passes < 10_000:
        passes += 1
        msg = late_benchmark_round_skip(True, t, next_round_at, RUN - t, min_remaining)
        if msg and skip:
            journal.append(msg)
            next_round_at = float("inf")
        sleep = loop_sleep_seconds(t, t + 30, next_round_at, float("inf"), float("inf"), RUN)
        if sleep <= 0:
            zero_sleeps += 1
        journal.append("health")
        t += sleep + 0.001  # work per pass
    return passes, zero_sleeps, journal


def test_last_minute_skips_the_round_once_and_sleeps_to_the_end():
    passes, zero, journal = _simulate(start=1760.0, next_round_at=1750.0, min_remaining=72.0)
    skips = [j for j in journal if j.startswith("benchmark round skipped")]
    assert skips == ["benchmark round skipped: 40 s left, need 72 s"]
    assert zero == 0
    assert passes <= 3  # a couple of health checks, not one per millisecond
    assert journal.count("health") == passes


def test_sleep_is_floored_even_when_an_event_is_already_due():
    assert loop_sleep_seconds(100.0, 130.0, 50.0, RUN) == 1.0
    assert loop_sleep_seconds(1799.5, 1829.5, 50.0, RUN) == 0.5  # never past the end
    assert loop_sleep_seconds(RUN, RUN + 30, RUN) == 0.0


def test_round_that_fits_is_not_skipped():
    assert late_benchmark_round_skip(True, 1000.0, 900.0, 800.0, 72.0) is None
    assert late_benchmark_round_skip(False, 1760.0, 1750.0, 40.0, 72.0) is None
    assert late_benchmark_round_skip(True, 1700.0, 1750.0, 100.0, 72.0) is None


def test_loop_wires_the_skip_and_the_floored_sleep():
    import lakebench.cli._sustained as sus

    src = inspect.getsource(sus._run_sustained)
    tree = ast.parse(src)
    calls = {
        n.func.id
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    }
    assert {"late_benchmark_round_skip", "loop_sleep_seconds"} <= calls
    ifs = [n for n in ast.walk(tree) if isinstance(n, ast.If) and ast.unparse(n.test) == "skip_msg"]
    assert len(ifs) == 1
    body = ast.unparse(ifs[0])
    assert "next_round_at = float('inf')" in body and "_journal_safe" in body
