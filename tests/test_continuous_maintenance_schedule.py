"""A default continuous run runs table maintenance and compaction inside its window;
an explicit interval that cannot fire is refused at start."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lakebench.cli import _sustained as sus
from lakebench.cli._sustained import resolve_maintenance_schedule
from lakebench.config.schema import SustainedConfig
from tests.conftest import make_config


def _schedule(run_duration=1800, skip=False, **kw):
    return resolve_maintenance_schedule(SustainedConfig(**kw), run_duration, skip_maintenance=skip)


@pytest.mark.parametrize("bench_s", [0, 150, 300])
def test_default_run_fires_maintenance_and_compaction(monkeypatch, tmp_path, bench_s):
    """The real continuous loop on a fake clock over a default-length window.

    Worst case: every maintenance and compaction round uses its whole budget
    plus the statement grace, every maintenance round ends on a timeout (so
    compaction is held one statement timeout), and each in-stream benchmark
    round takes *bench_s*.
    """
    from tests.test_continuous_window import _drive

    run = SustainedConfig().run_duration
    s = _schedule(run)
    assert s["problem"] is None and s["warnings"] == []
    intervals = {"expire": s["retention_interval"], "compaction": s["compaction_interval"]}
    start: list[float] = []
    fired: dict[str, list[tuple[float, float]]] = {"expire": [], "compaction": []}

    real_loop_sleep = sus.loop_sleep_seconds

    def sleep_seconds(now, *events):
        if not start:
            start.append(sus.time.time() - now)
        return real_loop_sleep(now, *events)

    def round_(kind, timed_out):
        def run_round(*a, timeout, budget, live_streams=False, **kw):
            if not live_streams:
                return None
            elapsed = sus.time.time() - start[0]
            bounds = sus.continuous_round_bounds(intervals[kind], run - elapsed)
            assert bounds is not None and (timeout, budget.seconds) == bounds, (kind, elapsed)
            sus.time.sleep(budget.seconds + sus._BUDGET_GRACE_SECONDS)
            if timed_out:
                budget.stopped = "a statement timed out"
            fired[kind].append((elapsed, sus.time.time() - start[0]))
            return None

        return run_round

    monkeypatch.setattr(sus, "loop_sleep_seconds", sleep_seconds)
    monkeypatch.setattr(sus, "_run_iceberg_maintenance", round_("expire", True))
    monkeypatch.setattr(sus, "_run_iceberg_compaction", round_("compaction", False))
    bench_rounds: list[float] = []

    def bench_round(**kw):
        bench_rounds.append(sus.time.time() - start[0])
        sus.time.sleep(bench_s)

    monkeypatch.setattr(sus, "_run_benchmark_round", bench_round)
    _drive(
        monkeypatch,
        tmp_path,
        lambda job, end: "",
        duration=run,
        runner=MagicMock() if bench_s else None,
    )

    assert bool(bench_rounds) == bool(bench_s)
    assert fired["expire"] and fired["compaction"], (bench_s, fired)
    for kind, rounds in fired.items():
        due = intervals[kind]
        for began, ended in rounds:
            assert due <= began < run, (kind, bench_s, rounds)
            due = ended + intervals[kind]
    if not bench_s:
        assert fired["expire"][0][0] == intervals["expire"]
        # The first maintenance round timed out: compaction waits one
        # statement timeout after it.
        hold = (
            fired["expire"][0][1]
            + sus.continuous_round_bounds(intervals["expire"], run - intervals["expire"])[0]
        )
        assert fired["compaction"][0][0] == max(intervals["compaction"], hold)


@pytest.mark.parametrize(
    ("window", "explicit", "skip", "retention", "compaction", "refused"),
    [
        (900, {}, False, 300, 600, False),
        (1800, {}, False, 600, 1200, False),
        (3600, {}, False, 1200, 2400, False),
        (86400, {}, False, 7200, 14400, False),
        (1800, {"retention_interval": 450, "compaction_interval": 900}, False, 450, 900, False),
        (1800, {"retention_interval": 1800}, False, 1800, 3600, True),
        (1800, {"retention_interval": 600, "compaction_interval": 1800}, False, 600, 1800, True),
        (1800, {"retention_interval": 1800}, True, 1800, 3600, False),
    ],
    ids=[
        "auto-900",
        "auto-1800",
        "auto-3600",
        "auto-86400",
        "explicit-fires",
        "retention-not-inside-window",
        "compaction-not-inside-window",
        "unfireable-with-skip-maintenance",
    ],
)
def test_interval_derives_from_the_window_and_unfireable_explicit_is_refused(
    window, explicit, skip, retention, compaction, refused
):
    s = _schedule(window, skip=skip, **explicit)
    assert (s["retention_interval"], s["compaction_interval"]) == (retention, compaction)
    assert (s["problem"] is not None) == refused


@pytest.mark.parametrize(("mode", "moves"), [("continuous", True), ("batch", False)])
def test_snapshot_maintenance_follows_the_window_only_in_continuous(mode, moves):
    from lakebench.metrics.collector import build_config_snapshot

    def snap(run_duration):
        cfg = make_config(
            architecture={"pipeline": {"mode": mode, "continuous": {"run_duration": run_duration}}}
        )
        s = build_config_snapshot(cfg, run_mode=mode)
        return s["maintenance"], s["experiment_inputs"]["maintenance_config"]

    assert (snap(1800) != snap(3600)) == moves


def test_run_start_writes_the_resolved_values_back(monkeypatch):
    cfg = make_config(architecture={"pipeline": {"mode": "continuous"}})
    monkeypatch.setattr(
        sus,
        "resolve_trickle",
        lambda cfg, d, **kw: {"problem": None, "value": 10, "arrival_seconds": None, "source": "t"},
    )

    class Stop(Exception):
        pass

    def stop(*a, **k):
        raise Stop

    monkeypatch.setattr(sus, "journal_open", stop)
    with pytest.raises(Stop):
        sus._run_sustained(cfg, Path("c.yaml"), 100, False, 3600)
    s = cfg.architecture.pipeline.sustained
    assert (s.retention_interval, s.compaction_interval) == (1200, 2400)
