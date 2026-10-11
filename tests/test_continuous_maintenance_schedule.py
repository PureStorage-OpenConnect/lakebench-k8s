"""A default continuous run runs table maintenance and compaction inside its window;
an explicit interval that cannot fire is refused at start."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakebench.cli import _sustained as sus
from lakebench.cli._sustained import resolve_maintenance_schedule
from lakebench.config.schema import SustainedConfig
from tests.conftest import make_config


def _simulate_continuous_loop(run, ri, ci, bench_s, warmup=300, bench_interval=300):
    """(maintenance rounds, compaction rounds) the _run_sustained loop fires.

    Worst case: every maintenance and compaction round uses its whole budget
    plus the statement grace,
    every maintenance round ends on a timeout (so compaction is held one
    statement timeout), and each in-stream benchmark round takes bench_s and
    wins the loop pass (it `continue`s). Mirrors the order in _run_sustained.
    """
    from lakebench.cli._sustained import _BUDGET_GRACE_SECONDS, continuous_round_bounds

    t, next_round, next_maint, next_comp = 0.0, float(warmup), float(ri), float(ci)
    last_round = hold = 0.0
    maint = comp = 0
    while t < run:
        elapsed = t
        if bench_s and elapsed >= next_round and run - elapsed >= max(60, last_round * 1.2):
            t += bench_s
            last_round = bench_s
            next_round = t + bench_interval
            continue
        if elapsed >= next_maint:
            b = continuous_round_bounds(ri, run - t)
            if b:
                # The last statement may overrun the budget by the grace.
                t += b[1] + _BUDGET_GRACE_SECONDS
                maint += 1
                hold = t + b[0]
            next_maint = t + ri
        if elapsed >= next_comp:
            if t < hold:
                next_comp = hold
            else:
                b = continuous_round_bounds(ci, run - t)
                if b:
                    t += b[1] + _BUDGET_GRACE_SECONDS
                    comp += 1
                next_comp = t + ci
        wake = min(elapsed + 30, next_round if bench_s else run, next_maint, next_comp, run)
        t = max(t + 1.0, wake)  # real time moves on even when nothing sleeps
    return maint, comp


def _schedule(run_duration=1800, skip=False, **kw):
    return resolve_maintenance_schedule(SustainedConfig(**kw), run_duration, skip_maintenance=skip)


def test_default_run_fires_maintenance_and_compaction():
    for bench_s in range(0, 301, 30):
        s = _schedule()
        assert s["problem"] is None and s["warnings"] == []
        maint, comp = _simulate_continuous_loop(
            1800, s["retention_interval"], s["compaction_interval"], bench_s
        )
        assert maint >= 1 and comp >= 1, (bench_s, maint, comp)


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
