"""A default continuous run runs table maintenance inside its window.

lb16-cs (2026-09-27): retention_interval defaulted to 1800 s, equal to the
default run_duration, so the first round never came due and every Iceberg
recipe recorded effective maintenance "not run" on a defaults-only run
(runs 20260927-073818-7934eb, 20260927-073533-9de9c9). DESIGN 5: continuous
mode includes periodic maintenance. Unset intervals now derive from the
window, and an explicit interval that cannot fire is refused at start.
"""

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
        assert (s["retention_interval"], s["compaction_interval"]) == (600, 1200)
        assert s["problem"] is None and s["warnings"] == []
        maint, comp = _simulate_continuous_loop(
            1800, s["retention_interval"], s["compaction_interval"], bench_s
        )
        assert maint >= 1 and comp >= 1, (bench_s, maint, comp)


def test_auto_interval_follows_the_window():
    for window, expected in [(900, 300), (1800, 600), (3600, 1200), (86400, 7200)]:
        assert _schedule(window)["retention_interval"] == expected
        assert _schedule(window)["retention_source"] == "auto: run_duration / 3"


def test_explicit_interval_that_fires_is_kept():
    s = _schedule(1800, retention_interval=450, compaction_interval=900)
    assert s["problem"] is None
    assert (s["retention_interval"], s["compaction_interval"]) == (450, 900)
    assert s["retention_source"] == s["compaction_source"] == "set in config"


def test_config_default_is_auto():
    c = make_config().architecture.pipeline.sustained
    assert c.retention_interval is None
    assert c.effective_retention_interval() == 600
    assert c.effective_compaction_interval() == 1200


def test_snapshot_records_the_effective_intervals():
    from lakebench.metrics.collector import build_config_snapshot

    cfg = make_config(architecture={"pipeline": {"mode": "continuous"}})
    snap = build_config_snapshot(cfg, run_mode="continuous")
    assert snap["maintenance"]["retention_interval"] == 600
    assert snap["maintenance"]["compaction_interval"] == 1200
    settings = snap["experiment_inputs"]["maintenance_config"]["settings"]
    assert (settings["retention_interval"], settings["compaction_interval"]) == (600, 1200)


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


def test_batch_snapshot_does_not_carry_continuous_intervals():
    """Review finding: a batch fingerprint moved with sustained.run_duration."""
    from lakebench.metrics.collector import build_config_snapshot

    snap = build_config_snapshot(make_config())
    assert snap["maintenance"]["retention_interval"] is None
    assert snap["maintenance"]["compaction_interval"] is None
