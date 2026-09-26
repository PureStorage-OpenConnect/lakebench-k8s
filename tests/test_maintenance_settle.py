"""The post-maintenance round waits for storage to settle (LB-150).

Live c360 scale 10 on FlashBlade: the same compacted files read QpH 546 about
2 minutes after maintenance, 569 at +15 min and 841 at +35 min, against 828
before maintenance. The probes here replay that shape with a fake clock.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from lakebench.benchmark.queries import BenchmarkQuery
from lakebench.benchmark.runner import QueryResult
from lakebench.benchmark.settle import (
    MAX_CONSECUTIVE_PROBE_FAILURES,
    SettleResult,
    wait_for_settle,
)
from lakebench.cli._run import _maintenance_value, _settle_after_maintenance
from lakebench.config.schema import BenchmarkConfig, MaintenanceSettleConfig


class _Clock:
    """Fake monotonic clock; sleep and probes advance it."""

    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        assert s >= 0
        self.t += s


def _prober(clock: _Clock, times):
    """Probe returning *times* in order; None raises. Each probe takes its time."""
    it = iter(times)

    def _probe(remaining: float) -> float:
        assert remaining >= 0
        t = next(it)
        if t is None:
            clock.t += 5.0
            raise RuntimeError("Query exceeded maximum time limit")
        clock.t += t
        return t

    return _probe


def _wait(clock, times, **kw):
    args = {
        "probe_query": "Q1",
        "started_at": clock(),
        "max_seconds": 2700,
        "interval_seconds": 60,
        "tolerance_pct": 10.0,
        "clock": clock,
        "sleep": clock.sleep,
    }
    args.update(kw)
    return wait_for_settle(_prober(clock, times), **args)


def test_decay_to_plateau_settles_on_first_stable_pair():
    clock = _Clock()
    r = _wait(clock, [30.0, 20.0, 14.0, 12.0, 11.5, 11.4, 99.0], reference_seconds=11.0)
    assert r.settled and not r.capped and r.reason == ""
    # 12.0 then 11.5 is the first pair within 10%.
    assert [p.seconds for p in r.probes] == [30.0, 20.0, 14.0, 12.0, 11.5]
    assert [round(p.offset_seconds) for p in r.probes] == [0, 60, 120, 180, 240]
    assert r.settle_seconds == pytest.approx(240 + 11.5)
    assert r.value_reason() == ""


def test_slow_plateau_is_not_accepted_when_pre_time_is_known():
    """+2 and +15 min agreed within 4% while 33% slow; consecutive agreement
    alone would have settled there."""
    clock = _Clock()
    slow = [15.2] * 20  # 546 QpH-like plateau
    fast = [10.0, 9.9]  # back to the 828 QpH-like pre time
    r = _wait(clock, slow + fast, reference_seconds=10.0, max_seconds=2700)
    assert r.settled
    assert len(r.probes) == 22
    assert r.probes[-1].seconds == 9.9
    assert r.settle_seconds > 20 * 60


def test_without_reference_three_agreeing_probes_settle_unverified():
    clock = _Clock()
    r = _wait(clock, [15.2, 15.0, 14.9], reference_seconds=None)
    assert r.settled and len(r.probes) == 3
    assert r.verified is False and r.to_dict()["verified"] is False
    clock = _Clock()
    assert _wait(clock, [10.0, 10.1], reference_seconds=10.0).verified is True


def test_stable_but_slower_than_pre_is_named_not_blamed_on_storage():
    clock = _Clock()
    r = _wait(clock, [14.0] * 100, reference_seconds=10.0, max_seconds=600)
    assert r.capped and not r.settled
    assert "slower than the pre-maintenance 10.0 s" in r.reason
    assert "not separable" in r.value_reason()


def test_probe_is_told_the_time_left_before_the_cap():
    clock = _Clock()
    seen = []

    def _probe(remaining):
        seen.append(remaining)
        clock.t += 10.0
        return 10.0 + len(seen) * 5  # never stable

    wait_for_settle(
        _probe,
        probe_query="Q1",
        started_at=clock(),
        max_seconds=300,
        interval_seconds=60,
        tolerance_pct=10.0,
        clock=clock,
        sleep=clock.sleep,
    )
    assert seen[0] == 300 and seen[1] == 240 and seen == sorted(seen, reverse=True)


def test_never_settling_hits_the_cap():
    clock = _Clock()
    times = [10.0, 20.0] * 100
    r = _wait(clock, times, max_seconds=600)
    assert not r.settled and r.capped
    assert r.value_reason() == "storage did not settle within 600 s"
    # No probe starts past the cap.
    assert all(p.offset_seconds <= 600 for p in r.probes)
    assert r.settle_seconds <= 600 + 20


def test_zero_cap_takes_one_probe_and_reports_capped():
    clock = _Clock()
    r = _wait(clock, [10.0, 10.0], max_seconds=0)
    assert r.capped and not r.settled and len(r.probes) == 1


def test_failing_probe_ends_the_wait_early():
    clock = _Clock()
    r = _wait(clock, [None] * 10)
    assert not r.settled and not r.capped
    assert len(r.probes) == MAX_CONSECUTIVE_PROBE_FAILURES
    assert "failed 3 times in a row" in r.reason
    assert r.value_reason().startswith("settle wait ended without settling")
    assert r.settle_seconds < 300


def test_one_failed_probe_breaks_the_pair_but_not_the_wait():
    clock = _Clock()
    r = _wait(clock, [12.0, None, 12.1, 12.0], reference_seconds=12.0)
    assert r.settled
    assert [p.seconds for p in r.probes] == [12.0, None, 12.1, 12.0]
    assert r.probes[1].error.startswith("Query exceeded")


def test_slow_probe_does_not_sleep_negative():
    # A probe longer than the interval starts the next one immediately.
    clock = _Clock()
    r = _wait(clock, [90.0, 80.0, 79.0], interval_seconds=60, reference_seconds=80.0)
    assert r.settled
    assert r.probes[1].offset_seconds == pytest.approx(90.0)


def test_to_dict_records_probes():
    clock = _Clock()
    r = _wait(clock, [12.0, None, 12.1, 12.0], reference_seconds=11.5)
    d = r.to_dict()
    assert d["probe_query"] == "Q1" and d["settled"] is True and d["capped"] is False
    assert d["reference_seconds"] == 11.5
    assert [p["seconds"] for p in d["probes"]] == [12.0, None, 12.1, 12.0]
    assert "error" in d["probes"][1]
    json.dumps(d)


# -- maintenance value --------------------------------------------------------


def _qr(name, ok, secs, jitter=0.02):
    return QueryResult(
        query=BenchmarkQuery(name=name, display_name=name, query_class="scan", sql="select 1"),
        elapsed_seconds=secs,
        rows_returned=1,
        success=ok,
        samples=[secs * (1 - jitter), secs, secs * (1 + jitter)],
    )


def _settle(settled, capped=False, reason=""):
    return SettleResult(
        probe_query="Q1",
        settled=settled,
        settle_seconds=2700.0 if capped else 300.0,
        capped=capped,
        max_seconds=2700,
        tolerance_pct=10.0,
        reference_seconds=10.0,
        reason=reason,
    )


_PRE = [_qr("Q1", True, 10.0), _qr("Q2", True, 10.0)]
_POST = [_qr("Q1", True, 5.0), _qr("Q2", True, 5.0)]


def test_capped_settle_nulls_the_maintenance_value():
    value, n, reason = _maintenance_value(_PRE, _POST, 66, 61, 180.0, _settle(False, capped=True))
    assert value is None and n == 2
    assert reason == "storage did not settle within 2700 s"


def test_settled_wait_keeps_the_maintenance_value():
    value, n, reason = _maintenance_value(_PRE, _POST, 66, 61, 180.0, _settle(True))
    assert round(value, 1) == 100.0 and reason == ""


def test_disabled_wait_keeps_old_behaviour():
    value, _n, reason = _maintenance_value(_PRE, _POST, 66, 61, 180.0, None)
    assert round(value, 1) == 100.0 and reason == ""


# -- the run-side helper, with a fake runner ------------------------------------


class _FakeRunner:
    def __init__(self, clock, times, queries):
        self.clock = clock
        self.times = iter(times)
        self.queries = queries
        self.calls = []

    def probe_query(self, name=None):
        from lakebench.benchmark.runner import BenchmarkRunner

        return BenchmarkRunner.probe_query(self, name)

    def _queries(self):
        return self.queries

    def time_query(self, query, iterations=1, query_timeout=300):
        self.calls.append((query.name, iterations, query_timeout))
        t = next(self.times)
        if t is None:
            self.clock.t += 1.0
            return QueryResult(query, 300.0, 0, False, "timeout", [300.0])
        self.clock.t += t
        return QueryResult(query, t, 1, True, "", [t])


_QUERIES = [
    BenchmarkQuery(name="Q9", display_name="Q9", query_class="operational", sql="select 1"),
    BenchmarkQuery(name="Q1", display_name="Q1", query_class="scan", sql="select 1"),
]


def _cfg(**settle):
    return SimpleNamespace(
        architecture=SimpleNamespace(
            benchmark=SimpleNamespace(maintenance_settle=MaintenanceSettleConfig(**settle))
        )
    )


def test_helper_probes_scan_query_against_pre_time():
    clock = _Clock()
    runner = _FakeRunner(clock, [16.0, 15.8, 10.4, 10.2], _QUERIES)
    pre = [_qr("Q9", True, 3.0), _qr("Q1", True, 10.0)]
    r = _settle_after_maintenance(
        _cfg(probe_samples=2), runner, pre, 900, clock(), clock=clock, sleep=clock.sleep
    )
    assert r.settled and r.probe_query == "Q1" and r.reference_seconds == 10.0
    assert len(r.probes) == 4  # 16.0/15.8 agree but are 58% slower than pre
    assert runner.calls[0] == ("Q1", 2, 900)


def test_helper_failed_probe_query_raises_into_wait():
    clock = _Clock()
    runner = _FakeRunner(clock, [None, None, None], _QUERIES)
    r = _settle_after_maintenance(
        _cfg(), runner, None, 300, clock(), clock=clock, sleep=clock.sleep
    )
    assert not r.settled and not r.capped and "timeout" in r.reason


def test_helper_disabled_or_unknown_query_returns_none():
    clock = _Clock()
    runner = _FakeRunner(clock, [], _QUERIES)
    assert _settle_after_maintenance(_cfg(enabled=False), runner, None, 300, clock()) is None
    with pytest.raises(ValueError):
        _settle_after_maintenance(_cfg(probe_query="nope"), runner, None, 300, clock())
    assert runner.calls == []


def test_config_defaults():
    sc = BenchmarkConfig().maintenance_settle
    assert sc.enabled and sc.max_seconds == 2700 and sc.interval_seconds == 60
    assert sc.tolerance_pct == 10.0 and sc.probe_query is None and sc.probe_samples == 1
    with pytest.raises(ValueError):
        MaintenanceSettleConfig(tolerance_pct=0)
    with pytest.raises(ValueError):
        MaintenanceSettleConfig(max_seconds=0)
    with pytest.raises(ValueError):
        MaintenanceSettleConfig(bogus=1)


# -- recording: metrics.json, scorecard, report; TTV untouched ------------------


def _pb_with_settle(settle: SettleResult | None):
    from lakebench.metrics import BenchmarkMetrics, JobMetrics, PipelineMetrics
    from lakebench.metrics.collector import build_pipeline_benchmark

    t0 = datetime(2026, 9, 25, 10, 0, 0)
    pm = PipelineMetrics(
        run_id="20260925-100000-abcdef", deployment_name="x", start_time=t0, success=True
    )
    pm.jobs.append(
        JobMetrics(
            job_name="lakebench-silver-build",
            job_type="silver-build",
            start_time=t0,
            end_time=t0 + timedelta(seconds=100),
            elapsed_seconds=100.0,
            success=True,
        )
    )
    pm.benchmark = BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1,
        qph=1800.0,
        total_seconds=2.0,
        queries=[{"name": "Q1", "elapsed_seconds": 2.0, "success": True, "samples": [2, 2, 2]}],
        iterations=3,
    )
    # The run's wall clock includes a 45-minute wait; no stage does.
    pm.end_time = t0 + timedelta(hours=1)
    pb = build_pipeline_benchmark(pm)
    pb.maintenance_elapsed_seconds = 120.0
    pb.pre_compaction_qph = 1700.0
    pb.post_compaction_qph = 1800.0
    if settle is not None:
        pb.maintenance_settle_seconds = settle.settle_seconds
        pb.maintenance_settled = settle.settled
        pb.maintenance_settle_capped = settle.capped
        pb.maintenance_settle = settle.to_dict()
        pre = [_qr("Q1", True, 10.0)]
        post = [_qr("Q1", True, 5.0)]
        value, n, reason = _maintenance_value(pre, post, 66, 61, 120.0, settle)
        pb.maintenance_value_pct = value
        pb.maintenance_value_reason = reason
    pm.pipeline_benchmark = pb
    return pm, pb


def test_settle_is_recorded_and_ttv_is_unchanged(tmp_path):
    from lakebench.metrics.storage import MetricsStorage

    clock = _Clock()
    capped = _wait(clock, [10.0, 20.0] * 100, max_seconds=2700)
    pm_without, pb_without = _pb_with_settle(None)
    pm, pb = _pb_with_settle(capped)
    assert pb.time_to_value_seconds == pb_without.time_to_value_seconds == 100.0
    assert pb.total_elapsed_seconds == pb_without.total_elapsed_seconds

    path = MetricsStorage(tmp_path).save_run(pm)
    raw = json.loads(path.read_text())
    scores = raw["pipeline_benchmark"]["scores"]
    assert scores["maintenance_settle_capped"] is True
    assert scores["maintenance_settled"] is False
    assert scores["maintenance_value_pct"] is None
    assert scores["maintenance_value_reason"] == "storage did not settle within 2700 s"
    assert scores["time_to_value_seconds"] == 100.0
    detail = raw["pipeline_benchmark"]["maintenance_settle"]
    assert detail["capped"] and len(detail["probes"]) > 2

    loaded = MetricsStorage(tmp_path).load_run(pm.run_id).pipeline_benchmark
    assert loaded.maintenance_settle_capped is True
    assert loaded.maintenance_settled is False
    assert loaded.maintenance_settle_seconds == pytest.approx(capped.settle_seconds, abs=0.1)
    assert loaded.maintenance_settle["probe_query"] == "Q1"

    # Without a settle wait the scorecard carries no settle keys.
    raw_without = pb_without.to_dict()["scorecard"]
    assert "maintenance_settle_seconds" not in raw_without


def test_perf_gate_numbers_ignore_the_settle_wait():
    from lakebench.cli._reproduce import _extract_expected_numbers

    clock = _Clock()
    pm_without, _ = _pb_with_settle(None)
    pm, _ = _pb_with_settle(_wait(clock, [12.0, 12.0]))
    assert _extract_expected_numbers(pm) == _extract_expected_numbers(pm_without)


def test_report_shows_settle_rows():
    from lakebench.reports.generator import ReportGenerator

    clock = _Clock()
    pm, _ = _pb_with_settle(_wait(clock, [12.0, None, 12.1, 12.0], reference_seconds=12.0))
    html = ReportGenerator(metrics_dir="/tmp/unused-rg")._generate_maintenance_section(pm)
    assert "Storage settle wait" in html and "settled after" in html
    assert "12.0s, fail, 12.1s, 12.0s" in html

    clock = _Clock()
    pm, _ = _pb_with_settle(_wait(clock, [10.0, 20.0] * 100, max_seconds=600))
    html = ReportGenerator(metrics_dir="/tmp/unused-rg")._generate_maintenance_section(pm)
    assert "did not settle within 600s" in html


# -- pre-benchmark maintenance stopped early (review of 79db73c) ----------------


def test_stopped_maintenance_nulls_the_maintenance_value():
    value, n, reason = _maintenance_value(
        _PRE, _POST, 66, 61, 180.0, _settle(True), stopped_reason="OPTIMIZE gold.t timed out"
    )
    assert value is None
    assert "maintenance stopped before completion" in reason


def test_stopped_maintenance_is_persisted_and_flagged_in_the_report(tmp_path):
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.generator import ReportGenerator

    pm, pb = _pb_with_settle(None)
    pb.maintenance_stopped = True
    pb.maintenance_stop_reason = "OPTIMIZE lakehouse.gold.t timed out after 1800s"

    path = MetricsStorage(tmp_path).save_run(pm)
    scores = json.loads(path.read_text())["pipeline_benchmark"]["scores"]
    assert scores["maintenance_stopped"] is True
    assert scores["maintenance_stop_reason"].startswith("OPTIMIZE")

    loaded = MetricsStorage(tmp_path).load_run(pm.run_id)
    assert loaded.pipeline_benchmark.maintenance_stopped is True
    html = ReportGenerator(metrics_dir="/tmp/unused-rg")._generate_maintenance_section(loaded)
    assert "maintenance stopped before completion" in html
    assert "not a clean measurement" in html


def test_completed_maintenance_is_not_flagged(tmp_path):
    from lakebench.reports.generator import ReportGenerator

    pm, pb = _pb_with_settle(None)
    assert "maintenance_stopped" not in pb.to_dict()["scorecard"]
    html = ReportGenerator(metrics_dir="/tmp/unused-rg")._generate_maintenance_section(pm)
    assert "stopped before completion" not in html


def test_run_passes_the_stop_reason_and_records_it():
    import inspect

    import lakebench.cli._run as run_mod

    src = inspect.getsource(run_mod)
    assert "stopped_reason=maint_stop_reason" in src
    assert "pb.maintenance_stopped = True" in src
