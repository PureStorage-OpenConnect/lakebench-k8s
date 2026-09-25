"""The maintenance value is only reported when compaction changed files, and
both compared rounds get the same warm-up (LB-141).

Four live runs on 2026-09-24 with identical file counts before and after
compaction read -12.4%, -8.9%, +31.2% and +46.8%: the difference between two
single passes, not a maintenance effect.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakebench.benchmark.queries import BenchmarkQuery
from lakebench.benchmark.runner import QueryResult
from lakebench.cli._run import _data_file_total, _maintenance_value, _warm_benchmark


def _qr(name, ok, secs, samples=None):
    return QueryResult(
        query=BenchmarkQuery(name=name, display_name=name, query_class="scan", sql="select 1"),
        elapsed_seconds=secs,
        rows_returned=1,
        success=ok,
        samples=list(samples) if samples is not None else [],
    )


def _qr3(name, ok, secs, jitter=0.05):
    """Three samples within +-jitter of secs, median secs."""
    return _qr(name, ok, secs, [secs * (1 - jitter), secs, secs * (1 + jitter)])


# The c360 scale-10 run (run-20260924-125512-9f7711): 1,201 -> 1,201 files.
_PRE = [_qr(f"Q{i}", True, t) for i, t in enumerate([16.9, 8.8, 7.3, 8.5, 5.1, 5.0, 8.9, 2.1])]
_POST = [_qr(f"Q{i}", True, t) for i, t in enumerate([13.0, 11.1, 9.3, 8.5, 7.1, 7.7, 12.1, 2.7])]


def test_unchanged_file_count_is_not_measured():
    value, n, reason = _maintenance_value(_PRE, _POST, 1201, 1201, 45.0)
    assert value is None and n == 0
    assert "changed no files" in reason and "1,201 -> 1,201" in reason


def test_file_reduction_is_measured_over_paired_queries():
    pre = [_qr3("Q1", True, 10.0), _qr3("Q2", True, 10.0), _qr("Q3", False, 300.0)]
    post = [_qr3("Q1", True, 5.0), _qr3("Q2", True, 5.0), _qr3("Q3", True, 1.0)]
    value, n, reason = _maintenance_value(pre, post, 66, 61, 180.0)
    assert n == 2 and reason == ""
    assert round(value, 1) == 100.0


def test_single_sample_rounds_are_not_reported():
    """One sample per query has no spread to judge the difference against (LB-150)."""
    pre = [_qr("Q1", True, 10.0), _qr("Q2", True, 10.0)]
    post = [_qr("Q1", True, 5.0), _qr("Q2", True, 5.0)]
    value, n, reason = _maintenance_value(pre, post, 66, 61, 180.0)
    assert value is None and n == 2
    assert "one sample per query" in reason and "+100.0%" in reason


def test_difference_inside_the_spread_is_within_noise():
    # The live AML s10 shape: the post round reads slower, but repeats of
    # each query range more widely than the difference between the rounds.
    pre = [_qr("Q1", True, 60.0, [50.0, 60.0, 90.0]), _qr("Q5", True, 20.0, [18.0, 20.0, 40.0])]
    post = [_qr("Q1", True, 70.0, [55.0, 70.0, 85.0]), _qr("Q5", True, 25.0, [19.0, 25.0, 38.0])]
    value, n, reason = _maintenance_value(pre, post, 900, 300, 120.0)
    assert value is None and n == 2
    assert reason.startswith("within noise:") and "-" in reason


def test_difference_beyond_the_spread_is_reported():
    pre = [_qr3("Q1", True, 60.0), _qr3("Q5", True, 20.0)]
    post = [_qr3("Q1", True, 83.0), _qr3("Q5", True, 37.0)]
    value, n, reason = _maintenance_value(pre, post, 900, 300, 120.0)
    assert reason == "" and n == 2
    assert value == pytest.approx((80 / 120 - 1) * 100)


def test_noise_uses_only_paired_queries():
    # Q9 is wildly noisy but failed in the post round, so it is not paired
    # and must not widen the band.
    pre = [_qr3("Q1", True, 10.0), _qr("Q9", True, 10.0, [1.0, 10.0, 100.0])]
    post = [_qr3("Q1", True, 5.0), _qr("Q9", False, 900.0)]
    value, n, reason = _maintenance_value(pre, post, 66, 61, 180.0)
    assert reason == "" and n == 1 and round(value, 1) == 100.0


def test_other_unmeasurable_cases():
    assert _maintenance_value(_PRE, _POST, 100, 50, 0.0)[2] == "maintenance did not run"
    assert "unavailable" in _maintenance_value(_PRE, _POST, 0, 50, 10.0)[2]
    assert "unavailable" in _maintenance_value(_PRE, _POST, 100, 0, 10.0)[2]
    pre = [_qr("Q1", False, 1.0)]
    assert "no query" in _maintenance_value(pre, _POST, 100, 50, 10.0)[2]


def test_warm_pass_runs_once_and_swallows_failure():
    runner = MagicMock()
    _warm_benchmark(runner, 300)
    runner.run_power.assert_called_once_with(cache="hot", query_timeout=300, iterations=1)
    runner.run_power.side_effect = RuntimeError("trino down")
    _warm_benchmark(runner, 300)  # must not raise


def test_failed_probe_makes_the_file_count_unknown():
    assert _data_file_total({"silver_data_file_count": 1200, "gold_data_file_count": 1}) == 1201
    assert _data_file_total({"silver_data_file_count": 1200, "gold_data_file_count": -1}) == 0
    assert _data_file_total({"silver_snapshot_count": 3}) == 0
    assert _data_file_total({}) == 0
