"""A benchmark with failed queries is not a score (live AML run 2026-09-24:
1 of 8 queries passed, QpH 166 printed, run exited 0)."""

from __future__ import annotations

from lakebench.benchmark.queries import BenchmarkQuery
from lakebench.benchmark.runner import QueryResult
from lakebench.cli._run import _benchmark_gate_problems
from tests.conftest import make_config


def _q(name, ok):
    return QueryResult(
        query=BenchmarkQuery(name=name, display_name=name, query_class="scan", sql="select 1"),
        elapsed_seconds=1.0,
        rows_returned=1,
        success=ok,
    )


def test_all_pass_is_clean():
    assert _benchmark_gate_problems(make_config(), [_q("Q1", True), _q("Q2", True)]) == []


def test_any_failure_fails_the_run():
    probs = _benchmark_gate_problems(make_config(), [_q("FQ1", False), _q("FQ5", True)])
    assert probs and "1 of 2" in probs[0] and "FQ1" in probs[0]


def test_documented_delta_thrift_q2_is_tolerated():
    cfg = make_config(recipe="hive-delta-spark-thrift")
    assert _benchmark_gate_problems(cfg, [_q("Q2_filtered_aggregation", False)]) == []
    # The same failure on Iceberg is a real defect.
    cfg2 = make_config(recipe="hive-iceberg-spark-thrift")
    assert _benchmark_gate_problems(cfg2, [_q("Q2_filtered_aggregation", False)])
