"""A benchmark with failed queries is not a score."""

from __future__ import annotations

from lakebench.benchmark.queries import BenchmarkQuery
from lakebench.benchmark.runner import QueryResult
from lakebench.cli._run import _benchmark_gate_problems
from tests.conftest import make_config


def _q(name, ok, secs=1.0):
    return QueryResult(
        query=BenchmarkQuery(name=name, display_name=name, query_class="scan", sql="select 1"),
        elapsed_seconds=secs,
        rows_returned=1,
        success=ok,
    )


def test_all_pass_is_clean():
    assert _benchmark_gate_problems(make_config(), [_q("Q1", True), _q("Q2", True)]) == []


def test_any_failure_fails_the_run():
    probs = _benchmark_gate_problems(make_config(), [_q("FQ1", False), _q("FQ5", True)])
    assert probs and "FQ1" in probs[0]


def test_delta_thrift_q2_failure_fails_the_run():
    """The Q2/Q7 crash is worked around, so Delta + Thrift Q2 is no
    longer tolerated."""
    for recipe in ("hive-delta-spark-thrift", "hive-iceberg-spark-thrift"):
        cfg = make_config(recipe=recipe)
        for name in ("Q2_filtered_aggregation", "Q6_customer_rfm"):
            assert _benchmark_gate_problems(cfg, [_q(name, False)]), (recipe, name)


def test_in_stream_round_dicts_are_gated():
    """In-stream rounds store QueryResult.to_dict(); the gate must read them."""
    rnd = [{"name": "FQ3", "success": False}, {"name": "FQ1", "success": True}]
    probs = _benchmark_gate_problems(make_config(), rnd)
    assert probs and "FQ3" in probs[0]


def test_paired_qph_ignores_queries_that_failed_in_either_run():
    from lakebench.cli._run import _paired_qph

    def r(name, secs, ok=True):
        q = _q(name, ok)
        q.elapsed_seconds = secs
        return q

    pre = [r("A", 10.0), r("B", 10.0), r("C", 180.0, ok=False)]
    post = [r("A", 5.0), r("B", 5.0), r("C", 200.0)]
    pre_q, post_q, n = _paired_qph(pre, post)
    assert n == 2 and post_q == 2 * pre_q  # C is excluded from both


def test_paired_qph_none_paths():
    from lakebench.cli._run import _paired_qph

    assert _paired_qph([_q("Q1", True, 2.0)], [_q("Q2", True, 2.0)]) is None
    assert _paired_qph([_q("Q1", True, 0.0)], [_q("Q1", True, 0.0)]) is None
    assert _paired_qph([_q("Q1", False, 2.0)], [_q("Q1", True, 2.0)]) is None
