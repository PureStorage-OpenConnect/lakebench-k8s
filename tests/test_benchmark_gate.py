"""A benchmark with failed queries is not a score."""

from __future__ import annotations

from lakebench.benchmark.queries import BenchmarkQuery
from lakebench.benchmark.runner import QueryResult
from lakebench.cli._run import _benchmark_gate_problems
from tests.conftest import make_config


def _q(name, ok, secs=1.0, timed_out=False):
    return QueryResult(
        query=BenchmarkQuery(name=name, display_name=name, query_class="scan", sql="select 1"),
        elapsed_seconds=secs,
        rows_returned=1,
        success=ok,
        timed_out=timed_out,
    )


def _gate_cases():
    # (label, rounds, passed_ever_passed, passed_ever, expected_name_or_None)
    # expected is a query name that must appear in probs[0], or None for clean.
    return [
        # every query passed: clean
        ("all-pass", [_q("Q1", True), _q("Q2", True)], False, None, None),
        # any failed query fails the run
        ("any-failure", [_q("FQ1", False), _q("FQ5", True)], False, None, "FQ1"),
        # in-stream rounds store QueryResult.to_dict(); the gate reads the dicts
        (
            "in-stream-dicts",
            [{"name": "FQ3", "success": False}, {"name": "FQ1", "success": True}],
            False,
            None,
            "FQ3",
        ),
        # 1.7.11: a query that passed earlier and now only times out is a
        # measured SLA crossing, not a failure, when passed_ever names it
        (
            "timeout-after-prior-pass",
            [_q("FQ1", False, secs=300.0, timed_out=True), _q("FQ5", True)],
            True,
            {"FQ1", "FQ5"},
            None,
        ),
        # a query that NEVER passed measured nothing: a timeout still fails
        (
            "timeout-without-prior-pass",
            [_q("FQ1", False, secs=300.0, timed_out=True)],
            True,
            {"FQ5"},
            "FQ1",
        ),
        # only timeouts are demoted: a non-timeout error after a prior pass
        # still fails (the 8d3d383d hole the demotion must not reopen)
        (
            "non-timeout-error-after-prior-pass",
            [_q("FQ1", False, timed_out=False)],
            True,
            {"FQ1"},
            "FQ1",
        ),
        # batch mode passes passed_ever=None: no earlier round, timeout fails
        (
            "batch-mode-timeout",
            [_q("FQ1", False, secs=900.0, timed_out=True)],
            True,
            None,
            "FQ1",
        ),
    ]


def test_benchmark_gate_problems():
    for label, rounds, has_passed_ever, passed_ever, expected in _gate_cases():
        if has_passed_ever:
            probs = _benchmark_gate_problems(make_config(), rounds, passed_ever=passed_ever)
        else:
            probs = _benchmark_gate_problems(make_config(), rounds)
        if expected is None:
            assert probs == [], (label, probs)
        else:
            assert probs and expected in probs[0], (label, probs)


def test_paired_qph():
    from lakebench.cli._run import _paired_qph

    def r(name, secs, ok=True):
        q = _q(name, ok)
        q.elapsed_seconds = secs
        return q

    # queries that failed in either run are excluded from both sides
    pre = [r("A", 10.0), r("B", 10.0), r("C", 180.0, ok=False)]
    post = [r("A", 5.0), r("B", 5.0), r("C", 200.0)]
    pre_q, post_q, n = _paired_qph(pre, post)
    assert n == 2 and post_q == 2 * pre_q  # C is excluded from both

    # None paths: no shared passed query, a zero-time pair, a failure
    assert _paired_qph([_q("Q1", True, 2.0)], [_q("Q2", True, 2.0)]) is None
    assert _paired_qph([_q("Q1", True, 0.0)], [_q("Q1", True, 0.0)]) is None
    assert _paired_qph([_q("Q1", False, 2.0)], [_q("Q1", True, 2.0)]) is None


def test_q9_retry_keeps_failed_queries_out_of_qph():
    """After a successful Q9 retry the round's QpH follows the power-run rule:
    a query that timed out in the same round is not counted as answered, and
    its timeout seconds stay out of the denominator."""
    from types import SimpleNamespace

    from lakebench.cli._sustained import _replace_q9_result

    queries = [_q("Q1", True, 10.0), _q("Q2", False, 300.0, timed_out=True), _q("Q9", False, 5.0)]
    bench_result = SimpleNamespace(queries=queries, total_seconds=315.0, qph=0.0)

    _replace_q9_result(bench_result, _q("Q9", True, 20.0))

    assert bench_result.queries[2].success
    assert bench_result.total_seconds == 330.0
    # Two answered queries over their own 30 s, not three over 330 s.
    assert bench_result.qph == 2 / 30.0 * 3600
