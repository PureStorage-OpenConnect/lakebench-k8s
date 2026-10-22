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


def test_all_pass_is_clean():
    assert _benchmark_gate_problems(make_config(), [_q("Q1", True), _q("Q2", True)]) == []


def test_any_failure_fails_the_run():
    probs = _benchmark_gate_problems(make_config(), [_q("FQ1", False), _q("FQ5", True)])
    assert probs and "FQ1" in probs[0]


def test_in_stream_round_dicts_are_gated():
    """In-stream rounds store QueryResult.to_dict(); the gate must read them."""
    rnd = [{"name": "FQ3", "success": False}, {"name": "FQ1", "success": True}]
    probs = _benchmark_gate_problems(make_config(), rnd)
    assert probs and "FQ3" in probs[0]


# -- 1.7.11: a time-limit crossing is a measurement, not a failure -------------


def test_timeout_after_a_prior_pass_is_a_crossing_not_a_failure():
    """A query that passed in an earlier round and now only exceeds its time
    limit is a measured SLA crossing (the corpus grew past what the engine
    answers inside the limit). With passed_ever naming it, the gate does not
    fail the run."""
    rnd = [_q("FQ1", False, secs=300.0, timed_out=True), _q("FQ5", True)]
    probs = _benchmark_gate_problems(make_config(), rnd, passed_ever={"FQ1", "FQ5"})
    assert probs == []


def test_timeout_without_a_prior_pass_still_fails():
    """A query that NEVER passed measured nothing, even if it timed out: it is
    not a crossing, so it still fails."""
    rnd = [_q("FQ1", False, secs=300.0, timed_out=True)]
    probs = _benchmark_gate_problems(make_config(), rnd, passed_ever={"FQ5"})
    assert probs and "FQ1" in probs[0]


def test_non_timeout_error_still_fails_even_after_a_prior_pass():
    """Only timeouts are demoted. A query that passed earlier but now errors
    for another reason (a real defect, a hung engine reported as an ordinary
    error) still fails -- this is the 8d3d383d hole the demotion must not
    reopen."""
    rnd = [_q("FQ1", False, timed_out=False)]
    probs = _benchmark_gate_problems(make_config(), rnd, passed_ever={"FQ1"})
    assert probs and "FQ1" in probs[0]


def test_batch_mode_timeout_still_fails():
    """Batch mode passes passed_ever=None: no earlier round exists, so a
    timeout fails as before."""
    rnd = [_q("FQ1", False, secs=900.0, timed_out=True)]
    probs = _benchmark_gate_problems(make_config(), rnd, passed_ever=None)
    assert probs and "FQ1" in probs[0]


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
