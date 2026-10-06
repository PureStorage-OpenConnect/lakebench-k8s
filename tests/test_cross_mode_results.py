"""Batch and continuous results are never compared (LB-264).

Continuous AML writes silver.counterparty_edges as one row per pair per
micro-batch (batch writes one per pair) and silver.account_statements
running balances in arrival order. FQ4 and IQ3 no longer read either layout
raw (tests/test_aml_queries_mode_invariant.py and the Spark tier pin that),
but a batch/continuous pair is still never read as comparable: the mode is
a workload identity key, so compare stops at step 3 (one workload on one
corpus) before any result is read, and the perf gate refuses the pair on
its identity.
"""

from __future__ import annotations

import copy

from lakebench.metrics import experiment as ex
from tests.fixtures import stored_records as sr

#: An AML batch record whose results include FQ4 and IQ3.
AML_BATCH = "de1772"


def _pair(*, mode_b: str, change: tuple[str, ...] = ("FQ4_running_balance_window",)):
    a = sr.load_record(AML_BATCH)
    b = copy.deepcopy(a)
    b["run_id"] = a["run_id"][:-6] + "bbbbbb"
    b["experiment"]["mode"] = mode_b
    fps = b["experiment"]["results"]["fingerprints"]
    for name in change:
        assert name in fps, name
        fps[name] = {**fps[name], "exact": "0" * 64}
    return a, b


def test_the_aml_record_fingerprints_the_mode_sensitive_queries():
    fps = sr.load_record(AML_BATCH)["experiment"]["results"]["fingerprints"]
    assert {"FQ4_running_balance_window", "IQ3_counterparty_two_hop"} <= set(fps)


def test_the_perf_gate_refuses_a_cross_mode_baseline():
    a, b = _pair(mode_b="sustained")
    refusals = ex.stored_identity_refusals(
        ex.identity(a["experiment"]),
        ex.result_fingerprints(a["experiment"]),
        b["experiment"],
        "baseline",
    )
    assert any("mode" in r for r in refusals), refusals
