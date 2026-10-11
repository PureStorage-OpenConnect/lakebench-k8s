"""c360 expected results: parsing, generator moments, row-count shapes, wiring.

The checks' behaviour on real (Spark-computed) facts is executed in
tests/spark/test_c360_kpi_correctness_spark.py. This file covers the pure
Python around it, and that the verdict is reporting only (D6).
"""

from __future__ import annotations

import json
from datetime import date

import pytest

from lakebench.metrics import c360_correctness as c3
from lakebench.metrics.collector import JobMetrics, MetricsCollector, PipelineMetrics


def test_the_benchmark_pass_judges_only_the_shapes(monkeypatch):
    """The pipeline pass leaves the benchmark shapes to the pass after the
    benchmark ran; that pass does not repeat a missing-facts report."""
    monkeypatch.setattr(c3, "GATING_CHECKS", frozenset({"x", "benchmark_rows_Q1"}))
    shapes = ("benchmark_rows_",)
    shp = c3.verdict([c3._check("benchmark_rows_Q1", "shape", False, 2, 1)])
    shp["facts_present"] = True
    assert len(c3.gating_problems(shp, only=shapes)) == 1
    ok = c3.verdict([c3._check("benchmark_rows_Q1", "shape", True, 1, 1)])
    ok["facts_present"] = True
    assert c3.gating_problems(ok, only=shapes) == []
    absent = dict(c3.verdict([]), facts_present=True)
    assert c3.gating_problems(absent, only=shapes)
    assert c3.gating_problems({"facts_present": False}, only=shapes) == []


def _gated_record(**status):
    checks = [
        c3._check(cid, "invariant", status.get(cid, True), 1, 0)
        for cid in sorted(c3.GATING_CHECKS) + ["interaction_mix", "benchmark_rows_Q2"]
        if status.get(cid, True) != "drop"
    ]
    return dict(c3.verdict(checks), facts_present=True)


def _checks_record(*checks):
    return dict(c3.verdict(list(checks)), facts_present=True)


_X_GATED = frozenset({"x", "benchmark_rows_Q1"})
_NO_FACTS = {"facts_present": False, "reason": "boom", "checks": []}


@pytest.mark.parametrize(
    ("rec", "gating", "fails"),
    [
        (_gated_record(), None, False),
        (_gated_record(interaction_mix=False), None, False),
        (_gated_record(benchmark_rows_Q2=False), None, False),
        (_gated_record(silver_to_gold_days=False), None, True),
        (_gated_record(dates_in_window=None), None, True),
        (_gated_record(gold_daily_identities="drop"), None, True),
        (_NO_FACTS, None, True),
        ({"status": "pass", "checks": []}, None, True),
        # a custom gating set: a failed and an unchecked gated check fail; a
        # passing one does not
        (_checks_record(c3._check("x", "invariant", False, 1, 0)), _X_GATED, True),
        (_checks_record(c3._check("x", "invariant", None, None, 0)), _X_GATED, True),
        (
            _checks_record(
                c3._check("x", "invariant", True, 0, 0),
                c3._check("benchmark_rows_Q1", "shape", True, 1, 1),
            ),
            _X_GATED,
            False,
        ),
        (_checks_record(c3._check("x", "invariant", False, 1, 0)), frozenset(), False),
        (_NO_FACTS, frozenset(), False),
        (None, frozenset(), False),
        (None, None, True),
    ],
    ids=[
        "pass",
        "non-gating-fail",
        "non-gating-shape-fail",
        "gated-fail",
        "gated-unchecked",
        "gated-dropped",
        "no-facts",
        "no-facts-key",
        "set-failed",
        "set-unchecked",
        "set-all-pass",
        "empty-set-fail",
        "empty-set-no-facts",
        "empty-set-no-record",
        "no-record",
    ],
)
def test_verdict_gate_and_cli_gate_agree(monkeypatch, rec, gating, fails):
    """One rule: gating_outcome (the verdict) fails exactly when the CLI's
    two gating_problems passes report a problem, except that a missing record
    is the verdict's to scope out (no check was made)."""
    if gating is not None:
        monkeypatch.setattr(c3, "GATING_CHECKS", gating)
    cli = c3.gating_problems(rec) + c3.gating_problems(rec, only=("benchmark_rows_",))
    outcome, reason = c3.gating_outcome(rec)
    assert bool(cli) is fails
    if rec is None:
        # No record: the CLI fails closed, the verdict has nothing to judge.
        assert (outcome, reason) == (None, None)
        return
    assert (outcome == "FAIL") is fails
    assert (reason is not None) is fails


_FAILING = {"silver_to_gold_days": False}


def _continuous(**kw):
    return dict(_gated_record(**_FAILING), reporting_only=True, **kw)


@pytest.mark.parametrize(
    ("rec", "empty_set", "fails"),
    [
        (None, False, False),
        (_gated_record(**_FAILING), True, False),
        (c3.unevaluated_record("boom"), True, False),
        (_continuous(mode="continuous"), False, False),
        # reporting_only counts only on a continuous record
        (_continuous(), False, True),
        (c3.unevaluated_record("boom"), False, True),
    ],
    ids=[
        "no-record",
        "empty-set",
        "empty-set-no-facts",
        "continuous-reporting-only",
        "reporting-only-not-continuous",
        "no-facts",
    ],
)
def test_gating_outcome_and_judged_ids_scope_out_what_cannot_gate(
    monkeypatch, rec, empty_set, fails
):
    if empty_set:
        monkeypatch.setattr(c3, "GATING_CHECKS", frozenset())
    judged = c3.judged_gating_ids(rec)
    assert (c3.gating_outcome(rec)[0] == "FAIL") is fails
    # The ids it judges are all of them, or none when nothing can gate.
    assert judged == (c3.GATING_CHECKS if fails else set())
    if empty_set:
        assert c3.gating_problems(rec) == []


def _drops_fail(rec, gid):
    edited = dict(rec, checks=[c for c in rec["checks"] if c.get("id") != gid])
    return c3.gating_outcome(edited)[0] == "FAIL"


@pytest.mark.parametrize("shape_gated", [False, True])
def test_judged_gating_ids_is_the_set_gating_outcome_judges(monkeypatch, shape_gated):
    """judged_gating_ids is public for the report's chip (ER-21): dropping a
    GATING_CHECKS id from a passing record fails gating_outcome exactly when
    the id is in judged_gating_ids, with and without a gated benchmark
    shape, and with and without shape checks in the record."""
    if shape_gated:
        monkeypatch.setattr(c3, "GATING_CHECKS", c3.GATING_CHECKS | {"benchmark_rows_Q2"})
    # A second shape keeps the record "benchmark ran" when Q2 is dropped.
    with_shapes = _gated_record()
    with_shapes["checks"].append(c3._check("benchmark_rows_Q1", "shape", True, 1, 0))
    no_shapes = dict(
        with_shapes,
        checks=[c for c in with_shapes["checks"] if not c["id"].startswith("benchmark_rows_")],
    )
    for rec in (with_shapes, no_shapes):
        assert c3.gating_outcome(rec) == (None, None)
        judged = c3.judged_gating_ids(rec)
        assert judged <= c3.GATING_CHECKS
        for gid in c3.GATING_CHECKS:
            assert _drops_fail(rec, gid) is (gid in judged), gid
    assert ("benchmark_rows_Q2" in c3.judged_gating_ids(with_shapes)) is shape_gated
    assert "benchmark_rows_Q2" not in c3.judged_gating_ids(no_shapes)


def test_reporting_failures_lists_only_checks_outside_the_list():
    rec = _gated_record(interaction_mix=False, silver_to_gold_days=False, dates_in_window=None)
    assert c3.reporting_failures(rec) == ["interaction_mix"]
    assert c3.reporting_failures(None) == []


def test_repeated_gated_id_passes_only_when_every_entry_passes():
    rec = _gated_record()
    rec["checks"].insert(0, c3._check("bronze_to_silver_rows", "reconcile", False, 1, 0))
    assert c3.gating_outcome(rec)[0] == "FAIL"
    assert c3.gating_problems(rec)


def test_facts_present_must_be_true():
    rec = dict(_gated_record(), facts_present="false")
    assert c3.gating_outcome(rec)[0] == "FAIL"
    assert c3.gating_problems(rec)


def test_unevaluated_record_fails_closed_in_both_gates():
    rec = c3.unevaluated_record("the check could not run: KeyError('window_start')")
    assert rec["status"] == "unknown" and rec["facts_present"] is False
    assert c3.gating_outcome(rec)[0] == "FAIL"
    assert c3.gating_problems(rec)


def test_pass_requires_every_core_check():
    passing = [c3._check(cid, "reconcile", True, 0, 0) for cid in c3.CORE_CHECKS]
    assert c3.verdict(passing)["status"] == "pass"
    v = c3.verdict(passing[1:])
    assert v["status"] == "unknown" and c3.CORE_CHECKS[0] in v["reason"]
    # Benchmark shapes that need no facts cannot make a fact-less run pass.
    rec = c3.evaluate_run([], [], {})
    out = c3.add_benchmark_checks(rec, [_q("Q1_full_aggregation_scan", 1)])
    assert out["status"] == "unknown" and out["facts_present"] is False


def test_datagen_rows_match_generate_rs():
    # Scale 1: 10 GB -> --target-tb 0.009766 -> 160 files of 64 MiB; snappy
    # 4332 bytes a row -> 15,491 rows a file.
    assert c3.datagen_rows(10.0, 64, 4332.0) == 160 * 15_491
    # Tiny targets still write one file of at least 1,000 rows.
    assert c3.datagen_rows(0.001, 64, 4332.0) == 15_491
    assert c3.datagen_rows(0.001, 1, 4332.0) == 1_000


def test_stage_time_excludes_the_check():
    from datetime import datetime, timedelta

    from lakebench.cli._run import _exclude_c360_check_time

    end = datetime(2026, 1, 1, 12, 0, 0)
    jm = JobMetrics(job_name="g", job_type="gold-finalize", end_time=end, elapsed_seconds=100.0)
    jm.c360_check = {"check_seconds": 12.5}
    assert _exclude_c360_check_time(jm) == 12.5
    assert jm.elapsed_seconds == 87.5 and jm.end_time == end - timedelta(seconds=12.5)
    # Nothing to take off, or an implausible value: unchanged.
    for facts in (None, {"check_seconds": 0}, {"check_seconds": 500}, {"check_seconds": "x"}):
        jm2 = JobMetrics(job_name="g", job_type="gold-finalize", end_time=end, elapsed_seconds=100)
        jm2.c360_check = facts
        assert _exclude_c360_check_time(jm2) == 0.0 and jm2.elapsed_seconds == 100


def test_evaluate_run_uses_the_last_gold_job_only():
    early = JobMetrics(job_name="g", job_type="gold-finalize")
    early.c360_check = {"version": 1, "silver": {}, "gold": {}}
    last = JobMetrics(job_name="g", job_type="gold-finalize")
    v = c3.evaluate_run(
        [early, last], [], {"window_start": "2024-01-01", "window_end": "2025-01-01"}
    )
    assert v["facts_present"] is False and v["status"] == "unknown"


def test_transaction_value_moments_match_generator():
    mean, sd = c3.transaction_value_moments()
    # exp(4.3 + 1.2^2 / 2) = 151.4 before the clamps; the 9999.99 cap trims it.
    assert mean == pytest.approx(151.34, abs=0.05)
    assert sd == pytest.approx(267.6, abs=0.5)


def test_expected_distinct_customers_limits():
    assert c3.expected_distinct_customers(100_000, 0) == pytest.approx(0)
    # Saturates at the id space with very many sessions.
    assert c3.expected_distinct_customers(1_000, 1e7) == pytest.approx(1_000, rel=1e-6)
    # Tiny id space (all hot): still bounded by the ids.
    assert c3.expected_distinct_customers(100, 1e6) == pytest.approx(100, rel=1e-6)


def test_parse_lines_last_wins():
    logs = "\n".join(
        [
            "[lb] 2026-01-01T00:00:00 - [c360-bronze] rows=100 silver_filter_rows=98",
            '[lb] 2026-01-01T00:00:01 - [c360-check] {"version":1,"silver":{"rows":1}}',
            '[lb] 2026-01-01T00:00:02 - [c360-check] {"version":1,"silver":{"rows":2}}',
        ]
    )
    assert c3.parse_c360_bronze(logs) == {"rows": 100, "silver_filter_rows": 98}
    assert c3.parse_c360_check(logs)["silver"]["rows"] == 2
    assert c3.parse_c360_check("nothing") is None
    assert c3.parse_c360_bronze(None) is None


def test_collector_parses_and_storage_round_trips(tmp_path):
    from lakebench.metrics.storage import MetricsStorage

    facts = {"version": 1, "silver": {"rows": 5}, "gold": {"rows": 2}}
    logs = (
        "[c360-bronze] rows=7 silver_filter_rows=5\n"
        f"[lb] x - [c360-check] {json.dumps(facts)}\n"
        "=== JOB METRICS: gold-finalize ===\nelapsed_seconds: 3.0\n" + "=" * 40
    )
    jm = MetricsCollector().parse_driver_logs(logs, "gold-finalize")
    assert jm.c360_check == facts
    assert jm.c360_bronze == {"rows": 7, "silver_filter_rows": 5}

    from datetime import datetime

    run = PipelineMetrics(run_id="r1", deployment_name="d", start_time=datetime.now())
    run.jobs.append(jm)
    run.c360_correctness = {"status": "pass", "gating": False, "checks": []}
    store = MetricsStorage(tmp_path)
    store.save_run(run)
    back = store.load_run("r1")
    assert back.c360_correctness == run.c360_correctness
    assert back.jobs[0].c360_check == facts
    assert back.jobs[0].c360_bronze == {"rows": 7, "silver_filter_rows": 5}


def _facts(days, rows_per_day=2_000, tx_per_day=360, support_per_day=240):
    ds = [date(2024, 1, 1).fromordinal(date(2024, 1, 1).toordinal() + i) for i in range(days)]
    return {
        "silver": {
            "rows": rows_per_day * days,
            "transaction_rows": tx_per_day * days,
            "support_rows": support_per_day * days,
        },
        "gold": {
            "rows": days,
            "days": [[d.isoformat(), tx_per_day, 150.0, 1000, 10.5, 1830, 240, 3.0] for d in ds],
        },
    }


def _q(name, rows, ok=True, revenue=None):
    q = {"name": name, "rows_returned": rows, "success": ok}
    if revenue is not None:
        q["result_fingerprint"] = {"approx": {"3": revenue}}
    return q


def test_benchmark_row_counts_on_a_dense_year():
    facts = _facts(366)
    facts["gold"]["sums"] = {"total_daily_revenue": 54_900.0}
    queries = [
        _q("Q1_full_aggregation_scan", 1, revenue=54_900.0),
        _q("Q2_filtered_aggregation", 91 * 5),  # 2024-01-01 .. 2024-03-31
        _q("Q4_churn_risk_analysis", 6),
        _q("Q3_customer_segmentation", 12),
        _q("Q7_channel_conversion_funnel", 5),
        _q("Q5_revenue_trend_ma7", 90),
        _q("Q6_customer_rfm", 4),
        _q("Q9_executive_dashboard", 30),
    ]
    checks = c3.benchmark_checks(queries, facts, {})
    assert {c["status"] for c in checks} == {"pass"}, checks


@pytest.mark.parametrize(
    ("revenue", "status"),
    [(54_900.004, "pass"), (55_900.0, "fail"), (None, "unchecked")],
)
def test_q1_revenue_answer_matches_gold(revenue, status):
    """Q1's total_revenue is the benchmark's answer for the quantity gold
    sums as total_daily_revenue: they agree within rounding, or the check
    fails (reporting only); without Q1's fingerprint it is unchecked."""
    facts = _facts(366)
    facts["gold"]["sums"] = {"total_daily_revenue": 54_900.0}
    checks = {
        c["id"]: c
        for c in c3.benchmark_checks(
            [_q("Q1_full_aggregation_scan", 1, revenue=revenue)], facts, {}
        )
    }
    assert checks["benchmark_answer_Q1_revenue"]["status"] == status
    assert "benchmark_answer_Q1_revenue" not in c3.GATING_CHECKS


def test_benchmark_row_counts_catch_a_wrong_shape_and_skip_failed_queries():
    facts = _facts(366)
    checks = {
        c["id"]: c
        for c in c3.benchmark_checks(
            [
                _q("Q7_channel_conversion_funnel", 4),
                _q("Q9_executive_dashboard", 0, ok=False),
                _q("Q2_filtered_aggregation", 400),
            ],
            facts,
            {},
        )
    }
    assert checks["benchmark_rows_Q7"]["status"] == "fail"
    assert checks["benchmark_rows_Q9"]["status"] == "unchecked"
    assert checks["benchmark_rows_Q2"]["status"] == "fail"


def test_benchmark_shapes_not_required_on_a_thin_corpus():
    facts = _facts(10, rows_per_day=100, tx_per_day=18, support_per_day=12)
    checks = {
        c["id"]: c["status"]
        for c in c3.benchmark_checks(
            [_q("Q3_customer_segmentation", 9), _q("Q2_filtered_aggregation", 40)], facts, {}
        )
    }
    assert checks == {"benchmark_rows_Q3": "unchecked", "benchmark_rows_Q2": "unchecked"}


def test_add_benchmark_checks_keeps_context():
    rec = c3.verdict([c3._check("a", "invariant", True, 0, 0)])
    rec.update(context={"x": 1}, facts=_facts(40), bronze=None, facts_present=True)
    out = c3.add_benchmark_checks(rec, [_q("Q1_full_aggregation_scan", 2)])
    assert out["status"] == "fail" and out["failed"] == ["benchmark_rows_Q1"]
    assert out["context"] == {"x": 1} and out["gating"] is c3.GATING


def test_c360_bronze_path(monkeypatch, load_script):
    c360_bronze_path = load_script("common").c360_bronze_path
    base = "s3a://b/customer/interactions/"
    monkeypatch.delenv("LB_BRONZE_CYCLE", raising=False)
    assert c360_bronze_path("s3a://b/", appending=True) == base
    monkeypatch.setenv("LB_BRONZE_CYCLE", "2")
    assert c360_bronze_path("s3a://b/", appending=True) == base + "part-c002-*.parquet"
    assert c360_bronze_path("s3a://b/", appending=False) == base
    # Cycle 0 of a multi-cycle run never reads another run's cycle files.
    monkeypatch.setenv("LB_BRONZE_CYCLE", "0")
    assert c360_bronze_path("s3a://b/", appending=False) == base + "part-[0-9]*.parquet"


def test_c360_bronze_run_path(monkeypatch, load_script):
    c360_bronze_run_path = load_script("common").c360_bronze_run_path
    base = "s3a://b/customer/interactions/"
    monkeypatch.delenv("LB_BRONZE_CYCLE", raising=False)
    assert c360_bronze_run_path("s3a://b/") == base
    monkeypatch.setenv("LB_BRONZE_CYCLE", "0")
    assert c360_bronze_run_path("s3a://b/") == base + "part-[0-9]*.parquet"
    monkeypatch.setenv("LB_BRONZE_CYCLE", "2")
    assert c360_bronze_run_path("s3a://b/") == (
        base + "{part-[0-9]*.parquet,part-c001-*.parquet,part-c002-*.parquet}"
    )
