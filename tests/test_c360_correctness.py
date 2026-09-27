"""c360 expected results: parsing, generator moments, row-count shapes, wiring.

The checks' behaviour on real (Spark-computed) facts is executed in
tests/spark/test_c360_kpi_correctness_spark.py. This file covers the pure
Python around it, and that the verdict is reporting only (D6).
"""

from __future__ import annotations

import ast
import json
import sys
from datetime import date
from pathlib import Path

import pytest

from lakebench.metrics import c360_correctness as c3
from lakebench.metrics.collector import JobMetrics, MetricsCollector, PipelineMetrics
from tests.conftest import make_config

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "src/lakebench/spark/scripts"


def test_reporting_only_until_owner_approves():
    assert c3.GATING is False
    v = c3.verdict([c3._check("x", "invariant", False, 1, 0)])
    assert v["status"] == "fail" and v["gating"] is False and "D6" in v["note"]


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


def test_add_months_matches_trino():
    assert c3._add_months(date(2024, 1, 1), 3) == date(2024, 4, 1)
    assert c3._add_months(date(2024, 11, 30), 3) == date(2025, 2, 28)
    assert c3._add_months(date(2023, 12, 31), 2) == date(2024, 2, 29)


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


def test_apply_parsed_job_metrics_carries_c360_facts():
    from lakebench.cli._run import _apply_parsed_job_metrics

    parsed = JobMetrics(job_name="j", job_type="gold-finalize")
    parsed.c360_check = {"version": 1}
    parsed.c360_bronze = {"rows": 1, "silver_filter_rows": 1}
    target = JobMetrics(job_name="j", job_type="gold-finalize")
    _apply_parsed_job_metrics(target, parsed)
    assert target.c360_check == {"version": 1}
    assert target.c360_bronze == {"rows": 1, "silver_filter_rows": 1}


def test_expected_context_defaults_follow_datagen():
    cfg = make_config()
    ctx = c3.expected_context(cfg)
    assert ctx["window_start"] == "2024-01-01"
    assert ctx["window_end"] == "2025-01-01"  # datagen_rs default (exclusive)
    # The id space datagen is given (deploy/datagen.py datagen_customer_id_max).
    assert ctx["customers"] == cfg.get_scale_dimensions().customers


def test_evaluate_run_without_facts_is_unknown():
    gold = JobMetrics(job_name="g", job_type="gold-finalize")
    v = c3.evaluate_run([gold], [], {"window_start": "2024-01-01", "window_end": "2025-01-01"})
    assert v["status"] == "unknown" and "no [c360-check]" in v["reason"]
    gold.c360_check = {"version": 1, "error": "boom"}
    v = c3.evaluate_run([gold], [], {})
    assert v["status"] == "unknown" and "boom" in v["reason"]


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


def _q(name, rows, ok=True):
    return {"name": name, "rows_returned": rows, "success": ok}


def test_benchmark_row_counts_on_a_dense_year():
    facts = _facts(366)
    queries = [
        _q("Q1_full_aggregation_scan", 1),
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
    rec.update(context={"x": 1}, facts=_facts(40), bronze=None)
    out = c3.add_benchmark_checks(rec, [_q("Q1_full_aggregation_scan", 2)])
    assert out["status"] == "fail" and out["failed"] == ["benchmark_rows_Q1"]
    assert out["context"] == {"x": 1} and out["gating"] is False


def test_run_wiring_never_touches_pipeline_success():
    """The c360 blocks in the run path only record and print (D6)."""
    src = (ROOT / "src/lakebench/cli/_run.py").read_text()
    for marker in ("_c360.evaluate_run(", "_c360.add_benchmark_checks("):
        i = src.index(marker)
        block = src[src.rfind("if (", 0, i) : src.index("except Exception", i)]
        assert "pipeline_success" not in block
        assert "raise" not in block


def test_both_gold_adapters_log_the_facts_and_share_the_kpis():
    for name in ("gold_finalize.py", "gold_finalize_delta.py"):
        src = (SCRIPTS / name).read_text()
        assert "log_c360_check(spark, silver_tbl, gold_tbl)" in src
        assert "get_daily_kpi_aggregations()" in src
        # No adapter-local KPI definitions that could drift.
        assert "avg_transaction_value" not in src


def test_kpi_averages_are_conditional():
    """Guard the four fixed KPIs against a revert to all-row averages."""
    src = (SCRIPTS / "common.py").read_text()
    tree = ast.parse(src)
    fn = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "get_daily_kpi_aggregations"
    )
    body = ast.get_source_segment(src, fn)
    assert 'avg("transaction_amount")' not in body
    assert 'avg("time_on_site_seconds")' not in body
    assert 'avg("page_views")' not in body
    assert 'avg("lifetime_value_estimate")' not in body
    assert 'countDistinct("support_ticket_id")' not in body
    assert 'count("support_ticket_id").alias("support_tickets_created")' in body


def test_c360_bronze_path(monkeypatch):
    sys.path.insert(0, str(SCRIPTS))
    try:
        from common import c360_bronze_path
    finally:
        sys.path.remove(str(SCRIPTS))
    base = "s3a://b/customer/interactions/"
    monkeypatch.delenv("LB_BRONZE_CYCLE", raising=False)
    assert c360_bronze_path("s3a://b/", appending=True) == base
    monkeypatch.setenv("LB_BRONZE_CYCLE", "2")
    assert c360_bronze_path("s3a://b/", appending=True) == base + "part-c002-*.parquet"
    assert c360_bronze_path("s3a://b/", appending=False) == base
    monkeypatch.setenv("LB_BRONZE_CYCLE", "0")
    assert c360_bronze_path("s3a://b/", appending=True) == base


def test_cycle_index_is_passed_to_every_multi_cycle_job():
    src = (ROOT / "src/lakebench/cli/_run.py").read_text()
    i = src.index('cycle_env: dict[str, str] = {"LB_RUN_ID"')
    window = src[i : i + 600]
    assert 'cycle_env["LB_BRONZE_CYCLE"] = str(cycle_idx)' in window
    # datagen_rs names cycle n's files part-c{n:03}-...; the index passed to
    # datagen as --cycle is the same cycle_idx.
    dg = (ROOT / "src/lakebench/deploy/datagen.py").read_text()
    assert 'context["datagen_cycle"] = cycle_index' in dg
    rs = (ROOT / "datagen_rs/src/cycle.rs").read_text()
    assert 'format!("part-c{cycle:03}-{fid:06}.parquet")' in rs
