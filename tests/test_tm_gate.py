"""P10 TM operations lines travel from the gold driver log to the gate and
the scorecard: parse -> JobMetrics -> metrics.json -> reload -> render."""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from lakebench.metrics import JobMetrics, MetricsCollector, MetricsStorage, PipelineMetrics
from lakebench.metrics.tm_ops import parse_tm_invariants, parse_tm_ops, tm_gate_problems

OPS = {
    "as_of_date": "2025-01-01",
    "cycle": 1,
    "simulation": {"analyst_accuracy": 0.9, "investigator_accuracy": 0.95, "seed": 7},
    "reconciliation": {"completeness.source": 31, "exclusion.no_customer_party": 5},
    "funnel": {
        "payments": 31,
        "monitored": 25,
        "alerts": 4,
        "escalated": 1,
        "cases": 1,
        "sars": 1,
    },
    "alert_aging_open": {"0-30": 1, "31-60": 0, "61-90": 0, "90+": 2},
    "open_cases": 0,
    "open_cases_over_60_days": 0,
    "scenarios": {"W3_round_tripping": {"alerts": 1, "escalated": 1, "productive_rate": 1.0}},
    "sars_filed": 1,
    "filed_over_30_days_pct": 0.0,
}

LOGS = (
    "[lb] 2026-09-24T00:00:00 - [detection] W2_structuring: alerts=3 elapsed=1.0s\n"
    "[lb] 2026-09-24T00:00:01 - [tm-invariant] reconciliation: status=pass cycle=1 "
    "detail=monitored 25 + excluded 6 = 31 vs source 31\n"
    "[lb] 2026-09-24T00:00:01 - [tm-invariant] sars_le_cases: status=fail cycle=1 "
    "detail=SARs 5 <= cases 4\n"
    f"[lb] 2026-09-24T00:00:02 - [tm-ops] {json.dumps(OPS)}\n"
)


def test_parse_invariants_and_ops():
    inv = parse_tm_invariants(LOGS)
    assert inv == {
        1: {
            "reconciliation": {
                "status": "pass",
                "detail": "monitored 25 + excluded 6 = 31 vs source 31",
            },
            "sars_le_cases": {"status": "fail", "detail": "SARs 5 <= cases 4"},
        }
    }
    assert parse_tm_ops(LOGS)["funnel"]["cases"] == 1
    assert parse_tm_ops("no lines") is None


def test_gate_names_the_failed_invariant_and_cycle():
    assert tm_gate_problems(parse_tm_invariants(LOGS), label="x") == [
        "x: cycle 1: workflow invariant sars_le_cases fail: SARs 5 <= cases 4"
    ]
    assert tm_gate_problems({}) and "did not run" in tm_gate_problems({})[0]


def test_continuous_ticks_are_all_gated():
    logs = (
        "[tm-invariant] reconciliation: status=pass cycle=1 detail=a\n"
        "[tm-invariant] reconciliation: status=fail cycle=2 detail=b\n"
        "[tm-invariant] reconciliation: status=pass cycle=3 detail=c\n"
    )
    assert [p.split(":")[0] for p in tm_gate_problems(parse_tm_invariants(logs))] == ["cycle 2"]


def test_driver_log_to_metrics_json_to_scorecard(tmp_path):
    from lakebench.cli._run import _apply_parsed_job_metrics
    from lakebench.reports.scorecard import FinancialScorecardBlock

    parsed = MetricsCollector().parse_driver_logs(LOGS, "gold-finalize")
    job = JobMetrics(job_name="lakebench-gold-finalize", job_type="gold-finalize", success=True)
    _apply_parsed_job_metrics(job, parsed)
    assert job.tm_invariants["1"]["sars_le_cases"]["status"] == "fail"
    assert job.tm_ops["funnel"]["sars"] == 1

    storage = MetricsStorage(tmp_path / "metrics")
    storage.save_run(
        PipelineMetrics(run_id="tm-1", deployment_name="t", start_time=datetime.now(), jobs=[job])
    )
    loaded = storage.load_run("tm-1").jobs[0]
    assert loaded.tm_invariants == job.tm_invariants
    assert loaded.tm_ops == job.tm_ops

    html = FinancialScorecardBlock().render_detail_html(
        SimpleNamespace(jobs=[loaded], financial_scoring=None, config_snapshot={})
    )
    assert "Transaction Monitoring Operations" in html
    assert "simulated from the datagen ground truth" in html
    assert "sars_le_cases" in html and "SARs 5 &lt;= cases 4" in html
    assert "Productive rate" in html and "precision" not in html.lower().split("detection")[0]
    assert "Queue health" in html and "Cycle funnel" in html


def test_scorecard_without_tm_lines_is_unchanged():
    from lakebench.reports.scorecard import _render_tm_operations

    assert _render_tm_operations([SimpleNamespace(tm_ops=None, tm_invariants={})]) == ""


def test_spark_jobs_get_tm_env_and_the_module():
    from lakebench.spark.job import JobType, SparkJobManager
    from tests.conftest import make_config

    cfg = make_config(
        architecture={
            "workload": {
                "schema": "financial",
                "datagen": {"scale": 1},
                "tm_operations": {"analyst_accuracy": 0.8, "seed": 5},
            }
        }
    )
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    manifest = SparkJobManager(cfg, k8s)._build_manifest(JobType.GOLD_FINALIZE)
    env = {e["name"]: e.get("value") for e in manifest["spec"]["driver"]["env"]}
    assert env["LB_TM_ANALYST_ACCURACY"] == "0.8"
    assert env["LB_TM_SEED"] == "5"
    assert env["LB_FINANCIAL_GOLD_CASES"] == "gold.cases"
    exec_env = {e["name"] for e in manifest["spec"]["executor"]["env"]}
    assert "LB_TM_SEED" in exec_env

    import inspect

    from lakebench.modules.pipeline_engines.spark.job import SparkJobManager as M

    assert '"tm_operations.py"' in inspect.getsource(M.deploy_scripts_configmap)
