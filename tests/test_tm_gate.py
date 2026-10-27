"""P10 TM operations lines travel from the gold driver log to the gate and
the scorecard: parse -> JobMetrics -> metrics.json -> reload -> render."""

from __future__ import annotations

import json
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

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
    assert tm_gate_problems({}) == []  # not run is the verdict's call, not a problem


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
    assert "sars_le_cases" in html and "SARs 5 &lt;= cases 4" in html


def test_status_lines_parse_and_last_wins():
    from lakebench.metrics.tm_ops import parse_tm_status

    logs = (
        "[tm-status] status=waiting cycle=1 reason=no manifest yet\n"
        "[tm-status] status=not_run cycle=2 reason=error: boom\n"
        "[tm-status] status=ran cycle=2 reason=ok\n"
    )
    assert parse_tm_status(logs) == {
        1: {"status": "waiting", "reason": "no manifest yet"},
        2: {"status": "ran", "reason": "ok"},
    }


_WAITING_ALL_WINDOW = "".join(
    f"[tm-status] status=waiting cycle={c} reason=no manifest yet\n" for c in range(1, 9)
)
_WAITING_THEN_RAN = (
    "[tm-status] status=waiting cycle=1 reason=no manifest yet\n"
    "[tm-status] status=ran cycle=1 reason=ok\n"
    "[tm-invariant] reconciliation: status=pass cycle=1 detail=a\n"
)
_INTERRUPTED_MID_CYCLE_2 = (
    "[tm-status] status=started cycle=1 reason=writing the TM tables\n"
    "[tm-status] status=ran cycle=1 reason=ok\n"
    "[tm-invariant] reconciliation: status=pass cycle=1 detail=a\n"
    "[tm-status] status=started cycle=2 reason=writing the TM tables\n"
)


@pytest.mark.parametrize(
    ("logs", "status", "reason_contains"),
    [
        # the manifest never arrived in the window: not run, window ended
        (_WAITING_ALL_WINDOW, "not_run", "window ended"),
        # waiting then running in the same cycle passes
        (_WAITING_THEN_RAN, "pass", ""),
        # a cycle that started but never ran: unknown, naming the cycle
        (_INTERRUPTED_MID_CYCLE_2, "unknown", "[2]"),
    ],
    ids=["no-manifest", "waiting-then-ran", "interrupted-mid-cycle"],
)
def test_continuous_tm_verdict(logs, status, reason_contains):
    from lakebench.metrics.tm_ops import parse_tm_invariants, parse_tm_status, tm_verdict

    v = tm_verdict(parse_tm_invariants(logs), parse_tm_status(logs), continuous=True)
    assert v["status"] == status, logs
    assert not v["problems"], logs
    if reason_contains:
        assert reason_contains in v["reason"], logs


def test_continuous_section_renders_from_the_run_record():
    from lakebench.reports.scorecard import FinancialScorecardBlock

    verdict = {
        "status": "not_run",
        "reason": "the window ended before the layer could run (no manifest yet)",
        "mode": "continuous",
        "invariants": {"1": {"reconciliation": {"status": "pass", "detail": ""}}},
        "ops": OPS,
    }
    html = FinancialScorecardBlock().render_detail_html(
        SimpleNamespace(jobs=[], financial_scoring=None, config_snapshot={}, tm_operations=verdict)
    )
    gate = html.split("TM operations gate")[1].split("</p>")[0]
    assert "not run" in gate and "var(--success)" not in gate


def test_bad_tm_summary_does_not_blank_the_detection_table():
    from lakebench.reports.scorecard import FinancialScorecardBlock

    job = JobMetrics(job_name="g", job_type="gold-finalize", success=True)
    job.alerts_by_rule = {"W2_structuring": 3}
    job.tm_ops = {"scenarios": {"W2": 5}, "funnel": "garbage"}
    html = FinancialScorecardBlock().render_detail_html(
        SimpleNamespace(jobs=[job], financial_scoring=None, config_snapshot={}, tm_operations=None)
    )
    assert "W2_structuring" in html
    assert "could not be rendered" in html


def test_gold_refresh_gets_the_bronze_checkpoint():
    """The TM layer's in-flight check reads the bronze stream's source log;
    gold-refresh must see the same checkpoint bronze-ingest writes."""
    from lakebench.modules.pipeline_engines.spark.job import bronze_ingest_checkpoint_uri
    from lakebench.spark.job import JobType, SparkJobManager
    from tests.conftest import make_config

    cfg = make_config(architecture={"workload": {"schema": "financial", "datagen": {"scale": 1}}})
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    env = {
        e["name"]: e.get("value")
        for e in SparkJobManager(cfg, k8s)._build_manifest(JobType.GOLD_REFRESH)["spec"]["driver"][
            "env"
        ]
    }
    assert env["LB_FINANCIAL_BRONZE_CHECKPOINT"] == bronze_ingest_checkpoint_uri(cfg)
