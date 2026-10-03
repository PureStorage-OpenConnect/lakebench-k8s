"""AML-3 attribution: experiment.attribution and limits.headroom_pct are
derived from the gold-finalize job's per-rule times, its stage profile and
the TM summary, and from the run's per-job timeout."""

from __future__ import annotations

from types import SimpleNamespace

from lakebench.metrics.attribution import attribution, headroom_pct
from lakebench.metrics.collector import JobMetrics, PipelineMetrics

STAGES = [
    {
        "stage": 184,
        "attempt": 0,
        "status": "COMPLETE",
        "tasks": 88,
        "wall_s": 480.0,
        "exec_s": 36000.0,
        "shuffle_read_mb": 9000.0,
        "max_task_s": 470.0,
        "stages": 12,
        "truncated": False,
        "complete": True,
        "lossy": False,
        "name": "count at NativeMethodAccessorImpl.java:0",
    },
    {"stage": 180, "exec_s": 4000.0, "complete": True, "truncated": False, "lossy": False},
]


def _gold(**kw):
    base = {
        "job_name": "lakebench-gold-finalize",
        "job_type": "gold-finalize",
        "elapsed_seconds": 6800.0,
        "rule_elapsed_s": {"W2_structuring": 300.0, "W5_sanctions_match": 4100.0, "W6": 900.0},
        "stage_profile": {"W5_sanctions_match": STAGES, "W2_structuring": []},
        "tm_ops": {"elapsed_seconds": 730.0},
    }
    base.update(kw)
    return JobMetrics(**base)


def test_dominant_rule_stage_and_shares():
    m = SimpleNamespace(jobs=[JobMetrics(job_name="s", job_type="silver-build"), _gold()])
    a = attribution(m)
    assert a["job"] == "gold-finalize"
    assert a["dominant_rule"] == "W5_sanctions_match"
    assert a["rule_elapsed_s"] == 4100.0
    assert a["share_of_job"] == round(4100 / 6800, 4)
    assert a["tm_share"] == round(730 / 6800, 4)
    st = a["dominant_stage"]
    assert (st["stage"], st["tasks"], st["exec_s"], st["max_task_s"]) == (184, 88, 36000.0, 470.0)
    assert st["share_of_rule_exec"] == round(36000 / 40000, 4)
    assert st["complete"] is True and a["profile"] == "read"


def test_missing_profile_says_why():
    m = SimpleNamespace(
        jobs=[
            _gold(
                stage_profile={},
                stage_profile_unavailable={"W5_sanctions_match": "Py4JError: no store"},
            )
        ]
    )
    a = attribution(m)
    assert a["dominant_stage"] is None
    assert a["profile"] == "unavailable: Py4JError: no store"
    m = SimpleNamespace(jobs=[_gold(rule_elapsed_s={"W2_structuring": 5.0})])
    assert attribution(m)["profile"] == "no_stage"
    m = SimpleNamespace(jobs=[_gold(rule_elapsed_s={"W9": 5.0})])
    assert attribution(m)["profile"] == "missing"


def test_no_gold_rule_times_no_attribution():
    assert attribution(SimpleNamespace(jobs=[])) is None
    c360 = JobMetrics(job_name="g", job_type="gold-finalize", elapsed_seconds=10.0)
    assert attribution(SimpleNamespace(jobs=[c360])) is None


def test_headroom_per_stage_and_benchmark():
    jobs = [
        JobMetrics(job_name="b", job_type="bronze-verify", elapsed_seconds=4278.0, success=True),
        JobMetrics(job_name="g", job_type="gold-finalize", elapsed_seconds=6800.0, success=True),
        JobMetrics(job_name="g2", job_type="gold-finalize", elapsed_seconds=9000.0, success=True),
        JobMetrics(job_name="s", job_type="silver-build", elapsed_seconds=200.0, success=False),
    ]
    queries = [
        {"query_name": "FQ1", "elapsed_seconds": 120.0, "success": True},
        # Median 450 s, slowest sample 450 s; another query's median hides
        # a 630 s sample.
        {"query_name": "FQ5", "elapsed_seconds": 450.0, "success": True},
        {
            "query_name": "FQ6",
            "elapsed_seconds": 300.0,
            "samples": [300.0, 630.0, 200.0],
            "success": True,
        },
    ]
    m = SimpleNamespace(
        jobs=jobs,
        job_timeout_seconds=12900,
        benchmark_query_timeout_seconds=900,
        benchmark=SimpleNamespace(total_seconds=570.0, queries=queries),
    )
    assert headroom_pct(m) == {
        # The benchmark is bounded per query, not by the per-job timeout.
        "benchmark_query": 30.0,
        "bronze-verify": round(100 * (1 - 4278 / 12900), 1),
        # The slower of two gold-finalize runs.
        "gold-finalize": round(100 * (1 - 9000 / 12900), 1),
        # A failed job has no headroom.
        "silver-build": None,
    }
    queries.append({"query_name": "FQ8", "elapsed_seconds": 900.0, "success": False})
    assert headroom_pct(m)["benchmark_query"] is None  # a timed-out query
    m.job_timeout_seconds = None
    assert headroom_pct(m) is None


def test_job_timeout_round_trips_through_the_record(tmp_path):
    from datetime import datetime

    from lakebench.metrics.storage import MetricsStorage

    run = PipelineMetrics(run_id="r1", deployment_name="d", start_time=datetime(2026, 10, 2))
    run.job_timeout_seconds = 12900
    run.benchmark_query_timeout_seconds = 900
    assert run.to_dict()["job_timeout_seconds"] == 12900
    storage = MetricsStorage(tmp_path)
    storage.save_run(run)
    back = storage.load_run("r1")
    assert (back.job_timeout_seconds, back.benchmark_query_timeout_seconds) == (12900, 900)
