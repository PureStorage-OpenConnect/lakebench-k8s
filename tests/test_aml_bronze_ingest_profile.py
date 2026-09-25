"""AML continuous bronze-ingest profile override.

run-20260925-104452-21bf3a (AML, continuous, scale 10): bronze-ingest on the
base 2 executors x 2 cores ran every 50-file micro-batch in 109.7 s under a
30 s trigger, 0.456 files/s, and left 42% of the 1,371-file corpus unread in
the 1800 s window. The override sizes bronze so the trickle ceiling
(50 files / 30 s), not bronze, bounds intake at scale 10.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lakebench.config import LakebenchConfig
from lakebench.k8s.client import ClusterCapacity
from lakebench.modules.pipeline_engines.spark.job import (
    _MAX_EXECUTORS_SAFE,
    _streaming_concurrent_budget,
    compute_peak_requirements,
    get_executor_count,
    get_job_profile,
)
from lakebench.spark.job import JobType, SparkJobManager

# Measured on the live run: 50 files per 109.7 s on 4 cores, one file per
# core per wave.
_WAVE_S = 109.7 / (50 / 4)
_BATCH_FILES = 50
_TRIGGER_S = 30
_CORPUS_FILES_S10 = 1371
_WINDOW_S = 1800


def _config(schema: str, scale: float) -> LakebenchConfig:
    return LakebenchConfig(
        name="t",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "a",
                    "secret_key": "b",
                    "buckets": {"bronze": "b", "silver": "s", "gold": "g"},
                }
            }
        },
        architecture={
            "workload": {"schema": schema, "datagen": {"scale": scale}},
            "pipeline": {"mode": "sustained"},
        },
    )


def test_financial_bronze_ingest_is_sized_to_drain_scale_10():
    import math

    p = get_job_profile("bronze-ingest", "financial")
    cores = get_executor_count("bronze-ingest", 10, "financial") * p["executor_cores"]
    batch_s = math.ceil(_BATCH_FILES / cores) * _WAVE_S
    # A batch fits inside the trigger, so the trigger rate bounds intake.
    assert batch_s < _TRIGGER_S
    drain_s = _CORPUS_FILES_S10 / _BATCH_FILES * max(batch_s, _TRIGGER_S)
    assert drain_s <= 0.6 * _WINDOW_S


def test_c360_bronze_ingest_is_unchanged():
    p = get_job_profile("bronze-ingest", "customer360")
    assert (p["base_executors"], p["executor_cores"], p["executor_memory"]) == (2, 2, "4g")


def test_override_scales_with_scale_and_respects_the_cap():
    assert get_executor_count("bronze-ingest", 10, "financial") == 5
    assert get_executor_count("bronze-ingest", 100, "financial") == 8
    assert get_executor_count("bronze-ingest", 10_000, "financial") <= _MAX_EXECUTORS_SAFE


def test_peak_requirements_count_the_override():
    """Gotcha 34: the capacity preflight reads compute_peak_requirements."""
    aml = compute_peak_requirements(10, "sustained", "financial")
    c360 = compute_peak_requirements(10, "sustained", "customer360")
    ingest = next(r for r in aml.per_job if r.job_type == "bronze-ingest")
    c360_ingest = next(r for r in c360.per_job if r.job_type == "bronze-ingest")
    assert ingest.executors == 5
    assert ingest.cpu_cores == 5 * 4 + 2
    assert c360_ingest.cpu_cores == 2 * 2 + 2


def test_concurrent_budget_does_not_cap_the_override_back_to_base():
    """The budget used the base profile, so on any cluster with known
    capacity it capped AML bronze-ingest to the c360 count."""
    budget = _streaming_concurrent_budget(_config("financial", 10), 434_000)
    assert budget[JobType.BRONZE_INGEST] == 5


def test_manifest_deploys_the_override():
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        total_cpu_millicores=434_000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=434_000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    manifest = SparkJobManager(_config("financial", 10), k8s)._build_manifest(JobType.BRONZE_INGEST)
    ex = manifest["spec"]["executor"]
    assert ex["instances"] == 5
    assert ex["cores"] == 4


_GRID = [(scale, cores) for scale in (1, 10, 100, 500) for cores in (60, 80, 100, 150, 434)]


@pytest.mark.parametrize(("scale", "cores"), _GRID)
def test_the_override_never_takes_cores_from_silver_or_gold(scale, cores):
    """The pre-override split is the c360 split (same base profiles). AML
    overrides all three streaming stages (bronze here, silver and gold in
    test_aml_continuous_sizing), so each keeps at least the whole executors
    its base cores hold and only the headroom above that is shared. Silver
    and gold keep 4-core executors, so they never drop below the old count."""
    aml = _streaming_concurrent_budget(_config("financial", scale), cores * 1000)
    base = _streaming_concurrent_budget(_config("customer360", scale), cores * 1000)
    assert aml[JobType.SILVER_STREAM] >= base[JobType.SILVER_STREAM]
    assert aml[JobType.GOLD_REFRESH] >= base[JobType.GOLD_REFRESH]
    # Bronze keeps at least the whole 4-core executors its base cores hold.
    assert aml[JobType.BRONZE_INGEST] >= max(1, base[JobType.BRONZE_INGEST] * 2 // 4)


@pytest.mark.parametrize("scale", [1, 10, 50, 100, 200, 500])
@pytest.mark.parametrize("cores", range(20, 502, 2))
def test_the_override_never_requests_more_than_the_budget_or_the_old_split(scale, cores):
    """Rounding bronze up to whole 4-core executors used to push the total
    past both the budget and what the base split requested."""
    cfg = _config("financial", scale)
    aml = _streaming_concurrent_budget(cfg, cores * 1000)
    base = _streaming_concurrent_budget(_config("customer360", scale), cores * 1000)
    per_core = {JobType.BRONZE_INGEST: 4, JobType.SILVER_STREAM: 4, JobType.GOLD_REFRESH: 4}
    base_cores = {JobType.BRONZE_INGEST: 2, JobType.SILVER_STREAM: 4, JobType.GOLD_REFRESH: 4}
    aml_total = sum(aml[j] * per_core[j] for j in aml)
    base_total = sum(base[j] * base_cores[j] for j in base)
    budget_cores = _budget_cores(cfg, cores)
    assert aml_total <= max(budget_cores, base_total)


def _budget_cores(cfg, cores):
    from lakebench.config.autosizer import _parse_cpu_millicores

    trino = cfg.architecture.query_engine.trino
    co = (
        _parse_cpu_millicores(trino.coordinator.cpu)
        + trino.worker.replicas * _parse_cpu_millicores(trino.worker.cpu)
        + 1000
    )
    dg = cfg.architecture.workload.datagen
    dg_m = dg.parallelism * _parse_cpu_millicores(dg.cpu)
    return int(max(0, cores * 1000 - co - dg_m) * 0.9) // 1000


@pytest.mark.parametrize(
    ("scale", "cores", "expected"),
    [
        # c360 split, pinned: the refactor into _proportional_caps must not
        # move it.
        (10, 60, (2, 4, 2)),
        (100, 80, (3, 8, 3)),
        (500, 150, (8, 16, 8)),
        (500, 434, (10, 20, 10)),
    ],
)
def test_c360_split_is_unchanged(scale, cores, expected):
    b = _streaming_concurrent_budget(_config("customer360", scale), cores * 1000)
    got = (b[JobType.BRONZE_INGEST], b[JobType.SILVER_STREAM], b[JobType.GOLD_REFRESH])
    assert got == expected


def _capacity_k8s(cores):
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        total_cpu_millicores=cores * 1000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=cores * 1000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    return k8s


def test_an_explicit_count_is_not_warned_as_capped():
    cfg = _config("financial", 10)
    cfg.platform.compute.spark.bronze_ingest_executors = 6
    mgr = SparkJobManager(cfg, _capacity_k8s(60))
    manifest = mgr._build_manifest(JobType.BRONZE_INGEST)
    assert manifest["spec"]["executor"]["instances"] == 6
    assert mgr.budget_warnings == []


def test_a_capped_stage_is_warned_about_by_name(caplog):
    import logging

    mgr = SparkJobManager(_config("financial", 10), _capacity_k8s(60))
    with caplog.at_level(logging.WARNING):
        manifest = mgr._build_manifest(JobType.BRONZE_INGEST)
    assert manifest["spec"]["executor"]["instances"] == 1
    assert mgr.budget_warnings == [
        "Concurrent budget: bronze-ingest capped from 5 to 1 executors "
        "(cluster too small for the profile)"
    ]
    assert any(
        r.levelno == logging.WARNING and "bronze-ingest" in r.message for r in caplog.records
    )


def test_scorecard_uses_the_requested_executor_count():
    """The budget capped bronze to 3; CPU-hours must count 3, not the
    profile's 5."""
    from datetime import datetime, timedelta

    from lakebench.metrics.collector import (
        MetricsCollector,
        StreamingJobMetrics,
        build_pipeline_benchmark,
    )

    collector = MetricsCollector()
    run = collector.start_run("r", "d", {"workload_schema": "financial", "scale": 10})
    run.start_time = datetime(2026, 9, 25, 10, 0)
    run.end_time = run.start_time + timedelta(seconds=1800)
    run.streaming.append(
        StreamingJobMetrics(
            job_name="lakebench-bronze-ingest",
            job_type="bronze-ingest",
            elapsed_seconds=1800,
            total_rows_processed=1000,
            requested_executors=3,
            success=True,
        )
    )
    pb = build_pipeline_benchmark(run)
    bronze = next(s for s in pb.stages if s.stage_name == "bronze")
    assert bronze.executor_count == 3
    assert pb.total_core_hours == pytest.approx(3 * 4 * 1800 / 3600)


def test_submit_reports_the_requested_count():
    src = (
        Path(__file__).resolve().parents[1] / "src/lakebench/modules/pipeline_engines/spark/job.py"
    ).read_text()
    assert 'executor_count=int(manifest["spec"]["executor"].get("instances") or 0)' in src
    sustained = (
        Path(__file__).resolve().parents[1] / "src/lakebench/cli/_sustained.py"
    ).read_text()
    assert "streaming_metrics.requested_executors = requested_executors.get(job_name)" in sustained
