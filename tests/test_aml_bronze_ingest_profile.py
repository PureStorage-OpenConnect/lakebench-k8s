"""AML continuous bronze-ingest profile override.

run-20260925-104452-21bf3a (AML, continuous, scale 10): bronze-ingest on the
base 2 executors x 2 cores ran every 50-file micro-batch in 109.7 s under a
30 s trigger, 0.456 files/s, and left 42% of the 1,371-file corpus unread in
the 1800 s window. The override sizes bronze so the trickle ceiling
(50 files / 30 s), not bronze, bounds intake at scale 10.
"""

from __future__ import annotations

from unittest.mock import MagicMock

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
    assert ingest.executors == 5
    assert ingest.cpu_cores == 5 * 4 + 2
    assert aml.cpu_cores - c360.cpu_cores == ingest.cpu_cores - (2 * 2 + 2)


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
