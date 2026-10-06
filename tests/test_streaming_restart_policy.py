"""Streaming SparkApplications do not auto-rerun a failed driver (LB-279 Part A).

The continuous gate refuses a run whose silver-stream driver was resubmitted
inside the window, because the first driver's work is not in the retained log.
An uncapped ``restartPolicy.type = "Always"`` on streaming jobs turned any
transient event into a silent FAIL with the diagnostic log lost (the Spark
Operator deletes the pod when it moves to PENDING_RERUN). ``OnFailure`` with
0 driver retries converts the same event into a definitive FAILED verdict
whose first driver's log is still inspectable.

Submission retries stay (they run before the driver starts and the submitting
operator can be transiently busy).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakebench.config import LakebenchConfig
from lakebench.k8s.client import ClusterCapacity
from lakebench.modules.pipeline_engines.spark.job import _STREAMING_JOB_TYPES
from lakebench.spark.job import JobType, SparkJobManager


def _cfg(schema: str = "customer360", scale: float = 1.0) -> LakebenchConfig:
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


def _capacity_k8s(cores: int = 434):
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        total_cpu_millicores=cores * 1000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=cores * 1000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    # Match the pattern in test_aml_continuous_sizing: give get_free_capacity
    # the same total so admission preflight passes.
    k8s.get_free_capacity.return_value = ClusterCapacity(
        total_cpu_millicores=cores * 1000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=cores * 1000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    return k8s


@pytest.mark.parametrize(
    "job_type",
    sorted(_STREAMING_JOB_TYPES, key=lambda j: j.value),
    ids=lambda j: j.value,
)
def test_streaming_jobs_do_not_auto_resubmit_failed_drivers(job_type):
    """A streaming driver that exits is a definitive failure (LB-279)."""
    # financial continuous for silver_stream, customer360 for the others
    schema = "financial" if "financial" in job_type.value else "customer360"
    mgr = SparkJobManager(_cfg(schema), _capacity_k8s())
    manifest = mgr._build_manifest(job_type)
    policy = manifest["spec"]["restartPolicy"]

    assert policy["type"] == "OnFailure", (
        f"streaming job {job_type.value} uses restartPolicy.type={policy['type']!r}; "
        "must be 'OnFailure' with onFailureRetries=0 so a mid-window driver exit "
        "becomes a definitive FAIL (LB-279 Part A)"
    )
    assert policy.get("onFailureRetries") == 0, (
        f"streaming job {job_type.value} has onFailureRetries="
        f"{policy.get('onFailureRetries')!r}; must be 0 to prevent silent "
        "mid-window resubmission (LB-279 Part A)"
    )
    # Submission retries stay (operator can be transiently busy).
    assert policy.get("onSubmissionFailureRetries", 0) >= 1


def test_batch_default_keeps_two_driver_retries():
    """Batch (non-streaming, non-special-case) still uses OnFailure=2.

    Regression guard: Part A changes only the streaming branch of the
    ``_restart_policy`` ternary; the batch branch is unchanged.
    """
    mgr = SparkJobManager(_cfg("customer360"), _capacity_k8s())
    manifest = mgr._build_manifest(JobType.SILVER_BUILD)
    policy = manifest["spec"]["restartPolicy"]
    assert policy["type"] == "OnFailure"
    assert policy.get("onFailureRetries") == 2
    assert policy.get("onSubmissionFailureRetries", 0) >= 1
