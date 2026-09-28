"""H4 fat-finger guard depends on job.py exporting the stream checkpoint URI
to the AML batch pod.

Without ``LB_FINANCIAL_SILVER_CHECKPOINT`` set for ``JobType.SILVER_BUILD``,
``silver_build_financial.main()``'s guard reads ``None`` and returns
silently, letting a concurrent batch run wipe silver.transactions the
continuous stream has committed since it started. That is the very
scenario the block was written to prevent.

Regression check: the pre-fix env-bundle set ``LB_FINANCIAL_SILVER_CHECKPOINT``
only inside the streaming-specific block (``job.py:2899``, gated on
``job_type == JobType.SILVER_STREAM``), so this test fails against the
pre-fix tree and passes once the AML env block exports the URI for
``JobType.SILVER_BUILD`` too.
"""

from __future__ import annotations

from unittest.mock import patch

from lakebench.config.schema import WorkloadSchema
from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.test_spark import _make_config, _mock_k8s


def _silver_build_env_for_financial() -> dict[str, str]:
    """Build the SILVER_BUILD driver env for a minimal AML deployment."""
    from kubernetes.client.exceptions import ApiException

    def _cm_404(name, namespace):
        raise ApiException(status=404)

    cfg = _make_config()
    cfg.architecture.workload.schema_type = WorkloadSchema.FINANCIAL
    with patch(
        "kubernetes.client.CoreV1Api.read_namespaced_config_map",
        side_effect=_cm_404,
    ):
        manifest = SparkJobManager(cfg, _mock_k8s())._build_manifest(JobType.SILVER_BUILD)
    return {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}


def test_aml_silver_build_env_carries_silver_stream_checkpoint():
    """SILVER_BUILD env sets LB_FINANCIAL_SILVER_CHECKPOINT so H4's guard fires."""
    env = _silver_build_env_for_financial()
    assert "LB_FINANCIAL_SILVER_CHECKPOINT" in env, (
        "AML batch silver must know the deployment's silver-stream checkpoint "
        "URI so H4's fat-finger guard can probe for _STARTED. Missing this "
        "makes the guard silent no-op (see cli_run.py/silver_build_financial "
        "guard call site) and a concurrent batch wipes silver.transactions."
    )


def test_h4_checkpoint_uri_matches_stream_side():
    """The URI silver_build sees is the same one silver_stream writes into.

    A different bucket, prefix, or path would let the batch guard probe an
    empty location and always fail-open, so the guard would only refuse in
    the trivial case where the operator happened to point both at the same
    directory.
    """
    env = _silver_build_env_for_financial()
    uri = env["LB_FINANCIAL_SILVER_CHECKPOINT"]
    # Same shape as job.py's trigger_map[SILVER_STREAM] entry:
    #   f"s3a://{s3.buckets.silver}/{sustained.checkpoint_base}/silver-stream/"
    assert uri.startswith("s3a://")
    assert "/silver-stream/" in uri, (
        "H4 guard reads {checkpoint}/_STARTED; the URI must match the "
        "stream's trigger_map entry exactly, not a sibling path."
    )


def test_h4_env_absent_for_c360_workloads():
    """C360 deployments do not need this env (H4 is AML-only)."""
    from kubernetes.client.exceptions import ApiException

    def _cm_404(name, namespace):
        raise ApiException(status=404)

    cfg = _make_config()  # default is c360
    with patch(
        "kubernetes.client.CoreV1Api.read_namespaced_config_map",
        side_effect=_cm_404,
    ):
        manifest = SparkJobManager(cfg, _mock_k8s())._build_manifest(JobType.SILVER_BUILD)
    env = {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}
    assert "LB_FINANCIAL_SILVER_CHECKPOINT" not in env, (
        "C360 silver_build has no continuous variant to collide with; "
        "the LB_FINANCIAL_* env space should stay AML-scoped."
    )
