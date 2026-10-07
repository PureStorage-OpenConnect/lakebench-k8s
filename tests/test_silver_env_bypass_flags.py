"""A1 (LB-044 gate) production-safety: job.py's env builder must never set
LB_SILVER_TEST_ALLOW_EMPTY or LB_TESTING for any silver job type.

If either leaks into a production driver env, the bypass in
assert_progress silences the gate on real runs -- the exact silent-PASS
the gate exists to prevent. Belt-and-braces against a future change to
_build_env_vars accidentally exporting a test flag.
"""

from __future__ import annotations

from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
from tests.fixtures.spark_helpers import _make_config, _mock_k8s

_BYPASS_FLAGS = ("LB_SILVER_TEST_ALLOW_EMPTY", "LB_TESTING")
_SILVER_JOB_TYPES = (JobType.SILVER_BUILD, JobType.SILVER_STREAM)


def _env(job_type: JobType) -> dict[str, str]:
    """The env-var dict SparkJobManager would put on a silver driver pod."""
    manifest = SparkJobManager(_make_config(), _mock_k8s())._build_manifest(job_type)
    return {e["name"]: e["value"] for e in manifest["spec"]["driver"]["env"] if "value" in e}


def test_bypass_flags_absent_from_all_silver_job_types():
    """Belt-and-braces across every silver JobType, no matter which
    downstream branch of _build_env_vars adds env for this workload.
    """
    for job_type in _SILVER_JOB_TYPES:
        env = _env(job_type)
        for flag in _BYPASS_FLAGS:
            assert flag not in env, f"{flag} leaked into {job_type.value} env"
