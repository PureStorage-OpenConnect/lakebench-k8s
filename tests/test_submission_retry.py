"""SUBMISSION_FAILED is waited through, not treated as final (2026-09-24 live
runs: two deployments submitting together both failed FAILED DOWNLOADS in the
shared operator's Ivy cache, SPARK-10878, and lakebench gave up before the
operator's own resubmission)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.modules.pipeline_engines.spark import monitor as mon
from lakebench.modules.pipeline_engines.spark.job import JobState, JobStatus


def _monitor(states):
    jm = MagicMock()
    jm.get_job_status.side_effect = [JobStatus(name="j", state=s, message="m") for s in states]
    m = mon.SparkJobMonitor.__new__(mon.SparkJobMonitor)
    m.job_manager = jm
    m.namespace = "ns"
    m._get_driver_logs = MagicMock(return_value="")
    return m


@patch.object(mon.time, "sleep", lambda s: None)
def test_operator_retry_after_submission_failure_succeeds():
    m = _monitor(
        [JobState.SUBMISSION_FAILED, JobState.SUBMITTED, JobState.RUNNING, JobState.COMPLETED]
    )
    assert m.wait_for_completion("j", timeout_seconds=100, poll_interval=0).success


@patch.object(mon.time, "sleep", lambda s: None)
def test_settled_failure_after_retries_is_final():
    m = _monitor([JobState.SUBMISSION_FAILED, JobState.FAILED])
    assert not m.wait_for_completion("j", timeout_seconds=100, poll_interval=0).success


def test_submission_failed_forever_is_bounded():
    clock = iter(range(0, 100_000, 60))
    m = _monitor([JobState.SUBMISSION_FAILED] * 1000)
    with patch.object(mon.time, "time", lambda: next(clock)), patch.object(mon.time, "sleep"):
        r = m.wait_for_completion("j", timeout_seconds=10**6, poll_interval=0)
    assert not r.success


@patch.object(mon.time, "sleep", lambda s: None)
def test_wait_until_running():
    m = _monitor([JobState.SUBMISSION_FAILED, JobState.SUBMITTED, JobState.RUNNING])
    assert m.wait_until_running("j", timeout_seconds=100, poll_interval=0).success
    m2 = _monitor([JobState.FAILED])
    assert not m2.wait_until_running("j", timeout_seconds=100, poll_interval=0).success


def test_operator_submission_retries_configured():
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager
    from tests.conftest import make_config

    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = None
    mgr = SparkJobManager(make_config(), k8s)
    batch = mgr._build_manifest(JobType.BRONZE_VERIFY)["spec"]["restartPolicy"]
    assert batch["onSubmissionFailureRetries"] >= 5
    assert batch["onSubmissionFailureRetryInterval"] >= 60
    stream = mgr._build_manifest(JobType.BRONZE_INGEST)["spec"]["restartPolicy"]
    assert stream["type"] == "Always" and stream["onSubmissionFailureRetryInterval"] >= 60
