"""Dependency-download submission failures are retried (2026-09-24 live runs:
two deployments submitting at the same second both failed with FAILED
DOWNLOADS from the shared operator's Ivy cache, SPARK-10878)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.modules.pipeline_engines.spark import monitor as mon
from lakebench.modules.pipeline_engines.spark.job import (
    JobState,
    JobStatus,
    is_transient_submission_failure,
)

IVY = (
    "failed to submit spark application: failed to run spark-submit: ... "
    ":: FAILED DOWNLOADS :: software.amazon.awssdk#bundle;2.24.6!bundle.jar"
)


def test_classifier():
    assert is_transient_submission_failure(IVY)
    assert not is_transient_submission_failure("Driver exited with code 1")
    # A download word outside a submission failure is not a submission race.
    assert not is_transient_submission_failure("FAILED DOWNLOADS in driver log")


def _monitor(states):
    jm = MagicMock()
    jm.get_job_status.side_effect = [JobStatus(name="j", state=s, message=m) for s, m in states]
    jm.resubmit.return_value = JobStatus(name="j", state=JobState.SUBMITTED, message="")
    m = mon.SparkJobMonitor.__new__(mon.SparkJobMonitor)
    m.job_manager = jm
    m.namespace = "ns"
    m._get_driver_logs = MagicMock(return_value="")
    return m, jm


@patch.object(mon.time, "sleep", lambda s: None)
def test_wait_for_completion_resubmits_then_succeeds():
    m, jm = _monitor([(JobState.SUBMISSION_FAILED, IVY), (JobState.COMPLETED, "")])
    r = m.wait_for_completion("j", timeout_seconds=100, poll_interval=0)
    assert r.success
    jm.resubmit.assert_called_once_with("j")


@patch.object(mon.time, "sleep", lambda s: None)
def test_non_transient_failure_is_not_retried():
    m, jm = _monitor([(JobState.FAILED, "Driver exited with code 1")])
    r = m.wait_for_completion("j", timeout_seconds=100, poll_interval=0)
    assert not r.success
    jm.resubmit.assert_not_called()


@patch.object(mon.time, "sleep", lambda s: None)
def test_retries_are_bounded():
    m, jm = _monitor([(JobState.SUBMISSION_FAILED, IVY)] * 5)
    r = m.wait_for_completion("j", timeout_seconds=100, poll_interval=0)
    assert not r.success
    assert jm.resubmit.call_count == mon._MAX_SUBMISSION_RETRIES


@patch.object(mon.time, "sleep", lambda s: None)
def test_wait_until_running():
    m, jm = _monitor(
        [(JobState.SUBMISSION_FAILED, IVY), (JobState.SUBMITTED, ""), (JobState.RUNNING, "")]
    )
    assert m.wait_until_running("j", timeout_seconds=100, poll_interval=0).success
    m2, _ = _monitor([(JobState.FAILED, "OOMKilled")])
    assert not m2.wait_until_running("j", timeout_seconds=100, poll_interval=0).success


def test_resubmit_replays_the_last_submission():
    from lakebench.modules.pipeline_engines.spark.job import JobType, SparkJobManager

    jm = SparkJobManager.__new__(SparkJobManager)
    jm._submit_args = {"lakebench-bronze-verify": (JobType.BRONZE_VERIFY, None, {"A": "1"}, None)}
    with patch.object(SparkJobManager, "submit_job", return_value="ok") as sj:
        assert jm.resubmit("lakebench-bronze-verify") == "ok"
        sj.assert_called_once_with(
            JobType.BRONZE_VERIFY, None, cycle_env={"A": "1"}, arguments=None
        )
    assert jm.resubmit("lakebench-unknown") is None
