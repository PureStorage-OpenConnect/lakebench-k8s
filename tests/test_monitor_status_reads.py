"""Stage monitor: bounded status reads, transient errors, stall logging.

The 2026-09-27 sweep lost 6-10 min in bronze-verify on two Polaris recipes
with no progress line: the monitor saw RUNNING once and then nothing it
reported until the end. A status read had no timeout, and a job sitting in a
non-running, non-terminal operator state was invisible.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _monitor():
    from lakebench.spark.monitor import SparkJobMonitor

    m = SparkJobMonitor.__new__(SparkJobMonitor)
    m.namespace = "ns"
    m.job_manager = MagicMock()
    return m


def _status(state):
    from lakebench.spark.job import JobStatus

    return JobStatus(name="j", state=state, message="")


def test_status_read_is_bounded():
    from lakebench.modules.pipeline_engines.spark import job as jobmod

    mgr = jobmod.SparkJobManager.__new__(jobmod.SparkJobManager)
    mgr.namespace = "ns"
    api = MagicMock()
    api.get_namespaced_custom_object.return_value = {"status": {}}
    with patch("kubernetes.client.CustomObjectsApi", return_value=api):
        mgr.get_job_status("j")
    kw = api.get_namespaced_custom_object.call_args.kwargs
    assert kw["_request_timeout"] == jobmod.STATUS_REQUEST_TIMEOUT
    assert all(0 < t <= 60 for t in jobmod.STATUS_REQUEST_TIMEOUT)


def test_transient_read_failure_does_not_end_the_wait():
    from urllib3.exceptions import ReadTimeoutError

    from lakebench.spark.job import JobState

    m = _monitor()
    m.job_manager.get_job_status.side_effect = [
        _status(JobState.RUNNING),
        ReadTimeoutError(None, "/x", "read timed out"),
        _status(JobState.COMPLETED),
    ]
    with patch.object(m, "_get_driver_logs", return_value=""):
        r = m.wait_for_completion("j", timeout_seconds=60, poll_interval=0)
    assert r.success is True


def test_non_transient_read_failure_still_raises():
    from kubernetes.client.rest import ApiException

    m = _monitor()
    m.job_manager.get_job_status.side_effect = ApiException(status=403)
    with pytest.raises(ApiException):
        m.wait_for_completion("j", timeout_seconds=60, poll_interval=0)


def test_stall_after_running_is_logged(caplog):
    from lakebench.modules.pipeline_engines.spark import monitor as monmod
    from lakebench.spark.job import JobState

    m = _monitor()
    m.job_manager.get_job_status.side_effect = [
        _status(JobState.RUNNING),
        _status(JobState.SUBMITTED),
        _status(JobState.SUBMITTED),
        _status(JobState.COMPLETED),
    ]
    with (
        patch.object(monmod, "_STALL_WARN_S", 0.0),
        patch.object(m, "_get_driver_logs", return_value=""),
        caplog.at_level("WARNING"),
    ):
        r = m.wait_for_completion("j", timeout_seconds=60, poll_interval=0)
    assert r.success is True
    stalls = [x for x in caplog.messages if "was RUNNING and the operator has reported" in x]
    assert len(stalls) == 1 and "SUBMITTED" in stalls[0]
