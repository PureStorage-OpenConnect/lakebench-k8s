"""Tests for Spark Operator application-state handling.

The operator's ``ApplicationStateType`` defines thirteen states. `JobState`
modelled six, and `wait_for_completion` treated exactly two as terminal, so
every other state fell through to ``UNKNOWN`` and the monitor polled a
finished job until it timed out.

``SUCCEEDING`` is the one that bites: the operator sets it the moment the
driver finishes and only later settles on ``COMPLETED``. A successful job
observed in that window is reported as a timeout.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.modules.pipeline_engines.spark.job import (
    FAILURE_STATES,
    SUCCESS_STATES,
    JobState,
    is_terminal,
)
from lakebench.modules.pipeline_engines.spark.monitor import SparkJobMonitor

# Verbatim from api/v1beta2/sparkapplication_types.go (operator v2.4.0).
# If the operator adds a state, this list is what should be updated first.
_OPERATOR_STATES = [
    "",
    "SUBMITTED",
    "RUNNING",
    "COMPLETED",
    "FAILED",
    "SUBMISSION_FAILED",
    "PENDING_RERUN",
    "INVALIDATING",
    "SUCCEEDING",
    "FAILING",
    "SUSPENDING",
    "SUSPENDED",
    "RESUMING",
    "UNKNOWN",
]


class TestEveryOperatorStateIsModelled:
    def test_no_operator_state_is_unmappable(self):
        """Any state falling to UNKNOWN would be polled until timeout."""
        for raw in _OPERATOR_STATES:
            JobState(raw)  # raises ValueError if unmodelled

    def test_succeeding_is_success_not_pending(self):
        assert JobState.SUCCEEDING in SUCCESS_STATES
        assert is_terminal(JobState.SUCCEEDING)

    def test_failing_is_failure_not_pending(self):
        assert JobState.FAILING in FAILURE_STATES
        assert is_terminal(JobState.FAILING)

    def test_submission_failed_is_terminal(self):
        assert is_terminal(JobState.SUBMISSION_FAILED)

    def test_unknown_is_not_terminal(self):
        """An unrecognised state must keep waiting, not guess an outcome."""
        assert not is_terminal(JobState.UNKNOWN)

    def test_new_is_not_terminal(self):
        assert not is_terminal(JobState.NEW)

    def test_running_is_not_terminal(self):
        assert not is_terminal(JobState.RUNNING)

    def test_success_and_failure_do_not_overlap(self):
        assert not (SUCCESS_STATES & FAILURE_STATES)


def _monitor_returning(*states: JobState) -> tuple[SparkJobMonitor, MagicMock]:
    """A monitor whose job manager yields the given states in order."""
    mgr = MagicMock()
    mgr.get_job_status.side_effect = [
        MagicMock(state=s, executor_count=0, message=s.value) for s in states
    ]
    monitor = SparkJobMonitor.__new__(SparkJobMonitor)
    monitor.job_manager = mgr
    return monitor, mgr


class TestWaitForCompletionTerminates:
    def test_succeeding_ends_the_wait_as_success(self):
        """The u02 case: driver finished, monitor kept polling for 1200s."""
        monitor, _ = _monitor_returning(JobState.RUNNING, JobState.SUCCEEDING)
        with (
            patch.object(SparkJobMonitor, "_get_driver_logs", return_value=""),
            patch("time.sleep"),
        ):
            result = monitor.wait_for_completion("lakebench-bronze-verify", timeout_seconds=60)

        assert result.success is True
        assert "timed out" not in result.message

    def test_failing_ends_the_wait_as_failure(self):
        monitor, _ = _monitor_returning(JobState.RUNNING, JobState.FAILING)
        with (
            patch.object(SparkJobMonitor, "_get_driver_logs", return_value=""),
            patch("time.sleep"),
        ):
            result = monitor.wait_for_completion("lakebench-bronze-verify", timeout_seconds=60)

        assert result.success is False
        assert "timed out" not in result.message

    def test_completed_still_works(self):
        monitor, _ = _monitor_returning(JobState.RUNNING, JobState.COMPLETED)
        with (
            patch.object(SparkJobMonitor, "_get_driver_logs", return_value=""),
            patch("time.sleep"),
        ):
            result = monitor.wait_for_completion("j", timeout_seconds=60)
        assert result.success is True

    def test_unknown_is_polled_not_resolved(self):
        """UNKNOWN must not be guessed either way -- it times out."""
        mgr = MagicMock()
        mgr.get_job_status.return_value = MagicMock(
            state=JobState.UNKNOWN, executor_count=0, message="UNKNOWN"
        )
        monitor = SparkJobMonitor.__new__(SparkJobMonitor)
        monitor.job_manager = mgr

        with (
            patch.object(SparkJobMonitor, "_get_driver_logs", return_value=""),
            patch("time.sleep"),
        ):
            result = monitor.wait_for_completion("j", timeout_seconds=-1)

        assert result.success is False
        assert "timed out" in result.message


class TestTimeoutNamesTheState:
    def test_timeout_reports_last_observed_state(self):
        """'timed out' alone cannot distinguish stuck from unmodelled."""
        mgr = MagicMock()
        mgr.get_job_status.return_value = MagicMock(
            state=JobState.RUNNING, executor_count=4, message="RUNNING"
        )
        monitor = SparkJobMonitor.__new__(SparkJobMonitor)
        monitor.job_manager = mgr

        with (
            patch.object(SparkJobMonitor, "_get_driver_logs", return_value=""),
            patch("time.sleep"),
            patch("time.time", side_effect=[0, 0, 1, 9999, 9999]),
        ):
            result = monitor.wait_for_completion("j", timeout_seconds=60)

        assert "RUNNING" in result.message


class TestUnrecognisedStateIsLoud:
    def test_unmodelled_state_warns(self, caplog):
        """A future operator state must not be absorbed silently."""
        from lakebench.modules.pipeline_engines.spark.job import SparkJobManager

        mgr = SparkJobManager.__new__(SparkJobManager)
        mgr.namespace = "u01"

        api = MagicMock()
        api.get_namespaced_custom_object.return_value = {
            "status": {"applicationState": {"state": "TIME_TRAVELLING"}}
        }

        with (
            patch("kubernetes.client.CustomObjectsApi", return_value=api),
            caplog.at_level("WARNING"),
        ):
            status = mgr.get_job_status("lakebench-bronze-verify")

        assert status.state == JobState.UNKNOWN
        assert "TIME_TRAVELLING" in caplog.text
        assert "polled until timeout" in caplog.text
