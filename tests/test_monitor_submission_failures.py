"""A batch stage that went through SUBMISSION_FAILED records the failure and the time it lost."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

_MAVEN = (
    "failed to run spark-submit: ... [FAILED     ] "
    "software.amazon.awssdk#aws-sdk-java-pom;2.24.6!aws-sdk-java-pom.pom: "
    "Downloaded file size (0) doesn't match expected Content Length (79350)"
)


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def time(self):
        return self.now

    def sleep(self, s):
        self.now += s


def _monitor():
    from lakebench.spark.monitor import SparkJobMonitor

    m = SparkJobMonitor.__new__(SparkJobMonitor)
    m.namespace = "ns"
    m.job_manager = MagicMock()
    return m


def _status(state, message="", attempts=0):
    from lakebench.spark.job import JobStatus

    return JobStatus(name="j", state=state, message=message, submission_attempts=attempts)


def _status_row(state, message="", attempts=0):
    from lakebench.spark.job import JobState

    return (JobState[state], message, attempts)


def _wait(statuses, **kwargs):
    m = _monitor()
    m.job_manager.get_job_status.side_effect = statuses
    clock = _Clock()
    with (
        patch("lakebench.modules.pipeline_engines.spark.monitor.time", clock),
        patch.object(m, "_get_driver_logs", return_value=""),
    ):
        return m.wait_for_completion("j", timeout_seconds=3600, poll_interval=20, **kwargs)


def test_submission_failure_is_recorded_with_time_lost():
    from lakebench.spark.job import JobState

    seen = []
    r = _wait(
        [
            _status(JobState.SUBMISSION_FAILED, _MAVEN, 1),
            _status(JobState.SUBMISSION_FAILED, _MAVEN, 1),
            _status(JobState.SUBMISSION_FAILED, _MAVEN, 1),
            _status(JobState.SUBMITTED),
            _status(JobState.RUNNING),
            _status(JobState.COMPLETED),
        ],
        on_submission_failure=seen.append,
    )
    assert r.success is True
    assert len(r.submission_failures) == 1
    f = r.submission_failures[0]
    assert f["attempt"] == 1
    assert "aws-sdk-java-pom" in f["reason"] and "0 of 79,350 bytes" in f["reason"]
    assert f["lost_seconds"] == 60.0  # three polls at 20 s
    assert r.submission_retry_seconds == 60.0
    assert len(seen) == 1 and seen[0]["reason"] == f["reason"]


@pytest.mark.parametrize(
    ("states", "attempts", "retry_seconds"),
    [
        # each retry that fails again is its own record
        (
            [
                _status_row("SUBMISSION_FAILED", "driver pod already exist", 1),
                _status_row("SUBMISSION_FAILED", "driver pod already exist", 2),
                _status_row("RUNNING"),
                _status_row("COMPLETED"),
            ],
            [1, 2],
            40.0,
        ),
        # a clean stage has no failures and no retry time
        ([_status_row("RUNNING"), _status_row("COMPLETED")], [], 0.0),
    ],
)
def test_each_failed_submission_is_its_own_record(states, attempts, retry_seconds):
    r = _wait([_status(*row) for row in states])
    assert [f["attempt"] for f in r.submission_failures] == attempts
    assert r.submission_retry_seconds == retry_seconds


def test_failure_still_open_at_the_end_is_closed():
    from lakebench.spark.job import JobState

    r = _wait([_status(JobState.SUBMISSION_FAILED, _MAVEN, 1), _status(JobState.FAILED, "gave up")])
    assert r.success is False
    assert r.submission_failures[0]["lost_seconds"] == 20.0


def test_a_raising_callback_does_not_end_the_wait():
    from lakebench.spark.job import JobState

    def boom(_):
        raise RuntimeError("console gone")

    r = _wait(
        [
            _status(JobState.SUBMISSION_FAILED, _MAVEN, 1),
            _status(JobState.RUNNING),
            _status(JobState.COMPLETED),
        ],
        on_submission_failure=boom,
    )
    assert r.success is True and len(r.submission_failures) == 1


def test_metrics_carry_the_failures_through_the_scorecard_and_storage(tmp_path):
    from lakebench.metrics.collector import JobMetrics, PipelineMetrics, build_pipeline_benchmark
    from lakebench.metrics.storage import MetricsStorage

    failures = [
        {"at": "2026-09-27T15:06:08+00:00", "attempt": 1, "reason": "r", "lost_seconds": 59.0}
    ]
    run = PipelineMetrics(run_id="run-x", deployment_name="d", start_time=datetime(2026, 9, 27))
    run.jobs.append(
        JobMetrics(
            job_name="lakebench-bronze-verify",
            job_type="bronze-verify",
            elapsed_seconds=149.7,
            success=True,
            submission_failures=failures,
            submission_retry_seconds=59.0,
        )
    )
    run.pipeline_benchmark = build_pipeline_benchmark(run)
    (bronze,) = [s for s in run.pipeline_benchmark.stages if s.stage_name == "bronze"]
    assert bronze.submission_failures == failures
    assert bronze.submission_retry_seconds == 59.0

    storage = MetricsStorage(tmp_path)
    storage.save_run(run)
    back = storage.load_run("run-x")
    assert back.jobs[0].submission_failures == failures
    assert back.jobs[0].submission_retry_seconds == 59.0
    (bronze,) = [s for s in back.pipeline_benchmark.stages if s.stage_name == "bronze"]
    assert bronze.submission_retry_seconds == 59.0


def test_report_marks_elapsed_that_includes_retries():
    from lakebench.metrics.collector import JobMetrics, PipelineMetrics
    from lakebench.reports.generator import ReportGenerator

    run = PipelineMetrics(run_id="r", deployment_name="d", start_time=datetime(2026, 9, 27))
    run.jobs.append(
        JobMetrics(
            job_name="lakebench-bronze-verify",
            job_type="bronze-verify",
            elapsed_seconds=149.7,
            submission_failures=[{"attempt": 1, "lost_seconds": 59.0}],
            submission_retry_seconds=59.0,
        )
    )
    run.jobs.append(
        JobMetrics(job_name="lakebench-silver-build", job_type="silver-build", elapsed_seconds=52.0)
    )
    gen = ReportGenerator.__new__(ReportGenerator)
    rows = gen._generate_jobs_table(run).split("<tr")
    (retried,) = [r for r in rows if "lakebench-bronze-verify" in r]
    (clean,) = [r for r in rows if "lakebench-silver-build" in r]
    assert "59" in retried
    assert "59" not in clean and "failed submission" not in clean
