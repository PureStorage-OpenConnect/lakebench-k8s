"""Spark Job monitoring for Lakebench.

Provides waiting and progress tracking for Spark jobs.
"""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

from lakebench.modules.pipeline_engines.spark.job import (
    FAILURE_STATES,
    SUCCESS_STATES,
    JobState,
    JobStatus,
    SparkJobManager,
    is_transient_status_error,
)

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig
    from lakebench.k8s import K8sClient

logger = logging.getLogger(__name__)

# SUBMISSION_FAILED is not final: the operator resubmits on its own
# (restartPolicy onSubmissionFailureRetries) and settles on FAILED once the
# retries run out. Treating it as final gave up on jobs the operator was
# about to resubmit, e.g. after a shared-Ivy-cache download race. Waiting is
# bounded in case the operator never settles.
# The operator backs off linearly (interval x attempt): 5 retries at 60 s
# start at 60, 180, 360, 600 and 900 s after the first failure, and the app
# stays SUBMISSION_FAILED throughout, so the grace must outlast 900 s plus a
# slow Ivy resolve.
_SUBMISSION_FAILED_GRACE_S = 1800

# A status read slower than this is logged: it separates a stalled poll on
# this host from an operator that is slow to report the end.
_SLOW_POLL_S = 45.0
# A job that was RUNNING and then sits in a non-terminal, non-running state
# (SUBMITTED, PENDING_RERUN, UNKNOWN, ...) this long is logged once per state.
# A 2026-09-27 sweep lost ~10 min per stage this way with nothing on screen.
_STALL_WARN_S = 60.0


# What a failed jar fetch from the deployment's dependency server looks like
# in a driver log. Spark opens http jars with URL.openConnection(): a 404 is a
# FileNotFoundException naming the URL; a refused or unresolvable connection
# names no URL, so it counts only next to Spark's fetch frames.
_DEPS_NOT_SERVED = re.compile(r"java\.io\.FileNotFoundException: (http://lb-deps\.\S+)")
_DEPS_SERVER_ERROR = re.compile(
    r"Server returned HTTP response code: (5\d\d) for URL: (http://lb-deps\.\S+)"
)
_DEPS_UNREACHABLE = re.compile(
    r"(java\.net\.ConnectException[^\n]*|java\.net\.UnknownHostException: lb-deps\.\S+"
    r"|java\.net\.SocketTimeoutException[^\n]*)"
)
_FETCH_FRAMES = ("Utils$.doFetchFile", "Utils.doFetchFile", "DependencyUtils", "downloadFile")


def classify_dependency_failure(log: str | None) -> str | None:
    """A reason when a driver failed fetching the dependency set, else None."""
    text = log or ""
    m = _DEPS_NOT_SERVED.search(text)
    if m:
        return (
            f"dependency server does not serve this set ({m.group(1)} not found); "
            "the deployment's set changed since the run started: re-run deploy, then run"
        )
    m = _DEPS_SERVER_ERROR.search(text)
    if m:
        return f"dependency server error {m.group(1)} for {m.group(2)}"
    # A connection error counts only when Spark's fetch frames are in its
    # own stack (the frame and "Caused by" lines right after it), not
    # anywhere in the tail. The JDK puts about 20 frames above Spark's.
    lines = text.splitlines()
    for i, line in enumerate(lines):
        m = _DEPS_UNREACHABLE.search(line)
        if not m:
            continue
        stack = []
        for ln in lines[i + 1 :]:
            s = ln.strip()
            if not (s.startswith("at ") or s.startswith("...") or s.startswith("Caused by")):
                break
            stack.append(s)
        if any(f in s for s in stack for f in _FETCH_FRAMES):
            return f"dependency server unreachable ({m.group(1).strip()[:200]})"
    return None


@dataclass
class JobResult:
    """Final result of a Spark job."""

    job_name: str
    success: bool
    message: str
    elapsed_seconds: float
    driver_logs: str | None = None
    # The SparkApplication status that ended the wait (None on a timeout
    # before any status was read).
    final_status: JobStatus | None = None
    # Seconds into the wait of the last poll whose status was not terminal
    # (None: none seen). The application ended after about this point.
    last_running_elapsed: float | None = None
    # Each SUBMISSION_FAILED the operator reported before the job ended:
    # {"at", "attempt", "reason", "lost_seconds"}. lost_seconds runs from the
    # first poll that saw the failure to the first that saw another state,
    # so it is good to one poll interval; the stage's elapsed includes it.
    submission_failures: list[dict[str, Any]] = field(default_factory=list)

    @property
    def submission_retry_seconds(self) -> float:
        """Seconds the stage spent waiting on operator submission retries."""
        return round(sum(f.get("lost_seconds") or 0.0 for f in self.submission_failures), 1)


# Stage timing sources, best first (JobMetrics.timing_source).
TIMING_DRIVER = "driver_container"  # driver container terminated.finishedAt
TIMING_APPLICATION = "spark_application"  # SparkApplication status.terminationTime
TIMING_POLL = "poll"  # the monitor poll that first saw the terminal state

# A cluster timestamp is RFC 3339 truncated to the second (its true time is up
# to 1 s later) and the host-to-cluster offset comes from a Date header of the
# same resolution: each is centred with +0.5 s, which leaves about +/-1 s.
# Node clocks (kubelet, operator) are not corrected by the offset and the
# offset read carries half its round trip, so 2 s, not 1 s.
_CLUSTER_TIMING_RESOLUTION_S = 2.0
# The operator reported the application still running at the last
# non-terminal poll; its view lags the driver by its reconcile delay. An end
# earlier than that poll minus this lag is a slow node clock or a stale
# status. Generous: a busy shared operator can lag tens of seconds.
_OPERATOR_LAG_S = 60.0
# How far a cluster end may sit outside [submitted, observed] before it is
# taken as clock skew or a stale status rather than rounding.
_CLUSTER_TIMING_TOLERANCE_S = 2.0


@dataclass
class StageTiming:
    """When a batch stage ran, on this host's clock."""

    start: datetime
    end: datetime
    source: str
    resolution_seconds: float
    note: str = ""

    @property
    def elapsed_seconds(self) -> float:
        return max(0.0, (self.end - self.start).total_seconds())


def parse_k8s_time(value: object) -> datetime | None:
    """An aware UTC datetime from a Kubernetes timestamp (str or datetime)."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        at = value
    else:
        try:
            at = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    return at.astimezone(timezone.utc)


def stage_timing(
    submitted_at: datetime,
    observed_end: datetime,
    cluster_end: datetime | None,
    cluster_source: str,
    clock_offset_seconds: float | None,
    poll_interval: float,
    last_submission: datetime | None = None,
    last_running: datetime | None = None,
) -> StageTiming:
    """Time a batch stage from its submission to the application's real end.

    *submitted_at* and *observed_end* are this host's clock, aware: the moment
    the SparkApplication was created and the poll that first saw it finish.
    *cluster_end* is the cluster's own end time for the application (see
    TIMING_DRIVER / TIMING_APPLICATION), mapped to this host's clock with
    *clock_offset_seconds* (cluster minus host). Without both, or when the
    mapped end falls outside [submitted_at, observed_end] by more than
    rounding (clock skew, or a status left from an earlier attempt: an end
    before *last_submission*), the poll time is used and the resolution is
    the poll interval.
    """
    poll = StageTiming(submitted_at, observed_end, TIMING_POLL, float(poll_interval))
    if cluster_end is None:
        return poll
    if clock_offset_seconds is None:
        poll.note = "cluster clock offset unknown"
        return poll
    if last_submission is not None and cluster_end < last_submission:
        poll.note = f"{cluster_source} precedes the last submission (stale status)"
        return poll
    end = cluster_end + timedelta(seconds=0.5 - clock_offset_seconds)
    tol = timedelta(seconds=_CLUSTER_TIMING_TOLERANCE_S)
    # Without a running poll to anchor on, one poll interval before the
    # observed end stands in for it.
    anchor = last_running or observed_end - timedelta(seconds=poll_interval)
    earliest = max(submitted_at - tol, anchor - timedelta(seconds=_OPERATOR_LAG_S))
    if end < earliest or end > observed_end + tol:
        poll.note = (
            f"{cluster_source} {cluster_end.isoformat()} is outside the observed run "
            f"({submitted_at.isoformat()} .. {observed_end.isoformat()}) after the "
            f"{clock_offset_seconds:+.1f}s clock offset"
        )
        return poll
    end = min(max(end, submitted_at), observed_end)
    return StageTiming(submitted_at, end, cluster_source, _CLUSTER_TIMING_RESOLUTION_S)


class SparkJobMonitor:
    """Monitors Spark job progress and completion."""

    def __init__(
        self,
        config: LakebenchConfig,
        k8s: K8sClient,
        job_manager: SparkJobManager | None = None,
    ):
        """Initialize Spark job monitor.

        Args:
            config: Lakebench configuration
            k8s: Kubernetes client
            job_manager: Optional shared job manager (a private one is
                built otherwise).
        """
        self.config = config
        self.k8s = k8s
        self.job_manager = job_manager or SparkJobManager(config, k8s)
        self.namespace = config.get_namespace()

    def wait_for_completion(
        self,
        job_name: str,
        timeout_seconds: int = 3600,
        poll_interval: int = 10,
        progress_callback: Callable[[JobStatus], None] | None = None,
        on_submission_failure: Callable[[dict[str, Any]], None] | None = None,
    ) -> JobResult:
        """Wait for a Spark job to complete.

        Args:
            job_name: Name of the SparkApplication
            timeout_seconds: Maximum wait time
            poll_interval: Seconds between status checks
            progress_callback: Optional callback for progress updates
            on_submission_failure: Called once per SUBMISSION_FAILED the
                operator reports (a new attempt or message), with the
                failure record, while the operator retries it. Every
                failure is also logged and returned in the JobResult.

        Returns:
            JobResult with final status
        """
        start = time.time()
        last_state = None
        sub_failed_since: float | None = None
        last_running: float | None = None
        seen_running = False
        state_since = start
        stall_warned: JobState | None = None
        failed_reads = 0
        sub_failures: list[dict[str, Any]] = []
        sub_open: tuple[dict[str, Any], float] | None = None  # (record, first seen)
        sub_key: tuple | None = None

        def _close_open_failure() -> None:
            nonlocal sub_open
            if sub_open is not None:
                record, seen = sub_open
                record["lost_seconds"] = round(time.time() - seen, 1)
                sub_open = None

        def _result(**kwargs: Any) -> JobResult:
            _close_open_failure()
            return JobResult(submission_failures=sub_failures, **kwargs)

        while True:
            elapsed = time.time() - start

            if elapsed > timeout_seconds:
                # Name the state we gave up in. A job stuck in UNKNOWN means
                # the operator reported something this client does not model,
                # which is a very different problem from a job that is
                # genuinely still working.
                stuck_in = last_state.value if last_state else "no state observed"
                return _result(
                    job_name=job_name,
                    success=False,
                    message=(
                        f"Job timed out after {timeout_seconds}s "
                        f"(last observed state: {stuck_in!r})"
                    ),
                    elapsed_seconds=elapsed,
                    # The whole log, as on success: the per-rule detection
                    # and stage-profile lines of a gold job that ran out of
                    # time are what show where the time went.
                    driver_logs=self._get_driver_logs(job_name, tail_lines=None),
                )

            read_started = time.time()
            try:
                status = self.job_manager.get_job_status(job_name)
            except Exception as e:
                if not is_transient_status_error(e):
                    raise
                failed_reads += 1
                logger.warning(
                    "status read for %s failed (%s: %s), %d in a row; polling on",
                    job_name,
                    type(e).__name__,
                    e,
                    failed_reads,
                )
                time.sleep(poll_interval)
                continue
            failed_reads = 0
            read_seconds = time.time() - read_started
            if read_seconds > _SLOW_POLL_S:
                logger.warning(
                    "status read for %s took %.0fs (poll interval %ss)",
                    job_name,
                    read_seconds,
                    poll_interval,
                )

            # Call progress callback on state change or while RUNNING
            if status.state != last_state or status.state == JobState.RUNNING:
                if status.state != last_state:
                    state_since = time.time()
                if progress_callback:
                    progress_callback(status)
                last_state = status.state

            if status.state == JobState.RUNNING:
                seen_running = True
                stall_warned = None
            elif (
                seen_running
                and status.state != stall_warned
                and status.state not in SUCCESS_STATES
                and status.state not in FAILURE_STATES
                and time.time() - state_since >= _STALL_WARN_S
            ):
                stall_warned = status.state
                logger.warning(
                    "%s was RUNNING and the operator has reported %s for %.0fs since; "
                    "the stage clock runs until the operator reports an end",
                    job_name,
                    status.state.value or "NEW",
                    time.time() - state_since,
                )

            # Check terminal states. SUCCEEDING and FAILING are terminal for
            # our purposes: the operator sets them as soon as the driver
            # finishes and only settles on COMPLETED/FAILED afterwards.
            # Waiting for the settled state alone risks reporting a finished
            # job as a timeout.
            if status.state in SUCCESS_STATES:
                return _result(
                    job_name=job_name,
                    success=True,
                    message="Job completed successfully",
                    elapsed_seconds=elapsed,
                    driver_logs=self._get_driver_logs(job_name, tail_lines=None),
                    final_status=status,
                    last_running_elapsed=last_running,
                )

            if status.state == JobState.SUBMISSION_FAILED:
                key = (status.submission_attempts, status.message)
                if key != sub_key:
                    # A new failure: the operator retried and failed again,
                    # or failed for the first time.
                    sub_key = key
                    _close_open_failure()
                    from lakebench.metrics.continuous_window import (
                        classify_submission_failure,
                    )

                    record: dict[str, Any] = {
                        "at": datetime.now(timezone.utc).isoformat(),
                        "attempt": status.submission_attempts or len(sub_failures) + 1,
                        "reason": classify_submission_failure(status.message),
                        "lost_seconds": None,
                    }
                    sub_failures.append(record)
                    sub_open = (record, time.time())
                    logger.warning(
                        "%s submission attempt %s failed: %s; the Spark Operator retries it",
                        job_name,
                        record["attempt"],
                        record["reason"],
                    )
                    if on_submission_failure is not None:
                        try:
                            on_submission_failure(dict(record))
                        except Exception as e:  # noqa: BLE001 -- reporting must not end the wait
                            logger.debug("submission-failure callback failed: %s", e)
                sub_failed_since = sub_failed_since or time.time()
                if time.time() - sub_failed_since < _SUBMISSION_FAILED_GRACE_S:
                    time.sleep(poll_interval)
                    continue
            else:
                sub_failed_since = None
                sub_key = None
                _close_open_failure()

            if status.state in FAILURE_STATES:
                # The whole log, as on success (see the timeout path).
                logs = self._get_driver_logs(job_name, tail_lines=None)
                message = f"Job failed: {status.message}"
                reason = classify_dependency_failure(logs) or self._init_failure(job_name)
                if reason:
                    message += f" ({reason})"
                return _result(
                    job_name=job_name,
                    success=False,
                    message=message,
                    elapsed_seconds=elapsed,
                    driver_logs=logs,
                    final_status=status,
                    last_running_elapsed=last_running,
                )

            last_running = elapsed
            time.sleep(poll_interval)

    def application_end(
        self, job_name: str, status: JobStatus | None
    ) -> tuple[datetime | None, str]:
        """The cluster's end time for a finished application, and its source.

        The driver container's terminated.finishedAt (kubelet, the current
        attempt's pod) is preferred; the SparkApplication's terminationTime
        is the fallback. (None, "") when neither is readable.
        """
        from kubernetes import client as k8s_client

        try:
            pods = k8s_client.CoreV1Api().list_namespaced_pod(
                self.namespace,
                label_selector=f"spark-role=driver,sparkoperator.k8s.io/app-name={job_name}",
            )
            for pod in pods.items or []:
                if (
                    status is not None
                    and status.driver_pod
                    and pod.metadata.name != status.driver_pod
                ):
                    continue
                for cs in (pod.status and pod.status.container_statuses) or []:
                    if cs.name != "spark-kubernetes-driver":
                        continue
                    term = cs.state and cs.state.terminated
                    finished = parse_k8s_time(term.finished_at) if term else None
                    if finished is not None:
                        return finished, TIMING_DRIVER
        except Exception as e:  # noqa: BLE001 -- best effort; falls back to the status
            logger.debug("driver pod end time for %s not read: %s", job_name, e)
        ended = parse_k8s_time(status.completion_time) if status is not None else None
        if ended is not None:
            return ended, TIMING_APPLICATION
        return None, ""

    def wait_until_running(
        self,
        job_name: str,
        timeout_seconds: int = 1800,
        poll_interval: int = 10,
        on_status: Callable[[JobStatus, float], None] | None = None,
    ) -> JobResult:
        """Wait until a long-lived (continuous) job's driver is running.

        Streaming jobs never complete, so wait_for_completion does not fit.
        SUBMISSION_FAILED is waited through (the operator retries); any
        settled failure, or a timeout, is returned as unsuccessful. ``success`` means RUNNING (or already
        finished successfully). *on_status* is called with every polled
        status and the seconds waited, so the caller can report submission
        failures while the operator retries them instead of waiting silently.
        """
        start = time.time()
        sub_failed_since: float | None = None
        while True:
            elapsed = time.time() - start
            status = self.job_manager.get_job_status(job_name)
            if on_status is not None:
                on_status(status, elapsed)
            if status.state == JobState.RUNNING or status.state in SUCCESS_STATES:
                return JobResult(
                    job_name=job_name,
                    success=True,
                    message=f"{job_name} running",
                    elapsed_seconds=elapsed,
                    final_status=status,
                )
            if status.state == JobState.SUBMISSION_FAILED:
                sub_failed_since = sub_failed_since or time.time()
                if time.time() - sub_failed_since < _SUBMISSION_FAILED_GRACE_S:
                    time.sleep(poll_interval)
                    continue
            else:
                sub_failed_since = None
            if status.state in FAILURE_STATES:
                return JobResult(
                    job_name=job_name,
                    success=False,
                    message=f"Job failed: {status.message}",
                    elapsed_seconds=elapsed,
                    driver_logs=self._get_driver_logs(job_name),
                )
            if elapsed > timeout_seconds:
                return JobResult(
                    job_name=job_name,
                    success=False,
                    message=(
                        f"{job_name} not running after {timeout_seconds}s "
                        f"(state: {status.state.value!r})"
                    ),
                    elapsed_seconds=elapsed,
                    driver_logs=self._get_driver_logs(job_name),
                )
            time.sleep(poll_interval)

    def _init_failure(self, job_name: str) -> str | None:
        """A driver init container that failed (the reference wheels install
        from the dependency server), with its last log line."""
        from kubernetes import client as k8s_client

        try:
            core_v1 = k8s_client.CoreV1Api()
            pods = core_v1.list_namespaced_pod(
                self.namespace,
                label_selector=f"spark-role=driver,sparkoperator.k8s.io/app-name={job_name}",
            ).items
            for pod in pods:
                for cs in (pod.status.init_container_statuses or []) if pod.status else []:
                    term = cs.state.terminated if cs.state else None
                    if term is not None and term.exit_code:
                        log = core_v1.read_namespaced_pod_log(
                            pod.metadata.name, self.namespace, container=cs.name, tail_lines=5
                        )
                        last = (str(log or "").strip().splitlines() or ["no log"])[-1]
                        return f"init container {cs.name} exited {term.exit_code}: {last[:200]}"
        except Exception as e:  # noqa: BLE001 -- diagnostics only
            logger.debug("cannot read %s init containers: %s", job_name, e)
        return None

    def _get_driver_logs(self, job_name: str, tail_lines: int | None = 100) -> str | None:
        """Get driver pod logs for debugging.

        Args:
            job_name: Name of the SparkApplication
            tail_lines: Number of lines to retrieve, or None for all logs

        Returns:
            Log content or None
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        core_v1 = k8s_client.CoreV1Api()

        try:
            # Find driver pod
            pods = core_v1.list_namespaced_pod(
                self.namespace,
                label_selector=f"spark-role=driver,sparkoperator.k8s.io/app-name={job_name}",
            )

            if not pods.items:
                return None

            pod_name = pods.items[0].metadata.name

            kwargs: dict = {
                "container": "spark-kubernetes-driver",
            }
            if tail_lines is not None:
                kwargs["tail_lines"] = tail_lines

            logs = core_v1.read_namespaced_pod_log(
                pod_name,
                self.namespace,
                **kwargs,
            )

            return logs

        except ApiException:
            return None

    def get_executor_metrics(self, job_name: str) -> dict:
        """Get metrics about job executors.

        Args:
            job_name: Name of the SparkApplication

        Returns:
            Dict with executor metrics
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        core_v1 = k8s_client.CoreV1Api()

        try:
            pods = core_v1.list_namespaced_pod(
                self.namespace,
                label_selector=f"spark-role=executor,sparkoperator.k8s.io/app-name={job_name}",
            )

            running = 0
            pending = 0
            failed = 0

            for pod in pods.items:
                phase = pod.status.phase
                if phase == "Running":
                    running += 1
                elif phase == "Pending":
                    pending += 1
                elif phase in ("Failed", "Unknown"):
                    failed += 1

            return {
                "total": len(pods.items),
                "running": running,
                "pending": pending,
                "failed": failed,
            }

        except ApiException:
            return {"total": 0, "running": 0, "pending": 0, "failed": 0}
