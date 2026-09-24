"""Spark Job monitoring for Lakebench.

Provides waiting and progress tracking for Spark jobs.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lakebench.modules.pipeline_engines.spark.job import (
    FAILURE_STATES,
    SUCCESS_STATES,
    JobState,
    JobStatus,
    SparkJobManager,
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
_SUBMISSION_FAILED_GRACE_S = 900


@dataclass
class JobResult:
    """Final result of a Spark job."""

    job_name: str
    success: bool
    message: str
    elapsed_seconds: float
    driver_logs: str | None = None


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
    ) -> JobResult:
        """Wait for a Spark job to complete.

        Args:
            job_name: Name of the SparkApplication
            timeout_seconds: Maximum wait time
            poll_interval: Seconds between status checks
            progress_callback: Optional callback for progress updates

        Returns:
            JobResult with final status
        """
        start = time.time()
        last_state = None
        sub_failed_since: float | None = None

        while True:
            elapsed = time.time() - start

            if elapsed > timeout_seconds:
                # Name the state we gave up in. A job stuck in UNKNOWN means
                # the operator reported something this client does not model,
                # which is a very different problem from a job that is
                # genuinely still working.
                stuck_in = last_state.value if last_state else "no state observed"
                return JobResult(
                    job_name=job_name,
                    success=False,
                    message=(
                        f"Job timed out after {timeout_seconds}s "
                        f"(last observed state: {stuck_in!r})"
                    ),
                    elapsed_seconds=elapsed,
                    driver_logs=self._get_driver_logs(job_name),
                )

            status = self.job_manager.get_job_status(job_name)

            # Call progress callback on state change or while RUNNING
            if status.state != last_state or status.state == JobState.RUNNING:
                if progress_callback:
                    progress_callback(status)
                last_state = status.state

            # Check terminal states. SUCCEEDING and FAILING are terminal for
            # our purposes: the operator sets them as soon as the driver
            # finishes and only settles on COMPLETED/FAILED afterwards.
            # Waiting for the settled state alone risks reporting a finished
            # job as a timeout.
            if status.state in SUCCESS_STATES:
                return JobResult(
                    job_name=job_name,
                    success=True,
                    message="Job completed successfully",
                    elapsed_seconds=elapsed,
                    driver_logs=self._get_driver_logs(job_name, tail_lines=None),
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

            time.sleep(poll_interval)

    def wait_until_running(
        self,
        job_name: str,
        timeout_seconds: int = 900,
        poll_interval: int = 10,
    ) -> JobResult:
        """Wait until a long-lived (continuous) job's driver is running.

        Streaming jobs never complete, so wait_for_completion does not fit.
        SUBMISSION_FAILED is waited through (the operator retries); any
        settled failure, or a timeout, is returned as unsuccessful. ``success`` means RUNNING (or already
        finished successfully).
        """
        start = time.time()
        sub_failed_since: float | None = None
        while True:
            elapsed = time.time() - start
            status = self.job_manager.get_job_status(job_name)
            if status.state == JobState.RUNNING or status.state in SUCCESS_STATES:
                return JobResult(
                    job_name=job_name,
                    success=True,
                    message=f"{job_name} running",
                    elapsed_seconds=elapsed,
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
