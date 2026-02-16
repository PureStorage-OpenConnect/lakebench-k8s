"""Spark Job monitoring for Lakebench.

Provides waiting and progress tracking for Spark jobs.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from .job import JobState, JobStatus, SparkJobManager

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig
    from lakebench.k8s import K8sClient

logger = logging.getLogger(__name__)


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

    def __init__(self, config: LakebenchConfig, k8s: K8sClient):
        """Initialize Spark job monitor.

        Args:
            config: Lakebench configuration
            k8s: Kubernetes client
        """
        self.config = config
        self.k8s = k8s
        self.job_manager = SparkJobManager(config, k8s)
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

        while True:
            elapsed = time.time() - start

            if elapsed > timeout_seconds:
                return JobResult(
                    job_name=job_name,
                    success=False,
                    message=f"Job timed out after {timeout_seconds}s",
                    elapsed_seconds=elapsed,
                    driver_logs=self._get_driver_logs(job_name),
                )

            status = self.job_manager.get_job_status(job_name)

            # Call progress callback on state change or while RUNNING
            if status.state != last_state or status.state == JobState.RUNNING:
                if progress_callback:
                    progress_callback(status)
                last_state = status.state

            # Check terminal states
            if status.state == JobState.COMPLETED:
                return JobResult(
                    job_name=job_name,
                    success=True,
                    message="Job completed successfully",
                    elapsed_seconds=elapsed,
                    driver_logs=self._get_driver_logs(job_name, tail_lines=None),
                )

            if status.state == JobState.FAILED:
                return JobResult(
                    job_name=job_name,
                    success=False,
                    message=f"Job failed: {status.message}",
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
