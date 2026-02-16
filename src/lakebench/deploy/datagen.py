"""Data generation deployment for Lakebench."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

import yaml

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from .engine import DeploymentEngine


class DatagenDeployer:
    """Deploys and monitors the datagen job."""

    TEMPLATES = ["datagen/job.yaml.j2"]

    def __init__(self, engine: DeploymentEngine):
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    _PAYLOAD_SIZE_BYTES = 2048  # 2KB hex payloads for compression profiling

    def _build_datagen_context(self) -> dict[str, Any]:
        """Build context for datagen template."""
        cfg = self.config
        workload = cfg.architecture.workload
        datagen = workload.datagen
        medallion = cfg.architecture.pipeline.medallion

        # Derive dimensions from scale factor
        dims = cfg.get_scale_dimensions()
        file_size_bytes = self._parse_size_to_bytes(datagen.file_size)
        file_size_mb = file_size_bytes // (1024 * 1024)

        # Target in TB for CLI args interface
        target_tb = dims.approx_bronze_gb / 1024.0

        # Resolve effective mode (auto → batch/continuous)
        from lakebench.config.autosizer import _resolve_datagen_mode

        effective_mode = _resolve_datagen_mode(cfg)

        context = dict(self.context)  # Copy base context
        context.update(
            {
                "datagen_parallelism": datagen.parallelism,
                "datagen_target_tb": f"{target_tb:.6f}",
                "datagen_file_size_mb": file_size_mb,
                "datagen_payload_kb": self._PAYLOAD_SIZE_BYTES // 1024,
                "datagen_path_prefix": medallion.bronze.path_template,
                "datagen_seed": 42,  # Fixed seed for reproducibility
                "datagen_resume": False,  # Can be overridden
                "datagen_cpu": datagen.cpu,
                "datagen_memory": datagen.memory,
                "datagen_mode": effective_mode,
                "datagen_dirty_ratio": datagen.dirty_data_ratio,
                "datagen_image": cfg.images.datagen,
                "datagen_timestamp_start": datagen.timestamp_start,
                "datagen_timestamp_end": datagen.timestamp_end,
            }
        )
        return context

    def _parse_size_to_bytes(self, size_str: str) -> int:
        """Parse size string to bytes.

        Args:
            size_str: Size string (e.g., "100GB", "1TB")

        Returns:
            Size in bytes
        """
        size_str = size_str.strip().upper()
        multipliers = {
            "B": 1,
            "KB": 1024,
            "MB": 1024**2,
            "GB": 1024**3,
            "TB": 1024**4,
        }

        for suffix, multiplier in sorted(multipliers.items(), key=lambda x: -len(x[0])):
            if size_str.endswith(suffix):
                value = float(size_str[: -len(suffix)])
                return int(value * multiplier)

        # Assume bytes if no suffix
        return int(size_str)

    def deploy(self, resume: bool = False) -> DeploymentResult:
        """Deploy the datagen job.

        Args:
            resume: If True, enable checkpoint resume

        Returns:
            DeploymentResult with status
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.engine.dry_run:
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy datagen job",
                elapsed_seconds=0,
            )

        try:
            context = self._build_datagen_context()
            context["datagen_resume"] = resume

            self._delete_existing_job(namespace)

            # Render and apply job template
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, context)
                manifest = yaml.safe_load(yaml_content)
                self.k8s.apply_manifest(manifest, namespace=namespace)

            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.SUCCESS,
                message="Datagen job submitted",
                elapsed_seconds=time.time() - start,
                details={
                    "job_name": "lakebench-datagen",
                    "namespace": namespace,
                    "parallelism": context["datagen_parallelism"],
                    "target_tb": context["datagen_target_tb"],
                },
            )

        except Exception as e:
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.FAILED,
                message=f"Datagen job submission failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _delete_existing_job(self, namespace: str) -> None:
        """Delete existing datagen job if present."""
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        batch_v1 = k8s_client.BatchV1Api()

        try:
            batch_v1.delete_namespaced_job(
                name="lakebench-datagen",
                namespace=namespace,
                body=k8s_client.V1DeleteOptions(propagation_policy="Background"),
            )
            # Wait for job to be deleted
            time.sleep(2)
        except ApiException as e:
            if e.status != 404:
                raise

    def wait_for_completion(
        self,
        timeout_seconds: int = 7200,
        poll_interval: int = 30,
    ) -> DeploymentResult:
        """Wait for datagen job to complete.

        Args:
            timeout_seconds: Maximum wait time (default 2 hours)
            poll_interval: Seconds between status checks

        Returns:
            DeploymentResult with completion status
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        start = time.time()
        namespace = self.config.get_namespace()
        batch_v1 = k8s_client.BatchV1Api()

        while time.time() - start < timeout_seconds:
            try:
                job = batch_v1.read_namespaced_job_status(
                    name="lakebench-datagen",
                    namespace=namespace,
                )

                status = job.status
                completions = job.spec.completions or 1
                succeeded = status.succeeded or 0
                failed = status.failed or 0
                active = status.active or 0

                if succeeded >= completions:
                    return DeploymentResult(
                        component="datagen",
                        status=DeploymentStatus.SUCCESS,
                        message=f"Datagen completed: {succeeded}/{completions} pods succeeded",
                        elapsed_seconds=time.time() - start,
                        details={
                            "succeeded": succeeded,
                            "failed": failed,
                            "completions": completions,
                        },
                    )

                # Check for failure
                if failed > 0 and active == 0:
                    return DeploymentResult(
                        component="datagen",
                        status=DeploymentStatus.FAILED,
                        message=f"Datagen failed: {failed} pods failed",
                        elapsed_seconds=time.time() - start,
                        details={
                            "succeeded": succeeded,
                            "failed": failed,
                            "completions": completions,
                        },
                    )

                # Still running
                time.sleep(poll_interval)

            except ApiException as e:
                if e.status == 404:
                    return DeploymentResult(
                        component="datagen",
                        status=DeploymentStatus.FAILED,
                        message="Datagen job not found",
                        elapsed_seconds=time.time() - start,
                    )
                raise

        return DeploymentResult(
            component="datagen",
            status=DeploymentStatus.FAILED,
            message=f"Datagen timed out after {timeout_seconds}s",
            elapsed_seconds=time.time() - start,
        )

    def get_progress(self) -> dict[str, Any]:
        """Get current progress of datagen job.

        Returns:
            Dict with progress information
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        namespace = self.config.get_namespace()
        batch_v1 = k8s_client.BatchV1Api()
        core_v1 = k8s_client.CoreV1Api()

        try:
            job = batch_v1.read_namespaced_job_status(
                name="lakebench-datagen",
                namespace=namespace,
            )

            status = job.status
            completions = job.spec.completions or 1
            succeeded = status.succeeded or 0
            failed = status.failed or 0
            active = status.active or 0

            # Get pod logs for progress
            pods = core_v1.list_namespaced_pod(
                namespace,
                label_selector="app=lakebench-datagen",
            )

            pod_status = []
            oom_pods: list[str] = []
            crash_pods: list[str] = []
            pending_pods: list[str] = []
            for pod in pods.items:
                pod_name = pod.metadata.name
                pod_info = {
                    "name": pod_name,
                    "phase": pod.status.phase,
                    "index": pod.metadata.annotations.get(
                        "batch.kubernetes.io/job-completion-index", "?"
                    ),
                }
                pod_status.append(pod_info)

                # Detect OOM and crash-looping containers
                if pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        terminated = cs.last_state and cs.last_state.terminated
                        if terminated and terminated.reason == "OOMKilled":
                            oom_pods.append(pod_name)
                        elif cs.restart_count and cs.restart_count >= 3:
                            crash_pods.append(pod_name)

                # Detect pending pods
                if pod.status.phase == "Pending":
                    pending_pods.append(pod_name)

            return {
                "running": active > 0 or (succeeded < completions and failed == 0),
                "succeeded": succeeded,
                "failed": failed,
                "active": active,
                "completions": completions,
                "progress_pct": (succeeded / completions * 100) if completions > 0 else 0,
                "pods": pod_status,
                "oom_pods": oom_pods,
                "crash_pods": crash_pods,
                "pending_pods": pending_pods,
            }

        except ApiException as e:
            if e.status == 404:
                return {"running": False, "error": "Job not found"}
            raise
