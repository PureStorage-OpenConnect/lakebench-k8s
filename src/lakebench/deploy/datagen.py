"""Data generation deployment for Lakebench."""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import yaml

from lakebench.config.datagen_seed import config_perturbation, config_seed

from .engine import DeploymentResult, DeploymentStatus

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig

    from .engine import DeploymentEngine


def bronze_datagen_prefix(config: LakebenchConfig) -> str:
    """Return the S3 prefix under bronze where datagen writes.

    Kept in one place so the deployer, the CLI's ``--regenerate`` guard and
    tests all read the same mapping. Financial (AML) datagen writes under
    ``pacs008/`` on the C360 default ``customer/interactions/`` template
    (see ``_build_datagen_context``); every other schema writes under
    ``bronze.path_template`` unchanged.
    """
    schema_value = config.architecture.workload.schema_type.value
    path_prefix = config.architecture.pipeline.medallion.bronze.path_template
    if schema_value == "financial" and path_prefix == "customer/interactions":
        path_prefix = "pacs008"
    return path_prefix


class DatagenDeployer:
    """Deploys and monitors the datagen job."""

    TEMPLATES = ["datagen/job.yaml.j2"]

    def __init__(self, engine: DeploymentEngine):
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    # Payload size was a template context knob feeding --payload-kb; dropped
    # with the CLI arg 2026-09-28 (LB-191 companion). The Rust binary now
    # hardcodes 2 KiB. Kept here as documentation of what the fixed value is.
    _PAYLOAD_SIZE_BYTES = 2048  # 2 KiB hex payloads for compression profiling

    def _build_datagen_context(self) -> dict[str, Any]:
        """Build context for datagen template."""
        cfg = self.config
        workload = cfg.architecture.workload
        datagen = workload.datagen

        # Derive dimensions from scale factor
        dims = cfg.get_scale_dimensions()
        file_size_bytes = self._parse_size_to_bytes(datagen.file_size)
        file_size_mb = file_size_bytes // (1024 * 1024)

        # Target in TB for CLI args interface
        target_tb = dims.approx_bronze_gb / 1024.0

        # Resolve effective mode (auto → batch/continuous)
        from lakebench.config.autosizer import _resolve_datagen_mode

        effective_mode = _resolve_datagen_mode(cfg)

        # Route datagen to the right Generator + schema-appropriate S3 prefix.
        # When path_template is still the C360 default and schema=financial,
        # substitute the pacs.008 prefix the Financial Spark scripts read from
        # (LB_FINANCIAL_BRONZE_PREFIX default). Keeps the datagen upload and
        # bronze_verify_financial.py pointed at the same S3 location.
        schema_value = workload.schema_type.value
        path_prefix = bronze_datagen_prefix(cfg)

        context = dict(self.context)  # Copy base context
        context.update(
            {
                "datagen_parallelism": datagen.parallelism,
                "datagen_target_tb": f"{target_tb:.6f}",
                # Explicit lakebench scale factor (1, 5, 10, ...) -- schema
                # generators like FinancialGenerator use this directly for
                # typology instance counts. Not derivable from target_tb
                # inside the container without assuming ~10GB/scale.
                "datagen_scale_factor": f"{datagen.get_effective_scale():.6f}",
                # c360 customer id space: scale-derived (100K per scale unit)
                # or customer360.unique_customers. From the TOTAL scale, so
                # every multi-cycle cycle draws from the same customers.
                "datagen_customer_id_max": dims.customers,
                "datagen_file_size_mb": file_size_mb,
                # datagen_payload_kb context var dropped 2026-09-28; template
                # no longer renders --payload-kb; Rust hardcodes 2 KiB.
                "datagen_path_prefix": path_prefix,
                "datagen_schema": schema_value,
                # From config, or the pre-registration's calibration seed for
                # financial (config/datagen_seed.py); spent seeds are refused.
                "datagen_seed": config_seed(cfg),
                # Robustness corpus flag (financial only), checked against
                # the declared corpus role (config/datagen_seed.py).
                "datagen_robustness_perturbation": config_perturbation(cfg),
                "datagen_cpu": datagen.cpu,
                "datagen_memory": datagen.memory,
                "datagen_mode": effective_mode,
                "datagen_workers": datagen.generators,
                "datagen_dirty_ratio": datagen.dirty_data_ratio,
                "datagen_image": cfg.images.datagen,
                "datagen_timestamp_start": datagen.timestamp_start,
                "datagen_timestamp_end": datagen.timestamp_end,
            }
        )

        # datagen_duration context var dropped 2026-09-28 (Wave 2 D8). The
        # entrypoint.py argparse layer silently discarded --duration; the Rust
        # binary never read it; a continuous / sustained pipeline is driven by
        # the pipeline side trickle-reading a pre-written finite corpus (LB-156)
        # or, in a future release, by a producer-driven delivery mode
        # (--delivery-mode). Passing --duration was misleading.

        # Live observability: point datagen pods at the per-deployment
        # Pushgateway. Best-effort -- the Rust binary no-ops when the env is
        # unset. Only when observability + the pushgateway are enabled. LB_RUN_ID
        # is shared across datagen and the Spark stages via the orchestrator's
        # env (set by the run flow) so the dashboard can correlate a run and a
        # re-run does not read as the previous run's series.
        obs = cfg.observability
        if obs.enabled and obs.pushgateway_enabled:
            namespace = cfg.get_namespace()
            context["pushgateway_url"] = f"http://lakebench-pushgateway.{namespace}.svc:9091"
            context["run_id"] = os.environ.get("LB_RUN_ID", "")

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

    @staticmethod
    def _cycle_timestamp_range(
        cycle_index: int,
        total_cycles: int,
        timestamp_start: str | None = None,
        timestamp_end: str | None = None,
    ) -> tuple[str, str]:
        """Compute the timestamp window for a given cycle.

        Divides the configured date range evenly across cycles.
        Non-overlapping, chronologically ordered.  Last cycle gets remainder.
        """
        start = datetime.strptime(timestamp_start or "2024-01-01", "%Y-%m-%d")
        end = datetime.strptime(timestamp_end or "2025-12-31", "%Y-%m-%d")
        total_days = (end - start).days
        days_per_cycle = total_days // total_cycles

        cycle_start = start + timedelta(days=days_per_cycle * cycle_index)
        if cycle_index == total_cycles - 1:
            cycle_end = end
        else:
            cycle_end = start + timedelta(days=days_per_cycle * (cycle_index + 1))

        return cycle_start.strftime("%Y-%m-%d"), cycle_end.strftime("%Y-%m-%d")

    def deploy_cycle(
        self,
        cycle_index: int,
        total_cycles: int,
    ) -> DeploymentResult:
        """Deploy datagen for a specific batch cycle.

        Overrides the timestamp range and scale to produce a per-cycle
        slice of the total data.  Data appends to the same bronze bucket.
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.engine.dry_run:
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.SUCCESS,
                message=f"Would deploy datagen job for cycle {cycle_index + 1}/{total_cycles}",
                elapsed_seconds=0,
            )

        try:
            context = self._build_datagen_context()

            # Per-cycle timestamp window
            datagen = self.config.architecture.workload.datagen
            ts_start, ts_end = self._cycle_timestamp_range(
                cycle_index,
                total_cycles,
                datagen.timestamp_start,
                datagen.timestamp_end,
            )
            context["datagen_timestamp_start"] = ts_start
            context["datagen_timestamp_end"] = ts_end
            # Each cycle gets its own seed stream / time slice and object keys
            # (WORKPLAN B4); without these every cycle rewrote cycle 0's files
            # with the same seed.
            context["datagen_cycle"] = cycle_index
            context["datagen_cycles"] = total_cycles

            # Per-cycle scale (divide total evenly, minimum 1)
            total_scale = datagen.get_effective_scale()
            scale_per_cycle = max(1, total_scale // total_cycles)
            dims = self.config.get_scale_dimensions()
            target_tb = (dims.approx_bronze_gb / total_cycles) / 1024.0
            context["datagen_target_tb"] = f"{target_tb:.6f}"

            self._delete_existing_job(namespace)

            # Cycle 0 is a fresh write: clear stale files a prior generate left,
            # after the previous job is gone so nothing writes mid-clear. Append
            # cycles (n > 0) keep the earlier cycles' files (LB-185).
            self._clear_bronze_prefix_if_fresh(cycle_index, context["datagen_path_prefix"])

            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, context)
                manifest = yaml.safe_load(yaml_content)
                self.k8s.apply_manifest(manifest, namespace=namespace)

            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.SUCCESS,
                message=f"Datagen cycle {cycle_index + 1}/{total_cycles} submitted ({ts_start} to {ts_end})",
                elapsed_seconds=time.time() - start,
                details={
                    "job_name": "lakebench-datagen",
                    "namespace": namespace,
                    "cycle_index": cycle_index,
                    "total_cycles": total_cycles,
                    "timestamp_start": ts_start,
                    "timestamp_end": ts_end,
                    "scale_per_cycle": scale_per_cycle,
                    "target_tb": context["datagen_target_tb"],
                },
            )

        except Exception as e:
            logger.exception("Datagen cycle deployment failed")
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.FAILED,
                message=f"Datagen cycle {cycle_index + 1} failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _bronze_bucket_is_owned(self, bucket: str) -> bool:
        """True only when this deployment's namespace records creating ``bucket``.

        The clear below is destructive, so it must respect the same ownership
        model as destroy and clean: on FlashBlade bucket tagging is unsupported
        (LB-088), so the namespace's created-buckets record is the authoritative
        proof of ownership. Fail-safe: any uncertainty (record unreadable,
        namespace absent, bucket not listed) returns False, so a generate into a
        shared, adopted or foreign bronze bucket never deletes another
        deployment's data (invariant 4). `lakebench generate` calls the deployer
        directly and never runs the deploy engine's bucket-ownership guard, so
        this check is the only thing standing between the clear and foreign data.
        """
        try:
            from kubernetes import client as k8s_client

            from lakebench.deploy.ownership import read_created_buckets

            core_v1 = k8s_client.CoreV1Api()
            namespace = self.config.get_namespace()
            return bucket in read_created_buckets(core_v1, namespace)
        except Exception as e:  # noqa: BLE001
            logger.info(
                "LB-185: could not confirm this deployment created %s (%s); not clearing it",
                bucket,
                e,
            )
            return False

    def _clear_bronze_prefix_if_fresh(self, cycle_index: int, path_prefix: str) -> None:
        """Clear stale datagen files before a fresh generate (LB-185).

        A re-generate into a reused bronze bucket must not inherit part-* files
        a larger earlier generate left behind: they share the ``part-NNNNNN``
        naming, so the bronze read cannot tell them apart and silver over-counts
        (a smaller generate over a larger one's leftovers). Clear the workload's
        bronze prefix before cycle 0 writes; append cycles (n > 0) keep the
        earlier cycles' files. Skipped when the operator manages the bucket
        (``create_buckets`` false) or when this deployment cannot prove it
        created the bucket (invariant 4). Scoped to the datagen prefix via
        ``delete_prefix``, which refuses an empty or root prefix.

        For an owned bucket the clear MUST succeed: a half-cleared prefix leaves
        stale files that silver over-counts, the exact LB-185 bug, so a clearing
        failure raises and fails the generate rather than proceeding with a PASS
        on wrong data (invariant 3).
        """
        if cycle_index != 0:
            return
        s3_cfg = self.config.platform.storage.s3
        if not s3_cfg.create_buckets:
            logger.info(
                "LB-185: create_buckets is false; leaving the bronze prefix for "
                "the operator to manage"
            )
            return
        prefix = path_prefix.strip("/")
        if not prefix:
            logger.warning("LB-185: datagen path prefix is empty; not clearing the bronze bucket")
            return
        bucket = s3_cfg.buckets.bronze
        if not self._bronze_bucket_is_owned(bucket):
            logger.info(
                "LB-185: %s is not recorded as created by this deployment; not clearing it "
                "(a re-generate into a reused bucket may inherit stale files)",
                bucket,
            )
            return
        # Owned: the prefix must be empty before datagen writes. Any failure
        # propagates so deploy()/deploy_cycle() report FAILED rather than
        # generating over a half-cleared prefix.
        from lakebench.s3 import S3Client

        s3 = S3Client(
            endpoint=s3_cfg.endpoint,
            access_key=s3_cfg.access_key,
            secret_key=s3_cfg.secret_key,
            region=s3_cfg.region,
            path_style=s3_cfg.path_style,
            ca_cert=s3_cfg.ca_cert,
            verify_ssl=s3_cfg.verify_ssl,
        )
        if s3._init_error:
            raise RuntimeError(
                f"LB-185: cannot clear the bronze prefix s3://{bucket}/{prefix} before a "
                f"fresh generate: {s3._init_error}"
            )
        n = s3.delete_prefix(bucket, prefix)
        if n:
            logger.info(
                "LB-185: cleared %d stale object(s) under s3://%s/%s before a fresh generate",
                n,
                bucket,
                prefix,
            )

    def deploy(self) -> DeploymentResult:
        """Deploy the datagen job.

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

            self._delete_existing_job(namespace)

            # A single-cycle generate is a fresh write: clear stale files after
            # the previous job is gone, so nothing writes into the prefix
            # mid-clear.
            self._clear_bronze_prefix_if_fresh(0, context["datagen_path_prefix"])

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
            logger.exception("Datagen deployment failed")
            return DeploymentResult(
                component="datagen",
                status=DeploymentStatus.FAILED,
                message=f"Datagen job submission failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _delete_existing_job(
        self, namespace: str, *, request_timeout: int | None = None
    ) -> None:
        """Delete existing datagen job if present.

        ``request_timeout`` caps the API call so a caller invoked because
        the K8s API itself hung (datagen timeout handler) does not block
        indefinitely on the delete.
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        batch_v1 = k8s_client.BatchV1Api()

        try:
            kwargs: dict = {
                "name": "lakebench-datagen",
                "namespace": namespace,
                "body": k8s_client.V1DeleteOptions(propagation_policy="Background"),
            }
            if request_timeout is not None:
                kwargs["_request_timeout"] = request_timeout
            batch_v1.delete_namespaced_job(**kwargs)
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
            crash_details: dict[str, str] = {}
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
                        elif (
                            cs.restart_count
                            and cs.restart_count >= 3
                            and pod.status.phase != "Succeeded"
                            and cs.state is not None
                            and cs.state.waiting is not None
                            and cs.state.waiting.reason == "CrashLoopBackOff"
                        ):
                            # restart_count never resets, so require the pod to
                            # be crash-looping NOW: one that recovered after a
                            # few transient failures is still making progress.
                            crash_pods.append(pod_name)
                            if terminated:
                                crash_details[pod_name] = f"exit {terminated.exit_code}" + (
                                    f" ({terminated.reason})" if terminated.reason else ""
                                )

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
                "crash_details": crash_details,
                "pending_pods": pending_pods,
            }

        except ApiException as e:
            if e.status == 404:
                return {"running": False, "error": "Job not found"}
            raise
