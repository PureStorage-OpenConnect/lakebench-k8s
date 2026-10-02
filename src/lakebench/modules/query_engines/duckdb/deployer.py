"""DuckDB query engine deployment for Lakebench.

Deploys DuckDB as a single-pod Deployment with ``sleep infinity``.
Queries are executed via ``kubectl exec`` into the DuckDB CLI.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import yaml

from lakebench.deploy import deadline as deploy_deadline
from lakebench.deploy.engine import DeploymentResult, DeploymentStatus, image_tag

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from lakebench.deploy.engine import DeploymentEngine


class DuckDBDeployer:
    """Deploys the DuckDB query engine as a Deployment."""

    TEMPLATES = [
        "duckdb/deployment.yaml.j2",
        "duckdb/service.yaml.j2",
    ]

    def __init__(self, engine: DeploymentEngine):
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    def deploy(self) -> DeploymentResult:
        """Deploy the DuckDB query engine."""
        start = time.time()
        namespace = self.config.get_namespace()

        if self.config.architecture.query_engine.type.value != "duckdb":
            return DeploymentResult(
                component="duckdb",
                status=DeploymentStatus.SKIPPED,
                message=(
                    f"Skipping DuckDB "
                    f"(query engine is {self.config.architecture.query_engine.type.value})"
                ),
                elapsed_seconds=0,
            )

        if self.engine.dry_run:
            return DeploymentResult(
                component="duckdb",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy DuckDB",
                elapsed_seconds=0,
            )

        try:
            from lakebench.deploy.deps import consumer_context

            context = consumer_context(self.engine, "DuckDB")
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, context)
                for doc in yaml.safe_load_all(yaml_content):
                    if doc:
                        self.k8s.apply_manifest(doc, namespace=namespace)

            self._wait_for_ready(namespace, timeout_seconds=deploy_deadline.clamp(900))

            duckdb_version = image_tag(self.config.images.duckdb)
            return DeploymentResult(
                component="duckdb",
                status=DeploymentStatus.SUCCESS,
                message=f"DuckDB deployed (image {duckdb_version})",
                elapsed_seconds=time.time() - start,
                label="DuckDB",
                detail=duckdb_version,
            )

        except Exception as e:
            logger.exception("DuckDB deployment failed")
            return DeploymentResult(
                component="duckdb",
                status=DeploymentStatus.FAILED,
                message=f"DuckDB deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _wait_for_ready(self, namespace: str, timeout_seconds: float = 300) -> None:
        """Wait until the DuckDB Deployment has rolled out and its pod runs
        this deploy's dependency set and is Ready.

        Readiness alone cannot say *why* a pod is not ready, so a timeout
        names the pod state (crash loop, unschedulable, never created).
        """
        from lakebench.deploy.deps import wait_consumer_rolled

        handle = self.engine.deps
        try:
            wait_consumer_rolled(
                namespace,
                "lakebench-duckdb",
                handle.pinset_sha256 if handle is not None else None,
                timeout_seconds=timeout_seconds,
                what="DuckDB",
            )
        except RuntimeError as e:
            raise RuntimeError(f"{e}. {self._describe_not_ready(namespace)}") from e

    def _describe_not_ready(self, namespace: str) -> str:
        """Best-effort explanation of why the DuckDB pod is not ready.

        Diagnostics must never mask the original timeout, so every failure
        here degrades to a note rather than an exception.
        """
        from kubernetes import client as k8s_client

        try:
            core_api = k8s_client.CoreV1Api()
            pods = core_api.list_namespaced_pod(
                namespace=namespace,
                label_selector=(
                    "app.kubernetes.io/name=lakebench,app.kubernetes.io/component=duckdb"
                ),
            ).items
        except Exception as e:  # noqa: BLE001 - diagnostics only
            return f"Could not inspect pods to explain the failure: {e}"

        if not pods:
            return (
                "No DuckDB pod was created. Check the Deployment's events -- "
                "this usually means the pod could not be scheduled or was "
                "rejected by a security context constraint."
            )

        notes: list[str] = []
        for pod in pods:
            name = pod.metadata.name
            phase = pod.status.phase
            for cs in pod.status.container_statuses or []:
                waiting = cs.state.waiting
                terminated = cs.state.terminated
                if waiting and waiting.reason:
                    notes.append(
                        f"{name}: waiting ({waiting.reason}: {waiting.message or 'no detail'})"
                    )
                elif terminated:
                    notes.append(
                        f"{name}: terminated ({terminated.reason}, exit {terminated.exit_code})"
                    )
                elif not cs.ready:
                    # Running but failing its probe. Restarts distinguish a
                    # slow start from the probe repeatedly killing it.
                    notes.append(
                        f"{name}: running but not ready after "
                        f"{cs.restart_count} restart(s) -- the startup probe "
                        f"is likely still failing"
                    )
            if not notes:
                notes.append(f"{name}: phase {phase}")

        return " | ".join(notes)
