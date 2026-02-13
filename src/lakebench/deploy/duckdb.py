"""DuckDB query engine deployment for Lakebench.

Deploys DuckDB as a single-pod Deployment with ``sleep infinity``.
Queries are executed via ``kubectl exec`` into the DuckDB CLI
(Python + duckdb module).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import yaml

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from .engine import DeploymentEngine


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
        """Deploy the DuckDB query engine.

        Skips deployment if ``query_engine.type`` is not ``duckdb``.
        """
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
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, self.context)
                for doc in yaml.safe_load_all(yaml_content):
                    if doc:
                        self.k8s.apply_manifest(doc, namespace=namespace)

            self._wait_for_ready(namespace, timeout_seconds=180)

            return DeploymentResult(
                component="duckdb",
                status=DeploymentStatus.SUCCESS,
                message="DuckDB deployed",
                elapsed_seconds=time.time() - start,
            )

        except Exception as e:
            return DeploymentResult(
                component="duckdb",
                status=DeploymentStatus.FAILED,
                message=f"DuckDB deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _wait_for_ready(self, namespace: str, timeout_seconds: int = 180) -> None:
        """Wait for the DuckDB Deployment to have ready replicas."""
        from kubernetes import client as k8s_client

        apps_api = k8s_client.AppsV1Api()
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            try:
                dep = apps_api.read_namespaced_deployment(
                    name="lakebench-duckdb",
                    namespace=namespace,
                )
                ready = dep.status.ready_replicas or 0
                desired = dep.spec.replicas or 1
                if ready >= desired:
                    return
            except k8s_client.rest.ApiException as e:
                if e.status != 404:
                    raise
            time.sleep(5)

        raise RuntimeError(f"DuckDB did not become ready within {timeout_seconds}s")
