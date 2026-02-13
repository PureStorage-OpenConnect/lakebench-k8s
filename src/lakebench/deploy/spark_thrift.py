"""Spark Thrift Server deployment for Lakebench.

Deploys Spark Thrift Server (HiveThriftServer2) as a long-running
Deployment with spark-submit in client mode, exposed on port 10000.

The Spark Operator SparkApplication CRD does not support client mode,
and HiveThriftServer2 does not support cluster mode, so a standard
Kubernetes Deployment is used instead.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import yaml

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from .engine import DeploymentEngine


class SparkThriftDeployer:
    """Deploys the Spark Thrift Server as a Deployment."""

    TEMPLATES = [
        "spark-thrift/service.yaml.j2",
        "spark-thrift/sparkapplication.yaml.j2",
    ]

    def __init__(self, engine: DeploymentEngine):
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    def deploy(self) -> DeploymentResult:
        """Deploy the Spark Thrift Server.

        Skips deployment if ``query_engine.type`` is not ``spark-thrift``.
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.config.architecture.query_engine.type.value != "spark-thrift":
            return DeploymentResult(
                component="spark-thrift",
                status=DeploymentStatus.SKIPPED,
                message=(
                    f"Skipping Spark Thrift Server "
                    f"(query engine is {self.config.architecture.query_engine.type.value})"
                ),
                elapsed_seconds=0,
            )

        if self.engine.dry_run:
            return DeploymentResult(
                component="spark-thrift",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy Spark Thrift Server",
                elapsed_seconds=0,
            )

        try:
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, self.context)
                for doc in yaml.safe_load_all(yaml_content):
                    if doc:
                        self.k8s.apply_manifest(doc, namespace=namespace)

            # Wait for the Deployment to become ready
            self._wait_for_ready(namespace, timeout_seconds=300)

            return DeploymentResult(
                component="spark-thrift",
                status=DeploymentStatus.SUCCESS,
                message="Spark Thrift Server deployed",
                elapsed_seconds=time.time() - start,
            )

        except Exception as e:
            return DeploymentResult(
                component="spark-thrift",
                status=DeploymentStatus.FAILED,
                message=f"Spark Thrift Server deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _wait_for_ready(self, namespace: str, timeout_seconds: int = 300) -> None:
        """Wait for the Spark Thrift Server Deployment to have ready replicas."""
        from kubernetes import client as k8s_client

        apps_api = k8s_client.AppsV1Api()
        deadline = time.time() + timeout_seconds

        while time.time() < deadline:
            try:
                dep = apps_api.read_namespaced_deployment(
                    name="lakebench-spark-thrift",
                    namespace=namespace,
                )
                ready = dep.status.ready_replicas or 0
                desired = dep.spec.replicas or 1
                if ready >= desired:
                    return
            except k8s_client.rest.ApiException as e:
                if e.status != 404:
                    raise
            time.sleep(10)

        raise RuntimeError(f"Spark Thrift Server did not become ready within {timeout_seconds}s")
