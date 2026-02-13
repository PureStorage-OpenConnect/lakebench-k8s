"""Trino deployment for Lakebench.

Deploys Trino coordinator and workers as the query engine.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import yaml

from lakebench.k8s import (
    WaitStatus,
    wait_for_deployment_ready,
    wait_for_statefulset_ready,
    wait_for_trino_catalog,
)

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from .engine import DeploymentEngine


class TrinoDeployer:
    """Deploys Trino coordinator and workers."""

    TEMPLATES = [
        "trino/configmap.yaml.j2",
        "trino/service.yaml.j2",
        "trino/coordinator.yaml.j2",
        "trino/worker.yaml.j2",
    ]

    def __init__(self, engine: DeploymentEngine):
        """Initialize Trino deployer.

        Args:
            engine: Parent deployment engine
        """
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    def deploy(self) -> DeploymentResult:
        """Deploy Trino coordinator and workers.

        Returns:
            DeploymentResult with status
        """
        start = time.time()
        namespace = self.config.get_namespace()

        # Check if Trino query engine is configured
        if self.config.architecture.query_engine.type.value != "trino":
            return DeploymentResult(
                component="trino",
                status=DeploymentStatus.SKIPPED,
                message=f"Skipping Trino (query engine is {self.config.architecture.query_engine.type.value})",
                elapsed_seconds=0,
            )

        if self.engine.dry_run:
            return DeploymentResult(
                component="trino",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy Trino coordinator and workers",
                elapsed_seconds=0,
            )

        try:
            # Render and apply templates
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, self.context)
                # Handle multi-document YAML (configmap has multiple docs)
                for doc in yaml.safe_load_all(yaml_content):
                    if doc:  # Skip empty documents
                        self.k8s.apply_manifest(doc, namespace=namespace)

            # Wait for coordinator Deployment to be ready
            result = wait_for_deployment_ready(
                self.k8s,
                "lakebench-trino-coordinator",
                namespace,
                timeout_seconds=300,
                poll_interval=5,
            )

            if result.status != WaitStatus.READY:
                return DeploymentResult(
                    component="trino",
                    status=DeploymentStatus.FAILED,
                    message=f"Trino coordinator not ready: {result.message}",
                    elapsed_seconds=time.time() - start,
                )

            # Wait for workers (StatefulSet, optional -- may have 0 workers in dev mode)
            worker_replicas = self.config.architecture.query_engine.trino.worker.replicas
            if worker_replicas > 0:
                worker_result = wait_for_statefulset_ready(
                    self.k8s,
                    "lakebench-trino-worker",
                    namespace,
                    timeout_seconds=300,
                    poll_interval=5,
                )

                if worker_result.status != WaitStatus.READY:
                    return DeploymentResult(
                        component="trino",
                        status=DeploymentStatus.FAILED,
                        message=f"Trino workers not ready: {worker_result.message}",
                        elapsed_seconds=time.time() - start,
                    )

            # Verify catalog is accessible by running SHOW CATALOGS
            from kubernetes import client as k8s_client

            core_v1 = k8s_client.CoreV1Api()
            pods = core_v1.list_namespaced_pod(
                namespace,
                label_selector="app=lakebench-trino,component=coordinator",
            )

            if not pods.items:
                return DeploymentResult(
                    component="trino",
                    status=DeploymentStatus.FAILED,
                    message="No Trino coordinator pods found",
                    elapsed_seconds=time.time() - start,
                )

            pod_name = pods.items[0].metadata.name
            catalog_name = self.config.architecture.query_engine.trino.catalog_name

            catalog_result = wait_for_trino_catalog(
                self.k8s,
                pod_name,
                namespace,
                catalog_name,
                timeout_seconds=120,
                poll_interval=10,
            )

            if catalog_result.status != WaitStatus.READY:
                return DeploymentResult(
                    component="trino",
                    status=DeploymentStatus.FAILED,
                    message=f"Trino catalog '{catalog_name}' not accessible: {catalog_result.message}",
                    elapsed_seconds=time.time() - start,
                )

            return DeploymentResult(
                component="trino",
                status=DeploymentStatus.SUCCESS,
                message=f"Trino deployed and catalog '{catalog_name}' verified",
                elapsed_seconds=time.time() - start,
                details={
                    "coordinator_pod": pod_name,
                    "service": "lakebench-trino",
                    "port": 8080,
                    "catalog": catalog_name,
                    "worker_replicas": worker_replicas,
                    "jdbc_url": f"jdbc:trino://lakebench-trino.{namespace}.svc.cluster.local:8080/{catalog_name}",
                },
            )

        except Exception as e:
            return DeploymentResult(
                component="trino",
                status=DeploymentStatus.FAILED,
                message=f"Trino deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def get_jdbc_url(self, schema: str = "silver") -> str:
        """Get Trino JDBC URL.

        Args:
            schema: Schema name to connect to

        Returns:
            JDBC URL string
        """
        namespace = self.config.get_namespace()
        catalog = self.config.architecture.query_engine.trino.catalog_name
        return f"jdbc:trino://lakebench-trino.{namespace}.svc.cluster.local:8080/{catalog}/{schema}"
