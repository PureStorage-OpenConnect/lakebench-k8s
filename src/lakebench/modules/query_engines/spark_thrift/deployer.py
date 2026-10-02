"""Spark Thrift Server deployment for Lakebench.

Deploys Spark Thrift Server (HiveThriftServer2) as a long-running
Deployment with spark-submit in client mode, exposed on port 10000.
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
        """Deploy the Spark Thrift Server."""
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
            from lakebench.deploy.deps import consumer_context

            context = consumer_context(self.engine, "Spark Thrift Server")
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, context)
                for doc in yaml.safe_load_all(yaml_content):
                    if doc:
                        self.k8s.apply_manifest(doc, namespace=namespace)

            self._wait_for_ready(namespace, timeout_seconds=deploy_deadline.clamp(300))

            spark_version = image_tag(self.config.images.spark)
            return DeploymentResult(
                component="spark-thrift",
                status=DeploymentStatus.SUCCESS,
                message=f"Spark Thrift Server deployed (Spark {spark_version})",
                elapsed_seconds=time.time() - start,
                label="Spark Thrift",
                detail=spark_version,
            )

        except Exception as e:
            logger.exception("Spark Thrift deployment failed")
            return DeploymentResult(
                component="spark-thrift",
                status=DeploymentStatus.FAILED,
                message=f"Spark Thrift Server deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _wait_for_ready(self, namespace: str, timeout_seconds: float = 300) -> None:
        """Wait until the Thrift Deployment has rolled out and its pod runs
        this deploy's dependency set and is Ready (DeployTimeout when the
        deploy deadline cuts the wait)."""
        from lakebench.deploy.deps import wait_consumer_rolled

        handle = self.engine.deps
        wait_consumer_rolled(
            namespace,
            "lakebench-spark-thrift",
            handle.pinset_sha256 if handle is not None else None,
            timeout_seconds=timeout_seconds,
            what="Spark Thrift Server",
        )
