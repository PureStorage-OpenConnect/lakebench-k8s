"""PostgreSQL deployment for Lakebench.

Deploys PostgreSQL as the metadata backend for Hive Metastore.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import yaml

from lakebench.k8s import (
    WaitStatus,
    wait_for_postgres_ready,
    wait_for_statefulset_ready,
)

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from .engine import DeploymentEngine


class PostgresDeployer:
    """Deploys PostgreSQL StatefulSet and Service."""

    TEMPLATES = [
        "postgres/serviceaccount.yaml.j2",
        "postgres/statefulset.yaml.j2",
        "postgres/service.yaml.j2",
    ]

    def __init__(self, engine: DeploymentEngine):
        """Initialize PostgreSQL deployer.

        Args:
            engine: Parent deployment engine
        """
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    def deploy(self) -> DeploymentResult:
        """Deploy PostgreSQL.

        Returns:
            DeploymentResult with status
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.engine.dry_run:
            return DeploymentResult(
                component="postgres",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy PostgreSQL StatefulSet",
                elapsed_seconds=0,
            )

        try:
            # Render and apply templates
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, self.context)
                manifest = yaml.safe_load(yaml_content)
                self.k8s.apply_manifest(manifest, namespace=namespace)

            # On OpenShift, grant anyuid SCC to the postgres service account
            if self.context.get("openshift_mode"):
                self._grant_anyuid_scc(namespace)

            # Wait for StatefulSet to be ready
            result = wait_for_statefulset_ready(
                self.k8s,
                "lakebench-postgres",
                namespace,
                timeout_seconds=300,
                poll_interval=5,
            )

            if result.status != WaitStatus.READY:
                return DeploymentResult(
                    component="postgres",
                    status=DeploymentStatus.FAILED,
                    message=f"PostgreSQL StatefulSet not ready: {result.message}",
                    elapsed_seconds=time.time() - start,
                )

            # Wait for pg_isready
            pg_result = wait_for_postgres_ready(
                self.k8s,
                "lakebench-postgres-0",  # First pod in StatefulSet
                namespace,
                database="hive",
                user="hive",
                timeout_seconds=120,
                poll_interval=5,
            )

            if pg_result.status != WaitStatus.READY:
                return DeploymentResult(
                    component="postgres",
                    status=DeploymentStatus.FAILED,
                    message=f"PostgreSQL not accepting connections: {pg_result.message}",
                    elapsed_seconds=time.time() - start,
                )

            return DeploymentResult(
                component="postgres",
                status=DeploymentStatus.SUCCESS,
                message="PostgreSQL deployed and accepting connections",
                elapsed_seconds=time.time() - start,
                details={
                    "pod": "lakebench-postgres-0",
                    "service": "lakebench-postgres",
                    "port": 5432,
                },
            )

        except Exception as e:
            return DeploymentResult(
                component="postgres",
                status=DeploymentStatus.FAILED,
                message=f"PostgreSQL deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _grant_anyuid_scc(self, namespace: str) -> None:
        """Grant anyuid SCC to postgres service account on OpenShift.

        This is required because the PostgreSQL container runs as UID 999.

        Args:
            namespace: Namespace where the service account exists
        """
        import logging
        import subprocess

        logger = logging.getLogger(__name__)

        # Use oc command to add SCC (requires cluster-admin or appropriate RBAC)
        try:
            result = subprocess.run(
                [
                    "oc",
                    "adm",
                    "policy",
                    "add-scc-to-user",
                    "anyuid",
                    "-z",
                    "lakebench-postgres",
                    "-n",
                    namespace,
                ],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                logger.info(f"Granted anyuid SCC to lakebench-postgres in {namespace}")
            else:
                logger.warning(f"Could not grant anyuid SCC: {result.stderr}")
        except FileNotFoundError:
            # oc command not found, try kubectl approach
            logger.warning("oc command not found, SCC may need manual configuration")

    def get_connection_info(self) -> dict[str, str]:
        """Get PostgreSQL connection information.

        Returns:
            Dict with host, port, database, user
        """
        namespace = self.config.get_namespace()
        return {
            "host": f"lakebench-postgres.{namespace}.svc.cluster.local",
            "port": "5432",
            "database": "hive",
            "user": "hive",
            "jdbc_url": f"jdbc:postgresql://lakebench-postgres.{namespace}.svc.cluster.local:5432/hive",
        }
