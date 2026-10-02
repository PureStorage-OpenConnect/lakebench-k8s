"""PostgreSQL deployment for Lakebench.

Deploys PostgreSQL as the metadata backend for Hive Metastore.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import yaml

from lakebench.k8s import (
    WaitStatus,
    wait_for_postgres_ready,
    wait_for_statefulset_ready,
)
from lakebench.k8s.security import SCCGrantError, ensure_scc_rolebinding

from .deployment_secrets import (
    HIVE_DB_KEY,
    HIVE_DB_SECRET,
    DeploymentSecretError,
    postgres_psql,
    read_secret_key,
    sync_role_password,
)
from .engine import DeploymentResult, DeploymentStatus, image_tag

logger = logging.getLogger(__name__)

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

            # On OpenShift, grant anyuid SCC to the postgres service account.
            # A failed grant stops here: the pod would only be
            # admission-rejected later, with a less useful message.
            if self.context.get("openshift_mode"):
                try:
                    self._grant_anyuid_scc(namespace)
                except SCCGrantError as e:
                    return DeploymentResult(
                        component="postgres",
                        status=DeploymentStatus.FAILED,
                        message=str(e),
                        elapsed_seconds=time.time() - start,
                    )

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

            # Postgres reads POSTGRES_PASSWORD only at initdb. Make the
            # hive role match the Secret every deploy, so existing data whose
            # Secret was lost, or a wrong v1.6 guess, cannot lock the
            # metastore out. Sends a SCRAM verifier only.
            try:
                self._sync_hive_role(namespace)
            except DeploymentSecretError as e:
                return DeploymentResult(
                    component="postgres",
                    status=DeploymentStatus.FAILED,
                    message=str(e),
                    elapsed_seconds=time.time() - start,
                )

            pg_version = image_tag(self.config.images.postgres)
            return DeploymentResult(
                component="postgres",
                status=DeploymentStatus.SUCCESS,
                message=f"PostgreSQL {pg_version} deployed and accepting connections",
                elapsed_seconds=time.time() - start,
                details={
                    "pod": "lakebench-postgres-0",
                    "service": "lakebench-postgres",
                    "port": 5432,
                },
                label="PostgreSQL",
                detail=pg_version,
            )

        except Exception as e:
            logger.exception("PostgreSQL deployment failed")
            return DeploymentResult(
                component="postgres",
                status=DeploymentStatus.FAILED,
                message=f"PostgreSQL deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _sync_hive_role(self, namespace: str) -> None:
        from kubernetes import client as k8s_client

        core_v1 = k8s_client.CoreV1Api()
        password = self.context.get("postgres_password") or read_secret_key(
            core_v1, namespace, HIVE_DB_SECRET, HIVE_DB_KEY
        )
        if not password:
            raise DeploymentSecretError(
                f"Secret {HIVE_DB_SECRET} is missing in namespace {namespace}; re-run deploy"
            )
        sync_role_password(postgres_psql(core_v1, namespace), "hive", password)

    def _grant_anyuid_scc(self, namespace: str) -> None:
        """Grant anyuid SCC to the postgres service account on OpenShift.

        PostgreSQL runs as UID 999. The grant is the namespaced RoleBinding
        ``system:openshift:scc:anyuid`` made through the API, so no ``oc`` is
        needed on OCP 4.10+. Raises ``SCCGrantError`` when it cannot be made.
        """
        from kubernetes import client as k8s_client

        ensure_scc_rolebinding(
            k8s_client.RbacAuthorizationV1Api(), namespace, "lakebench-postgres", "anyuid"
        )
        logger.info("Granted anyuid SCC to lakebench-postgres in %s", namespace)

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
