"""Unity Catalog deployment for Lakebench.

Unity Catalog is an open-source REST catalog (Apache-licensed) that provides:
- REST API (default port 8181) for Iceberg/Delta table management
- PostgreSQL persistence backend
- S3-compatible storage integration
- No operator dependency (self-managed Deployment)

FlashBlade integration:
- pathStyleAccess=true (FlashBlade requires path-style)
- Static S3 credentials (no STS credential vending)
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import yaml

from lakebench.deploy.engine import DeploymentResult, DeploymentStatus, image_tag
from lakebench.k8s import WaitStatus, wait_for_deployment_ready

if TYPE_CHECKING:
    from lakebench.deploy.engine import DeploymentEngine

logger = logging.getLogger(__name__)


class UnityDeployer:
    """Deploys Unity Catalog REST catalog.

    Deployment sequence:
    1. Create 'unity' database in shared PostgreSQL
    2. Deploy Unity server (ConfigMap + Deployment + Service)
    3. Wait for deployment ready (300s timeout)
    4. Run bootstrap Job (create catalog + schemas)
    5. Wait for Job completion
    """

    TEMPLATES = [
        "unity/configmap.yaml.j2",
        "unity/service.yaml.j2",
        "unity/deployment.yaml.j2",
    ]

    BOOTSTRAP_TEMPLATE = "unity/bootstrap-job.yaml.j2"

    def __init__(self, engine: DeploymentEngine):
        """Initialize Unity deployer.

        Args:
            engine: Parent deployment engine
        """
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    def deploy(self) -> DeploymentResult:
        """Deploy Unity Catalog.

        Returns:
            DeploymentResult with status
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.config.architecture.catalog.type.value != "unity":
            return DeploymentResult(
                component="unity",
                status=DeploymentStatus.SKIPPED,
                message=f"Skipping Unity (catalog type is {self.config.architecture.catalog.type.value})",
                elapsed_seconds=0,
            )

        if self.engine.dry_run:
            return DeploymentResult(
                component="unity",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy Unity Catalog",
                elapsed_seconds=0,
            )

        try:
            # Step 1: Create unity database in PostgreSQL
            self._create_unity_db(namespace)

            # Step 2: Deploy server + service
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, self.context)
                for doc in yaml.safe_load_all(yaml_content):
                    if doc:
                        self.k8s.apply_manifest(doc, namespace=namespace)

            # Step 3: Wait for Unity deployment to be ready
            result = wait_for_deployment_ready(
                self.k8s,
                "lakebench-unity",
                namespace,
                timeout_seconds=600,
                poll_interval=5,
            )

            if result.status != WaitStatus.READY:
                return DeploymentResult(
                    component="unity",
                    status=DeploymentStatus.FAILED,
                    message=f"Unity deployment not ready: {result.message}",
                    elapsed_seconds=time.time() - start,
                )

            # Step 4: Run bootstrap job (delete old job first for idempotency)
            self._delete_old_bootstrap_job(namespace)
            bootstrap_yaml = self.renderer.render(self.BOOTSTRAP_TEMPLATE, self.context)
            for doc in yaml.safe_load_all(bootstrap_yaml):
                if doc:
                    self.k8s.apply_manifest(doc, namespace=namespace)

            # Step 5: Wait for bootstrap job completion
            bootstrap_result = self._wait_for_bootstrap_job(namespace, timeout_seconds=300)

            if not bootstrap_result:
                return DeploymentResult(
                    component="unity",
                    status=DeploymentStatus.FAILED,
                    message="Unity bootstrap job did not complete in time",
                    elapsed_seconds=time.time() - start,
                )

            port = self.config.architecture.catalog.unity.port
            unity_version = image_tag(self.config.images.unity)
            return DeploymentResult(
                component="unity",
                status=DeploymentStatus.SUCCESS,
                message=f"Unity Catalog {unity_version} deployed and bootstrapped",
                elapsed_seconds=time.time() - start,
                details={
                    "type": "unity",
                    "service": "lakebench-unity",
                    "port": port,
                    "rest_uri": self.get_rest_uri(),
                },
                label="Unity",
                detail=unity_version,
            )

        except Exception as e:
            logger.exception("Unity deployment failed")
            return DeploymentResult(
                component="unity",
                status=DeploymentStatus.FAILED,
                message=f"Unity deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _create_unity_db(self, namespace: str) -> None:
        """Create unity database in the shared PostgreSQL instance.

        Uses kubectl exec to run psql commands. Idempotent -- safe to
        run if the database already exists.

        Args:
            namespace: Target namespace
        """
        try:
            from kubernetes import client as k8s_client
            from kubernetes.stream import stream

            core_v1 = k8s_client.CoreV1Api()

            def _exec_psql(database: str, sql: str) -> str:
                return str(
                    stream(
                        core_v1.connect_get_namespaced_pod_exec,
                        "lakebench-postgres-0",
                        namespace,
                        command=["psql", "-U", "hive", "-d", database, "-c", sql],
                        stderr=True,
                        stdout=True,
                        stdin=False,
                        tty=False,
                    )
                )

            # Create user (idempotent via DO block)
            resp = _exec_psql(
                "hive",
                "DO $$ BEGIN "
                "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'unity') THEN "
                "CREATE USER unity WITH PASSWORD 'lakebench-unity-2024'; "
                "END IF; END $$;",
            )
            logger.info(f"Create unity user: {resp}")

            # Create database (idempotent -- check first, CREATE DATABASE
            # cannot run inside a DO block / transaction)
            resp = _exec_psql(
                "hive",
                "SELECT datname FROM pg_database WHERE datname = 'unity';",
            )
            if "unity" not in resp:
                resp = _exec_psql("hive", "CREATE DATABASE unity OWNER unity;")
                logger.info(f"Create unity database: {resp}")
            else:
                logger.info("Unity database already exists")

            # Grant privileges on database
            resp = _exec_psql(
                "hive",
                "GRANT ALL PRIVILEGES ON DATABASE unity TO unity;",
            )
            logger.info(f"Grant unity privileges: {resp}")

            # Grant schema permissions (needed for Unity to create tables)
            resp = _exec_psql(
                "unity",
                "GRANT ALL ON SCHEMA public TO unity;",
            )
            logger.info(f"Grant unity schema access: {resp}")

        except Exception as e:
            logger.warning(f"Could not create unity database: {e}")
            raise

    def _delete_old_bootstrap_job(self, namespace: str) -> None:
        """Delete any existing bootstrap Job for idempotent re-deploy.

        Kubernetes Jobs are immutable once created. We must delete
        the old Job (and its pods) before creating a new one.

        Args:
            namespace: Target namespace
        """
        try:
            from kubernetes import client as k8s_client
            from kubernetes.client.rest import ApiException

            batch_v1 = k8s_client.BatchV1Api()
            batch_v1.delete_namespaced_job(
                "lakebench-unity-bootstrap",
                namespace,
                propagation_policy="Background",
            )
            logger.info("Deleted existing unity bootstrap job")
            # Brief pause for pod cleanup
            time.sleep(5)
        except ApiException as e:
            if e.status == 404:
                pass  # No existing job -- normal for first deploy
            else:
                logger.warning(f"Could not delete old unity bootstrap job: {e}")
        except Exception as e:
            logger.warning(f"Could not delete old unity bootstrap job: {e}")

    def _wait_for_bootstrap_job(self, namespace: str, timeout_seconds: int = 180) -> bool:
        """Wait for the Unity bootstrap Job to complete.

        Args:
            namespace: Target namespace
            timeout_seconds: Maximum wait time

        Returns:
            True if job completed successfully
        """
        try:
            from kubernetes import client as k8s_client
            from kubernetes.client.rest import ApiException

            batch_v1 = k8s_client.BatchV1Api()
            deadline = time.time() + timeout_seconds

            while time.time() < deadline:
                try:
                    job = batch_v1.read_namespaced_job("lakebench-unity-bootstrap", namespace)
                    status = job.status

                    if status.succeeded and status.succeeded >= 1:
                        logger.info("Unity bootstrap job completed successfully")
                        return True

                    if status.failed and status.failed >= 3:
                        logger.error("Unity bootstrap job failed after 3 attempts")
                        return False

                except ApiException as e:
                    if e.status == 404:
                        pass  # Job not created yet
                    else:
                        raise

                time.sleep(5)

            logger.error(f"Unity bootstrap job timed out after {timeout_seconds}s")
            return False

        except Exception as e:
            logger.error(f"Error waiting for unity bootstrap job: {e}")
            return False

    def get_rest_uri(self) -> str:
        """Get Unity Catalog REST URI.

        Returns:
            REST URI string (e.g., http://lakebench-unity.<ns>:8181/api/2.1/unity-catalog)
        """
        namespace = self.config.get_namespace()
        port = self.config.architecture.catalog.unity.port
        return f"http://lakebench-unity.{namespace}.svc.cluster.local:{port}/api/2.1/unity-catalog/iceberg/"
