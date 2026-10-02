"""Apache Polaris REST catalog deployment for Lakebench.

Polaris is an open-source Iceberg REST catalog that provides:
- REST API (port 8181) instead of Thrift (Hive)
- OAuth2 authentication for Spark/Trino
- relational-jdbc persistence backed by PostgreSQL
- No operator dependency (unlike Stackable Hive)

FlashBlade integration:
- stsUnavailable=true (no AWS STS, no credential vending)
- pathStyleAccess=true (FlashBlade requires path-style)
- Each client (Spark, Trino) keeps its own static S3 credentials
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import yaml

from lakebench.deploy.deployment_secrets import (
    DeploymentSecretError,
    ensure_polaris_client_secret,
    ensure_polaris_db_password,
    polaris_role_exists,
    psql_command_ok,
    scram_sha256_verifier,
    sync_role_password,
)
from lakebench.deploy.engine import DeploymentResult, DeploymentStatus, image_tag
from lakebench.k8s import WaitStatus, wait_for_deployment_ready

if TYPE_CHECKING:
    from lakebench.deploy.engine import DeploymentEngine

logger = logging.getLogger(__name__)


class PolarisDeployer:
    """Deploys Apache Polaris REST catalog.

    Deployment sequence:
    1. Create 'polaris' database in shared PostgreSQL
    2. Deploy Polaris server (Deployment + Service)
    3. Wait for health check (port 8182)
    4. Run bootstrap Job (realm, catalog, roles)
    5. Wait for Job completion
    """

    TEMPLATES = [
        "polaris/configmap.yaml.j2",
        "polaris/service.yaml.j2",
        "polaris/deployment.yaml.j2",
    ]

    BOOTSTRAP_TEMPLATE = "polaris/bootstrap-job.yaml.j2"

    def __init__(self, engine: DeploymentEngine):
        """Initialize Polaris deployer.

        Args:
            engine: Parent deployment engine
        """
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context

    def deploy(self) -> DeploymentResult:
        """Deploy Apache Polaris catalog.

        Returns:
            DeploymentResult with status
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.config.architecture.catalog.type.value != "polaris":
            return DeploymentResult(
                component="polaris",
                status=DeploymentStatus.SKIPPED,
                message=f"Skipping Polaris (catalog type is {self.config.architecture.catalog.type.value})",
                elapsed_seconds=0,
            )

        if self.engine.dry_run:
            return DeploymentResult(
                component="polaris",
                status=DeploymentStatus.SUCCESS,
                message="Would deploy Polaris REST catalog",
                elapsed_seconds=0,
            )

        try:
            # Step 1: Create polaris database in PostgreSQL, with this
            # deployment's own DB password and client secret.
            self._create_polaris_db(namespace)

            # Step 2: Deploy server + service
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, self.context)
                for doc in yaml.safe_load_all(yaml_content):
                    if doc:
                        self.k8s.apply_manifest(doc, namespace=namespace)

            # Step 3: Wait for Polaris deployment to be ready
            result = wait_for_deployment_ready(
                self.k8s,
                "lakebench-polaris",
                namespace,
                timeout_seconds=600,
                poll_interval=5,
            )

            if result.status != WaitStatus.READY:
                return DeploymentResult(
                    component="polaris",
                    status=DeploymentStatus.FAILED,
                    message=f"Polaris deployment not ready: {result.message}",
                    elapsed_seconds=time.time() - start,
                )

            # Step 4: Run bootstrap job (delete old job first for idempotency)
            self._delete_old_bootstrap_job(namespace)
            bootstrap_yaml = self.renderer.render(self.BOOTSTRAP_TEMPLATE, self.context)
            for doc in yaml.safe_load_all(bootstrap_yaml):
                if doc:
                    self.k8s.apply_manifest(doc, namespace=namespace)

            # Step 5: Wait for bootstrap job completion
            bootstrap_result = self._wait_for_bootstrap_job(namespace, timeout_seconds=600)

            if not bootstrap_result:
                return DeploymentResult(
                    component="polaris",
                    status=DeploymentStatus.FAILED,
                    message="Polaris bootstrap job did not complete in time",
                    elapsed_seconds=time.time() - start,
                )

            port = self.config.architecture.catalog.polaris.port
            polaris_version = image_tag(self.config.images.polaris)
            return DeploymentResult(
                component="polaris",
                status=DeploymentStatus.SUCCESS,
                message=f"Polaris REST catalog {polaris_version} deployed and bootstrapped",
                elapsed_seconds=time.time() - start,
                details={
                    "type": "polaris",
                    "service": "lakebench-polaris",
                    "port": port,
                    "rest_uri": self.get_rest_uri(),
                },
                label="Polaris",
                detail=polaris_version,
            )

        except DeploymentSecretError as e:
            return DeploymentResult(
                component="polaris",
                status=DeploymentStatus.FAILED,
                message=str(e),
                elapsed_seconds=time.time() - start,
            )
        except Exception as e:
            logger.exception("Polaris deployment failed")
            return DeploymentResult(
                component="polaris",
                status=DeploymentStatus.FAILED,
                message=f"Polaris deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def _create_polaris_db(self, namespace: str) -> None:
        """Create polaris database in the shared PostgreSQL instance.

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

            # An existing polaris role means Polaris was bootstrapped
            # before (a v1.6 deployment keeps its client secret); an
            # unreadable answer stops the deploy.
            role_exists = polaris_role_exists(_exec_psql)
            password = ensure_polaris_db_password(
                core_v1, self.config, namespace, role_exists=role_exists
            )
            ensure_polaris_client_secret(core_v1, self.config, namespace, fresh=not role_exists)
            if role_exists:
                # The Secret is the authority: make the role match it, so a
                # wrong v1.6 guess or a lost Secret cannot lock Polaris out.
                sync_role_password(_exec_psql, "polaris", password)
            else:
                # Only a SCRAM verifier crosses the exec request and the logs.
                resp = _exec_psql(
                    "hive",
                    f"CREATE USER polaris WITH PASSWORD '{scram_sha256_verifier(password)}';",
                )
                if not psql_command_ok(resp, "CREATE ROLE") and polaris_role_exists(_exec_psql):
                    # An overlapping deploy created it first: sync to the Secret.
                    sync_role_password(_exec_psql, "polaris", password)
                elif not psql_command_ok(resp, "CREATE ROLE"):
                    raise DeploymentSecretError(
                        "could not create the polaris role in lakebench-postgres-0; check the "
                        "pod, then re-run deploy"
                    )

            # Create database (idempotent -- check first, CREATE DATABASE
            # cannot run inside a DO block / transaction)
            resp = _exec_psql(
                "hive",
                "SELECT datname FROM pg_database WHERE datname = 'polaris';",
            )
            if "polaris" not in resp:
                resp = _exec_psql("hive", "CREATE DATABASE polaris OWNER polaris;")
                logger.info(f"Create polaris database: {resp}")
            else:
                logger.info("Polaris database already exists")

            # Grant privileges on database
            resp = _exec_psql(
                "hive",
                "GRANT ALL PRIVILEGES ON DATABASE polaris TO polaris;",
            )
            logger.info(f"Grant polaris privileges: {resp}")

            # Grant schema permissions (needed for Polaris to create tables)
            resp = _exec_psql(
                "polaris",
                "GRANT ALL ON SCHEMA public TO polaris;",
            )
            logger.info(f"Grant polaris schema access: {resp}")

        except DeploymentSecretError:
            raise
        except Exception as e:
            logger.warning(f"Could not create polaris database: {e}")
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
                "lakebench-polaris-bootstrap",
                namespace,
                propagation_policy="Background",
            )
            logger.info("Deleted existing bootstrap job")
            # Brief pause for pod cleanup
            time.sleep(5)
        except ApiException as e:
            if e.status == 404:
                pass  # No existing job -- normal for first deploy
            else:
                logger.warning(f"Could not delete old bootstrap job: {e}")
        except Exception as e:
            logger.warning(f"Could not delete old bootstrap job: {e}")

    def _wait_for_bootstrap_job(self, namespace: str, timeout_seconds: int = 180) -> bool:
        """Wait for the Polaris bootstrap Job to complete.

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
                    job = batch_v1.read_namespaced_job("lakebench-polaris-bootstrap", namespace)
                    status = job.status

                    if status.succeeded and status.succeeded >= 1:
                        logger.info("Polaris bootstrap job completed successfully")
                        return True

                    if status.failed and status.failed >= 3:
                        logger.error("Polaris bootstrap job failed after 3 attempts")
                        return False

                except ApiException as e:
                    if e.status == 404:
                        pass  # Job not created yet
                    else:
                        raise

                time.sleep(5)

            logger.error(f"Polaris bootstrap job timed out after {timeout_seconds}s")
            return False

        except Exception as e:
            logger.error(f"Error waiting for bootstrap job: {e}")
            return False

    def get_rest_uri(self) -> str:
        """Get Polaris REST catalog URI.

        Returns:
            REST URI string (e.g., http://lakebench-polaris.<ns>:8181/api/catalog)
        """
        namespace = self.config.get_namespace()
        port = self.config.architecture.catalog.polaris.port
        return f"http://lakebench-polaris.{namespace}.svc.cluster.local:{port}/api/catalog"
