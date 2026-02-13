"""RBAC deployment for Lakebench.

Deploys ServiceAccount, Role, and RoleBinding for Spark.
On OpenShift, also adds the anyuid SCC to the lakebench-spark-runner service account.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import yaml

from lakebench._constants import SPARK_SERVICE_ACCOUNT
from lakebench.k8s import PlatformType, SecurityVerifier

from .engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from .engine import DeploymentEngine


class RBACDeployer:
    """Deploys Spark RBAC resources."""

    TEMPLATES = [
        "rbac/serviceaccount.yaml.j2",
        "rbac/role.yaml.j2",
        "rbac/rolebinding.yaml.j2",
    ]

    def __init__(self, engine: DeploymentEngine):
        """Initialize RBAC deployer.

        Args:
            engine: Parent deployment engine
        """
        self.engine = engine
        self.config = engine.config
        self.k8s = engine.k8s
        self.renderer = engine.renderer
        self.context = engine.context
        self._security_verifier: SecurityVerifier | None = None

    @property
    def security_verifier(self) -> SecurityVerifier:
        """Get or create security verifier."""
        if self._security_verifier is None:
            self._security_verifier = SecurityVerifier(self.k8s)
        return self._security_verifier

    def deploy(self) -> DeploymentResult:
        """Deploy Spark RBAC resources.

        Returns:
            DeploymentResult with status
        """
        start = time.time()
        namespace = self.config.get_namespace()

        if self.engine.dry_run:
            return DeploymentResult(
                component="rbac",
                status=DeploymentStatus.SUCCESS,
                message="Would create Spark RBAC (ServiceAccount, Role, RoleBinding)",
                elapsed_seconds=0,
            )

        try:
            # Render and apply templates
            for template_name in self.TEMPLATES:
                yaml_content = self.renderer.render(template_name, self.context)
                manifest = yaml.safe_load(yaml_content)
                self.k8s.apply_manifest(manifest, namespace=namespace)

            # On OpenShift, add anyuid SCC for Spark pods (UID 185)
            scc_added = False
            platform = self.security_verifier.detect_platform()
            if platform == PlatformType.OPENSHIFT:
                scc_added = self.security_verifier.ensure_openshift_scc(namespace)

            message = "Created Spark RBAC resources"
            if scc_added:
                message += " (with OpenShift anyuid SCC)"

            return DeploymentResult(
                component="rbac",
                status=DeploymentStatus.SUCCESS,
                message=message,
                elapsed_seconds=time.time() - start,
                details={
                    "service_account": SPARK_SERVICE_ACCOUNT,
                    "role": SPARK_SERVICE_ACCOUNT,
                    "role_binding": SPARK_SERVICE_ACCOUNT,
                    "openshift_scc": "anyuid" if scc_added else None,
                },
            )

        except Exception as e:
            return DeploymentResult(
                component="rbac",
                status=DeploymentStatus.FAILED,
                message=f"RBAC deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )
