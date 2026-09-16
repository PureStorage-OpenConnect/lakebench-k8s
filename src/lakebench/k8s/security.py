"""Platform-aware security context verification.

Detects platform type (OpenShift vs vanilla K8s) and verifies
appropriate security requirements are met before deployment.
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from lakebench._constants import SPARK_SERVICE_ACCOUNT

if TYPE_CHECKING:
    from lakebench.k8s import K8sClient

logger = logging.getLogger(__name__)


class PlatformType(Enum):
    """Kubernetes platform types."""

    VANILLA = "vanilla"
    OPENSHIFT = "openshift"


@dataclass
class SCCStatus:
    """Status of a Security Context Constraint assignment."""

    name: str
    assigned: bool
    service_account: str
    namespace: str
    message: str = ""


@dataclass
class SecurityCheckResult:
    """Result of security verification."""

    platform: PlatformType
    platform_version: str = ""
    checks_passed: int = 0
    checks_failed: int = 0
    checks_skipped: int = 0
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    recommendations: list[str] = field(default_factory=list)
    scc_status: list[SCCStatus] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Return True if all required checks passed."""
        return self.checks_failed == 0


class SecurityVerifier:
    """Verifies platform-specific security requirements.

    This class detects the platform type and verifies:
    - OpenShift: SCC assignments for service accounts
    - Vanilla K8s: PSA/PSP configuration (if applicable)
    """

    # Required SCCs for Spark workloads on OpenShift
    SPARK_SCCS = [
        ("anyuid", SPARK_SERVICE_ACCOUNT, "Spark pods run as UID 185"),
    ]

    # OpenShift CRD markers
    OPENSHIFT_CRD_MARKERS = [
        "securitycontextconstraints.security.openshift.io",
        "routes.route.openshift.io",
        "projects.project.openshift.io",
    ]

    def __init__(self, k8s: K8sClient):
        """Initialize security verifier.

        Args:
            k8s: Kubernetes client
        """
        self.k8s = k8s
        self._platform: PlatformType | None = None
        self._platform_version: str = ""

    def detect_platform(self) -> PlatformType:
        """Detect Kubernetes platform type.

        Checks for OpenShift-specific CRDs and API resources.

        Returns:
            PlatformType enum
        """
        if self._platform is not None:
            return self._platform

        try:
            from kubernetes import client as k8s_client

            api_ext = k8s_client.ApiextensionsV1Api()
            crds = api_ext.list_custom_resource_definition()

            for crd in crds.items:
                for marker in self.OPENSHIFT_CRD_MARKERS:
                    if marker in crd.metadata.name:
                        self._platform = PlatformType.OPENSHIFT
                        self._detect_openshift_version()
                        return self._platform

            self._platform = PlatformType.VANILLA
            return self._platform

        except Exception:
            # Default to vanilla K8s if detection fails
            self._platform = PlatformType.VANILLA
            return self._platform

    def _detect_openshift_version(self) -> None:
        """Detect OpenShift version from ClusterVersion CRD."""
        try:
            from kubernetes import client as k8s_client

            custom_api = k8s_client.CustomObjectsApi()

            # Try to get ClusterVersion
            cv = custom_api.get_cluster_custom_object(
                group="config.openshift.io",
                version="v1",
                plural="clusterversions",
                name="version",
            )

            status = cv.get("status", {})
            history = status.get("history", [])
            if history:
                self._platform_version = history[0].get("version", "")

        except Exception:
            self._platform_version = ""

    def get_platform_version(self) -> str:
        """Get platform version string.

        Returns:
            Version string (e.g., "4.19.0" for OpenShift)
        """
        self.detect_platform()
        return self._platform_version

    def verify_security(self, namespace: str) -> SecurityCheckResult:
        """Verify platform-specific security requirements.

        Args:
            namespace: Target namespace for workloads

        Returns:
            SecurityCheckResult with detailed findings
        """
        platform = self.detect_platform()

        result = SecurityCheckResult(
            platform=platform,
            platform_version=self._platform_version,
        )

        if platform == PlatformType.OPENSHIFT:
            self._verify_openshift_security(namespace, result)
        else:
            self._verify_vanilla_security(namespace, result)

        return result

    def _verify_openshift_security(self, namespace: str, result: SecurityCheckResult) -> None:
        """Verify OpenShift-specific security requirements.

        Checks:
        - anyuid SCC assignment for lakebench-spark-runner service account
        - Operator SCCs if Spark operator is installed

        Args:
            namespace: Target namespace
            result: Result object to populate
        """
        # Check required SCCs for Spark
        for scc_name, sa_name, reason in self.SPARK_SCCS:
            scc_status = self._check_scc_assignment(scc_name, sa_name, namespace)
            result.scc_status.append(scc_status)

            if scc_status.assigned:
                result.checks_passed += 1
            else:
                result.checks_failed += 1
                result.issues.append(
                    f"SCC '{scc_name}' not assigned to '{sa_name}' in namespace '{namespace}'. "
                    f"Reason: {reason}. "
                    f"Fix: oc adm policy add-scc-to-user {scc_name} -z {sa_name} -n {namespace}"
                )

        # Check if service account exists
        if self._sa_exists(namespace, SPARK_SERVICE_ACCOUNT):
            result.checks_passed += 1
        else:
            result.warnings.append(
                f"ServiceAccount '{SPARK_SERVICE_ACCOUNT}' not found in namespace '{namespace}'. "
                f"It will be created during deployment."
            )
            result.checks_skipped += 1

        # OpenShift-specific recommendations
        result.recommendations.append(
            "For OpenShift production deployments, consider creating a custom SCC "
            "with minimal privileges instead of using 'anyuid'."
        )

    def _verify_vanilla_security(self, namespace: str, result: SecurityCheckResult) -> None:
        """Verify vanilla Kubernetes security requirements.

        Checks:
        - Pod Security Admission (K8s 1.25+) or PSP (deprecated)
        - RBAC for Spark service account

        Args:
            namespace: Target namespace
            result: Result object to populate
        """
        # Check namespace PSA labels (K8s 1.25+)
        psa_status = self._check_psa_labels(namespace)
        if psa_status is not None:
            if psa_status:
                result.checks_passed += 1
            else:
                result.warnings.append(
                    f"Namespace '{namespace}' may have restrictive Pod Security Admission. "
                    f"Spark pods need 'baseline' or 'privileged' enforcement. "
                    f"Check: kubectl get ns {namespace} -o yaml | grep pod-security"
                )
                result.checks_skipped += 1
        else:
            # Namespace doesn't exist yet - will be created
            result.checks_skipped += 1

        # Note about vanilla K8s
        result.recommendations.append(
            "For vanilla Kubernetes, ensure the namespace allows running containers "
            "as non-root users with specific UIDs (Spark uses UID 185)."
        )

        result.checks_passed += 1  # Vanilla K8s typically works without extra config

    def _check_scc_assignment(self, scc_name: str, sa_name: str, namespace: str) -> SCCStatus:
        """Check if SCC is assigned to a service account.

        Uses `oc` command to verify SCC assignment.

        Args:
            scc_name: Name of the SCC (e.g., "anyuid")
            sa_name: Service account name
            namespace: Namespace

        Returns:
            SCCStatus with assignment details
        """
        try:
            # Check SCC users/groups
            result = subprocess.run(
                ["oc", "get", "scc", scc_name, "-o", "jsonpath={.users}"],
                capture_output=True,
                text=True,
                timeout=30,
            )

            if result.returncode != 0:
                return SCCStatus(
                    name=scc_name,
                    assigned=False,
                    service_account=sa_name,
                    namespace=namespace,
                    message=f"Failed to check SCC: {result.stderr}",
                )

            # Parse users list - format is ["system:serviceaccount:ns:sa", ...]
            users = result.stdout.strip()
            expected_user = f"system:serviceaccount:{namespace}:{sa_name}"

            if expected_user in users:
                return SCCStatus(
                    name=scc_name,
                    assigned=True,
                    service_account=sa_name,
                    namespace=namespace,
                    message=f"SCC '{scc_name}' assigned to {sa_name}",
                )
            else:
                return SCCStatus(
                    name=scc_name,
                    assigned=False,
                    service_account=sa_name,
                    namespace=namespace,
                    message=f"SCC '{scc_name}' not assigned to {sa_name}",
                )

        except subprocess.TimeoutExpired:
            return SCCStatus(
                name=scc_name,
                assigned=False,
                service_account=sa_name,
                namespace=namespace,
                message="Timeout checking SCC",
            )
        except FileNotFoundError:
            return SCCStatus(
                name=scc_name,
                assigned=False,
                service_account=sa_name,
                namespace=namespace,
                message="oc command not found - cannot verify SCC",
            )
        except Exception as e:
            return SCCStatus(
                name=scc_name,
                assigned=False,
                service_account=sa_name,
                namespace=namespace,
                message=f"Error checking SCC: {e}",
            )

    def _check_psa_labels(self, namespace: str) -> bool | None:
        """Check Pod Security Admission labels on namespace.

        Args:
            namespace: Namespace to check

        Returns:
            True if PSA allows Spark workloads, False if restrictive, None if ns doesn't exist
        """
        try:
            from kubernetes import client as k8s_client

            core_v1 = k8s_client.CoreV1Api()
            ns = core_v1.read_namespace(namespace)

            labels = ns.metadata.labels or {}

            # Check PSA enforcement label
            psa_enforce = labels.get("pod-security.kubernetes.io/enforce", "")

            # Spark needs at least 'baseline' or 'privileged'
            if psa_enforce in ("restricted",):
                return False  # Too restrictive for Spark

            return True

        except Exception:
            return None  # Namespace doesn't exist

    def _sa_exists(self, namespace: str, sa_name: str) -> bool:
        """Check if service account exists.

        Args:
            namespace: Namespace
            sa_name: Service account name

        Returns:
            True if exists
        """
        try:
            from kubernetes import client as k8s_client

            core_v1 = k8s_client.CoreV1Api()
            core_v1.read_namespaced_service_account(sa_name, namespace)
            return True
        except Exception:
            return False

    def ensure_openshift_scc(self, namespace: str) -> bool:
        """Ensure required SCCs are assigned on OpenShift.

        This should be called during deployment to automatically
        configure SCCs.

        Args:
            namespace: Target namespace

        Returns:
            True if all SCCs assigned successfully
        """
        if self.detect_platform() != PlatformType.OPENSHIFT:
            return True  # Not OpenShift, nothing to do

        success = True
        for scc_name, sa_name, _ in self.SPARK_SCCS:
            if not self._add_scc(scc_name, sa_name, namespace):
                success = False

        return success

    # `oc adm policy add-scc-to-user` on OpenShift 4.10+ creates a
    # namespaced RoleBinding named `system:openshift:scc:<scc>` that binds
    # the SA to a ClusterRole -- NOT a mutation of the SCC's cluster-scoped
    # `.users` array (that legacy field stays empty on modern OCP). The
    # binding is per-namespace, so the cross-namespace race the original
    # LB-088 finding assumed does not apply here. The retry-with-verify
    # still buys us two useful things: (1) within a single namespace,
    # sequential adds for postgres + spark-runner are read-modify-write on
    # the same RoleBinding and can race between phases; (2) surface a real
    # `oc` failure loud instead of returning False silently. Verification
    # reads the namespaced RoleBinding, not the (always-empty) SCC users.
    _SCC_ADD_MAX_ATTEMPTS = 4
    _SCC_ADD_BACKOFF_SECONDS = 1.5

    def _add_scc(self, scc_name: str, sa_name: str, namespace: str) -> bool:
        """Add SCC to service account with post-write verification.

        Retries `oc adm policy add-scc-to-user` up to _SCC_ADD_MAX_ATTEMPTS
        times, verifying after each attempt that the SA appears as a
        subject of the namespaced `system:openshift:scc:<scc>` RoleBinding.
        Returns True only when the subject is confirmed present. Returns
        False (with a warning log) if no attempt succeeds -- the caller
        should treat this as a real RBAC failure.
        """
        last_stderr = ""

        for attempt in range(1, self._SCC_ADD_MAX_ATTEMPTS + 1):
            try:
                result = subprocess.run(
                    [
                        "oc",
                        "adm",
                        "policy",
                        "add-scc-to-user",
                        scc_name,
                        "-z",
                        sa_name,
                        "-n",
                        namespace,
                    ],
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                last_stderr = result.stderr
            except Exception as e:
                last_stderr = str(e)
                result = None

            if self._scc_binding_has_subject(scc_name, sa_name, namespace):
                return True

            if attempt < self._SCC_ADD_MAX_ATTEMPTS:
                time.sleep(self._SCC_ADD_BACKOFF_SECONDS * attempt)

        logger.warning(
            "SCC %s add for serviceaccount %s/%s failed to land after %d attempts "
            "(last stderr: %s). This deploy's pods may be admission-rejected -- "
            "fix RBAC before running.",
            scc_name,
            namespace,
            sa_name,
            self._SCC_ADD_MAX_ATTEMPTS,
            last_stderr.strip()[:200],
        )
        return False

    def _scc_binding_has_subject(self, scc_name: str, sa_name: str, namespace: str) -> bool:
        """Return True iff the namespaced `system:openshift:scc:<scc>`
        RoleBinding lists `sa_name` as a ServiceAccount subject.

        This is how `oc adm policy add-scc-to-user` records the grant on
        OpenShift 4.10+. Best-effort read via `oc get rolebinding`; on any
        read failure returns False so the caller retries.
        """
        try:
            result = subprocess.run(
                [
                    "oc",
                    "get",
                    "rolebinding",
                    f"system:openshift:scc:{scc_name}",
                    "-n",
                    namespace,
                    "-o",
                    "jsonpath={range .subjects[?(@.kind=='ServiceAccount')]}"
                    "{.namespace}/{.name} {end}",
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode != 0:
                return False
            return f"{namespace}/{sa_name}" in result.stdout.split()
        except Exception:
            return False

    def _scc_has_user(self, scc_name: str, user: str) -> bool:
        """Legacy pre-4.10 helper: returns True iff `user` appears in the
        SCC's cluster-scoped `.users` array. Retained for the `oc get scc`
        path in older cluster paths / tests; new call sites should use
        `_scc_binding_has_subject`.
        """
        try:
            result = subprocess.run(
                [
                    "oc",
                    "get",
                    "scc",
                    scc_name,
                    "-o",
                    "jsonpath={.users}",
                ],
                capture_output=True,
                text=True,
                timeout=15,
            )
            if result.returncode != 0:
                return False
            # jsonpath returns something like: [system:sa:ns:foo system:sa:ns:bar]
            return user in result.stdout
        except Exception:
            return False
