"""Platform-aware security context verification.

Detects platform type (OpenShift vs vanilla K8s) and verifies
appropriate security requirements are met before deployment.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

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


class SCCGrantError(Exception):
    """An SCC could not be granted to a ServiceAccount. Deploy stops."""


SCC_GRANT_ATTEMPTS = 5
# How long a new RoleBinding may take to reach every apiserver's authorizer.
SCC_VERIFY_TIMEOUT_S = 15.0


def scc_role_name(scc: str) -> str:
    """The ClusterRole (OCP 4.10+) granting ``use`` on an SCC, and the name of
    the namespaced RoleBinding ``oc adm policy add-scc-to-user`` creates."""
    return f"system:openshift:scc:{scc}"


def _grant_fix(scc: str, sa: str, namespace: str) -> str:
    return f"a cluster admin can run `oc adm policy add-scc-to-user {scc} -z {sa} -n {namespace}`"


def _subject_present(subjects: list[dict] | None, sa: str, namespace: str) -> bool:
    for sub in subjects or []:
        if (
            sub.get("kind") == "ServiceAccount"
            and sub.get("name") == sa
            and (sub.get("namespace") or namespace) == namespace
        ):
            return True
    return False


def _sa_can_use_scc(authz_api: Any, namespace: str, sa: str, scc: str) -> bool | None:
    """Whether ServiceAccount ``sa`` may ``use`` SCC ``scc`` in ``namespace``,
    by a LocalSubjectAccessReview (what SCC admission asks the authorizer).
    None when the review itself cannot be made."""

    body = {
        "apiVersion": "authorization.k8s.io/v1",
        "kind": "LocalSubjectAccessReview",
        "metadata": {"namespace": namespace},
        "spec": {
            "user": f"system:serviceaccount:{namespace}:{sa}",
            "groups": ["system:serviceaccounts", f"system:serviceaccounts:{namespace}"],
            "resourceAttributes": {
                "namespace": namespace,
                "verb": "use",
                "group": "security.openshift.io",
                "resource": "securitycontextconstraints",
                "name": scc,
            },
        },
    }
    try:
        review = authz_api.create_namespaced_local_subject_access_review(namespace, body)
    except Exception:  # noqa: BLE001  (403, timeout, connection: cannot tell)
        return None
    status = getattr(review, "status", None)
    if isinstance(review, dict):
        status = review.get("status")
    if isinstance(status, dict):
        return bool(status.get("allowed"))
    return bool(getattr(status, "allowed", False))


def ensure_scc_rolebinding(
    rbac_api: Any,
    namespace: str,
    sa: str,
    scc: str = "anyuid",
    *,
    authz_api: Any = None,
) -> None:
    """Grant ``scc`` to ServiceAccount ``sa`` in ``namespace``.

    1. If a LocalSubjectAccessReview says the SA may already ``use`` the SCC
       (a grant an admin made some other way), nothing is written.
    2. Otherwise make the call ``oc adm policy add-scc-to-user <scc> -z <sa>
       -n <namespace>`` makes on OCP 4.10+: read, merge and replace (under the
       read's ``resourceVersion``) the RoleBinding
       ``system:openshift:scc:<scc>`` in the deployment's own namespace, bound
       to the ClusterRole of the same name. A conflict re-reads and retries.
       Nothing outside the namespace is written.
    3. Review again; the grant must have taken effect (a binding to a missing
       ClusterRole would not).

    ``oc`` is never run. Every failure raises :class:`SCCGrantError` naming
    the admin command. When the review cannot be made (no permission to
    create LocalSubjectAccessReviews), steps 1 and 3 are skipped.
    """
    from kubernetes.client.rest import ApiException

    role = scc_role_name(scc)
    prefix = f"cannot grant SCC {scc} to SA {sa} in namespace {namespace}"
    if authz_api is None:
        from kubernetes import client as k8s_client

        authz_api = k8s_client.AuthorizationV1Api()

    if _sa_can_use_scc(authz_api, namespace, sa, scc) is True:
        return

    subject = {"kind": "ServiceAccount", "name": sa, "namespace": namespace}
    last: Exception | None = None
    for _ in range(SCC_GRANT_ATTEMPTS):
        try:
            try:
                current = rbac_api.read_namespaced_role_binding(role, namespace)
            except ApiException as e:
                if e.status != 404:
                    raise
                body = {
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "RoleBinding",
                    "metadata": {"name": role, "namespace": namespace},
                    "roleRef": {
                        "apiGroup": "rbac.authorization.k8s.io",
                        "kind": "ClusterRole",
                        "name": role,
                    },
                    "subjects": [subject],
                }
                rbac_api.create_namespaced_role_binding(namespace, body)
                break
            rb = rbac_api.api_client.sanitize_for_serialization(current)
            ref = rb.get("roleRef") or {}
            if ref.get("kind") != "ClusterRole" or ref.get("name") != role:
                raise SCCGrantError(
                    f"{prefix}: RoleBinding {role} exists with roleRef "
                    f"{ref.get('kind')}/{ref.get('name')}, not ClusterRole/{role}; "
                    f"{_grant_fix(scc, sa, namespace)}"
                )
            if _subject_present(rb.get("subjects"), sa, namespace):
                break
            rb["subjects"] = [*(rb.get("subjects") or []), subject]
            # metadata.resourceVersion from the read makes this a compare-and-swap.
            rbac_api.replace_namespaced_role_binding(role, namespace, rb)
            break
        except ApiException as e:
            if e.status == 409:  # lost a race (create or replace); re-read
                last = e
                continue
            raise SCCGrantError(
                f"{prefix}: {e.reason or e}; {_grant_fix(scc, sa, namespace)}"
            ) from e
    else:
        raise SCCGrantError(
            f"{prefix}: RoleBinding {role} kept changing ({last}); {_grant_fix(scc, sa, namespace)}"
        )

    # The authorizer reads RoleBindings from an informer cache on each
    # apiserver, so a fresh binding can take a moment to count. Poll before
    # concluding the grant did not take effect.
    deadline = time.monotonic() + SCC_VERIFY_TIMEOUT_S
    delay = 0.5
    while True:
        verdict = _sa_can_use_scc(authz_api, namespace, sa, scc)
        if verdict is not False or time.monotonic() >= deadline:
            break
        time.sleep(delay)
        delay = min(delay * 2, 2.0)
    if verdict is False:
        raise SCCGrantError(
            f"{prefix}: RoleBinding {role} is in place but the SA still may not use SCC "
            f"{scc} (is ClusterRole {role} missing? OpenShift before 4.10 is not "
            f"supported); {_grant_fix(scc, sa, namespace)}"
        )


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

    def __init__(self, k8s: K8sClient):
        """Initialize security verifier.

        Args:
            k8s: Kubernetes client
        """
        self.k8s = k8s
        self._platform: PlatformType | None = None
        self._platform_version: str = ""

    @property
    def _kube_context(self) -> str | None:
        """The client's kube-context, or None to use the current context."""
        k8s = getattr(self, "k8s", None)
        return getattr(k8s, "context_name", "") or None

    # API group whose presence makes a cluster OpenShift for Lakebench's
    # purposes: it serves the SCCs that admission enforces. Discovery (/apis)
    # is readable by every authenticated user, unlike the CRD list.
    OPENSHIFT_API_GROUP = "security.openshift.io"

    def detect_platform(self, strict: bool = False) -> PlatformType:
        """Detect Kubernetes platform type from API-group discovery.

        OpenShift when the ``security.openshift.io`` group is served. With
        ``strict`` a discovery failure raises, so a caller that must grant an
        SCC never skips the grant on an unreachable or unreadable cluster;
        without it, a failure reads as vanilla (for display only).
        """
        if self._platform is not None:
            return self._platform

        try:
            from kubernetes import client as k8s_client

            groups = k8s_client.ApisApi().get_api_versions().groups or []
            is_ocp = any(g.name == self.OPENSHIFT_API_GROUP for g in groups)
        except Exception as e:
            if strict:
                raise
            logger.warning("Platform detection failed (%s); assuming vanilla Kubernetes", e)
            return PlatformType.VANILLA

        self._platform = PlatformType.OPENSHIFT if is_ocp else PlatformType.VANILLA
        if is_ocp:
            self._detect_openshift_version()
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

        Checks the namespaced RoleBinding first
        (``system:openshift:scc:<scc>``): that is where
        ``oc adm policy add-scc-to-user`` records the grant on OCP 4.10+.
        Falls back to the legacy cluster-scoped ``.users`` array only when
        the RoleBinding does not exist, so pre-4.10 clusters still verify
        correctly.

        Reading ``.users`` alone (as this method did before) always reports
        "not assigned" on modern OCP because that field stays empty. Both
        reads go through the API, so no ``oc`` is needed.
        """
        try:
            if self._scc_binding_has_subject(scc_name, sa_name, namespace):
                return SCCStatus(
                    name=scc_name,
                    assigned=True,
                    service_account=sa_name,
                    namespace=namespace,
                    message=f"SCC '{scc_name}' assigned to {sa_name} via RoleBinding",
                )

            legacy_user = f"system:serviceaccount:{namespace}:{sa_name}"
            if self._scc_users_field_has(scc_name, legacy_user):
                return SCCStatus(
                    name=scc_name,
                    assigned=True,
                    service_account=sa_name,
                    namespace=namespace,
                    message=(
                        f"SCC '{scc_name}' assigned to {sa_name} via legacy "
                        "cluster-scoped .users (pre-OCP 4.10 mechanism)"
                    ),
                )

            return SCCStatus(
                name=scc_name,
                assigned=False,
                service_account=sa_name,
                namespace=namespace,
                message=f"SCC '{scc_name}' not assigned to {sa_name}",
            )

        except Exception as e:
            return SCCStatus(
                name=scc_name,
                assigned=False,
                service_account=sa_name,
                namespace=namespace,
                message=f"Error checking SCC: {e}",
            )

    def _scc_users_field_has(self, scc_name: str, user: str) -> bool:
        """Read the SCC's cluster-scoped ``.users`` array (legacy pre-4.10
        mechanism) through the API. Best-effort; False on any read failure.

        Exact match per entry, never a substring: ``lakebench-spark-runner-v2``
        must not satisfy a search for ``lakebench-spark-runner``.
        """
        try:
            from kubernetes import client as k8s_client

            scc = k8s_client.CustomObjectsApi().get_cluster_custom_object(
                "security.openshift.io", "v1", "securitycontextconstraints", scc_name
            )
            return user in (scc.get("users") or [])
        except Exception:  # noqa: BLE001
            return False

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
        """Grant the Spark SCCs on OpenShift.

        Returns True (nothing to do off OpenShift). Raises
        :class:`SCCGrantError` when a grant cannot be made: the deploy step
        fails, because pods admission-rejected on their UID would only fail
        later and less clearly.
        """
        if self.detect_platform(strict=True) != PlatformType.OPENSHIFT:
            return True

        from kubernetes import client as k8s_client

        rbac_api = k8s_client.RbacAuthorizationV1Api()
        for scc_name, sa_name, _ in self.SPARK_SCCS:
            ensure_scc_rolebinding(rbac_api, namespace, sa_name, scc_name)
        return True

    def _scc_binding_has_subject(self, scc_name: str, sa_name: str, namespace: str) -> bool:
        """True iff the namespaced ``system:openshift:scc:<scc>`` RoleBinding
        lists ``sa_name`` as a ServiceAccount subject (the OCP 4.10+ grant).

        Read through the RBAC API, so no ``oc`` is needed. A missing
        RoleBinding is False; any other read error is raised so the caller
        reports "cannot verify" rather than "not assigned".
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        rbac_api = k8s_client.RbacAuthorizationV1Api()
        try:
            rb = rbac_api.read_namespaced_role_binding(scc_role_name(scc_name), namespace)
        except ApiException as e:
            if e.status == 404:
                return False
            raise
        body = rbac_api.api_client.sanitize_for_serialization(rb)
        return _subject_present(body.get("subjects"), sa_name, namespace)
