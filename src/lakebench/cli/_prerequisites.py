"""Prerequisite detection for Lakebench run command.

Performs 8 checks before deploying or running the pipeline, each with
an actionable error message if the check fails.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class PrereqResult:
    """Result of a single prerequisite check."""

    name: str
    passed: bool
    message: str
    hint: str = ""


@dataclass
class PrereqReport:
    """Results of all prerequisite checks."""

    checks: list[PrereqResult] = field(default_factory=list)

    @property
    def all_passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failed(self) -> list[PrereqResult]:
        return [c for c in self.checks if not c.passed]


def run_prerequisites(cfg) -> PrereqReport:
    """Run all 8 prerequisite checks.

    Returns a PrereqReport with results for each check. Does not exit
    on failure -- the caller decides how to handle failures.
    """
    report = PrereqReport()

    # 1. kubectl accessible
    report.checks.append(_check_kubectl())

    # 2. Helm available
    report.checks.append(_check_helm())

    # 3. K8s cluster reachable
    report.checks.append(_check_k8s_cluster(cfg))

    # 4. S3 endpoint configured
    report.checks.append(_check_s3_config(cfg))

    # 5. S3 connectivity + credentials
    report.checks.append(_check_s3_connectivity(cfg))

    # 6. Spark Operator installed
    report.checks.append(_check_spark_operator(cfg))

    # 7. Stackable operators (Hive only)
    if cfg.architecture.catalog.type.value == "hive":
        report.checks.append(_check_stackable_operators(cfg))

    # 8. Namespace writable
    report.checks.append(_check_namespace(cfg))

    return report


def _check_kubectl() -> PrereqResult:
    """Check that kubectl is on PATH and executable."""
    if shutil.which("kubectl"):
        return PrereqResult(
            name="kubectl",
            passed=True,
            message="kubectl found on PATH",
        )
    return PrereqResult(
        name="kubectl",
        passed=False,
        message="kubectl not found",
        hint="Install kubectl: https://kubernetes.io/docs/tasks/tools/",
    )


def _check_helm() -> PrereqResult:
    """Check that helm is on PATH (needed for Spark Operator + observability)."""
    if shutil.which("helm"):
        return PrereqResult(
            name="helm",
            passed=True,
            message="helm found on PATH",
        )
    return PrereqResult(
        name="helm",
        passed=False,
        message="helm not found",
        hint="Install helm: https://helm.sh/docs/intro/install/",
    )


def _check_k8s_cluster(cfg) -> PrereqResult:
    """Check that the K8s cluster is reachable."""
    try:
        from lakebench.k8s import get_k8s_client

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        ok, msg = k8s.test_connectivity()
        if ok:
            return PrereqResult(
                name="k8s-cluster",
                passed=True,
                message="Kubernetes cluster reachable",
            )
        return PrereqResult(
            name="k8s-cluster",
            passed=False,
            message=f"Cluster not reachable: {msg}",
            hint="Check kubectl context: kubectl config current-context",
        )
    except Exception as e:
        return PrereqResult(
            name="k8s-cluster",
            passed=False,
            message=f"K8s connection failed: {e}",
            hint="Check kubectl context and cluster connectivity",
        )


def _check_s3_config(cfg) -> PrereqResult:
    """Check that S3 endpoint and credentials are configured."""
    s3 = cfg.platform.storage.s3
    if not s3.endpoint:
        return PrereqResult(
            name="s3-config",
            passed=False,
            message="S3 endpoint not configured",
            hint="Set 'endpoint' in config or platform.storage.s3.endpoint",
        )
    has_creds = bool(s3.access_key and s3.secret_key) or bool(getattr(s3, "secret_ref", None))
    if not has_creds:
        return PrereqResult(
            name="s3-config",
            passed=False,
            message="S3 credentials not configured",
            hint="Set access_key/secret_key or secret_ref in config",
        )
    return PrereqResult(
        name="s3-config",
        passed=True,
        message=f"S3 configured: {s3.endpoint}",
    )


def _check_s3_connectivity(cfg) -> PrereqResult:
    """Test S3 connectivity by listing buckets."""
    s3 = cfg.platform.storage.s3
    if not s3.endpoint or not s3.access_key:
        return PrereqResult(
            name="s3-connectivity",
            passed=False,
            message="S3 not configured (skipping connectivity test)",
            hint="Configure S3 first",
        )
    try:
        from lakebench.s3 import test_s3_connectivity

        ok, msg = test_s3_connectivity(
            endpoint=s3.endpoint,
            access_key=s3.access_key,
            secret_key=s3.secret_key,
            region=s3.region,
            path_style=s3.path_style,
        )
        if ok:
            return PrereqResult(
                name="s3-connectivity",
                passed=True,
                message="S3 credentials valid (ListBuckets OK)",
            )
        return PrereqResult(
            name="s3-connectivity",
            passed=False,
            message=f"S3 connection failed: {msg}",
            hint="Check endpoint URL, credentials, and network access",
        )
    except Exception as e:
        return PrereqResult(
            name="s3-connectivity",
            passed=False,
            message=f"S3 test error: {e}",
            hint="Check endpoint URL and credentials",
        )


def _check_spark_operator(cfg) -> PrereqResult:
    """Check that the Spark Operator CRD exists."""
    try:
        from kubernetes import client as k8s_client

        api_ext = k8s_client.ApiextensionsV1Api()
        crds = api_ext.list_custom_resource_definition()
        crd_names = {crd.metadata.name for crd in crds.items}
        if "sparkapplications.sparkoperator.k8s.io" in crd_names:
            return PrereqResult(
                name="spark-operator",
                passed=True,
                message="Spark Operator CRD found",
            )
        op_install = getattr(
            getattr(cfg.platform.compute, "spark", None),
            "operator",
            None,
        )
        auto = getattr(op_install, "install", False) if op_install else False
        if auto:
            return PrereqResult(
                name="spark-operator",
                passed=True,
                message="Spark Operator not found (will auto-install)",
            )
        return PrereqResult(
            name="spark-operator",
            passed=False,
            message="Spark Operator not installed",
            hint=(
                "Install: helm install spark-operator oci://ghcr.io/kubeflow/helm-charts/spark-operator "
                "--version 2.4.0 --namespace spark-operator --create-namespace\n"
                "Or set platform.compute.spark.operator.install: true"
            ),
        )
    except Exception as e:
        return PrereqResult(
            name="spark-operator",
            passed=False,
            message=f"CRD check failed: {e}",
            hint="Check K8s connectivity",
        )


def _check_stackable_operators(cfg) -> PrereqResult:
    """Check Stackable operator CRDs (Hive catalog only)."""
    try:
        from kubernetes import client as k8s_client

        api_ext = k8s_client.ApiextensionsV1Api()
        crds = api_ext.list_custom_resource_definition()
        crd_names = {crd.metadata.name for crd in crds.items}
        required = {
            "hiveclusters.hive.stackable.tech": "hive-operator",
            "secretclasses.secrets.stackable.tech": "secret-operator",
        }
        missing = [op for crd, op in required.items() if crd not in crd_names]
        if not missing:
            return PrereqResult(
                name="stackable-operators",
                passed=True,
                message="Stackable operators found",
            )
        op_install = getattr(
            getattr(getattr(cfg.architecture.catalog, "hive", None), "operator", None),
            "install",
            False,
        )
        if op_install:
            return PrereqResult(
                name="stackable-operators",
                passed=True,
                message=f"Missing {', '.join(missing)} (will auto-install)",
            )
        return PrereqResult(
            name="stackable-operators",
            passed=False,
            message=f"Missing Stackable operators: {', '.join(missing)}",
            hint=(
                "Install operators, or switch to Polaris recipe (no operators needed):\n"
                "  recipe: polaris-iceberg-spark-trino"
            ),
        )
    except Exception as e:
        return PrereqResult(
            name="stackable-operators",
            passed=False,
            message=f"CRD check failed: {e}",
            hint="Check K8s connectivity",
        )


def _check_namespace(cfg) -> PrereqResult:
    """Check that the namespace exists or can be created."""
    ns = cfg.get_namespace()
    create = cfg.platform.kubernetes.create_namespace
    try:
        from lakebench.k8s import get_k8s_client

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=ns,
        )
        if k8s.namespace_exists(ns):
            return PrereqResult(
                name="namespace",
                passed=True,
                message=f"Namespace '{ns}' exists",
            )
        if create:
            return PrereqResult(
                name="namespace",
                passed=True,
                message=f"Namespace '{ns}' will be created on deploy",
            )
        return PrereqResult(
            name="namespace",
            passed=False,
            message=f"Namespace '{ns}' does not exist",
            hint=f"Create it: kubectl create namespace {ns}\nOr set create_namespace: true",
        )
    except Exception as e:
        return PrereqResult(
            name="namespace",
            passed=False,
            message=f"Namespace check failed: {e}",
            hint="Check K8s connectivity",
        )
