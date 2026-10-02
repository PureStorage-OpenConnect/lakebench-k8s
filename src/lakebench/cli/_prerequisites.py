"""Prerequisite detection for Lakebench run command.

Checks run before deploying or running the pipeline, each with an
actionable error message if it fails. The cluster-side checks come from the
shared registry in ``lakebench.deploy.prereqs``.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from typing import Any

from lakebench.config.sizing import SAME_CAPACITY

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


def run_prerequisites(
    cfg,
    *,
    sustained: bool | None = None,
    datagen_runs: bool = True,
    sizing_capacity: Any = SAME_CAPACITY,
) -> PrereqReport:
    """Run the prerequisite checks.

    Returns a PrereqReport with results for each check. Does not exit
    on failure -- the caller decides how to handle failures.

    ``sustained`` overrides ``pipeline.mode`` for the capacity check (the
    ``run --sustained`` flag does not write back to the config), and
    ``datagen_runs=False`` (the run creates no datagen pod) leaves datagen
    out of it. ``sizing_capacity`` is the capacity the caller already
    auto-sized *cfg* against (``run`` passes its own fetch, or None when
    that failed), so the check sizes the config as it will deploy.
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

    # 5-7. The shared registry (deploy/prereqs.py): scratch
    # StorageClass, Spark Operator, Stackable (Hive), observability, the
    # OpenShift SCC ClusterRole and S3. docs/prerequisites.md is generated
    # from the same entries, and `plan` runs them too.
    report.checks.extend(_registry_checks(cfg))

    # 8. Namespace writable
    report.checks.append(_check_namespace(cfg))

    # 9. Cluster has capacity to schedule the pipeline
    report.checks.append(
        _check_cluster_capacity(
            cfg,
            sustained=sustained,
            datagen_runs=datagen_runs,
            sizing_capacity=sizing_capacity,
        )
    )

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
    has_creds = bool(s3.access_key and s3.secret_key)
    if not has_creds:
        return PrereqResult(
            name="s3-config",
            passed=False,
            message="S3 credentials not configured",
            hint="Set access_key and secret_key in config",
        )
    return PrereqResult(
        name="s3-config",
        passed=True,
        message=f"S3 configured: {s3.endpoint}",
    )


def _registry_checks(cfg) -> list[PrereqResult]:
    """The registry's checks as preflight results. WARN passes (the message
    says why); FAIL and UNKNOWN fail with the registry's fix as the hint."""
    from lakebench.deploy.prereqs import KubeClusterReader, PrereqStatus, run_prereqs
    from lakebench.k8s import get_k8s_client

    try:
        # Loads the client config exactly as every other cluster call here.
        get_k8s_client(context=cfg.platform.kubernetes.context, namespace=cfg.get_namespace())
        reader = KubeClusterReader(cfg, load_config=False)
    except Exception as e:  # noqa: BLE001
        return [
            PrereqResult(
                name="cluster-prerequisites",
                passed=False,
                message=f"Cluster prerequisites not checked: {e}",
                hint="Check kubectl context and cluster connectivity",
            )
        ]
    results: list[PrereqResult] = []
    for o in run_prereqs(cfg, reader, for_run=True):
        status = o.result.status
        if status is PrereqStatus.SKIPPED:
            continue
        passed = status in (PrereqStatus.OK, PrereqStatus.WARN, PrereqStatus.INFO)
        hint = ""
        if status is PrereqStatus.FAIL:
            hint = o.prereq.fix
        elif status is PrereqStatus.UNKNOWN:
            hint = "Check K8s connectivity and permissions"
        results.append(
            PrereqResult(name=o.prereq.id, passed=passed, message=o.result.message, hint=hint)
        )
    return results


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
            hint=(
                f"Create it: lakebench deploy <config> creates namespace {ns!r} "
                "automatically; ensure your config sets "
                "platform.kubernetes.context to this cluster first. "
                "(Do not pre-create the namespace with kubectl: the operator "
                "watch list is mutated under a cluster lease, and a "
                "pre-created namespace has caused a destroy cascade that "
                "crash-looped the shared Spark Operator.)\n"
                "Or set create_namespace: true"
            ),
        )
    except Exception as e:
        return PrereqResult(
            name="namespace",
            passed=False,
            message=f"Namespace check failed: {e}",
            hint="Check K8s connectivity",
        )


def _check_cluster_capacity(
    cfg,
    *,
    sustained: bool | None = None,
    datagen_runs: bool = True,
    sizing_capacity: Any = SAME_CAPACITY,
) -> PrereqResult:
    """Check that the cluster can schedule the config's floor request.

    Without this, an undersized cluster produces Pending pods and a job
    timeout tens of minutes later with no explanation. Comparing the
    request against allocatable capacity turns that into an immediate,
    actionable error.

    The decision is ``config.sizing.check_capacity``, the one
    sizing source ``info``, ``config show``, ``recommend`` and the
    docs tables also use. It checks that the floor (the Spark peak, or one
    batch datagen pod, plus the always-on pods and continuous datagen) fits
    the cluster, and that the largest pod fits the largest node. A batch
    datagen Job too large to run all at once passes with a warning: its pods
    queue.
    """
    try:
        from lakebench.config.sizing import check_capacity
        from lakebench.k8s import get_k8s_client

        scale = cfg.architecture.workload.datagen.scale
        run_mode = None if sustained is None else ("continuous" if sustained else "batch")

        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        capacity = k8s.get_cluster_capacity()
        if capacity is None:
            return PrereqResult(
                name="cluster-capacity",
                passed=True,
                message="Cluster capacity unknown (node list unavailable) -- skipping check",
                hint="Requires permission to list nodes",
            )

        verdict = check_capacity(
            cfg,
            capacity,
            run_mode=run_mode,
            datagen_runs=datagen_runs,
            sizing_capacity=sizing_capacity,
        )
        plan = verdict.plan
        gib = 1024**3
        avail_cores = capacity.total_cpu_millicores / 1000.0
        avail_gb = capacity.total_memory_bytes / gib
        summary = (
            f"scale {scale} ({plan.mode}) needs ~{plan.floor.cpu_cores} cores / "
            f"{plan.floor.memory_gb} GB, driven by {plan.floor_driver} plus "
            f"{plan.co_resident.label}"
        )
        if plan.overrides_not_counted:
            summary += "; per-job executor and driver overrides are not counted"
        hint_lines = "\n".join(f"  {s}" for s in verdict.shortfalls)

        if verdict.status == "degraded":
            names = ", ".join(verdict.capped)
            capped = verdict.capped_request or plan.floor
            logger.warning("Continuous streams will be capped to fit the cluster: %s", names)
            return PrereqResult(
                name="cluster-capacity",
                passed=True,
                message=(
                    f"WARNING: cluster below the full request ({summary}); "
                    f"running degraded at ~{capped.cpu_cores} cores / "
                    f"{capped.memory_gb} GB with Trino and datagen, capped: {names}"
                ),
                hint=hint_lines,
            )

        if verdict.status == "refused":
            return PrereqResult(
                name="cluster-capacity",
                passed=False,
                message=f"Insufficient cluster capacity -- {summary}",
                hint=(
                    hint_lines
                    + f"\nCluster: {capacity.node_count} worker node(s), "
                    + f"{avail_cores:.1f} cores / {avail_gb:.1f} GB allocatable."
                    + "\nReduce 'scale' or use a larger cluster."
                    + (
                        "\nA finished datagen Job is not counted: run 'lakebench "
                        "generate' first, then 'lakebench run --skip-generate' within "
                        "an hour of it finishing (the Job is deleted after 3600 s, and "
                        "an absent Job is counted as running)."
                        if plan.co_resident.includes_datagen
                        else ""
                    )
                ),
            )

        message = (
            f"Cluster capacity OK ({avail_cores:.0f} cores / "
            f"{avail_gb:.0f} GB available, {summary})"
        )
        for warning in verdict.warnings:
            logger.warning("Capacity: %s", warning)
            message += f"; WARNING: {warning}"
        return PrereqResult(name="cluster-capacity", passed=True, message=message)
    except Exception as e:
        # Never block a deploy because the capacity estimate itself failed.
        logger.debug("Capacity check error: %s", e, exc_info=True)
        return PrereqResult(
            name="cluster-capacity",
            passed=True,
            message=f"Capacity check skipped: {e}",
            hint="Could not determine cluster capacity",
        )
