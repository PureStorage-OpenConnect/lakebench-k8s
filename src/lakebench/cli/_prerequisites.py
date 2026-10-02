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
    #: What the check records in the run's provenance (the capacity check's
    #: ``provenance.preflight``), or None.
    record: dict[str, Any] | None = None


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

    @property
    def preflight(self) -> dict[str, Any] | None:
        """The capacity check's record, for ``provenance.preflight``."""
        for c in self.checks:
            if c.record is not None:
                return c.record
        return None


#: ``provenance.preflight`` of a run started with ``--skip-preflight``.
PREFLIGHT_SKIPPED: dict[str, Any] = {
    "capacity": "skipped",
    "scratch": "skipped",
    "scratch_reason": "--skip-preflight (every prerequisite check was skipped)",
    "storage_class": None,
}


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
    # OpenShift SCC ClusterRole and S3; the deploy-phase entries are left to
    # deploy. docs/prerequisites.md is generated from the same entries.
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


def _unreadable_config(e: Exception) -> PrereqResult:
    """The capacity check's failure when the config's request cannot be read."""
    return PrereqResult(
        name="cluster-capacity",
        passed=False,
        message=f"Capacity check cannot read the config: {type(e).__name__}: {e}",
        hint=(
            "Fix the value it names: memory and CPU are Kubernetes quantities "
            "(16Gi, 4G, 500m, 2), Spark heaps are Spark sizes (16g)."
        ),
    )


def deploy_capacity_check(cfg) -> PrereqResult:
    """The capacity check ``deploy`` runs before it creates anything.

    Read-only: ``run``'s check (``config.sizing.check_capacity``, which
    sizes a copy of the config against the cluster as ``run`` does) in the
    config's own mode, without datagen. Deploy does not generate, so it
    refuses only a config whose pipeline and always-on pods cannot fit;
    ``run`` checks datagen too when it creates datagen pods.

    It refuses on free capacity, as ``run`` does. Capacity it cannot read
    (an unreachable cluster, a node or pod list it may not read) is a
    warning, not a refusal: deploy has no ``--skip-preflight``, it measures
    nothing, and ``run``'s preflight fails closed before anything is
    measured. A config value it cannot read still refuses.
    """
    return _check_cluster_capacity(cfg, datagen_runs=False, fail_closed=False)


def _check_cluster_capacity(
    cfg,
    *,
    sustained: bool | None = None,
    datagen_runs: bool = True,
    sizing_capacity: Any = SAME_CAPACITY,
    fail_closed: bool = True,
) -> PrereqResult:
    """Check that the cluster's free capacity holds the config's floor request.

    Without this, an undersized cluster produces Pending pods and a job
    timeout tens of minutes later with no explanation. Comparing the
    request against what the cluster can still take turns that into an
    immediate, actionable error.

    The decision is ``config.sizing.check_capacity``, the one sizing source
    ``info``, ``config show``, ``recommend`` and the docs tables also use,
    applied to the schedulable nodes' free capacity
    (``K8sClient.get_free_capacity``): allocatable minus what other pods
    already request. It checks that the floor (the Spark peak, or one batch
    datagen pod, plus the always-on pods and continuous datagen) fits, and
    that the largest pod fits the node with the most free memory. A batch
    datagen Job too large to run all at once passes with a warning: its
    pods queue.

    It fails closed: capacity that cannot be read (a forbidden or failed
    node or pod list, a pod on a node the list does not show, a node or pod
    quantity it cannot parse, no schedulable node) is a failed check, not a
    pass. A config value it cannot read fails the check too, whether or not
    the cluster can be read. Scratch is checked against the
    ``CSIStorageCapacity`` its StorageClass publishes; when none is
    published the run is admitted with a warning and the record says
    ``not_measurable``. With *fail_closed* False (``deploy``), capacity that
    cannot be read passes with a warning instead.
    """
    from lakebench.config.sizing import plan_requirements
    from lakebench.k8s import get_k8s_client
    from lakebench.k8s.client import CapacityUnknown
    from lakebench.k8s.target import ContextConflictError

    run_mode = None if sustained is None else ("continuous" if sustained else "batch")
    # The request comes from the config alone, so read it before the cluster:
    # a value the plan cannot read fails the check even when the cluster
    # cannot be read either.
    try:
        plan_requirements(cfg, run_mode=run_mode, datagen_runs=datagen_runs)
    except Exception as e:
        return _unreadable_config(e)

    record: dict[str, Any] = {
        "capacity": "checked",
        "scratch": "disabled",
        "scratch_reason": None,
        "storage_class": None,
    }

    def _count_own(pod: Any) -> bool:
        # The deployment's own pods are left out (the plan counts its
        # Trino, catalog and Postgres), except what the plan does not:
        # Spark pods left by an earlier run, and a datagen Job still running
        # when this run does not count datagen itself.
        labels = getattr(pod.metadata, "labels", None) or {}
        if "spark-role" in labels:
            return True
        is_job = "job-name" in labels or "batch.kubernetes.io/job-name" in labels
        return is_job and not datagen_runs

    try:
        k8s = get_k8s_client(
            context=cfg.platform.kubernetes.context,
            namespace=cfg.get_namespace(),
        )
        found = k8s.get_free_capacity(exclude_namespace=cfg.get_namespace(), count_own=_count_own)
    except ContextConflictError:
        raise  # exit 3 (context.changed), as everywhere else
    except Exception as e:  # noqa: BLE001 -- unreachable cluster or bad context
        found = CapacityUnknown(f"{type(e).__name__}: {e}")
    if isinstance(found, CapacityUnknown) and not fail_closed:
        return PrereqResult(
            name="cluster-capacity",
            passed=True,
            message=f"WARNING: capacity could not be read ({found.reason}) -- check skipped",
            hint="run's preflight checks it again and refuses until it can be read",
        )
    if isinstance(found, CapacityUnknown):
        return PrereqResult(
            name="cluster-capacity",
            passed=False,
            message=f"capacity could not be read: {found.reason}",
            hint=(
                "Next: ask the cluster admin for node and pod read access, or run "
                "with --skip-preflight (the run records 'capacity not checked')"
            ),
        )
    try:
        return _decide_capacity(cfg, k8s, found, record, sustained, datagen_runs, sizing_capacity)
    except ContextConflictError:
        raise
    except Exception as e:  # noqa: BLE001 -- fail closed on a broken estimate too
        logger.debug("Capacity check error: %s", e, exc_info=True)
        return PrereqResult(
            name="cluster-capacity",
            passed=False,
            message=f"capacity check failed: {type(e).__name__}: {e}",
            hint="Next: report this, or run with --skip-preflight (the run records "
            "'capacity not checked')",
        )


def _decide_capacity(
    cfg, k8s, found, record: dict[str, Any], sustained, datagen_runs, sizing_capacity
) -> PrereqResult:
    """The capacity decision on a free capacity that was read."""
    from lakebench.config.sizing import check_capacity

    free, allocatable = found.free, found.allocatable

    scale = cfg.architecture.workload.datagen.scale
    run_mode = None if sustained is None else ("continuous" if sustained else "batch")
    try:
        verdict = check_capacity(
            cfg,
            free,
            run_mode=run_mode,
            datagen_runs=datagen_runs,
            # Sized as run sizes it: against the allocatable total, not what
            # is free, unless run says what it sized against.
            sizing_capacity=allocatable if sizing_capacity is SAME_CAPACITY else sizing_capacity,
            allocatable=allocatable,
            # One pod must fit one node in cores and memory at once; that
            # needs the per-node free list, so it is checked here.
            check_pod=False,
        )
    except Exception as e:
        # Sized against the cluster, the plan can still fail on a value
        # (autosizing to the capacity): the config's fault.
        return _unreadable_config(e)
    plan = verdict.plan
    gib = 1024**3
    shortfalls = list(verdict.shortfalls)
    warnings = list(verdict.warnings)

    pod = plan.largest_pod
    pod_short = not found.pod_fits(pod.cpu_cores, pod.memory_gb)
    if pod_short:
        most_cores = max((c for c, _ in found.free_by_node), default=0) / 1000.0
        most_gb = max((m for _, m in found.free_by_node), default=0) / gib
        shortfalls.append(
            f"Largest pod needs {pod.cpu_cores:g} cores ({pod.cpu_from}) and "
            f"{pod.memory_gb:g} GB ({pod.memory_from}) on one node; no schedulable node has "
            f"both free (most free: {most_cores:.1f} cores, {most_gb:.1f} GB)"
        )

    scratch_short = False
    if plan.scratch_enabled and plan.scratch_gb > 0:
        sc = plan.scratch_storage_class
        record["storage_class"] = sc
        published = k8s.get_scratch_capacity(sc)
        if published.total_bytes is None:
            record["scratch"] = "not_measurable"
            record["scratch_reason"] = published.reason
            warnings.append(
                f"scratch capacity not measurable for StorageClass {sc} "
                f"({published.reason}); the run requests {plan.scratch_gb:,} Gi of "
                "scratch PVCs"
            )
        else:
            record["scratch"] = "checked"
            have_gi = published.total_bytes / gib
            if plan.scratch_gb > have_gi:
                scratch_short = True
                shortfalls.append(
                    f"Scratch: need {plan.scratch_gb:,} Gi of StorageClass {sc}, "
                    f"its CSIStorageCapacity totals {have_gi:,.0f} Gi"
                )

    free_cores = free.total_cpu_millicores / 1000.0
    free_gb = free.total_memory_bytes / gib
    alloc_cores = allocatable.total_cpu_millicores / 1000.0
    alloc_gb = allocatable.total_memory_bytes / gib
    summary = (
        f"scale {scale} ({plan.mode}) needs ~{plan.floor.cpu_cores} cores / "
        f"{plan.floor.memory_gb} GB, driven by {plan.floor_driver} plus "
        f"{plan.co_resident.label}"
    )
    if plan.overrides_not_counted:
        summary += "; per-job executor overrides are not counted"
    hint_lines = "\n".join(f"  {s}" for s in shortfalls)

    if verdict.status == "refused" or scratch_short or pod_short:
        return PrereqResult(
            name="cluster-capacity",
            passed=False,
            message=f"Insufficient free cluster capacity -- {summary}",
            hint=(
                hint_lines
                + f"\nCluster: {free.node_count} schedulable node(s), {free_cores:.1f} cores / "
                + f"{free_gb:.1f} GB free of {alloc_cores:.1f} cores / {alloc_gb:.1f} GB "
                + "allocatable."
                + "\nReduce 'scale', free the cluster, or use a larger one."
                + (
                    "\nA finished datagen Job is not counted: run 'lakebench "
                    "generate' first, then 'lakebench run --skip-generate' within "
                    "an hour of it finishing (the Job is deleted after 3600 s, and "
                    "an absent Job is counted as running)."
                    if plan.co_resident.includes_datagen
                    else ""
                )
            ),
            record=record,
        )

    for warning in warnings:
        logger.warning("Capacity: %s", warning)
    if verdict.status == "degraded":
        names = ", ".join(verdict.capped)
        capped = verdict.capped_request or plan.floor
        logger.warning("Continuous streams will be capped to fit the cluster: %s", names)
        message = (
            f"WARNING: free capacity below the full request ({summary}); "
            f"running degraded at ~{capped.cpu_cores} cores / "
            f"{capped.memory_gb} GB with {plan.co_resident.label}, capped: {names}"
        )
    else:
        message = (
            f"Cluster capacity OK ({free_cores:.0f} cores / {free_gb:.0f} GB free of "
            f"{alloc_cores:.0f} / {alloc_gb:.0f} allocatable, {summary})"
        )
    for warning in warnings:
        message += f"; WARNING: {warning}"
    return PrereqResult(
        name="cluster-capacity", passed=True, message=message, hint=hint_lines, record=record
    )
