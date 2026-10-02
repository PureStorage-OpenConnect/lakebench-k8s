"""Prerequisite detection for Lakebench run command.

Checks run before deploying or running the pipeline, each with an
actionable error message if it fails. The cluster-side checks come from the
shared registry in ``lakebench.deploy.prereqs``.
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


def run_prerequisites(
    cfg, *, sustained: bool | None = None, datagen_runs: bool = True
) -> PrereqReport:
    """Run the prerequisite checks.

    Returns a PrereqReport with results for each check. Does not exit
    on failure -- the caller decides how to handle failures.

    ``sustained`` overrides ``pipeline.mode`` for the capacity check (the
    ``run --sustained`` flag does not write back to the config), and
    ``datagen_runs=False`` (``--skip-generate``) leaves datagen out of it.
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
        _check_cluster_capacity(cfg, sustained=sustained, datagen_runs=datagen_runs)
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


def _co_resident_request(
    cfg, sustained: bool, *, datagen_runs: bool = True
) -> tuple[int, int, str]:
    """(cores, GB, label) held by pods that run beside the Spark jobs (LB-155).

    The query engine, catalog and Postgres are always on. Datagen runs
    concurrently with the streams in continuous mode unless the run skips
    generation; in batch it finishes before the Spark jobs start, so it is
    not counted there.
    """
    from lakebench.config.autosizer import (
        _co_resident_cpu_m,
        _parse_cpu_millicores,
        _parse_memory_gi,
    )
    from lakebench.deps.manifest import POD_REQUEST_MEMORY_MI

    qe = cfg.architecture.query_engine
    engine = qe.type.value
    # Includes the lb-deps pod's CPU reservation; its memory is added here.
    cpu_m = _co_resident_cpu_m(cfg)
    mem_gi = POD_REQUEST_MEMORY_MI / 1024
    parts = ["catalog/Postgres", "lb-deps"]
    if engine == "trino":
        mem_gi += _parse_memory_gi(qe.trino.coordinator.memory)
        mem_gi += qe.trino.worker.replicas * _parse_memory_gi(qe.trino.worker.memory)
        parts.insert(0, "Trino")
    elif engine == "spark-thrift":
        mem_gi += _parse_memory_gi(qe.spark_thrift.memory)
        parts.insert(0, "Spark Thrift")
    elif engine == "duckdb":
        mem_gi += _parse_memory_gi(qe.duckdb.memory)
        parts.insert(0, "DuckDB")
    if sustained and datagen_runs:
        dg = cfg.architecture.workload.datagen
        cpu_m += dg.parallelism * _parse_cpu_millicores(dg.cpu)
        mem_gi += dg.parallelism * _parse_memory_gi(dg.memory)
        parts.append("datagen")
    return -(-cpu_m // 1000), int(-(-mem_gi // 1)), ", ".join(parts)


def _check_cluster_capacity(
    cfg, *, sustained: bool | None = None, datagen_runs: bool = True
) -> PrereqResult:
    """Check that the cluster can schedule the pipeline's peak request.

    Without this, an undersized cluster produces Pending pods and a job
    timeout tens of minutes later with no explanation. Comparing the peak
    request against allocatable capacity turns that into an immediate,
    actionable error.

    Checks two things:
      1. Aggregate capacity across worker nodes covers the peak request.
      2. The largest single pod fits on the largest single node. A cluster
         can have 200 GB spread over 10 nodes and still never schedule a
         60 GB executor.
    """
    try:
        from lakebench.k8s import get_k8s_client
        from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

        scale = cfg.architecture.workload.datagen.scale
        raw_mode = cfg.architecture.pipeline.mode
        mode = getattr(raw_mode, "value", raw_mode)
        if sustained is not None:
            mode = "continuous" if sustained else "batch"
        raw_schema = getattr(cfg.architecture.workload, "schema_type", None)
        schema = getattr(raw_schema, "value", raw_schema)
        peak = compute_peak_requirements(scale, mode, schema)

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

        gib = 1024**3
        avail_cores = capacity.total_cpu_millicores / 1000.0
        avail_gb = capacity.total_memory_bytes / gib
        node_cores = capacity.largest_node_cpu_millicores / 1000.0
        node_gb = capacity.largest_node_memory_bytes / gib

        # The pipeline peak alone understates the request: the query engine,
        # catalog/Postgres and (continuous) datagen hold their cores for the
        # whole run (LB-155).
        from lakebench.config.schema import is_continuous_mode

        is_sustained = is_continuous_mode(mode)
        co_cores, co_gb, co_label = _co_resident_request(
            cfg, is_sustained, datagen_runs=datagen_runs
        )
        need_cores = peak.cpu_cores + co_cores
        need_gb = peak.memory_gb + co_gb

        shortfalls = []
        if need_cores > avail_cores:
            shortfalls.append(
                f"CPU: need {need_cores} cores ({peak.cpu_cores} cores pipeline + "
                f"{co_cores} {co_label}), cluster has {avail_cores:.1f} allocatable"
            )
        if need_gb > avail_gb:
            shortfalls.append(
                f"Memory: need {need_gb} GB ({peak.memory_gb} GB pipeline + "
                f"{co_gb} GB {co_label}), cluster has {avail_gb:.1f} GB allocatable"
            )
        if peak.max_pod_cpu_cores > node_cores:
            shortfalls.append(
                f"Largest pod needs {peak.max_pod_cpu_cores} cores, "
                f"biggest node has {node_cores:.1f}"
            )
        if peak.max_pod_memory_gb > node_gb:
            shortfalls.append(
                f"Largest pod needs {peak.max_pod_memory_gb} GB, biggest node has {node_gb:.1f} GB"
            )

        summary = (
            f"scale {scale} ({mode}) needs ~{need_cores} cores / "
            f"{need_gb} GB, driven by {peak.driving_job} plus {co_label}"
        )

        # Continuous mode: the run caps the streams to the cluster's concurrent
        # budget and warns naming each capped stage, so an aggregate shortfall
        # is fatal only if even the capped request does not fit. A pod that
        # fits no node stays fatal: capping counts does not shrink a pod.
        pod_fits = peak.max_pod_cpu_cores <= node_cores and peak.max_pod_memory_gb <= node_gb
        if shortfalls and pod_fits and is_sustained:
            from lakebench.modules.pipeline_engines.spark.job import (
                streaming_request_under_budget,
            )

            try:
                capped = streaming_request_under_budget(
                    cfg, capacity.total_cpu_millicores, datagen_running=datagen_runs
                )
            except Exception as e:  # fall through to the hard failure below
                logger.debug("Capped continuous request unavailable: %s", e, exc_info=True)
                capped = None
            if (
                capped is not None
                and capped.capped
                and capped.cpu_cores <= avail_cores
                and capped.memory_gb <= avail_gb
            ):
                names = ", ".join(capped.capped)
                logger.warning("Continuous streams will be capped to fit the cluster: %s", names)
                return PrereqResult(
                    name="cluster-capacity",
                    passed=True,
                    message=(
                        f"WARNING: cluster below the full request ({summary}); "
                        f"running degraded at ~{capped.cpu_cores} cores / "
                        f"{capped.memory_gb} GB with Trino and datagen, capped: {names}"
                    ),
                    hint="\n".join(f"  {s}" for s in shortfalls),
                )

        if shortfalls:
            return PrereqResult(
                name="cluster-capacity",
                passed=False,
                message=f"Insufficient cluster capacity -- {summary}",
                hint=(
                    "\n".join(f"  {s}" for s in shortfalls)
                    + f"\nCluster: {capacity.node_count} worker node(s), "
                    + f"{avail_cores:.1f} cores / {avail_gb:.1f} GB allocatable."
                    + "\nReduce 'scale', lower per-job executor counts "
                    + "(silver_executors, gold_executors), or use a larger cluster."
                ),
            )

        return PrereqResult(
            name="cluster-capacity",
            passed=True,
            message=(
                f"Cluster capacity OK ({avail_cores:.0f} cores / "
                f"{avail_gb:.0f} GB available, {summary})"
            ),
        )
    except Exception as e:
        # Never block a deploy because the capacity estimate itself failed.
        logger.debug("Capacity check error: %s", e, exc_info=True)
        return PrereqResult(
            name="cluster-capacity",
            passed=True,
            message=f"Capacity check skipped: {e}",
            hint="Could not determine cluster capacity",
        )
