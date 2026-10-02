"""One sizing source for the capacity preflight, ``info``, ``config show``,
``recommend``, the generated docs tables and, later, ``plan``.

Every figure the CLI or the docs quote for "how big a cluster does this
config need" comes from :func:`plan_requirements`, and every capacity
decision from :func:`check_capacity`. They combine three inputs, none of
which they re-derive:

* the Spark request, from ``compute_peak_requirements()`` in
  ``modules/pipeline_engines/spark/job.py`` (``_JOB_PROFILES`` and the
  per-schema overrides);
* the always-on pods beside the pipeline (query engine, catalog and
  Postgres, and in continuous mode the datagen Job), from the config after
  ``resolve_auto_sizing``;
* the datagen Job, ``parallelism`` pods of ``(cpu, memory)``, after
  ``resolve_auto_sizing`` on a deep copy, with the cluster capacity when one
  is known.

Two totals come out:

* the **floor**, what must fit at once for the run to proceed:
  - batch: ``max(spark, one datagen pod) + always_on``. Datagen runs before
    the Spark jobs, and its Indexed Job is elastic: the Job template sets no
    ``activeDeadlineSeconds``, ``backoffLimit`` counts failed pods, not
    Pending ones, and the generate wait loops treat Pending pods as active,
    so pods the cluster cannot place wait and run as others finish. Only one
    of its pods has to fit;
  - continuous: ``spark + always_on``, with datagen inside ``always_on``
    (while the datagen Job is unfinished the streaming budget
    reserves its cores);
* the **full request**, everything at the configured parallelism at once:
  batch ``max(spark, datagen) + always_on``; continuous equals the floor.

A batch cluster between the two runs, with datagen pods queueing; the
preflight says so. These are requested resources, not measured
utilisation.
"""

from __future__ import annotations

import contextlib
import logging
import math
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s.client import ClusterCapacity
    from lakebench.modules.pipeline_engines.spark.job import PeakRequirement

__all__ = [
    "BEFORE_CLUSTER_SCALING",
    "SAME_CAPACITY",
    "CapacityVerdict",
    "CoResidentRequest",
    "DatagenRequest",
    "PodRequest",
    "Resources",
    "SizingPlan",
    "breakdown_text",
    "check_capacity",
    "co_resident_request",
    "default_sizing_config",
    "floor_text",
    "largest_fitting_scale",
    "peak_for_config",
    "plan_requirements",
    "plan_shortfalls",
]

#: Label for a datagen request sized without a cluster: the declared or
#: default parallelism, which ``run`` may still cap or raise to fit the
#: cluster it finds.
BEFORE_CLUSTER_SCALING = "before cluster scaling"

#: PostgreSQL pod memory request (templates/postgres/statefulset.yaml.j2).
POSTGRES_MEMORY_GI = 1.0

#: The Spark overrides the peak does not count yet: the per-job
#: executor counts, by pipeline mode, and the global driver overrides the
#: manifests apply to every job. A plan whose config sets one says so.
_DRIVER_OVERRIDE_FIELDS = ("driver_cores", "driver_memory")
_OVERRIDE_FIELDS = {
    "batch": ("bronze_executors", "silver_executors", "gold_executors", *_DRIVER_OVERRIDE_FIELDS),
    "continuous": (
        "bronze_ingest_executors",
        "silver_stream_executors",
        "gold_refresh_executors",
        *_DRIVER_OVERRIDE_FIELDS,
    ),
}

#: Passed as ``sizing_capacity`` to mean "size against the same capacity
#: the plan is checked against".
SAME_CAPACITY = object()


@dataclass(frozen=True)
class Resources:
    """A CPU and memory request, rounded up to whole cores and GB."""

    cpu_cores: int
    memory_gb: int


@dataclass(frozen=True)
class PodRequest:
    """The largest single pod. CPU and memory are maximised separately
    (the pod with the most CPU need not be the one with the most memory),
    so ``cpu_from`` and ``memory_from`` name the pod behind each."""

    cpu_cores: float
    memory_gb: float
    cpu_from: str
    memory_from: str


@dataclass(frozen=True)
class CoResidentRequest:
    """Pods that hold their request for the whole run beside the Spark jobs."""

    cpu_cores: int
    memory_gb: int
    label: str
    includes_datagen: bool


@dataclass(frozen=True)
class DatagenRequest:
    """The batch datagen Job: ``pods`` pods of ``pod_cpu_cores`` and
    ``pod_memory_gb``. Pods the cluster cannot place queue."""

    pods: int
    pod_cpu_cores: float
    pod_memory_gb: float
    cpu_cores: int
    memory_gb: int
    #: True when sized against a known cluster capacity, as ``run`` sizes
    #: it; False for the declared or default parallelism
    #: (:data:`BEFORE_CLUSTER_SCALING`).
    cluster_scaled: bool


@dataclass(frozen=True)
class SizingPlan:
    """What one config requests from the cluster."""

    workload: str
    mode: str
    scale: float
    spark: PeakRequirement
    co_resident: CoResidentRequest
    #: Batch only, and only when the run generates data; None otherwise.
    datagen: DatagenRequest | None
    #: What must fit at once (module docstring).
    floor: Resources
    #: Everything at the configured parallelism at once.
    full: Resources
    largest_pod: PodRequest
    #: Spark scratch PVC total from the job profiles. Requested only when
    #: ``scratch_enabled``; with scratch off, executors spill to node
    #: storage instead and no PVC is requested.
    scratch_gb: int
    scratch_enabled: bool
    scratch_storage_class: str
    #: The config sets a per-job executor count or a driver override the
    #: Spark peak does not count yet.
    overrides_not_counted: bool
    #: Auto-sizing cuts made to fit ``capacity`` (empty offline).
    cuts: tuple[str, ...]
    basis: tuple[str, ...]

    @property
    def floor_driver(self) -> str:
        """What sets the CPU part of the floor before the always-on pods:
        the driving Spark job, or one batch datagen pod when it is larger."""
        dg = self.datagen
        if dg is not None and _ceil(dg.pod_cpu_cores) > self.spark.cpu_cores:
            return "one datagen pod"
        return self.spark.driving_job


@dataclass(frozen=True)
class CapacityVerdict:
    """The capacity decision for one plan against one cluster.

    ``status`` is ``"fits"``; ``"degraded"`` (continuous only: the full
    request does not fit, but the streams capped to the cluster's budget
    do, and the run caps them and warns); or ``"refused"``.
    """

    status: str
    plan: SizingPlan
    #: One line per missing resource; empty unless refused or degraded.
    shortfalls: tuple[str, ...]
    #: Non-fatal notes: batch datagen pods that will queue, capped streams.
    warnings: tuple[str, ...]
    capped: tuple[str, ...] = ()
    capped_request: Resources | None = None

    @property
    def admitted(self) -> bool:
        return self.status != "refused"


def _mode_of(cfg: LakebenchConfig, run_mode: str | None) -> str:
    from lakebench.config.schema import is_continuous_mode

    raw = run_mode if run_mode is not None else cfg.architecture.pipeline.mode
    return "continuous" if is_continuous_mode(raw) else "batch"


def _schema_of(cfg: LakebenchConfig) -> str:
    raw = cfg.architecture.workload.schema_type
    return str(getattr(raw, "value", raw))


def _ceil(x: float) -> int:
    return int(math.ceil(x - 1e-9))


def _n(count: float, unit: str) -> str:
    return f"{count:g} {unit}" if count == 1 else f"{count:g} {unit}s"


@contextlib.contextmanager
def _quiet_autosizer() -> Iterator[None]:
    """Silence ``resolve_auto_sizing`` while it sizes a private copy.

    The caller has already resolved (and shown the cuts for) its own config;
    the copy repeats the same cuts, and logging them again would print every
    auto-sizing warning twice. The cuts are returned in ``SizingPlan.cuts``
    for callers that show them.
    """
    log = logging.getLogger("lakebench.config.autosizer")
    previous = log.disabled
    log.disabled = True
    try:
        yield
    finally:
        log.disabled = previous


def _resolved_copy(
    cfg: LakebenchConfig, capacity: ClusterCapacity | None
) -> tuple[LakebenchConfig, list[str]]:
    """Deep copy of *cfg* after ``resolve_auto_sizing``; *cfg* is untouched.

    ``model_copy(deep=True)`` keeps each model's ``model_fields_set``, so
    the copy tells user-set fields from defaults exactly as *cfg* does.
    Re-resolving a config that was already resolved against the same
    capacity gives the same plan
    (``tests/test_sizing.py::test_resolved_config_sizes_the_same``). The
    Spark peak comes from the job profiles, not from autosized fields.
    """
    from lakebench.config.autosizer import resolve_auto_sizing

    copy = cfg.model_copy(deep=True)
    with _quiet_autosizer():
        cuts = list(resolve_auto_sizing(copy, capacity) or [])
    return copy, cuts


def peak_for_config(cfg: LakebenchConfig, *, mode: str | None = None) -> PeakRequirement:
    """``compute_peak_requirements`` for *cfg*'s scale, mode and schema.

    *mode* overrides the config's pipeline mode (``run --sustained`` on a
    batch config). Per-job executor overrides are not counted yet;
    ``SizingPlan.overrides_not_counted`` flags a config that sets one.
    """
    from lakebench.modules.pipeline_engines.spark.job import compute_peak_requirements

    scale = cfg.architecture.workload.datagen.get_effective_scale()
    return compute_peak_requirements(scale, _mode_of(cfg, mode), _schema_of(cfg))


def _engine_pods(cfg: LakebenchConfig) -> list[tuple[str, float, float]]:
    """``(name, cpu cores, memory GiB)`` of each query-engine pod."""
    from lakebench.config.autosizer import _parse_cpu_millicores, _parse_memory_gi

    qe = cfg.architecture.query_engine
    engine = qe.type.value
    pods: list[tuple[str, float, float]] = []
    if engine == "trino":
        coord, worker = qe.trino.coordinator, qe.trino.worker
        pods.append(
            (
                "Trino coordinator",
                _parse_cpu_millicores(coord.cpu) / 1000,
                _parse_memory_gi(coord.memory),
            )
        )
        pods.extend(
            (
                "Trino worker",
                _parse_cpu_millicores(worker.cpu) / 1000,
                _parse_memory_gi(worker.memory),
            )
            for _ in range(worker.replicas)
        )
    elif engine == "spark-thrift":
        # The pod requests the heap plus overhead, as deploy renders it. A
        # heap deploy cannot parse (a Kubernetes unit such as "30Gi") fails
        # loudly at deploy; here it is sized as written rather than raising
        # inside the preflight, which would skip the whole check.
        from lakebench.deploy.engine import thrift_pod_memory_limit

        heap = qe.spark_thrift.memory
        try:
            pod_gi = _parse_memory_gi(thrift_pod_memory_limit(heap))
        except ValueError:
            pod_gi = _parse_memory_gi(heap)
        pods.append(("Spark Thrift", float(qe.spark_thrift.cores), pod_gi))
    elif engine == "duckdb":
        pods.append(("DuckDB", float(qe.duckdb.cores), _parse_memory_gi(qe.duckdb.memory)))
    return pods


def _catalog_memory_gi(cfg: LakebenchConfig) -> float:
    """Memory request of the catalog pod plus Postgres."""
    from lakebench.config.autosizer import _parse_memory_gi

    cat = cfg.architecture.catalog
    kind = cat.type.value
    if kind == "hive":
        return _parse_memory_gi(cat.hive.resources.memory) + POSTGRES_MEMORY_GI
    if kind == "polaris":
        return _parse_memory_gi(cat.polaris.resources.memory) + POSTGRES_MEMORY_GI
    return POSTGRES_MEMORY_GI


def co_resident_request(
    cfg: LakebenchConfig, sustained: bool, *, datagen_runs: bool = True
) -> CoResidentRequest:
    """Request of the pods that run beside the Spark jobs.

    The query engine, catalog and Postgres are always on. Datagen runs
    concurrently with the streams in continuous mode unless the run skips
    generation; in batch it finishes before the Spark jobs start, so it is
    not counted here (``plan_requirements`` sets it against the Spark peak).
    CPU for the catalog and Postgres is the autosizer's one-core budget
    (``_co_resident_cpu_m``); their memory is the pods' requests.

    *cfg* should be resolved (``resolve_auto_sizing``) first, as
    ``plan_requirements`` does on its copy.
    """
    from lakebench.config.autosizer import (
        _co_resident_cpu_m,
        _parse_cpu_millicores,
        _parse_memory_gi,
    )
    from lakebench.deps.manifest import POD_REQUEST_MEMORY_MI

    # The CPU budget includes the lb-deps pod's reservation; its memory is
    # added here.
    cpu_m = _co_resident_cpu_m(cfg)
    mem_gi = (
        sum(mem for _, _, mem in _engine_pods(cfg))
        + _catalog_memory_gi(cfg)
        + POD_REQUEST_MEMORY_MI / 1024
    )
    engine = cfg.architecture.query_engine.type.value
    parts = ["catalog/Postgres", "lb-deps"]
    engine_label = {"trino": "Trino", "spark-thrift": "Spark Thrift", "duckdb": "DuckDB"}
    if engine in engine_label:
        parts.insert(0, engine_label[engine])
    with_datagen = sustained and datagen_runs
    if with_datagen:
        dg = cfg.architecture.workload.datagen
        cpu_m += dg.parallelism * _parse_cpu_millicores(dg.cpu)
        mem_gi += dg.parallelism * _parse_memory_gi(dg.memory)
        parts.append("datagen")
    return CoResidentRequest(
        cpu_cores=-(-cpu_m // 1000),
        memory_gb=_ceil(mem_gi),
        label=", ".join(parts),
        includes_datagen=with_datagen,
    )


def _datagen_pod(cfg: LakebenchConfig) -> tuple[int, float, float]:
    """``(pods, cpu cores, memory GiB)`` of the datagen Job in *cfg*."""
    from lakebench.config.autosizer import _parse_cpu_millicores, _parse_memory_gi

    dg = cfg.architecture.workload.datagen
    return (
        int(dg.parallelism),
        _parse_cpu_millicores(dg.cpu) / 1000,
        _parse_memory_gi(dg.memory),
    )


def _plan(
    cfg: LakebenchConfig,
    *,
    run_mode: str | None,
    capacity: ClusterCapacity | None,
    datagen_runs: bool,
) -> tuple[SizingPlan, LakebenchConfig]:
    """*capacity* is what auto-sizing sizes the copy against (None:
    offline, the declared or default parallelism)."""
    from lakebench.config.schema import is_continuous_mode

    mode = _mode_of(cfg, run_mode)
    continuous = is_continuous_mode(mode)
    resolved, cuts = _resolved_copy(cfg, capacity)
    spark = peak_for_config(resolved, mode=mode)
    co = co_resident_request(resolved, continuous, datagen_runs=datagen_runs)
    scale = resolved.architecture.workload.datagen.get_effective_scale()
    scaled = "cluster-scaled" if capacity is not None else BEFORE_CLUSTER_SCALING

    basis = [
        f"spark: compute_peak_requirements({scale:g}, {mode!r}, {_schema_of(resolved)!r}), "
        f"driven by {spark.driving_job or 'no job'}",
        f"always on: {co.label}",
    ]

    datagen: DatagenRequest | None = None
    pods, pod_cpu, pod_mem = _datagen_pod(resolved)
    if datagen_runs and not continuous:
        datagen = DatagenRequest(
            pods=pods,
            pod_cpu_cores=pod_cpu,
            pod_memory_gb=pod_mem,
            cpu_cores=_ceil(pods * pod_cpu),
            memory_gb=_ceil(pods * pod_mem),
            cluster_scaled=capacity is not None,
        )
        basis.append(
            f"datagen: {pods} pods x {pod_cpu:g} cores / {pod_mem:g} GiB ({scaled}); "
            "runs before the Spark jobs; pods that do not fit queue, so one pod "
            "counts in the floor and all of them in the full request"
        )
        floor = Resources(
            cpu_cores=max(spark.cpu_cores, _ceil(pod_cpu)) + co.cpu_cores,
            memory_gb=max(spark.memory_gb, _ceil(pod_mem)) + co.memory_gb,
        )
        full = Resources(
            cpu_cores=max(spark.cpu_cores, datagen.cpu_cores) + co.cpu_cores,
            memory_gb=max(spark.memory_gb, datagen.memory_gb) + co.memory_gb,
        )
    else:
        if continuous and datagen_runs:
            basis.append(
                f"datagen: {pods} pods x {pod_cpu:g} cores / {pod_mem:g} GiB ({scaled}); "
                "counted beside the streams while its Job runs (always-on line)"
            )
        elif not datagen_runs:
            basis.append("datagen: not counted (the run skips generation)")
        floor = full = Resources(
            cpu_cores=spark.cpu_cores + co.cpu_cores,
            memory_gb=spark.memory_gb + co.memory_gb,
        )

    # Largest single pod across everything the deployment requests: the
    # Spark driver and executors, the datagen pods when they run, and the
    # query-engine pods. CPU and memory are maximised separately.
    candidates: list[tuple[str, float, float]] = [
        ("Spark pod", float(spark.max_pod_cpu_cores), float(spark.max_pod_memory_gb))
    ]
    if datagen_runs:
        candidates.append(("datagen pod", pod_cpu, pod_mem))
    candidates.extend(_engine_pods(resolved))
    cpu_pod = max(candidates, key=lambda c: c[1])
    mem_pod = max(candidates, key=lambda c: c[2])
    largest = PodRequest(
        cpu_cores=cpu_pod[1],
        memory_gb=mem_pod[2],
        cpu_from=cpu_pod[0],
        memory_from=mem_pod[0],
    )

    scratch = resolved.platform.storage.scratch
    if not scratch.enabled:
        basis.append("scratch: not requested (scratch disabled)")

    spark_cfg = resolved.platform.compute.spark
    overrides = any(getattr(spark_cfg, f, None) is not None for f in _OVERRIDE_FIELDS[mode])
    if overrides:
        basis.append("spark: per-job executor overrides are not counted in the peak yet")

    plan = SizingPlan(
        workload=_schema_of(resolved),
        mode=mode,
        scale=scale,
        spark=spark,
        co_resident=co,
        datagen=datagen,
        floor=floor,
        full=full,
        largest_pod=largest,
        scratch_gb=spark.scratch_gb,
        scratch_enabled=bool(scratch.enabled),
        scratch_storage_class=str(scratch.storage_class),
        overrides_not_counted=overrides,
        cuts=tuple(c for c in cuts if not c.startswith("datagen.mode=")),
        basis=tuple(basis),
    )
    return plan, resolved


def plan_requirements(
    cfg: LakebenchConfig,
    *,
    run_mode: str | None = None,
    capacity: ClusterCapacity | None = None,
    datagen_runs: bool = True,
) -> SizingPlan:
    """What *cfg* requests from the cluster.

    Args:
        cfg: The loaded config. It is not mutated; auto-sizing runs on a
            deep copy.
        run_mode: Overrides the config's pipeline mode (``"batch"``,
            ``"continuous"`` or ``"sustained"``), as ``run --sustained``
            does.
        capacity: The cluster's allocatable capacity when known. Auto-sizing
            then caps or scales datagen and Trino to it, as ``run`` does;
            without it the datagen request is the declared or default
            parallelism, labelled :data:`BEFORE_CLUSTER_SCALING`.
        datagen_runs: False when the run skips generation; datagen is then
            left out in both modes.
    """
    return _plan(cfg, run_mode=run_mode, capacity=capacity, datagen_runs=datagen_runs)[0]


def default_sizing_config(workload: str, mode: str, scale: float) -> LakebenchConfig:
    """The default-recipe config (Hive, Iceberg, Trino) for one sizing cell.

    ``recommend`` without a config and the generated docs tables size this
    config, so their figures are what ``config show`` prints for a config
    that sets only the workload, mode and scale.
    """
    from lakebench.config.schema import LakebenchConfig

    return LakebenchConfig.model_validate(
        {
            "name": "sizing",
            "workload": {"schema": workload, "datagen": {"scale": scale}},
            "architecture": {"pipeline": {"mode": mode}},
        }
    )


def floor_text(plan: SizingPlan) -> str:
    """One line: the floor and the scratch PVC total (PVCs are sized in Gi)."""
    scratch = f"{plan.scratch_gb} Gi scratch"
    if not plan.scratch_enabled:
        scratch += " (not requested: scratch disabled)"
    return f"{plan.floor.cpu_cores} cores / {plan.floor.memory_gb} GB memory / {scratch}"


def breakdown_text(plan: SizingPlan) -> str:
    """One line: the parts the floor is built from."""
    spark = plan.spark
    parts = [f"Spark {spark.cpu_cores} cores / {spark.memory_gb} GB ({spark.driving_job})"]
    dg = plan.datagen
    if dg is not None:
        when = "cluster-scaled" if dg.cluster_scaled else BEFORE_CLUSTER_SCALING
        parts.append(
            f"datagen {_n(dg.pods, 'pod')}, {dg.cpu_cores} cores / {dg.memory_gb} GB "
            f"({when}; runs before Spark, pods that do not fit queue)"
        )
    co = plan.co_resident
    parts.append(f"always on {_n(co.cpu_cores, 'core')} / {co.memory_gb} GB ({co.label})")
    if plan.full != plan.floor:
        parts.append(
            f"every datagen pod at once {plan.full.cpu_cores} cores / {plan.full.memory_gb} GB"
        )
    if plan.overrides_not_counted:
        parts.append("per-job executor and driver overrides not counted")
    return "; ".join(parts)


def plan_shortfalls(
    plan: SizingPlan, capacity: ClusterCapacity, *, check_pod: bool = True
) -> list[str]:
    """What *plan*'s floor needs beyond *capacity*, short form ("+12
    cores"); empty when it fits. ``check_pod=False`` skips the largest-pod
    test when the node shape is unknown (``recommend --cores --memory``)."""
    gib = 1024**3
    cores = capacity.total_cpu_millicores / 1000
    mem = capacity.total_memory_bytes / gib
    out: list[str] = []
    if plan.floor.cpu_cores > cores:
        out.append(f"+{_ceil(plan.floor.cpu_cores - cores):,} cores")
    if plan.floor.memory_gb > mem:
        out.append(f"+{_ceil(plan.floor.memory_gb - mem):,} GB")
    if check_pod:
        if plan.largest_pod.cpu_cores > capacity.largest_node_cpu_millicores / 1000:
            out.append(f"a node with {_n(plan.largest_pod.cpu_cores, 'core')}")
        if plan.largest_pod.memory_gb > capacity.largest_node_memory_bytes / gib:
            out.append(f"a node with {plan.largest_pod.memory_gb:g} GB")
    return out


def check_capacity(
    cfg: LakebenchConfig,
    capacity: ClusterCapacity,
    *,
    run_mode: str | None = None,
    datagen_runs: bool = True,
    check_pod: bool = True,
    sizing_capacity: ClusterCapacity | None | object = SAME_CAPACITY,
) -> CapacityVerdict:
    """Can *capacity* hold *cfg*? The one capacity decision: the ``run``
    preflight and ``recommend`` both call it.

    Refused when the floor does not fit, or (``check_pod``) when the
    largest pod fits no node. In continuous mode a floor that does not fit
    is ``"degraded"`` instead when the streams capped to the cluster's
    concurrent budget fit, since ``run`` caps them and warns. In batch a
    full request that does not fit adds a warning: some datagen pods queue.

    *sizing_capacity* is the capacity auto-sizing sizes the config against
    before the check. By default it is *capacity*, which is right for a
    caller that has not resolved the config itself (``info``,
    ``recommend``). ``run`` resolved its config against the capacity it
    fetched (or None when that fetch failed) and passes that, so the plan
    checked is the one it deploys even if *capacity* differs (a later
    preflight checks free capacity, while ``run`` sizes against the total).
    """
    from lakebench.config.schema import is_continuous_mode

    size_against = capacity if sizing_capacity is SAME_CAPACITY else sizing_capacity
    plan, resolved = _plan(
        cfg,
        run_mode=run_mode,
        capacity=cast("ClusterCapacity | None", size_against),
        datagen_runs=datagen_runs,
    )
    spark, co, dg = plan.spark, plan.co_resident, plan.datagen
    gib = 1024**3
    avail_cores = capacity.total_cpu_millicores / 1000.0
    avail_gb = capacity.total_memory_bytes / gib
    node_cores = capacity.largest_node_cpu_millicores / 1000.0
    node_gb = capacity.largest_node_memory_bytes / gib

    def _pipeline(spark_v: int, pod_v: float, unit: str) -> str:
        if dg is not None and _ceil(pod_v) > spark_v:
            return f"{_ceil(pod_v)}{unit} datagen pod"
        return f"{spark_v}{unit} pipeline"

    shortfalls: list[str] = []
    if plan.floor.cpu_cores > avail_cores:
        shortfalls.append(
            f"CPU: need {plan.floor.cpu_cores} cores "
            f"({_pipeline(spark.cpu_cores, dg.pod_cpu_cores if dg else 0, ' cores')} + "
            f"{co.cpu_cores} {co.label}), cluster has {avail_cores:.1f} allocatable"
        )
    if plan.floor.memory_gb > avail_gb:
        shortfalls.append(
            f"Memory: need {plan.floor.memory_gb} GB "
            f"({_pipeline(spark.memory_gb, dg.pod_memory_gb if dg else 0, ' GB')} + "
            f"{co.memory_gb} GB {co.label}), cluster has {avail_gb:.1f} GB allocatable"
        )
    pod = plan.largest_pod
    pod_fits = True
    if check_pod and pod.cpu_cores > node_cores:
        pod_fits = False
        shortfalls.append(
            f"Largest pod ({pod.cpu_from}) needs {_n(pod.cpu_cores, 'core')}, "
            f"biggest node has {node_cores:.1f}"
        )
    if check_pod and pod.memory_gb > node_gb:
        pod_fits = False
        shortfalls.append(
            f"Largest pod ({pod.memory_from}) needs {pod.memory_gb:g} GB, "
            f"biggest node has {node_gb:.1f} GB"
        )

    warnings: list[str] = []
    if not shortfalls:
        if dg is not None and (plan.full.cpu_cores > avail_cores or plan.full.memory_gb > avail_gb):
            room_cpu = avail_cores - co.cpu_cores
            room_mem = avail_gb - co.memory_gb
            at_once = int(
                min(
                    dg.pods,
                    room_cpu // dg.pod_cpu_cores if dg.pod_cpu_cores else dg.pods,
                    room_mem // dg.pod_memory_gb if dg.pod_memory_gb else dg.pods,
                )
            )
            warnings.append(
                f"datagen: {dg.pods} pods need {dg.cpu_cores} cores / {dg.memory_gb} GB "
                f"beside the always-on pods; about {at_once} run at once and the rest "
                "queue, so generation takes longer and must still finish within the "
                "datagen timeout"
            )
        return CapacityVerdict("fits", plan, (), tuple(warnings))

    # Continuous: the run caps the streams to the cluster's concurrent
    # budget and warns naming each capped stage, so an aggregate shortfall
    # is fatal only if even the capped request does not fit. A pod that
    # fits no node stays fatal: capping counts does not shrink a pod.
    if pod_fits and is_continuous_mode(plan.mode):
        from lakebench.modules.pipeline_engines.spark.job import streaming_request_under_budget

        try:
            capped = streaming_request_under_budget(
                resolved, capacity.total_cpu_millicores, datagen_running=datagen_runs
            )
        except Exception as e:  # fall through to the refusal below
            logging.getLogger(__name__).debug("Capped continuous request unavailable: %s", e)
            capped = None
        if (
            capped is not None
            and capped.capped
            and capped.cpu_cores <= avail_cores
            and capped.memory_gb <= avail_gb
        ):
            return CapacityVerdict(
                "degraded",
                plan,
                tuple(shortfalls),
                (f"streams capped to fit the cluster: {', '.join(capped.capped)}",),
                capped=tuple(capped.capped),
                capped_request=Resources(capped.cpu_cores, capped.memory_gb),
            )
    return CapacityVerdict("refused", plan, tuple(shortfalls), ())


def largest_fitting_scale(fits: Callable[[int], bool], *, upper: int) -> int:
    """Largest integer scale *s* <= *upper* such that ``fits(t)`` holds for
    every scale 1..s; 0 when scale 1 does not fit.

    A scan, not the bisection the design named: the floor is not monotonic
    in scale. The autosizer's tier guidance moves Trino from 20 workers of 8
    cores at scale 500 to 10 workers at 501, and offline datagen from 50
    pods to 16, so a bisection could step over a scale that does not fit.
    "Every scale up to s" keeps the answer monotonic in cluster size.
    *upper* is the workload's datagen ceiling (scales above it are refused
    at load), at most a few hundred plans.
    """
    best = 0
    for scale in range(1, upper + 1):
        if not fits(scale):
            break
        best = scale
    return best


# ---------------------------------------------------------------------------
# Docs tables (generated by scripts/gen_sizing_tables.py; the drift test
# holds README.md and docs/getting-started.md equal to these)
# ---------------------------------------------------------------------------

#: The cells the docs publish: workload x mode x scale.
TABLE_WORKLOADS: tuple[str, ...] = ("customer360", "financial")
TABLE_MODES: tuple[str, ...] = ("batch", "continuous")
TABLE_SCALES: tuple[int, ...] = (1, 10, 100)

_TABLE_LABELS = {"customer360": "Customer 360", "financial": "AML"}

# The marker format and the block finder are config/support.py's, so every
# generated docs block reads the same way.
_REGEN = (
    "<!-- Generated from the code by `python3.11 scripts/gen_sizing_tables.py`; "
    "do not edit by hand. -->"
)


def table_plans() -> list[SizingPlan]:
    """The published cells, offline (no cluster), default recipe."""
    return [
        plan_requirements(default_sizing_config(wl, m, s))
        for wl in TABLE_WORKLOADS
        for m in TABLE_MODES
        for s in TABLE_SCALES
    ]


def _pod_cell(plan: SizingPlan) -> str:
    pod = plan.largest_pod
    return f"{pod.cpu_cores:g} cores / {pod.memory_gb:g} GB"


def render_minimums_table() -> str:
    """Markdown: the minimum cluster per published cell (README)."""
    lines = [
        "| Workload | Mode | Scale | Minimum CPU | Minimum RAM | Scratch PVC (if enabled) "
        "| Largest pod |",
        "|:---|:---|---:|---:|---:|---:|---:|",
    ]
    for p in table_plans():
        lines.append(
            f"| {_TABLE_LABELS[p.workload]} | {p.mode} | {p.scale:g} | "
            f"{p.floor.cpu_cores:,} cores | {p.floor.memory_gb:,} GB | "
            f"{p.scratch_gb:,} Gi | {_pod_cell(p)} |"
        )
    return "\n".join(lines)


def render_detail_table() -> str:
    """Markdown: the minimum cluster and what it is built from
    (docs/getting-started.md)."""
    lines = [
        "| Workload | Mode | Scale | Minimum CPU | Minimum RAM | Spark peak | "
        "Datagen (default parallelism) | Always on | Scratch PVC (if enabled) | Largest pod |",
        "|:---|:---|---:|---:|---:|:---|:---|:---|---:|---:|",
    ]
    for p in table_plans():
        spark = f"{p.spark.cpu_cores:,} cores / {p.spark.memory_gb:,} GB"
        if p.datagen is not None:
            dg = p.datagen
            datagen = f"{dg.pods} pods, {dg.cpu_cores:,} cores / {dg.memory_gb:,} GB"
        else:
            datagen = "in always on"
        co = p.co_resident
        lines.append(
            f"| {_TABLE_LABELS[p.workload]} | {p.mode} | {p.scale:g} | "
            f"{p.floor.cpu_cores:,} cores | {p.floor.memory_gb:,} GB | {spark} | {datagen} | "
            f"{co.cpu_cores:,} cores / {co.memory_gb:,} GB | {p.scratch_gb:,} Gi | "
            f"{_pod_cell(p)} |"
        )
    return "\n".join(lines)


GENERATED_BLOCKS: dict[str, Callable[[], str]] = {
    "sizing-minimums": render_minimums_table,
    "sizing-detail": render_detail_table,
}

#: docs file (relative to the repo root) -> the sizing blocks it carries.
DOCS_WITH_BLOCKS: dict[str, tuple[str, ...]] = {
    "README.md": ("sizing-minimums",),
    "docs/getting-started.md": ("sizing-detail",),
}


def expected_block(name: str) -> str:
    """The full generated block *name*, markers included."""
    from lakebench.config.support import _BEGIN, _END

    return "\n".join(
        [
            _BEGIN.format(name=name),
            _REGEN,
            "",
            GENERATED_BLOCKS[name](),
            "",
            _END.format(name=name),
        ]
    )


def block_in(text: str, name: str) -> str | None:
    """The generated block *name* as it appears in *text*, or None."""
    from lakebench.config.support import block_in as _block_in

    return _block_in(text, name)
