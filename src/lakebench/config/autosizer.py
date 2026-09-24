"""Auto-sizing of compute resources based on scale factor and cluster capacity.

Inspects the LakebenchConfig and, for any resource field the user did not
explicitly set, applies scale-derived recommendations.  If a ClusterCapacity
snapshot is available the algorithm becomes cluster-aware:

* **Small scales (≤ 50)** -- tier guidance only.  Cluster capacity is used
  solely to *cap* values that don't fit.
* **Large scales (> 50)** -- datagen and Spark are *scaled up* to use the
  available cluster budget (after Trino + infra).  This is the only case
  where resources exceed tier guidance.

The algorithm is phase-aware:

* **Trino + infra** are always running (deployed first, never torn down).
  Their resources come from tier guidance and are never boosted -- only
  capped if the cluster is too small.
* **Batch mode** (MEDALLION pattern): Datagen runs concurrently with Trino
  but exits before Spark starts.  Spark runs after datagen finishes.
  Because they never overlap, each gets the full remaining budget.
* **Streaming mode** (STREAMING pattern): Datagen and Spark streaming jobs
  run concurrently.  The budget is split: 40 % datagen, 60 % streaming Spark.

Usage::

    from lakebench.config.autosizer import resolve_auto_sizing
    resolve_auto_sizing(config, cluster_capacity)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from lakebench.config.scale import full_compute_guidance

if TYPE_CHECKING:
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s.client import ClusterCapacity

log = logging.getLogger(__name__)

# What fraction of remaining cluster CPU (after Trino + infra) each phase can use.
_PHASE_CPU_BUDGET = 0.90
# In streaming mode, datagen and Spark are concurrent.  Split the budget:
_STREAMING_DATAGEN_SHARE = 0.40  # 40 % for datagen
_STREAMING_SPARK_SHARE = 0.60  # 60 % for streaming Spark jobs
_POD_MEMORY_HEADROOM = 0.85  # per-pod memory cap vs largest node


def _set_if_default(model: Any, field: str, value: object) -> bool:
    """Set *field* on a Pydantic model only if the user did not supply it.

    Returns True if the field was auto-set.
    """
    if field not in model.model_fields_set:
        object.__setattr__(model, field, value)
        return True
    return False


def _parse_memory_gi(mem: str) -> float:
    """Parse a Kubernetes-style memory string to GiB (float)."""
    mem = mem.strip()
    if mem.lower().endswith("gi"):
        return float(mem[:-2])
    if mem.lower().endswith("mi"):
        return float(mem[:-2]) / 1024
    if mem.lower().endswith("g"):
        return float(mem[:-1])
    if mem.lower().endswith("m"):
        return float(mem[:-1]) / 1024
    # plain bytes
    return float(mem) / (1024**3)


def _parse_cpu_millicores(cpu: str | int | float) -> int:
    """Parse a Kubernetes-style CPU string to integer millicores.

    Accepts:
      - "500m", "1500m"        -> 500, 1500
      - "1", "2", "1.5"        -> 1000, 2000, 1500
      - int/float (whole cores) -> value * 1000

    Raises ValueError on unparseable input.
    """
    if isinstance(cpu, (int, float)):
        return int(cpu * 1000)
    s = cpu.strip()
    if not s:
        raise ValueError(f"empty CPU value: {cpu!r}")
    if s.endswith("m"):
        return int(float(s[:-1]))
    return int(float(s) * 1000)


# Datagen memory model, fitted to measured peak RSS of the real generator
# binary at 64 MB and 512 MB files, 1 and 8 threads (2026-09-24): each worker
# thread holds about 4.8x the output file size for financial (row vectors,
# sorted copies, the Arrow batch, writer buffers) and about 3.0x for c360; a
# pod carries a fixed ~1.7 GiB (financial) or ~0.3 GiB (c360); node 0 builds
# the full financial world at about 650 B per entity (measured scale 1-100:
# node-0 peak 3.54, 4.66, 7.62, 10.69 GiB at scale 1, 10, 50, 100). The same
# coefficients are used by datagen_rs/entrypoint.py to cap threads, so the
# two never disagree.
DATAGEN_PER_THREAD_FILE_MULTIPLIER = {"financial": 4.8, "customer360": 3.0}
DATAGEN_WORLD_BYTES_PER_ENTITY_NODE0 = 650
DATAGEN_ENTITIES_PER_SCALE = 111_111
DATAGEN_BASE_GIB = {"financial": 1.7, "customer360": 0.3}
DATAGEN_HEADROOM = 1.25


def _parse_size_mb(size: str) -> float:
    """'64mb' / '1GB' / '512MB' -> MiB (same units as the datagen deployer)."""
    t = size.strip().upper()
    for suffix, mult in (("TB", 2**20), ("GB", 2**10), ("MB", 1.0), ("KB", 2**-10)):
        if t.endswith(suffix):
            return float(t[: -len(suffix)]) * mult
    return float(t.rstrip("B") or 0) / 2**20


def datagen_memory_gib(schema: str, scale: float, threads: int, file_size_mb: float) -> float:
    """Estimated peak RSS in GiB for the busiest datagen pod (node 0)."""
    per_thread = (file_size_mb / 1024.0) * DATAGEN_PER_THREAD_FILE_MULTIPLIER.get(schema, 3.0)
    world = 0.0
    if schema == "financial":
        world = DATAGEN_ENTITIES_PER_SCALE * scale * DATAGEN_WORLD_BYTES_PER_ENTITY_NODE0 / 2**30
    return (world + threads * per_thread + DATAGEN_BASE_GIB.get(schema, 0.3)) * DATAGEN_HEADROOM


def _datagen_memory_default(config: LakebenchConfig, cpu: str) -> str:
    """Per-pod datagen memory limit that fits the measured peak RSS."""
    import math

    datagen = config.architecture.workload.datagen
    schema = getattr(config.architecture.workload.schema_type, "value", "customer360")
    threads = max(1, _parse_cpu_millicores(cpu) // 1000)
    file_mb = _parse_size_mb(datagen.file_size)
    gib = datagen_memory_gib(schema, float(datagen.scale or 1), threads, file_mb)
    return f"{max(4, math.ceil(gib))}Gi"


def _resolve_datagen_mode(config: LakebenchConfig) -> str:
    """Resolve the effective datagen mode from config.

    If mode is 'auto', selects based on scale:
      scale <= 10 (~100 GB) -> DatagenMode.BATCH
      scale > 10            -> DatagenMode.CONTINUOUS

    Returns:
        DatagenMode.BATCH.value or DatagenMode.CONTINUOUS.value
    """
    from lakebench.config.schema import DatagenMode

    datagen = config.architecture.workload.datagen
    mode = datagen.mode

    if mode == DatagenMode.AUTO:
        return DatagenMode.BATCH.value if datagen.scale <= 10 else DatagenMode.CONTINUOUS.value
    return mode.value


def resolve_auto_sizing(
    config: LakebenchConfig,
    cluster_capacity: ClusterCapacity | None = None,
) -> None:
    """Resolve auto-sized resource fields on *config* in place.

    For each component (Spark executor/driver, Trino worker/coordinator,
    Datagen) this function:

    1. Resolves datagen mode (auto -> batch or continuous).
    2. Applies scale-derived tier guidance for fields the user
       did not explicitly set.
    3. If *cluster_capacity* is provided:
       - Trino is only capped (never boosted).
       - For small scales (≤ 50), datagen/Spark are only capped.
       - For large scales (> 50), datagen/Spark are scaled up to
         use the available cluster budget.

    Args:
        config: The Lakebench configuration -- **mutated in place**.
        cluster_capacity: Optional snapshot of cluster node resources.
    """

    scale = config.architecture.workload.datagen.scale
    guidance = full_compute_guidance(scale)

    changes: list[str] = []

    # -- Resolve datagen mode --
    effective_mode = _resolve_datagen_mode(config)
    changes.append(f"datagen.mode={effective_mode}")

    # -- Spark executor --
    executor = config.platform.compute.spark.executor
    if _set_if_default(executor, "instances", guidance.spark.recommended_executors):
        changes.append(f"spark.executor.instances={guidance.spark.recommended_executors}")
    if _set_if_default(executor, "memory", guidance.spark.recommended_memory):
        changes.append(f"spark.executor.memory={guidance.spark.recommended_memory}")
    if _set_if_default(executor, "cores", guidance.spark.recommended_cores):
        changes.append(f"spark.executor.cores={guidance.spark.recommended_cores}")

    # Memory overhead: ~25% of executor memory
    overhead_gi = max(1, int(_parse_memory_gi(executor.memory) * 0.25))
    if _set_if_default(executor, "memory_overhead", f"{overhead_gi}g"):
        changes.append(f"spark.executor.memory_overhead={overhead_gi}g")

    # -- Spark driver --
    driver = config.platform.compute.spark.driver
    if _set_if_default(driver, "cores", guidance.spark.recommended_cores):
        changes.append(f"spark.driver.cores={guidance.spark.recommended_cores}")
    if _set_if_default(driver, "memory", guidance.spark.min_memory):
        changes.append(f"spark.driver.memory={guidance.spark.min_memory}")

    # -- Trino coordinator --
    coord = config.architecture.query_engine.trino.coordinator
    if _set_if_default(coord, "cpu", guidance.trino.coordinator_cpu):
        changes.append(f"trino.coordinator.cpu={guidance.trino.coordinator_cpu}")
    if _set_if_default(coord, "memory", guidance.trino.coordinator_memory):
        changes.append(f"trino.coordinator.memory={guidance.trino.coordinator_memory}")

    # -- Trino worker --
    worker = config.architecture.query_engine.trino.worker
    if _set_if_default(worker, "replicas", guidance.trino.worker_replicas):
        changes.append(f"trino.worker.replicas={guidance.trino.worker_replicas}")
    if _set_if_default(worker, "cpu", guidance.trino.worker_cpu):
        changes.append(f"trino.worker.cpu={guidance.trino.worker_cpu}")
    if _set_if_default(worker, "memory", guidance.trino.worker_memory):
        changes.append(f"trino.worker.memory={guidance.trino.worker_memory}")

    # -- Datagen --
    # Per-pod sizing for the Rust generator (both schemas, both modes; the
    # corpus is always pre-written, so continuous is a pipeline mode, not a
    # datagen mode). Measured 2026-09-18..21: 8-core pods reached 338-600
    # MB/s; per-node memory bandwidth, not the array, limits aggregate
    # throughput past about 24 active generator cores per node, so 8 cores
    # per pod packs well on 40-core workers.
    #
    # Earlier code hard-locked 4 CPU / 4Gi in batch and forced generators=1,
    # which the entrypoint turned into a single rayon thread per pod (product
    # path measured 36 MB/s/pod). Now: user-set cpu/memory are honoured, and
    # generators stays 0 ("auto"), so the entrypoint sizes threads from the
    # pod's CPU request.
    #
    # Memory: derived from measured peak RSS (see datagen_memory_gib), for
    # the CPU (thread count) actually used.
    datagen = config.architecture.workload.datagen
    dg_cpu = datagen.cpu if "cpu" in datagen.model_fields_set else "8"
    dg_memory = _datagen_memory_default(config, dg_cpu)
    if _set_if_default(datagen, "cpu", dg_cpu):
        changes.append(f"datagen.cpu={dg_cpu}")
    if _set_if_default(datagen, "memory", dg_memory):
        changes.append(f"datagen.memory={dg_memory}")

    if _set_if_default(datagen, "parallelism", guidance.datagen.parallelism):
        changes.append(f"datagen.parallelism={guidance.datagen.parallelism}")

    # -- Schema-specific overrides (ENG-2C.10) --
    # Workload schemas that differ from Customer360 on baseline resource shape
    # override defaults here. Currently just the silver-build scratch PVC
    # size for Financial (200Gi vs Customer360's 150Gi) per spec §2C.21;
    # workload-specific stage profiles (W1-W7) are applied by ENG-2C.3
    # at manifest-build time, not autosizer time.
    schema_change = _apply_schema_overrides(config, cluster_capacity)
    if schema_change:
        changes.append(schema_change)

    # -- Cluster capacity: cap to fit --
    if cluster_capacity is not None:
        _apply_cluster_scaling(config, cluster_capacity, effective_mode, guidance, changes)

    if changes:
        log.info(
            "Auto-sized for scale=%d (tier=%s, mode=%s): %s",
            scale,
            guidance.spark.tier_name,
            effective_mode,
            ", ".join(changes),
        )


def _apply_schema_overrides(
    config: LakebenchConfig,
    cluster_capacity: ClusterCapacity | None = None,
) -> str | None:
    """Apply per-workload-schema default overrides.

    Baseline (Customer360) leaves everything at scale-tier guidance.
    Financial (FinServ-Crime, AML) bumps the shared scratch PVC to 200 Gi
    for silver_build headroom on pacs.008 rows (spec §2C.21) and lifts
    the Spark Thrift default from 4g toward 16g -- LB-093, first live
    S1 run OOM'd every FAML benchmark query at 4g because the silver
    aggregation and rule-target joins are heavier than C360's silver.
    Only fields the user did not explicitly set are touched.

    Cluster-cap on the thrift bump: on a small cluster whose largest
    node cannot fit 24g + Spark overhead (~2.4g) + a safety margin for
    kubelet / co-scheduled pods, requesting 24g causes the thrift pod
    to sit Pending forever or drives the node into memory pressure.
    ``largest_node_memory_bytes`` is *allocatable* (post-reservation);
    Spark still needs its own overhead on top and the node still needs
    room to run other pods. Rule of thumb: leave ~8 GiB headroom below
    allocatable. When ``cluster_capacity`` is available and the largest
    allocatable node has less than 36 GiB, fall back to
    ``min(20g, largest_node_gi - 8g)`` (with a floor of 4g so we never
    silently regress below the original 4g default). When capacity is
    not available (offline autosizing), stay at 24g -- users on a small
    cluster can override explicitly.
    """
    from lakebench.config.schema import WorkloadSchema

    schema = config.architecture.workload.schema_type
    if schema != WorkloadSchema.FINANCIAL:
        return None

    changes: list[str] = []
    scratch = config.platform.storage.scratch
    if _set_if_default(scratch, "size", "200Gi"):
        changes.append("storage.scratch.size=200Gi")

    if config.architecture.query_engine.type.value == "spark-thrift":
        thrift = config.architecture.query_engine.spark_thrift
        # LB-117: 16g was on the edge for FAML analytical queries -- three
        # S1 iters saw QpH 6.6 / 0.0 / 8.2 with the 0.0 being a thrift-pod
        # OOM mid-benchmark on aggregate_typology_coverage.sql. 24g clears
        # it with headroom. Cluster-cap threshold is 36 GiB *allocatable*:
        # 24g heap + ~2.4g Spark overhead + ~8 GiB safety margin for the
        # driver, kubelet and any co-scheduled pod. Below the threshold,
        # target = min(20g, allocatable - 8g) with a 4g floor.
        target_memory = "24g"
        if cluster_capacity is not None:
            largest_node_gi = _largest_node_memory_gi(cluster_capacity)
            if largest_node_gi is not None and largest_node_gi < 36.0:
                fitted = max(4.0, min(20.0, largest_node_gi - 8.0))
                target_memory = f"{int(fitted)}g"
        if _set_if_default(thrift, "memory", target_memory):
            changes.append(f"query_engine.spark_thrift.memory={target_memory}")

    if not changes:
        return None
    return ", ".join(changes) + " (schema=financial)"


def _largest_node_memory_gi(cluster_capacity: ClusterCapacity) -> float | None:
    """Largest allocatable node memory in GiB, or None if the field
    is missing / unreadable. Best-effort guard for the 16g thrift bump
    on tiny clusters -- a miss falls through to 16g rather than to a
    fabricated cap."""
    val = getattr(cluster_capacity, "largest_node_memory_bytes", None)
    if val is None:
        return None
    try:
        return float(val) / (1024**3)
    except (TypeError, ValueError):
        return None


def _round_down_even(n: int) -> int:
    """Round *n* down to the nearest even number (minimum 2)."""
    return max(2, n - (n % 2))


def _co_resident_cpu_m(config: LakebenchConfig) -> int:
    """Compute total CPU (millicores) committed to always-on co-resident pods.

    These are the components that run alongside every workload phase.
    The engine overhead depends on ``query_engine.type``:

    - **trino**: Trino coordinator + workers
    - **spark-thrift**: Spark Thrift Server driver + executors
    - **none**: No engine overhead

    Hive Metastore and PostgreSQL are always included (~1 CPU total).
    """
    engine_type = config.architecture.query_engine.type.value
    # Hive + Postgres are small but add up (~1 CPU total)
    infra_m = 1000

    if engine_type == "trino":
        coord = config.architecture.query_engine.trino.coordinator
        worker = config.architecture.query_engine.trino.worker
        trino_coord_m = _parse_cpu_millicores(coord.cpu)
        trino_workers_m = worker.replicas * _parse_cpu_millicores(worker.cpu)
        return trino_coord_m + trino_workers_m + infra_m
    elif engine_type == "spark-thrift":
        thrift = config.architecture.query_engine.spark_thrift
        thrift_m = thrift.cores * 1000
        return thrift_m + infra_m
    elif engine_type == "duckdb":
        duckdb_cfg = config.architecture.query_engine.duckdb
        duckdb_m = duckdb_cfg.cores * 1000
        return duckdb_m + infra_m
    else:
        # engine_type == "none" -- no engine overhead
        return infra_m


def _apply_cluster_scaling(
    config: LakebenchConfig,
    cap: ClusterCapacity,
    effective_mode: str,
    guidance: object,
    changes: list[str],
) -> None:
    """Fit workload to the cluster, scaling up large workloads.

    The algorithm is phase-aware:

    1. **Trino** uses tier guidance only -- never boosted.  If the cluster
       is too small to fit the tier's worker count, replicas are reduced.
    2. **Batch** (MEDALLION/BATCH): Datagen and Spark are sequential phases
       so each gets the full remaining CPU budget.
       **Streaming** (STREAMING): Datagen and Spark run concurrently so
       the budget is split (40 % datagen, 60 % Spark).
       - For scales ≤ 50: only *cap* to fit.
       - For scales > 50: *scale up* to use the available budget.
    3. Per-pod memory is capped to 85 % of the largest node.
    """
    scale = config.architecture.workload.datagen.scale
    executor = config.platform.compute.spark.executor
    datagen = config.architecture.workload.datagen
    engine_type = config.architecture.query_engine.type.value

    # Max per-pod memory: 85% of largest node
    max_pod_mem_bytes = int(cap.largest_node_memory_bytes * _POD_MEMORY_HEADROOM)
    max_pod_mem_gi = max_pod_mem_bytes / (1024**3)

    # --- Cap per-pod memory ---

    exec_mem_gi = _parse_memory_gi(executor.memory)
    if exec_mem_gi > max_pod_mem_gi:
        capped = f"{int(max_pod_mem_gi)}g"
        object.__setattr__(executor, "memory", capped)
        changes.append(f"spark.executor.memory capped to {capped} (node limit)")

    # --- Trino-specific: cap worker memory and worker count ---
    if engine_type == "trino":
        worker = config.architecture.query_engine.trino.worker
        worker_mem_gi = _parse_memory_gi(worker.memory)
        if worker_mem_gi > max_pod_mem_gi:
            capped = f"{int(max_pod_mem_gi)}Gi"
            object.__setattr__(worker, "memory", capped)
            changes.append(f"trino.worker.memory capped to {capped} (node limit)")

        # Cap Trino worker count (never boost).
        # Trino is always running.  Subtract coordinator + infra overhead
        # from the cluster, then compute how many workers fit.
        coord = config.architecture.query_engine.trino.coordinator
        coord_and_infra_m = _parse_cpu_millicores(coord.cpu) + 1000  # coordinator + Hive/Postgres
        trino_worker_budget_m = max(0, cap.total_cpu_millicores - coord_and_infra_m)
        worker_cpu_m = _parse_cpu_millicores(worker.cpu)
        cluster_max_workers = max(1, trino_worker_budget_m // worker_cpu_m)

        if worker.replicas > cluster_max_workers:
            object.__setattr__(worker, "replicas", cluster_max_workers)
            changes.append(f"trino.worker.replicas capped to {cluster_max_workers} (cluster CPU)")

    # --- CPU committed to always-on pods (Trino + Hive + Postgres) ---
    # Computed after any Trino capping so we use the final worker count.
    co_resident_m = _co_resident_cpu_m(config)
    phase_budget_m = max(0, cap.total_cpu_millicores - co_resident_m)
    phase_budget_m = int(phase_budget_m * _PHASE_CPU_BUDGET)

    # In STREAMING mode, datagen and Spark are concurrent -- split the budget.
    # In BATCH/MEDALLION mode, they are sequential -- each gets the full budget.
    from lakebench.config.schema import ProcessingPattern

    is_streaming = config.architecture.pipeline.pattern == ProcessingPattern.STREAMING
    if is_streaming:
        spark_budget_m = int(phase_budget_m * _STREAMING_SPARK_SHARE)
        datagen_budget_m = int(phase_budget_m * _STREAMING_DATAGEN_SHARE)
    else:
        spark_budget_m = phase_budget_m
        datagen_budget_m = phase_budget_m

    # --- Spark executors: cap or scale up ---
    exec_cpu_m = executor.cores * 1000
    cluster_max_executors = _round_down_even(spark_budget_m // exec_cpu_m)

    if "instances" not in executor.model_fields_set:
        if scale > 50 and cluster_max_executors > executor.instances:
            # Large scale: use the cluster
            object.__setattr__(executor, "instances", cluster_max_executors)
            changes.append(
                f"spark.executor.instances scaled to {cluster_max_executors} "
                f"(cluster has {cap.total_cpu_millicores // 1000} cores)"
            )
        elif executor.instances > cluster_max_executors:
            object.__setattr__(executor, "instances", cluster_max_executors)
            changes.append(
                f"spark.executor.instances capped to {cluster_max_executors} (cluster CPU)"
            )
    elif executor.instances > cluster_max_executors:
        # User-set value still gets capped to fit
        object.__setattr__(executor, "instances", cluster_max_executors)
        changes.append(f"spark.executor.instances capped to {cluster_max_executors} (cluster CPU)")

    # --- Datagen parallelism: cap or scale up ---
    if datagen.parallelism > 0:
        datagen_cpu_m = _parse_cpu_millicores(datagen.cpu)
        cluster_max_datagen = _round_down_even(datagen_budget_m // datagen_cpu_m)

        if "parallelism" not in datagen.model_fields_set:
            if scale > 50 and cluster_max_datagen > datagen.parallelism:
                # Large scale: use the cluster
                object.__setattr__(datagen, "parallelism", cluster_max_datagen)
                changes.append(f"datagen.parallelism scaled to {cluster_max_datagen} (cluster CPU)")
            elif datagen.parallelism > cluster_max_datagen:
                object.__setattr__(datagen, "parallelism", cluster_max_datagen)
                changes.append(f"datagen.parallelism capped to {cluster_max_datagen} (cluster CPU)")
        elif datagen.parallelism > cluster_max_datagen:
            # User-set value still gets capped to fit
            object.__setattr__(datagen, "parallelism", cluster_max_datagen)
            changes.append(f"datagen.parallelism capped to {cluster_max_datagen} (cluster CPU)")
