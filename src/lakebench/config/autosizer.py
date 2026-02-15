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


def _resolve_datagen_mode(config: LakebenchConfig) -> str:
    """Resolve the effective datagen mode from config.

    If mode is 'auto', selects based on scale:
      scale <= 10 (~100 GB) -> batch
      scale > 10            -> continuous

    Returns:
        "batch" or "continuous"
    """
    from lakebench.config.schema import DatagenMode

    datagen = config.architecture.workload.datagen
    mode = datagen.mode

    if mode == DatagenMode.AUTO:
        return "batch" if datagen.scale <= 10 else "continuous"
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
    # CPU and memory per pod are hard-locked by mode (MVP sizing):
    #   batch:      4 CPU, 4Gi
    #   continuous: 8 CPU, 24Gi
    # These are always overridden regardless of user config to prevent
    # OOMKill from mode/memory mismatches.
    # Scaling is via parallelism (number of pods).
    datagen = config.architecture.workload.datagen

    if effective_mode == "continuous":
        mode_cpu, mode_memory = "8", "24Gi"
        mode_generators, mode_uploaders = 8, 2
    else:
        mode_cpu, mode_memory = "4", "4Gi"
        mode_generators, mode_uploaders = 1, 1

    if _set_if_default(datagen, "parallelism", guidance.datagen.parallelism):
        changes.append(f"datagen.parallelism={guidance.datagen.parallelism}")

    # Hard-lock CPU and memory -- always set to mode-correct values
    if datagen.cpu != mode_cpu:
        datagen.cpu = mode_cpu
        changes.append(f"datagen.cpu={mode_cpu} (locked by {effective_mode} mode)")
    if datagen.memory != mode_memory:
        datagen.memory = mode_memory
        changes.append(f"datagen.memory={mode_memory} (locked by {effective_mode} mode)")
    if datagen.generators == 0 or datagen.generators != mode_generators:
        datagen.generators = mode_generators
        changes.append(f"datagen.generators={mode_generators}")
    if datagen.uploaders == 0 or datagen.uploaders != mode_uploaders:
        datagen.uploaders = mode_uploaders
        changes.append(f"datagen.uploaders={mode_uploaders}")

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
        trino_coord_m = int(coord.cpu) * 1000
        trino_workers_m = worker.replicas * int(worker.cpu) * 1000
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
        coord_and_infra_m = int(coord.cpu) * 1000 + 1000  # coordinator + Hive/Postgres
        trino_worker_budget_m = max(0, cap.total_cpu_millicores - coord_and_infra_m)
        worker_cpu_m = int(worker.cpu) * 1000
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
        datagen_cpu_m = int(datagen.cpu) * 1000
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
