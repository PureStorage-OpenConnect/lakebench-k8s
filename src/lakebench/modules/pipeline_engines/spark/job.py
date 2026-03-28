"""Spark Job management for Lakebench.

Handles SparkApplication submission and lifecycle.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any

from lakebench._constants import POLARIS_CLIENT_ID, POLARIS_CLIENT_SECRET, SPARK_SERVICE_ACCOUNT

if TYPE_CHECKING:
    from lakebench.config import LakebenchConfig
    from lakebench.k8s import K8sClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-job-type resource profiles (proven at 1TB+ scale)
#
# Per-executor sizing (cores, memory, overhead, PVC) is fixed --
# these are the proven values that avoid OOM and disk-full failures.
# Executor *count* scales with the data (scale factor) since jobs run
# sequentially in batch mode -- no risk of over-provisioning the scheduler.
#
# MINIMUM CLUSTER REQUIREMENTS (per executor, non-negotiable):
#
#   bronze-verify:  2 cores,  6g total (4g + 2g overhead),  50Gi Portworx PVC
#   silver-build:   4 cores, 60g total (48g + 12g overhead), 150Gi Portworx PVC
#   gold-finalize:  4 cores, 40g total (32g + 8g overhead),  100Gi Portworx PVC
#
# The silver job is the bottleneck -- at scale 100 (~1TB) it requests
# 19 executors × 60g = ~1.14 TB RAM + 19 × 150Gi = 2.85 TB scratch PVC.
# The cluster must have enough capacity to schedule all executors plus
# the driver (4 cores, 8g) and existing infra (Trino, Hive, Postgres).
#
# StorageClass must be Portworx repl=1 (px-csi-scratch) for scratch PVCs.
# Using repl=2+ doubles the storage requirement with no benefit for scratch.
# ---------------------------------------------------------------------------

# fabric8 Kubernetes client causes API polling storms at 32+ executors.
# 28 is the proven safe ceiling from scale testing.
_MAX_EXECUTORS_SAFE = 28

_JOB_PROFILES: dict[str, dict[str, Any]] = {
    "bronze-verify": {
        "driver_cores": 2,
        "driver_memory": "4g",
        "executor_cores": 2,
        "executor_memory": "4g",
        "executor_memory_overhead": "2g",
        "scratch_size": "50Gi",
        # Executor count and partitions scale with data
        "base_executors": 4,  # scale <= 10
        "executors_per_100_scale": 4,  # add 4 per 100 scale units
        "max_executors": 20,
        "base_partitions": 32,
    },
    "silver-build": {
        "driver_cores": 4,
        "driver_memory": "32g",  # BUG-005: 24g OOM with Spark 4 (558MB SDK v2 bundle + K8s API polling)
        "executor_cores": 4,
        "executor_memory": "48g",
        "executor_memory_overhead": "12g",
        "scratch_size": "150Gi",
        "base_executors": 8,  # scale <= 10
        "executors_per_100_scale": 12,  # add 12 per 100 scale units
        "max_executors": _MAX_EXECUTORS_SAFE,
        "base_partitions": 64,
    },
    "gold-finalize": {
        "driver_cores": 4,
        "driver_memory": "32g",  # BUG-007: 24g OOM with Spark 4 (558MB SDK v2 bundle + K8s API polling)
        "executor_cores": 4,
        "executor_memory": "32g",
        "executor_memory_overhead": "8g",
        "scratch_size": "100Gi",
        "base_executors": 4,  # scale <= 10
        "executors_per_100_scale": 8,  # add 8 per 100 scale units
        "max_executors": _MAX_EXECUTORS_SAFE,
        "base_partitions": 32,
    },
    # -----------------------------------------------------------------------
    # Streaming job profiles (lighter than batch -- micro-batches, not full scan)
    # -----------------------------------------------------------------------
    "bronze-ingest": {
        "driver_cores": 2,
        "driver_memory": "4g",
        "executor_cores": 2,
        "executor_memory": "4g",
        "executor_memory_overhead": "2g",
        "scratch_size": "20Gi",
        "base_executors": 2,
        "executors_per_100_scale": 4,
        "max_executors": 10,
        "base_partitions": 16,
    },
    "silver-stream": {
        "driver_cores": 4,
        "driver_memory": "8g",
        "executor_cores": 4,
        "executor_memory": "32g",
        "executor_memory_overhead": "8g",
        "scratch_size": "100Gi",
        "base_executors": 4,
        "executors_per_100_scale": 8,
        "max_executors": 20,
        "base_partitions": 32,
    },
    "gold-refresh": {
        "driver_cores": 4,
        "driver_memory": "8g",
        "executor_cores": 4,
        "executor_memory": "32g",
        "executor_memory_overhead": "8g",
        "scratch_size": "100Gi",
        "base_executors": 2,
        "executors_per_100_scale": 4,
        "max_executors": 10,
        "base_partitions": 32,
    },
}


def _scale_executor_count(profile: dict[str, Any], scale: int) -> int:
    """Derive executor count from scale factor and job profile.

    Uses base counts for small scales and adds more executors
    linearly for larger datasets. Capped at max_executors.
    """
    base = profile["base_executors"]
    if scale <= 10:
        return base
    extra = ((scale - 10) * profile["executors_per_100_scale"]) // 100
    return min(base + extra, profile["max_executors"])


def get_job_profile(job_type: str) -> dict[str, Any] | None:
    """Return the resource profile for a given job type.

    Args:
        job_type: Job type string (e.g. "bronze-verify", "silver-build").

    Returns:
        Profile dict copy or None if job_type is unknown.
    """
    profile = _JOB_PROFILES.get(job_type)
    return dict(profile) if profile else None


def get_executor_count(job_type: str, scale: int) -> int:
    """Compute the deterministic executor count for a job at a given scale.

    Args:
        job_type: Job type string.
        scale: Scale factor from config.

    Returns:
        Expected executor count, or 0 if job_type is unknown.
    """
    profile = _JOB_PROFILES.get(job_type)
    if not profile:
        return 0
    return _scale_executor_count(profile, scale)


def _streaming_concurrent_budget(
    config: LakebenchConfig,
    cluster_cpu_millicores: int | None,
) -> dict[JobType, int]:
    """Compute max executor count per streaming job for concurrent execution.

    In sustained mode, datagen + 3 streaming jobs share the cluster.
    Divides the available CPU (after Trino + infra + datagen) among
    streaming jobs proportionally to their uncapped demand.

    Returns:
        Dict mapping each streaming JobType to its capped executor count.
        Empty dict if cluster capacity is unavailable.
    """
    if cluster_cpu_millicores is None:
        return {}

    scale = config.architecture.workload.datagen.scale

    # Co-resident pods (Trino + Hive + Postgres)
    trino = config.architecture.query_engine.trino
    co_resident_m = (
        int(trino.coordinator.cpu) * 1000
        + trino.worker.replicas * int(trino.worker.cpu) * 1000
        + 1000  # Hive + Postgres
    )

    # Datagen runs concurrently with streaming
    datagen = config.architecture.workload.datagen
    datagen_m = datagen.parallelism * int(datagen.cpu) * 1000

    # Budget for all streaming jobs combined (90% of remaining after co-resident + datagen)
    remaining_m = max(0, cluster_cpu_millicores - co_resident_m - datagen_m)
    streaming_budget_m = int(remaining_m * 0.90)

    # Compute each streaming job's uncapped CPU demand
    demands: dict[JobType, int] = {}
    for jt in _STREAMING_JOB_TYPES:
        profile = _JOB_PROFILES[jt.value]
        count = _scale_executor_count(profile, scale)
        demands[jt] = count * profile["executor_cores"] * 1000

    total_demand_m = sum(demands.values())
    if total_demand_m == 0:
        return {}

    # Proportional allocation
    caps: dict[JobType, int] = {}
    for jt in _STREAMING_JOB_TYPES:
        profile = _JOB_PROFILES[jt.value]
        fraction = demands[jt] / total_demand_m
        job_budget_m = streaming_budget_m * fraction
        exec_cpu_m = profile["executor_cores"] * 1000
        max_executors = max(2, int(job_budget_m // exec_cpu_m))
        # Never exceed the uncapped profile count
        uncapped = _scale_executor_count(profile, scale)
        caps[jt] = min(max_executors, uncapped)

    return caps


def _scale_partitions(profile: dict[str, Any], scale: int, executor_count: int, cores: int) -> str:
    """Derive shuffle partition count from executor count and cores.

    Target: 2x total cores.
    """
    if scale <= 10:
        return str(profile["base_partitions"])
    return str(executor_count * cores * 2)


# Supported Spark minor versions.  Each entry maps to a validated set of
# Hadoop, Scala, and AWS SDK dependencies in ``_spark_compat()``.  Only
# these (major, minor) pairs are accepted -- untested combinations risk
# dependency mismatches at runtime.
_SUPPORTED_SPARK_VERSIONS: dict[tuple[int, int], str] = {
    (3, 5): "3.5.x (Scala 2.12, Hadoop AWS 3.3.4)",
    (4, 0): "4.0.x (Scala 2.13, Hadoop AWS 3.4.1)",
    (4, 1): "4.1.x (Scala 2.13, Hadoop AWS 3.4.1)",
}

# ---------------------------------------------------------------------------
# Table format version compatibility matrix
# ---------------------------------------------------------------------------
# Maps (spark_major, spark_minor) -> {format: [compatible_versions]}.
# Used by resolve_format_version() and validate_format_version().

_FORMAT_VERSION_COMPAT: dict[tuple[int, int], dict[str, list[str]]] = {
    (3, 5): {
        "iceberg": ["1.5.2", "1.6.1", "1.7.1", "1.8.1", "1.9.1", "1.10.1"],
        "delta": [],  # Delta 4.x requires Spark 4.x
    },
    (4, 0): {
        "iceberg": ["1.10.0", "1.10.1"],
        "delta": ["4.0.0"],
    },
    (4, 1): {
        "iceberg": ["1.10.0", "1.10.1"],
        "delta": ["4.1.0"],
    },
}

# Default format version per Spark version (auto-selected when user doesn't override).
_FORMAT_VERSION_DEFAULTS: dict[tuple[int, int], dict[str, str]] = {
    (3, 5): {"iceberg": "1.10.1", "delta": ""},
    (4, 0): {"iceberg": "1.10.1", "delta": "4.0.0"},
    (4, 1): {"iceberg": "1.10.1", "delta": "4.1.0"},
}

# Iceberg runtime artifact suffix per Spark version.  Iceberg publishes
# one runtime jar per Spark major release (3.5, 4.0) -- Spark 4.1 reuses
# the 4.0 runtime because Iceberg hasn't published a 4.1-specific artifact.
_ICEBERG_RUNTIME_SUFFIX: dict[tuple[int, int], str] = {
    (3, 5): "3.5",
    (4, 0): "4.0",
    (4, 1): "4.0",  # Iceberg 1.10.1 has no 4.1 runtime; 4.0 is compatible
}


def _parse_spark_major(image: str) -> int:
    """Parse and validate the Spark version from an image tag.

    Accepts tags like ``apache/spark:3.5.4-python3``, ``apache/spark:4.0.2-python3``,
    or custom registries like ``my-registry/spark:3.5.4-python3``.  The ``-python3``
    suffix is required -- PySpark scripts need a Python-enabled image.

    Only tested minor versions are accepted (see ``_SUPPORTED_SPARK_VERSIONS``).

    Returns:
        3 or 4

    Raises:
        ValueError: If the image tag cannot be parsed, the version is not
            supported, or the ``-python3`` suffix is missing.
    """
    tag = image.split(":")[-1]

    # Require -python3 suffix (PySpark scripts need Python in the image)
    if "-python3" not in tag:
        raise ValueError(
            f"Spark image '{image}' must use a '-python3' tag "
            f"(e.g., 'apache/spark:3.5.4-python3'). "
            f"PySpark scripts require a Python-enabled image."
        )

    # Strip known suffixes so "3.5.4-python3" becomes "3.5.4"
    for suffix in ("-python3", "-java17", "-java11", "-scala2.12", "-scala2.13"):
        tag = tag.replace(suffix, "")

    parts = tag.split(".")
    try:
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else -1
    except (ValueError, IndexError):
        raise ValueError(
            f"Cannot parse Spark version from image '{image}'. "
            f"Expected a tag like '3.5.4-python3' or '4.0.2-python3'."
        ) from None

    if (major, minor) in _SUPPORTED_SPARK_VERSIONS:
        return major

    supported = ", ".join(desc for desc in _SUPPORTED_SPARK_VERSIONS.values())
    raise ValueError(
        f"Unsupported Spark version {major}.{minor} in image '{image}'. "
        f"Tested versions: {supported}."
    )


def _parse_spark_major_minor(image: str) -> tuple[int, int]:
    """Parse Spark major.minor version from image tag.

    Returns (major, minor) tuple, e.g. (4, 1) for ``apache/spark:4.1.1-python3``.
    """
    tag = image.split(":")[-1]
    for suffix in ("-python3", "-java17", "-java11", "-scala2.12", "-scala2.13"):
        tag = tag.replace(suffix, "")
    parts = tag.split(".")
    try:
        return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
    except (ValueError, IndexError):
        return 0, 0


def _spark_compat(image: str) -> tuple[str, str, str]:
    """Derive Scala suffix, Hadoop AWS version, and AWS SDK version from Spark image tag.

    Returns:
        Tuple of (scala_suffix, hadoop_aws_version, aws_sdk_version).
        Spark 3.x -> (``"_2.12"``, ``"3.3.4"``, ``"1.12.262"``),
        Spark 4.x -> (``"_2.13"``, ``"3.4.1"``, ``"1.12.367"``).
    """
    major = _parse_spark_major(image)
    if major >= 4:
        return "_2.13", "3.4.1", "1.12.367"
    return "_2.12", "3.3.4", "1.12.262"


def _delta_spark_artifact(scala_suffix: str, delta_version: str) -> str:
    """Build the Maven artifact ID for delta-spark.

    Delta 4.1.0+ changed the naming convention to include the Spark version
    prefix in the artifact ID: ``delta-spark_4.1_2.13``.  Earlier versions
    (4.0.x) use the plain ``delta-spark_2.13`` naming.
    """
    parts = delta_version.split(".")
    major = int(parts[0]) if parts else 0
    minor = int(parts[1]) if len(parts) > 1 else 0
    if major >= 4 and minor >= 1:
        return f"io.delta:delta-spark_{major}.{minor}{scala_suffix}:{delta_version}"
    return f"io.delta:delta-spark{scala_suffix}:{delta_version}"


def resolve_format_version(spark_image: str, format_type: str, user_version: str) -> str:
    """Resolve the table format version for the given Spark image.

    If *user_version* is the schema default (matching the hardcoded default in
    IcebergConfig or DeltaConfig), returns the auto-selected version for the
    Spark major.minor. Otherwise validates *user_version* against the
    compatibility matrix and returns it unchanged.

    Raises:
        ValueError: If *user_version* is incompatible with the Spark version.
    """
    major, minor = _parse_spark_major_minor(spark_image)
    key = (major, minor)

    defaults = _FORMAT_VERSION_DEFAULTS.get(key, {})
    compat = _FORMAT_VERSION_COMPAT.get(key, {})

    # Auto-resolve: return default for this Spark+format combo
    if not user_version or user_version == "auto":
        default = defaults.get(format_type, "")
        if not default:
            raise ValueError(f"{format_type.title()} is not supported with Spark {major}.{minor}.")
        return default

    # Validate user-specified version
    compatible = compat.get(format_type, [])
    if not compatible:
        raise ValueError(f"{format_type.title()} is not supported with Spark {major}.{minor}.")
    if user_version not in compatible:
        raise ValueError(
            f"{format_type.title()} {user_version} is not compatible with "
            f"Spark {major}.{minor}. Compatible versions: {', '.join(compatible)}."
        )
    return user_version


def validate_format_version(spark_image: str, format_type: str, version: str) -> None:
    """Validate that a table format version is compatible with the Spark image.

    Raises ValueError if incompatible.
    """
    resolve_format_version(spark_image, format_type, version)


class JobType(Enum):
    """Types of Spark jobs in the medallion pipeline."""

    # Batch jobs
    BRONZE_VERIFY = "bronze-verify"
    SILVER_BUILD = "silver-build"
    GOLD_FINALIZE = "gold-finalize"

    # Streaming jobs (sustained pipeline)
    BRONZE_INGEST = "bronze-ingest"
    SILVER_STREAM = "silver-stream"
    GOLD_REFRESH = "gold-refresh"


# Streaming job types (for conditional manifest logic)
_STREAMING_JOB_TYPES = frozenset(
    {
        JobType.BRONZE_INGEST,
        JobType.SILVER_STREAM,
        JobType.GOLD_REFRESH,
    }
)


class JobState(Enum):
    """State of a Spark job."""

    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


@dataclass
class JobStatus:
    """Status of a Spark job."""

    name: str
    state: JobState
    message: str
    driver_pod: str | None = None
    start_time: str | None = None
    completion_time: str | None = None
    executor_count: int = 0


class SparkJobManager:
    """Manages Spark job submission and tracking.

    Implements the PipelineEngine protocol defined in lakebench.engine.
    """

    def engine_name(self) -> str:
        """Return engine identifier."""
        return "spark"

    def __init__(self, config: LakebenchConfig, k8s: K8sClient):
        """Initialize Spark job manager.

        Args:
            config: Lakebench configuration
            k8s: Kubernetes client
        """
        self.config = config
        self.k8s = k8s
        self.namespace = config.get_namespace()

        # Cache cluster capacity for streaming concurrent budget calculation
        try:
            cap = k8s.get_cluster_capacity()
            self._cluster_cpu_m: int | None = cap.total_cpu_millicores if cap else None
        except Exception as e:
            logger.warning("Could not get cluster capacity for streaming budget: %s", e)
            self._cluster_cpu_m = None

    def submit_job(
        self,
        job_type: JobType,
        extra_conf: dict[str, str] | None = None,
        cycle_env: dict[str, str] | None = None,
    ) -> JobStatus:
        """Submit a Spark job.

        Args:
            job_type: Type of job to submit
            extra_conf: Additional Spark configuration
            cycle_env: Extra environment variables for multi-cycle batch runs
                       (e.g. LB_SILVER_INCREMENTAL, LB_GOLD_INCREMENTAL)

        Returns:
            Initial JobStatus
        """
        job_name = f"lakebench-{job_type.value}"

        # Delete existing job if present
        self._delete_job(job_name)

        # Build SparkApplication manifest
        manifest = self._build_manifest(job_type, extra_conf, cycle_env=cycle_env)

        # Apply manifest
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        custom_api = k8s_client.CustomObjectsApi()

        retryable_codes = {429, 500, 502, 503, 504}
        last_exc: ApiException | None = None

        for attempt in range(4):  # 1 initial + 3 retries
            try:
                custom_api.create_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=self.namespace,
                    plural="sparkapplications",
                    body=manifest,
                )

                logger.info(f"Submitted Spark job: {job_name}")

                return JobStatus(
                    name=job_name,
                    state=JobState.SUBMITTED,
                    message=f"Job {job_name} submitted",
                )

            except ApiException as e:
                if e.status not in retryable_codes or attempt == 3:
                    logger.error(f"Failed to submit job: {e}")
                    return JobStatus(
                        name=job_name,
                        state=JobState.FAILED,
                        message=f"Failed to submit: {e.reason}",
                    )
                last_exc = e
                import random
                import time

                delay = (2**attempt) + random.uniform(0, 0.5)
                logger.warning(
                    "Transient K8s API error submitting %s (attempt %d/4), retrying in %.1fs: %s",
                    job_name,
                    attempt + 1,
                    delay,
                    e.reason,
                )
                time.sleep(delay)

        # Should not reach here, but satisfy type checker
        logger.error(f"Failed to submit job after retries: {last_exc}")
        return JobStatus(
            name=job_name,
            state=JobState.FAILED,
            message=f"Failed to submit after retries: {last_exc}",
        )

    def _delete_job(self, job_name: str) -> None:
        """Delete existing Spark job if present.

        Args:
            job_name: Name of job to delete
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        custom_api = k8s_client.CustomObjectsApi()

        try:
            custom_api.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=job_name,
            )
            # Wait for deletion
            time.sleep(2)
        except ApiException as e:
            if e.status != 404:
                logger.warning(f"Error deleting job {job_name}: {e}")

    def get_job_status(self, job_name: str) -> JobStatus:
        """Get status of a Spark job.

        Args:
            job_name: Name of job

        Returns:
            Current JobStatus
        """
        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        custom_api = k8s_client.CustomObjectsApi()

        try:
            obj = custom_api.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=self.namespace,
                plural="sparkapplications",
                name=job_name,
            )

            status = obj.get("status", {})
            app_state = status.get("applicationState", {})
            state_str = app_state.get("state", "UNKNOWN")

            try:
                state = JobState(state_str)
            except ValueError:
                state = JobState.UNKNOWN

            driver_info = status.get("driverInfo", {})

            return JobStatus(
                name=job_name,
                state=state,
                message=app_state.get("errorMessage", "") or state_str,
                driver_pod=driver_info.get("podName"),
                start_time=status.get("lastSubmissionAttemptTime"),
                completion_time=status.get("terminationTime"),
                executor_count=len(status.get("executorState", {}))
                if status.get("executorState")
                else 0,
            )

        except ApiException as e:
            if e.status == 404:
                return JobStatus(
                    name=job_name,
                    state=JobState.UNKNOWN,
                    message="Job not found",
                )
            raise

    def _build_manifest(
        self,
        job_type: JobType,
        extra_conf: dict[str, str] | None = None,
        cycle_env: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Build SparkApplication manifest.

        Args:
            job_type: Type of job
            extra_conf: Additional Spark configuration
            cycle_env: Extra env vars for multi-cycle batch (e.g. LB_SILVER_INCREMENTAL)

        Returns:
            SparkApplication manifest dict
        """
        cfg = self.config
        spark_cfg = cfg.platform.compute.spark
        s3 = cfg.platform.storage.s3

        job_name = f"lakebench-{job_type.value}"
        spark_major = _parse_spark_major(cfg.images.spark)

        # Per-job resource profile (proven at 1TB+ scale)
        profile = _JOB_PROFILES.get(job_type.value, _JOB_PROFILES["silver-build"])
        scale = cfg.architecture.workload.datagen.scale
        executor_count = _scale_executor_count(profile, scale)

        # Streaming: cap to concurrent budget (all 3 jobs + datagen share cluster)
        if job_type in _STREAMING_JOB_TYPES and self._cluster_cpu_m is not None:
            budget = _streaming_concurrent_budget(cfg, self._cluster_cpu_m)
            if job_type in budget and budget[job_type] < executor_count:
                logger.info(
                    "Concurrent budget: %s capped from %d to %d executors",
                    job_type.value,
                    executor_count,
                    budget[job_type],
                )
                executor_count = budget[job_type]

        # Per-job executor override (user escape hatch)
        override_map = {
            JobType.BRONZE_VERIFY: cfg.platform.compute.spark.bronze_executors,
            JobType.SILVER_BUILD: cfg.platform.compute.spark.silver_executors,
            JobType.GOLD_FINALIZE: cfg.platform.compute.spark.gold_executors,
            JobType.BRONZE_INGEST: cfg.platform.compute.spark.bronze_ingest_executors,
            JobType.SILVER_STREAM: cfg.platform.compute.spark.silver_stream_executors,
            JobType.GOLD_REFRESH: cfg.platform.compute.spark.gold_refresh_executors,
        }
        override = override_map.get(job_type)
        if override is not None:
            logger.info(
                "Using executor override for %s: %d (auto would be %d)",
                job_type.value,
                override,
                executor_count,
            )
            executor_count = override

        shuffle_partitions = _scale_partitions(
            profile,
            scale,
            executor_count,
            profile["executor_cores"],
        )

        # Driver resource overrides (global, applies to all jobs)
        driver_cores = profile["driver_cores"]
        driver_memory = profile["driver_memory"]

        # Version-conditional driver memory: Spark 4 needs 32g for silver/gold
        # due to 558MB SDK v2 bundle + K8s API polling overhead. Spark 3 is
        # fine with 24g (280MB SDK v1 bundle).  Profile defaults are the Spark 4
        # values (safe ceiling); downsize for Spark 3 when no user override.
        if (
            spark_major < 4
            and spark_cfg.driver_memory is None
            and job_type in (JobType.SILVER_BUILD, JobType.GOLD_FINALIZE)
        ):
            driver_memory = "24g"

        if spark_cfg.driver_cores is not None:
            logger.info(
                "Using driver cores override: %d (profile default: %d)",
                spark_cfg.driver_cores,
                driver_cores,
            )
            driver_cores = spark_cfg.driver_cores
        if spark_cfg.driver_memory is not None:
            logger.info(
                "Using driver memory override: %s (profile default: %s)",
                spark_cfg.driver_memory,
                driver_memory,
            )
            driver_memory = spark_cfg.driver_memory

        # Warn when executor count is high and driver resources may be insufficient
        if executor_count > 24:
            mem_lower = driver_memory.lower()
            if mem_lower.endswith("g"):
                try:
                    mem_gb = int(mem_lower.rstrip("g"))
                    if mem_gb < 24:
                        logger.warning(
                            "%s: %d executors but driver memory is %s -- "
                            "recommend >= 24g for >24 executors "
                            "(K8s API polling overhead)",
                            job_type.value,
                            executor_count,
                            driver_memory,
                        )
                except ValueError:
                    pass
        if executor_count > _MAX_EXECUTORS_SAFE:
            logger.warning(
                "%s: %d executors exceeds safe limit of %d -- "
                "32+ executors can cause K8s API polling storms",
                job_type.value,
                executor_count,
                _MAX_EXECUTORS_SAFE,
            )

        # Dynamic maxResultSize: scales with executor count.
        # Each executor sends serialized task results to the driver.
        # Spark 4 needs a higher floor (8g) because the larger SDK v2 bundle
        # consumes more driver heap, leaving less room for result buffers.
        # Spark 3 uses the original formula (floor 4g).
        if spark_major >= 4:
            max_result_gb = min(16, max(8, executor_count // 2))
        else:
            max_result_gb = min(16, max(4, executor_count // 3))

        # Select script based on job type and table format.
        # Scripts are mounted from ConfigMap at /opt/spark/scripts.
        # Delta scripts use *_delta.py variants; format-agnostic scripts
        # (bronze_verify) are shared.
        table_format = cfg.architecture.table_format.type.value
        script_map = {
            JobType.BRONZE_VERIFY: "bronze_verify.py",
            JobType.SILVER_BUILD: "silver_build.py",
            JobType.GOLD_FINALIZE: "gold_finalize.py",
            JobType.BRONZE_INGEST: "bronze_ingest.py",
            JobType.SILVER_STREAM: "silver_stream.py",
            JobType.GOLD_REFRESH: "gold_refresh.py",
        }
        if table_format == "delta":
            script_map.update(
                {
                    JobType.SILVER_BUILD: "silver_build_delta.py",
                    JobType.GOLD_FINALIZE: "gold_finalize_delta.py",
                    JobType.BRONZE_INGEST: "bronze_ingest_delta.py",
                    JobType.SILVER_STREAM: "silver_stream_delta.py",
                    JobType.GOLD_REFRESH: "gold_refresh_delta.py",
                }
            )
        # Use local:// to reference scripts already in the container filesystem
        # (mounted from lakebench-spark-scripts ConfigMap)
        main_file = f"local:///opt/spark/scripts/{script_map[job_type]}"

        # Build Spark configuration (start from user overrides, then apply profile)
        spark_conf = dict(cfg.spark.conf)

        # Apply per-job shuffle partition count (scales with executor count)
        spark_conf["spark.sql.shuffle.partitions"] = shuffle_partitions
        spark_conf["spark.default.parallelism"] = shuffle_partitions

        # Parse Spark version from image tag (e.g., apache/spark:3.5.4 -> 3.5)
        spark_image_tag = cfg.images.spark.split(":")[-1]
        major_minor_key = _parse_spark_major_minor(cfg.images.spark)
        scala_suffix, hadoop_version, aws_sdk_version = _spark_compat(cfg.images.spark)
        # Iceberg runtime artifact suffix (Spark 4.1 reuses the 4.0 runtime)
        iceberg_runtime_suffix = _ICEBERG_RUNTIME_SUFFIX.get(
            major_minor_key, f"{major_minor_key[0]}.{major_minor_key[1]}"
        )

        # Catalog type needed for packages and catalog config
        catalog_name = cfg.architecture.query_engine.trino.catalog_name
        catalog_type = cfg.architecture.catalog.type.value

        # Build spark.jars.packages and extensions based on table format
        if table_format == "delta":
            delta_version = cfg.architecture.table_format.delta.version
            packages = [
                _delta_spark_artifact(scala_suffix, delta_version),
                f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
            ]
            if spark_major < 4:
                packages.append(
                    f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}",
                )
            if catalog_type == "unity":
                unity_version = cfg.architecture.catalog.unity.spark_connector_version
                packages.append(
                    f"io.unitycatalog:unitycatalog-spark{scala_suffix}:{unity_version}",
                )
            spark_conf["spark.jars.packages"] = ",".join(packages)
            spark_conf["spark.sql.extensions"] = "io.delta.sql.DeltaSparkSessionExtension"
        else:
            # Iceberg (default)
            iceberg_version = cfg.architecture.table_format.iceberg.version
            packages = [
                f"org.apache.iceberg:iceberg-spark-runtime-{iceberg_runtime_suffix}{scala_suffix}:{iceberg_version}",
                f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}",
                f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
            ]
            if spark_major < 4:
                packages.append(
                    f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}",
                )
            if catalog_type == "unity":
                unity_version = cfg.architecture.catalog.unity.spark_connector_version
                packages.append(
                    f"io.unitycatalog:unitycatalog-spark{scala_suffix}:{unity_version}",
                )
            spark_conf["spark.jars.packages"] = ",".join(packages)
            spark_conf["spark.sql.extensions"] = (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )

        # Warehouse bucket depends on which stage writes: gold jobs target the
        # gold bucket, bronze-ingest targets the bronze bucket, all others
        # target the silver bucket so Iceberg table data lands correctly.
        if job_type in (JobType.GOLD_FINALIZE, JobType.GOLD_REFRESH):
            warehouse_bucket = s3.buckets.gold
        elif job_type == JobType.BRONZE_INGEST:
            warehouse_bucket = s3.buckets.bronze
        else:
            warehouse_bucket = s3.buckets.silver

        # S3A endpoint (shared -- both Hive and Polaris catalogs use S3A for data I/O)
        spark_conf.update(
            {
                "spark.hadoop.fs.s3a.endpoint": s3.endpoint,
                "spark.hadoop.fs.s3a.path.style.access": str(s3.path_style).lower(),
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            }
        )

        # Catalog-specific config -- varies by table format AND catalog type
        if table_format == "delta":
            # Delta Lake catalog configuration
            if catalog_type == "unity":
                unity_port = cfg.architecture.catalog.unity.port
                # UCSingleCatalog adds /api/2.1/unity-catalog internally --
                # pass the base URL only.
                unity_uri = (
                    f"http://lakebench-unity.{self.namespace}.svc.cluster.local:{unity_port}"
                )
                spark_conf.update(
                    {
                        # Named catalog for Unity table registration
                        f"spark.sql.catalog.{catalog_name}": "io.unitycatalog.spark.UCSingleCatalog",
                        f"spark.sql.catalog.{catalog_name}.uri": unity_uri,
                        f"spark.sql.catalog.{catalog_name}.token": "",
                        # Delta also requires DeltaCatalog on spark_catalog
                        # for EXTERNAL table writes (CREATE TABLE ... USING DELTA)
                        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    }
                )
            else:
                # Delta + Hive Metastore
                # DeltaCatalog is a CatalogExtension -- must override spark_catalog
                # (not a named catalog) so Spark wires up the Hive delegate correctly.
                hive_uri = (
                    f"thrift://lakebench-hive-metastore.{self.namespace}.svc.cluster.local:9083"
                )
                spark_conf.update(
                    {
                        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                        "spark.sql.catalogImplementation": "hive",
                        "spark.hadoop.hive.metastore.uris": hive_uri,
                        "spark.sql.warehouse.dir": f"s3a://{warehouse_bucket}/warehouse/",
                    }
                )
        elif catalog_type == "polaris":
            # Iceberg + Polaris REST catalog
            polaris_port = cfg.architecture.catalog.polaris.port
            polaris_uri = (
                f"http://lakebench-polaris.{self.namespace}"
                f".svc.cluster.local:{polaris_port}/api/catalog"
            )
            polaris_conf = {
                f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
                f"spark.sql.catalog.{catalog_name}.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
                f"spark.sql.catalog.{catalog_name}.uri": polaris_uri,
                f"spark.sql.catalog.{catalog_name}.warehouse": catalog_name,
                f"spark.sql.catalog.{catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                f"spark.sql.catalog.{catalog_name}.s3.endpoint": s3.endpoint,
                f"spark.sql.catalog.{catalog_name}.s3.path-style-access": str(
                    s3.path_style
                ).lower(),
                # OAuth2 credential (client_id:client_secret)
                f"spark.sql.catalog.{catalog_name}.credential": f"{POLARIS_CLIENT_ID}:{POLARIS_CLIENT_SECRET}",
                f"spark.sql.catalog.{catalog_name}.scope": "PRINCIPAL_ROLE:ALL",
                f"spark.sql.catalog.{catalog_name}.token-refresh-enabled": "true",
                # FlashBlade: static S3 credentials on catalog (no STS vending)
                f"spark.sql.catalog.{catalog_name}.s3.access-key-id": s3.access_key,
                f"spark.sql.catalog.{catalog_name}.s3.secret-access-key": s3.secret_key,
            }
            if spark_major >= 4:
                polaris_conf[f"spark.sql.catalog.{catalog_name}.rest.http-client.type"] = "apache"
            spark_conf.update(polaris_conf)
        elif catalog_type == "unity":
            # Iceberg + Unity Catalog (uses UCSingleCatalog for read+write)
            # Unity OSS Iceberg REST only supports GET -- table creation
            # must go via Unity's native API, which UCSingleCatalog handles.
            # UCSingleCatalog adds /api/2.1/unity-catalog internally.
            unity_port = cfg.architecture.catalog.unity.port
            unity_uri = f"http://lakebench-unity.{self.namespace}.svc.cluster.local:{unity_port}"
            spark_conf.update(
                {
                    f"spark.sql.catalog.{catalog_name}": "io.unitycatalog.spark.UCSingleCatalog",
                    f"spark.sql.catalog.{catalog_name}.uri": unity_uri,
                    f"spark.sql.catalog.{catalog_name}.token": "",
                }
            )
        else:
            # Iceberg + Hive Metastore (default)
            spark_conf.update(
                {
                    f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
                    f"spark.sql.catalog.{catalog_name}.type": "hive",
                    f"spark.sql.catalog.{catalog_name}.uri": f"thrift://lakebench-hive-metastore.{self.namespace}.svc.cluster.local:9083",
                    f"spark.sql.catalog.{catalog_name}.warehouse": f"s3a://{warehouse_bucket}/warehouse/",
                    f"spark.sql.catalog.{catalog_name}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                    f"spark.sql.catalog.{catalog_name}.s3.endpoint": s3.endpoint,
                    f"spark.sql.catalog.{catalog_name}.s3.path-style-access": str(
                        s3.path_style
                    ).lower(),
                    f"spark.sql.catalog.{catalog_name}.hive.metastore-timeout": "5m",
                }
            )

        # Table-format-specific tuning (only for Iceberg)
        if table_format == "iceberg":
            spark_conf.update(
                {
                    f"spark.sql.catalog.{catalog_name}.s3.multipart.size": "268435456",
                    f"spark.sql.catalog.{catalog_name}.io.threads": "32",
                    f"spark.sql.catalog.{catalog_name}.io.manifest-encoder-threads": "16",
                    "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
                }
            )

        # Ivy cache must be writable
        spark_conf["spark.jars.ivy"] = "/tmp/.ivy2"
        spark_conf["spark.files.useFetchCache"] = "false"

        # S3A performance tuning (proven FlashBlade defaults)
        spark_conf.update(
            {
                "spark.hadoop.fs.s3a.fast.upload": "true",
                "spark.hadoop.fs.s3a.fast.upload.buffer": "bytebuffer",
                "spark.hadoop.fs.s3a.connection.maximum": "200",
                "spark.hadoop.fs.s3a.threads.max": "100",
                "spark.hadoop.fs.s3a.multipart.threshold": "268435456",
                "spark.hadoop.fs.s3a.max.total.tasks": "200",
                "spark.hadoop.fs.s3a.block.size": "268435456",
                "spark.hadoop.fs.s3a.connection.timeout": "60000",
            }
        )

        # Adaptive Query Execution
        spark_conf.update(
            {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "268435456",
            }
        )

        # Memory and stability tuning
        spark_conf.update(
            {
                "spark.memory.fraction": "0.8",
                "spark.memory.storageFraction": "0.3",
                "spark.dynamicAllocation.enabled": "false",
                "spark.network.timeout": "600s",
                "spark.executor.heartbeatInterval": "30s",
                "spark.task.maxFailures": "4",
                "spark.rpc.askTimeout": "300s",
                "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200",
                "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:MaxGCPauseMillis=200",
                "spark.kubernetes.executor.deleteOnTermination": "true",
                "spark.kubernetes.driver.service.deleteOnTermination": "true",
            }
        )

        # Dynamic maxResultSize -- scales with executor count.
        # Respect user override from spark.conf if already set.
        if "spark.driver.maxResultSize" not in spark_conf:
            spark_conf["spark.driver.maxResultSize"] = f"{max_result_gb}g"

        # Hive metastore client tuning (only for Hive catalog)
        if catalog_type != "polaris":
            spark_conf.update(
                {
                    "spark.hadoop.hive.metastore.client.socket.timeout": "300s",
                }
            )

        # Parquet tuning
        # BUG-006: Fixed 256MB partitions caused 26k tasks at scale 500,
        # overwhelming driver with Iceberg commit metadata. Scale-aware sizing:
        # - Target ~5000 tasks max to keep driver responsive
        # - Floor at 256MB to maintain parallelism at small scales
        # - Ceiling at 1GB (diminishing returns beyond that)
        target_tasks = 5000
        approx_bronze_gb = scale * 10
        partition_mb = max(256, min(1024, (approx_bronze_gb * 1024) // target_tasks))
        partition_bytes = partition_mb * 1024 * 1024
        spark_conf.update(
            {
                "spark.sql.parquet.compression.codec": "snappy",
                "spark.sql.parquet.filterPushdown": "true",
                "spark.sql.files.maxPartitionBytes": str(partition_bytes),
            }
        )

        # Spark 4 AppStatusListener regression: processing
        # SparkListenerExecutorMetricsUpdate events takes 25+ seconds each,
        # causing the driver's task scheduler to starve and executors to time
        # out.  Disable appStatusSource to prevent the listener from being
        # registered.  The UI stays enabled for prometheus scraping, but the
        # expensive per-event processing in AppStatusListener is eliminated.
        if spark_major >= 4:
            spark_conf["spark.metrics.appStatusSource.enabled"] = "false"

        # Prometheus metrics (for observability layer)
        if cfg.observability.enabled:
            spark_conf.update(
                {
                    "spark.ui.prometheus.enabled": "true",
                    "spark.metrics.namespace": "lakebench",
                    # PrometheusServlet sink -- exposes metrics at driver
                    # and executor scrape endpoints for Prometheus collection
                    "spark.metrics.conf.*.sink.prometheusServlet.class": "org.apache.spark.metrics.sink.PrometheusServlet",
                    "spark.metrics.conf.*.sink.prometheusServlet.path": "/metrics/prometheus",
                    "spark.metrics.conf.master.sink.prometheusServlet.path": "/metrics/master/prometheus",
                    "spark.metrics.conf.applications.sink.prometheusServlet.path": "/metrics/applications/prometheus",
                }
            )

        # Add extra configuration
        if extra_conf:
            spark_conf.update(extra_conf)

        # All volumes are defined exclusively in driver.template /
        # executor.template pod templates.  spark-submit merges the
        # template into its generated pod spec, avoiding duplicates
        # with the operator webhook (which also injects .spec.volumes
        # when it has RBAC).  ConfigMap volumes MUST go through the
        # pod template because Spark's KubernetesVolumeUtils does not
        # support the configMap type.
        packages_str = ",".join(packages)
        _pod_template_volumes: list[dict[str, Any]] = [
            {
                "name": "spark-scripts",
                "configMap": {"name": "lakebench-spark-scripts"},
            },
            {
                "name": "spark-work-dir",
                "emptyDir": {"sizeLimit": "20Gi"},
            },
            {
                "name": "spark-ivy-cache",
                "emptyDir": {"sizeLimit": "5Gi"},
            },
        ]
        _pod_template_volume_mounts = [
            {"name": "spark-scripts", "mountPath": "/opt/spark/scripts"},
            {"name": "spark-work-dir", "mountPath": "/opt/spark/work-dir"},
            {"name": "spark-ivy-cache", "mountPath": "/tmp/.ivy2"},
        ]
        driver_pod_template: dict[str, Any] = {
            "spec": {
                "volumes": _pod_template_volumes,
                "initContainers": [
                    {
                        "name": "resolve-deps",
                        "image": cfg.images.spark,
                        "imagePullPolicy": cfg.images.pull_policy.value,
                        "securityContext": {
                            "runAsUser": 185,
                            "runAsGroup": 185,
                        },
                        "command": [
                            "/bin/bash",
                            "-c",
                            (
                                "set -e; "
                                "/opt/spark/bin/spark-submit "
                                f'--packages "{packages_str}" '
                                "--conf spark.jars.ivy=/tmp/.ivy2 "
                                "--class org.apache.spark.deploy.DummyNonExistent "
                                "local:///dev/null 2>&1 || true; "
                                "echo 'Ivy cache warmed:'; "
                                "ls /tmp/.ivy2/jars/ 2>/dev/null | wc -l; "
                                "echo 'jars resolved'"
                            ),
                        ],
                        "volumeMounts": [
                            {"name": "spark-ivy-cache", "mountPath": "/tmp/.ivy2"},
                        ],
                    },
                ],
                "containers": [
                    {
                        "name": "spark-kubernetes-driver",
                        "volumeMounts": _pod_template_volume_mounts,
                    },
                ],
            },
        }
        executor_pod_template: dict[str, Any] = {
            "spec": {
                "volumes": _pod_template_volumes,
                "containers": [
                    {
                        "name": "spark-kubernetes-executor",
                        "volumeMounts": _pod_template_volume_mounts,
                    },
                ],
            },
        }

        # TLS truststore for HTTPS S3 endpoints with custom CA
        s3 = cfg.platform.storage.s3
        if s3.ca_cert:
            # Add CA cert secret + truststore emptyDir volumes
            _pod_template_volumes.extend(
                [
                    {
                        "name": "ca-cert",
                        "secret": {"secretName": "lakebench-ca-certificate"},
                    },
                    {"name": "truststore", "emptyDir": {}},
                ]
            )
            _truststore_mounts = [
                {
                    "name": "ca-cert",
                    "mountPath": "/etc/ssl/certs/custom-ca",
                    "readOnly": True,
                },
                {"name": "truststore", "mountPath": "/truststore"},
            ]
            # Init container: convert PEM to JKS truststore
            truststore_init: dict[str, Any] = {
                "name": "import-ca-cert",
                "image": cfg.images.spark,
                "imagePullPolicy": cfg.images.pull_policy.value,
                "securityContext": {"runAsUser": 185, "runAsGroup": 185},
                "command": [
                    "/bin/bash",
                    "-c",
                    "keytool -import -trustcacerts -alias custom-ca "
                    "-file /etc/ssl/certs/custom-ca/ca.crt "
                    "-keystore /truststore/truststore.jks "
                    "-storepass changeit -noprompt 2>/dev/null || true",
                ],
                "volumeMounts": _truststore_mounts,
            }
            driver_pod_template["spec"]["initContainers"].insert(0, truststore_init)
            # Add truststore mount to driver + executor containers
            for container in driver_pod_template["spec"]["containers"]:
                container["volumeMounts"].append({"name": "truststore", "mountPath": "/truststore"})
            for container in executor_pod_template["spec"]["containers"]:
                container["volumeMounts"].append({"name": "truststore", "mountPath": "/truststore"})
            # JVM truststore args for driver and executor
            _ts_opts = (
                " -Djavax.net.ssl.trustStore=/truststore/truststore.jks"
                " -Djavax.net.ssl.trustStorePassword=changeit"
            )
            spark_conf["spark.driver.extraJavaOptions"] = (
                spark_conf.get("spark.driver.extraJavaOptions", "") + _ts_opts
            ).strip()
            spark_conf["spark.executor.extraJavaOptions"] = (
                spark_conf.get("spark.executor.extraJavaOptions", "") + _ts_opts
            ).strip()

        # Check for scratch storage (Portworx) configuration
        scratch = cfg.platform.storage.scratch
        if scratch.enabled and scratch.storage_class:
            # Dynamic PVC for executor local storage
            # Use per-job profile PVC size -- silver needs 150Gi,
            # gold needs 100Gi, bronze-verify needs 50Gi.
            pvc_size = profile["scratch_size"]
            spark_conf.update(
                {
                    "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.claimName": "OnDemand",
                    "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.storageClass": scratch.storage_class,
                    "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit": pvc_size,
                    "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.path": "/tmp/spark-local",
                    "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.mount.readOnly": "false",
                }
            )
            # Point spark.local.dir to the PVC mount
            spark_conf["spark.local.dir"] = "/tmp/spark-local"

        _restart_policy: dict[str, Any] = (
            {"type": "Always"}
            if job_type in _STREAMING_JOB_TYPES
            else {
                "type": "OnFailure",
                "onFailureRetries": 2,
                "onFailureRetryInterval": 30,
                "onSubmissionFailureRetries": 2,
                "onSubmissionFailureRetryInterval": 30,
            }
        )

        manifest = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": job_name,
                "namespace": self.namespace,
                "labels": {
                    "app.kubernetes.io/name": "lakebench",
                    "app.kubernetes.io/instance": cfg.name,
                    "app.kubernetes.io/component": f"spark-{job_type.value}",
                    "app.kubernetes.io/managed-by": "lakebench",
                },
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": cfg.images.spark,
                "imagePullPolicy": cfg.images.pull_policy.value,
                "mainApplicationFile": main_file,
                # No deps.pyFiles needed - PYTHONPATH includes /opt/spark/scripts for common.py imports
                "sparkVersion": spark_image_tag,  # From config: images.spark
                "restartPolicy": _restart_policy,
                "driver": {
                    "cores": driver_cores,
                    "coreLimit": str(driver_cores),
                    "memory": driver_memory,
                    "serviceAccount": SPARK_SERVICE_ACCOUNT,
                    "securityContext": {
                        "runAsUser": 185,
                        "runAsGroup": 185,
                    },
                    "labels": {
                        "app": "lakebench-spark",
                        "component": "driver",
                    },
                    "env": self._build_env_vars(job_type, cycle_env=cycle_env),
                    "template": driver_pod_template,
                },
                "executor": {
                    "cores": profile["executor_cores"],
                    "coreLimit": str(profile["executor_cores"]),
                    "instances": executor_count,
                    "memory": profile["executor_memory"],
                    "memoryOverhead": profile["executor_memory_overhead"],
                    "podSecurityContext": {
                        "fsGroup": 185,
                    },
                    "securityContext": {
                        "runAsUser": 185,
                        "runAsGroup": 185,
                    },
                    "labels": {
                        "app": "lakebench-spark",
                        "component": "executor",
                    },
                    "env": self._build_env_vars(job_type, cycle_env=cycle_env),
                    "template": executor_pod_template,
                },
                "sparkConf": spark_conf,
            },
        }

        return manifest

    def _build_env_vars(
        self,
        job_type: JobType | None = None,
        cycle_env: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Build environment variables for Spark pods.

        Args:
            job_type: Job type (used for streaming-specific env vars)
            cycle_env: Extra env vars for multi-cycle batch runs

        Returns:
            List of env var dicts
        """
        cfg = self.config
        s3 = cfg.platform.storage.s3

        env: list[dict[str, Any]] = [
            # PYTHONPATH for finding common.py
            {"name": "PYTHONPATH", "value": "/opt/spark/scripts"},
            {
                "name": "AWS_ACCESS_KEY_ID",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "lakebench-s3-credentials",
                        "key": "accessKey",
                    },
                },
            },
            {
                "name": "AWS_SECRET_ACCESS_KEY",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": "lakebench-s3-credentials",
                        "key": "secretKey",
                    },
                },
            },
            {"name": "AWS_REGION", "value": s3.region},
            {"name": "S3_ENDPOINT", "value": s3.endpoint},
            {"name": "S3_REGION", "value": s3.region},
            {"name": "BRONZE_BUCKET", "value": s3.buckets.bronze},
            {"name": "SILVER_BUCKET", "value": s3.buckets.silver},
            {"name": "GOLD_BUCKET", "value": s3.buckets.gold},
            {"name": "CATALOG_NAME", "value": cfg.architecture.query_engine.trino.catalog_name},
            {"name": "LAKEBENCH_NAMESPACE", "value": self.namespace},
            # Lakebench env vars used by the pipeline scripts
            {"name": "LB_BRONZE_URI", "value": f"s3a://{s3.buckets.bronze}/"},
            {"name": "LB_SILVER_URI", "value": f"s3a://{s3.buckets.silver}/"},
            {"name": "LB_GOLD_URI", "value": f"s3a://{s3.buckets.gold}/"},
            {
                "name": "LB_ICEBERG_CATALOG",
                # Delta + Hive must use spark_catalog (session catalog);
                # all other formats use the named catalog from the recipe.
                "value": "spark_catalog"
                if cfg.architecture.table_format.type.value == "delta"
                and cfg.architecture.catalog.type.value != "unity"
                else cfg.architecture.query_engine.trino.catalog_name,
            },
            # Catalog type -- used by Delta scripts to choose managed vs EXTERNAL writes
            {"name": "LB_CATALOG_TYPE", "value": cfg.architecture.catalog.type.value},
            # Table names (configurable via architecture.tables)
            {"name": "LB_BRONZE_TABLE", "value": cfg.architecture.tables.bronze},
            {"name": "LB_SILVER_TABLE", "value": cfg.architecture.tables.silver},
            {"name": "LB_GOLD_TABLE", "value": cfg.architecture.tables.gold},
        ]

        # Streaming-specific env vars
        if job_type is not None and job_type in _STREAMING_JOB_TYPES:
            sustained = cfg.architecture.pipeline.sustained
            checkpoint_base = sustained.checkpoint_base

            # Per-job trigger interval and checkpoint location
            trigger_map = {
                JobType.BRONZE_INGEST: (
                    sustained.bronze_trigger_interval,
                    f"s3a://{s3.buckets.bronze}/{checkpoint_base}/bronze-ingest/",
                ),
                JobType.SILVER_STREAM: (
                    sustained.silver_trigger_interval,
                    f"s3a://{s3.buckets.silver}/{checkpoint_base}/silver-stream/",
                ),
                JobType.GOLD_REFRESH: (
                    sustained.gold_refresh_interval,
                    f"s3a://{s3.buckets.gold}/{checkpoint_base}/gold-refresh/",
                ),
            }
            trigger_interval, checkpoint_location = trigger_map[job_type]

            # Per-job target file size (MB → bytes for Iceberg property)
            target_file_size_map = {
                JobType.BRONZE_INGEST: sustained.bronze_target_file_size_mb,
                JobType.SILVER_STREAM: sustained.silver_target_file_size_mb,
                JobType.GOLD_REFRESH: sustained.gold_target_file_size_mb,
            }
            target_file_size_bytes = str(target_file_size_map[job_type] * 1024 * 1024)

            env.extend(
                [
                    {"name": "RUN_MODE", "value": "streaming"},
                    {"name": "TRIGGER_INTERVAL", "value": trigger_interval},
                    {"name": "CHECKPOINT_LOCATION", "value": checkpoint_location},
                    {
                        "name": "MAX_FILES_PER_TRIGGER",
                        "value": str(sustained.max_files_per_trigger),
                    },
                    {"name": "TARGET_FILE_SIZE_BYTES", "value": target_file_size_bytes},
                ]
            )

        # Multi-cycle batch env vars (e.g. LB_SILVER_INCREMENTAL=true)
        if cycle_env:
            env.extend({"name": k, "value": v} for k, v in cycle_env.items())

        return env

    def deploy_scripts_configmap(self) -> bool:
        """Deploy the Spark scripts as a ConfigMap.

        Creates lakebench-spark-scripts ConfigMap containing all pipeline scripts.
        These are mounted at /opt/spark/scripts in driver/executor pods.

        Returns:
            True if successful
        """
        cfg = self.config

        # Load script files (supports dev, pip install, and PyInstaller)
        from lakebench._resources import get_scripts_dir

        scripts_dir = get_scripts_dir()
        script_files = [
            "common.py",
            "bronze_verify.py",
            "silver_build.py",
            "gold_finalize.py",
            "bronze_ingest.py",
            "silver_stream.py",
            "gold_refresh.py",
            # Delta variants (same pipeline logic, Delta write API)
            "silver_build_delta.py",
            "gold_finalize_delta.py",
            "gold_refresh_delta.py",
            "bronze_ingest_delta.py",
            "silver_stream_delta.py",
        ]

        # Build ConfigMap data
        data = {}
        for script_file in script_files:
            script_path = scripts_dir / script_file
            if script_path.exists():
                data[script_file] = script_path.read_text()
                logger.info(f"Loaded script: {script_file}")

        if not data:
            logger.warning("No Spark scripts found")
            return False

        # Create ConfigMap manifest
        configmap = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": "lakebench-spark-scripts",
                "namespace": self.namespace,
                "labels": {
                    "app.kubernetes.io/name": "lakebench",
                    "app.kubernetes.io/instance": cfg.name,
                    "app.kubernetes.io/component": "spark-scripts",
                    "app.kubernetes.io/managed-by": "lakebench",
                },
            },
            "data": data,
        }

        # Apply ConfigMap
        try:
            success = self.k8s.apply_manifest(configmap)
            if success:
                logger.info("Deployed spark-scripts ConfigMap with %d scripts", len(data))
            else:
                logger.error("Failed to deploy spark-scripts ConfigMap (apply returned False)")
            return success
        except Exception as e:
            logger.error("Failed to deploy spark-scripts ConfigMap: %s", e)
            return False

    # Alias for backward compatibility
    def deploy_scripts(self) -> bool:
        """Deploy scripts. Alias for deploy_scripts_configmap()."""
        return self.deploy_scripts_configmap()
