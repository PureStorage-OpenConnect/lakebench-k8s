"""Scale factor definitions and schema-specific mappings.

Each schema maps an abstract scale factor (integer >= 1) to concrete
domain dimensions. The invariant is: scale 1 ≈ 10 GB on-disk bronze.

Scale replaces the old target_size configuration. Instead of specifying
raw data sizes, users specify a scale factor like HammerDB warehouses
or TPC-H SF. Each schema maps scale to its own domain dimensions.

Example YAML::

    datagen:
      scale: 10   # ~100 GB bronze, 1M customers for Customer360
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ScaleDimensions:
    """Concrete dimensions derived from a scale factor.

    Each schema produces these from its mapping function.
    The ``approx_bronze_gb`` is guidance only -- actual disk
    usage is measured from S3 after generation.
    """

    scale: int
    customers: int
    events_per_customer: int
    date_range_days: int
    approx_rows: int
    approx_bronze_gb: float

    @property
    def approx_bronze_tb(self) -> float:
        """Approximate bronze size in TB."""
        return self.approx_bronze_gb / 1024


@dataclass(frozen=True)
class ComputeGuidance:
    """Compute resource recommendation for a given scale.

    Used by ``lakebench validate`` to check whether the configured
    Spark executor resources are adequate for the chosen scale.
    """

    min_executors: int
    recommended_executors: int
    recommended_memory: str
    recommended_cores: int
    min_memory: str
    min_cores: int
    tier_name: str
    warning: str | None = None


@dataclass(frozen=True)
class TrinoGuidance:
    """Trino resource recommendation for a given scale."""

    worker_replicas: int
    worker_cpu: str
    worker_memory: str
    coordinator_cpu: str
    coordinator_memory: str


@dataclass(frozen=True)
class DatagenGuidance:
    """Datagen resource recommendation for a given scale.

    Proven defaults:
    - batch mode: 1 generator, 1 uploader, 4 CPU / 4Gi per pod
    - continuous mode: 8 generators, 2 uploaders, 8 CPU / 24Gi per pod
    """

    parallelism: int
    cpu: str
    memory: str
    mode: str = "batch"  # "batch" or "continuous"
    generators: int = 1  # per-pod generator processes
    uploaders: int = 1  # per-pod uploader threads


@dataclass(frozen=True)
class FullComputeGuidance:
    """Combined resource guidance for all auto-sized components."""

    spark: ComputeGuidance
    trino: TrinoGuidance
    datagen: DatagenGuidance


# ---------------------------------------------------------------------------
# Schema dimension functions
# ---------------------------------------------------------------------------


def customer360_dimensions(scale: int) -> ScaleDimensions:
    """Map scale factor to Customer360 domain dimensions.

    Scale 1  -> 100K customers,   ~24 events/customer, 365 days, ~10 GB
    Scale 10 -> 1M customers,     ~24 events/customer, 365 days, ~100 GB
    Scale 100 -> 10M customers,   ~24 events/customer, 365 days, ~1 TB
    Scale 1000 -> 100M customers, ~24 events/customer, 365 days, ~10 TB

    Scales linearly: customers = scale * 100,000.
    Events per customer is fixed to keep distribution realistic.
    """
    customers = scale * 100_000
    events_per_customer = 24
    date_range_days = 365
    approx_rows = customers * events_per_customer

    return ScaleDimensions(
        scale=scale,
        customers=customers,
        events_per_customer=events_per_customer,
        date_range_days=date_range_days,
        approx_rows=approx_rows,
        approx_bronze_gb=scale * 10.0,
    )


def iot_dimensions(scale: int) -> ScaleDimensions:
    """Map scale factor to IoT telemetry domain dimensions (future).

    Scale 1 -> 1K sensors x 30 days @ 1 reading/min, ~10 GB
    """
    sensors = scale * 1_000
    readings_per_sensor_per_day = 1440  # 1 per minute
    date_range_days = 30
    approx_rows = sensors * readings_per_sensor_per_day * date_range_days

    return ScaleDimensions(
        scale=scale,
        customers=sensors,  # "customers" = sensors in IoT context
        events_per_customer=readings_per_sensor_per_day * date_range_days,
        date_range_days=date_range_days,
        approx_rows=approx_rows,
        approx_bronze_gb=scale * 10.0,
    )


def financial_dimensions(scale: int) -> ScaleDimensions:
    """Map scale factor to Financial transactions domain dimensions (future).

    Scale 1 -> 500K accounts x 12 months @ 4 txns/account/month, ~10 GB
    """
    accounts = scale * 500_000
    txns_per_account_per_month = 4
    months = 12
    approx_rows = accounts * txns_per_account_per_month * months

    return ScaleDimensions(
        scale=scale,
        customers=accounts,  # "customers" = accounts in financial context
        events_per_customer=txns_per_account_per_month * months,
        date_range_days=365,
        approx_rows=approx_rows,
        approx_bronze_gb=scale * 10.0,
    )


# Registry of schema type -> dimension function
SCHEMA_DIMENSION_MAP: dict[str, Callable[..., ScaleDimensions]] = {
    "customer360": customer360_dimensions,
    "iot": iot_dimensions,
    "financial": financial_dimensions,
}


def get_dimensions(schema_type: str, scale: int) -> ScaleDimensions:
    """Get dimensions for any schema type.

    Args:
        schema_type: One of "customer360", "iot", "financial"
        scale: Scale factor (integer >= 1)

    Returns:
        ScaleDimensions with all derived values

    Raises:
        ValueError: If schema_type is unknown or scale < 1
    """
    if scale < 1:
        raise ValueError(f"Scale must be >= 1, got {scale}")
    func = SCHEMA_DIMENSION_MAP.get(schema_type)
    if func is None:
        raise ValueError(
            f"Unknown schema type: {schema_type}. Valid: {', '.join(sorted(SCHEMA_DIMENSION_MAP))}"
        )
    return func(scale)


# ---------------------------------------------------------------------------
# Compute guidance
# ---------------------------------------------------------------------------


def compute_guidance(scale: int) -> ComputeGuidance:
    """Get compute resource guidance for a given scale.

    These recommendations are schema-independent since scale always
    maps to approximately the same data volume (~10 GB per unit).

    Args:
        scale: Scale factor (>= 1)

    Returns:
        ComputeGuidance with tier name, minimum and recommended resources
    """
    if scale <= 5:
        return ComputeGuidance(
            min_executors=2,
            recommended_executors=2,
            recommended_memory="4g",
            recommended_cores=2,
            min_memory="2g",
            min_cores=1,
            tier_name="minimal",
        )
    elif scale <= 50:
        rec_exec = max(4, min(8, scale // 6))
        return ComputeGuidance(
            min_executors=4,
            recommended_executors=rec_exec,
            recommended_memory="16g",
            recommended_cores=4,
            min_memory="8g",
            min_cores=2,
            tier_name="balanced",
        )
    elif scale <= 500:
        rec_exec = max(8, min(16, scale // 30))
        return ComputeGuidance(
            min_executors=8,
            recommended_executors=rec_exec,
            recommended_memory="32g",
            recommended_cores=8,
            min_memory="16g",
            min_cores=4,
            tier_name="performance",
        )
    else:
        rec_exec = max(16, min(32, scale // 50))
        return ComputeGuidance(
            min_executors=16,
            recommended_executors=rec_exec,
            recommended_memory="48g",
            recommended_cores=8,
            min_memory="32g",
            min_cores=4,
            tier_name="extreme",
            warning=(
                f"Scale {scale} requires significant compute resources. "
                f"Recommend 48g+ memory and 16+ executors."
            ),
        )


def full_compute_guidance(scale: int) -> FullComputeGuidance:
    """Get combined resource guidance for Spark, Trino, and Datagen.

    Derives recommended resources from scale factor using four tiers.
    Datagen mode selection:
    - scale <= 10 (~100 GB): batch mode (1 generator, low resources)
    - scale > 10: continuous mode (8 generators + 2 uploaders, high resources)

    Args:
        scale: Scale factor (>= 1)

    Returns:
        FullComputeGuidance with Spark, Trino, and Datagen recommendations
    """
    spark = compute_guidance(scale)

    # Datagen: CPU and memory are hard-locked per mode (MVP sizing):
    #   batch:      4 CPU, 4Gi per pod, 1 generator, 1 uploader
    #   continuous: 8 CPU, 24Gi per pod, 8 generators, 2 uploaders
    # Observed peak at scale 100: ~18.4Gi with spikes above 20Gi.
    # 24Gi provides ~30% headroom above steady-state peak.
    # Scaling is done by parallelism (number of pods), not per-pod resources.

    if scale <= 5:
        # Minimal: ~10-50 GB, batch mode
        trino = TrinoGuidance(
            worker_replicas=1,
            worker_cpu="2",
            worker_memory="8Gi",
            coordinator_cpu="1",
            coordinator_memory="4Gi",
        )
        datagen = DatagenGuidance(
            parallelism=2,
            cpu="4",
            memory="4Gi",
            mode="batch",
            generators=1,
            uploaders=1,
        )
    elif scale <= 50:
        # Balanced: ~60-500 GB
        # scale <= 10 stays batch, scale > 10 switches to continuous
        if scale <= 10:
            datagen = DatagenGuidance(
                parallelism=4,
                cpu="4",
                memory="4Gi",
                mode="batch",
                generators=1,
                uploaders=1,
            )
        else:
            datagen = DatagenGuidance(
                parallelism=max(4, scale // 5),
                cpu="8",
                memory="24Gi",
                mode="continuous",
                generators=8,
                uploaders=2,
            )
        trino = TrinoGuidance(
            worker_replicas=2,
            worker_cpu="4",
            worker_memory="16Gi",
            coordinator_cpu="2",
            coordinator_memory="8Gi",
        )
    elif scale <= 500:
        # Performance: ~510 GB - 5 TB, continuous mode
        datagen = DatagenGuidance(
            parallelism=max(8, scale // 10),
            cpu="8",
            memory="24Gi",
            mode="continuous",
            generators=8,
            uploaders=2,
        )
        trino = TrinoGuidance(
            worker_replicas=max(4, scale // 50),
            worker_cpu="8",
            worker_memory="32Gi",
            coordinator_cpu="4",
            coordinator_memory="16Gi",
        )
    else:
        # Extreme: > 5 TB, continuous mode
        datagen = DatagenGuidance(
            parallelism=max(16, scale // 30),
            cpu="8",
            memory="24Gi",
            mode="continuous",
            generators=8,
            uploaders=2,
        )
        trino = TrinoGuidance(
            worker_replicas=max(8, scale // 100),
            worker_cpu="8",
            worker_memory="64Gi",
            coordinator_cpu="4",
            coordinator_memory="16Gi",
        )

    return FullComputeGuidance(spark=spark, trino=trino, datagen=datagen)
