"""Scale factor definitions and schema-specific mappings.

Each schema maps an abstract scale factor to concrete domain dimensions. The
invariant is: scale 1 ~ 10 GB on-disk bronze.

Users specify a scale factor, like HammerDB warehouses or TPC-H SF, rather
than a raw data size. Each schema maps scale to its own domain dimensions.

Scale is a float so local mode can ask for datasets smaller than 10 GB, which
is the smallest a cluster run would ever want. Scale 0.01 is ~100 MB and
completes on a laptop in seconds. Every derived count is rounded to an int at
the point of derivation, so no fractional row or customer count reaches
datagen.

Example YAML::

    datagen:
      scale: 10     # ~100 GB bronze, 1M customers for Customer360
      scale: 0.1    # ~1 GB bronze, 10K customers -- local mode
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ScaleDimensions:
    """Concrete dimensions derived from a scale factor.

    Each schema produces these from its mapping function.
    ``approx_bronze_gb`` is the expected bronze size ``scale_ratio`` divides
    by (measured for AML); actual usage is measured from S3 after
    generation.
    """

    scale: float
    customers: int
    events_per_customer: int
    date_range_days: int
    approx_rows: int
    approx_bronze_gb: float
    #: The size the datagen Job passes as ``--target-tb``, when it is not
    #: ``approx_bronze_gb`` (``datagen_target_gb``).
    generator_target_gb: float | None = None

    @property
    def approx_bronze_tb(self) -> float:
        """Approximate bronze size in TB."""
        return self.approx_bronze_gb / 1024

    @property
    def datagen_target_gb(self) -> float:
        """The size the datagen Job's ``--target-tb`` carries. Customer 360's
        generator sizes its file count from it; the AML generator sizes from
        ``--scale`` and ignores it, and AML keeps the value it always had so
        the Job's arguments do not move."""
        if self.generator_target_gb is not None:
            return self.generator_target_gb
        return self.approx_bronze_gb


@dataclass(frozen=True)
class ComputeGuidance:
    """The compute tier of a scale, and a warning for the extreme tier.

    Spark is not sized from it: executor resources are the job profiles.
    """

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
    """The default datagen pod count for a scale. Per-pod CPU and memory
    come from the autosizer's memory model, not from the tier."""

    parallelism: int


@dataclass(frozen=True)
class FullComputeGuidance:
    """Combined resource guidance for all auto-sized components."""

    spark: ComputeGuidance
    trino: TrinoGuidance
    datagen: DatagenGuidance


# ---------------------------------------------------------------------------
# Scale tier thresholds
# ---------------------------------------------------------------------------

# Upper bounds for each compute tier (inclusive).
# "minimal" = scale 1..5, "balanced" = 6..50, "performance" = 51..500, "extreme" = 501+
SCALE_TIER_MINIMAL = 5
SCALE_TIER_BALANCED = 50
SCALE_TIER_PERFORMANCE = 500

# Balanced-tier scale up to which datagen keeps 4 pods; above it the pod
# count grows with scale.
SCALE_DATAGEN_BATCH_MAX = 10

# Spark memory the extreme-tier warning recommends.
SPARK_MEM_EXTREME = "48g"

# ---------------------------------------------------------------------------
# Schema dimension functions
# ---------------------------------------------------------------------------

# Smallest scale that still produces a dataset. Below 10 GB the numbers stop
# being a benchmark and start being a smoke test, which is exactly what local
# mode wants.
MIN_SCALE = 0.01


def _entity_count(scale: float, per_unit: int) -> int:
    """Scale an entity count, keeping it a whole number of at least one.

    Datagen cannot generate a fraction of a customer, and a scale small enough
    to round to zero would produce an empty dataset that fails much later with
    a confusing error.
    """
    return max(1, round(scale * per_unit))


def customer360_dimensions(scale: float) -> ScaleDimensions:
    """Map scale factor to Customer360 domain dimensions.

    Scale 0.01 -> 1K customers,   ~24 events/customer, 365 days, ~100 MB
    Scale 1  -> 100K customers,   ~24 events/customer, 365 days, ~10 GB
    Scale 10 -> 1M customers,     ~24 events/customer, 365 days, ~100 GB
    Scale 100 -> 10M customers,   ~24 events/customer, 365 days, ~1 TB
    Scale 1000 -> 100M customers, ~24 events/customer, 365 days, ~10 TB

    Scales linearly: customers = scale * 100,000.
    Events per customer is fixed to keep distribution realistic.
    """
    customers = _entity_count(scale, 100_000)
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


def financial_dimensions(scale: float) -> ScaleDimensions:
    """Map scale factor to Financial (AML) domain dimensions.

    Mirrors what datagen_rs writes (``world::dimensions``): 111,111 entities
    per scale unit, 4 transactions per entity per month, over a 60-month
    corpus (the entrypoint's ``--corpus-months`` default).

    Scale 1   -> 111K entities, ~26.7M txns, ~8.5 GB pacs.008
    Scale 10  -> 1.1M entities, ~267M txns,  ~94 GB
    Scale 100 -> 11M entities,  ~2.7B txns,  ~936 GB

    ``approx_bronze_gb`` is measured, not estimated: ``scale *
    financial_gb_per_scale_unit(scale)``, from the pacs.008 bytes
    bronze-verify read at scales 1 and 10 (docs/data-generation.md has the
    runs). It feeds ``scale_ratio``, so a flat 10 GB/scale guess made
    every complete AML run read as 84% "incomplete", and the flat 8.4 GB of
    scale 1 made complete scale-10 and scale-100 runs read 111% to 112%. The
    reference tables (party, account) and the manifest are excluded, as
    bronze-verify measures only the pacs.008 tree.
    """
    entities = _entity_count(scale, 111_111)
    txns_per_entity_per_month = 4
    months = 60
    approx_rows = entities * txns_per_entity_per_month * months

    return ScaleDimensions(
        scale=scale,
        customers=entities,  # "customers" = entities in the financial context
        events_per_customer=txns_per_entity_per_month * months,
        date_range_days=1826,
        approx_rows=approx_rows,
        approx_bronze_gb=scale * financial_gb_per_scale_unit(scale),
        # The Job's --target-tb as before (8.4 GB per unit): the AML generator
        # ignores it, and the argument list is pinned (byte-compare reference).
        generator_target_gb=scale * 8.4,
    )


#: pacs.008 Parquet GB per AML scale unit, measured with the default 64 MB
#: files on the v1.6 generator: what bronze-verify read at scale 1 (8.47 GB,
#: 26.7M rows) and 10 (93.6 GB, 267M rows). The collector's GB are GiB
#: (bytes / 1024**3). Rows are linear in scale; bytes per row grow from about
#: 341 to 377 between the two. Above scale 10 the
#: scale-10 value holds: the two scale-100 runs on record are not the same
#: setup (128 MB files, or an earlier generator) and read 0.4% and 1.8% above
#: it (docs/data-generation.md).
FINANCIAL_GB_PER_SCALE_UNIT: tuple[tuple[float, float], ...] = (
    (1.0, 8.47),
    (10.0, 9.36),
)


def financial_gb_per_scale_unit(scale: float) -> float:
    """GB per scale unit at *scale*: the measured value at a measured scale,
    interpolated linearly in log10(scale) between two, and the nearest
    measurement outside them (below scale 1 the scale-1 value, above scale
    10 the scale-10 value)."""
    import math

    points = FINANCIAL_GB_PER_SCALE_UNIT
    if scale <= points[0][0]:
        return points[0][1]
    if scale >= points[-1][0]:
        return points[-1][1]
    for (s0, g0), (s1, g1) in zip(points, points[1:], strict=False):
        if s0 <= scale <= s1:
            t = (math.log10(scale) - math.log10(s0)) / (math.log10(s1) - math.log10(s0))
            return g0 + t * (g1 - g0)
    return points[-1][1]  # not reached: the points cover every scale above


# Registry of schema type -> dimension function
SCHEMA_DIMENSION_MAP: dict[str, Callable[..., ScaleDimensions]] = {
    "customer360": customer360_dimensions,
    "financial": financial_dimensions,
}


def get_dimensions(schema_type: str, scale: float) -> ScaleDimensions:
    """Get dimensions for any schema type.

    Args:
        schema_type: One of "customer360", "financial"
        scale: Scale factor (>= MIN_SCALE)

    Returns:
        ScaleDimensions with all derived values

    Raises:
        ValueError: If schema_type is unknown or scale < MIN_SCALE
    """
    if scale < MIN_SCALE:
        raise ValueError(f"Scale must be >= {MIN_SCALE}, got {scale}")
    func = SCHEMA_DIMENSION_MAP.get(schema_type)
    if func is None:
        raise ValueError(
            f"Unknown schema type: {schema_type}. Valid: {', '.join(sorted(SCHEMA_DIMENSION_MAP))}"
        )
    return func(scale)


# ---------------------------------------------------------------------------
# Compute guidance
# ---------------------------------------------------------------------------


def compute_guidance(scale: float) -> ComputeGuidance:
    """Get compute resource guidance for a given scale.

    These recommendations are schema-independent since scale always
    maps to approximately the same data volume (~10 GB per unit).

    Args:
        scale: Scale factor (>= 1)

    Returns:
        ComputeGuidance with the tier name and, for the extreme tier, a warning
    """
    if scale <= SCALE_TIER_MINIMAL:
        return ComputeGuidance(tier_name="minimal")
    if scale <= SCALE_TIER_BALANCED:
        return ComputeGuidance(tier_name="balanced")
    if scale <= SCALE_TIER_PERFORMANCE:
        return ComputeGuidance(tier_name="performance")
    return ComputeGuidance(
        tier_name="extreme",
        warning=(
            f"Scale {scale} requires significant compute resources. "
            f"Recommend {SPARK_MEM_EXTREME}+ memory and 16+ executors."
        ),
    )


def full_compute_guidance(scale: float) -> FullComputeGuidance:
    """Get combined resource guidance for Spark, Trino, and Datagen.

    Derives Trino resources and the datagen pod count from the scale factor
    using four tiers.

    Args:
        scale: Scale factor (>= 1)

    Returns:
        FullComputeGuidance with Spark, Trino, and Datagen recommendations
    """
    spark = compute_guidance(scale)

    # Datagen scales by pod count only; the autosizer sizes each pod.

    if scale <= SCALE_TIER_MINIMAL:
        # Minimal: ~10-50 GB
        trino = TrinoGuidance(
            worker_replicas=1,
            worker_cpu="2",
            worker_memory="8Gi",
            coordinator_cpu="1",
            coordinator_memory="4Gi",
        )
        datagen = DatagenGuidance(parallelism=2)
    elif scale <= SCALE_TIER_BALANCED:
        # Balanced: ~60-500 GB
        if scale <= SCALE_DATAGEN_BATCH_MAX:
            datagen = DatagenGuidance(parallelism=4)
        else:
            datagen = DatagenGuidance(parallelism=max(4, int(scale // 5)))
        trino = TrinoGuidance(
            worker_replicas=2,
            worker_cpu="4",
            worker_memory="16Gi",
            coordinator_cpu="2",
            coordinator_memory="8Gi",
        )
    elif scale <= SCALE_TIER_PERFORMANCE:
        # Performance: ~510 GB - 5 TB
        datagen = DatagenGuidance(parallelism=max(8, int(scale // 10)))
        trino = TrinoGuidance(
            worker_replicas=max(4, int(scale // 25)),
            worker_cpu="8",
            worker_memory="48Gi",
            coordinator_cpu="4",
            coordinator_memory="16Gi",
        )
    else:
        # Extreme: > 5 TB
        datagen = DatagenGuidance(parallelism=max(16, int(scale // 30)))
        trino = TrinoGuidance(
            worker_replicas=max(10, int(scale // 50)),
            worker_cpu="8",
            worker_memory="64Gi",
            coordinator_cpu="4",
            coordinator_memory="16Gi",
        )

    return FullComputeGuidance(spark=spark, trino=trino, datagen=datagen)
