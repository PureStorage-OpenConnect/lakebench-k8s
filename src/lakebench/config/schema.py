"""Pydantic models for Lakebench configuration.

This module defines the complete configuration schema for Lakebench,
matching the specification in lakebench-spec.md Section 4.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# =============================================================================
# Enums
# =============================================================================


class ImagePullPolicy(str, Enum):
    """Kubernetes image pull policy."""

    ALWAYS = "Always"
    IF_NOT_PRESENT = "IfNotPresent"
    NEVER = "Never"


class CatalogType(str, Enum):
    """Supported catalog types."""

    HIVE = "hive"
    POLARIS = "polaris"
    UNITY = "unity"
    NONE = "none"


class TableFormatType(str, Enum):
    """Supported table formats."""

    ICEBERG = "iceberg"
    HUDI = "hudi"


class FileFormatType(str, Enum):
    """Supported file formats."""

    PARQUET = "parquet"
    ORC = "orc"
    AVRO = "avro"


class QueryEngineType(str, Enum):
    """Supported query engines."""

    TRINO = "trino"
    SPARK_THRIFT = "spark-thrift"
    DUCKDB = "duckdb"
    NONE = "none"


class PipelineEngineType(str, Enum):
    """Supported pipeline processing engines."""

    SPARK = "spark"


class BenchmarkMode(str, Enum):
    """Benchmark execution mode.

    ``standard`` and ``extended`` are the user-facing names from the spec.
    ``power``, ``throughput``, and ``composite`` are TPC-style internal modes.
    The runner maps standard → power and extended → power (with iterations > 1).
    """

    STANDARD = "standard"
    EXTENDED = "extended"
    POWER = "power"
    THROUGHPUT = "throughput"
    COMPOSITE = "composite"


class ProcessingPattern(str, Enum):
    """Supported processing patterns."""

    MEDALLION = "medallion"
    STREAMING = "streaming"
    BATCH = "batch"
    CUSTOM = "custom"


class WorkloadSchema(str, Enum):
    """Supported workload schemas for data generation."""

    CUSTOMER360 = "customer360"
    IOT = "iot"
    FINANCIAL = "financial"
    CUSTOM = "custom"


class DatagenMode(str, Enum):
    """Datagen execution mode.

    Distinguished between:
    - batch: single generator thread per pod, low resource profile (4 CPU / 4Gi).
      Best for small datasets (< 100 GB).
    - continuous: multi-process pipeline with multiple generator workers and
      dedicated uploader threads, higher resource profile (8 CPU / 24Gi).
      Best for large datasets (>= 100 GB) where sustained throughput matters.
    - auto: automatically selects batch or continuous based on scale factor.
      scale <= 10 (~100 GB) -> batch, scale > 10 -> continuous.

    CPU and memory are hard-locked per mode and cannot be overridden by the
    user.  The autosizer always sets them to the mode-correct values.
    """

    BATCH = "batch"
    CONTINUOUS = "continuous"
    AUTO = "auto"


class PipelineMode(str, Enum):
    """Pipeline execution mode.

    - batch: sequential medallion jobs
      (bronze-verify -> silver-build -> gold-finalize)
    - continuous: concurrent streaming jobs
      (bronze-ingest + silver-stream + gold-refresh)
    """

    BATCH = "batch"
    CONTINUOUS = "continuous"


class ReportFormat(str, Enum):
    """Supported report output formats."""

    HTML = "html"
    JSON = "json"
    BOTH = "both"


# =============================================================================
# Images Configuration
# =============================================================================


class ImagesConfig(BaseModel):
    """Container image configuration for all Lakebench components."""

    datagen: str = "docker.io/sillidata/lb-datagen:v2"
    spark: str = "apache/spark:3.5.4-python3"
    postgres: str = "postgres:17"
    hive: str = "apache/hive:3.1.3"
    polaris: str = "apache/polaris:1.3.0-incubating"
    unity: str = "unitycatalog/unitycatalog:0.1.0"
    trino: str = "trinodb/trino:479"
    duckdb: str = "python:3.11-slim"
    prometheus: str = "prom/prometheus:v2.48.0"
    grafana: str = "grafana/grafana:10.2.0"
    jmx_exporter: str = "bitnami/jmx-exporter:latest"

    pull_policy: ImagePullPolicy = ImagePullPolicy.IF_NOT_PRESENT
    pull_secrets: list[str] = Field(default_factory=list)


# =============================================================================
# Platform Layer Configuration (Layer 1)
# =============================================================================


class KubernetesConfig(BaseModel):
    """Kubernetes connection and namespace configuration."""

    context: str = ""  # Empty = use current context
    namespace: str = ""  # Empty = use default namespace
    create_namespace: bool = True


class S3BucketsConfig(BaseModel):
    """S3 bucket names for each data layer."""

    bronze: str = "lakebench-bronze"
    silver: str = "lakebench-silver"
    gold: str = "lakebench-gold"


class S3Config(BaseModel):
    """S3/object storage configuration."""

    endpoint: str = Field(
        default="", description="S3 endpoint URL (e.g., http://your-s3-endpoint:80)"
    )
    region: str = "us-east-1"
    path_style: bool = True  # Required for FlashBlade, MinIO

    # Credentials - either inline or reference to existing secret
    access_key: str = ""
    secret_key: str = ""
    secret_ref: str = ""  # Name of existing K8s secret

    buckets: S3BucketsConfig = Field(default_factory=S3BucketsConfig)
    create_buckets: bool = True

    @model_validator(mode="after")
    def validate_credentials(self) -> S3Config:
        """Ensure either inline credentials or secret_ref is provided."""
        has_inline = bool(self.access_key and self.secret_key)
        has_ref = bool(self.secret_ref)
        if not has_inline and not has_ref:
            # Defer validation - will be checked at runtime
            pass
        return self


class ScratchStorageConfig(BaseModel):
    """Scratch storage configuration for Spark shuffle."""

    enabled: bool = False
    storage_class: str = "px-csi-scratch"
    size: str = "100Gi"
    create_storage_class: bool = True
    provisioner: str = "pxd.portworx.com"
    parameters: dict[str, str] = Field(
        default_factory=lambda: {"repl": "1", "io_profile": "auto", "priority_io": "high"}
    )


class StorageConfig(BaseModel):
    """Storage configuration including S3 and scratch volumes."""

    s3: S3Config = Field(default_factory=S3Config)
    scratch: ScratchStorageConfig = Field(default_factory=ScratchStorageConfig)


class SparkDriverConfig(BaseModel):
    """Spark driver resource configuration."""

    cores: int = 4
    memory: str = "8g"


class SparkExecutorConfig(BaseModel):
    """Spark executor resource configuration."""

    instances: int = 8
    cores: int = 4
    memory: str = "48g"
    memory_overhead: str = "12g"


class SparkOperatorConfig(BaseModel):
    """Spark operator installation configuration."""

    install: bool = False
    namespace: str = "spark-operator"
    version: str = "2.4.0"  # v2.x properly injects volumes via webhook


class SparkComputeConfig(BaseModel):
    """Spark compute configuration."""

    operator: SparkOperatorConfig = Field(default_factory=SparkOperatorConfig)
    driver: SparkDriverConfig = Field(default_factory=SparkDriverConfig)
    executor: SparkExecutorConfig = Field(default_factory=SparkExecutorConfig)

    # Per-job executor count overrides (None = auto from scale).
    # When set, these override the auto-scaled executor count for that job.
    # Per-executor sizing (cores, memory, PVC) remains fixed from proven profiles.
    bronze_executors: int | None = Field(
        default=None,
        description="Override bronze-verify executor count. None = auto from scale.",
    )
    silver_executors: int | None = Field(
        default=None,
        description="Override silver-build executor count. None = auto from scale.",
    )
    gold_executors: int | None = Field(
        default=None,
        description="Override gold-finalize executor count. None = auto from scale.",
    )

    # Streaming job executor count overrides (None = auto from scale).
    bronze_ingest_executors: int | None = Field(
        default=None,
        description="Override bronze-ingest executor count. None = auto from scale.",
    )
    silver_stream_executors: int | None = Field(
        default=None,
        description="Override silver-stream executor count. None = auto from scale.",
    )
    gold_refresh_executors: int | None = Field(
        default=None,
        description="Override gold-refresh executor count. None = auto from scale.",
    )

    # Driver resource overrides (None = use profile defaults).
    # These are global - they apply to all Spark jobs. Use when cluster nodes
    # have limited memory or when running at extreme scales (500+).
    driver_memory: str | None = Field(
        default=None,
        description="Override driver memory (e.g., '8g', '16g'). None = profile default.",
    )
    driver_cores: int | None = Field(
        default=None,
        description="Override driver cores. None = profile default (typically 4).",
    )


class PostgresConfig(BaseModel):
    """PostgreSQL configuration for metadata backend."""

    storage: str = "10Gi"
    storage_class: str = ""  # Empty = default storage class


class ComputeConfig(BaseModel):
    """Compute resource configuration."""

    spark: SparkComputeConfig = Field(default_factory=SparkComputeConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)


class PlatformConfig(BaseModel):
    """Layer 1: Platform configuration."""

    kubernetes: KubernetesConfig = Field(default_factory=KubernetesConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    compute: ComputeConfig = Field(default_factory=ComputeConfig)


# =============================================================================
# Data Architecture Configuration (Layer 2)
# =============================================================================


class HiveThriftConfig(BaseModel):
    """Hive Metastore thrift server configuration."""

    min_threads: int = 10
    max_threads: int = 50
    client_timeout: str = "300s"


class HiveResourcesConfig(BaseModel):
    """Hive Metastore resource configuration."""

    cpu_min: str = "500m"
    cpu_max: str = "2"
    memory: str = "4Gi"


class HiveConfig(BaseModel):
    """Hive Metastore configuration."""

    thrift: HiveThriftConfig = Field(default_factory=HiveThriftConfig)
    resources: HiveResourcesConfig = Field(default_factory=HiveResourcesConfig)


class PolarisResourcesConfig(BaseModel):
    """Polaris resource configuration."""

    cpu: str = "1"
    memory: str = "2Gi"


class PolarisConfig(BaseModel):
    """Apache Polaris REST catalog configuration.

    Polaris is an open-source Iceberg REST catalog (port 8181).
    Uses relational-jdbc persistence backed by the shared PostgreSQL.
    On FlashBlade: stsUnavailable=true, pathStyleAccess=true.
    """

    version: str = "1.3.0-incubating"
    port: int = 8181
    resources: PolarisResourcesConfig = Field(default_factory=PolarisResourcesConfig)


class UnityConfig(BaseModel):
    """Unity Catalog configuration (placeholder for future)."""


class CatalogConfig(BaseModel):
    """Catalog service configuration."""

    type: CatalogType = CatalogType.HIVE
    hive: HiveConfig = Field(default_factory=HiveConfig)
    polaris: PolarisConfig = Field(default_factory=PolarisConfig)
    unity: UnityConfig = Field(default_factory=UnityConfig)


class IcebergConfig(BaseModel):
    """Apache Iceberg table format configuration."""

    version: str = "1.10.1"
    file_format: FileFormatType = FileFormatType.PARQUET
    properties: dict[str, Any] = Field(default_factory=dict)


class HudiConfig(BaseModel):
    """Apache Hudi table format configuration."""

    version: str = "0.14.0"
    properties: dict[str, Any] = Field(default_factory=dict)


class TableFormatConfig(BaseModel):
    """Table format configuration."""

    type: TableFormatType = TableFormatType.ICEBERG
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)
    hudi: HudiConfig = Field(default_factory=HudiConfig)


class TrinoCoordinatorConfig(BaseModel):
    """Trino coordinator resource configuration."""

    cpu: str = "2"
    memory: str = "8Gi"


class TrinoWorkerConfig(BaseModel):
    """Trino worker configuration."""

    replicas: int = 2
    cpu: str = "4"
    memory: str = "16Gi"
    spill_enabled: bool = True
    spill_max_per_node: str = "40Gi"
    storage: str = "50Gi"
    storage_class: str = ""


class TrinoConfig(BaseModel):
    """Trino query engine configuration."""

    coordinator: TrinoCoordinatorConfig = Field(default_factory=TrinoCoordinatorConfig)
    worker: TrinoWorkerConfig = Field(default_factory=TrinoWorkerConfig)
    catalog_name: str = "lakehouse"


class SparkThriftConfig(BaseModel):
    """Spark Thrift Server configuration."""

    cores: int = 2
    memory: str = "4g"
    catalog_name: str = "lakehouse"


class DuckDBConfig(BaseModel):
    """DuckDB query engine configuration."""

    cores: int = 2
    memory: str = "4g"
    catalog_name: str = "lakehouse"


class QueryEngineConfig(BaseModel):
    """Query engine configuration."""

    type: QueryEngineType = QueryEngineType.TRINO
    trino: TrinoConfig = Field(default_factory=TrinoConfig)
    spark_thrift: SparkThriftConfig = Field(default_factory=SparkThriftConfig)
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)


class BronzeLayerConfig(BaseModel):
    """Bronze layer configuration."""

    format: str = "parquet"
    path_template: str = "customer/interactions"


class SilverStrategyConfig(BaseModel):
    """Silver layer adaptive strategy configuration."""

    simple_threshold: str = "100gb"
    streaming_threshold: str = "5tb"
    enable_salting: bool = True


class SilverLayerConfig(BaseModel):
    """Silver layer configuration."""

    format: str = "iceberg"
    table_name: str = "customer_interactions_enriched"
    partition_by: list[str] = Field(default_factory=lambda: ["date"])
    transforms: list[str] = Field(
        default_factory=lambda: [
            "normalize_email",
            "normalize_phone",
            "geo_enrichment",
            "customer_segmentation",
            "quality_flags",
        ]
    )
    strategy: SilverStrategyConfig = Field(default_factory=SilverStrategyConfig)


class GoldTableConfig(BaseModel):
    """Gold layer table configuration."""

    name: str
    partition_by: list[str] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)


class GoldLayerConfig(BaseModel):
    """Gold layer configuration."""

    format: str = "iceberg"
    tables: list[GoldTableConfig] = Field(
        default_factory=lambda: [
            GoldTableConfig(
                name="customer_executive_dashboard",
                partition_by=["date"],
                aggregations=[
                    "daily_revenue",
                    "daily_engagement",
                    "churn_indicators",
                    "channel_performance",
                ],
            ),
        ]
    )


class MedallionConfig(BaseModel):
    """Medallion processing pattern configuration."""

    bronze: BronzeLayerConfig = Field(default_factory=BronzeLayerConfig)
    silver: SilverLayerConfig = Field(default_factory=SilverLayerConfig)
    gold: GoldLayerConfig = Field(default_factory=GoldLayerConfig)


class ContinuousConfig(BaseModel):
    """Continuous processing pattern configuration.

    Controls trigger intervals for streaming jobs, run duration,
    checkpoint path prefix in S3, and throughput tuning knobs.
    """

    bronze_trigger_interval: str = "30 seconds"
    silver_trigger_interval: str = "60 seconds"
    gold_refresh_interval: str = "5 minutes"
    run_duration: int = Field(
        default=1800,
        ge=60,
        description="Streaming run duration in seconds (default 30 min)",
    )
    checkpoint_base: str = "checkpoints"

    # Throughput tuning -- these control how much data the streaming
    # pipeline can process per trigger interval.
    max_files_per_trigger: int = Field(
        default=50,
        ge=1,
        description=(
            "Max Parquet files bronze-ingest reads per micro-batch. "
            "This is the primary throughput cap for the streaming pipeline. "
            "At 50 files * ~122K rows/file, each batch processes ~6.1M rows."
        ),
    )
    bronze_target_file_size_mb: int = Field(
        default=512,
        ge=32,
        description="Target Iceberg file size for bronze writes (MB)",
    )
    silver_target_file_size_mb: int = Field(
        default=512,
        ge=32,
        description="Target Iceberg file size for silver writes (MB)",
    )
    gold_target_file_size_mb: int = Field(
        default=128,
        ge=32,
        description="Target Iceberg file size for gold writes (MB)",
    )

    # In-stream benchmark settings -- controls periodic benchmark
    # rounds that run while streaming jobs are active.  Both warmup
    # and interval are hard-floored to gold_refresh_interval so that
    # every round lands in a clean window after gold has refreshed.
    benchmark_interval: int = Field(
        default=300,
        ge=300,
        le=3600,
        description="Seconds between in-stream benchmark rounds",
    )
    benchmark_warmup: int = Field(
        default=300,
        ge=300,
        le=1800,
        description="Seconds before first in-stream benchmark round",
    )

    @model_validator(mode="after")
    def _benchmark_ge_gold_refresh(self) -> ContinuousConfig:
        """Clamp benchmark_warmup and benchmark_interval to gold_refresh_interval.

        Gold rewrites the entire table each refresh cycle via
        createOrReplace().  Benchmark rounds that fire before the first
        refresh produce inflated QpH against an empty/stale gold table,
        and intervals shorter than the gold cycle cause Q9 contention
        as rounds overlap with gold rewrites.

        Both fields are clamped up to gold_refresh_interval (default
        300s).  Users who want more rounds must run the pipeline longer.
        """
        parts = self.gold_refresh_interval.strip().lower().split()
        gold_s = 300  # fallback
        if len(parts) == 2:
            try:
                val = int(parts[0])
                unit = parts[1].rstrip("s")
                if unit == "second":
                    gold_s = val
                elif unit == "minute":
                    gold_s = val * 60
                elif unit == "hour":
                    gold_s = val * 3600
            except ValueError:
                pass
        if self.benchmark_warmup < gold_s:
            self.benchmark_warmup = gold_s
        if self.benchmark_interval < gold_s:
            self.benchmark_interval = gold_s
        return self


class ProcessingConfig(BaseModel):
    """Processing pattern configuration."""

    pattern: ProcessingPattern = ProcessingPattern.MEDALLION
    mode: PipelineMode = PipelineMode.BATCH
    medallion: MedallionConfig = Field(default_factory=MedallionConfig)
    continuous: ContinuousConfig = Field(default_factory=ContinuousConfig)


class DatagenCheckpointConfig(BaseModel):
    """Data generation checkpoint configuration."""

    enabled: bool = True
    path: str = ".lakebench_checkpoint.json"


class DatagenConfig(BaseModel):
    """Data generation configuration.

    Uses an abstract scale factor instead of explicit data sizes.
    One scale unit generates approximately 10 GB of on-disk bronze data.
    Each schema maps scale to its own domain dimensions (customers,
    sensors, accounts, etc).

    Example::

        datagen:
          scale: 10   # ~100 GB bronze, 1M customers for Customer360
    """

    scale: int = Field(
        default=10,
        ge=1,
        le=10000,
        description=(
            "Abstract scale factor. 1 unit ~ 10 GB on-disk bronze. "
            "Scale 10 = ~100 GB, Scale 100 = ~1 TB."
        ),
    )

    # Deprecated: kept for backward compatibility
    target_size: str | None = Field(
        default=None,
        description="DEPRECATED: Use 'scale' instead. Will be removed in a future version.",
    )

    mode: DatagenMode = DatagenMode.AUTO
    parallelism: int = 4
    file_size: str = "512mb"
    dirty_data_ratio: float = 0.08
    cpu: str = "2"
    memory: str = "4Gi"
    generators: int = 0  # per-pod generator processes (0 = auto: 1 for batch, 8 for continuous)
    uploaders: int = 0  # per-pod uploader threads   (0 = auto: 1 for batch, 2 for continuous)
    checkpoint: DatagenCheckpointConfig = Field(default_factory=DatagenCheckpointConfig)
    timestamp_start: str | None = Field(
        default=None,
        description="Start date for generated timestamps (ISO format, e.g. '2024-01-01'). Default: datagen built-in (2024-01-01).",
    )
    timestamp_end: str | None = Field(
        default=None,
        description="End date for generated timestamps (ISO format, e.g. '2025-12-31'). Default: datagen built-in (2025-12-31).",
    )

    @field_validator("dirty_data_ratio")
    @classmethod
    def validate_dirty_data_ratio(cls, v: float) -> float:
        """Ensure dirty data ratio is between 0 and 1."""
        if not 0 <= v <= 1:
            raise ValueError("dirty_data_ratio must be between 0 and 1")
        return v

    @model_validator(mode="after")
    def resolve_scale_from_target_size(self) -> DatagenConfig:
        """If legacy target_size is set, derive scale from it."""
        if self.target_size is not None:
            import warnings

            warnings.warn(
                "datagen.target_size is deprecated. Use datagen.scale instead. "
                "Example: scale: 10 (for ~100 GB)",
                DeprecationWarning,
                stacklevel=2,
            )
            bytes_val = parse_size_to_bytes(self.target_size)
            # 1 scale unit ~ 10 GB = 10 * 1024^3 bytes
            derived_scale = max(1, round(bytes_val / (10 * 1024**3)))
            object.__setattr__(self, "scale", derived_scale)
        return self

    def get_effective_scale(self) -> int:
        """Get the effective scale value."""
        return self.scale


class QualityDistributionConfig(BaseModel):
    """Data quality distribution configuration."""

    clean: float = 0.92
    duplicate_suspected: float = 0.02
    incomplete: float = 0.03
    format_inconsistent: float = 0.03

    @model_validator(mode="after")
    def validate_sum(self) -> QualityDistributionConfig:
        """Ensure distribution sums to 1.0."""
        total = self.clean + self.duplicate_suspected + self.incomplete + self.format_inconsistent
        if abs(total - 1.0) > 0.001:
            raise ValueError(f"Quality distribution must sum to 1.0, got {total}")
        return self


class Customer360Config(BaseModel):
    """Customer360 workload schema configuration.

    Domain dimensions (customers, date_range) are derived from
    ``datagen.scale``.  These fields allow manual overrides for
    advanced use cases.
    """

    unique_customers: int | None = Field(
        default=None,
        description="Override: unique customer count. If None, derived from scale.",
    )
    date_range_days: int | None = Field(
        default=None,
        description="Override: date range in days. If None, defaults to 365.",
    )
    channels: list[str] = Field(
        default_factory=lambda: ["web", "mobile", "store", "call_center", "social_media"]
    )
    event_types: list[str] = Field(
        default_factory=lambda: ["purchase", "browse", "support", "login", "abandoned_cart"]
    )
    quality_distribution: QualityDistributionConfig = Field(
        default_factory=QualityDistributionConfig
    )


class WorkloadConfig(BaseModel):
    """Workload/data generation configuration."""

    schema_type: WorkloadSchema = Field(default=WorkloadSchema.CUSTOMER360, alias="schema")
    datagen: DatagenConfig = Field(default_factory=DatagenConfig)
    customer360: Customer360Config = Field(default_factory=Customer360Config)

    model_config = {"populate_by_name": True}


# Supported component combinations (catalog, table_format, query_engine).
# Unsupported combinations fail validation with a clear error.
_SUPPORTED_COMBINATIONS = [
    # (catalog, table_format, pipeline_engine, query_engine)
    ("hive", "iceberg", "spark", "trino"),
    ("hive", "iceberg", "spark", "spark-thrift"),
    ("hive", "iceberg", "spark", "duckdb"),
    ("hive", "iceberg", "spark", "none"),
    # Polaris REST catalog (Iceberg only)
    ("polaris", "iceberg", "spark", "trino"),
    ("polaris", "iceberg", "spark", "spark-thrift"),
    ("polaris", "iceberg", "spark", "duckdb"),
    ("polaris", "iceberg", "spark", "none"),
]


class TableNamesConfig(BaseModel):
    """Fully-qualified Iceberg table names (namespace.table).

    Defaults match the Customer 360 pipeline.  Override these when using a
    different workload schema or custom table naming conventions.  The
    ``{catalog}`` prefix is added at runtime from the catalog configuration.

    Examples::

        # Default (Customer 360)
        bronze: "default.bronze_raw"
        silver: "silver.customer_interactions_enriched"
        gold:   "gold.customer_executive_dashboard"

        # IoT workload
        bronze: "default.sensor_raw"
        silver: "silver.sensor_readings_cleaned"
        gold:   "gold.device_health_dashboard"
    """

    bronze: str = Field(
        default="default.bronze_raw",
        description="Bronze table: namespace.table (e.g. default.bronze_raw)",
    )
    silver: str = Field(
        default="silver.customer_interactions_enriched",
        description="Silver table: namespace.table",
    )
    gold: str = Field(
        default="gold.customer_executive_dashboard",
        description="Gold table: namespace.table",
    )


class BenchmarkConfig(BaseModel):
    """Benchmark configuration.

    Controls how the Trino query benchmark is executed.
    Defaults produce today's behavior (power run, single stream, hot cache).
    """

    mode: BenchmarkMode = BenchmarkMode.POWER
    streams: int = Field(
        default=4,
        ge=1,
        le=64,
        description="Number of concurrent query streams for throughput mode",
    )
    cache: str = Field(
        default="hot",
        pattern=r"^(hot|cold)$",
        description="Cache mode: 'hot' or 'cold'",
    )
    iterations: int = Field(
        default=1,
        ge=1,
        le=100,
        description="Iterations per query (>1 uses median, extended mode)",
    )


class ArchitectureConfig(BaseModel):
    """Layer 2: Data architecture configuration."""

    catalog: CatalogConfig = Field(default_factory=CatalogConfig)
    table_format: TableFormatConfig = Field(default_factory=TableFormatConfig)
    pipeline_engine: PipelineEngineType = PipelineEngineType.SPARK
    query_engine: QueryEngineConfig = Field(default_factory=QueryEngineConfig)
    pipeline: ProcessingConfig = Field(default_factory=ProcessingConfig)
    workload: WorkloadConfig = Field(default_factory=WorkloadConfig)
    benchmark: BenchmarkConfig = Field(default_factory=BenchmarkConfig)
    tables: TableNamesConfig = Field(default_factory=TableNamesConfig)

    @model_validator(mode="before")
    @classmethod
    def migrate_processing_to_pipeline(cls, data: object) -> object:
        """Accept deprecated ``processing`` key as alias for ``pipeline``."""
        if not isinstance(data, dict):
            return data
        if "processing" in data:
            import warnings

            warnings.warn(
                "'processing' is deprecated, use 'pipeline' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if "pipeline" not in data:
                data["pipeline"] = data.pop("processing")
            else:
                data.pop("processing")  # pipeline takes precedence
        return data

    @model_validator(mode="after")
    def validate_component_combination(self) -> ArchitectureConfig:
        """Validate that the selected component combination is supported."""
        combo = (
            self.catalog.type.value,
            self.table_format.type.value,
            self.pipeline_engine.value,
            self.query_engine.type.value,
        )
        if combo not in _SUPPORTED_COMBINATIONS:
            supported = "\n".join(
                f"  - catalog={c}, table_format={t}, engine={e}, query_engine={q}"
                for c, t, e, q in _SUPPORTED_COMBINATIONS
            )
            raise ValueError(
                f"Unsupported component combination: catalog={combo[0]}, "
                f"table_format={combo[1]}, engine={combo[2]}, "
                f"query_engine={combo[3]}.\n"
                f"Supported combinations:\n{supported}"
            )
        return self


# =============================================================================
# Observability Configuration (Layer 3)
# =============================================================================


class ReportIncludeConfig(BaseModel):
    """Report content configuration."""

    summary: bool = True
    stage_breakdown: bool = True
    storage_metrics: bool = True
    resource_utilization: bool = True
    recommendations: bool = True
    platform_metrics: bool = True


class ReportsConfig(BaseModel):
    """Reports configuration.

    Reports are written into per-run directories under the metrics output_dir.
    The output_dir field is kept for backward compatibility but is no longer
    the primary output location.
    """

    enabled: bool = True
    output_dir: str = "./lakebench-output/runs"
    format: ReportFormat = ReportFormat.HTML
    include: ReportIncludeConfig = Field(default_factory=ReportIncludeConfig)


class ObservabilityConfig(BaseModel):
    """Layer 3: Observability configuration.

    Flat model -- use top-level keys (enabled, prometheus_stack_enabled, etc.).
    Deeply nested YAML (metrics.prometheus.enabled) is rejected to prevent
    silent data loss (see BUG-029).
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    prometheus_stack_enabled: bool = True
    s3_metrics_enabled: bool = True
    spark_metrics_enabled: bool = True
    dashboards_enabled: bool = True
    retention: str = "7d"
    storage: str = "10Gi"
    storage_class: str = ""
    reports: ReportsConfig = Field(default_factory=ReportsConfig)


# =============================================================================
# Spark Configuration Overrides
# =============================================================================


class SparkConfOverrides(BaseModel):
    """Spark configuration overrides.

    These are proven defaults that can be customized.
    """

    conf: dict[str, str] = Field(
        default_factory=lambda: {
            # S3A settings (proven defaults for FlashBlade)
            "spark.hadoop.fs.s3a.connection.maximum": "500",
            "spark.hadoop.fs.s3a.threads.max": "200",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.multipart.size": "268435456",
            "spark.hadoop.fs.s3a.fast.upload.active.blocks": "16",
            "spark.hadoop.fs.s3a.attempts.maximum": "20",
            "spark.hadoop.fs.s3a.retry.limit": "10",
            "spark.hadoop.fs.s3a.retry.interval": "500ms",
            # Shuffle settings
            "spark.sql.shuffle.partitions": "200",
            "spark.default.parallelism": "200",
            # Memory settings
            "spark.memory.fraction": "0.8",
            "spark.memory.storageFraction": "0.3",
        }
    )


# =============================================================================
# Root Configuration
# =============================================================================


class LakebenchConfig(BaseModel):
    """Root configuration for Lakebench.

    This is the master configuration that matches Section 4 of the spec.
    All values shown are defaults unless marked REQUIRED.
    """

    # Metadata
    name: str = Field(
        default="",
        description="Unique name for this deployment (REQUIRED)",
    )
    description: str = ""
    version: int = 1  # Config schema version
    recipe: str | None = None

    # Container images
    images: ImagesConfig = Field(default_factory=ImagesConfig)

    # Layer 1: Platform
    platform: PlatformConfig = Field(default_factory=PlatformConfig)

    # Layer 2: Data Architecture
    architecture: ArchitectureConfig = Field(default_factory=ArchitectureConfig)

    # Layer 3: Observability
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)

    # Spark configuration overrides
    spark: SparkConfOverrides = Field(default_factory=SparkConfOverrides)

    @model_validator(mode="before")
    @classmethod
    def apply_recipe_defaults(cls, data: object) -> object:
        """Expand recipe defaults into the config dict.

        Recipe defaults are merged via ``_deep_setdefault`` so user-specified
        values always take precedence.
        """
        if not isinstance(data, dict):
            return data
        recipe_name = data.get("recipe")
        if recipe_name:
            from lakebench.config.recipes import RECIPES, _deep_setdefault

            defaults = RECIPES.get(recipe_name)
            if not defaults:
                valid = ", ".join(sorted(RECIPES.keys()))
                raise ValueError(f"Unknown recipe: {recipe_name}. Valid recipes: {valid}")
            _deep_setdefault(data, defaults)
        return data

    @model_validator(mode="after")
    def validate_required_fields(self) -> LakebenchConfig:
        """Validate required fields are present."""
        if not self.name:
            raise ValueError("'name' is required")
        return self

    def get_s3_endpoint_url(self) -> str:
        """Get the full S3 endpoint URL."""
        return self.platform.storage.s3.endpoint

    def get_namespace(self) -> str:
        """Get the Kubernetes namespace, defaulting to deployment name."""
        return self.platform.kubernetes.namespace or self.name

    def has_inline_s3_credentials(self) -> bool:
        """Check if inline S3 credentials are provided."""
        s3 = self.platform.storage.s3
        return bool(s3.access_key and s3.secret_key)

    def has_s3_secret_ref(self) -> bool:
        """Check if S3 credentials reference an existing secret."""
        return bool(self.platform.storage.s3.secret_ref)

    def get_scale_dimensions(self):
        """Get the resolved scale dimensions for the current workload.

        Returns:
            ScaleDimensions with customers, rows, approx size, etc.
        """
        from lakebench.config.scale import ScaleDimensions, get_dimensions

        workload = self.architecture.workload
        scale = workload.datagen.get_effective_scale()
        dims = get_dimensions(workload.schema_type.value, scale)

        # Apply overrides from Customer360Config if present
        c360 = workload.customer360
        if c360.unique_customers is not None or c360.date_range_days is not None:
            customers = c360.unique_customers or dims.customers
            date_range = c360.date_range_days or dims.date_range_days
            dims = ScaleDimensions(
                scale=dims.scale,
                customers=customers,
                events_per_customer=dims.events_per_customer,
                date_range_days=date_range,
                approx_rows=customers * dims.events_per_customer,
                approx_bronze_gb=dims.approx_bronze_gb,
            )

        return dims

    def get_compute_guidance(self):
        """Get compute guidance for the current scale.

        Returns:
            ComputeGuidance with tier name, minimum and recommended resources
        """
        from lakebench.config.scale import compute_guidance

        return compute_guidance(self.architecture.workload.datagen.get_effective_scale())


# =============================================================================
# Config Validation Helpers
# =============================================================================


def parse_size_to_bytes(size_str: str) -> int:
    """Parse a human-readable size string to bytes.

    Supports: b, kb, mb, gb, tb (case-insensitive)

    Examples:
        >>> parse_size_to_bytes("100gb")
        107374182400
        >>> parse_size_to_bytes("512mb")
        536870912
    """
    size_str = size_str.lower().strip()

    units = [
        ("tb", 1024**4),
        ("gb", 1024**3),
        ("mb", 1024**2),
        ("kb", 1024),
        ("b", 1),
    ]

    for unit, multiplier in units:
        if size_str.endswith(unit):
            try:
                value = float(size_str[: -len(unit)])
                return int(value * multiplier)
            except ValueError:
                raise ValueError(f"Invalid size format: {size_str}")  # noqa: B904

    # No unit, assume bytes
    try:
        return int(size_str)
    except ValueError:
        raise ValueError(f"Invalid size format: {size_str}")  # noqa: B904


def parse_spark_memory(memory_str: str) -> int:
    """Parse Spark memory string to bytes.

    Supports: g, m, k (case-insensitive)

    Examples:
        >>> parse_spark_memory("48g")
        51539607552
        >>> parse_spark_memory("4096m")
        4294967296
    """
    memory_str = memory_str.lower().strip()

    units = {
        "k": 1024,
        "m": 1024**2,
        "g": 1024**3,
        "t": 1024**4,
    }

    for unit, multiplier in units.items():
        if memory_str.endswith(unit):
            try:
                value = float(memory_str[:-1])
                return int(value * multiplier)
            except ValueError:
                raise ValueError(f"Invalid memory format: {memory_str}")  # noqa: B904

    # No unit, assume bytes
    try:
        return int(memory_str)
    except ValueError:
        raise ValueError(f"Invalid memory format: {memory_str}")  # noqa: B904
