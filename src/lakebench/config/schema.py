"""Pydantic models for Lakebench configuration.

This module defines the complete configuration schema for Lakebench,
matching the specification in lakebench-spec.md Section 4.
"""

from __future__ import annotations

import logging
import warnings
from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

logger = logging.getLogger(__name__)


class ConfigModel(BaseModel):
    """Base for every user-facing config model: unknown keys are errors.

    A misspelt or misplaced key used to be dropped silently, so the run went
    ahead on the default and the user never learned their setting was
    ignored. Deprecated spellings stay accepted because each is migrated by a
    ``mode="before"`` validator, which runs before the extra-key check.
    """

    model_config = ConfigDict(extra="forbid")

    # Keys that were once valid and now do nothing. They are dropped with a
    # DeprecationWarning instead of rejected, so existing configs keep
    # loading. Maps key -> what to do instead.
    _removed_keys: ClassVar[dict[str, str]] = {}

    @model_validator(mode="before")
    @classmethod
    def _drop_removed_keys(cls, data: object) -> object:
        if not isinstance(data, dict) or not cls._removed_keys:
            return data
        present = [k for k in cls._removed_keys if k in data]
        if not present:
            return data
        data = dict(data)
        for key in present:
            data.pop(key)
            msg = f"'{key}' ({cls.__name__}) is no longer used and is ignored. {cls._removed_keys[key]}"
            logger.warning(msg)
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
        return data


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
    DELTA = "delta"


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
    - sustained: concurrent streaming jobs with periodic gold recomputation
      (bronze-ingest + silver-stream + gold-refresh)
    """

    BATCH = "batch"
    SUSTAINED = "sustained"


class ReportFormat(str, Enum):
    """Supported report output formats."""

    HTML = "html"
    JSON = "json"
    BOTH = "both"


# =============================================================================
# Images Configuration
# =============================================================================


class ImagesConfig(ConfigModel):
    """Container image configuration for all Lakebench components."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "pull_secrets": "No deployer ever applied it; removed in v1.5.",
    }

    # Immutable tag = the datagen_rs commit it was built from. Bump it with
    # every datagen_rs change; :latest drifted from the code it claimed to be.
    datagen: str = "docker.io/sillidata/lb-datagen:14c4eee"
    spark: str = "apache/spark:4.0.2-python3"
    postgres: str = "postgres:17"  # Tested with 16, 17, 18
    hive: str = "apache/hive:3.1.3"
    polaris: str = "apache/polaris:1.6.0"
    polaris_admin_tool: str = "apache/polaris-admin-tool:1.6.0"
    unity: str = (
        "unitycatalog/unitycatalog:main"  # OSS Unity has no version tags; :main tracks 0.4.x
    )
    trino: str = "trinodb/trino:483"
    duckdb: str = "python:3.11-slim"
    prometheus: str = "prom/prometheus:v2.48.0"
    grafana: str = "grafana/grafana:10.2.0"
    jmx_exporter: str = "bitnami/jmx-exporter:latest"

    pull_policy: ImagePullPolicy = ImagePullPolicy.ALWAYS

    @field_validator("spark")
    @classmethod
    def _validate_spark_image(cls, v: str) -> str:
        """Validate that the Spark image tag contains a supported version."""
        from lakebench.spark.job import _parse_spark_major

        _parse_spark_major(v)  # raises ValueError for unsupported versions
        return v


# =============================================================================
# Platform Layer Configuration (Layer 1)
# =============================================================================


class KubernetesConfig(ConfigModel):
    """Kubernetes connection and namespace configuration."""

    context: str = ""  # Empty = use current context
    namespace: str = ""  # Empty = use default namespace
    create_namespace: bool = True


class S3BucketsConfig(ConfigModel):
    """S3 bucket names for each data layer."""

    bronze: str = "lakebench-bronze"
    silver: str = "lakebench-silver"
    gold: str = "lakebench-gold"


class S3Config(ConfigModel):
    """S3/object storage configuration."""

    endpoint: str = Field(
        default="",
        description="S3 endpoint URL (e.g., http://your-s3:80 or https://your-s3:443)",
    )
    region: str = "us-east-1"
    path_style: bool = True  # Required for FlashBlade, MinIO

    # Credentials - either inline or reference to existing secret
    access_key: str = ""
    secret_key: str = ""
    secret_ref: str = ""  # Name of existing K8s secret

    # TLS / HTTPS support
    ca_cert: str = Field(
        default="",
        description="Path to PEM CA certificate bundle for HTTPS S3 endpoints. "
        "Empty = system default CAs.",
    )
    verify_ssl: bool = Field(
        default=True,
        description="Verify SSL certificates for HTTPS endpoints. "
        "Set false only for self-signed certs in dev.",
    )

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


class ScratchStorageConfig(ConfigModel):
    """Scratch storage configuration for Spark shuffle.

    The StorageClass is Category 2 shared infrastructure: lakebench uses
    it, lakebench does not create or destroy it. ``deploy`` verifies the
    named StorageClass exists at preflight; a cluster admin installs it
    once with ``lakebench admin install-scratch-storage-class``. The
    ``provisioner`` and ``parameters`` fields are consumed by that admin
    command via the ``storageclass/px-csi-scratch.yaml.j2`` template.
    """

    _removed_keys: ClassVar[dict[str, str]] = {
        "create_storage_class": (
            "lakebench no longer creates StorageClasses; a cluster admin runs "
            "'lakebench admin install-scratch-storage-class' once."
        ),
    }

    enabled: bool = False
    storage_class: str = "px-csi-scratch"
    size: str = "100Gi"
    provisioner: str = "pxd.portworx.com"
    parameters: dict[str, str] = Field(
        default_factory=lambda: {"repl": "1", "io_profile": "auto", "priority_io": "high"}
    )


class StorageConfig(ConfigModel):
    """Storage configuration including S3 and scratch volumes."""

    s3: S3Config = Field(default_factory=S3Config)
    scratch: ScratchStorageConfig = Field(default_factory=ScratchStorageConfig)


class SparkDriverConfig(ConfigModel):
    """Spark driver resource configuration."""

    cores: int = 4
    memory: str = "8g"


class SparkExecutorConfig(ConfigModel):
    """Spark executor resource configuration."""

    instances: int = 8
    cores: int = 4
    memory: str = "48g"
    memory_overhead: str = "12g"


class SparkOperatorConfig(ConfigModel):
    """Spark operator installation configuration."""

    install: bool = False
    namespace: str = "spark-operator"
    version: str = "2.5.1"  # webhook volume injection gap (gotcha 3) unchanged from 2.4.0; template workaround stays


class SparkComputeConfig(ConfigModel):
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


class PostgresConfig(ConfigModel):
    """PostgreSQL configuration for metadata backend."""

    storage: str = "10Gi"
    storage_class: str = ""  # Empty = default storage class


class ComputeConfig(ConfigModel):
    """Compute resource configuration."""

    spark: SparkComputeConfig = Field(default_factory=SparkComputeConfig)
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)


class PlatformConfig(ConfigModel):
    """Layer 1: Platform configuration."""

    kubernetes: KubernetesConfig = Field(default_factory=KubernetesConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    compute: ComputeConfig = Field(default_factory=ComputeConfig)


# =============================================================================
# Data Architecture Configuration (Layer 2)
# =============================================================================


class HiveThriftConfig(ConfigModel):
    """Hive Metastore thrift server configuration."""

    min_threads: int = 10
    max_threads: int = 50
    client_timeout: str = "300s"


class HiveResourcesConfig(ConfigModel):
    """Hive Metastore resource configuration."""

    cpu_min: str = "500m"
    cpu_max: str = "2"
    memory: str = "4Gi"


class StackableOperatorConfig(ConfigModel):
    """Stackable operator installation configuration."""

    install: bool = False
    namespace: str = "stackable"
    version: str = "25.7.0"


class HiveConfig(ConfigModel):
    """Hive Metastore configuration."""

    operator: StackableOperatorConfig = Field(default_factory=StackableOperatorConfig)
    thrift: HiveThriftConfig = Field(default_factory=HiveThriftConfig)
    resources: HiveResourcesConfig = Field(default_factory=HiveResourcesConfig)


class PolarisResourcesConfig(ConfigModel):
    """Polaris resource configuration."""

    cpu: str = "1"
    memory: str = "2Gi"


class PolarisConfig(ConfigModel):
    """Apache Polaris REST catalog configuration.

    Polaris is an open-source Iceberg REST catalog (port 8181).
    Uses relational-jdbc persistence backed by the shared PostgreSQL.
    On FlashBlade: stsUnavailable=true, pathStyleAccess=true.

    ``client_secret`` MUST be supplied by the user when catalog type is
    polaris. Auto-generation across CLI calls does not work: `deploy`,
    `run`, and `destroy` each load the config independently, so an
    auto-generated secret would differ between invocations and Spark
    jobs submitted by `run` would fail OAuth2 against the Polaris
    instance bootstrapped by `deploy`. A hardcoded default (the
    pre-LB-090 behaviour) would share one OAuth2 client secret across
    every install. ``CatalogConfig`` validates and rejects the empty
    value with a message that includes a generator command.
    """

    version: str = "1.6.0"
    port: int = 8181
    client_secret: str = ""
    resources: PolarisResourcesConfig = Field(default_factory=PolarisResourcesConfig)


class UnityConfig(ConfigModel):
    """Unity Catalog configuration.

    OSS Unity Catalog is a self-hosted REST catalog server (Apache-licensed).
    Uses PostgreSQL for persistence, similar to Polaris.
    """

    version: str = "0.4.0"
    spark_connector_version: str = "0.4.0"
    port: int = 8080
    resources: PolarisResourcesConfig = Field(default_factory=PolarisResourcesConfig)


class CatalogConfig(ConfigModel):
    """Catalog service configuration."""

    type: CatalogType = CatalogType.HIVE
    hive: HiveConfig = Field(default_factory=HiveConfig)
    polaris: PolarisConfig = Field(default_factory=PolarisConfig)
    unity: UnityConfig = Field(default_factory=UnityConfig)


class PolarisClientSecretMissing(ValueError):
    """Raised at deploy/run time when a Polaris config has no client secret.

    Kept as a distinct exception type so ``lakebench validate`` and
    ``lakebench info`` (which do not deploy) can load Polaris configs
    without a secret -- the check runs where the secret is actually used
    (deploy, spark-job submission), not at config load. See LB-090 for
    why load-time auto-generation is unsafe: independent CLI invocations
    would each generate a different value.
    """


def require_polaris_client_secret(cfg: Any) -> str:
    """Return the Polaris client secret, or raise if missing.

    ``cfg`` is the root ``LakebenchConfig``. Call this from every code
    path that actually needs the secret to talk to Polaris (bootstrap
    job template render, Spark job manifest build, Trino configmap
    render), not from validators.
    """
    secret = cfg.architecture.catalog.polaris.client_secret
    if not secret:
        raise PolarisClientSecretMissing(
            "architecture.catalog.polaris.client_secret is required "
            "when catalog.type is 'polaris'. Generate one with:\n"
            "  python3 -c 'import secrets; print(secrets.token_urlsafe(32))'\n"
            "and set it in the config file (or via the "
            "LAKEBENCH_POLARIS_CLIENT_SECRET environment variable if "
            "you use the ${VAR} substitution)."
        )
    return secret


class IcebergConfig(ConfigModel):
    """Apache Iceberg table format configuration."""

    # 1.11.0 is the first release compiled for Java 17 and the first to publish
    # a native Spark 4.1 runtime. Spark 3.5 images must use a java17 tag with
    # this version; validate_iceberg_java_runtime() refuses the combination
    # rather than letting it fail inside the driver.
    version: str = "1.11.0"
    file_format: FileFormatType = FileFormatType.PARQUET
    properties: dict[str, Any] = Field(default_factory=dict)


class DeltaConfig(ConfigModel):
    """Delta Lake table format configuration."""

    version: str = "auto"
    properties: dict[str, Any] = Field(default_factory=dict)


class TableFormatConfig(ConfigModel):
    """Table format configuration."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "hudi": "Hudi is not a supported table format; removed in v1.2.",
    }

    type: TableFormatType = TableFormatType.ICEBERG
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)
    delta: DeltaConfig = Field(default_factory=DeltaConfig)


class TrinoCoordinatorConfig(ConfigModel):
    """Trino coordinator resource configuration."""

    cpu: str = "2"
    memory: str = "8Gi"


class TrinoWorkerConfig(ConfigModel):
    """Trino worker configuration."""

    replicas: int = 2
    cpu: str = "4"
    memory: str = "16Gi"
    spill_enabled: bool = True
    spill_max_per_node: str = "40Gi"
    storage: str = "50Gi"
    storage_class: str = ""

    @model_validator(mode="after")
    def spill_fits_storage(self) -> TrinoWorkerConfig:
        """The spill cap must fit the worker's storage volume with headroom: a
        worker that fills it is evicted by the kubelet mid-query. The spill
        cap must be in Gi, the only unit the Trino config template converts;
        storage may be any Kubernetes quantity."""
        if not self.spill_enabled:
            return self
        if not str(self.spill_max_per_node).endswith("Gi"):
            raise ValueError(
                f"trino.worker.spill_max_per_node must be in Gi (got {self.spill_max_per_node!r})"
            )
        units = {
            "Ki": 2**-20,
            "Mi": 2**-10,
            "Gi": 1.0,
            "Ti": 2**10,
            "K": 1e3 / 2**30,
            "M": 1e6 / 2**30,
            "G": 1e9 / 2**30,
            "T": 1e12 / 2**30,
        }
        storage = str(self.storage)
        unit = next((u for u in sorted(units, key=len, reverse=True) if storage.endswith(u)), None)
        if unit is None:
            return self  # plain bytes or an unusual quantity: leave it to Kubernetes
        storage_gib = float(storage[: -len(unit)]) * units[unit]
        # The volume also holds /data/trino; keep 10% headroom.
        if float(self.spill_max_per_node[:-2]) > 0.9 * storage_gib:
            raise ValueError(
                f"trino.worker.spill_max_per_node ({self.spill_max_per_node}) leaves less "
                f"than 10% of trino.worker.storage ({self.storage}); a spilling worker "
                "would be evicted"
            )
        return self
        for field in ("spill_max_per_node", "storage"):
            if not str(getattr(self, field)).endswith("Gi"):
                raise ValueError(
                    f"trino.worker.{field} must be in Gi (got {getattr(self, field)!r})"
                )
        if float(self.spill_max_per_node[:-2]) > float(self.storage[:-2]):
            raise ValueError(
                f"trino.worker.spill_max_per_node ({self.spill_max_per_node}) exceeds "
                f"trino.worker.storage ({self.storage}); a spilling worker would be evicted"
            )
        return self


class TrinoConfig(ConfigModel):
    """Trino query engine configuration."""

    coordinator: TrinoCoordinatorConfig = Field(default_factory=TrinoCoordinatorConfig)
    worker: TrinoWorkerConfig = Field(default_factory=TrinoWorkerConfig)
    catalog_name: str = "lakehouse"


class SparkThriftConfig(ConfigModel):
    """Spark Thrift Server configuration."""

    cores: int = 2
    memory: str = "4g"
    catalog_name: str = "lakehouse"


class DuckDBConfig(ConfigModel):
    """DuckDB query engine configuration."""

    cores: int = 2
    memory: str = "4g"
    catalog_name: str = "lakehouse"
    # Pinned, not floating. Both install sites used a bare `pip install duckdb`,
    # so every deploy took whatever was current and two runs weeks apart could
    # compare different query engines while reporting the difference as a
    # result. Measured local run-to-run spread is 0.9%, well below what an
    # engine change would move, so the drift was invisible to the noise floor.
    version: str = "1.5.5"


class QueryEngineConfig(ConfigModel):
    """Query engine configuration."""

    type: QueryEngineType = QueryEngineType.TRINO
    trino: TrinoConfig = Field(default_factory=TrinoConfig)
    spark_thrift: SparkThriftConfig = Field(default_factory=SparkThriftConfig)
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)


class BronzeLayerConfig(ConfigModel):
    """Bronze layer configuration."""

    format: str = "parquet"
    path_template: str = "customer/interactions"


class SilverLayerConfig(ConfigModel):
    """Silver layer configuration."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "strategy": "The silver build never read it; removed in v1.5.",
    }

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


class GoldTableConfig(ConfigModel):
    """Gold layer table configuration."""

    name: str
    partition_by: list[str] = Field(default_factory=list)
    aggregations: list[str] = Field(default_factory=list)


class GoldLayerConfig(ConfigModel):
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


class MedallionConfig(ConfigModel):
    """Medallion processing pattern configuration."""

    bronze: BronzeLayerConfig = Field(default_factory=BronzeLayerConfig)
    silver: SilverLayerConfig = Field(default_factory=SilverLayerConfig)
    gold: GoldLayerConfig = Field(default_factory=GoldLayerConfig)


class SustainedConfig(ConfigModel):
    """Sustained pipeline configuration.

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

    # Iceberg retention -- periodic expire_snapshots + remove_orphan_files
    # via Trino to prevent unbounded snapshot/metadata growth during long
    # sustained runs.
    retention_interval: int = Field(
        default=1800,
        ge=300,
        le=7200,
        description="Seconds between Iceberg maintenance rounds (expire_snapshots + remove_orphan_files)",
    )
    retention_threshold: str = Field(
        default="30m",
        description=(
            "Iceberg snapshot retention threshold passed to Trino "
            "(e.g. '30m', '1h', '7d'). Snapshots older than this are expired."
        ),
    )

    # Iceberg compaction -- periodic rewrite_data_files / optimize to
    # merge small files produced by streaming micro-batches.  Heavier
    # than expire_snapshots, so it runs less frequently.
    compaction_enabled: bool = Field(
        default=True,
        description="Enable periodic Iceberg compaction (rewrite_data_files) during sustained runs",
    )
    compaction_interval: int = Field(
        default=0,
        ge=0,
        description=("Seconds between compaction rounds. 0 = auto (2x retention_interval)."),
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
    def _benchmark_ge_gold_refresh(self) -> SustainedConfig:
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
        # Resolve compaction_interval=0 to 2x retention_interval
        if self.compaction_interval == 0:
            self.compaction_interval = self.retention_interval * 2
        return self


class ProcessingConfig(ConfigModel):
    """Processing pattern configuration."""

    pattern: ProcessingPattern = ProcessingPattern.MEDALLION
    mode: PipelineMode = PipelineMode.BATCH
    cycles: int = Field(
        default=1,
        ge=1,
        le=50,
        description=(
            "Number of batch iterations. "
            "Cycle 1 = full overwrite (current behavior), "
            "cycles 2-N = incremental append/merge."
        ),
    )
    pre_benchmark_maintenance: bool = Field(
        default=True,
        description="Run Iceberg compaction + expire_snapshots before benchmark for clean QpH",
    )
    medallion: MedallionConfig = Field(default_factory=MedallionConfig)
    sustained: SustainedConfig = Field(default_factory=SustainedConfig)

    @model_validator(mode="before")
    @classmethod
    def _migrate_continuous_key(cls, data: object) -> object:
        """Accept deprecated ``continuous`` key as alias for ``sustained``."""
        if not isinstance(data, dict):
            return data
        if "continuous" in data:
            import warnings

            logger.warning("'pipeline.continuous' is deprecated, use 'pipeline.sustained' instead.")
            warnings.warn(
                "'pipeline.continuous' is deprecated, use 'pipeline.sustained' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if "sustained" in data:
                # Dropping one silently would lose settings without a trace.
                raise ValueError(
                    "both 'pipeline.continuous' (deprecated) and 'pipeline.sustained' are "
                    "set; move the settings under 'sustained' and remove 'continuous'"
                )
            data = dict(data)
            data["sustained"] = data.pop("continuous")
        return data

    @field_validator("mode", mode="before")
    @classmethod
    def _migrate_continuous_mode(cls, v: object) -> object:
        """Accept deprecated ``continuous`` value as alias for ``sustained``."""
        if v == "continuous":
            import warnings

            logger.warning("pipeline mode 'continuous' is deprecated, use 'sustained' instead.")
            warnings.warn(
                "pipeline mode 'continuous' is deprecated, use 'sustained' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            return "sustained"
        return v

    @model_validator(mode="after")
    def _validate_cycles(self) -> ProcessingConfig:
        """Ensure cycles > 1 is only used with batch mode."""
        if self.cycles > 1 and self.mode != PipelineMode.BATCH:
            raise ValueError(
                "cycles > 1 requires pipeline mode 'batch' (sustained mode has its own iteration model)"
            )
        return self


class DatagenCheckpointConfig(ConfigModel):
    """Data generation checkpoint configuration."""

    enabled: bool = True
    path: str = ".lakebench_checkpoint.json"


class DatagenConfig(ConfigModel):
    """Data generation configuration.

    Uses an abstract scale factor instead of explicit data sizes.
    One scale unit generates approximately 10 GB of on-disk bronze data.
    Each schema maps scale to its own domain dimensions (customers,
    sensors, accounts, etc).

    Example::

        datagen:
          scale: 10   # ~100 GB bronze, 1M customers for Customer360
    """

    scale: float = Field(
        default=10,
        ge=0.01,
        le=10000,
        description=(
            "Abstract scale factor. 1 unit ~ 10 GB on-disk bronze. "
            "Scale 10 = ~100 GB, Scale 100 = ~1 TB. "
            "Values below 1 are intended for local mode: 0.1 = ~1 GB."
        ),
    )

    # Deprecated: kept for backward compatibility
    target_size: str | None = Field(
        default=None,
        description="DEPRECATED: Use 'scale' instead. Will be removed in a future version.",
    )

    mode: DatagenMode = DatagenMode.AUTO
    # Top-level generator seed. Unset: the AML pre-registration's calibration
    # seed for the financial schema, 42 otherwise (config/datagen_seed.py). A
    # financial seed the pre-registration lists as spent is refused.
    seed: int | None = Field(default=None, ge=0, le=2**63 - 1)
    # AML corpus role (financial only). The evaluation and robustness seeds are
    # refused unless the run declares its role here: each is generated once,
    # as the registered gate run for that role. Set without a seed, the role's
    # registered seed is used.
    corpus_role: Literal["calibration", "evaluation", "robustness"] | None = None
    # Robustness corpus (financial only; AML-GOALS R3(b)): datagen shifts the
    # nuisance parameters by corpora.robustness_perturbation in the
    # pre-registration (median amount, persona sds, dormancy, each x1.2 in
    # natural units). Required with corpus_role: robustness, refused with a
    # calibration or evaluation role. Off: output is unchanged.
    robustness_perturbation: bool = False
    parallelism: int = Field(default=4, ge=1)
    # Datagen output file size. Per-thread generator memory scales with it
    # (about 4.8x for financial, 3.0x for c360, measured), so the old 512mb
    # default needed 12-26 GiB per 8-thread pod. At 64mb, measured single-pod
    # throughput at 8 threads was c360 982 MB/s, financial 221-309 MB/s.
    file_size: str = "64mb"
    dirty_data_ratio: float = 0.08
    cpu: str = "2"
    memory: str = "4Gi"
    generators: int = 0  # generator threads per pod (0 = auto: follow the pod CPU)
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

    def get_effective_scale(self) -> float:
        """Get the effective scale value."""
        return self.scale


class Customer360Config(ConfigModel):
    """Customer360 workload schema configuration.

    Domain dimensions (customers, date_range) are derived from
    ``datagen.scale``.  These fields allow manual overrides for
    advanced use cases.
    """

    _removed_keys: ClassVar[dict[str, str]] = {
        "channels": "The customer360 generator never read it; removed in v1.5.",
        "event_types": "The customer360 generator never read it; removed in v1.5.",
        "quality_distribution": "The customer360 generator never read it; removed in v1.5.",
    }

    unique_customers: int | None = Field(
        default=None,
        description="Override: unique customer count. If None, derived from scale.",
    )
    date_range_days: int | None = Field(
        default=None,
        description="Override: date range in days. If None, defaults to 365.",
    )


class TmOperationsConfig(ConfigModel):
    """Simulated TM operations on top of the AML alerts (GOALS P10 stages 6-8).

    Dispositions are simulated from the datagen ground truth: the L1 analyst
    decides an alert correctly with probability ``analyst_accuracy``, the L2
    investigator (and the QA reviewer) with ``investigator_accuracy``. Every
    operations metric the report publishes is conditional on these values.
    Read by tm_operations.py through the LB_TM_* env vars.
    """

    enabled: bool = Field(
        default=True,
        description=(
            "Run the TM operations layer. When it cannot run (no manifest, an error) the "
            "run reports it as not run; only violated workflow invariants fail a run"
        ),
    )
    seed: int = Field(default=20260924, description="Seed for every simulated decision")
    analyst_accuracy: float = Field(default=0.90, ge=0.5, le=1.0)
    investigator_accuracy: float = Field(default=0.95, ge=0.5, le=1.0)
    qa_sample_rate: float = Field(
        default=0.05, ge=0.0, le=1.0, description="Share of L1 decisions QA re-reviews"
    )
    alert_sla_days: int = Field(
        default=60, ge=1, le=365, description="Policy SLA from alert to final decision"
    )
    case_lookback_months: int = Field(
        default=12, ge=6, le=12, description="Activity a case pulls in before its opening"
    )
    late_filing_rate: float = Field(
        default=0.03, ge=0.0, le=1.0, description="Share of SARs filed after the deadline"
    )
    no_suspect_rate: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="Share of new cases with no suspect identified (60-day filing clock)",
    )
    max_alerts_per_customer: int = Field(
        default=50_000,
        ge=100,
        le=10_000_000,
        description=(
            "Alerts replayed per customer; a hub customer's alerts past this are "
            "dispositioned over_capacity and counted in the report"
        ),
    )
    continuous_interval_seconds: int = Field(
        default=1800,
        ge=60,
        le=86_400,
        description=(
            "Continuous mode: seconds between operations passes. One pass costs minutes "
            "at scale 10, so running it every gold-refresh tick would wreck freshness"
        ),
    )
    counterparty_scenarios: list[str] = Field(
        default_factory=lambda: [
            "W1_connected_components",
            "W3_round_tripping",
            "W4_risk_propagation",
            "W17_layering_chain",
        ],
        description=(
            "Scenarios declared to alert on counterparties as well as customers (graph "
            "overlays). An alert on a non-customer from any other scenario fails an invariant"
        ),
    )

    def env(self) -> dict[str, str]:
        return {
            "LB_TM_ENABLED": str(self.enabled).lower(),
            "LB_TM_NO_SUSPECT_RATE": str(self.no_suspect_rate),
            "LB_TM_MAX_ALERTS_PER_CUSTOMER": str(self.max_alerts_per_customer),
            "LB_TM_CONTINUOUS_INTERVAL_S": str(self.continuous_interval_seconds),
            "LB_TM_COUNTERPARTY_SCENARIOS": ",".join(self.counterparty_scenarios),
            "LB_TM_SEED": str(self.seed),
            "LB_TM_ANALYST_ACCURACY": str(self.analyst_accuracy),
            "LB_TM_INVESTIGATOR_ACCURACY": str(self.investigator_accuracy),
            "LB_TM_QA_SAMPLE_RATE": str(self.qa_sample_rate),
            "LB_TM_ALERT_SLA_DAYS": str(self.alert_sla_days),
            "LB_TM_CASE_LOOKBACK_MONTHS": str(self.case_lookback_months),
            "LB_TM_LATE_FILING_RATE": str(self.late_filing_rate),
        }


class WorkloadConfig(ConfigModel):
    """Workload/data generation configuration."""

    @model_validator(mode="before")
    @classmethod
    def _one_schema_spelling(cls, data: object) -> object:
        # 'schema' is the documented key and 'schema_type' the field name;
        # with both set pydantic reports the second as an unknown key.
        if isinstance(data, dict) and "schema" in data and "schema_type" in data:
            raise ValueError(
                "both 'workload.schema' and 'workload.schema_type' are set; set only 'schema'"
            )
        return data

    schema_type: WorkloadSchema = Field(default=WorkloadSchema.CUSTOMER360, alias="schema")
    datagen: DatagenConfig = Field(default_factory=DatagenConfig)
    customer360: Customer360Config = Field(default_factory=Customer360Config)

    # Snapshot-retention policy. Set retention_workload=True on Financial
    # recipes whose workload set includes historical replay (W8) or
    # time-travel reproduction (W10): the pre-benchmark maintenance step
    # then preserves snapshots covering retention_months + headroom, rather
    # than expiring everything with the default "0s" threshold. See
    # REQ-R-03/REQ-R-05 in the FinServ-Crime spec.
    retention_workload: bool = False
    retention_months: int = Field(default=60, ge=1, le=120)

    # W1 connected-components vertex cap for the Financial detection path.
    # Default sits above the scale-10 vertex count (1.1M entities) so W1 runs
    # out of the box at scale 10; raise it for larger scales that have the
    # executor budget. Whether W1 completes in acceptable wall-clock above
    # the cap is a measured question (LB-120), not a config guarantee, so the
    # ceiling stays generous rather than unbounded. Consumed by
    # gold_finalize_financial via LB_FINANCIAL_W1_MAX_VERTICES.
    w1_max_vertices: int = Field(default=8_000_000, ge=1, le=200_000_000)

    # Financial transaction-monitoring operations layer (GOALS P10).
    tm_operations: TmOperationsConfig = Field(default_factory=TmOperationsConfig)

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @model_validator(mode="after")
    def _seed_allowed(self) -> WorkloadConfig:
        # Refused at load, before anything is deployed: a spent AML seed would
        # regenerate a corpus that has already been looked at, and an
        # evaluation or robustness seed without its declared role would burn
        # it (AML-GOALS R3).
        from lakebench.config.datagen_seed import check_perturbation, resolve_seed

        resolve_seed(self.datagen.seed, self.schema_type.value, self.datagen.corpus_role)
        check_perturbation(
            self.schema_type.value, self.datagen.corpus_role, self.datagen.robustness_perturbation
        )
        return self


# Supported component combinations (catalog, table_format, query_engine).
# Unsupported combinations fail validation with a clear error.
_SUPPORTED_COMBINATIONS = [
    # (catalog, table_format, pipeline_engine, query_engine)
    # -- Hive + Iceberg (v1.1) --
    ("hive", "iceberg", "spark", "trino"),
    ("hive", "iceberg", "spark", "spark-thrift"),
    ("hive", "iceberg", "spark", "duckdb"),
    ("hive", "iceberg", "spark", "none"),
    # -- Polaris REST catalog + Iceberg (v1.1) --
    ("polaris", "iceberg", "spark", "trino"),
    ("polaris", "iceberg", "spark", "spark-thrift"),
    ("polaris", "iceberg", "spark", "duckdb"),
    ("polaris", "iceberg", "spark", "none"),
    # -- Hive + Delta Lake (v1.2) --
    ("hive", "delta", "spark", "trino"),
    ("hive", "delta", "spark", "spark-thrift"),
    ("hive", "delta", "spark", "none"),
    # Note: Unity + Delta excluded from v1.2. UCSingleCatalog 0.4.0 always
    # calls generateTemporaryTableCredentials (STS) even for CREATE TABLE
    # ... LOCATION (EXTERNAL tables). No workaround without upstream fix
    # or direct REST API table registration. Planned for v1.3.
    # Note: Polaris + Delta is excluded -- Polaris is Iceberg-native.
]


# Why a combination is unsupported, keyed by the pair that causes it.
#
# A rejection that only prints the valid list makes the user diff their request
# against it to work out what they did wrong, and teaches them nothing. Every
# entry here is a real limitation that cost someone a debugging session; the
# gotcha numbers refer to the list in CLAUDE.md.
#
# Keys are checked most-specific first: a full 4-tuple, then (table_format,
# query_engine), then (catalog, table_format).
_COMBINATION_NOTES: dict[tuple[str, ...], str] = {
    ("delta", "duckdb"): (
        "DuckDB cannot read Delta on non-AWS S3. Its delta extension uses "
        "delta-kernel-rs, which ignores DuckDB's httpfs S3 settings and tries "
        "AWS IMDS (169.254.169.254) for credentials -- that hangs indefinitely "
        "against a non-AWS endpoint. There is no way to pass a custom endpoint "
        "to the delta kernel. The iceberg extension has no such limitation. "
        "(gotcha 18)"
    ),
    ("polaris", "delta"): (
        "Polaris is an Iceberg-native REST catalog and has no Delta Lake "
        "support. Use Hive as the catalog for Delta tables."
    ),
    ("unity", "iceberg"): (
        "OSS Unity Catalog's Iceberg REST API is read-only (GET only), and "
        "UCSingleCatalog 0.4.0 cannot write Iceberg from Spark 4.0. "
        "(gotcha 19)"
    ),
    ("unity", "delta", "spark", "trino"): (
        "Trino's Delta Lake connector requires a Hive Metastore "
        "(hive.metastore.uri), and Unity deployments do not include one. "
        "Trino has no native OSS Unity integration for Delta. (gotcha 25)"
    ),
}


def explain_combination(
    catalog: str, table_format: str, pipeline_engine: str, query_engine: str
) -> str:
    """Return why a component combination is unsupported, or '' if unknown.

    Not every rejected combination has a recorded reason -- some are simply
    untested rather than known-broken -- so callers must handle an empty
    string.
    """
    for key in (
        (catalog, table_format, pipeline_engine, query_engine),
        (table_format, query_engine),
        (catalog, table_format),
    ):
        note = _COMBINATION_NOTES.get(key)
        if note:
            return note
    return ""


def nearest_supported(
    catalog: str, table_format: str, pipeline_engine: str, query_engine: str
) -> tuple[str, ...] | None:
    """Return the supported combination closest to the one requested.

    "Closest" is the fewest component swaps. Ties break toward changing the
    query engine before the catalog or format, since the query engine is
    usually the least consequential choice and the one a user is most willing
    to change.
    """
    requested = (catalog, table_format, pipeline_engine, query_engine)
    # Later positions are cheaper to change, so weight earlier ones higher.
    weights = (8, 4, 2, 1)

    best: tuple[str, ...] | None = None
    best_cost = 99
    for candidate in _SUPPORTED_COMBINATIONS:
        cost = sum(w for w, r, c in zip(weights, requested, candidate, strict=True) if r != c)
        if cost < best_cost:
            best, best_cost = candidate, cost
    return best


class TableNamesConfig(ConfigModel):
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

    # Financial (FinServ-Crime, AML) auxiliary tables. These are ignored by
    # Customer 360 recipes but referenced by Financial pipeline scripts and
    # workloads (W1-W11). Defaults match the DDL in
    # ``src/lakebench/deploy/financial_ddl.py``.
    silver_entities: str = Field(
        default="silver.entities",
        description="Silver entities table (Financial): namespace.table",
    )
    silver_accounts: str = Field(
        default="silver.accounts",
        description="Silver accounts table (Financial): namespace.table",
    )
    silver_account_statements: str = Field(
        default="silver.account_statements",
        description="Silver camt.053-shaped statement-line table with running balance (Financial): namespace.table",
    )
    silver_counterparty_edges: str = Field(
        default="silver.counterparty_edges",
        description="Silver entity-to-entity edge table (Financial): namespace.table",
    )
    silver_entity_profiles: str = Field(
        default="silver.entity_profiles",
        description="Silver per-entity behavioural baseline table (Financial, C-PROFILES): namespace.table",
    )
    gold_alerts: str = Field(
        default="gold.alerts",
        description="Gold alerts table (Financial): namespace.table",
    )
    gold_risk_scores: str = Field(
        default="gold.risk_scores",
        description="Gold entity risk-score table (Financial): namespace.table",
    )
    gold_entity_clusters: str = Field(
        default="gold.entity_clusters",
        description="Gold synthetic-id / community-detection cluster table (Financial): namespace.table",
    )
    gold_daily_dashboards: str = Field(
        default="gold.daily_dashboards",
        description="Gold daily-aggregate dashboard table (Financial): namespace.table",
    )
    # Transaction-monitoring operations layer (GOALS P10), written by
    # tm_operations.py after detection.
    gold_tm_reconciliation: str = Field(
        default="gold.tm_reconciliation",
        description="Per-cycle monitoring completeness and funnel ledger (Financial)",
    )
    gold_scenario_coverage: str = Field(
        default="gold.scenario_coverage",
        description="Scenario-to-typology coverage matrix (Financial)",
    )
    gold_alert_dispositions: str = Field(
        default="gold.alert_dispositions",
        description="L1 triage priority and disposition per alert (Financial)",
    )
    gold_cases: str = Field(
        default="gold.cases",
        description="Customer-keyed L2 cases with SAR decisions (Financial)",
    )

    def financial_env(self) -> dict[str, str]:
        """Env vars the financial Spark scripts read their table names from.

        Without these the scripts fell back to their own hard-coded defaults,
        so a table override in config changed what the benchmark, maintenance
        and destroy touched but not what the pipeline wrote.
        """
        return {
            "LB_FINANCIAL_SILVER_TRANSACTIONS": self.silver,
            "LB_FINANCIAL_SILVER_TXNS": self.silver,
            "LB_FINANCIAL_SILVER_ENTITIES": self.silver_entities,
            "LB_FINANCIAL_SILVER_ACCOUNTS": self.silver_accounts,
            "LB_FINANCIAL_SILVER_STATEMENTS": self.silver_account_statements,
            "LB_FINANCIAL_SILVER_EDGES": self.silver_counterparty_edges,
            "LB_FINANCIAL_SILVER_PROFILES": self.silver_entity_profiles,
            "LB_FINANCIAL_GOLD_ALERTS": self.gold_alerts,
            "LB_FINANCIAL_GOLD_RISK_SCORES": self.gold_risk_scores,
            "LB_FINANCIAL_GOLD_CLUSTERS": self.gold_entity_clusters,
            "LB_FINANCIAL_GOLD_DASHBOARDS": self.gold_daily_dashboards,
            "LB_FINANCIAL_GOLD_TM_RECONCILIATION": self.gold_tm_reconciliation,
            "LB_FINANCIAL_GOLD_SCENARIO_COVERAGE": self.gold_scenario_coverage,
            "LB_FINANCIAL_GOLD_ALERT_DISPOSITIONS": self.gold_alert_dispositions,
            "LB_FINANCIAL_GOLD_CASES": self.gold_cases,
        }

    def workload_tables(
        self, schema: str, *, layers: tuple[str, ...] = ("bronze", "silver", "gold")
    ) -> list[str]:
        """Every table the pipeline writes for ``schema``, bronze first.

        Customer 360 writes one table per layer. Financial writes several per
        layer; maintenance, compaction and destroy that only looked at
        ``silver``/``gold`` missed all but two of them.
        """
        if schema != "financial":
            by_layer = {"bronze": [self.bronze], "silver": [self.silver], "gold": [self.gold]}
        else:
            by_layer = {
                "bronze": [self.bronze, "bronze.manifest"],
                "silver": [
                    self.silver,
                    self.silver_entities,
                    self.silver_accounts,
                    self.silver_account_statements,
                    self.silver_counterparty_edges,
                    self.silver_entity_profiles,
                ],
                "gold": [
                    self.gold_alerts,
                    self.gold_risk_scores,
                    self.gold_entity_clusters,
                    self.gold_daily_dashboards,
                    "gold.detection_status",
                    self.gold_tm_reconciliation,
                    self.gold_scenario_coverage,
                    self.gold_alert_dispositions,
                    self.gold_cases,
                ],
            }
        out: list[str] = []
        for layer in layers:
            for t in by_layer[layer]:
                if t not in out:
                    out.append(t)
        return out


class MaintenanceSettleConfig(ConfigModel):
    """Wait for storage to settle between batch maintenance and the post round.

    On FlashBlade at c360 scale 10 the compacted tables read QpH 546 two
    minutes after maintenance, 569 at +15 min and 841 at +35 min, against 828
    before it (LB-150). A post round taken straight away measured the object
    store working off the delete and rewrite burst. The wait probes one
    storage-bound query until it is stable; see ``lakebench.benchmark.settle``.
    Batch mode only: continuous-mode maintenance runs during the stream and
    is not waited on.
    """

    enabled: bool = Field(
        default=True,
        description="Probe until storage settles before the post-maintenance round",
    )
    # Recovery took about 35 minutes in the one measured case; 45 minutes
    # leaves 10 minutes of margin before the post round runs unsettled.
    max_seconds: int = Field(
        default=2700,
        ge=60,
        le=14400,
        description=(
            "Longest wait after maintenance. When reached, the post round still runs "
            "but maintenance_value_pct is null"
        ),
    )
    interval_seconds: int = Field(
        default=60,
        ge=5,
        le=3600,
        description="Seconds between the start of consecutive probes",
    )
    # The unsettled rounds were 27-34% slow and the in-round spread of one
    # query at scale 10 is a few percent, so 10% separates the two.
    tolerance_pct: float = Field(
        default=10.0,
        gt=0,
        le=100,
        description=(
            "Settled when two consecutive probes differ by at most this percent and, "
            "when a pre-maintenance time is known, neither is slower than it by more"
        ),
    )
    probe_query: str | None = Field(
        default=None,
        description=(
            "Benchmark query name to probe with. Default: the workload's first "
            "scan-class query (a full scan of the table compaction rewrote)"
        ),
    )
    probe_samples: int = Field(
        default=1,
        ge=1,
        le=10,
        description="Timed runs per probe; the probe time is their median",
    )


class BenchmarkConfig(ConfigModel):
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
    # One sample per query cannot tell a change from noise: same-run rounds
    # on the live cluster differed 3-11% in QpH and a post-maintenance round
    # read 10-80% slower per query with nothing to compare that against
    # (LB-150). Three samples give a median and a measured spread.
    iterations: int = Field(
        default=3,
        ge=1,
        le=100,
        description=(
            "Timed runs of each query per benchmark round. QpH is scored from the "
            "per-query median and the spread is recorded; 1 is a quick run with no "
            "measured spread"
        ),
    )
    maintenance_settle: MaintenanceSettleConfig = Field(default_factory=MaintenanceSettleConfig)


class ArchitectureConfig(ConfigModel):
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
            if "pipeline" in data:
                raise ValueError(
                    "both 'architecture.processing' (deprecated) and 'architecture.pipeline' "
                    "are set; move the settings under 'pipeline' and remove 'processing'"
                )
            data = dict(data)
            data["pipeline"] = data.pop("processing")
        return data

    @model_validator(mode="after")
    def financial_table_defaults(self) -> ArchitectureConfig:
        """Point ``tables.silver``/``tables.gold`` at the financial tables.

        Their defaults are the Customer 360 names. The financial scripts
        write ``silver.transactions`` and the dashboards, so on an AML run
        the benchmark queried ``silver.customer_interactions_enriched`` (7 of
        8 queries failed TABLE_NOT_FOUND) and maintenance, compaction and
        destroy targeted tables that did not exist.

        A value equal to the Customer 360 default counts as unset: a config
        saved with every field and later switched to ``schema: financial``
        still resolves to the financial tables. Other explicit values win.
        The fields are not marked as explicitly set, so dumping with
        ``exclude_unset`` and switching the schema back both behave.
        """
        if self.workload.schema_type.value != "financial":
            return self
        t = self.tables
        c360 = TableNamesConfig()
        swaps = {"silver": "silver.transactions", "gold": t.gold_daily_dashboards}
        for field, value in swaps.items():
            if getattr(t, field) == getattr(c360, field):
                setattr(t, field, value)
                t.__pydantic_fields_set__.discard(field)
        return self
        explicit = self.tables.model_fields_set
        if "silver" not in explicit:
            self.tables.silver = "silver.transactions"
        if "gold" not in explicit:
            self.tables.gold = self.tables.gold_alerts
        return self

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
            parts = [
                f"Unsupported component combination: catalog={combo[0]}, "
                f"table_format={combo[1]}, engine={combo[2]}, "
                f"query_engine={combo[3]}."
            ]

            # Lead with why, not with the list. The reason is what stops the
            # user retrying a variation that fails for the same cause.
            reason = explain_combination(*combo)
            if reason:
                parts.append(f"\nWhy: {reason}")

            nearest = nearest_supported(*combo)
            if nearest:
                parts.append(
                    f"\nClosest supported: catalog={nearest[0]}, "
                    f"table_format={nearest[1]}, engine={nearest[2]}, "
                    f"query_engine={nearest[3]}"
                )

            supported = "\n".join(
                f"  - catalog={c}, table_format={t}, engine={e}, query_engine={q}"
                for c, t, e, q in _SUPPORTED_COMBINATIONS
            )
            parts.append(f"\nAll supported combinations:\n{supported}")
            raise ValueError("\n".join(parts))
        return self


# =============================================================================
# Observability Configuration (Layer 3)
# =============================================================================


class ReportIncludeConfig(ConfigModel):
    """Report content configuration."""

    summary: bool = True
    stage_breakdown: bool = True
    storage_metrics: bool = True
    resource_utilization: bool = True
    recommendations: bool = True
    platform_metrics: bool = True


class ReportsConfig(ConfigModel):
    """Reports configuration.

    Reports are written into per-run directories under the metrics output_dir.
    The output_dir field is kept for backward compatibility but is no longer
    the primary output location.
    """

    enabled: bool = True
    output_dir: str = "./lakebench-output/runs"
    format: ReportFormat = ReportFormat.HTML
    include: ReportIncludeConfig = Field(default_factory=ReportIncludeConfig)


class ObservabilityConfig(ConfigModel):
    """Layer 3: Observability configuration.

    Flat model -- use top-level keys (enabled, prometheus_stack_enabled, etc.).
    Deeply nested YAML (metrics.prometheus.enabled) is rejected to prevent
    silent data loss (see BUG-029).
    """

    enabled: bool = False
    prometheus_stack_enabled: bool = True
    # DEPRECATED: no consumer wires these to PodMonitor deployment.
    # Default is None (not True) so a dump/load roundtrip does not carry
    # a value that trips the deprecation warning below -- the warning
    # is intended to fire only when a user explicitly writes the field
    # in their YAML.
    s3_metrics_enabled: bool | None = None
    spark_metrics_enabled: bool | None = None
    dashboards_enabled: bool = True
    retention: str = "7d"
    storage: str = "10Gi"
    storage_class: str = ""
    # kube-prometheus-stack chart version (bundles Prometheus + Grafana +
    # node-exporter + kube-state-metrics as one unit). Pinned as of 2026-07-27
    # -- the deploy previously carried no --version flag at all, so it
    # silently tracked whatever the Helm repo served at install time. That
    # currently resolves to Prometheus v3.13.1 + Grafana v13.1.x.
    chart_version: str = "87.19.2"
    reports: ReportsConfig = Field(default_factory=ReportsConfig)

    @model_validator(mode="after")
    def _warn_dead_metric_flags(self) -> ObservabilityConfig:
        # Only warn when the user gave the field a real value. None is the
        # sentinel default; a dump/load roundtrip that carries None back
        # in must not re-trigger the warning.
        for field in ("s3_metrics_enabled", "spark_metrics_enabled"):
            if getattr(self, field) is not None:
                import warnings

                warnings.warn(
                    f"observability.{field} is unwired -- setting it has no effect. "
                    "PodMonitor deployment is not gated on this flag today.",
                    DeprecationWarning,
                    stacklevel=2,
                )
        return self


# =============================================================================
# Spark Configuration Overrides
# =============================================================================


class SparkConfOverrides(ConfigModel):
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


class LakebenchConfig(ConfigModel):
    """Root configuration for Lakebench.

    This is the master configuration that matches Section 4 of the spec.
    All values shown are defaults unless marked REQUIRED.
    """

    # Metadata
    name: str = Field(
        default="",
        description="Unique name for this deployment (REQUIRED)",
        max_length=63,  # matches K8s namespace + S3 bucket-tag safe length
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

    @model_validator(mode="after")
    def resolve_format_versions(self) -> LakebenchConfig:
        """Auto-resolve table format versions based on the Spark image.

        When the user hasn't overridden the format version (or set it to
        ``"auto"``), this picks the best default for the configured Spark
        major.minor.  When the user *has* specified a version, this
        validates it against the compatibility matrix.
        """
        from lakebench.spark.job import resolve_format_version

        spark_image = self.images.spark
        fmt = self.architecture.table_format

        if fmt.type == TableFormatType.ICEBERG:
            resolved = resolve_format_version(
                spark_image,
                "iceberg",
                fmt.iceberg.version,
            )
            if resolved != fmt.iceberg.version:
                fmt.iceberg.version = resolved
        elif fmt.type == TableFormatType.DELTA:
            resolved = resolve_format_version(
                spark_image,
                "delta",
                fmt.delta.version,
            )
            if resolved != fmt.delta.version:
                fmt.delta.version = resolved
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
