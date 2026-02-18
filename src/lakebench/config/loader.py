"""Configuration loader for Lakebench."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from .schema import LakebenchConfig


class ConfigError(Exception):
    """Base exception for configuration errors."""

    pass


class ConfigFileNotFoundError(ConfigError):
    """Raised when configuration file is not found."""

    pass


class ConfigParseError(ConfigError):
    """Raised when configuration file cannot be parsed."""

    pass


class ConfigValidationError(ConfigError):
    """Raised when configuration validation fails."""

    def __init__(self, message: str, errors: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.errors = errors or []


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML file and return as dictionary.

    Args:
        path: Path to YAML file

    Returns:
        Dictionary containing parsed YAML

    Raises:
        ConfigFileNotFoundError: If file doesn't exist
        ConfigParseError: If YAML parsing fails
    """
    if not path.exists():
        raise ConfigFileNotFoundError(f"Configuration file not found: {path}")

    try:
        with open(path) as f:
            content = yaml.safe_load(f)
            return content if content else {}
    except yaml.YAMLError as e:
        raise ConfigParseError(f"Failed to parse YAML: {e}")  # noqa: B904


def load_config(path: str | Path) -> LakebenchConfig:
    """Load and validate Lakebench configuration from file.

    Args:
        path: Path to configuration YAML file

    Returns:
        Validated LakebenchConfig object

    Raises:
        ConfigFileNotFoundError: If file doesn't exist
        ConfigParseError: If YAML parsing fails
        ConfigValidationError: If validation fails
    """
    path = Path(path)
    data = load_yaml(path)

    try:
        return LakebenchConfig.model_validate(data)
    except ValidationError as e:
        errors = e.errors()
        error_messages = []
        for err in errors:
            loc = ".".join(str(x) for x in err["loc"])
            msg = err["msg"]
            error_messages.append(f"  - {loc}: {msg}")

        raise ConfigValidationError(  # noqa: B904
            "Configuration validation failed:\n" + "\n".join(error_messages),
            errors=[dict(e) for e in errors],  # type: ignore[call-overload]
        )


def save_config(config: LakebenchConfig, path: str | Path) -> None:
    """Save configuration to YAML file.

    Args:
        config: LakebenchConfig object
        path: Path to save YAML file
    """
    path = Path(path)
    data = config.model_dump(mode="json", exclude_defaults=False)

    with open(path, "w") as f:
        yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False, indent=2)


def generate_default_config(
    name: str,
    s3_endpoint: str = "",
    s3_access_key: str = "",
    s3_secret_key: str = "",
    namespace: str = "",
) -> LakebenchConfig:
    """Generate a default configuration with common values pre-filled.

    This is used by `lakebench init` to create a starter configuration.

    Args:
        name: Deployment name (required)
        s3_endpoint: S3 endpoint URL
        s3_access_key: S3 access key
        s3_secret_key: S3 secret key
        namespace: Kubernetes namespace (defaults to name)

    Returns:
        LakebenchConfig with defaults
    """
    config_dict: dict[str, Any] = {
        "name": name,
        "description": f"Lakebench deployment: {name}",
        "version": 1,
    }

    # Platform configuration
    platform: dict[str, Any] = {}

    if namespace:
        platform["kubernetes"] = {"namespace": namespace}

    if s3_endpoint or s3_access_key or s3_secret_key:
        s3_config: dict[str, Any] = {}
        if s3_endpoint:
            s3_config["endpoint"] = s3_endpoint
        if s3_access_key:
            s3_config["access_key"] = s3_access_key
        if s3_secret_key:
            s3_config["secret_key"] = s3_secret_key
        platform["storage"] = {"s3": s3_config}

    if platform:
        config_dict["platform"] = platform

    return LakebenchConfig.model_validate(config_dict)


def generate_example_config_yaml() -> str:
    """Generate example configuration YAML with comments.

    This produces a well-documented configuration file that users can
    customize for their environment. Only fields the user MUST fill in
    are uncommented; all other options are shown commented-out with
    their defaults so users can discover and enable them.

    Returns:
        String containing commented YAML configuration
    """
    return """# Lakebench Configuration
# ========================
# This file defines your lakehouse deployment configuration.
#
# LEGEND:
#   Uncommented fields  = REQUIRED or explicitly set values
#   # field: value      = Available option with its DEFAULT value.
#                         When commented out, this default is still ACTIVE.
#   ## Section Header   = Section label (not a config field)
#
# Key behavior: commenting out an optional section does NOT disable it --
# Pydantic fills in defaults. To truly disable something, set its 'enabled'
# or 'install' field to false explicitly.
#
# Full reference: docs/configuration.md
# Recipe guide:   docs/recipes.md
#
# MINIMUM VIABLE CONFIG (3 fields):
#   name: my-lakehouse
#   platform.storage.s3.endpoint: http://your-s3:80
#   platform.storage.s3.access_key / secret_key: your-credentials
# Everything else has sensible defaults.

# REQUIRED: Unique name for this deployment (also used as K8s namespace)
name: my-lakehouse

# Optional description
# description: "My Lakebench lakehouse deployment"

# Recipe shorthand -- sets catalog + table_format + query_engine in one line.
# Valid recipes: default, hive-iceberg-trino, hive-iceberg-spark,
#   hive-iceberg-duckdb, hive-iceberg-none, hive-delta-trino, hive-delta-none,
#   polaris-iceberg-trino, polaris-iceberg-spark, polaris-iceberg-duckdb,
#   polaris-iceberg-none
# See docs/recipes.md for details.
# recipe: hive-iceberg-trino

# Config schema version (always 1)
# version: 1

# ============================================================================
# IMAGES
# ============================================================================
# Container images for all components. Override for private registries.
# See docs/datagen-custom-images.md for building custom datagen images.
# images:
#   datagen: docker.io/sillidata/lb-datagen:v2    # Customizable (see docs/datagen-custom-images.md)
#   spark: apache/spark:3.5.4-python3
#   postgres: postgres:17
#   hive: apache/hive:3.1.3
#   trino: trinodb/trino:479
#   polaris: apache/polaris:1.3.0-incubating
#   prometheus: prom/prometheus:v2.48.0
#   grafana: grafana/grafana:10.2.0
#   pull_policy: IfNotPresent        # Always | IfNotPresent | Never
#   pull_secrets:                     # List of K8s imagePullSecret names
#     - my-registry-secret

# ============================================================================
# LAYER 1: PLATFORM
# ============================================================================
platform:
  kubernetes:
    # context: ""                    # Empty = use current kubectl context
    namespace: ""                    # Empty = use deployment name
    # create_namespace: true

  storage:
    s3:
      # REQUIRED: S3-compatible endpoint URL
      # Examples:
      #   FlashBlade: http://your-s3-endpoint:80
      #   MinIO: http://minio:9000
      #   AWS S3: https://s3.us-east-1.amazonaws.com
      endpoint: ""

      # REQUIRED: S3 credentials (either inline or secret_ref)
      access_key: ""
      secret_key: ""
      # secret_ref: ""               # OR: name of existing K8s Secret

      # region: us-east-1
      # path_style: true             # true for FlashBlade/MinIO, false for AWS S3
      # buckets:
      #   bronze: lakebench-bronze
      #   silver: lakebench-silver
      #   gold: lakebench-gold
      # create_buckets: true

    ## Scratch storage for Spark shuffle PVCs
    ## When enabled, Spark shuffle data uses PVCs instead of emptyDir.
    ## Any StorageClass that provides RWO volumes works (Portworx, local-path, EBS, etc.).
    # scratch:
    #   enabled: false
    #   storage_class: px-csi-scratch    # Name of the StorageClass to use
    #   size: 100Gi
    #   create_storage_class: true       # If true, lakebench creates this StorageClass
    #                                    # on deploy (requires cluster-admin). Set false
    #                                    # if the SC already exists or is managed externally.
    #   provisioner: pxd.portworx.com    # CSI provisioner for the SC. Examples:
    #                                    #   pxd.portworx.com (Portworx)
    #                                    #   rancher.io/local-path (local-path)
    #                                    #   ebs.csi.aws.com (AWS EBS)
    #   parameters:                      # Provider-specific StorageClass parameters
    #     repl: "1"
    #     io_profile: auto
    #     priority_io: high

  # compute:
  #   spark:
  #     operator:
  #       install: false             # Set true to auto-install Spark Operator.
  #                                  # Default is false -- install the operator
  #                                  # manually or set true for auto-install.
  #       namespace: spark-operator
  #       version: "2.4.0"           # v2.x uses webhook for volume injection
  #
  #     driver:
  #       cores: 4
  #       memory: 8g
  #
  #     # Default executor sizing (proven at 1 TB+ scale)
  #     executor:
  #       instances: 8
  #       cores: 4
  #       memory: 48g
  #       memory_overhead: 12g       # Critical for stability
  #
  #     ## Per-job executor count overrides (null = auto from scale factor).
  #     ## Per-executor sizing (cores, memory, PVC) stays fixed from proven profiles.
  #     # bronze_executors: null
  #     # silver_executors: null
  #     # gold_executors: null
  #     ## Streaming job executor overrides (continuous mode)
  #     # bronze_ingest_executors: null
  #     # silver_stream_executors: null
  #     # gold_refresh_executors: null
  #     ## Global driver resource overrides
  #     # driver_memory: "8g"
  #     # driver_cores: 4
  #
  #   postgres:
  #     storage: 10Gi
  #     # storage_class: ""           # Empty = cluster default

# ============================================================================
# LAYER 2: DATA ARCHITECTURE
# ============================================================================
# See docs/recipes.md for supported (catalog, table_format, query_engine)
# combinations and guidance on choosing a recipe.
architecture:
  catalog:
    type: hive                     # hive | polaris | none
    ## Hive Metastore tuning (uncomment to override defaults)
    # hive:
    #   thrift:
    #     min_threads: 10
    #     max_threads: 50
    #     client_timeout: 300s
    #   resources:
    #     cpu_min: 500m
    #     cpu_max: "2"
    #     memory: 4Gi
    ## Polaris REST catalog settings (used when type: polaris)
    # polaris:
    #   version: 1.3.0-incubating    # Min 1.3.0 for FlashBlade/MinIO
    #   port: 8181
    #   resources:
    #     cpu: "1"
    #     memory: 2Gi

  # table_format:
  #   type: iceberg                  # iceberg (only fully supported format)
  #   iceberg:
  #     version: "1.10.1"
  #     file_format: parquet         # parquet | orc | avro
  #     properties: {}               # Additional Iceberg table properties

  # query_engine:
  #   type: trino                    # trino | spark-thrift | duckdb | none
  #   trino:
  #     coordinator:
  #       cpu: "2"
  #       memory: 8Gi
  #     worker:
  #       replicas: 2
  #       cpu: "4"
  #       memory: 16Gi
  #       spill_enabled: true
  #       spill_max_per_node: 40Gi
  #       storage: 50Gi
  #       storage_class: ""          # Empty = cluster default
  #     catalog_name: lakehouse      # Trino catalog name for Iceberg
  #   # spark_thrift:                 # Spark Thrift Server (alternative to Trino)
  #   #   cores: 2
  #   #   memory: 4g
  #   # duckdb:                        # DuckDB (lightweight in-process engine)
  #   #   cores: 2
  #   #   memory: 4g
  #   #   catalog_name: lakehouse

  pipeline:
    mode: batch                    # batch | continuous
  #   pattern: medallion             # medallion | streaming | batch
  #   ## Medallion layer configuration
  #   medallion:
  #     bronze:
  #       format: parquet
  #       path_template: customer/interactions
  #     silver:
  #       format: iceberg
  #       table_name: customer_interactions_enriched
  #       partition_by:
  #         - date
  #       transforms:
  #         - normalize_email
  #         - normalize_phone
  #         - geo_enrichment
  #         - customer_segmentation
  #         - quality_flags
  #     gold:
  #       format: iceberg
  #       tables:
  #         - name: customer_executive_dashboard
  #           partition_by: [date]
  #           aggregations: [daily_revenue, daily_engagement, churn_indicators, channel_performance]
  #   ## Continuous/streaming settings (used when mode: continuous)
  #   continuous:
  #     bronze_trigger_interval: "30 seconds"
  #     silver_trigger_interval: "60 seconds"
  #     gold_refresh_interval: "5 minutes"
  #     run_duration: 1800           # Streaming run duration in seconds
  #     checkpoint_base: checkpoints # S3 prefix for checkpoint data
  #     ## Throughput tuning
  #     max_files_per_trigger: 50    # Max Parquet files per micro-batch
  #     bronze_target_file_size_mb: 512
  #     silver_target_file_size_mb: 512
  #     gold_target_file_size_mb: 128

  workload:
    # schema: customer360            # customer360 | iot | financial
    datagen:
      # Image: configured via images.datagen (see docs/datagen-custom-images.md)
      scale: 10                    # 1 unit ~ 10 GB bronze (10 = ~100 GB)
      # mode: auto                    # auto | batch | continuous
      parallelism: 1                 # Number of datagen pods
      # file_size: 512mb
      # dirty_data_ratio: 0.08
      ## CPU and memory are hard-locked per mode (cannot be overridden):
      ##   batch:      4 CPU, 4Gi   (scale <= 10)
      ##   continuous: 8 CPU, 24Gi  (scale > 10)
      # generators: 0                # Per-pod generator processes (0 = auto)
      # uploaders: 0                 # Per-pod uploader threads (0 = auto)
      # timestamp_start: "2024-01-01"
      # timestamp_end: "2025-12-31"
      # checkpoint:
      #   enabled: true
      #   path: .lakebench_checkpoint.json
    ## Customer360 workload overrides
    # customer360:
    #   unique_customers: null       # Override: derived from scale if null
    #   date_range_days: null        # Override: defaults to 365 if null
    #   channels: [web, mobile, store, call_center, social_media]
    #   event_types: [purchase, browse, support, login, abandoned_cart]
    #   quality_distribution:
    #     clean: 0.92
    #     duplicate_suspected: 0.02
    #     incomplete: 0.03
    #     format_inconsistent: 0.03

  ## Benchmark configuration
  ## Runs analytical SQL queries against silver/gold tables via the configured
  ## query engine (Trino, Spark Thrift, or DuckDB). See docs/benchmarking.md.
  # benchmark:
  #   mode: power                    # power: single sequential stream (per-query latency)
  #                                  # throughput: N concurrent streams (aggregate QpH)
  #                                  # composite: geometric mean of power + throughput
  #   streams: 4                     # Concurrent streams (throughput/composite only;
  #                                  # ignored in power mode)
  #   cache: hot                     # hot: caches stay populated between queries
  #                                  # cold: metadata cache flushed before each run
  #   iterations: 1                  # Runs per query. 1 = raw timing, 3+ = median

  ## Table name overrides (namespace.table format)
  # tables:
  #   bronze: default.bronze_raw
  #   silver: silver.customer_interactions_enriched
  #   gold: gold.customer_executive_dashboard

# ============================================================================
# LAYER 3: OBSERVABILITY
# ============================================================================
# Flat schema -- use top-level keys directly under observability:
# observability:
#   enabled: false                   # Deploy kube-prometheus-stack (Prometheus + Grafana)
#   prometheus_stack_enabled: true   # Prometheus collection
#   s3_metrics_enabled: true         # S3 operation metrics
#   spark_metrics_enabled: true      # Spark job metrics
#   dashboards_enabled: true         # Grafana dashboards
#   retention: 7d                    # Prometheus data retention
#   storage: 10Gi                    # Prometheus PVC size
#   storage_class: ""                # PVC storage class (empty = default)
#   reports:
#     enabled: true
#     output_dir: ./lakebench-output/runs
#     format: html                   # html | json | both
#     include:
#       summary: true
#       stage_breakdown: true
#       storage_metrics: true
#       resource_utilization: true
#       recommendations: true

# ============================================================================
# SPARK CONFIGURATION OVERRIDES
# ============================================================================
# Proven defaults for S3A and shuffle. Override as needed.
# spark:
#   conf:
#     # S3A performance settings
#     spark.hadoop.fs.s3a.connection.maximum: "500"
#     spark.hadoop.fs.s3a.threads.max: "200"
#     spark.hadoop.fs.s3a.fast.upload: "true"
#     spark.hadoop.fs.s3a.multipart.size: "268435456"
#     spark.hadoop.fs.s3a.fast.upload.active.blocks: "16"
#     spark.hadoop.fs.s3a.attempts.maximum: "20"
#     spark.hadoop.fs.s3a.retry.limit: "10"
#     spark.hadoop.fs.s3a.retry.interval: "500ms"
#     # Shuffle settings
#     spark.sql.shuffle.partitions: "200"
#     spark.default.parallelism: "200"
#     # Memory settings
#     spark.memory.fraction: "0.8"
#     spark.memory.storageFraction: "0.3"
"""
