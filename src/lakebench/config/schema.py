"""Pydantic models for Lakebench configuration.

This module defines the complete configuration schema for Lakebench,
matching the specification in lakebench-spec.md Section 4.
"""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PrivateAttr,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from ._load_context import CHANGES_DATA, emit_note, purpose_from_context

logger = logging.getLogger(__name__)


class ConfigModel(BaseModel):
    """Base for every user-facing config model: unknown keys are errors.

    A misspelt or misplaced key used to be dropped silently, so the run went
    ahead on the default and the user never learned their setting was
    ignored. Deprecated spellings stay accepted because each is migrated by a
    ``mode="before"`` validator, which runs before the extra-key check.
    """

    model_config = ConfigDict(extra="forbid")

    # Keys that were once valid and now do nothing. Maps key -> what to do
    # instead. A load for a command that changes data (LoadPurpose MUTATE or
    # RUN) refuses them with that text; the teardown and read commands drop
    # them with a note, so an old config can still be destroyed, inspected
    # and converted. A model built without a purpose drops them with a
    # DeprecationWarning, as before v1.7.
    _removed_keys: ClassVar[dict[str, str]] = {}

    @model_validator(mode="before")
    @classmethod
    def _drop_removed_keys(cls, data: object, info: ValidationInfo) -> object:
        if not isinstance(data, dict) or not cls._removed_keys:
            return data
        present = [k for k in cls._removed_keys if k in data]
        if not present:
            return data
        if purpose_from_context(info.context) in CHANGES_DATA:
            raise ValueError(
                "; ".join(
                    f"'{key}' was removed: {cls._removed_keys[key]} Delete it from the config "
                    "(destroy, status and the read-only commands still load it)"
                    for key in present
                )
            )
        data = dict(data)
        for key in present:
            data.pop(key)
            emit_note(
                f"'{key}' ({cls.__name__}) is no longer used and is ignored. "
                f"{cls._removed_keys[key]} Commands that change data refuse the config "
                "until it is deleted.",
                kind="removed",
            )
        return data

    # Fields that load but that nothing in lakebench reads (D19). Setting one
    # to anything other than its default emits a warning that it has no
    # effect; the field is removed in v1.7. Comparing against the default
    # rather than "was the key written" keeps a saved config that carries
    # every field (save_config) from warning on reload. Maps field -> note.
    _dead_fields: ClassVar[dict[str, str]] = {}

    @model_validator(mode="after")
    def _warn_dead_fields(self) -> Any:
        dead = type(self)._dead_fields
        if not dead:
            return self
        fields = type(self).model_fields
        for name, note in dead.items():
            default = fields[name].get_default(call_default_factory=True)
            if getattr(self, name) == default:
                continue
            msg = (
                f"'{name}' ({type(self).__name__}) has no effect: nothing in lakebench "
                f"reads it. It will be removed in v1.7; delete it from the config. {note}"
            ).rstrip()
            emit_note(msg, kind="dead")
        return self


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
    """Datagen delivery mode (redefined 2026-09-28, Wave 2 D1).

    One corpus per (workload, seed) -- deterministic and layout-invariant;
    mode is how the same corpus is DELIVERED to S3, not what it contains.
    Row content is byte-for-byte identical across modes at fixed seed
    (verified by tests/cycles.rs::c360_row_identity_across_delivery_modes
    and aml_row_identity_across_delivery_modes).

    - continuous (default per owner D18, matches PipelineMode.CONTINUOUS):
      streams each bronze parquet file through S3 multipart as row-groups
      close. Files arrive in S3 progressively rather than in bursts.
      RSS bound is MPU_MAX_CONCURRENT_PARTS x 5 MiB plus one row-group
      buffer per worker; the row-group buffer is the whole file today
      because the parquet crate defaults to 1M-row groups and no shipping
      config crosses that. A DG_ROW_GROUP env override in
      datagen_rs/src/writer.rs enables multi-row-group streaming (used by
      the row-identity tests); production tuning to bound RSS is a
      follow-up sprint task with live-scale measurement.
    - batch: buffers the whole file in memory then does one S3 PUT per
      file. Higher peak RSS (whole file per worker), bursty network,
      simpler failure semantics (single-PUT retry loop). Kept for stress
      tests, whole-file atomicity, and to preserve the pre-2026-09-28
      regression pins.
    - auto: resolves to continuous. No scale-based split today; the choice
      affects per-worker RSS, not correctness.

    CPU and memory are sized by scale via the autosizer, independently of
    delivery mode. Pre-2026-09-28 semantics used this enum for a
    resource-profile tier; that role has moved into scale-based sizing.
    """

    BATCH = "batch"
    CONTINUOUS = "continuous"
    AUTO = "auto"


class PipelineMode(str, Enum):
    """Pipeline execution mode.

    - batch: sequential medallion jobs
      (bronze-verify -> silver-build -> gold-finalize)
    - continuous: concurrent jobs over a corpus that keeps arriving, with
      periodic gold recomputation (bronze-ingest + silver-stream +
      gold-refresh)

    ``continuous`` is the canonical name (owner decision D18, v1.6).
    ``sustained`` is the transitional alias: ``PipelineMode.SUSTAINED`` is the
    same member as ``PipelineMode.CONTINUOUS``, ``PipelineMode("sustained")``
    resolves to it, and a config that writes ``mode: sustained`` loads with a
    DeprecationWarning. Metrics files still record ``pipeline_mode="sustained"``
    for continuous runs; read either spelling with ``is_continuous_mode``.
    """

    BATCH = "batch"
    CONTINUOUS = "continuous"
    SUSTAINED = "continuous"  # alias of CONTINUOUS, not a separate member

    @classmethod
    def _missing_(cls, value: object) -> PipelineMode | None:
        if isinstance(value, str) and value.strip().lower() == "sustained":
            return cls.CONTINUOUS
        return None


def is_continuous_mode(mode: object) -> bool:
    """True for the continuous pipeline mode under either spelling.

    Accepts a ``PipelineMode``, a string (``"continuous"`` or the legacy
    ``"sustained"`` that metrics files and old configs carry) or None.
    """
    value = getattr(mode, "value", mode)
    return isinstance(value, str) and value.strip().lower() in ("continuous", "sustained")


class ReportFormat(str, Enum):
    """Supported report output formats."""

    HTML = "html"
    JSON = "json"
    BOTH = "both"


# =============================================================================
# Images Configuration
# =============================================================================


# The Hive the Stackable HiveCluster runs (its spec.image.productVersion).
# Fixed on purpose: Stackable recommends 3.1.3, and Hive 4 breaks Iceberg
# (get_table TApplicationException) and Trino ANALYZE. The template renders
# this constant and the deploy result and run provenance record it, so the
# evidence names the version that ran. images.hive does not select it: the
# Stackable operator takes a productVersion, not an image.
STACKABLE_HIVE_VERSION = "3.1.3"


def _hive_version_of(image: str) -> str:
    """The Hive version an ``images.hive`` value names.

    Accepts an image reference (``apache/hive:3.1.3``) or a bare
    productVersion (``3.1.3``). A digest or an untagged image names no
    version and returns the value unchanged, so it never matches.
    """
    if "@" in image:
        return image
    last = image.rsplit("/", 1)[-1]
    if ":" in last:
        tag = last.rsplit(":", 1)[1]
        # Stackable's own image tag: <hive version>-stackable<sdp version>.
        return tag.split("-stackable", 1)[0]
    return image


class ImagesConfig(ConfigModel):
    """Container image configuration for all Lakebench components."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "pull_secrets": "No deployer ever applied it; removed in v1.5.",
    }

    # Immutable tag = the datagen_rs commit it was built from. Bump it with
    # every datagen_rs change; :latest drifted from the code it claimed to be.
    # b6f2905 is the datagen v1.6 sprint tip on integrate (2026-09-28) with
    # the Python base bumped from the floating python:3.13-slim tag to
    # python:3.14-slim, digest-pinned so a rebuild reproduces the same
    # Python layer bytes. Rust source unchanged from 25f1aa8, so corpus
    # bytes at a fixed seed are unchanged; MODEL_VERSION stays
    # datagen-v2-rs-0.3. Datagen_rs freeze wiring carried forward from
    # 25f1aa8: LB-191 dirty-ratio semantics, screening rates in prereg
    # 3.6.1, calibration-replicate seed refusal in perturbation_for_seed,
    # multi-writer guard on --mode reference, S3Sink::put_multipart retry,
    # --delivery-mode {batch|continuous} MpuWriter switch with continuous
    # as the Rust binary default, and the #52 registered_looks_open freeze
    # wiring.
    # 30603b1 re-fits the datagen peak-RSS memory model (LB-199): the autosizer
    # under-sized financial node-0 at scale 100, OOMKilling it at the 17Gi
    # default. Only datagen_rs/entrypoint.py (thread cap) and config/autosizer.py
    # (pod memory request) change; the Rust generator is unchanged, so the corpus
    # is byte-identical -- PROVEN on the pushed image (seed-43 --mode all
    # byte-compare vs 9382420: 73/73 objects identical, and vs a thread-throttled
    # run: identical; cargo output pins hold). MODEL_VERSION stays
    # datagen-v2-rs-0.3, so it belongs to the same freeze under a new tag. This is
    # the FUNCTIONAL default; it is DISQUALIFIED from generating any D8 / A6 /
    # registered-look / calibration corpus -- those runs pass an explicit frozen
    # digest via --generator-image (aml-protocol.md), never this default.
    # e14d0fd (LB-204): mimalloc allocator, typology payloads pruned to each
    # pod's own files, world columns recomputed on demand, fixed 64 MB files,
    # re-fit memory model with a 16Gi pod cap, LB-196 delivery-mode forwarding.
    # AML pod peak at scale 100 fell from 18.18 to 5.70 GiB. Output-neutral,
    # PROVEN on the pushed image: seed-43 --mode all byte-compare against the
    # frozen generator (rebuilt from 9382420 source) -- 73/73 objects at 128 MB,
    # 141/141 at 64 MB, 141/141 with 4 pods, 141/141 thread-throttled; cargo pin
    # cycles.rs::financial_output_is_pinned_to_the_frozen_generator. MODEL_VERSION
    # stays datagen-v2-rs-0.3, same freeze. Functional default, disqualified from
    # registered-look corpora as below.
    # 034f998: same Rust source as e14d0fd; entrypoint.py thread-cap model gains
    # the batch-delivery allowance. Byte-identical to e14d0fd at seed 43 on the
    # pushed images (141/141 objects), so identical to the frozen generator.
    # Pushed digest (034f998):
    # sha256:0dc67b26e6130acebd796082137fde8c6dac57e8cd668039d585ae9d086d29dc
    #   e14d0fd (sha256:ed57c1580d6babd4a504cda69a93811ab8c92d23f92978adfc4638ab691b0591)
    # 034f998 and e14d0fd were deleted with the repository wipe of 2026-09-30.
    # Prior tags (deleted from docker.io between 2026-09-29 22:30 and 23:29;
    # rebuild from source to reproduce):
    #   30603b1 (sha256:608425f46ed0f211f7eff1e63b0713a76835ad57e4cd1828252cc28fc776ea16) LB-199 memory refit
    #   9382420 (sha256:2faad1cc0252a165a56361a06f159a62ba7c4387c83adfb7c46fe260af23b8f2) live-metrics Pushgateway push
    #   b6f2905 (sha256:312f9ecfa301b09f696cd04f9b6d44052656041fc293d9d76f0adddd01fbd4f6)
    #   25f1aa8 (sha256:8dbc2705c6d95dbc3a259b3d9e3007e5cd951db3df2655afc66d357fd1fed5f7)
    #   0a83acd (sha256:acdf3925...)
    #   7c24641 (sha256:c5a6bc80...)
    # 1.6.0: the v1.6 release image, rebuilt from the same datagen_rs source as
    # 034f998 (datagen_rs unchanged 034f998..release; base images digest-pinned,
    # cargo --locked) after the docker.io repository was wiped (LB-209). Release
    # tags are the exception to the commit-tag rule above.
    # Pushed digest (1.6.0): sha256:5fda9025fb9b455b390e1138d82e9f6ef16d214dfa9419815be0111d2f6fce0a
    datagen: str = "docker.io/sillidata/lb-datagen:1.6.0"
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

    _dead_fields: ClassVar[dict[str, str]] = {
        "prometheus": (
            "Prometheus deploys from the kube-prometheus-stack chart; pin it with "
            "observability.chart_version."
        ),
        "grafana": (
            "Grafana deploys from the kube-prometheus-stack chart; pin it with "
            "observability.chart_version."
        ),
    }

    @model_validator(mode="after")
    def _warn_hive_not_deployed(self) -> ImagesConfig:
        # The HiveCluster always runs STACKABLE_HIVE_VERSION. A
        # different images.hive used to be recorded as the Hive that ran
        # while 3.1.3 was deployed; now it is ignored, and the user is told.
        version = _hive_version_of(self.hive)
        if version == STACKABLE_HIVE_VERSION:
            return self
        msg = (
            f"images.hive '{self.hive}' has no effect: the Stackable HiveCluster always "
            f"runs Hive {STACKABLE_HIVE_VERSION} (Hive 4 breaks Iceberg and Trino ANALYZE), "
            f"and run provenance records {STACKABLE_HIVE_VERSION}. Delete images.hive "
            f"from the config or set it to apache/hive:{STACKABLE_HIVE_VERSION}."
        )
        emit_note(msg, kind="dead")
        return self

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
    """S3 bucket names for each data layer.

    These defaults apply only to a bare ``S3BucketsConfig``. In a full config
    ``LakebenchConfig.default_bucket_names`` replaces every name the user did
    not set with ``<deployment name>-<layer>``.
    """

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

    # Credentials. Only the inline keys are used: deploy writes them into the
    # lakebench-s3-credentials Secret and the CLI's S3 client reads them.
    access_key: str = ""
    secret_key: str = ""
    # Not consumed: no deployer reads an existing Secret. Refused
    # without inline keys, which would deploy empty credentials.
    secret_ref: str = ""

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

    _dead_fields: ClassVar[dict[str, str]] = {
        "secret_ref": (
            "Deploy writes the S3 Secret from access_key/secret_key and never reads "
            "an existing Secret."
        ),
    }

    @model_validator(mode="after")
    def validate_credentials(self, info: ValidationInfo) -> S3Config:
        """Refuse secret_ref without inline keys, which would deploy empty credentials.

        Missing credentials alone are left to deploy's preflight so that
        ``info`` and ``validate`` still load a config without keys. A
        secret_ref with no inline keys is refused here: deploy would
        render the credentials Secret with empty keys. Teardown and
        inspection commands (destroy, status, clean load with
        ``allow_long_names``) still load it, so destroy can load a
        deployment made before this refusal (its S3 steps still have no
        keys, as before); the dead-field warning still fires.
        """
        has_inline = bool(self.access_key and self.secret_key)
        teardown = bool(info.context and info.context.get("allow_long_names"))
        if self.secret_ref and not has_inline and not teardown:
            raise ValueError(
                f"platform.storage.s3.secret_ref ('{self.secret_ref}') is not supported: "
                "lakebench never reads an existing Secret, so this config would deploy "
                "empty S3 credentials. Set access_key and secret_key instead; use "
                '${VAR} substitution (for example access_key: "${S3_ACCESS_KEY}") '
                "to keep the keys out of the file."
            )
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

    cores: int = Field(default=4, ge=1)
    memory: str = "8g"


class SparkExecutorConfig(ConfigModel):
    """Spark executor resource configuration."""

    instances: int = Field(default=8, ge=1)
    cores: int = Field(default=4, ge=1)
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
        ge=1,
        description="Override bronze-verify executor count. None = auto from scale.",
    )
    silver_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override silver-build executor count. None = auto from scale.",
    )
    gold_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override gold-finalize executor count. None = auto from scale.",
    )

    # Streaming job executor count overrides (None = auto from scale).
    bronze_ingest_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override bronze-ingest executor count. None = auto from scale.",
    )
    silver_stream_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override silver-stream executor count. None = auto from scale.",
    )
    gold_refresh_executors: int | None = Field(
        default=None,
        ge=1,
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
        ge=1,
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

    min_threads: int = Field(default=10, ge=1)
    max_threads: int = Field(default=50, ge=1)
    client_timeout: str = "300s"

    _dead_fields: ClassVar[dict[str, str]] = {
        "min_threads": ("The HiveCluster template sets hive.metastore.server.min.threads to 10."),
        "max_threads": ("The HiveCluster template sets hive.metastore.server.max.threads to 50."),
        "client_timeout": (
            "The HiveCluster template sets hive.metastore.client.socket.timeout to 300s."
        ),
    }


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
    port: int = Field(default=8181, ge=1, le=65535)
    client_secret: str = ""
    resources: PolarisResourcesConfig = Field(default_factory=PolarisResourcesConfig)

    _dead_fields: ClassVar[dict[str, str]] = {
        "version": "The Polaris that runs, and is recorded, is the tag of images.polaris.",
    }


class UnityConfig(ConfigModel):
    """Unity Catalog configuration.

    OSS Unity Catalog is a self-hosted REST catalog server (Apache-licensed).
    Uses PostgreSQL for persistence, similar to Polaris.
    """

    version: str = "0.4.0"
    spark_connector_version: str = "0.4.0"
    port: int = Field(default=8080, ge=1, le=65535)
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

    _dead_fields: ClassVar[dict[str, str]] = {
        "file_format": "Tables are always written as Parquet.",
        "properties": "No table property is applied from the config.",
    }


class DeltaConfig(ConfigModel):
    """Delta Lake table format configuration."""

    version: str = "auto"
    properties: dict[str, Any] = Field(default_factory=dict)

    _dead_fields: ClassVar[dict[str, str]] = {
        "properties": "No table property is applied from the config.",
    }


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

    # le: the largest tier in config/scale.py asks for scale // 50 workers,
    # 200 at the top scale of 10000.
    replicas: int = Field(default=2, ge=1, le=256)
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

    cores: int = Field(default=2, ge=1)
    memory: str = "4g"
    catalog_name: str = "lakehouse"


class DuckDBConfig(ConfigModel):
    """DuckDB query engine configuration."""

    cores: int = Field(default=2, ge=1)
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
    max_files_per_trigger: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Max Parquet files bronze-ingest reads per trigger. With "
            "bronze_trigger_interval it sets the offered load. Unset (auto): "
            "the run derives it from the corpus size and run_duration so data "
            "keeps arriving through the window (about 1.2 x run_duration of "
            "arrival), capped at 50 files per trigger (about 107 MB/s at 30 s). "
            "An explicit value that would offer the whole corpus before the "
            "window ends is refused at run start."
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
    silver_bronze_wait_seconds: int | None = Field(
        default=None,
        ge=10,
        description=(
            "Seconds silver-stream waits for the bronze table to appear before "
            "raising SilverAbort (A3, silver-plan). Unset (auto): run_duration / "
            "4, floored at 10 s, so a short run cannot spend its whole window on "
            "the wait. The old fixed 1800 was longer than a default run_duration "
            "and prevented the LB-044 gate from ever firing on a stalled "
            "bronze-ingest."
        ),
    )
    gold_target_file_size_mb: int = Field(
        default=128,
        ge=32,
        description="Target Iceberg file size for gold writes (MB)",
    )

    # Iceberg retention -- periodic expire_snapshots + remove_orphan_files
    # via Trino to prevent unbounded snapshot/metadata growth during long
    # sustained runs.
    retention_interval: int | None = Field(
        default=None,
        ge=300,
        le=7200,
        description=(
            "Seconds between table maintenance rounds (Iceberg expire_snapshots + "
            "remove_orphan_files, Delta VACUUM). Unset (auto): run_duration / 3, "
            "within 300..7200, so a default run maintains inside its window; the "
            "old fixed 1800 equalled the default run_duration and never fired. An "
            "explicit value too long to fire inside the window is refused at run "
            "start unless --skip-maintenance is given."
        ),
    )
    retention_threshold: str = Field(
        default="30m",
        description=(
            "Iceberg snapshot retention threshold passed to Trino "
            "(e.g. '30m', '1h', '7d'). Snapshots older than this are expired. "
            "A whole number and one unit: s, m, h or d."
        ),
    )

    @field_validator("retention_threshold")
    @classmethod
    def _validate_retention_threshold(cls, v: str) -> str:
        # The maintenance parser used to guess: an unknown unit read as
        # minutes ("7D" -> 7 min) and "1.5h" / "30min" raised mid-run.
        import re as _re

        if not isinstance(v, str) or not _re.fullmatch(r"\s*\d+\s*[smhdSMHD]\s*", v):
            raise ValueError(
                f"retention_threshold {v!r} is not a duration: use a whole number and one "
                "unit, s, m, h or d (for example '30m', '1h', '7d')"
            )
        return "".join(v.split()).lower()

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
        description=(
            "Seconds between compaction rounds. 0 = auto (2x the effective "
            "retention_interval, resolved at run start). An explicit value too "
            "long to fire inside the window is refused at run start unless "
            "compaction_enabled is false or --skip-maintenance is given."
        ),
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
        return self

    def effective_retention_interval(self, run_duration: int | None = None) -> int:
        """retention_interval, or the auto value for *run_duration* (the
        config's own when None): a third of the window, within 300..7200."""
        if self.retention_interval is not None:
            return self.retention_interval
        window = self.run_duration if run_duration is None else run_duration
        return min(7200, max(300, int(window) // 3))

    def effective_compaction_interval(self, run_duration: int | None = None) -> int:
        """compaction_interval, or auto: 2x the effective retention_interval."""
        if self.compaction_interval:
            return self.compaction_interval
        return 2 * self.effective_retention_interval(run_duration)

    def effective_silver_bronze_wait_seconds(self, run_duration: int | None = None) -> int:
        """silver_bronze_wait_seconds, or auto: run_duration // 4 floored at 10.

        A3 (silver-plan): caps the wait for the bronze table so a stream
        cannot burn its whole window on the wait. The floor keeps the value
        positive even for a minimum-length run.
        """
        if self.silver_bronze_wait_seconds is not None:
            return self.silver_bronze_wait_seconds
        window = self.run_duration if run_duration is None else run_duration
        return max(10, int(window) // 4)


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
        """``pipeline.continuous`` is the canonical key; ``sustained`` the alias.

        The settings block is stored on the ``sustained`` field, so the
        canonical key is renamed onto it silently and the transitional
        spelling is accepted with a DeprecationWarning (D18).
        """
        if not isinstance(data, dict):
            return data
        if "continuous" in data and "sustained" in data:
            # Dropping one silently would lose settings without a trace.
            raise ValueError(
                "both 'pipeline.continuous' and 'pipeline.sustained' (deprecated) are "
                "set; move the settings under 'continuous' and remove 'sustained'"
            )
        if "continuous" in data:
            data = dict(data)
            data["sustained"] = data.pop("continuous")
        elif "sustained" in data:
            msg = (
                "'pipeline.sustained' is deprecated and will be removed in a future "
                "release; rename the block to 'pipeline.continuous'."
            )
            emit_note(msg)
        return data

    @field_validator("mode", mode="before")
    @classmethod
    def _migrate_sustained_mode(cls, v: object) -> object:
        """Accept the transitional ``sustained`` value as ``continuous``."""
        if isinstance(v, str) and v.strip().lower() == "sustained":
            msg = (
                "pipeline mode 'sustained' is deprecated and will be removed in a future "
                "release; use 'mode: continuous'."
            )
            emit_note(msg)
            return PipelineMode.CONTINUOUS.value
        return v

    @model_validator(mode="after")
    def _warn_pattern(self) -> ProcessingConfig:
        # ProcessingPattern predates pipeline.mode and is removed in v1.7
        # (D18). Stages are chosen by the mode; the only remaining reader is
        # the auto-sizer, which splits the CPU budget for 'streaming'.
        if self.pattern != ProcessingPattern.MEDALLION:
            msg = (
                f"'pipeline.pattern: {self.pattern.value}' is deprecated and will be "
                "removed in v1.7. The pipeline stages are chosen by 'pipeline.mode' "
                "(batch or continuous). The pattern does not select stages: it is "
                "recorded in the run's config snapshot, and 'streaming' makes the "
                "auto-sizer give Spark 60% and datagen 40% of the CPU budget."
            )
            emit_note(msg)
        return self

    @model_validator(mode="after")
    def _validate_cycles(self) -> ProcessingConfig:
        """Ensure cycles > 1 is only used with batch mode."""
        if self.cycles > 1 and self.mode != PipelineMode.BATCH:
            raise ValueError(
                "cycles > 1 requires pipeline mode 'batch' (continuous mode has its own "
                "iteration model)"
            )
        return self


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

    _removed_keys: ClassVar[dict[str, str]] = {
        "checkpoint": (
            "The Rust generator never implemented checkpoint-resume; "
            "removed 2026-09-28. Re-run generation from the start on failure."
        ),
        "uploaders": (
            "Never forwarded to the Rust generator; removed 2026-09-28. "
            "Uploader concurrency is fixed inside the S3 sink."
        ),
    }

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
    # Datagen output file size, fixed at 64mb for every workload and mode
    # (owner decision 2026-09-29). c360 rows are drawn per file and truncated
    # by file size, so one size keeps row content identical across delivery
    # modes (DESIGN.md) and keeps corpus identity stable. The field stays so
    # existing configs that set 64mb still load; any other value is refused.
    file_size: Literal["64mb"] = "64mb"
    dirty_data_ratio: float = 0.08
    cpu: str = "2"
    memory: str = "4Gi"
    # Generator threads per pod; 0 = auto (follow the pod CPU).
    generators: int = Field(default=0, ge=0, le=1024)
    timestamp_start: str | None = Field(
        default=None,
        description="Start date for generated timestamps (ISO format, e.g. '2024-01-01'). Default: datagen built-in (2024-01-01).",
    )
    timestamp_end: str | None = Field(
        default=None,
        description=(
            "End date for generated timestamps (ISO format, exclusive, e.g. "
            "'2025-01-01'). Default: Rust generator built-in '2025-01-01' for "
            "single-cycle runs; a multi-cycle run (cycles > 1) instead splits "
            "a wider '2024-01-01' to '2025-12-31' window across cycles "
            "(deploy/datagen.py fallback, matched by metrics.c360_correctness)."
        ),
    )

    @field_validator("file_size", mode="before")
    @classmethod
    def _fixed_file_size(cls, v: object, info: ValidationInfo) -> object:
        """Accept any spelling of 64mb; refuse every other size. The loads
        that do not change data (LoadPurpose TEARDOWN, READ, COMPARE) accept
        an old size with a note, as they do a removed key, so a deployment
        made from an older config can still be inspected and torn down."""
        if isinstance(v, str) and v.strip().lower() == "64mb":
            return "64mb"
        purpose = purpose_from_context(info.context)
        if purpose is not None and purpose not in CHANGES_DATA:
            emit_note(
                f"datagen.file_size {v!r} is ignored: the file size is fixed at 64mb.",
                kind="removed",
            )
            return "64mb"
        raise ValueError(
            f"datagen.file_size is fixed at 64mb; got {v!r}. Remove the key or set 64mb. "
            "(Configs and reproduce packages from before v1.6 that used another size "
            "cannot be regenerated with this version.)"
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
            emit_note(
                "datagen.target_size is deprecated. Use datagen.scale instead. "
                "Example: scale: 10 (for ~100 GB)"
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
        ge=1,
        description="Override: unique customer count. If None, derived from scale.",
    )
    date_range_days: int | None = Field(
        default=None,
        ge=1,
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

    @field_validator("schema_type", mode="after")
    @classmethod
    def _reject_custom(cls, v: WorkloadSchema) -> WorkloadSchema:
        # 'custom' had no generator, stages or correctness contract of its own
        # and silently ran the Customer 360 query set (D13).
        if v == WorkloadSchema.CUSTOM:
            raise ValueError(
                "workload schema 'custom' is not supported in this release: it has no "
                "generator, pipeline or correctness checks of its own and would run the "
                "Customer 360 benchmark. Use 'customer360' or 'financial'."
            )
        return v

    @model_validator(mode="after")
    def _note_wrong_workload_keys(self) -> WorkloadConfig:
        """Note settings that belong to the other workload.

        A note, not a refusal, so no identity or hash moves. The note does not
        say "delete it": the financial corpus id still hashes the customer360
        and datagen fields (metrics/experiment.py), so deleting one moves the
        id of an otherwise identical corpus. Only values that
        differ from the default count, so a saved config that carries every
        field (save_config) stays quiet.
        """
        schema = self.schema_type
        wrong: list[str] = []
        if schema == WorkloadSchema.FINANCIAL:
            c360_defaults = Customer360Config()
            for name in Customer360Config.model_fields:
                if getattr(self.customer360, name) != getattr(c360_defaults, name):
                    wrong.append(f"customer360.{name}")
            # datagen.timestamp_* are not listed: the financial generator
            # ignores them, but they set silver's data clock (job.py
            # _resolve_silver_data_clock) for every workload.
            if (
                self.datagen.dirty_data_ratio
                != DatagenConfig.model_fields["dirty_data_ratio"].default
            ):
                wrong.append("datagen.dirty_data_ratio")
            other = "customer360"
            reader = "the financial generator does not read it"
        else:
            # retention_workload and retention_months are not listed: the
            # pre-benchmark maintenance honours them for every workload
            # (cli/_sustained.py resolve_maintenance_retention).
            if self.tm_operations != TmOperationsConfig():
                wrong.append("tm_operations")
            if self.w1_max_vertices != type(self).model_fields["w1_max_vertices"].default:
                wrong.append("w1_max_vertices")
            other = "financial"
            reader = "nothing in a customer360 run reads it"
        for key in wrong:
            emit_note(
                f"workload.{key} is a {other} setting; {reader}.",
                kind="wrong-workload",
                category=None,  # a UserWarning would print the note twice
            )
        return self

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


# What each workload can run on (DESIGN.md 6.5, layers 2 and 3). The
# architecture tuple list above says nothing about workloads; this does. A
# workload x format or workload x mode pair not listed here is refused at load.
# The AML stage scripts and DDL write Iceberg tables (USING iceberg), so AML on
# Delta used to pass validation and then run Iceberg code on a Delta
# deployment.
WORKLOAD_TABLE_FORMATS: dict[str, tuple[str, ...]] = {
    "customer360": ("iceberg", "delta"),
    "financial": ("iceberg",),
}
WORKLOAD_MODES: dict[str, tuple[str, ...]] = {
    "customer360": ("batch", "continuous"),
    "financial": ("batch", "continuous"),
}
_WORKLOAD_LABELS = {"customer360": "Customer 360", "financial": "financial (AML)"}


def workload_compatibility_problem(workload: str, table_format: str, mode: str) -> str:
    """Why a workload cannot run on this table format or mode, or ''."""
    label = _WORKLOAD_LABELS.get(workload, workload)
    formats = WORKLOAD_TABLE_FORMATS.get(workload)
    if formats is not None and table_format not in formats:
        return (
            f"The {label} workload supports table_format {', '.join(formats)}, not "
            f"{table_format}. Its stage scripts and table DDL are written for "
            f"{' and '.join(formats)} only, so this combination would not run the "
            f"workload it names. Set architecture.table_format.type to "
            f"{formats[0]} (for example recipe: polaris-{formats[0]}-spark-trino)."
        )
    modes = WORKLOAD_MODES.get(workload)
    if modes is not None and mode not in modes:
        return f"The {label} workload supports pipeline mode {', '.join(modes)}, not {mode}."
    return ""


# Why a combination is unsupported, keyed by the pair that causes it.
#
# A rejection that only prints the valid list makes the user diff their request
# against it to work out what they did wrong, and teaches them nothing. Every
# entry here is a real limitation that cost someone a debugging session. The
# text is shown to users, so it explains itself and cites nothing internal.
#
# Keys are checked most-specific first: a full 4-tuple, then (table_format,
# query_engine), then (catalog, table_format).
_COMBINATION_NOTES: dict[tuple[str, ...], str] = {
    ("delta", "duckdb"): (
        "DuckDB cannot read Delta on non-AWS S3. Its delta extension uses "
        "delta-kernel-rs, which ignores DuckDB's httpfs S3 settings and tries "
        "AWS IMDS (169.254.169.254) for credentials -- that hangs indefinitely "
        "against a non-AWS endpoint. There is no way to pass a custom endpoint "
        "to the delta kernel. The iceberg extension has no such limitation."
    ),
    ("polaris", "delta"): (
        "Polaris is an Iceberg-native REST catalog and has no Delta Lake "
        "support. Use Hive as the catalog for Delta tables."
    ),
    ("unity", "iceberg"): (
        "OSS Unity Catalog's Iceberg REST API is read-only (GET only), and "
        "UCSingleCatalog 0.4.0 cannot write Iceberg from Spark 4.0."
    ),
    ("unity", "delta", "spark", "trino"): (
        "Trino's Delta Lake connector requires a Hive Metastore "
        "(hive.metastore.uri), and Unity deployments do not include one. "
        "Trino has no native OSS Unity integration for Delta."
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
    silver_batch_versions: str = Field(
        default="silver.silver_batch_versions",
        description="Silver sealed-batch marker sidecar (I10, Financial): one row per (stream_id, batch_id) written last so downstream consumers hide mid-batch crashes",
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
            "LB_FINANCIAL_SILVER_BATCH_VERSIONS": self.silver_batch_versions,
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
                    self.silver_batch_versions,
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
            emit_note("'processing' is deprecated, use 'pipeline' instead.")
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

    @model_validator(mode="after")
    def validate_workload_compatibility(self) -> ArchitectureConfig:
        """Refuse a workload on a table format or mode it does not declare."""
        problem = workload_compatibility_problem(
            self.workload.schema_type.value,
            self.table_format.type.value,
            self.pipeline.mode.value,
        )
        if problem:
            raise ValueError(problem)
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
    # Per-deployment Prometheus Pushgateway for live datagen + bronze->silver
    # metrics (batch jobs Prometheus pull cannot catch). Deployed only when
    # observability is enabled; a best-effort live view, never a published
    # source (metrics.json stays authoritative). See
    # docs/internal/observability-pushgateway.md.
    pushgateway_enabled: bool = True
    pushgateway_image: str = "prom/pushgateway:v1.11.1"
    pushgateway_storage: str = "1Gi"
    pushgateway_storage_class: str = "px-csi-scratch"
    reports: ReportsConfig = Field(default_factory=ReportsConfig)

    _dead_fields: ClassVar[dict[str, str]] = {
        "reports": (
            "Every run writes report.html into its run directory under "
            "lakebench-output/runs; 'lakebench report --render' writes a "
            "fresh copy to lakebench-output/reports/ without overwriting."
        ),
        "storage_class": (
            "The Prometheus volume claim is created without a storageClassName, "
            "so it uses the cluster default StorageClass."
        ),
    }

    @model_validator(mode="after")
    def _warn_dead_metric_flags(self) -> ObservabilityConfig:
        # Only warn when the user gave the field a real value. None is the
        # sentinel default; a dump/load roundtrip that carries None back
        # in must not re-trigger the warning.
        for field in ("s3_metrics_enabled", "spark_metrics_enabled"):
            if getattr(self, field) is not None:
                emit_note(
                    f"observability.{field} is unwired -- setting it has no effect. "
                    "PodMonitor deployment is not gated on this flag today.",
                    kind="dead",
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


# Kubernetes caps namespaces, label values and pod volume names at 63
# characters (a DNS-1123 label). S3 bucket names are at most 63 characters
# (the 3-character minimum is left to the S3 client, as unit tests use
# one-letter placeholders).
K8S_LABEL_MAX = 63
S3_BUCKET_MAX = 63

# Names lakebench or a Stackable operator derives from the namespace, as
# (template, when it is rendered, what the object is). The templates are the
# literal forms in templates/secrets.yaml.j2, templates/hive/stackable-*.yaml.j2
# and stackable-operator 0.94.0, the version SDP 25.7.0's hive-operator pins:
#   crates/stackable-operator/src/crd/s3/connection/v1alpha1_impl.rs
#     volume_name = format!("{secret_class}-s3-credentials")
#   crates/stackable-operator/src/commons/tls_verification.rs
#     volume_name = format!("{secret_class}-ca-cert")
# The metastore pod volume is the tightest: 40 + len(namespace) <= 63, so a
# Hive recipe accepts a namespace of at most 23 characters. Past that the
# metastore pod is rejected, no pod is ever created, the hive-operator logs
# "metastore listener has no adress", and deploy times out after 600 s
# (LB-153).
_S3_CRED_CLASS = "lakebench-s3-credentials-{ns}"
_S3_CA_CLASS = "lakebench-s3-ca-cert-{ns}"
_DERIVED_NAMES: tuple[tuple[str, str, str], ...] = (
    (
        _S3_CRED_CLASS,
        "all",
        "label secrets.stackable.tech/class on Secret lakebench-s3-credentials",
    ),
    (
        _S3_CA_CLASS,
        "ca_cert",
        "label secrets.stackable.tech/class on Secret lakebench-ca-certificate",
    ),
    (
        _S3_CRED_CLASS + "-s3-credentials",
        "hive",
        "Hive metastore pod volume for the Stackable S3 credentials SecretClass",
    ),
    (
        _S3_CA_CLASS + "-ca-cert",
        "hive+ca_cert",
        "Hive metastore pod volume for the Stackable S3 CA SecretClass",
    ),
)


def _applicable_derived_names(cfg: LakebenchConfig) -> list[tuple[str, str]]:
    """(template, description) for every derived name this config renders."""
    is_hive = cfg.architecture.catalog.type == CatalogType.HIVE
    has_ca = bool(cfg.platform.storage.s3.ca_cert)
    applies = {
        "all": True,
        "ca_cert": has_ca,
        "hive": is_hive,
        "hive+ca_cert": is_hive and has_ca,
    }
    return [(t, what) for t, when, what in _DERIVED_NAMES if applies[when]]


def max_namespace_length(cfg: LakebenchConfig) -> int:
    """Longest namespace this config's catalog and S3 settings accept."""
    limit = K8S_LABEL_MAX
    for template, _ in _applicable_derived_names(cfg):
        limit = min(limit, K8S_LABEL_MAX - len(template.format(ns="")))
    return limit


def derived_name_violations(cfg: LakebenchConfig) -> list[str]:
    """Return one message per derived name that would exceed its limit.

    Checks the namespace itself, every name in ``_DERIVED_NAMES`` the config
    renders, and the three bucket names.
    """
    ns = cfg.get_namespace()
    ns_src = "platform.kubernetes.namespace" if cfg.platform.kubernetes.namespace else "name"
    problems: list[str] = []
    if len(ns) > K8S_LABEL_MAX:
        problems.append(
            f"namespace {ns!r} (from {ns_src}) is {len(ns)} characters; "
            f"Kubernetes allows at most {K8S_LABEL_MAX}"
        )
    for template, what in _applicable_derived_names(cfg):
        derived = template.format(ns=ns)
        if len(derived) > K8S_LABEL_MAX:
            problems.append(
                f"{what} would be {derived!r} ({len(derived)} characters, limit "
                f"{K8S_LABEL_MAX}); shorten the namespace (from {ns_src}) to at "
                f"most {max_namespace_length(cfg)} characters (it is {len(ns)})"
            )
    for layer in ("bronze", "silver", "gold"):
        bucket = getattr(cfg.platform.storage.s3.buckets, layer)
        if len(bucket) > S3_BUCKET_MAX:
            problems.append(
                f"platform.storage.s3.buckets.{layer} {bucket!r} is {len(bucket)} "
                f"characters; S3 allows at most {S3_BUCKET_MAX}"
            )
    return problems


def _normalise_workload_block(block: object) -> object:
    # An empty 'workload:' is None; 'schema_type' is the field name for the
    # documented 'schema' key. Normalise both so the two locations compare.
    if block is None:
        return {}
    if isinstance(block, dict) and "schema_type" in block and "schema" not in block:
        block = dict(block)
        block["schema"] = block.pop("schema_type")
    return block


def _merge_workload_blocks(top: object, nested: object, path: str, conflicts: list[str]) -> object:
    """Merge the top-level and legacy workload blocks, recording disagreements."""
    if isinstance(top, dict) and isinstance(nested, dict):
        merged = dict(nested)
        for key, value in top.items():
            if key in nested:
                merged[key] = _merge_workload_blocks(value, nested[key], f"{path}.{key}", conflicts)
            else:
                merged[key] = value
        return merged
    if top != nested:
        conflicts.append(f"{path}: {top!r} at the top level, {nested!r} under architecture")
    return top


def resolve_workload_location(data: dict[str, Any]) -> dict[str, Any]:
    """Move the workload block to where the model stores it.

    ``workload`` is a top-level config key (D12). The model still stores it
    at ``architecture.workload``, which every reader uses, so this is the one
    place the two locations are reconciled: the top-level block is canonical,
    the old location is accepted with a DeprecationWarning, and when both are
    set any key they disagree on is an error.
    """
    arch = data.get("architecture")
    has_top = "workload" in data
    has_nested = isinstance(arch, dict) and "workload" in arch
    if not has_top and not has_nested:
        return data
    data = dict(data)
    top = _normalise_workload_block(data.pop("workload", None))
    if has_nested:
        assert isinstance(arch, dict)
        arch = dict(arch)
        nested = _normalise_workload_block(arch["workload"])
        msg = (
            "'architecture.workload' is deprecated and will be removed in a future "
            "release; move the block to a top-level 'workload:' key."
        )
        emit_note(msg)
        if has_top:
            conflicts: list[str] = []
            merged = _merge_workload_blocks(top, nested, "workload", conflicts)
            if conflicts:
                raise ValueError(
                    "'workload' is set both at the top level and under 'architecture' "
                    "(deprecated), and they disagree:\n  - "
                    + "\n  - ".join(conflicts)
                    + "\nKeep only the top-level 'workload:' block."
                )
            arch["workload"] = merged
        else:
            arch["workload"] = nested
    else:
        arch = dict(arch) if isinstance(arch, dict) else {}
        arch["workload"] = top
    data["architecture"] = arch
    return data


# Namespaces that hold state shared by every deployment. A deployment may not
# use one: destroy deletes the deployment's namespace.
RESERVED_NAMESPACES: dict[str, str] = {
    "lakebench-observability": "the shared kube-prometheus-stack release",
    "lakebench-system": "the cluster lease",
}


def _is_recipe_shaped(name: str, recipes: Any) -> bool:
    """Whether every slot of *name* names a known component.

    Recipe names are ``<catalog>-<format>-<engine>-<query engine>``. A slot
    is known when a recipe uses it or the schema has that component (so
    ``unity-delta-spark-trino`` is a combination, not a typo). The query
    engine slot spells Spark Thrift as ``thrift``.
    """
    slots: list[set[str]] = [
        {c.value for c in CatalogType},
        {f.value for f in TableFormatType},
        {e.value for e in PipelineEngineType},
        {q.value for q in QueryEngineType if q is not QueryEngineType.SPARK_THRIFT} | {"thrift"},
    ]
    for known in recipes:
        parts = known.split("-", 3)
        if len(parts) == 4:
            for i, part in enumerate(parts):
                slots[i].add(part)
    parts = name.split("-", 3)
    return len(parts) == 4 and all(p in slots[i] for i, p in enumerate(parts))


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

    # Set by load_config: the notes the load collected and how the name was
    # resolved (config/loader.py load_notes, name_resolution).
    _load_notes: Any = PrivateAttr(default=None)
    _name_resolution: Any = PrivateAttr(default=None)

    @model_validator(mode="before")
    @classmethod
    def apply_recipe_defaults(cls, data: object) -> object:
        """Expand recipe defaults into the config dict.

        Recipe defaults are merged via ``_deep_setdefault`` so user-specified
        values always take precedence.
        """
        if not isinstance(data, dict):
            return data
        data = resolve_workload_location(data)
        recipe_name = data.get("recipe")
        if recipe_name:
            from lakebench.config.recipes import RECIPES, _deep_setdefault

            if not isinstance(recipe_name, str):
                raise PydanticCustomError(
                    "unknown_recipe",
                    "{text}",
                    {"text": f"recipe must be one name, not {type(recipe_name).__name__}"},
                )
            defaults = RECIPES.get(recipe_name)
            if not defaults:
                from lakebench.config._hints import nearest

                valid = "valid recipes: " + ", ".join(sorted(RECIPES))
                if _is_recipe_shaped(recipe_name, RECIPES):
                    # Every slot names a real component, so the nearest
                    # spelling would swap a component the user chose (a
                    # Unity recipe offered as Hive): say the combination
                    # is not a recipe instead.
                    detail = f"that combination is not a recipe; {valid}"
                elif near := nearest(recipe_name, RECIPES):
                    detail = f"did you mean '{near}'?"
                else:
                    detail = valid
                # The text goes in through the context, so braces in a
                # user's recipe name are not read as template fields.
                raise PydanticCustomError(
                    "unknown_recipe",
                    "{text}",
                    {"text": f"unknown recipe '{recipe_name}'; {detail}"},
                )
            _deep_setdefault(data, defaults)
        return data

    @property
    def workload(self) -> WorkloadConfig:
        """The workload block (config key ``workload``; stored on architecture)."""
        return self.architecture.workload

    @model_validator(mode="after")
    def validate_required_fields(self) -> LakebenchConfig:
        """Validate required fields are present."""
        if not self.name:
            raise ValueError("'name' is required")
        return self

    @model_validator(mode="after")
    def refuse_reserved_namespace(self) -> LakebenchConfig:
        """Refuse a deployment namespace that holds shared lakebench state.

        Destroy deletes the deployment's namespace. If that namespace were
        the shared observability namespace or the cluster-lock namespace,
        tearing down one deployment would remove what every other deployment
        uses (DESIGN.md invariant 6).
        """
        ns = self.get_namespace()
        if ns in RESERVED_NAMESPACES:
            raise ValueError(
                f"namespace {ns!r} is reserved for shared lakebench state "
                f"({RESERVED_NAMESPACES[ns]}); a deployment cannot use it because "
                "destroy deletes the deployment's namespace. Choose another name "
                "or set platform.kubernetes.namespace."
            )
        return self

    @model_validator(mode="after")
    def default_bucket_names(self) -> LakebenchConfig:
        """Name unset buckets after the deployment: ``<name>-bronze`` and so on.

        The defaults were the fixed names ``lakebench-bronze/-silver/-gold``.
        On a store where bucket names are global (FlashBlade across accounts,
        AWS across everyone) a fixed name is usually taken, and HeadBucket on
        a bucket another account owns returns 403, so the documented quick
        start failed at deploy. Two deployments on one store also shared the
        buckets. Deriving the name from the deployment keeps each
        deployment's buckets its own and matches the name-prefix ownership
        fallback in ``deploy/ownership.py``. Explicit names are kept.
        """
        import re as _re

        buckets = self.platform.storage.s3.buckets
        for layer in ("bronze", "silver", "gold"):
            if layer not in buckets.model_fields_set:
                derived = f"{self.name}-{layer}"
                # Derived names are checked here; explicit ones are the
                # user's (some stores accept names S3 does not).
                if not _re.fullmatch(r"[a-z0-9][a-z0-9.-]*[a-z0-9]", derived):
                    raise ValueError(
                        f"default bucket name {derived!r} (from name {self.name!r}) is "
                        "not a valid S3 bucket name: use lowercase letters, digits, "
                        "'-' and '.', or set platform.storage.s3.buckets explicitly"
                    )
                setattr(buckets, layer, derived)
        return self

    @model_validator(mode="after")
    def validate_derived_name_lengths(self, info: ValidationInfo) -> LakebenchConfig:
        """Refuse a namespace or bucket name that breaks a name derived from it.

        See ``derived_name_violations`` for the objects and limits checked.
        Without this, a namespace a few characters too long deploys up to
        the Hive metastore and then hangs until the readiness timeout
        (LB-153). Teardown and diagnostic commands load with
        ``allow_long_names`` in the validation context (see ``load_config``)
        so a deployment that failed this way can still be torn down.
        """
        if info.context and info.context.get("allow_long_names"):
            return self
        problems = derived_name_violations(self)
        if problems:
            raise ValueError(
                "deployment name/namespace or bucket name too long for a derived "
                "Kubernetes or S3 name:\n  - " + "\n  - ".join(problems)
            )
        return self

    @model_validator(mode="after")
    def resolve_format_versions(self) -> LakebenchConfig:
        """Auto-resolve table format versions based on the Spark image.

        When the user hasn't overridden the format version (or set it to
        ``"auto"``), this picks the best default for the configured Spark
        major.minor.  When the user *has* specified a version, this
        validates it against the compatibility matrix.
        """
        from lakebench.spark.job import resolve_format_version, validate_format_version

        spark_image = self.images.spark
        fmt = self.architecture.table_format

        if fmt.type == TableFormatType.ICEBERG:
            # A version the user chose is validated as written; an unset
            # version or 'auto' may fall back below.
            user_chose = "version" in fmt.iceberg.model_fields_set and fmt.iceberg.version not in (
                "",
                "auto",
            )
            resolved = resolve_format_version(
                spark_image,
                "iceberg",
                fmt.iceberg.version,
            )
            # Also refuses Iceberg 1.11+ on a Java 11 Spark image, which would
            # otherwise fail inside the driver with UnsupportedClassVersionError.
            # When the user did not choose a version, pick the newest release
            # built for Java 11 instead of refusing the image.
            try:
                validate_format_version(spark_image, "iceberg", resolved)
            except ValueError:
                if user_chose:
                    raise
                fallback = "1.10.1"
                validate_format_version(spark_image, "iceberg", fallback)
                logger.warning(
                    "Spark image %s ships Java 11 and Iceberg %s needs Java 17; using "
                    "Iceberg %s. Set table_format.iceberg.version, or use a java17 image "
                    "tag, to choose explicitly.",
                    spark_image,
                    resolved,
                    fallback,
                )
                resolved = fallback
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
