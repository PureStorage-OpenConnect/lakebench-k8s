"""Pydantic models for Lakebench configuration.

This module defines the complete configuration schema for Lakebench,
matching the specification in lakebench-spec.md Section 4.
"""

from __future__ import annotations

import dataclasses
import logging
import re
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

from ._load_context import CHANGES_DATA, LoadPurpose, emit_note, purpose_from_context

logger = logging.getLogger(__name__)


def _matches_old_default(value: object, default: object) -> bool:
    """Whether *value* (as written in YAML) is the old *default*.

    A mapping matches when every key it sets matches the default's (keys it
    leaves out keep the default); a list matches element by element; a
    callable default is a predicate on the value.
    """
    if callable(default):
        return bool(default(value))
    if isinstance(default, dict):
        if value is None:  # an empty mapping in YAML ("thrift:")
            return True
        return isinstance(value, dict) and all(
            k in default and _matches_old_default(v, default[k]) for k, v in value.items()
        )
    if isinstance(default, list):
        return (
            isinstance(value, list)
            and len(value) == len(default)
            and all(_matches_old_default(v, d) for v, d in zip(value, default, strict=True))
        )
    if isinstance(default, bool) or isinstance(value, bool):
        return value is default
    return bool(value == default)


class ConfigModel(BaseModel):
    """Base for every user-facing config model: unknown keys are errors.

    A misspelt or misplaced key used to be dropped silently, so the run went
    ahead on the default and the user never learned their setting was
    ignored. Deprecated spellings stay accepted because each is migrated by a
    ``mode="before"`` validator, which runs before the extra-key check.
    """

    # hide_input_in_errors: a validation error must not echo the input it
    # failed on. A model-level error carries the whole block it validated,
    # which can hold a datagen seed or a credential, and the CLI prints the
    # error text. Pydantic takes the setting from the model validation
    # starts at (LakebenchConfig for every config load), which inherits it.
    # A string literal right after a field is its description
    # (scripts/gen_config_reference.py writes docs/configuration.md from it).
    model_config = ConfigDict(
        extra="forbid", hide_input_in_errors=True, use_attribute_docstrings=True
    )

    # Keys that were once valid and now do nothing. Maps key -> what to do
    # instead. A load for a command that changes data (LoadPurpose MUTATE or
    # RUN) refuses them with that text; the teardown and read commands drop
    # them with a note, so an old config can still be destroyed, inspected
    # and converted. A model built without a purpose drops them with a
    # DeprecationWarning, as before v1.7.
    _removed_keys: ClassVar[dict[str, str]] = {}

    # Removed keys whose old default changed nothing, mapped to that default
    # (or a predicate on the value). A key still at that value is inert, the
    # same as absent (v1.6 examples, docs, ledger configs and save_config
    # carry such keys), so it is dropped with a note under every purpose; any
    # other value takes the _removed_keys path.
    _removed_defaults: ClassVar[dict[str, Any]] = {}

    @model_validator(mode="before")
    @classmethod
    def _drop_removed_keys(cls, data: object, info: ValidationInfo) -> object:
        if not isinstance(data, dict) or not cls._removed_keys:
            return data
        present = [k for k in cls._removed_keys if k in data]
        if not present:
            return data
        at_default = [
            k
            for k in present
            if k in cls._removed_defaults
            and _matches_old_default(data[k], cls._removed_defaults[k])
        ]
        if at_default:
            data = dict(data)
            for key in at_default:
                data.pop(key)
                emit_note(
                    f"'{key}' ({cls.__name__}) is no longer used and is ignored (it carried "
                    f"its old default): {cls._removed_keys[key]} Delete it from the config.",
                    kind="removed",
                )
            present = [k for k in present if k not in at_default]
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
                f"'{key}' ({cls.__name__}) is no longer used and is ignored: "
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


#: Query engines that run the investigator sessions (AML continuous).
INVESTIGATOR_QUERY_ENGINES = ("trino", "spark-thrift")


def investigator_sessions_problem(
    arch: Any, run_mode: str | None = None, *, check_mode: bool = True
) -> str | None:
    """Why ``architecture.benchmark.investigator_sessions`` cannot run, or None
    (unset, or every condition holds). The one predicate for the load-time
    refusal (``check_mode=False``: workload, TM operations and engine) and
    ``run``'s (``cli._run_args.RUN_RULES``: also the run's resolved mode).
    *run_mode* is ``batch`` or ``continuous``; None reads the config's
    pipeline mode. The text names every condition that fails."""
    n = arch.benchmark.investigator_sessions
    if n is None:
        return None
    workload = arch.workload
    continuous = (
        run_mode == "continuous" if run_mode is not None else is_continuous_mode(arch.pipeline.mode)
    )
    missing: list[str] = []
    if workload.schema_type.value != "financial":
        missing.append(f"the workload is {workload.schema_type.value}, not financial")
    if check_mode and not continuous:
        missing.append("the run is batch, not continuous")
    if workload.schema_type.value == "financial" and not workload.tm_operations.enabled:
        missing.append("workload.tm_operations.enabled is false")
    engine = arch.query_engine.type.value
    if engine not in INVESTIGATOR_QUERY_ENGINES:
        missing.append(f"the query engine is {engine}, not trino or spark-thrift")
    if not missing:
        return None
    return (
        f"architecture.benchmark.investigator_sessions ({n}) runs only on an AML continuous run "
        "with TM operations on a trino or spark-thrift query engine: " + "; ".join(missing)
    )


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
        "hive": (
            f"the Stackable HiveCluster always runs Hive {STACKABLE_HIVE_VERSION} "
            "(Hive 4 breaks Iceberg and Trino ANALYZE), whatever images.hive named, and run "
            f"provenance records {STACKABLE_HIVE_VERSION}."
        ),
        "prometheus": (
            "Prometheus deploys from the kube-prometheus-stack chart; pin it with "
            "observability.chart_version."
        ),
        "grafana": (
            "Grafana deploys from the kube-prometheus-stack chart; pin it with "
            "observability.chart_version."
        ),
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {
        # Any value naming the Hive that runs changed nothing.
        "hive": lambda v: isinstance(v, str) and _hive_version_of(v) == STACKABLE_HIVE_VERSION,
        "prometheus": "prom/prometheus:v2.48.0",
        "grafana": "grafana/grafana:10.2.0",
    }

    # Immutable tag = the datagen_rs commit it was built from. Bump it with
    # every datagen_rs change; :latest drifted from the code it claimed to be.
    # b6f2905 is the datagen v1.6 sprint tip on integrate (2026-09-28) with
    # the Python base bumped from the floating python:3.13-slim tag to
    # python:3.14-slim, digest-pinned so a rebuild reproduces the same
    # Python layer bytes. Rust source unchanged from 25f1aa8, so corpus
    # bytes at a fixed seed are unchanged; MODEL_VERSION stays
    # datagen-v2-rs-0.3. Datagen_rs freeze wiring carried forward from
    # 25f1aa8: dirty-ratio semantics, screening rates in prereg
    # 3.6.1, calibration-replicate seed refusal in perturbation_for_seed,
    # multi-writer guard on --mode reference, S3Sink::put_multipart retry,
    # --delivery-mode {batch|continuous} MpuWriter switch with continuous
    # as the Rust binary default, and the #52 registered_looks_open freeze
    # wiring.
    # 30603b1 re-fits the datagen peak-RSS memory model: the autosizer
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
    # e14d0fd: mimalloc allocator, typology payloads pruned to each
    # pod's own files, world columns recomputed on demand, fixed 64 MB files,
    # re-fit memory model with a 16Gi pod cap, delivery-mode forwarding.
    # AML pod peak at scale 100 fell from 18.18 to 5.70 GiB. Output-neutral,
    # PROVEN on the pushed image: seed-43 --mode all byte-compare against the
    # frozen generator (rebuilt from 9382420 source) -- 73/73 objects at 128 MB,
    # 141/141 at 64 MB, 141/141 with 4 pods, 141/141 thread-throttled. MODEL_VERSION
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
    #   30603b1 (sha256:608425f46ed0f211f7eff1e63b0713a76835ad57e4cd1828252cc28fc776ea16) memory refit
    #   9382420 (sha256:2faad1cc0252a165a56361a06f159a62ba7c4387c83adfb7c46fe260af23b8f2) live-metrics Pushgateway push
    #   b6f2905 (sha256:312f9ecfa301b09f696cd04f9b6d44052656041fc293d9d76f0adddd01fbd4f6)
    #   25f1aa8 (sha256:8dbc2705c6d95dbc3a259b3d9e3007e5cd951db3df2655afc66d357fd1fed5f7)
    #   0a83acd (sha256:acdf3925...)
    #   7c24641 (sha256:c5a6bc80...)
    # 1.6.0: the v1.6 release image, rebuilt from the same datagen_rs source as
    # 034f998 (datagen_rs unchanged 034f998..release; base images digest-pinned,
    # cargo --locked) after the docker.io repository was wiped. Release
    # tags are the exception to the commit-tag rule above.
    # Pushed digest (1.6.0): sha256:5fda9025fb9b455b390e1138d82e9f6ef16d214dfa9419815be0111d2f6fce0a
    # 2a36ae21: the v1.7 release image and v1.7 look image
    # (sha256:0502b700299948f43bb1b999d7ba29262a509306658b4e5f7c48738f88d31f04);
    # deleted from docker.io with the repository on 2026-10-06.
    # 3f4729b6 (sha256:7fbb35f1a94f5aea11a135e93cadbb969a97a37d04c9ce2d227fd7db266083cb):
    # adds continuous delivery (--deliver-until); batch output is
    # byte-identical to 4274bb67 on the nine A/B cases. Never released.
    # 3cb67f92: the 1.7.1 image, tagged by the git tree of datagen_rs/ it was
    # built from (`git rev-parse <commit>:datagen_rs` starts with it; pushed
    # 2026-10-07). Builds the AML world once per pod instead of every
    # continuous epoch and checks for a stop before an epoch's manifest;
    # its output is byte-identical to 3f4729b6 (AML continuous and batch
    # locally, an AML scale-10 batch corpus on the cluster from an earlier
    # push of this tag built from the same context). The default
    # names the tag and the digest; the runtime pulls the digest, so a
    # re-push of the tag cannot move it.
    # 5d7ce61a (datagen_rs tree 5d7ce61aa0c4, pushed 2026-10-09):
    # datagen-v2-rs-0.4. Surnames are synthetic at every scale and the surname
    # and company-head pools grow in proportion to the population, so
    # screening namesakes per watchlist entry stay flat across scale. Every
    # AML corpus differs from 3cb67f92's, scale 1 included; the Customer 360
    # generator is unchanged.
    datagen: str = (
        "docker.io/sillidata/lb-datagen:5d7ce61a"
        "@sha256:ed4057e097f09fdd3e37631bc37eb88e5fce561cb8ebe06cd6fa2fd7d23e4bfc"
    )
    """Data generator image, pinned by tag and digest (the digest is what is pulled).
    In continuous mode it generates until the run window ends: AML as successive
    24-month periods of the same bank, Customer360 as successive time slices.
    """
    spark: str = "apache/spark:4.1.1-python3"
    """Spark runtime image. Unset: the image of the config's recipe (or of the recipe its
    components name): `4.1.1-python3` on the Hive recipes, `4.0.2-python3` on the Polaris
    recipes, `hive-delta-spark-thrift` and `hive-delta-spark-none`; 4.0.2 also when the
    config writes a table format version Spark 4.1 cannot run (Delta 4.0.0).
    """
    postgres: str = "postgres:17"  # Tested with 16, 17, 18
    """PostgreSQL image (metadata backend)."""
    polaris: str = "apache/polaris:1.6.0"
    """Apache Polaris REST catalog image."""
    polaris_admin_tool: str = "apache/polaris-admin-tool:1.6.0"
    """Polaris admin tool image; bootstraps the Polaris metastore on a Polaris recipe."""
    unity: str = (
        "unitycatalog/unitycatalog:main"  # OSS Unity has no version tags; :main tracks 0.4.x
    )
    """Unity Catalog server image (Unity catalog only; no recipe uses it)."""
    trino: str = "trinodb/trino:483"
    """Trino query engine image."""
    duckdb: str = "python:3.11-slim"
    """Python image the DuckDB query engine pod runs in; DuckDB itself is pinned by
    `architecture.query_engine.duckdb.version`.
    """
    # Bitnami publishes only `latest` for this image now (versioned tags
    # moved to bitnamilegacy), so the default names it by digest: a re-push of
    # `latest` cannot change what a deployment runs (checked 2026-10-09).
    jmx_exporter: str = (
        "bitnami/jmx-exporter:latest"
        "@sha256:873527b34b55ca7b8f0b5f7efdf18c93e4337d06709da674ba7284fd3f6316de"
    )
    """JMX exporter image for the metrics sidecars, used when observability is enabled."""

    pull_policy: ImagePullPolicy = ImagePullPolicy.ALWAYS
    """`Always`, `IfNotPresent`, or `Never`."""

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
    """kubectl context name from the kubeconfig (`$KUBECONFIG`, else `~/.kube/config`). Empty =
    the kubeconfig's current context (`kubectl config current-context`; with several files
    in `$KUBECONFIG`, the first file that sets one), resolved by name at the command's first
    cluster call, API or `kubectl`/`helm`/`oc`; every later call in that `lakebench` process
    uses that context, even if the current context changes while it runs. The command stops
    at its next client load or tool call if the context's API server or CA changes in the
    kubeconfig. A name that is not in the kubeconfig is refused. In-cluster credentials are
    used only when no kubeconfig file exists. Set this to target a specific cluster when you
    have multiple contexts configured.
    """
    namespace: str = ""  # Empty = use default namespace
    """Kubernetes namespace for all resources. Empty = use the deployment `name`."""
    create_namespace: bool = True
    """Create the namespace if it does not exist."""


class S3BucketsConfig(ConfigModel):
    """S3 bucket names for each data layer.

    These defaults apply only to a bare ``S3BucketsConfig``. In a full config
    ``LakebenchConfig.default_bucket_names`` replaces every name the user did
    not set with ``<deployment name>-<layer>``.
    """

    bronze: str = "lakebench-bronze"
    """Bronze layer S3 bucket name. Unset, it is derived from the deployment `name`."""
    silver: str = "lakebench-silver"
    """Silver layer S3 bucket name. Unset, it is derived from the deployment `name`."""
    gold: str = "lakebench-gold"
    """Gold layer S3 bucket name. Unset, it is derived from the deployment `name`."""


class S3Config(ConfigModel):
    """S3/object storage configuration."""

    endpoint: str = Field(
        default="",
        description=(
            "S3-compatible endpoint URL (e.g., `http://minio:9000` or "
            "`https://s3.example.com:443`). HTTPS endpoints with self-signed CAs require "
            "`ca_cert`."
        ),
    )
    region: str = "us-east-1"
    """AWS region. Used by boto3 for signing."""
    path_style: bool = True  # Required for FlashBlade, MinIO
    """Path-style access (`true` for FlashBlade/MinIO, `false` for AWS S3). Spark and the datagen
    pods both follow it; with `false` requests go to `<bucket>.<endpoint host>`."""

    # Credentials. Only the inline keys are used: deploy writes them into the
    # lakebench-s3-credentials Secret and the CLI's S3 client reads them.
    access_key: str = ""
    """S3 access key. Required for deploy."""
    secret_key: str = ""
    """S3 secret key. Required for deploy."""

    # TLS / HTTPS support
    ca_cert: str = Field(
        default="",
        description=(
            "Path to a PEM CA certificate bundle for HTTPS endpoints with self-signed or private "
            "CAs. Empty = use system default CAs. The PEM content is read at deploy time and "
            "embedded into a Kubernetes Secret for all components."
        ),
    )
    verify_ssl: bool = Field(
        default=True,
        description=(
            "Verify SSL certificates for HTTPS endpoints. Set `false` only for development with "
            "self-signed certs when you don't have the CA certificate file."
        ),
    )

    buckets: S3BucketsConfig = Field(default_factory=S3BucketsConfig)
    """Bronze, silver and gold bucket names."""
    create_buckets: bool = True
    """Create buckets if they do not exist."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "secret_ref": (
            "lakebench never reads an existing Secret: deploy writes the S3 Secret from "
            "access_key and secret_key. Set those instead (use ${VAR} substitution, for "
            'example access_key: "${S3_ACCESS_KEY}", to keep the keys out of the file).'
        ),
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {"secret_ref": ""}


class ScratchStorageConfig(ConfigModel):
    """Scratch storage configuration for Spark shuffle.

    The StorageClass is Category 2 shared infrastructure: lakebench uses
    it, lakebench does not create or destroy it. ``deploy`` verifies the
    named StorageClass exists at preflight; a cluster admin installs it
    once with ``lakebench admin install --component scratch-storage-class``,
    which reads ``provisioner`` and ``parameters``.
    """

    _removed_keys: ClassVar[dict[str, str]] = {
        "create_storage_class": (
            "lakebench no longer creates StorageClasses; a cluster admin runs "
            "'lakebench admin install --component scratch-storage-class' once."
        ),
        # Nothing sized a PVC from it: each executor's scratch volume is the
        # job profile's scratch_size (modules/pipeline_engines/spark/job.py).
        "size": "per-job scratch comes from the job profiles (silver-build: 60 GiB x scale / executors, 50-300Gi).",
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {"size": "100Gi"}

    enabled: bool = False
    """Enable scratch StorageClass for Spark PVCs. Unset, a batch run at scale 50 and above
    turns it on (Spark shuffle there outgrows pod ephemeral storage)."""
    storage_class: str = "px-csi-scratch"
    """StorageClass name for scratch volumes."""
    provisioner: str = "pxd.portworx.com"
    """CSI provisioner for the StorageClass. Use `rancher.io/local-path`, `ebs.csi.aws.com`,
    etc. for non-Portworx providers.
    """
    parameters: dict[str, str] = Field(
        default_factory=lambda: {"repl": "1", "io_profile": "auto", "priority_io": "high"}
    )
    """Provider-specific StorageClass parameters."""


class StorageConfig(ConfigModel):
    """Storage configuration including S3 and scratch volumes."""

    s3: S3Config = Field(default_factory=S3Config)
    """S3-compatible object storage: endpoint, credentials and buckets."""
    scratch: ScratchStorageConfig = Field(default_factory=ScratchStorageConfig)
    """Scratch storage for Spark shuffle."""


def _refuse_operator_install(model: ConfigModel, info: ValidationInfo, key: str, fix: str) -> None:
    """``operator.install: true`` is refused by the commands that change data.

    Shared operators are cluster infrastructure: a deployment verifies them
    and never installs them. A command that changes data refuses ``true``
    with *fix*; teardown and read commands load it as ``false`` with a note,
    so an old config can still be destroyed and inspected. ``false`` loads
    as before. A model built without a purpose (not through ``load_config``)
    keeps the value. Checked after coercion, so ``"true"``, ``1`` and a
    ``${VAR}`` that resolves to ``yes`` count as true.
    """
    if not getattr(model, "install", False):
        return
    purpose = purpose_from_context(info.context)
    if purpose is None:
        return
    if purpose in CHANGES_DATA:
        raise ValueError(
            f"'{key}: true' is refused: {fix} Delete the key or set it to false "
            "(destroy, status and the read-only commands still load it)"
        )
    object.__setattr__(model, "install", False)
    emit_note(
        f"'{key}: true' is ignored: {fix} Commands that change data refuse it.",
        kind="removed",
    )


#: Keys that v1.6 read as "deploy installs this shared operator", and the
#: ``admin install`` component that does it now (see _refuse_operator_install).
#: A config translator drops both keys by this table.
OPERATOR_INSTALL_KEYS: dict[str, str] = {
    "platform.compute.spark.operator.install": "spark-operator",
    "architecture.catalog.hive.operator.install": "stackable",
}


def operator_install_fix(key: str) -> str:
    """What to do instead of ``<key>: true``."""
    component = OPERATOR_INSTALL_KEYS[key]
    return (
        "deploy never installs shared operators, because they serve every deployment on "
        "the cluster; a cluster admin installs them once with 'lakebench admin install "
        f"--component {component} <config>'."
    )


_SPARK_OPERATOR_INSTALL_FIX = operator_install_fix("platform.compute.spark.operator.install")
_STACKABLE_OPERATOR_INSTALL_FIX = operator_install_fix("architecture.catalog.hive.operator.install")


class SparkOperatorConfig(ConfigModel):
    """Where the shared Spark Operator runs and the chart a fresh install uses.

    ``deploy`` never installs it; ``lakebench admin install --component
    spark-operator`` does, at ``version`` when it is not installed. An
    installed operator keeps its version whatever this says.
    """

    #: Refused when true (see OPERATOR_INSTALL_KEYS); kept so ``false`` loads.
    install: bool = False
    """Refused when `true`: deploy never installs the shared operator; a cluster admin runs
    `lakebench admin install --component spark-operator`. `false` loads as before.
    """
    namespace: str = "spark-operator"
    """Namespace for the Spark Operator."""
    version: str = "2.5.1"  # webhook volume injection gap (gotcha 3) unchanged from 2.4.0; template workaround stays
    """Chart version a fresh `admin install` uses. v2.x required. An installed operator keeps
    its version."""

    @model_validator(mode="after")
    def _refuse_install(self, info: ValidationInfo) -> SparkOperatorConfig:
        if self.install:
            _refuse_operator_install(
                self, info, "platform.compute.spark.operator.install", _SPARK_OPERATOR_INSTALL_FIX
            )
        return self


# Spark's size grammar (JavaUtils.byteStringAsBytes): a whole number and a
# unit, with an optional "b". Fractions and Kubernetes units (Gi) fail in
# Spark; a bare number (MiB to Spark) and 0 are refused here too, so the
# capacity check never misreads one.
_SPARK_MEMORY_SIZE = re.compile(r"[1-9][0-9]*[kmgt]b?", re.IGNORECASE)


#: The largest per-job executor override: the proven executor ceiling (32
#: and more cause Kubernetes API polling storms). Held equal to the job
#: module's ``_MAX_EXECUTORS_SAFE`` by a test.
MAX_EXECUTOR_OVERRIDE = 28
#: The largest driver_cores override.
MAX_DRIVER_CORES = 16


class SparkComputeConfig(ConfigModel):
    """Spark compute configuration."""

    # Per-executor and driver sizing come from the job profiles
    # (_JOB_PROFILES in modules/pipeline_engines/spark/job.py). These blocks
    # were recorded and graded but sized nothing.
    _removed_keys: ClassVar[dict[str, str]] = {
        "driver": (
            "per-executor sizing is fixed in the job profiles; use "
            "platform.compute.spark.<job>_executors for counts and "
            "driver_memory/driver_cores for the driver."
        ),
        "executor": (
            "per-executor sizing is fixed in the job profiles; use "
            "platform.compute.spark.<job>_executors for counts and "
            "driver_memory/driver_cores for the driver."
        ),
    }
    # The v1.6 defaults, which v1.6 save_config wrote into every config.
    _removed_defaults: ClassVar[dict[str, Any]] = {
        "driver": {"cores": 4, "memory": "8g"},
        "executor": {"instances": 8, "cores": 4, "memory": "48g", "memory_overhead": "12g"},
    }

    operator: SparkOperatorConfig = Field(default_factory=SparkOperatorConfig)
    """The shared Kubeflow Spark Operator that runs the stage jobs."""

    # Per-job executor count overrides (None = auto from scale).
    # When set, these override the auto-scaled executor count for that job.
    # Per-executor sizing (cores, memory, PVC) remains fixed from proven profiles.
    bronze_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override bronze-verify executor count (1--28). Null = auto from scale.",
    )
    silver_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override silver-build executor count (1--28). Null = auto from scale.",
    )
    gold_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override gold-finalize executor count (1--28). Null = auto from scale.",
    )

    # Streaming job executor count overrides (None = auto from scale).
    bronze_ingest_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override bronze-ingest executor count (1--28). Null = auto from scale.",
    )
    silver_stream_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override silver-stream executor count (1--28). Null = auto from scale.",
    )
    gold_refresh_executors: int | None = Field(
        default=None,
        ge=1,
        description="Override gold-refresh executor count (1--28). Null = auto from scale.",
    )
    # Streaming executor size overrides (None = the profile's, grown to 8 or 16
    # cores when the offered load needs more cores than the executor cap).
    bronze_ingest_executor_cores: int | None = Field(
        default=None,
        ge=1,
        le=16,
        description="Override bronze-ingest cores per executor (1--16). Memory and scratch follow the profile's per-core share; the executor count is unchanged, so total cores grow with the size. Null = auto: the profile's, grown to 8 or 16 cores when the offered load needs more executors than the cap, unless `bronze_ingest_executors` is set.",
    )
    silver_stream_executor_cores: int | None = Field(
        default=None,
        ge=1,
        le=16,
        description="Override silver-stream cores per executor (1--16). Memory and scratch follow the profile's per-core share; the executor count is unchanged, so total cores grow with the size. Null = auto: the profile's, grown to 8 or 16 cores when the offered load needs more executors than the cap, unless `silver_stream_executors` is set.",
    )
    gold_refresh_executor_cores: int | None = Field(
        default=None,
        ge=1,
        le=16,
        description="Override gold-refresh cores per executor (1--16). Memory and scratch follow the profile's per-core share; the executor count is unchanged, so total cores grow with the size. Null = the profile's; gold-refresh is never grown automatically.",
    )

    # Driver resource overrides (None = use profile defaults).
    # These are global - they apply to all Spark jobs. Use when cluster nodes
    # have limited memory or when running at extreme scales (500+).
    driver_memory: str | None = Field(
        default=None,
        description=("Global driver memory override (e.g., `16g`). Null = profile default."),
    )
    driver_cores: int | None = Field(
        default=None,
        ge=1,
        description="Override driver cores (1--16). Null = profile default (typically 4).",
    )

    @field_validator("driver_memory", mode="after")
    @classmethod
    def _driver_memory_is_a_spark_size(cls, value: str | None, info: ValidationInfo) -> str | None:
        """A driver memory Spark cannot read fails the job at submit, and the
        capacity check cannot count it: commands that change data refuse it;
        teardown and read commands drop it with a note."""
        if value is None or _SPARK_MEMORY_SIZE.fullmatch(value.strip()):
            return value
        text = (
            f"platform.compute.spark.driver_memory {value!r} is not a Spark memory size "
            "Lakebench can count: write a whole number above 0 with a unit, k, m, g or t "
            "(for example 16g)"
        )
        purpose = purpose_from_context(info.context)
        if purpose is None or purpose in CHANGES_DATA:
            raise ValueError(text)
        emit_note(text + "; ignored. Commands that change data refuse it.", kind="removed")
        return None

    @field_validator(
        "bronze_executors",
        "silver_executors",
        "gold_executors",
        "bronze_ingest_executors",
        "silver_stream_executors",
        "gold_refresh_executors",
        "driver_cores",
        mode="after",
    )
    @classmethod
    def _override_within_bounds(cls, value: int | None, info: ValidationInfo) -> int | None:
        """An executor count above the proven ceiling (``MAX_EXECUTOR_OVERRIDE``,
        the job module's 28) or a driver above 16 cores is refused by the
        commands that change data; teardown and read commands drop it with a
        note, so an old config can still be destroyed."""
        limit = MAX_DRIVER_CORES if info.field_name == "driver_cores" else MAX_EXECUTOR_OVERRIDE
        if value is None or value <= limit:
            return value
        what = "driver cores" if info.field_name == "driver_cores" else "executors"
        text = (
            f"platform.compute.spark.{info.field_name}: {value} {what} is above the "
            f"proven ceiling of {limit}; set {limit} or less, or leave it unset for "
            "the scale-derived count"
        )
        purpose = purpose_from_context(info.context)
        if purpose is None or purpose in CHANGES_DATA:
            raise ValueError(text)
        emit_note(text + ". Ignored; commands that change data refuse it.", kind="removed")
        return None


class PostgresConfig(ConfigModel):
    """PostgreSQL configuration for metadata backend."""

    storage: str = "10Gi"
    """PVC size for PostgreSQL data."""
    storage_class: str = ""  # Empty = default storage class
    """StorageClass for PostgreSQL PVC. Empty = cluster default StorageClass (requires one to
    exist -- see [Prerequisites](getting-started.md#default-storageclass)).
    """


class ComputeConfig(ConfigModel):
    """Compute resource configuration."""

    spark: SparkComputeConfig = Field(default_factory=SparkComputeConfig)
    """Spark executor counts per job, driver resources and the Spark Operator."""
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    """PostgreSQL metadata backend."""


_DNS1123_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")


def _is_dns1123_subdomain(name: str) -> bool:
    return len(name) <= 253 and all(_DNS1123_LABEL.match(p) for p in name.split("."))


class DepsConfig(ConfigModel):
    """The deployment's dependency server, ``lb-deps``.

    Every key is optional. The three URL keys replace the public sources the
    ``lb-deps`` resolve reads, for clusters without egress to them. Each one
    enters the request hash, so changing it re-resolves the set at the next
    deploy; a mirror that serves the same bytes gives the same set hash.
    Mirrors are read anonymously, over plain HTTP or over HTTPS with a
    publicly trusted certificate.
    """

    maven_repository: str = Field(
        default="",
        description=(
            "The only Maven repository the resolve reads; replaces Maven Central and the Google "
            "mirror. An `http://` or `https://` base URL without credentials, query or fragment; "
            "stored with one trailing `/`."
        ),
    )
    pypi_index: str = Field(
        default="",
        description=(
            "PyPI simple index for the AML reference wheels and the DuckDB wheel. Empty = "
            "`https://pypi.org/simple/`. A plain-HTTP index is passed to pip as a trusted host."
        ),
    )
    duckdb_extension_repository: str = Field(
        default="",
        description=("DuckDB extension repository. Empty = `http://extensions.duckdb.org`."),
    )
    storage_class: str = Field(
        default="",
        description=(
            "StorageClass of the `lb-deps-data` PVC (5Gi, ReadWriteOnce). Empty = the cluster "
            "default StorageClass. Read only when the PVC is created; an existing PVC is never "
            "changed, so delete it to move the set. The volume must be writable by UID 185 "
            "through `fsGroup`."
        ),
    )

    @field_validator("maven_repository", "pypi_index", "duckdb_extension_repository")
    @classmethod
    def _mirror_url(cls, value: str, info: ValidationInfo) -> str:
        from urllib.parse import urlsplit

        url = value.strip()
        if not url:
            return ""
        key = f"platform.deps.{info.field_name}"
        if not url.isascii() or any(not c.isprintable() for c in url):
            raise ValueError(f"{key} must be a plain ASCII URL, not {url!r}")
        try:
            parts = urlsplit(url)
            host = parts.hostname
            _ = parts.port  # raises on a port that is not a number in range
            has_userinfo = parts.username is not None or parts.password is not None
        except ValueError as e:
            raise ValueError(f"{key} is not a URL: {url!r} ({e})") from e
        scheme = parts.scheme.lower()
        if scheme not in ("http", "https") or not host:
            raise ValueError(f"{key} must be an http:// or https:// URL with a host, not {url!r}")
        if has_userinfo:
            # It would be written into the lb-deps request ConfigMap and the
            # resolve logs. Mirror credentials are not supported.
            raise ValueError(
                f"{key} must not carry credentials (user:password@); mirrors are read anonymously"
            )
        if parts.query or parts.fragment or any(c.isspace() for c in url):
            raise ValueError(f"{key} must be a plain base URL, without a query or fragment")
        # One spelling per mirror (lowercase scheme and host, one trailing
        # slash or none), so a spelling alone does not change the request
        # hash and re-resolve the set.
        url = parts._replace(scheme=scheme, netloc=parts.netloc.lower()).geturl()
        if info.field_name == "duckdb_extension_repository":
            return url.rstrip("/")
        return url.rstrip("/") + "/"

    @field_validator("storage_class")
    @classmethod
    def _storage_class_name(cls, value: str) -> str:
        name = value.strip()
        if name and not _is_dns1123_subdomain(name):
            raise ValueError(
                "platform.deps.storage_class must be a StorageClass name "
                f"(a lowercase DNS subdomain), not {name!r}"
            )
        return name


class PlatformConfig(ConfigModel):
    """Layer 1: Platform configuration."""

    kubernetes: KubernetesConfig = Field(default_factory=KubernetesConfig)
    """Kubernetes context and namespace."""
    storage: StorageConfig = Field(default_factory=StorageConfig)
    """Object storage and scratch storage."""
    compute: ComputeConfig = Field(default_factory=ComputeConfig)
    """Spark and PostgreSQL compute settings."""
    deps: DepsConfig = Field(default_factory=DepsConfig)
    """The deployment's dependency server, `lb-deps`."""


# =============================================================================
# Data Architecture Configuration (Layer 2)
# =============================================================================


class HiveResourcesConfig(ConfigModel):
    """Hive Metastore resource configuration."""

    cpu_min: str = "500m"
    """Hive Metastore minimum CPU request."""
    cpu_max: str = "2"
    """Hive Metastore CPU limit."""
    memory: str = "4Gi"
    """Hive Metastore memory."""


class StackableOperatorConfig(ConfigModel):
    """Where the shared Stackable operators run and the SDP version a fresh
    install uses (``lakebench admin install --component stackable``)."""

    #: Refused when true (see OPERATOR_INSTALL_KEYS); kept so ``false`` loads.
    install: bool = False
    """Refused when `true`: deploy never installs the Stackable operators; a cluster admin runs
    `lakebench admin install --component stackable`. `false` loads as before.
    """
    namespace: str = "stackable"
    """Namespace for Stackable operators."""
    version: str = "25.7.0"
    """Stackable SDP chart version a fresh `admin install` uses."""

    @model_validator(mode="after")
    def _refuse_install(self, info: ValidationInfo) -> StackableOperatorConfig:
        if self.install:
            _refuse_operator_install(
                self,
                info,
                "architecture.catalog.hive.operator.install",
                _STACKABLE_OPERATOR_INSTALL_FIX,
            )
        return self


class HiveConfig(ConfigModel):
    """Hive Metastore configuration."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "thrift": (
            "the HiveCluster template sets hive.metastore.server.min.threads 10, "
            "max.threads 50 and hive.metastore.client.socket.timeout 300s."
        ),
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {
        "thrift": {"min_threads": 10, "max_threads": 50, "client_timeout": "300s"},
    }

    operator: StackableOperatorConfig = Field(default_factory=StackableOperatorConfig)
    """Where the shared Stackable operators run and the SDP version a fresh install uses."""
    resources: HiveResourcesConfig = Field(default_factory=HiveResourcesConfig)
    """Hive Metastore CPU and memory."""


class PolarisResourcesConfig(ConfigModel):
    """Polaris resource configuration."""

    cpu: str = "1"
    """Catalog server CPU request/limit."""
    memory: str = "2Gi"
    """Catalog server memory."""


class PolarisConfig(ConfigModel):
    """Apache Polaris REST catalog configuration.

    Polaris is an open-source Iceberg REST catalog (port 8181).
    Uses relational-jdbc persistence backed by the shared PostgreSQL.
    On FlashBlade: stsUnavailable=true, pathStyleAccess=true.

    ``client_secret`` is optional. Empty: ``deploy`` generates one
    per deployment, once, into the Secret ``lakebench-polaris-client``, and
    ``run``, ``benchmark`` and ``destroy`` read it back from there, so
    separate invocations agree (a value generated at config load would
    differ per invocation). Set: it is used as is and stored in that Secret.
    """

    port: int = Field(default=8181, ge=1, le=65535)
    """Polaris REST API port."""
    client_secret: str = ""
    """OAuth2 secret of the `lakebench` Polaris client. Empty = deploy generates one
    and keeps it in the Secret `lakebench-polaris-client`; a value is written there
    on the first deploy. Use a `${VAR}` reference rather than a literal.
    """
    resources: PolarisResourcesConfig = Field(default_factory=PolarisResourcesConfig)
    """Polaris server CPU and memory."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "version": "the Polaris that runs, and is recorded, is the tag of images.polaris.",
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {"version": "1.6.0"}


class UnityConfig(ConfigModel):
    """Unity Catalog configuration.

    OSS Unity Catalog is a self-hosted REST catalog server (Apache-licensed).
    Uses PostgreSQL for persistence, similar to Polaris.
    """

    spark_connector_version: str = "0.4.0"
    """Unity Catalog Spark connector version the Spark jobs load."""
    port: int = Field(default=8080, ge=1, le=65535)
    """Unity Catalog REST API port."""
    resources: PolarisResourcesConfig = Field(default_factory=PolarisResourcesConfig)
    """Unity Catalog server CPU and memory."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "version": "the Unity Catalog that runs is the tag of images.unity.",
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {"version": "0.4.0"}


class CatalogConfig(ConfigModel):
    """Catalog service configuration."""

    type: CatalogType = CatalogType.HIVE
    """Catalog service: `hive` or `polaris`. `unity` and `none` have no supported recipe and are refused."""
    hive: HiveConfig = Field(default_factory=HiveConfig)
    """Hive Metastore settings, used when `type` is `hive`."""
    polaris: PolarisConfig = Field(default_factory=PolarisConfig)
    """Polaris settings, used when `type` is `polaris`."""
    unity: UnityConfig = Field(default_factory=UnityConfig)
    """Unity Catalog settings, used when `type` is `unity`."""


class PolarisClientSecretMissing(ValueError):
    """No Polaris client secret: none in the config and none stored by
    deploy in the Secret ``lakebench-polaris-client``."""


class IcebergConfig(ConfigModel):
    """Apache Iceberg table format configuration."""

    # 1.11.0 is the first release compiled for Java 17 and the first to publish
    # a native Spark 4.1 runtime. Spark 3.5 images must use a java17 tag with
    # this version; validate_iceberg_java_runtime() refuses the combination
    # rather than letting it fail inside the driver.
    version: str = "1.11.0"
    """Apache Iceberg runtime JAR version."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "file_format": "Iceberg tables are always written as Parquet.",
        "properties": "no table property is applied from the config.",
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {"file_format": "parquet", "properties": {}}


class DeltaConfig(ConfigModel):
    """Delta Lake table format configuration."""

    version: str = "auto"
    """Delta Lake version. `auto` resolves a version that matches the Spark image."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "properties": "no table property is applied from the config.",
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {"properties": {}}


class TableFormatConfig(ConfigModel):
    """Table format configuration."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "hudi": "Hudi is not a supported table format; removed in v1.2.",
    }

    type: TableFormatType = TableFormatType.ICEBERG
    """Table format: `iceberg` or `delta`. Delta runs with the `hive` catalog and `trino`,
    `spark-thrift` or `none` query engines (the `hive-delta-*` recipes). The `financial`
    (AML) workload refuses Delta.
    """
    iceberg: IcebergConfig = Field(default_factory=IcebergConfig)
    """Iceberg settings, used when `type` is `iceberg`."""
    delta: DeltaConfig = Field(default_factory=DeltaConfig)
    """Delta Lake settings, used when `type` is `delta`."""


class TrinoCoordinatorConfig(ConfigModel):
    """Trino coordinator resource configuration."""

    cpu: str = "2"
    """Trino coordinator CPU."""
    memory: str = "8Gi"
    """Trino coordinator memory."""


class TrinoWorkerConfig(ConfigModel):
    """Trino worker configuration."""

    # le: the largest tier in config/scale.py asks for scale // 50 workers,
    # 200 at the top scale of 10000.
    replicas: int = Field(default=2, ge=1, le=256)
    """Number of Trino worker pods."""
    cpu: str = "4"
    """Trino worker CPU."""
    memory: str = "16Gi"
    """Trino worker memory."""
    spill_enabled: bool = True
    """Enable query spill to disk."""
    spill_max_per_node: str = "40Gi"
    """Maximum spill size per worker."""
    storage: str = "50Gi"
    """Worker storage size for spill and temp data."""
    storage_class: str = ""
    """Worker StorageClass. Empty = emptyDir (ephemeral, no PVC needed). Set a class name to
    use PVC-backed persistent volumes instead.
    """

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
    """Trino coordinator resources."""
    worker: TrinoWorkerConfig = Field(default_factory=TrinoWorkerConfig)
    """Trino worker replicas, resources and spill."""
    catalog_name: str = "lakehouse"
    """Trino catalog name for the Iceberg connector."""


class SparkThriftConfig(ConfigModel):
    """Spark Thrift Server configuration."""

    cores: int = Field(default=2, ge=1)
    """Spark Thrift Server CPU cores. Auto-sized to `8` on Delta + Hive when unset."""
    memory: str = "4g"
    """Spark Thrift Server heap. The pod limit adds max(10% of heap, 1 GiB). Auto-sized to
    `16g` on Delta + Hive and `24g` on the financial schema when unset.
    """
    catalog_name: str = "lakehouse"
    """Iceberg catalog name for Spark Thrift Server."""


class DuckDBConfig(ConfigModel):
    """DuckDB query engine configuration."""

    cores: int = Field(default=2, ge=1)
    """DuckDB CPU cores."""
    memory: str = "4g"
    """DuckDB pod memory. Auto-sized to `16g` on the financial schema when unset (less on a
    node with under 24 GiB allocatable)."""
    catalog_name: str = "lakehouse"
    """Iceberg catalog name for DuckDB."""
    # Pinned, not floating. Both install sites used a bare `pip install duckdb`,
    # so every deploy took whatever was current and two runs weeks apart could
    # compare different query engines while reporting the difference as a
    # result. Measured local run-to-run spread is 0.9%, well below what an
    # engine change would move, so the drift was invisible to the noise floor.
    version: str = "1.5.5"
    """DuckDB version installed at deploy time. Pinned deliberately: an unpinned install takes
    whatever is current, so two runs weeks apart can query with different engines and the
    difference would read as a result.
    """


class QueryEngineConfig(ConfigModel):
    """Query engine configuration."""

    type: QueryEngineType = QueryEngineType.TRINO
    """Query engine: `trino`, `spark-thrift`, `duckdb`, or `none`."""
    trino: TrinoConfig = Field(default_factory=TrinoConfig)
    """Trino settings, used when `type` is `trino`."""
    spark_thrift: SparkThriftConfig = Field(default_factory=SparkThriftConfig)
    """Spark Thrift Server settings, used when `type` is `spark-thrift`."""
    duckdb: DuckDBConfig = Field(default_factory=DuckDBConfig)
    """DuckDB settings, used when `type` is `duckdb`."""


# The v1.6 medallion block, at its defaults. Nothing read it except
# bronze.path_template, which the Spark stages ignored (they read a fixed
# layout); datagen wrote under it. Kept to recognise a config that carries
# it unchanged (see ArchitectureConfig._drop_default_medallion).
_MEDALLION_V16_DEFAULT: dict[str, Any] = {
    "bronze": {"format": "parquet", "path_template": "customer/interactions"},
    "silver": {
        "format": "iceberg",
        "table_name": "customer_interactions_enriched",
        "partition_by": ["date"],
        "transforms": [
            "normalize_email",
            "normalize_phone",
            "geo_enrichment",
            "customer_segmentation",
            "quality_flags",
        ],
    },
    "gold": {
        "format": "iceberg",
        "tables": [
            {
                "name": "customer_executive_dashboard",
                "partition_by": ["date"],
                "aggregations": [
                    "daily_revenue",
                    "daily_engagement",
                    "churn_indicators",
                    "channel_performance",
                ],
            }
        ],
    },
}


class SustainedConfig(ConfigModel):
    """Sustained pipeline configuration.

    Controls trigger intervals for streaming jobs, run duration,
    checkpoint path prefix in S3, and throughput tuning knobs.
    """

    bronze_trigger_interval: str = "0 seconds"
    """Bronze streaming trigger interval. "0 seconds" (the default) starts the
    next micro-batch as soon as the last one finishes and new files exist; a
    positive interval holds bronze to that cadence, labelled beside freshness.
    With a trickle (`max_files_per_trigger`, `--skip-generate`), "0 seconds"
    becomes 30 seconds, the trickle's cadence, and the run says so. A whole
    number and seconds, minutes or hours; anything else is refused at load."""
    silver_trigger_interval: str = "0 seconds"
    """Silver streaming trigger interval. "0 seconds" (the default) starts the
    next micro-batch as soon as the last one finishes and bronze has
    committed more; a positive interval is labelled beside freshness. A whole
    number and seconds, minutes or hours."""
    gold_refresh_interval: str = "0 seconds"
    """Gold refresh trigger interval. "0 seconds" (the default) starts the
    next refresh as soon as the last one finishes (Customer360: as soon as
    silver commits); a positive interval holds gold to that cadence, labelled
    beside freshness."""
    run_duration: int = Field(
        default=1800,
        ge=60,
        description=(
            "Measurement window in seconds. The schema accepts 60 and up; with gold on an "
            "interval a continuous run refuses less than 3 x `gold_refresh_interval`. Use 900 s "
            "or more, UAT included."
        ),
    )
    checkpoint_base: str = "checkpoints"
    """S3 prefix for streaming checkpoints."""

    # Throughput tuning -- these control how much data the streaming
    # pipeline can process per trigger interval.
    max_files_per_trigger: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Max Parquet files bronze reads per trigger, a Lakebench cap on intake. Unset: no "
            "limit when the run starts its own datagen, which generates for the whole window. "
            "With --skip-generate (a finite corpus) unset is derived per run so data keeps "
            "arriving for about 1.2 x `run_duration`, capped at 50, and an explicit value that "
            "would offer the corpus before the window ends is refused at run start."
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
            "Customer360 only: seconds silver-stream waits for the bronze table to "
            "appear before it stops the run. Unset (auto): the run's window "
            "(`--duration` or run_duration) / 4, floored at 600 s. "
            "The wait runs before the window opens: datagen starts once the "
            "streams run, and bronze creates its table with its first batch."
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
            "Seconds between table maintenance rounds during a continuous run: Iceberg "
            "`expire_snapshots` + `remove_orphan_files`, or Delta `VACUUM` (Trino only). Unset: "
            "`run_duration / 3`, within 300--7200, resolved at run start (600 s for the default "
            "1800 s window), so a default run maintains inside its window. An explicit value too "
            "long for the first round to run inside the window is refused at run start unless "
            "`--skip-maintenance` is given. While streams are live Delta `VACUUM` keeps Delta's "
            "7-day default retention, so a continuous Delta run shorter than 7 days removes no "
            "files. Range: 300--7200."
        ),
    )
    retention_threshold: str = Field(
        default="30m",
        description=(
            "Iceberg snapshot retention threshold. Snapshots older than this are expired. A whole "
            "number and one unit, `s`, `m`, `h` or `d` (e.g., `30m`, `1h`, `7d`); anything else "
            "is rejected at load. While streams are live, Iceberg expiry is floored at `1h`, and "
            "a continuous Iceberg config on Trino or Spark Thrift that sets a lower value prints "
            "a warning when it loads. The `30m` default does not warn: every continuous "
            "maintenance round runs beside live streams, so it expires at `1h`, and the run "
            "records the applied values in `continuous.retention` of `metrics.json`. Delta has no "
            "effective table maintenance in continuous mode (v1.6). Orphan-file removal never "
            "uses less than 24 h 10 min, on any engine."
        ),
    )

    @field_validator("bronze_trigger_interval", "silver_trigger_interval", "gold_refresh_interval")
    @classmethod
    def _validate_interval(cls, v: str) -> str:
        # A bare "0" passed straight to Spark for one workload and read as a
        # 10 s fallback for the other.
        import re as _re

        if not isinstance(v, str) or not _re.fullmatch(
            r"\s*\d+\s+(second|minute|hour)s?\s*", v, flags=_re.IGNORECASE
        ):
            raise ValueError(
                f"trigger interval {v!r} is not a whole number and a unit (seconds, minutes "
                "or hours), for example '0 seconds' (back to back) or '5 minutes'"
            )
        return v.strip()

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
        description=(
            "Run periodic Iceberg compaction (`rewrite_data_files` / `optimize`) during "
            "continuous runs."
        ),
    )
    compaction_interval: int = Field(
        default=0,
        ge=0,
        description=(
            "Seconds between compaction rounds. `0` = 2x the effective `retention_interval` (1200 "
            "s for the default window). An explicit value that cannot run inside the window is "
            "refused at run start unless `compaction_enabled` is false or `--skip-maintenance` is "
            "given. Minimum 0; no upper bound in the schema."
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
        description=(
            "Seconds between in-stream benchmark rounds, 300--3600. With gold on a longer "
            "interval, raised to that interval so rounds do not overlap gold rewrites."
        ),
    )
    benchmark_warmup: int = Field(
        default=300,
        ge=300,
        le=1800,
        description=(
            "Seconds before the first in-stream benchmark round, 300--1800. With gold on a "
            "longer interval, raised to that interval so gold refreshes once first."
        ),
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
        """silver_bronze_wait_seconds, or auto: run_duration // 4 floored at 600.

        A3 (silver-plan): caps the wait for the bronze table so a stalled
        bronze-ingest stops the run. The floor covers datagen's start (it
        starts once the streams run) and bronze's first batch, all before
        the window opens.
        """
        if self.silver_bronze_wait_seconds is not None:
            return self.silver_bronze_wait_seconds
        window = self.run_duration if run_duration is None else run_duration
        return max(600, int(window) // 4)


class ProcessingConfig(ConfigModel):
    """Processing pattern configuration."""

    pattern: ProcessingPattern = ProcessingPattern.MEDALLION
    """**Deprecated; removed in v1.7.** The stages are chosen by `pipeline.mode`. The only
    remaining effect is that `streaming` makes the auto-sizer give Spark 60% and datagen 40%
    of the CPU budget; any value other than `medallion` prints a warning.
    """
    mode: PipelineMode = PipelineMode.BATCH
    """Pipeline execution mode: `batch` (sequential medallion jobs) or `continuous` (concurrent
    jobs over arriving data). `sustained` is accepted as a deprecated alias. The
    `--continuous` CLI flag overrides this.
    """
    # The mode the config file set, when `run --continuous` replaced it on the
    # run's copy (cli/_run.py); the run record names the command line.
    _configured_mode: PipelineMode | None = PrivateAttr(default=None)
    cycles: int = Field(
        default=1,
        ge=1,
        le=50,
        description=(
            "Batch iterations (1--50). Cycle 1 is full overwrite; cycles 2+ are incremental "
            "append/merge. Simulates multi-day lakehouse behavior. Only valid when `mode: batch`. "
            "See [Multi-Cycle Batch](#multi-cycle-batch)."
        ),
    )
    pre_benchmark_maintenance: bool = Field(
        default=True,
        description=(
            "Run table maintenance before the benchmark phase so QpH is measured against "
            "maintained tables. Iceberg: `expire_snapshots`, `remove_orphan_files` (never below "
            "24 h 10 min) and compaction of silver and gold. Delta: `VACUUM` on Trino only; Delta "
            "`OPTIMIZE` is never run. All statements share one 30-minute budget; the first "
            "statement timeout or the deadline stops the rest, and the post-maintenance QpH "
            "is then not a measurement."
        ),
    )
    sustained: SustainedConfig = Field(default_factory=SustainedConfig)
    """Continuous-mode settings, written as `architecture.pipeline.continuous`."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "medallion": (
            "nothing read the medallion block except bronze.path_template, and the Spark "
            "stages read a fixed bronze layout (customer/interactions/, pacs008/ for the "
            "financial workload): a custom bronze layout is not supported."
        ),
    }

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
            "Scale factor (1 unit ~ 10 GB bronze). The schema accepts 0.01--10000, but datagen is "
            "banded per workload: Customer 360 supported to 300, unverified to 600, refused "
            "above; AML supported to 300, unverified to 800, refused above (see [Scale "
            "Factors](#scale-factors)). Values below 1 are intended for local mode."
        ),
    )

    # Deprecated: kept for backward compatibility
    target_size: str | None = Field(
        default=None,
        description=(
            "**Deprecated.** Legacy size string (e.g., `100gb`). Converted to scale automatically."
        ),
    )

    mode: DatagenMode = DatagenMode.AUTO
    """S3 delivery pattern: `batch` = one PUT per file, `continuous` = S3 multipart upload as
    row-groups close, `auto` = `continuous` at every scale. Row
    content is byte-identical across modes at a fixed seed. Sizing is keyed on scale, not
    mode.
    """
    # Top-level generator seed. Unset: the AML pre-registration's calibration
    # seed for the financial schema, 42 otherwise (config/datagen_seed.py). A
    # financial seed the pre-registration lists as spent is refused.
    seed: int | None = Field(default=None, ge=0, le=2**63 - 1)
    """Top-level generator seed, which names the corpus. Unset: the AML pre-registration's
    calibration seed for `financial`, 42 for other schemas. A `financial` seed listed in the
    pre-registration's `corpora.spent_seeds` is refused at config load, so a retired corpus
    is never regenerated by accident. The AML reference job reports the seed it scored. The
    held-out evaluation and robustness seeds are refused unless `corpus_role` declares that
    role, and even then every command that reads or scores data refuses them: their corpus is
    generated only by `generate --registered-corpus` and scored only by
    `scripts/aml_gate.py --registered`. They are known only as salted hashes in
    `spark/data/aml/heldout_hashes.json`, and the check hashes the configured seed.
    """
    # AML corpus role (financial only). The evaluation and robustness seeds are
    # refused unless the run declares its role here: each is generated once,
    # as the registered gate run for that role. An evaluation or robustness
    # role needs the seed set: it is checked against heldout_hashes.json.
    corpus_role: Literal["calibration", "evaluation", "robustness"] | None = None
    """`financial` only: `calibration`, `evaluation` or `robustness`. Declares this deployment
    as the registered corpus for that role. For `evaluation` and `robustness`, `seed` must be
    set and must hash to that role's entry in `heldout_hashes.json`; an unset `seed` is
    refused, `generate --registered-corpus` generates the corpus, and every other data
    command refuses it; its seed reaches the cluster only through a Secret in the
    deployment's namespace, never as a Job argument. For `calibration`, an unset `seed` uses
    the calibration seed. Only set it for the one registered gate run of that role.
    """
    # Robustness corpus (financial only; AML-GOALS R3(b)): datagen shifts the
    # nuisance parameters by corpora.robustness_perturbation in the
    # pre-registration (median amount, persona sds, dormancy, each x1.2 in
    # natural units). Required with corpus_role: robustness, refused with a
    # calibration or evaluation role. Off: output is unchanged.
    robustness_perturbation: bool = False
    """`financial` only. Generates the robustness corpus: the pre-registration's
    `corpora.robustness_perturbation` multipliers shift the nuisance parameters in natural
    units (median amount x1.2, persona activity and amount log-sds x1.2, dormancy lengths
    x1.2). Instances, participants and row counts are unchanged. Required with `corpus_role:
    robustness`, refused with `calibration` or `evaluation`; the generator also refuses the
    robustness seed without it. Off, the corpus is byte-identical to a run without the
    option.
    """
    parallelism: int = Field(default=4, ge=1)
    """Number of parallel datagen pods. A value you set is used exactly, with a warning when
    the cluster cannot fit it (batch pods queue; a continuous run, whose pods all run beside
    the streams, is refused at preflight) or it is under 8 for financial above scale 100.
    Unset: in batch the auto-sizer derives it from the scale, caps it to fit the cluster and
    raises financial above scale 100 to at least 8 pods; in continuous, with `cpu` also unset,
    it is the pods (of up to 8 cores) that offer the scale's load, still raised to the
    financial floor and capped to fit the cluster (a cap lowers the offered load). The schema
    fallback without auto-sizing is 4.
    """
    # Datagen output file size, fixed at 64mb for every workload and mode
    # (owner decision 2026-09-29). c360 rows are drawn per file and truncated
    # by file size, so one size keeps row content identical across delivery
    # modes and keeps corpus identity stable. The field stays so
    # existing configs that set 64mb still load; any other value is refused.
    file_size: Literal["64mb"] = "64mb"
    """Fixed at `64mb` for every workload and mode; any other value is refused. One size keeps
    row content identical across delivery modes.
    """
    dirty_data_ratio: float = 0.08
    """Fraction of intentionally dirty records (0.0--1.0). Applies to the `customer360` schema
    only; the `financial` (AML) generator ignores it.
    """
    cpu: str = "2"
    """CPU per datagen pod. A value you set is used as given. Unset: 8 in batch, and 8 in
    continuous when `parallelism` is set; in continuous with both unset, the cores that offer
    the scale's load (in 100m steps, at least 200m), split over the pods.
    """
    memory: str = "4Gi"
    """Memory per datagen pod. A value you set is used as given; unset, the auto-sizer derives
    it from the measured peak RSS model for the schema, scale, pod CPU (thread count) at the
    fixed 64mb file size, with a 4Gi floor.
    """
    # Generator threads per pod; 0 = auto (follow the pod CPU).
    generators: int = Field(default=0, ge=0, le=1024)
    """Generator threads per pod. 0 = auto: one thread per started core of the pod's CPU
    (Lakebench sets `CPU_LIMIT`), held to the CPU by its quota.
    """
    timestamp_start: str | None = Field(
        default=None,
        description=(
            "Start date for generated timestamps (ISO format). Default: `2024-01-01`. See "
            "[Timestamp Range Impact](#timestamp-range-impact)."
        ),
    )
    timestamp_end: str | None = Field(
        default=None,
        description=(
            "End date for generated timestamps (ISO format, exclusive). Default: `2025-01-01` for "
            "single-cycle runs (Rust generator built-in). Multi-cycle runs (`cycles > 1`) split a "
            "wider `2024-01-01` to `2025-12-31` default window across cycles "
            "(`config/c360_run.py` `cycle_windows`, which the datagen deployer and "
            "`metrics/c360_correctness.py` both read). See [Timestamp Range "
            "Impact](#timestamp-range-impact)."
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
            "(Configs from before v1.6 that used another size "
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
        "date_range_days": (
            "the customer360 generator never read it: the event window is "
            "datagen.timestamp_start and datagen.timestamp_end."
        ),
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {"date_range_days": None}

    unique_customers: int | None = Field(
        default=None,
        ge=1,
        description="Override: unique customer count. If None, derived from scale.",
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
    """Share of alerts the simulated L1 analyst dispositions correctly (0.5--1.0)."""
    investigator_accuracy: float = Field(default=0.95, ge=0.5, le=1.0)
    """Share of cases the simulated L2 investigator decides correctly (0.5--1.0)."""
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


#: Load purposes that skip the AML seed guard at load: they tear down, read
#: about or show a deployment, or resolve a config's name, and never generate
#: or score data (``WorkloadConfig._seed_allowed``).
_SEED_GUARD_SKIPPED = frozenset({LoadPurpose.TEARDOWN, LoadPurpose.READ, LoadPurpose.INSPECT})


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
    """Workload schema: `customer360` or `financial`. `custom` is refused at load in v1.6.
    `financial` requires `table_format: iceberg`. The block was `architecture.workload`
    before v1.6; that location still loads with a deprecation warning, and setting both with
    different values is an error.
    """
    datagen: DatagenConfig = Field(default_factory=DatagenConfig)
    """Data generation: scale, seed, delivery mode, pod sizing and event window."""
    customer360: Customer360Config = Field(default_factory=Customer360Config)
    """Customer 360 workload parameters."""

    # Snapshot-retention policy for batch runs: with retention_workload=True
    # the pre-benchmark maintenance step keeps snapshots covering
    # retention_months + headroom, rather than expiring everything with the
    # default "0s" threshold. No Lakebench command reads those older
    # snapshots; continuous time travel is bounded by
    # sustained.retention_threshold instead. Both fields are part of the AML
    # workload parameters (parameters_id).
    retention_workload: bool = False
    """AML batch: keep snapshot history through pre-benchmark maintenance, which
    then retains `retention_months` plus headroom instead of expiring every
    snapshot.
    """
    retention_months: int = Field(default=60, ge=1, le=120)
    """AML: months of snapshots kept when `retention_workload` is true (1--120)."""

    # W1 connected-components vertex cap for the Financial detection path.
    # Default sits above the scale-10 vertex count (1.1M entities) so W1 runs
    # out of the box at scale 10; raise it for larger scales that have the
    # executor budget. Whether W1 completes in acceptable wall-clock above
    # the cap is a measured question, not a config guarantee, so the
    # ceiling stays generous rather than unbounded. Consumed by
    # gold_finalize_financial via LB_FINANCIAL_W1_MAX_VERTICES.
    w1_max_vertices: int = Field(default=8_000_000, ge=1, le=200_000_000)
    """AML: vertex cap of the W1 connected-components rule, a Lakebench-imposed cap.
    The default covers scale 10 (1.1M entities); raise it for larger scales that
    have the executor budget.
    """

    # Financial transaction-monitoring operations layer (GOALS P10).
    tm_operations: TmOperationsConfig = Field(default_factory=TmOperationsConfig)
    """AML: the simulated transaction-monitoring operations on top of the alerts."""

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
    def _seed_allowed(self, info: ValidationInfo) -> WorkloadConfig:
        # Refused at load, before anything is deployed: a spent AML seed would
        # regenerate a corpus that has already been looked at, and an
        # evaluation or robustness seed without its declared role would burn
        # it (AML-GOALS R3). The commands that only tear down, read about or
        # show a deployment skip the seed guard: they generate and score
        # nothing, and a registered look's seed is spent once its look is
        # recorded, so its deployment must still be destroyable. The commands
        # that read or score data refuse a protected corpus themselves
        # (lakebench.aml.look_guard).
        from lakebench.config.datagen_seed import check_perturbation, resolve_seed

        if purpose_from_context(info.context) not in _SEED_GUARD_SKIPPED:
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


# What each workload can run on (support layers 2 and 3, config/support.py).
# The architecture tuple list above says nothing about workloads; this does. A
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
            f"workload it names. Use an {formats[0]} recipe (for example recipe: "
            f"polaris-{formats[0]}-spark-trino), or with no recipe set "
            f"architecture.table_format.type to {formats[0]}."
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
        description="Silver sealed-batch marker sidecar (Financial): one row per (stream_id, batch_id) written last so downstream consumers hide mid-batch crashes",
    )
    silver_counterparty_pairs: str = Field(
        default="silver.counterparty_pairs",
        description="Silver distinct (originator, beneficiary) pairs (Financial, continuous only): one row per pair, so the stream counts a batch's new counterparties without re-reading the history",
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
            "LB_FINANCIAL_SILVER_PAIRS": self.silver_counterparty_pairs,
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
        self,
        schema: str,
        *,
        layers: tuple[str, ...] = ("bronze", "silver", "gold"),
        continuous: bool = True,
    ) -> list[str]:
        """Every table the pipeline writes for ``schema``, bronze first.

        Customer 360 writes one table per layer. Financial writes several per
        layer; maintenance, compaction and destroy that only looked at
        ``silver``/``gold`` missed all but two of them. ``continuous=False``
        leaves out the tables only the continuous pipeline writes
        (``silver_counterparty_pairs``), which a batch run never creates.
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
                    *([self.silver_counterparty_pairs] if continuous else []),
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
    before it. A post round taken straight away measured the object
    store working off the delete and rewrite burst. The wait probes one
    storage-bound query until it is stable; see ``lakebench.benchmark.settle``.
    Batch mode only: continuous-mode maintenance runs during the stream and
    is not waited on.
    """

    enabled: bool = Field(
        default=True,
        description=(
            "Batch mode: probe until storage settles between maintenance and the post-maintenance "
            "round. See [Storage settle wait](benchmarking/maintenance.md#storage-settle-wait)."
        ),
    )
    # Recovery took about 35 minutes in the one measured case; 45 minutes
    # leaves 10 minutes of margin before the post round runs unsettled.
    max_seconds: int = Field(
        default=2700,
        ge=60,
        le=14400,
        description=(
            "Longest wait. When reached, the post round still runs and `maintenance_value_pct` is "
            "null. Range: 60--14400."
        ),
    )
    interval_seconds: int = Field(
        default=60,
        ge=5,
        le=3600,
        description=("Seconds between the starts of consecutive probes. Range: 5--3600."),
    )
    # The unsettled rounds were 27-34% slow and the in-round spread of one
    # query at scale 10 is a few percent, so 10% separates the two.
    tolerance_pct: float = Field(
        default=10.0,
        gt=0,
        le=100,
        description=(
            "Settled when two consecutive probes differ by at most this percent and neither is "
            "slower than the pre-maintenance time by more. Range: above 0, up to 100."
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
        description=("Timed runs per probe; the probe time is their median. Range: 1--10."),
    )


class BenchmarkConfig(ConfigModel):
    """Benchmark configuration.

    Controls how the Trino query benchmark is executed.
    Defaults produce today's behavior (power run, single stream, hot cache).
    """

    mode: BenchmarkMode = BenchmarkMode.POWER
    """Benchmark mode: `power`, `standard`, `extended`, `throughput`, or `composite`.
    `lakebench run` measures one power pass (`standard` and `extended` are power) and
    refuses `throughput` and `composite`, with or without `--skip-benchmark`; `lakebench
    benchmark --mode` runs them.
    """
    streams: int = Field(
        default=4,
        ge=1,
        le=64,
        description=(
            "Concurrent query streams for `lakebench benchmark` throughput mode. Range: 1--64. "
            "`lakebench run` uses one stream and refuses an explicit value above 1."
        ),
    )
    cache: str = Field(
        default="hot",
        pattern=r"^(hot|cold)$",
        description=(
            "Cache mode: `hot` (warm cache) or `cold` (cleared before each query). `lakebench "
            "run` measures a hot cache and refuses `cold`; use `lakebench benchmark --cold`."
        ),
    )
    # One sample per query cannot tell a change from noise: same-run rounds
    # on the live cluster differed 3-11% in QpH and a post-maintenance round
    # read 10-80% slower per query with nothing to compare that against
    # Three samples give a median and a measured spread.
    iterations: int = Field(
        default=3,
        ge=1,
        le=100,
        description=(
            "Timed runs of each query per benchmark round. QpH is scored from the per-query "
            "median and every sample plus the spread is recorded in `metrics.json`. `1` is a "
            "quick run with no measured spread; the maintenance value is then not reported. "
            "Range: 1--100."
        ),
    )
    maintenance_settle: MaintenanceSettleConfig = Field(default_factory=MaintenanceSettleConfig)
    """Batch: the storage settle wait between maintenance and the scored round."""
    investigator_sessions: int | None = Field(default=None, ge=1, le=32)
    """AML continuous only: concurrent investigator sessions run once, as an extra round
    after the first in-stream round that had a case, each working one case of the run (IQ1
    to IQ4). Range: 1--32. Unset runs no such round. Refused at load unless the workload is
    `financial`, `tm_operations.enabled` is true and the query engine is `trino` or
    `spark-thrift`, and by `run` unless the run is continuous. The sessions that ran (fewer
    when the run has fewer cases) are an outcome condition: two runs that differ in it
    are not like-for-like.
    """

    @model_validator(mode="after")
    def _refuse_what_run_does_not_do(self, info: ValidationInfo) -> BenchmarkConfig:
        """``lakebench run`` measures one hot power pass with one stream.

        A config asking ``run`` for another mode, a cold cache or several
        streams was recorded as if it had run that way. Under
        ``LoadPurpose.RUN`` it is refused; ``lakebench benchmark`` honours
        all three. ``standard`` and ``extended`` are power runs.
        """
        if purpose_from_context(info.context) != LoadPurpose.RUN:
            return self
        refused: list[str] = []
        if self.mode in (BenchmarkMode.THROUGHPUT, BenchmarkMode.COMPOSITE):
            refused.append(
                f"benchmark.mode {self.mode.value}: run measures one power pass; "
                "use 'lakebench benchmark --mode' for throughput and composite"
            )
        if self.cache == "cold":
            refused.append(
                "benchmark.cache cold: run measures a hot cache; use 'lakebench benchmark --cold'"
            )
        if "streams" in self.model_fields_set and self.streams > 1:
            refused.append(
                f"benchmark.streams {self.streams}: run uses one stream; "
                "use 'lakebench benchmark --streams'"
            )
        if refused:
            raise ValueError(
                "; ".join(refused) + ". Delete the setting from the config for 'lakebench run'"
            )
        return self


def _drop_default_medallion(data: dict) -> dict:
    """Drop a v1.6 ``pipeline.medallion`` block that changed nothing.

    It changed nothing when every key it sets is at its v1.6 default and
    ``bronze.path_template`` names the layout the workload uses (the C360
    default, or ``pacs008`` for financial, which v1.6 datagen wrote either
    way). Any other block is left for ``ProcessingConfig._removed_keys``.
    """
    pipeline = data.get("pipeline")
    if not isinstance(pipeline, dict) or "medallion" not in pipeline:
        return data
    workload = data.get("workload")
    schema = None
    if isinstance(workload, dict):
        schema = workload.get("schema", workload.get("schema_type"))
    schema = getattr(schema, "value", schema) or "customer360"

    def template_ok(v: object) -> bool:
        # The values whose v1.6 effective prefix is the fixed v1.7 one. v1.6
        # mapped financial to pacs008 only on the exact C360 default, and
        # datagen trims a trailing slash, never a leading one.
        if not isinstance(v, str):
            return False
        if schema == "financial":
            return v == "customer/interactions" or v.rstrip("/") == "pacs008"
        return v.rstrip("/") == "customer/interactions"

    default = {
        **_MEDALLION_V16_DEFAULT,
        "bronze": {**_MEDALLION_V16_DEFAULT["bronze"], "path_template": template_ok},
    }
    if not _matches_old_default(pipeline["medallion"], default):
        return data
    data = dict(data)
    data["pipeline"] = {k: v for k, v in pipeline.items() if k != "medallion"}
    emit_note(
        "'medallion' (ProcessingConfig) is no longer used and is ignored (it carried "
        f"its old default): {ProcessingConfig._removed_keys['medallion']} Delete it "
        "from the config.",
        kind="removed",
    )
    return data


class ArchitectureConfig(ConfigModel):
    """Layer 2: Data architecture configuration."""

    catalog: CatalogConfig = Field(default_factory=CatalogConfig)
    """Table catalog."""
    table_format: TableFormatConfig = Field(default_factory=TableFormatConfig)
    """Table format."""
    pipeline_engine: PipelineEngineType = PipelineEngineType.SPARK
    """Pipeline engine; `spark` is the only one."""
    query_engine: QueryEngineConfig = Field(default_factory=QueryEngineConfig)
    """Query engine that runs the benchmark."""
    pipeline: ProcessingConfig = Field(default_factory=ProcessingConfig)
    """Pipeline mode, cycles, maintenance and continuous settings."""
    workload: WorkloadConfig = Field(default_factory=WorkloadConfig)
    """Workload and data generation, written as the top-level `workload` block."""
    benchmark: BenchmarkConfig = Field(default_factory=BenchmarkConfig)
    """Benchmark settings."""
    tables: TableNamesConfig = Field(default_factory=TableNamesConfig)
    """Table names for each layer."""

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
        return _drop_default_medallion(data)

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
    def validate_investigator_sessions(self) -> ArchitectureConfig:
        """Refuse ``benchmark.investigator_sessions`` outside AML with TM
        operations on trino or spark-thrift. The mode is ``run``'s to check
        (``RUN_RULES``, with the mode it resolves), so ``run --continuous`` on
        a batch config with the key runs, and ``run`` of it in batch is
        refused before any cluster call."""
        problem = investigator_sessions_problem(self, check_mode=False)
        if problem:
            raise ValueError(problem + ". Delete the key, or fix the config")
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


class ObservabilityConfig(ConfigModel):
    """Layer 3: Observability configuration.

    Flat model -- use top-level keys (enabled, dashboards_enabled, etc.).
    Deeply nested YAML (metrics.prometheus.enabled) is rejected to prevent
    silent data loss (see BUG-029).
    """

    enabled: bool = False
    """Deploy the observability stack (Prometheus + Grafana)."""
    dashboards_enabled: bool = True
    """Deploy Grafana dashboards."""
    retention: str = "7d"
    """Prometheus data retention period."""
    storage: str = "10Gi"
    """Prometheus PVC size."""
    # kube-prometheus-stack chart version (bundles Prometheus + Grafana +
    # node-exporter + kube-state-metrics as one unit). Pinned as of 2026-07-27
    # -- the deploy previously carried no --version flag at all, so it
    # silently tracked whatever the Helm repo served at install time. That
    # currently resolves to Prometheus v3.13.1 + Grafana v13.1.x.
    chart_version: str = "87.19.2"
    """`kube-prometheus-stack` Helm chart version. Bundles Prometheus and Grafana as one unit
    -- there is no separate Prometheus/Grafana version field. The version a fresh `lakebench
    admin install --component observability` uses; an installed release keeps its version.
    """
    # Per-deployment Prometheus Pushgateway for live datagen + bronze->silver
    # metrics (batch jobs Prometheus pull cannot catch). Deployed only when
    # observability is enabled; a best-effort live view, never a published
    # source (metrics.json stays authoritative). See
    # docs/internal/observability-pushgateway.md.
    pushgateway_enabled: bool = True
    """Deploy a Prometheus Pushgateway for Spark job metrics when observability is
    enabled. A live view only; `metrics.json` stays the record.
    """
    pushgateway_image: str = "prom/pushgateway:v1.11.1"
    """Pushgateway image."""
    pushgateway_storage: str = "1Gi"
    """Pushgateway persistent volume size."""
    pushgateway_storage_class: str = "px-csi-scratch"
    """StorageClass of the Pushgateway volume."""

    _removed_keys: ClassVar[dict[str, str]] = {
        "reports": (
            "every run writes report.html into its run directory under "
            "lakebench-output/runs; 'lakebench report --render' writes a fresh copy to "
            "lakebench-output/reports/ without overwriting."
        ),
        "storage_class": (
            "the Prometheus volume claim is created without a storageClassName, so it "
            "uses the cluster default StorageClass."
        ),
        "prometheus_stack_enabled": (
            "observability.enabled always installs or reuses the full stack, Prometheus included."
        ),
        "s3_metrics_enabled": "PodMonitor deployment is not gated on it.",
        "spark_metrics_enabled": "PodMonitor deployment is not gated on it.",
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {
        "reports": {
            "enabled": True,
            "output_dir": "./lakebench-output/runs",
            "format": "html",
            "include": {
                "summary": True,
                "stage_breakdown": True,
                "storage_metrics": True,
                "resource_utilization": True,
                "recommendations": True,
                "platform_metrics": True,
            },
        },
        "storage_class": "",
        "prometheus_stack_enabled": True,
        # Defaulted to true before v1.6; neither value was ever wired.
        "s3_metrics_enabled": lambda v: v is None or v is True,
        "spark_metrics_enabled": lambda v: v is None or v is True,
    }


# =============================================================================
# Spark Configuration Overrides
# =============================================================================


class SparkConfOverrides(ConfigModel):
    """The user's Spark conf, merged over the job defaults.

    Holds user keys only. Each job's conf is the defaults
    (``SPARK_CONF_DEFAULTS``), then these keys, then the keys Lakebench owns
    (partitions, catalog, S3A, jars, UI); ``LakebenchConfig`` refuses a user
    value for an owned key, which would otherwise be overwritten.
    """

    conf: dict[str, str] = Field(default_factory=dict)
    """Your Spark keys, merged over the job defaults. Enters the experiment record
    (`architecture.spark_conf_user`) when it changes something.
    """


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
# "metastore listener has no adress", and deploy times out after 600 s.
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


#: The Spark image a default falls back to when the config writes a table
#: format version the Spark 4.1 default cannot run (Delta 4.0.0, written by a
#: v1.6 config), so such a config still loads, deploys and tears down.
_SPARK40_IMAGE = "apache/spark:4.0.2-python3"


def _default_spark_image(data: dict, injected: list[str], *, recipe_set: bool) -> None:
    """Choose the Spark image of a config that does not write ``images.spark``.

    With no recipe, it is the image of the recipe the config's components
    name (catalog, table format, query engine, the schema's defaults for the
    ones it leaves out), so it runs the Spark minor that recipe's
    release-matrix row is proven on; components no recipe names keep the
    schema default. With a recipe, the recipe already injected its image.
    Either way, when the config writes a table format version that image
    cannot run (Delta 4.0.0 on Spark 4.1), the Spark 4.0 image is taken
    instead, as v1.6 ran it."""
    from lakebench.config.recipes import RECIPES, written_recipe
    from lakebench.modules.pipeline_engines.spark.job import validate_format_version

    images = data.get("images")
    if images is not None and not isinstance(images, dict):
        return
    if (
        isinstance(images, dict)
        and images.get("spark") is not None
        and "images.spark" not in injected
    ):
        return  # the config wrote it
    if recipe_set:
        image = (images or {}).get("spark")
    else:
        try:
            matched = written_recipe(data, "hive-iceberg-spark-trino")
        except Exception:  # noqa: BLE001 -- a malformed block is refused by validation
            return
        image = (RECIPES.get(matched or "", {}).get("images") or {}).get("spark")
    image = image or ImagesConfig.model_fields["spark"].default
    arch = data.get("architecture")
    fmt_block = arch.get("table_format") if isinstance(arch, dict) else None
    if isinstance(fmt_block, dict):
        fmt = fmt_block.get("type") or "iceberg"
        fmt = str(getattr(fmt, "value", fmt))
        sub = fmt_block.get(fmt)
        version = sub.get("version") if isinstance(sub, dict) else None
        if isinstance(version, str) and version not in ("", "auto"):
            try:
                validate_format_version(image, fmt, version)
            except ValueError:
                try:
                    validate_format_version(_SPARK40_IMAGE, fmt, version)
                    image = _SPARK40_IMAGE
                except ValueError:
                    pass  # refused at validation, naming the version
    data["images"] = {**(images or {}), "spark": image}
    # A recipe's image already counts as filled in (``recipes.user_set``). A
    # recipe-less config's derived image is not recorded: the model must
    # round-trip through model_dump equal to itself, and the dump writes it.


class LakebenchConfig(ConfigModel):
    """Root configuration for Lakebench.

    This is the master configuration that matches Section 4 of the spec.
    All values shown are defaults unless marked REQUIRED.
    """

    # Metadata
    name: str = Field(
        default="",
        description=(
            "Unique deployment name. Also used as the K8s namespace when `namespace` is empty."
        ),
        max_length=63,  # matches K8s namespace + S3 bucket-tag safe length
    )
    recipe: str | None = None
    """Recipe shorthand (e.g., `hive-iceberg-spark-trino`). Sets catalog, table format, and
    query engine defaults. See [Recipes](recipes.md).
    """

    _removed_keys: ClassVar[dict[str, str]] = {
        "version": (
            "there is one config schema and nothing read the number; a removed key is "
            "named in the upgrade notes instead."
        ),
        "description": "nothing read it; keep notes in a YAML comment.",
        # The v1.6 flat spelling of platform.storage.s3.secret_ref.
        "secret_ref": (
            "lakebench never reads an existing Secret: deploy writes the S3 Secret from "
            "access_key and secret_key. Set those instead."
        ),
    }
    _removed_defaults: ClassVar[dict[str, Any]] = {
        "secret_ref": "",
        "version": 1,
        # Free text never changed what ran, whatever it said.
        "description": lambda v: v is None or isinstance(v, str),
    }

    # Container images
    images: ImagesConfig = Field(default_factory=ImagesConfig)
    """Container images of the components Lakebench renders; Hive, Prometheus,
    Grafana and the Pushgateway are set elsewhere."""

    # Layer 1: Platform
    platform: PlatformConfig = Field(default_factory=PlatformConfig)
    """Layer 1: Kubernetes, storage and compute."""

    # Layer 2: Data Architecture
    architecture: ArchitectureConfig = Field(default_factory=ArchitectureConfig)
    """Layer 2: catalog, table format, engines, pipeline and benchmark."""

    # Layer 3: Observability
    observability: ObservabilityConfig = Field(default_factory=ObservabilityConfig)
    """Layer 3: the Prometheus stack, dashboards and the Pushgateway."""

    # Spark configuration overrides
    spark: SparkConfOverrides = Field(default_factory=SparkConfOverrides)
    """The user's Spark conf, merged over the job defaults."""

    # Set by load_config: the notes the load collected and how the name was
    # resolved (config/loader.py load_notes, name_resolution).
    _load_notes: Any = PrivateAttr(default=None)
    _name_resolution: Any = PrivateAttr(default=None)
    # The dotted leaf paths the recipe filled in (apply_recipe_defaults);
    # recipes.user_set subtracts them.
    _recipe_injected: frozenset[str] = PrivateAttr(default=frozenset())

    @model_validator(mode="wrap")
    @classmethod
    def apply_recipe_defaults(cls, data: object, handler: Any, info: ValidationInfo) -> Any:
        """Expand recipe defaults into the config dict.

        Recipe defaults are merged via ``_deep_setdefault``, so images and
        engine resources the user wrote take precedence. A recipe-owned
        component (``RECIPE_OWNED_KEYS``) written with a different value is
        refused, naming both keys: before v1.7 the written value won
        silently, so ``recipe: polaris-...`` with ``catalog.type: hive``
        deployed Hive. The leaf paths the recipe filled in are kept on the
        model as ``_recipe_injected`` for ``recipes.user_set``.
        """
        if not isinstance(data, dict):
            return handler(data)
        data = resolve_workload_location(data)
        injected: list[str] = []
        recipe_name = data.get("recipe")
        if recipe_name:
            from lakebench.config.recipes import RECIPES, _deep_setdefault, recipe_conflicts

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
            conflicts = recipe_conflicts(data, recipe_name)
            if conflicts:
                from lakebench.config.recipes import written_recipe

                # v1.6 let the written component win, so a deployment made
                # from this file runs what the components say.
                written = written_recipe(data, recipe_name)
                purpose = purpose_from_context(info.context)
                if purpose is None or purpose in CHANGES_DATA:
                    keep = (
                        f". v1.6 used the written value, so a deployment made from "
                        f"this file is {written}: to keep it, write recipe: {written}"
                        if written
                        else ""
                    )
                    raise PydanticCustomError(
                        "recipe_conflict", "{text}", {"text": "; ".join(conflicts) + keep}
                    )
                # Destroy, status, report and the inspect commands load it as
                # v1.6 did, so a deployment made from it can still be found
                # and torn down (the same rule as for removed keys).
                as_loaded = written or recipe_name
                emit_note(
                    f"{'; '.join(conflicts)}. Loaded as {as_loaded}, as v1.6 did; "
                    f"deploy and run refuse it: write recipe: {as_loaded}",
                    kind="conflict",
                    category=None,
                )
                if written:
                    data["recipe"] = written
                    defaults = RECIPES[written]
            _deep_setdefault(data, defaults, injected)
        # "default" resolves like no recipe (to the components it sets).
        _default_spark_image(
            data, injected, recipe_set=bool(recipe_name) and recipe_name != "default"
        )
        model = handler(data)
        model._recipe_injected = frozenset(injected)
        return model

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

    @field_validator("spark", mode="after")
    @classmethod
    def refuse_unrunnable_gold_strategy(
        cls, spark: SparkConfOverrides, info: ValidationInfo
    ) -> SparkConfOverrides:
        """A Customer 360 ``spark.lb.gold.strategy`` the gold scripts would
        refuse (``incremental``, or a value naming no strategy) is refused
        at load by the commands that change data, before anything deploys."""
        from lakebench.config.c360_run import gold_override_problem

        arch = info.data.get("architecture")
        # No architecture: it failed validation and the load fails on that.
        if arch is None or arch.workload.schema_type != WorkloadSchema.CUSTOMER360:
            return spark
        purpose = purpose_from_context(info.context)
        if purpose is not None and purpose not in CHANGES_DATA:
            return spark
        problem = gold_override_problem(spark.conf)
        if problem:
            raise ValueError(
                f"{problem}. Delete it from spark.conf, or set auto, simple_agg or two_phase_agg"
            )
        return spark

    @field_validator("spark", mode="after")
    @classmethod
    def refuse_owned_spark_conf(
        cls, spark: SparkConfOverrides, info: ValidationInfo
    ) -> SparkConfOverrides:
        """A user ``spark.conf`` key that Lakebench owns is refused.

        Lakebench writes it for every job after the user's conf, so the value
        would be lost. A key at its v1.6 schema default (which v1.6 wrote and
        then overwrote) changed nothing and is dropped with a note. Teardown
        and read commands drop owned keys with a note, so an old config can
        still be destroyed.
        """
        from lakebench.modules.pipeline_engines.spark.conf_keys import (
            V16_DEFAULT_SPARK_CONF,
            is_owned_spark_key,
            owned_key_reason,
        )

        conf = spark.conf
        # architecture is declared (so validated) before spark.
        arch = info.data.get("architecture")
        catalog = arch.query_engine.trino.catalog_name if arch is not None else "lakehouse"
        owned = [k for k in conf if is_owned_spark_key(k, catalog)]
        if not owned:
            return spark
        inert = [k for k in owned if V16_DEFAULT_SPARK_CONF.get(k) == str(conf[k])]
        refused = [k for k in owned if k not in inert]
        purpose = purpose_from_context(info.context)
        if refused and (purpose is None or purpose in CHANGES_DATA):
            raise ValueError(
                "spark.conf sets keys Lakebench owns: "
                + "; ".join(owned_key_reason(k, catalog) for k in refused)
                + ". Delete them from spark.conf"
            )
        for key in owned:
            if key in inert:
                text = (
                    f"spark.conf '{key}' is ignored: it carried its v1.6 default, which "
                    "Lakebench overwrote then too. Delete it from spark.conf."
                )
            else:
                text = (
                    f"spark.conf '{key}' is ignored ({owned_key_reason(key, catalog)}). "
                    "Commands that change data refuse the config until it is deleted."
                )
            emit_note(text, kind="removed")
        object.__setattr__(spark, "conf", {k: v for k, v in conf.items() if k not in owned})
        return spark

    @model_validator(mode="after")
    def refuse_reserved_namespace(self) -> LakebenchConfig:
        """Refuse a deployment namespace that holds shared lakebench state.

        Destroy deletes the deployment's namespace. If that namespace were
        the shared observability namespace or the cluster-lock namespace,
        tearing down one deployment would remove what every other deployment
        uses: destroying deployment A must never affect deployment B.
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
        Teardown and diagnostic commands load with
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

    def get_scale_dimensions(self):
        """Get the resolved scale dimensions for the current workload.

        Returns:
            ScaleDimensions with customers, rows, approx size, etc.
        """
        from lakebench.config.scale import get_dimensions

        workload = self.architecture.workload
        scale = workload.datagen.get_effective_scale()
        dims = get_dimensions(workload.schema_type.value, scale)

        # Apply the customer count override from Customer360Config if present
        c360 = workload.customer360
        if c360.unique_customers is not None:
            customers = c360.unique_customers
            # replace keeps every other field (the datagen target included).
            dims = dataclasses.replace(
                dims, customers=customers, approx_rows=customers * dims.events_per_customer
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

    Supports: k, m, g, t, optionally followed by b (case-insensitive)

    Examples:
        >>> parse_spark_memory("48g")
        51539607552
        >>> parse_spark_memory("4096m")
        4294967296
    """
    memory_str = memory_str.lower().strip()
    # Spark also spells the units kb, mb, gb, tb.
    if len(memory_str) > 2 and memory_str.endswith("b") and memory_str[-2] in "kmgt":
        memory_str = memory_str[:-1]

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
