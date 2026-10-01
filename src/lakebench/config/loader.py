"""Configuration loader for Lakebench."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from ._load_context import (
    CHANGES_DATA,
    SKIPS_NAME_LENGTH,
    LoadNotes,
    LoadPurpose,
    collecting_notes,
)
from .deploy_state import NameResolution, resolve_name
from .schema import LakebenchConfig

# -- Env var substitution ----------------------------------------------------

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


def _substitute_env_vars(text: str) -> str:
    """Replace ``${VAR}`` and ``${VAR:-default}`` with environment values.

    Unresolved variables without defaults raise ``ConfigError``.
    """
    unresolved: list[str] = []

    def _replace(m: re.Match) -> str:
        var_name = m.group(1)
        default = m.group(2)
        value = os.environ.get(var_name)
        if value is not None:
            return value
        if default is not None:
            return default
        unresolved.append(var_name)
        return m.group(0)

    result = _ENV_PATTERN.sub(_replace, text)
    if unresolved:
        raise ConfigError(
            f"Unresolved environment variables: {', '.join(unresolved)}. "
            f"Set them or provide defaults with ${{VAR:-default}} syntax."
        )
    return result


# -- Flat field mapping (v2 config) ------------------------------------------

_FLAT_FIELD_MAP: dict[str, tuple[str, ...]] = {
    "endpoint": ("platform", "storage", "s3", "endpoint"),
    "access_key": ("platform", "storage", "s3", "access_key"),
    "secret_key": ("platform", "storage", "s3", "secret_key"),
    "secret_ref": ("platform", "storage", "s3", "secret_ref"),
    "scale": ("workload", "datagen", "scale"),
    "namespace": ("platform", "kubernetes", "namespace"),
    "mode": ("architecture", "pipeline", "mode"),
    "cycles": ("architecture", "pipeline", "cycles"),
    "spark_image": ("images", "spark"),
}


def _apply_flat_fields(data: dict[str, Any]) -> dict[str, Any]:
    """Promote flat top-level fields to their nested locations.

    If both flat and nested are present, flat wins and a warning is logged.
    """
    import logging

    logger = logging.getLogger(__name__)

    for flat_key, nested_path in _FLAT_FIELD_MAP.items():
        if flat_key not in data:
            continue
        value = data.pop(flat_key)

        # A config still using the deprecated 'architecture.workload' block
        # (and no top-level one) gets flat 'scale' there, so the two blocks
        # are not both set with only the flat value in one of them.
        arch = data.get("architecture")
        if (
            nested_path[0] == "workload"
            and "workload" not in data
            and isinstance(arch, dict)
            and "workload" in arch
        ):
            nested_path = ("architecture", *nested_path)

        # A config still using the deprecated 'architecture.processing' key
        # gets flat pipeline fields there, rather than a new 'pipeline'
        # block that would collide with it. An empty 'processing:' is None.
        arch = data.get("architecture")
        if (
            nested_path[:2] == ("architecture", "pipeline")
            and isinstance(arch, dict)
            and "processing" in arch
            and (arch["processing"] is None or isinstance(arch["processing"], dict))
            and "pipeline" not in arch
        ):
            nested_path = ("architecture", "processing", *nested_path[2:])

        # Walk the nested path, creating intermediate dicts as needed. An
        # empty YAML mapping ('architecture:' with nothing under it) is None.
        target = data
        for key in nested_path[:-1]:
            nxt = target.get(key)
            if nxt is None:
                nxt = target[key] = {}
            if not isinstance(nxt, dict):
                raise ConfigError(
                    f"flat field '{flat_key}' belongs under '{'.'.join(nested_path[:-1])}', "
                    f"but '{key}' is not a mapping"
                )
            target = nxt

        final_key = nested_path[-1]
        if final_key in target:
            logger.warning(
                "Both flat '%s' and nested '%s' are set -- flat value takes precedence",
                flat_key,
                ".".join(nested_path),
            )
        target[final_key] = value

    return data


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


class ConfigNameRequired(ConfigValidationError):
    """A nameless config was loaded by a command that may not use it (SAF-2).

    The commands that change data refuse every nameless config. The teardown
    commands refuse one that has only a suggested name: v1.7 never deploys a
    nameless config, so a deployment under that name was made by some other
    config file.
    """

    def __init__(self, resolution: NameResolution, *, teardown: bool = False):
        self.resolution = resolution
        name = resolution.name
        if teardown:
            msg = (
                "config has no name and this directory has no readable v1.6 "
                f"{resolution.legacy_state_path}, so no deployment can be its own; "
                f"'{name}' is only a suggestion. Fix: add the deployment's name to the "
                "config (the namespace's lakebench.deployment/name annotation holds it)."
            )
        elif resolution.source == "legacy-state":
            msg = (
                "config has no name, so it cannot change data; without a name it "
                f"resolves to '{name}' (read from {resolution.legacy_state_path}), which every "
                "other nameless config in this directory also resolves to. Fix: if this "
                f"config made deployment '{name}' and no other config here uses that "
                f"name, add 'name: {name}' to it; otherwise add a new unique name."
            )
        else:
            msg = (
                "config has no name, so it cannot change data. Fix: add a unique "
                f"name to the config, for example 'name: {name}'."
            )
        super().__init__(
            f"Configuration validation failed:\n  - name: {msg}",
            errors=[{"loc": ("name",), "msg": msg, "type": "name_required"}],
        )


def load_yaml(path: Path) -> dict[str, Any]:
    """Load YAML file and return as dictionary.

    Performs ``${VAR}`` / ``${VAR:-default}`` env-var substitution on
    the raw YAML text before parsing.

    Args:
        path: Path to YAML file

    Returns:
        Dictionary containing parsed YAML

    Raises:
        ConfigFileNotFoundError: If file doesn't exist
        ConfigParseError: If YAML parsing fails
        ConfigError: If env vars are unresolved
    """
    if not path.exists():
        raise ConfigFileNotFoundError(f"Configuration file not found: {path}")

    try:
        with open(path) as f:
            raw = f.read()
        text = _substitute_env_vars(raw)
        content = yaml.safe_load(text)
        return content if content else {}
    except yaml.YAMLError as e:
        raise ConfigParseError(f"Failed to parse YAML: {e}")  # noqa: B904


def load_config(
    path: str | Path,
    *,
    purpose: LoadPurpose | None = None,
    name_override: str | None = None,
    allow_long_names: bool = False,
    print_notes: bool = True,
) -> LakebenchConfig:
    """Load and validate Lakebench configuration from file.

    Processing order:
    1. Read YAML with ``${VAR}`` env-var substitution
    2. Promote flat top-level fields (v2 config) to nested locations
    3. Resolve the name (``deploy_state.resolve_name``; nothing is written)
    4. Validate with Pydantic, with the purpose in the validation context

    The loader never writes to disk.

    Args:
        path: Path to configuration YAML file
        purpose: What the calling command will do with the config (see
            ``LoadPurpose``). MUTATE and RUN refuse a config with no name and
            a config that carries a removed key; TEARDOWN, READ and COMPARE
            drop removed keys with a note and load a nameless config under
            its resolved name, except that TEARDOWN refuses one whose name is
            only a suggestion. Defaults to MUTATE, or to TEARDOWN when only
            ``allow_long_names`` is given.
        name_override: The name for a config that sets none (``--name``).
            It must equal the config's own name when the config has one.
        allow_long_names: Skip the derived-name length check (LB-153).
            TEARDOWN and READ always skip it. Given alone it means TEARDOWN,
            as in v1.6; with an explicit purpose it only skips the length
            check, which ``clean`` and the perf gate use so a deployment
            whose namespace is too long to finish deploying can still be
            cleaned while keeping the MUTATE refusals.
        print_notes: Print the notes block on stderr (the default). A caller
            that reports the notes itself (``load_notes``) passes False.

    Returns:
        Validated LakebenchConfig object. ``load_notes(cfg)`` and
        ``name_resolution(cfg)`` read what the load collected.

    Raises:
        ConfigFileNotFoundError: If file doesn't exist
        ConfigParseError: If YAML parsing fails
        ConfigNameRequired: A nameless config under MUTATE or RUN
        ConfigValidationError: If validation fails
    """
    if purpose is None:
        purpose = LoadPurpose.TEARDOWN if allow_long_names else LoadPurpose.MUTATE
    purpose = LoadPurpose(purpose)
    skip_name_length = allow_long_names or purpose in SKIPS_NAME_LENGTH

    path = Path(path)
    data = load_yaml(path)
    data = _apply_flat_fields(data)

    try:
        resolution = resolve_name(path, data, name_override)
    except ValueError as e:
        raise ConfigValidationError(
            f"Configuration validation failed:\n  - name: {e}",
            errors=[{"loc": ("name",), "msg": str(e), "type": "name_override"}],
        ) from None
    if resolution.nameless:
        if purpose in CHANGES_DATA:
            raise ConfigNameRequired(resolution)
        if purpose == LoadPurpose.TEARDOWN and resolution.source == "suggested":
            raise ConfigNameRequired(resolution, teardown=True)
        data["name"] = resolution.name

    # The model stores the workload block at architecture.workload; report
    # errors at the location the user wrote it.
    _arch = data.get("architecture")
    _top_level_workload = "workload" in data and not (
        isinstance(_arch, dict) and "workload" in _arch
    )

    context = {"purpose": purpose, "allow_long_names": skip_name_length}
    try:
        with collecting_notes() as notes:
            cfg = LakebenchConfig.model_validate(data, context=context)
    except ValidationError as e:
        errors = e.errors()
        if _top_level_workload:
            errors = [
                {**err, "loc": tuple(err["loc"][1:])}
                if tuple(err["loc"][:2]) == ("architecture", "workload")
                else err
                for err in errors
            ]
        # The continuous block is stored on the 'sustained' field; name it
        # the way the user wrote it.
        _pipe = _arch.get("pipeline") if isinstance(_arch, dict) else None
        if not (isinstance(_pipe, dict) and "sustained" in _pipe):
            errors = [
                {**err, "loc": ("architecture", "pipeline", "continuous", *err["loc"][3:])}
                if tuple(err["loc"][:3]) == ("architecture", "pipeline", "sustained")
                else err
                for err in errors
            ]
        error_messages = []
        for err in errors:
            loc = ".".join(str(x) for x in err["loc"])
            msg = err["msg"]
            error_messages.append(f"  - {loc}: {msg}")

        raise ConfigValidationError(  # noqa: B904
            "Configuration validation failed:\n" + "\n".join(error_messages),
            errors=[dict(e) for e in errors],  # type: ignore[call-overload]
        )
    cfg._load_notes = notes
    cfg._name_resolution = resolution
    if print_notes:
        _print_load_notes(path, notes)
    _print_load_advisories(cfg)
    return cfg


def load_notes(cfg: LakebenchConfig) -> LoadNotes:
    """The notes the ``load_config`` call that built *cfg* collected."""
    notes = cfg._load_notes
    return notes if notes is not None else LoadNotes()


def name_resolution(cfg: LakebenchConfig) -> NameResolution | None:
    """How *cfg*'s name was resolved; None for a config not built by ``load_config``."""
    return cfg._name_resolution


_printed_notes: set[tuple[str, str]] = set()


def _print_load_notes(path: Path, notes: LoadNotes) -> None:
    """Print a load's notes once per process and config as one block on stderr."""
    key = str(path.absolute())
    fresh = [t for t in notes.texts() if (key, t) not in _printed_notes]
    if not fresh:
        return
    _printed_notes.update((key, t) for t in fresh)
    from rich.console import Console
    from rich.markup import escape

    console = Console(stderr=True)
    console.print(f"[yellow]Upgrade notes[/yellow] for {escape(str(path))}:", soft_wrap=True)
    for text in fresh:
        console.print(f"  - {escape(text)}", highlight=False, soft_wrap=True)


# Iceberg snapshot-expiry floor while continuous streams are live. Mirrors
# LIVE_EXPIRE_MIN_RETENTION_SECONDS in
# modules/table_formats/iceberg/maintenance.py; a test holds them in step.
# Delta is not checked: continuous Delta has no effective table maintenance
# in v1.6 (VACUUM keeps the 7 d default while streams are live, OPTIMIZE is
# skipped), and the report says so.
_ICEBERG_LIVE_EXPIRE_FLOOR_SECONDS = 3600
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}
# Engines that run continuous Iceberg maintenance (DuckDB and none skip it).
_MAINTENANCE_ENGINES = ("trino", "spark-thrift")
_printed_advisories: set[str] = set()


def retention_floor_advisory(cfg: LakebenchConfig) -> str | None:
    """Warning text when the continuous retention_threshold is below the live floor.

    A continuous run floors Iceberg snapshot expiry at 1 h while streams are
    live, so a lower retention_threshold is silently raised. The maintenance
    journal records the effective value, but nothing told the user. Only
    Iceberg recipes whose query engine runs maintenance (Trino, Spark
    Thrift) are checked, and only a threshold the config sets. Ignores the
    pipeline mode; callers decide.
    """
    if cfg.architecture.table_format.type.value != "iceberg":
        return None
    if cfg.architecture.query_engine.type.value not in _MAINTENANCE_ENGINES:
        return None
    sustained = cfg.architecture.pipeline.sustained
    if "retention_threshold" not in sustained.model_fields_set:
        # The 30m default stays (it is in every config's perf-gate
        # fingerprint, so moving it would orphan every pinned baseline), and
        # the floor raises live expiry to 1h on its own. The run records the
        # applied value (continuous.retention), so the default needs no
        # warning.
        return None
    threshold = sustained.retention_threshold
    m = re.fullmatch(r"(\d+)([smhd])", threshold)
    if m is None:
        return None
    if int(m.group(1)) * _UNIT_SECONDS[m.group(2)] >= _ICEBERG_LIVE_EXPIRE_FLOOR_SECONDS:
        return None
    return (
        f"architecture.pipeline.continuous.retention_threshold is {threshold}, below the "
        "1h floor for Iceberg snapshot expiry while continuous streams are live; "
        "continuous maintenance expires at 1h. Set 1h or more to make the config say "
        "what runs."
    )


def load_advisories(cfg: LakebenchConfig) -> list[str]:
    """Settings that are valid but will not do what they say (continuous configs).

    A batch config run with ``run --continuous`` gets the same retention
    warning from the continuous loop instead: saved configs carry every
    field, so an explicit threshold does not mean the user chose it.
    """
    if cfg.architecture.pipeline.mode.value != "continuous":
        return []
    msg = retention_floor_advisory(cfg)
    return [msg] if msg else []


def _print_load_advisories(cfg: LakebenchConfig) -> None:
    advisories = load_advisories(cfg)
    if not advisories:
        return
    from rich.console import Console

    console = Console(stderr=True)
    for msg in advisories:
        # Once per process: some commands load the config several times.
        if msg in _printed_advisories:
            continue
        _printed_advisories.add(msg)
        console.print(f"[yellow]WARN[/yellow] {msg}")


def save_config(config: LakebenchConfig, path: str | Path) -> None:
    """Save configuration to YAML file.

    Args:
        config: LakebenchConfig object
        path: Path to save YAML file
    """
    path = Path(path)
    data = config.model_dump(mode="json", exclude_defaults=False)
    # Write the canonical locations, so the saved file reloads without
    # deprecation warnings: 'workload' is a top-level key and the continuous
    # settings block is 'pipeline.continuous' (the model stores them at
    # architecture.workload and pipeline.sustained).
    arch = data.get("architecture") or {}
    if "workload" in arch:
        data["workload"] = arch.pop("workload")
    pipeline = arch.get("pipeline") or {}
    if "sustained" in pipeline:
        pipeline["continuous"] = pipeline.pop("sustained")

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

# Recipe shorthand -- sets catalog + table_format + engine + query_engine in one line.
# Valid recipes: default, hive-iceberg-spark-trino, hive-iceberg-spark-thrift,
#   hive-iceberg-spark-duckdb, hive-iceberg-spark-none,
#   polaris-iceberg-spark-trino, polaris-iceberg-spark-thrift,
#   polaris-iceberg-spark-duckdb, polaris-iceberg-spark-none
# See docs/recipes.md for details.
# recipe: hive-iceberg-spark-trino

# Config schema version (always 1)
# version: 1

# ============================================================================
# IMAGES
# ============================================================================
# Container images for all components. Override for private registries.
# See docs/datagen-custom-images.md for building custom datagen images.
# images:
#   datagen: docker.io/sillidata/lb-datagen:1.6.0 # Customizable (see docs/datagen-custom-images.md)
#   spark: apache/spark:4.0.2-python3
#   postgres: postgres:17
#   hive: apache/hive:3.1.3
#   trino: trinodb/trino:483
#   polaris: apache/polaris:1.6.0
#   duckdb: python:3.11-slim
#   jmx_exporter: bitnami/jmx-exporter:latest
#   pull_policy: Always               # Always | IfNotPresent | Never

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
      # buckets:                     # default: <name>-bronze, <name>-silver, <name>-gold
      #   bronze: <name>-bronze        # bucket names are global on most stores; keep them unique
      #   silver: <name>-silver
      #   gold: <name>-gold
      # create_buckets: true

    ## Scratch storage for Spark shuffle PVCs
    ## When enabled, Spark shuffle data uses PVCs instead of emptyDir.
    ## Any StorageClass that provides RWO volumes works (Portworx, local-path, EBS, etc.).
    # scratch:
    #   enabled: false
    #   storage_class: px-csi-scratch    # Name of the StorageClass to use. Must exist
    #                                    # before `deploy` runs -- a cluster admin
    #                                    # installs it once with
    #                                    # `lakebench admin install-scratch-storage-class`.
    #   size: 100Gi
    #   provisioner: pxd.portworx.com    # CSI provisioner for the SC. Consumed by
    #                                    # `admin install-scratch-storage-class`. Examples:
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
  #       version: "2.5.1"           # v2.x uses webhook for volume injection
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
# See docs/recipes.md for supported (catalog, table_format, engine, query_engine)
# combinations and guidance on choosing a recipe.
architecture:
  # pipeline_engine: spark           # Pipeline engine (spark only today)
  catalog:
    type: hive                     # hive | polaris | none
    ## Hive Metastore tuning (uncomment to override defaults)
    # hive:
    #   operator:
    #     install: false             # Set true to auto-install Stackable operators.
    #                                # Requires cluster-admin. Installs commons,
    #                                # listener, secret, and hive operators.
    #     namespace: stackable
    #     version: "25.7.0"
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
    #   version: 1.6.0                # Min 1.3.0 for FlashBlade/MinIO
    #   port: 8181
    #   resources:
    #     cpu: "1"
    #     memory: 2Gi

  # table_format:
  #   type: iceberg                  # iceberg (only fully supported format)
  #   iceberg:
  #     version: "1.11.0"

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
  #       storage_class: ""          # Empty = emptyDir (ephemeral). Set a class name for PVC-backed storage.
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
  #   ## Continuous pipeline settings (used when mode: continuous)
  #   continuous:
  #     bronze_trigger_interval: "30 seconds"
  #     silver_trigger_interval: "60 seconds"
  #     gold_refresh_interval: "5 minutes"
  #     run_duration: 1800           # Streaming run duration in seconds
  #     checkpoint_base: checkpoints # S3 prefix for checkpoint data
  #     ## Throughput tuning
  #     max_files_per_trigger: 10    # Files per micro-batch; unset = auto (data arrives all window)
  #     bronze_target_file_size_mb: 512
  #     silver_target_file_size_mb: 512
  #     gold_target_file_size_mb: 128
  #     ## In-stream benchmark rounds (runs Trino queries while streaming)
  #     benchmark_interval: 300      # Seconds between rounds (300-3600)
  #     benchmark_warmup: 300        # Seconds before first round (300-1800)

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
# WORKLOAD
# ============================================================================
# What runs through the architecture: the generated corpus and its scale.
workload:
  # schema: customer360            # customer360 | financial
  datagen:
    # Image: configured via images.datagen (see docs/datagen-custom-images.md)
    ## Scale is per-schema:
    ##   customer360: ~10 GB bronze / unit (~100,000 customers)
    ##   financial:   ~8.4 GB bronze / unit (~111,111 entities and their
    ##                accounts + 60 months of pacs.008 transactions)
    scale: 10                    # Interpreted per-schema; see above
    # mode: auto                    # S3 delivery pattern: auto | batch | continuous
    ##   batch:      one PUT per Parquet file (bursty upload, higher peak RSS)
    ##   continuous: S3 multipart upload as row-groups close
    ##   auto:       continuous at every scale (owner D18, 2026-09-28)
    ## Row content is byte-identical across modes at fixed seed. CPU/memory
    ## are sized by scale via the autosizer independently of mode, and any
    ## cpu/memory you set are honoured.
    # parallelism: 8                # Number of datagen pods. Left commented so
                                    # the autosizer picks a value from cluster
                                    # capacity (scale > 50 scales up beyond the
                                    # default; small scales cap down). Set an
                                    # explicit integer to pin it.
    # file_size: 64mb
    # dirty_data_ratio: 0.08         # customer360 only; financial ignores it
    # generators: 0                # Per-pod generator threads (0 = auto: follow pod CPU)
    # timestamp_start: "2024-01-01"
    # timestamp_end: "2025-01-01"    # exclusive
  ## Customer360 workload overrides
  # customer360:
  #   unique_customers: null       # Override: derived from scale if null
  #   date_range_days: null        # Override: defaults to 365 if null

# ============================================================================
# LAYER 3: OBSERVABILITY
# ============================================================================
# Flat schema -- use top-level keys directly under observability:
# observability:
#   enabled: false                   # Deploy kube-prometheus-stack (Prometheus + Grafana)
#   prometheus_stack_enabled: true   # Prometheus collection
#   dashboards_enabled: true         # Grafana dashboards
#   retention: 7d                    # Prometheus data retention
#   storage: 10Gi                    # Prometheus PVC size
#   storage_class: ""                # PVC storage class (empty = default)

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
