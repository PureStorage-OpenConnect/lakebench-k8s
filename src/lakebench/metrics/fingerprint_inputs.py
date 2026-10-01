"""What the perf-gate fingerprint hashes beyond the config fields (version 2).

A run records these in ``config_snapshot["fingerprint_inputs"]`` at run start,
and the perf gate computes the same block for a pinned config
(``perf_gate.load_pinned``). The run side is read from the stored snapshot and
never rebuilt, so a record keeps the fingerprint of the code that ran it.

Version 1 hashed config fields only, so a change to a job profile or to the
Spark conf Lakebench writes left the fingerprint equal while the run measured
something else. Version 2 adds, per Spark job type of the run's mode:

- ``job_profiles``: driver and executor sizing as the job's manifest asks for
  it (the profile, the scale-derived executor count, any executor or driver
  override), plus the profile's scratch size;
- ``owned_conf``: the ``sparkConf`` of that manifest, without the keys that
  name where a deployment lives (``FINGERPRINT_LOCATION_KEYS``) or carry a
  credential.

and, for the query engine and catalog, the sizing blocks the deploy renders.

Both come from an offline manifest build: no cluster is read, so the executor
count is the one before any cluster-dependent cap (the continuous concurrent
budget), which the record labels separately.
"""

from __future__ import annotations

import re
from typing import Any

FINGERPRINT_VERSION = 2

BATCH_JOB_TYPES = ("bronze-verify", "silver-build", "gold-finalize")
CONTINUOUS_JOB_TYPES = ("bronze-ingest", "silver-stream", "gold-refresh")

# Spark conf keys that name where a deployment lives, not what it runs: the
# S3 endpoint, warehouse and bucket URIs, catalog and metastore URIs (which
# carry the namespace), and the jar URLs, whose content the dependency
# pinset covers. Exact keys, then key suffixes (catalog keys carry the
# catalog name).
FINGERPRINT_LOCATION_KEYS = frozenset(
    {
        "spark.hadoop.fs.s3a.endpoint",
        "spark.sql.warehouse.dir",
        "spark.hadoop.hive.metastore.uris",
        "spark.jars",
    }
)
FINGERPRINT_LOCATION_SUFFIXES = (".uri", ".warehouse", ".s3.endpoint")

# Keys that can hold a credential, matched case-insensitively anywhere in the
# key. The two exceptions are settings, not secrets.
_CREDENTIAL_KEY = re.compile(
    r"secret|password|passwd|credential|token|access[._-]?key|private[._-]?key|"
    r"encryption[._-]?key|keystore",
    re.IGNORECASE,
)
_NOT_CREDENTIALS = (".credentials.provider", ".token-refresh-enabled")

_REDACTED = "redacted"

BASIS = "offline manifest build; executor_instances is before any cluster-dependent cap"


def job_types_for(continuous: bool) -> tuple[str, ...]:
    """The Spark job types a run of this mode submits for its pipeline."""
    return CONTINUOUS_JOB_TYPES if continuous else BATCH_JOB_TYPES


def is_location_key(key: str) -> bool:
    return key in FINGERPRINT_LOCATION_KEYS or key.endswith(FINGERPRINT_LOCATION_SUFFIXES)


def is_credential_key(key: str) -> bool:
    if key.endswith(_NOT_CREDENTIALS):
        return False
    return _CREDENTIAL_KEY.search(key) is not None


def owned_conf(spark_conf: dict[str, Any]) -> dict[str, str]:
    """*spark_conf* without location and credential keys, values as strings."""
    return {
        str(k): str(v)
        for k, v in sorted(spark_conf.items())
        if not is_location_key(str(k)) and not is_credential_key(str(k))
    }


def scratch_size_per_job(cfg: Any, continuous: bool) -> dict[str, Any]:
    """``{job_type: scratch_size}`` from the job profiles, schema overrides applied."""
    from lakebench.modules.pipeline_engines.spark.job import get_job_profile

    schema = cfg.architecture.workload.schema_type.value
    out: dict[str, Any] = {}
    for jt in job_types_for(continuous):
        profile = get_job_profile(jt, schema) or {}
        out[jt] = profile.get("scratch_size")
    return out


class _NoCluster:
    """The one k8s call ``SparkJobManager.__init__`` makes, answered offline."""

    def get_cluster_capacity(self) -> None:
        return None


def _redacted_copy(cfg: Any) -> Any:
    """A deep copy of *cfg* whose credentials are placeholders.

    The manifest build writes the S3 keys and the Polaris client secret into
    catalog conf keys. They are dropped by key name as well; the placeholder
    makes sure no secret reaches the block whatever a key is called.
    """
    copy = cfg.model_copy(deep=True)
    s3 = copy.platform.storage.s3
    object.__setattr__(s3, "access_key", _REDACTED)
    object.__setattr__(s3, "secret_key", _REDACTED)
    polaris = copy.architecture.catalog.polaris
    object.__setattr__(polaris, "client_secret", _REDACTED)
    return copy


def _engine_block(cfg: Any) -> dict[str, Any]:
    qe = cfg.architecture.query_engine
    engine = qe.type.value
    block: dict[str, Any] = {"type": engine}
    sub = {"trino": qe.trino, "spark-thrift": qe.spark_thrift, "duckdb": qe.duckdb}.get(engine)
    if sub is not None:
        block[engine] = sub.model_dump(mode="json")
    return block


def _catalog_block(cfg: Any) -> dict[str, Any]:
    cat = cfg.architecture.catalog
    kind = cat.type.value
    sub = getattr(cat, kind, None)
    resources = getattr(sub, "resources", None)
    return {"type": kind, "resources": resources.model_dump(mode="json") if resources else None}


def _build(cfg: Any, continuous: bool) -> dict[str, Any]:
    from lakebench.modules.pipeline_engines.spark.job import (
        JobType,
        SparkJobManager,
        get_job_profile,
    )

    offline = _redacted_copy(cfg)
    manager = SparkJobManager(offline, _NoCluster())  # type: ignore[arg-type]
    # The real env build reads the deployment namespace and runs git; the
    # environment is not part of the fingerprint.
    setattr(manager, "_build_env_vars", lambda *a, **k: [])  # noqa: B010
    schema = cfg.architecture.workload.schema_type.value
    profiles: dict[str, Any] = {}
    confs: dict[str, Any] = {}
    for jt in job_types_for(continuous):
        spec = manager._build_manifest(JobType(jt))["spec"]
        driver, executor = spec["driver"], spec["executor"]
        profiles[jt] = {
            "driver_cores": driver["cores"],
            "driver_memory": driver["memory"],
            "executor_cores": executor["cores"],
            "executor_memory": executor["memory"],
            "executor_memory_overhead": executor["memoryOverhead"],
            "executor_instances": executor["instances"],
            "scratch_size": (get_job_profile(jt, schema) or {}).get("scratch_size"),
        }
        confs[jt] = owned_conf(spec["sparkConf"])
    return {
        "basis": BASIS,
        "job_profiles": profiles,
        "owned_conf": confs,
        "query_engine": _engine_block(cfg),
        "catalog": _catalog_block(cfg),
    }


def fingerprint_inputs(cfg: Any, continuous: bool) -> dict[str, Any]:
    """The version 2 fingerprint inputs of *cfg* for a run of this mode.

    Never raises: a run must not fail on its perf-gate record. A build
    failure is recorded as ``{"error": ...}``, and the gate refuses a
    snapshot carrying one.
    """
    try:
        return _build(cfg, continuous)
    except Exception as e:  # noqa: BLE001 -- recorded, never raised from a snapshot
        text = " ".join(str(e).split())[:300]
        return {"error": f"{type(e).__name__}: {text}"}
