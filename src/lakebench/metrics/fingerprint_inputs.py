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
  credential, and without the entries that came from the user's
  ``spark.conf``. Those are hashed into ``user_conf_sha256`` instead: a user
  conf key can hold a secret under any name, and the record must not carry
  it in plain text;

and, for the query engine, its config block plus the heap, query-memory and
pod-memory values the deploy derives from it, and the catalog's resources.

Both come from an offline manifest build: no cluster is read, so the executor
count is the one before any cluster-dependent cap (the continuous concurrent
budget), which the record labels separately.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

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


def owned_conf(spark_conf: dict[str, Any], user: dict[str, str] | None = None) -> dict[str, str]:
    """*spark_conf* without location and credential keys, values as strings.

    Entries whose value is the one *user* set (the user's own ``spark.conf``
    entries that reached the manifest unchanged) are left out too; see
    ``user_conf_digest``.
    """
    user = user or {}
    return {
        str(k): str(v)
        for k, v in sorted(spark_conf.items())
        if not is_location_key(str(k))
        and not is_credential_key(str(k))
        and not (str(k) in user and str(v) == str(user[str(k)]))
    }


def user_spark_conf(cfg: Any) -> dict[str, str]:
    """The ``spark.conf`` entries the user set: those that differ from the
    schema's default conf, or that the default does not have."""
    from lakebench.config.schema import SparkConfOverrides

    default = SparkConfOverrides().conf
    return {
        str(k): str(v)
        for k, v in (cfg.spark.conf or {}).items()
        if str(k) not in default or str(default[str(k)]) != str(v)
    }


def user_conf_digest(spark_conf: dict[str, Any], user: dict[str, str]) -> str | None:
    """sha256 over the user's ``spark.conf`` entries that reached the manifest
    unchanged, location keys left out; None when there are none."""
    applied = {
        k: str(v)
        for k, v in sorted(spark_conf.items())
        if k in user and str(v) == user[k] and not is_location_key(k)
    }
    if not applied:
        return None
    blob = json.dumps(applied, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()


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
    """The active query engine's config block and what the deploy derives
    from it (Trino heaps and query-memory limits, the Spark Thrift pod
    memory), so a change to those formulas moves the fingerprint too."""
    from lakebench.deploy.engine import DeploymentEngine

    qe = cfg.architecture.query_engine
    engine = qe.type.value
    block: dict[str, Any] = {"type": engine}
    sub = {"trino": qe.trino, "spark-thrift": qe.spark_thrift, "duckdb": qe.duckdb}.get(engine)
    if sub is not None:
        block[engine] = sub.model_dump(mode="json")
    if engine == "trino":
        block["derived"] = {
            "coordinator_heap": DeploymentEngine._trino_heap(cfg, qe.trino.coordinator.memory),
            "worker_heap": DeploymentEngine._trino_heap(cfg, qe.trino.worker.memory),
            "memory_properties": DeploymentEngine._trino_memory(cfg),
        }
    elif engine == "spark-thrift":
        block["derived"] = {"pod_memory": DeploymentEngine._thrift_pod_memory(cfg)}
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
    user = user_spark_conf(cfg)
    profiles: dict[str, Any] = {}
    confs: dict[str, Any] = {}
    user_digest: dict[str, Any] = {}
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
        confs[jt] = owned_conf(spec["sparkConf"], user)
        user_digest[jt] = user_conf_digest(spec["sparkConf"], user)
    return {
        "basis": BASIS,
        "job_profiles": profiles,
        "owned_conf": confs,
        "user_conf_sha256": user_digest,
        "query_engine": _engine_block(cfg),
        "catalog": _catalog_block(cfg),
    }


def fingerprint_inputs(cfg: Any, continuous: bool, *, local: bool = False) -> dict[str, Any]:
    """The version 2 fingerprint inputs of *cfg* for a run of this mode.

    Never raises: a run must not fail on its perf-gate record. A build
    failure is recorded as ``{"error": ...}`` and logged, and the gate
    refuses a snapshot carrying one. A local run submits no Spark job
    manifests, so it records none.
    """
    if local:
        return {"error": "local run: no Spark job manifests"}
    try:
        return _build(cfg, continuous)
    except Exception as e:  # noqa: BLE001 -- recorded, never raised from a snapshot
        text = " ".join(str(e).split())[:300]
        logger.warning(
            "perf-gate fingerprint inputs could not be built (%s: %s); this run "
            "cannot be compared or recorded as a perf baseline",
            type(e).__name__,
            text,
        )
        return {"error": f"{type(e).__name__}: {text}"}
