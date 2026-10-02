"""Which Spark conf keys a user may set, and the defaults a job starts from.

``SparkJobManager._build_manifest`` builds each job's ``sparkConf`` in three
layers, later ones winning:

1. ``SPARK_CONF_DEFAULTS``, proven settings a user may change;
2. the user's ``spark.conf`` (config key ``spark.conf``);
3. the keys Lakebench writes for the job (``LAKEBENCH_OWNED_SPARK_KEYS``):
   per-job partitions and result size, the catalog, S3A, jar, UI and
   Kubernetes settings. A user value for one of them would be overwritten,
   so the config refuses it instead of ignoring it.

``spark.driver.maxResultSize`` is the one key Lakebench writes only when the
user did not (``USER_OVERRIDABLE_SPARK_KEYS``).

This module imports nothing from lakebench, so the config schema can import
it without a cycle.
"""

from __future__ import annotations

# Proven defaults that are not owned: a job starts from these, and a user
# value replaces them. Before v1.7 they sat in the schema default of
# spark.conf, so setting any key there dropped all of them.
SPARK_CONF_DEFAULTS: dict[str, str] = {
    "spark.hadoop.fs.s3a.multipart.size": "268435456",
    "spark.hadoop.fs.s3a.fast.upload.active.blocks": "16",
    "spark.hadoop.fs.s3a.attempts.maximum": "20",
    "spark.hadoop.fs.s3a.retry.limit": "10",
    "spark.hadoop.fs.s3a.retry.interval": "500ms",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
}

# Written only when the user did not set it.
USER_OVERRIDABLE_SPARK_KEYS: frozenset[str] = frozenset({"spark.driver.maxResultSize"})

# Every key _build_manifest writes after the user map, for every job type,
# format, catalog, Spark line and option. "{catalog}" is the catalog name
# (architecture.query_engine.trino.catalog_name).
LAKEBENCH_OWNED_SPARK_KEYS: frozenset[str] = frozenset(
    {
        # per job, from the executor count
        "spark.sql.shuffle.partitions",
        "spark.default.parallelism",
        "spark.sql.files.maxPartitionBytes",
        # jars and session extensions
        "spark.jars",
        "spark.jars.packages",
        "spark.jars.repositories",
        "spark.jars.ivy",
        "spark.jars.ivySettings",
        "spark.submit.pyFiles",
        "spark.files",
        "spark.files.useFetchCache",
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
        "spark.sql.extensions",
        # S3A and filesystem
        "spark.hadoop.fs.s3a.endpoint",
        "spark.hadoop.fs.s3a.endpoint.region",
        "spark.hadoop.fs.s3a.path.style.access",
        "spark.hadoop.fs.s3a.impl",
        "spark.hadoop.fs.s3.impl",
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "spark.hadoop.fs.s3a.fast.upload",
        "spark.hadoop.fs.s3a.fast.upload.buffer",
        "spark.hadoop.fs.s3a.connection.maximum",
        "spark.hadoop.fs.s3a.threads.max",
        "spark.hadoop.fs.s3a.multipart.threshold",
        "spark.hadoop.fs.s3a.max.total.tasks",
        "spark.hadoop.fs.s3a.block.size",
        "spark.hadoop.fs.s3a.connection.timeout",
        # catalog
        "spark.sql.catalog.{catalog}",
        "spark.sql.catalog.{catalog}.type",
        "spark.sql.catalog.{catalog}.uri",
        "spark.sql.catalog.{catalog}.warehouse",
        "spark.sql.catalog.{catalog}.io-impl",
        "spark.sql.catalog.{catalog}.catalog-impl",
        "spark.sql.catalog.{catalog}.credential",
        "spark.sql.catalog.{catalog}.scope",
        "spark.sql.catalog.{catalog}.token",
        "spark.sql.catalog.{catalog}.token-refresh-enabled",
        "spark.sql.catalog.{catalog}.rest.http-client.type",
        "spark.sql.catalog.{catalog}.s3.endpoint",
        "spark.sql.catalog.{catalog}.s3.path-style-access",
        "spark.sql.catalog.{catalog}.s3.access-key-id",
        "spark.sql.catalog.{catalog}.s3.secret-access-key",
        "spark.sql.catalog.{catalog}.s3.multipart.size",
        "spark.sql.catalog.{catalog}.io.threads",
        "spark.sql.catalog.{catalog}.io.manifest-encoder-threads",
        "spark.sql.catalog.{catalog}.hive.metastore-timeout",
        "spark.sql.catalog.spark_catalog",
        "spark.sql.catalogImplementation",
        "spark.sql.warehouse.dir",
        "spark.sql.iceberg.handle-timestamp-without-timezone",
        "spark.hadoop.hive.metastore.uris",
        "spark.hadoop.hive.metastore.client.socket.timeout",
        "spark.databricks.delta.optimizeMetadataQuery.enabled",
        # adaptive execution, memory and stability
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        "spark.dynamicAllocation.enabled",
        "spark.network.timeout",
        "spark.executor.heartbeatInterval",
        "spark.task.maxFailures",
        "spark.rpc.askTimeout",
        "spark.driver.extraJavaOptions",
        "spark.executor.extraJavaOptions",
        "spark.sql.parquet.compression.codec",
        "spark.sql.parquet.filterPushdown",
        "spark.local.dir",
        # UI and metrics
        "spark.ui.enabled",
        "spark.ui.liveUpdate.minFlushPeriod",
        "spark.ui.liveUpdate.period",
        "spark.ui.retainedTasks",
        "spark.ui.retainedStages",
        "spark.ui.retainedJobs",
        "spark.ui.retainedDeadExecutors",
        "spark.scheduler.listenerbus.eventqueue.appStatus.capacity",
        "spark.ui.prometheus.enabled",
        "spark.metrics.namespace",
        "spark.metrics.conf.*.sink.prometheusServlet.class",
        "spark.metrics.conf.*.sink.prometheusServlet.path",
        "spark.metrics.conf.master.sink.prometheusServlet.path",
        "spark.metrics.conf.applications.sink.prometheusServlet.path",
    }
)

# Owned keys no reachable manifest writes: the in-deployment dependency set
# writes spark.jars (and may write the others), and the Unity catalog path,
# which no supported combination reaches, writes a token. Owned now, so a
# user value can never replace the resolved set. The drift test checks
# written == owned - these.
OWNED_AHEAD_OF_WRITER: frozenset[str] = frozenset(
    {
        "spark.jars",
        "spark.jars.ivySettings",
        "spark.submit.pyFiles",
        "spark.files",
        "spark.driver.extraClassPath",
        "spark.executor.extraClassPath",
        "spark.sql.catalog.{catalog}.token",
    }
)

# Keys a job script sets with spark.conf.set (spark/scripts), always or
# while a step runs: a user value would not hold for the whole job.
SCRIPT_SET_SPARK_KEYS: frozenset[str] = frozenset(
    {
        "spark.sql.session.timeZone",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.sql.adaptive.coalescePartitions.minPartitionSize",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
        "spark.sql.requireAllClusterKeysForCoPartition",
    }
)

# Reserved by prefix: the pod shape and placement (spark.kubernetes.*) and
# the jars (spark.jars.*) are Lakebench's. The sizing prefixes cover memory,
# overhead and overhead factors, which would change the pod request outside
# compute_peak_requirements().
LAKEBENCH_OWNED_SPARK_PREFIXES: tuple[str, ...] = (
    "spark.kubernetes.",
    "spark.jars.",
    "spark.executor.memory",
    "spark.driver.memory",
    "spark.executor.cores",
    "spark.driver.cores",
    "spark.executor.instances",
    "spark.memory.offHeap.",
    "spark.executor.pyspark.memory",
)
LAKEBENCH_OWNED_SIZING_KEYS: frozenset[str] = frozenset(
    {
        "spark.executor.memory",
        "spark.executor.cores",
        "spark.executor.instances",
        "spark.executor.memoryOverhead",
        "spark.executor.memoryOverheadFactor",
        "spark.driver.memory",
        "spark.driver.memoryOverhead",
        "spark.driver.memoryOverheadFactor",
        "spark.driver.cores",
        "spark.memory.offHeap.enabled",
        "spark.memory.offHeap.size",
        "spark.executor.pyspark.memory",
    }
)

# What to change instead, for the owned keys a user is most likely to try.
_INSTEAD: dict[str, str] = {
    "spark.sql.shuffle.partitions": (
        "it is set per job from the executor count; change platform.compute.spark.<job>_executors"
    ),
    "spark.default.parallelism": (
        "it is set per job from the executor count; change platform.compute.spark.<job>_executors"
    ),
    "spark.executor.instances": "change platform.compute.spark.<job>_executors",
    "spark.driver.memory": "change platform.compute.spark.driver_memory",
    "spark.driver.cores": "change platform.compute.spark.driver_cores",
    "spark.hadoop.fs.s3a.endpoint": "change platform.storage.s3.endpoint",
    "spark.hadoop.fs.s3a.endpoint.region": "change platform.storage.s3.region",
    "spark.hadoop.fs.s3a.path.style.access": "change platform.storage.s3.path_style",
    "spark.jars": (
        "the jars are the deployment's resolved dependency set; change the table "
        "format version or images.spark"
    ),
    "spark.jars.packages": (
        "the jars are the deployment's resolved dependency set; change the table "
        "format version or images.spark"
    ),
}


def _expand(catalog: str) -> frozenset[str]:
    return frozenset(k.replace("{catalog}", catalog) for k in LAKEBENCH_OWNED_SPARK_KEYS)


def is_owned_spark_key(key: str, catalog: str = "lakehouse") -> bool:
    """Whether Lakebench writes *key* for every job (so a user value is lost)."""
    return (
        key in _expand(catalog)
        or key in LAKEBENCH_OWNED_SIZING_KEYS
        or key in SCRIPT_SET_SPARK_KEYS
        or key.startswith(LAKEBENCH_OWNED_SPARK_PREFIXES)
    )


def owned_key_reason(key: str, catalog: str = "lakehouse") -> str:
    """Why *key* cannot be set in spark.conf, and what to change instead."""
    instead = _INSTEAD.get(key)
    written = _expand(catalog) - {k.replace("{catalog}", catalog) for k in OWNED_AHEAD_OF_WRITER}
    if instead is None and (
        key in LAKEBENCH_OWNED_SIZING_KEYS
        or key.startswith(("spark.executor.memory", "spark.driver.memory", "spark.memory.offHeap."))
    ):
        instead = (
            "pod sizing is fixed in the job profiles and counted by the capacity check; "
            "use platform.compute.spark.<job>_executors, driver_memory or driver_cores"
        )
    if instead is None and key in SCRIPT_SET_SPARK_KEYS:
        instead = "a job script sets it, so a value here would not hold"
    if instead is None and key in written:
        instead = "Lakebench writes it for every job"
    if instead is None:
        instead = "reserved for Lakebench (pod placement and shape, jars)"
    return f"{key} is owned by Lakebench: {instead}"


# The v1.6 schema default of spark.conf. A config written with it (v1.6
# save_config, docs) carries owned keys at these values; v1.6 overwrote
# them, so they changed nothing and are dropped with a note.
V16_DEFAULT_SPARK_CONF: dict[str, str] = {
    "spark.hadoop.fs.s3a.connection.maximum": "500",
    "spark.hadoop.fs.s3a.threads.max": "200",
    "spark.hadoop.fs.s3a.fast.upload": "true",
    "spark.hadoop.fs.s3a.multipart.size": "268435456",
    "spark.hadoop.fs.s3a.fast.upload.active.blocks": "16",
    "spark.hadoop.fs.s3a.attempts.maximum": "20",
    "spark.hadoop.fs.s3a.retry.limit": "10",
    "spark.hadoop.fs.s3a.retry.interval": "500ms",
    "spark.sql.shuffle.partitions": "200",
    "spark.default.parallelism": "200",
    "spark.memory.fraction": "0.8",
    "spark.memory.storageFraction": "0.3",
}


def user_spark_overrides(conf: dict[str, str]) -> dict[str, str]:
    """The user's spark.conf entries that change what a job runs with: every
    key except one set to its ``SPARK_CONF_DEFAULTS`` value, sorted."""
    return {
        str(k): str(v) for k, v in sorted(conf.items()) if SPARK_CONF_DEFAULTS.get(str(k)) != str(v)
    }
