"""Deployment engine for Lakebench."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from lakebench.config import LakebenchConfig
from lakebench.config.schema import CatalogType, QueryEngineType, require_polaris_client_secret
from lakebench.k8s import K8sClient, K8sResourceError

logger = logging.getLogger(__name__)


def image_tag(image: str) -> str:
    """Extract the version tag from a container image reference.

    >>> image_tag("postgres:17")
    '17'
    >>> image_tag("apache/polaris:1.6.0")
    '1.6.0'
    >>> image_tag("myregistry.io/org/app")
    'latest'
    """
    # Handle digest references (image@sha256:...)
    if "@" in image:
        return image.split("@", 1)[1][:12]
    tag = image.rsplit(":", 1)[1] if ":" in image else "latest"
    return tag


# LB-146: the JVM heap must sit well below the container memory limit. The
# process also needs metaspace, thread stacks, direct buffers, code cache and
# GC structures outside the heap; with -Xmx equal to the limit the kernel
# OOM-kills the container (exit 137) before the JVM ever reports a heap OOM.
# Trino's deployment guidance is 70-85% of the memory available to the JVM.
JVM_HEAP_FRACTION = 0.8

# LB-157: how long deploy waits for a same-named namespace that an earlier
# destroy left Terminating before failing with an explicit message.
_TERMINATING_NAMESPACE_WAIT_SECONDS = 120

_MEM_UNITS_BYTES = {
    "Ki": 2**10,
    "Mi": 2**20,
    "Gi": 2**30,
    "Ti": 2**40,
    "K": 10**3,
    "k": 10**3,
    "M": 10**6,
    "G": 10**9,
    "T": 10**12,
}


def k8s_memory_bytes(quantity: str) -> int:
    """Parse a Kubernetes memory quantity (``8Gi``, ``4096Mi``, ``8G``) to bytes.

    Binary suffixes (Ki/Mi/Gi/Ti) and decimal suffixes (K/M/G/T) follow
    Kubernetes semantics, because the value is also the pod limit and the
    kubelet reads it that way. A bare number is bytes. Anything else
    (including Spark-style ``8g``, which Kubernetes rejects) raises.
    """
    s = str(quantity).strip()
    for unit in sorted(_MEM_UNITS_BYTES, key=len, reverse=True):
        if s.endswith(unit):
            number = s[: -len(unit)]
            break
    else:
        unit, number = "", s
    try:
        value = float(number)
    except ValueError:
        raise ValueError(f"unparseable memory quantity {quantity!r}") from None
    if value <= 0:
        raise ValueError(f"memory quantity must be positive, got {quantity!r}")
    return int(value * _MEM_UNITS_BYTES.get(unit, 1))


def jvm_heap_for_limit(limit: str, fraction: float = JVM_HEAP_FRACTION) -> str:
    """Return a JVM heap size in whole MiB (e.g. ``6553m``) for a container limit.

    The value is valid both as ``-Xmx<value>`` and as ``spark.driver.memory``;
    both read a lowercase ``m`` suffix as MiB.
    """
    heap_mib = int(k8s_memory_bytes(limit) * fraction) // 2**20
    if heap_mib < 1:
        raise ValueError(f"memory limit {limit!r} is too small for a JVM heap")
    return f"{heap_mib}m"


# LB-148: the Spark Thrift pod limit is the heap plus a non-heap allowance.
# Spark's own rule for a JVM container is max(10% of heap, 384 MiB). The
# 384 MiB floor is sized for executors; the Thrift JVM is a driver that also
# loads Hive, Delta or Iceberg and the S3A client (metaspace plus code cache
# alone run to several hundred MiB) and runs HiveServer2's handler threads,
# so the floor here is 1 GiB. At the 4g default that gives a 5Gi pod; with
# heap == limit (the pre-LB-148 shape) the pod sat at 4014Mi of 4Gi mid-query.
THRIFT_OVERHEAD_FACTOR = 0.10
THRIFT_MIN_OVERHEAD_BYTES = 2**30

_SPARK_MEM_UNITS_BYTES = {
    "b": 1,
    "k": 2**10,
    "kb": 2**10,
    "m": 2**20,
    "mb": 2**20,
    "g": 2**30,
    "gb": 2**30,
    "t": 2**40,
    "tb": 2**40,
    "p": 2**50,
    "pb": 2**50,
}
_SPARK_MEM_RE = re.compile(r"([0-9]+)([a-z]+)?")


def spark_memory_bytes(value: str) -> int:
    """Parse a Spark memory string (``4g``, ``4096m``, ``24G``, ``8gb``) to bytes.

    Follows Spark's ``JavaUtils.byteStringAs``: a whole number with an
    optional binary suffix, case-insensitive, no fractions. A bare number is
    MiB, which is how Spark reads ``spark.driver.memory``. Anything Spark
    would reject raises ``ValueError`` here, before a pod is rendered.
    """
    s = str(value).strip().lower()
    m = _SPARK_MEM_RE.fullmatch(s)
    if not m or (m.group(2) and m.group(2) not in _SPARK_MEM_UNITS_BYTES):
        raise ValueError(f"unparseable Spark memory {value!r}")
    amount = int(m.group(1))
    if amount <= 0:
        raise ValueError(f"Spark memory must be positive, got {value!r}")
    return amount * _SPARK_MEM_UNITS_BYTES[m.group(2) or "m"]


def thrift_pod_memory_limit(heap: str) -> str:
    """Pod memory limit (whole MiB, e.g. ``5120Mi``) for a Spark Thrift heap.

    Always strictly above the heap: heap + max(10% of heap, 1 GiB).
    """
    heap_bytes = spark_memory_bytes(heap)
    overhead = max(int(heap_bytes * THRIFT_OVERHEAD_FACTOR), THRIFT_MIN_OVERHEAD_BYTES)
    limit_mib = -(-(heap_bytes + overhead) // 2**20)
    return f"{limit_mib}Mi"


# Trino query memory is sized from the deployed workers. Unset, the
# cluster-wide cap query.max-memory is a flat 20GB whatever the worker count,
# so four 48Gi workers (about 107 GB of memory pool) still failed AML FQ3 at
# scale 100 with "exceeded distributed user memory limit of 20GB".
#
# Per node, Trino splits the heap into headroom (untracked allocations) and a
# memory pool (heap minus headroom) that all queries share. The headroom stays
# at Trino's own 30% default, so the pool is 70% of the heap. One query may
# take 35% of the heap, half the pool, so two cap-sized queries fit a node at
# once with nothing spare; a third stream, or untracked system allocations
# past the headroom, block. Trino's 30% default fits two with 10% of the
# heap spare, and three not at all. This gives the power run (one query at
# a time) 17% more than the default. Past the pool, Trino blocks queries
# and only after query.low-memory-killer.delay (5 minutes, longer than the
# 300 s client timeout) kills one, so a blocked stream reads as a timeout.
# Trino refuses to start unless per-node + headroom <= heap; here they sum
# to 65%.
TRINO_QUERY_MEMORY_PER_NODE_FRACTION = 0.35
TRINO_HEAP_HEADROOM_FRACTION = 0.3


def _heap_mib(heap: str) -> int:
    """MiB of a JVM heap string produced by ``jvm_heap_for_limit`` (``39321m``)."""
    m = re.fullmatch(r"([0-9]+)m", str(heap).strip())
    if not m or int(m.group(1)) < 1:
        raise ValueError(f"unexpected JVM heap {heap!r}")
    return int(m.group(1))


def trino_memory_properties(
    coordinator_heap: str, worker_heap: str, workers: int
) -> dict[str, str]:
    """Trino memory properties (Trino DataSize strings; Trino's MB is MiB).

    Per-node values follow each role's own heap. The cluster-wide cap follows
    the workers, because with ``node-scheduler.include-coordinator=false`` only
    workers hold query memory:

    - ``query.max-memory`` (user memory, cluster) = workers x worker per-node.

    ``query.max-total-memory`` (user + revocable) is left at Trino's default,
    twice ``query.max-memory``, which here is about the workers' physical
    pool. Pinning it to exactly that pool added a failure mode in review:
    Trino sums every node, the coordinator included, so a coordinator
    reservation plus full revocable use on the workers could trip it.
    """
    workers = max(1, workers)
    out: dict[str, str] = {}
    for role, heap in (("coordinator", coordinator_heap), ("worker", worker_heap)):
        mib = _heap_mib(heap)
        out[f"{role}_max_memory_per_node"] = f"{int(mib * TRINO_QUERY_MEMORY_PER_NODE_FRACTION)}MB"
        out[f"{role}_heap_headroom"] = f"{int(mib * TRINO_HEAP_HEADROOM_FRACTION)}MB"
    per_node = int(_heap_mib(worker_heap) * TRINO_QUERY_MEMORY_PER_NODE_FRACTION)
    out["max_memory"] = f"{workers * per_node}MB"
    return out


def _try_cleanup_orphan_bucket(boto_client, bucket: str) -> None:
    """Delete a freshly-created bucket when ownership was refused.

    Round-3 F5: the naive ``delete_bucket`` call raced a concurrent
    writer who could have written objects between our CreateBucket
    and our refusal. If the bucket is non-empty at cleanup time we
    do NOT delete -- silently emptying-then-deleting could destroy
    data written by a racer. Instead we log a warning naming the
    orphan and leave it for the operator to investigate.

    All exceptions are caught and logged; the caller is already
    refusing the deploy for a different reason and cleanup is
    best-effort.
    """
    try:
        r = boto_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
    except Exception:  # noqa: BLE001
        logger.warning(
            "orphan bucket %s: could not verify empty before delete; "
            "leaving in place (operator: investigate).",
            bucket,
            exc_info=True,
        )
        return
    if r.get("KeyCount", 0) > 0 or r.get("Contents"):
        logger.warning(
            "orphan bucket %s: contains objects (a concurrent process "
            "wrote between our CreateBucket and our ownership refusal). "
            "Not deleting -- operator: investigate whether a racing "
            "lakebench deploy or an unrelated writer needs this data.",
            bucket,
        )
        return
    try:
        boto_client.delete_bucket(Bucket=bucket)
        logger.info(
            "deleted freshly-created bucket %s after ownership refusal (empty at cleanup time).",
            bucket,
        )
    except Exception:  # noqa: BLE001
        logger.warning(
            "could not delete freshly-created bucket %s; may leave an orphan on the backend.",
            bucket,
            exc_info=True,
        )


class DeploymentStatus(Enum):
    """Status of a deployment step."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class DeploymentResult:
    """Result of a deployment step."""

    component: str
    status: DeploymentStatus
    message: str
    elapsed_seconds: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)
    label: str = ""  # Short display name (e.g. "PostgreSQL")
    detail: str = ""  # Version or context (e.g. "17")


@dataclass
class DeploymentStep:
    """A single deployment step."""

    name: str
    component: str
    deploy_fn: Callable[[], DeploymentResult]
    depends_on: list[str] = field(default_factory=list)


class TemplateRenderer:
    """Renders Jinja2 templates for Kubernetes manifests."""

    def __init__(self, template_dir: Path | None = None):
        """Initialize template renderer.

        Args:
            template_dir: Path to templates directory. Defaults to package templates.
        """
        if template_dir is None:
            # Use package templates (supports dev, pip install, and PyInstaller)
            from lakebench._resources import get_templates_dir

            template_dir = get_templates_dir()

        self.env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=select_autoescape(["yaml", "yml", "j2"]),
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
        )

    def render(self, template_name: str, context: dict[str, Any]) -> str:
        """Render a template with the given context.

        Args:
            template_name: Name of template file (e.g., "postgres/statefulset.yaml.j2")
            context: Template variables

        Returns:
            Rendered YAML string
        """
        template = self.env.get_template(template_name)
        return template.render(**context)

    def render_all(self, template_names: list[str], context: dict[str, Any]) -> list[str]:
        """Render multiple templates with the same context.

        Args:
            template_names: List of template file names
            context: Template variables

        Returns:
            List of rendered YAML strings
        """
        return [self.render(name, context) for name in template_names]


class DeploymentEngine:
    """Orchestrates deployment of Lakebench components.

    Deployment order (from spec Section 2.1):
    1. Namespace + Secrets + ConfigMaps
    2. S3 bucket validation/creation
    3. PostgreSQL (StatefulSet + Service)
    4. Catalog Service (Hive Metastore / Polaris / Unity)
    5. Query Engine (Trino / Spark Thrift Server)
    6. Spark Operator (if not already installed)
    7. Spark RBAC (ServiceAccount, Role, RoleBinding)
    8. Monitoring Stack (if enabled)
    """

    def __init__(
        self,
        config: LakebenchConfig,
        k8s_client: K8sClient | None = None,
        dry_run: bool = False,
    ):
        """Initialize deployment engine.

        Args:
            config: Lakebench configuration
            k8s_client: Kubernetes client (created if not provided)
            dry_run: If True, show what would be deployed without making changes
        """
        self.config = config
        self.dry_run = dry_run
        self.results: list[DeploymentResult] = []
        # PR-1-F1: if we create the namespace in this run, remember it so
        # a transient stamp failure + retry does not turn the namespace
        # from "we created it" into "unowned legacy" that then requires
        # the dangerous --force-legacy flag.
        self._namespace_created_this_run: set[str] = set()

        if k8s_client:
            self.k8s = k8s_client
        else:
            from lakebench.k8s import get_k8s_client

            self.k8s = get_k8s_client(
                context=config.platform.kubernetes.context,
                namespace=config.get_namespace(),
            )

        # Template renderer
        self.renderer = TemplateRenderer()

        # Auto-size resources based on scale + cluster capacity
        from lakebench.config.autosizer import resolve_auto_sizing

        cluster_cap = None
        if not dry_run and self.k8s is not None:
            try:
                cluster_cap = self.k8s.get_cluster_capacity()
            except Exception as e:
                logger.warning("Could not get cluster capacity for auto-sizing: %s", e)
        resolve_auto_sizing(config, cluster_cap)

        self.context = self._build_context()

    def _detect_openshift(self) -> bool:
        """Detect if running on OpenShift.

        Returns:
            True if OpenShift is detected
        """
        try:
            from kubernetes import client as k8s_client

            api = k8s_client.ApisApi()
            groups = api.get_api_versions()
            for group in groups.groups:
                if "openshift.io" in group.name:
                    return True
            return False
        except Exception:
            return False

    @staticmethod
    def _get_spark_major_minor(cfg: Any) -> str:
        """Extract Spark major.minor from image tag."""
        tag = cfg.images.spark.split(":")[-1]
        return ".".join(tag.split(".")[:2])

    @staticmethod
    def _trino_heap(cfg: Any, limit: str) -> str:
        """Trino -Xmx for a pod memory limit (LB-146).

        Strict only when Trino is the active query engine: an unparseable
        Trino memory on a spark-thrift or duckdb deployment must not stop
        that deployment's destroy, which also builds this context.
        """
        if cfg.architecture.query_engine.type == QueryEngineType.TRINO:
            return jvm_heap_for_limit(limit)
        try:
            return jvm_heap_for_limit(limit)
        except ValueError:
            return ""

    @classmethod
    def _trino_memory(cls, cfg: Any) -> dict[str, str]:
        """Trino query memory properties for the configured heaps and workers.

        Empty when either heap is unavailable (a non-Trino deployment with an
        unparseable Trino memory, see ``_trino_heap``); the template then
        leaves Trino's defaults in place.
        """
        trino = cfg.architecture.query_engine.trino
        coord_heap = cls._trino_heap(cfg, trino.coordinator.memory)
        worker_heap = cls._trino_heap(cfg, trino.worker.memory)
        if not coord_heap or not worker_heap:
            return {}
        return trino_memory_properties(coord_heap, worker_heap, trino.worker.replicas)

    @staticmethod
    def _thrift_pod_memory(cfg: Any) -> str:
        """Spark Thrift pod memory limit for the configured heap (LB-148).

        Strict only when Spark Thrift is the active query engine, for the
        same reason as ``_trino_heap``: destroy builds this context too.
        """
        heap = cfg.architecture.query_engine.spark_thrift.memory
        if cfg.architecture.query_engine.type == QueryEngineType.SPARK_THRIFT:
            return thrift_pod_memory_limit(heap)
        try:
            return thrift_pod_memory_limit(heap)
        except ValueError:
            return ""

    @staticmethod
    def _spark_mem_to_k8s(spark_mem: str) -> str:
        """Convert Spark memory format (e.g. ``4g``) to K8s format (e.g. ``4Gi``)."""
        s = spark_mem.strip().lower()
        if s.endswith("g"):
            return s[:-1] + "Gi"
        if s.endswith("m"):
            return s[:-1] + "Mi"
        return spark_mem

    @staticmethod
    def _build_spark_thrift_packages(cfg: Any) -> str:
        """Build ``spark.jars.packages`` CSV for Spark Thrift Server.

        Format-aware: uses Iceberg or Delta packages depending on the
        configured table format.
        """
        from lakebench.spark.job import (
            _ICEBERG_RUNTIME_SUFFIX,
            _delta_spark_artifact,
            _parse_spark_major,
            _parse_spark_major_minor,
            _spark_compat,
        )

        table_format = cfg.architecture.table_format.type.value
        scala_suffix, hadoop_version, aws_sdk_version = _spark_compat(cfg.images.spark)

        if table_format == "delta":
            delta_version = cfg.architecture.table_format.delta.version
            catalog_type = cfg.architecture.catalog.type.value
            packages = [
                _delta_spark_artifact(scala_suffix, delta_version),
                f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
            ]
            if catalog_type == "unity":
                unity_version = cfg.architecture.catalog.unity.spark_connector_version
                packages.append(f"io.unitycatalog:unitycatalog-spark{scala_suffix}:{unity_version}")
        else:
            iceberg_version = cfg.architecture.table_format.iceberg.version
            key = _parse_spark_major_minor(cfg.images.spark)
            iceberg_suffix = _ICEBERG_RUNTIME_SUFFIX.get(key, f"{key[0]}.{key[1]}")
            packages = [
                f"org.apache.iceberg:iceberg-spark-runtime-{iceberg_suffix}{scala_suffix}:{iceberg_version}",
                f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}",
                f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
            ]
        if _parse_spark_major(cfg.images.spark) < 4:
            packages.append(
                f"com.amazonaws:aws-java-sdk-bundle:{aws_sdk_version}",
            )
        return ",".join(packages)

    @staticmethod
    def _read_ca_cert_pem(path: str) -> str:
        """Read PEM certificate file content for embedding in K8s Secret.

        Returns empty string if no path is provided.
        """
        if not path:
            return ""
        from pathlib import Path

        cert_path = Path(path)
        if not cert_path.is_file():
            raise FileNotFoundError(f"CA certificate not found: {path}")
        return cert_path.read_text()

    def _build_context(self) -> dict[str, Any]:
        """Build template context from configuration."""
        cfg = self.config
        s3 = cfg.platform.storage.s3

        # Detect OpenShift for SCC-aware templates
        openshift_mode = self._detect_openshift()

        # Parse S3 endpoint for Stackable (needs host and port separately)
        from urllib.parse import urlparse

        from lakebench.spark.job import _MAVEN_MIRROR_REPOS, _spark_compat

        parsed_s3 = urlparse(s3.endpoint)
        s3_host = (
            parsed_s3.hostname
            or s3.endpoint.replace("http://", "").replace("https://", "").split(":")[0]
        )
        s3_port = parsed_s3.port or (443 if parsed_s3.scheme == "https" else 80)

        return {
            # Core
            "name": cfg.name,
            "namespace": cfg.get_namespace(),
            "openshift_mode": openshift_mode,
            # Images
            "postgres_image": cfg.images.postgres,
            "hive_image": cfg.images.hive,
            "trino_image": cfg.images.trino,
            "spark_image": cfg.images.spark,
            "jmx_exporter_image": cfg.images.jmx_exporter,
            "image_pull_policy": cfg.images.pull_policy.value,
            # S3
            "s3_endpoint": s3.endpoint,
            "s3_host": s3_host,  # For Stackable HiveCluster
            "s3_port": s3_port,  # For Stackable HiveCluster
            "s3_scheme": parsed_s3.scheme or "http",
            "s3_use_ssl": parsed_s3.scheme == "https",
            "s3_ca_cert_pem": self._read_ca_cert_pem(s3.ca_cert),
            "s3_verify_ssl": s3.verify_ssl,
            "s3_region": s3.region,
            "s3_path_style": s3.path_style,
            "s3_access_key": s3.access_key,
            "s3_secret_key": s3.secret_key,
            "bucket_bronze": s3.buckets.bronze,
            "bucket_silver": s3.buckets.silver,
            "bucket_gold": s3.buckets.gold,
            # PostgreSQL
            "postgres_storage": cfg.platform.compute.postgres.storage,
            "storage_class": cfg.platform.compute.postgres.storage_class or None,
            # Catalog type (used in conditional templates)
            "catalog_type": cfg.architecture.catalog.type.value,
            # Table format type (iceberg or delta)
            "table_format_type": cfg.architecture.table_format.type.value,
            # Hive
            "hive_cpu_min": cfg.architecture.catalog.hive.resources.cpu_min,
            "hive_cpu_max": cfg.architecture.catalog.hive.resources.cpu_max,
            "hive_memory": cfg.architecture.catalog.hive.resources.memory,
            "hive_thrift_min_threads": cfg.architecture.catalog.hive.thrift.min_threads,
            "hive_thrift_max_threads": cfg.architecture.catalog.hive.thrift.max_threads,
            "hive_client_timeout": cfg.architecture.catalog.hive.thrift.client_timeout,
            # Polaris
            "polaris_version": cfg.architecture.catalog.polaris.version,
            "polaris_image": cfg.images.polaris,
            "polaris_admin_tool_image": cfg.images.polaris_admin_tool,
            "polaris_port": cfg.architecture.catalog.polaris.port,
            "polaris_cpu": cfg.architecture.catalog.polaris.resources.cpu,
            "polaris_memory": cfg.architecture.catalog.polaris.resources.memory,
            # LB-090: only require the secret when we're actually deploying
            # Polaris; hive/unity deploys never touch the templates that
            # consume it, and `lakebench validate` / `info` on a Polaris
            # config without a secret would otherwise be blocked from ever
            # printing the config that names the missing field.
            "polaris_client_secret": (
                require_polaris_client_secret(cfg)
                if cfg.architecture.catalog.type == CatalogType.POLARIS
                else ""
            ),
            # Unity
            "unity_image": cfg.images.unity,
            "unity_port": cfg.architecture.catalog.unity.port,
            "unity_cpu": cfg.architecture.catalog.unity.resources.cpu,
            "unity_memory": cfg.architecture.catalog.unity.resources.memory,
            # Trino
            "trino_coordinator_cpu": cfg.architecture.query_engine.trino.coordinator.cpu,
            "trino_coordinator_memory": cfg.architecture.query_engine.trino.coordinator.memory,
            "trino_worker_replicas": cfg.architecture.query_engine.trino.worker.replicas,
            "trino_worker_cpu": cfg.architecture.query_engine.trino.worker.cpu,
            "trino_worker_memory": cfg.architecture.query_engine.trino.worker.memory,
            # LB-146: heap is a fraction of the pod limit, never equal to it.
            "trino_coordinator_heap": self._trino_heap(
                cfg, cfg.architecture.query_engine.trino.coordinator.memory
            ),
            "trino_worker_heap": self._trino_heap(
                cfg, cfg.architecture.query_engine.trino.worker.memory
            ),
            # Query memory limits sized from the heaps and the worker count.
            "trino_memory": self._trino_memory(cfg),
            "trino_catalog_name": cfg.architecture.query_engine.trino.catalog_name,
            # Trino worker storage (StatefulSet PVCs + spill)
            "trino_worker_spill_enabled": cfg.architecture.query_engine.trino.worker.spill_enabled,
            "trino_worker_spill_max": cfg.architecture.query_engine.trino.worker.spill_max_per_node,
            "trino_worker_storage": cfg.architecture.query_engine.trino.worker.storage,
            "trino_worker_storage_class": cfg.architecture.query_engine.trino.worker.storage_class
            or None,
            # Scratch StorageClass
            "scratch_storage_class": cfg.platform.storage.scratch.storage_class,
            "scratch_provisioner": cfg.platform.storage.scratch.provisioner,
            "scratch_parameters": cfg.platform.storage.scratch.parameters,
            # Spark
            "spark_driver_cores": cfg.platform.compute.spark.driver.cores,
            "spark_driver_memory": cfg.platform.compute.spark.driver.memory,
            "spark_executor_instances": cfg.platform.compute.spark.executor.instances,
            "spark_executor_cores": cfg.platform.compute.spark.executor.cores,
            "spark_executor_memory": cfg.platform.compute.spark.executor.memory,
            "spark_executor_memory_overhead": cfg.platform.compute.spark.executor.memory_overhead,
            # Spark Thrift Server
            "spark_thrift_cores": cfg.architecture.query_engine.spark_thrift.cores,
            "spark_thrift_memory": cfg.architecture.query_engine.spark_thrift.memory,
            # LB-148: pod limit = heap + overhead, never heap == limit.
            "spark_thrift_memory_k8s": self._thrift_pod_memory(cfg),
            "spark_thrift_catalog_name": cfg.architecture.query_engine.spark_thrift.catalog_name,
            "query_engine_type": cfg.architecture.query_engine.type.value,
            # Spark Thrift packages (computed from config versions)
            "spark_thrift_packages": self._build_spark_thrift_packages(cfg),
            # Fallback Maven mirror -- Ivy falls to this when Central 429s
            # on the cluster's egress IP (see _MAVEN_MIRROR_REPOS in
            # spark/job.py for the rationale).
            "spark_thrift_repositories": _MAVEN_MIRROR_REPOS,
            "spark_major_minor": self._get_spark_major_minor(cfg),
            "scala_suffix": _spark_compat(cfg.images.spark)[0],
            # DuckDB
            "duckdb_image": cfg.images.duckdb,
            "duckdb_cores": cfg.architecture.query_engine.duckdb.cores,
            "duckdb_memory": cfg.architecture.query_engine.duckdb.memory,
            "duckdb_memory_k8s": self._spark_mem_to_k8s(
                cfg.architecture.query_engine.duckdb.memory
            ),
            "duckdb_catalog_name": cfg.architecture.query_engine.duckdb.catalog_name,
            "duckdb_version": cfg.architecture.query_engine.duckdb.version,
            # Observability
            "observability_enabled": cfg.observability.enabled,
        }

    def deploy_all(
        self,
        progress_callback: Callable[[str, DeploymentStatus, str], None] | None = None,
        timeout: int = 3600,
        force_legacy: bool = False,
    ) -> list[DeploymentResult]:
        """Deploy all components in order.

        Args:
            progress_callback: Optional callback for progress updates
                               (component, status, message)
            timeout: Global deployment timeout in seconds (0 = no timeout).
                     Checked between steps -- does not interrupt a step in progress.
            force_legacy: Claim ownership of a pre-existing annotation-less
                     namespace and untagged buckets. Use only when
                     migrating a pre-ownership-taxonomy deployment; a
                     mistake here can silently take over another team's
                     storage.

        Returns:
            List of deployment results
        """
        from .duckdb import DuckDBDeployer
        from .hive import HiveDeployer
        from .observability import ObservabilityDeployer
        from .polaris import PolarisDeployer
        from .postgres import PostgresDeployer
        from .rbac import RBACDeployer
        from .spark_thrift import SparkThriftDeployer
        from .trino import TrinoDeployer
        from .unity import UnityDeployer

        # Initialize deployers
        postgres = PostgresDeployer(self)
        hive = HiveDeployer(self)
        polaris = PolarisDeployer(self)
        unity = UnityDeployer(self)
        trino = TrinoDeployer(self)
        spark_thrift = SparkThriftDeployer(self)
        duckdb = DuckDBDeployer(self)
        rbac = RBACDeployer(self)
        observability = ObservabilityDeployer(self)

        # Both HiveDeployer and PolarisDeployer have self-skip guards.
        # Only the one matching config.architecture.catalog.type deploys;
        # the other returns SKIPPED.
        steps = [
            (
                "namespace",
                "Creating namespace",
                lambda: self._deploy_namespace(force_legacy=force_legacy),
            ),
            ("secrets", "Creating secrets", self._deploy_secrets),
            (
                "s3-buckets",
                "Creating S3 buckets",
                lambda: self._deploy_buckets(force_legacy=force_legacy),
            ),
            ("scratch-sc", "Creating scratch StorageClass", self._deploy_scratch_storageclass),
            ("postgres", "Deploying PostgreSQL", postgres.deploy),
            ("hive", "Deploying Hive Metastore", hive.deploy),
            ("polaris", "Deploying Polaris Catalog", polaris.deploy),
            ("rbac", "Creating Spark RBAC", rbac.deploy),
            ("unity", "Deploying Unity Catalog", unity.deploy),
            ("spark-operator", "Installing Spark Operator", self._deploy_spark_operator),
            ("trino", "Deploying Trino", trino.deploy),
            ("spark-thrift", "Deploying Spark Thrift Server", spark_thrift.deploy),
            ("duckdb", "Deploying DuckDB", duckdb.deploy),
            ("observability", "Deploying Observability Stack", observability.deploy),
        ]

        import time

        deadline = time.monotonic() + timeout if timeout > 0 else float("inf")

        for component, description, deploy_fn in steps:
            # Global timeout check (between steps, never interrupts mid-step)
            if time.monotonic() > deadline:
                result = DeploymentResult(
                    component=component,
                    status=DeploymentStatus.FAILED,
                    message=f"Global deployment timeout ({timeout}s) exceeded",
                )
                self.results.append(result)
                if progress_callback:
                    progress_callback(component, DeploymentStatus.FAILED, result.message)
                break

            if progress_callback:
                progress_callback(component, DeploymentStatus.IN_PROGRESS, description)

            result = None  # type: ignore[assignment]
            for attempt in range(2):  # 0 = first try, 1 = retry
                try:
                    result = deploy_fn()
                    break
                except Exception as e:
                    if attempt == 0 and self._is_transient_error(e):
                        logger.warning(
                            "Deployment step '%s' hit transient error, retrying in 5s: %s",
                            component,
                            e,
                        )
                        time.sleep(5)
                        continue
                    # Non-transient or second attempt: fail
                    result = DeploymentResult(
                        component=component,
                        status=DeploymentStatus.FAILED,
                        message=str(e),
                    )
                    break

            self.results.append(result)

            if progress_callback:
                progress_callback(component, result.status, result.message)

            # Stop on failure
            if result.status == DeploymentStatus.FAILED:
                break

        return self.results

    @staticmethod
    def _is_transient_error(exc: Exception) -> bool:
        """Return True if the exception is likely transient (worth retrying)."""
        from kubernetes.client.rest import ApiException

        if isinstance(exc, ApiException) and exc.status in (429, 500, 502, 503, 504):
            return True
        if isinstance(exc, (ConnectionError, TimeoutError)):
            return True
        exc_name = type(exc).__name__
        if "MaxRetryError" in exc_name or "NewConnectionError" in exc_name:
            return True
        return False

    def _namespace_already_using_name(self, namespace: str) -> str | None:
        """Return another namespace that carries this deployment's name, if any.

        Enumeration failures return None (not a refusal): the bucket step
        already refuses name-based ownership when it cannot list siblings.
        """
        try:
            from kubernetes import client as _kclient

            from lakebench.deploy.ownership import ANNOTATION_DEPLOYMENT_NAME
            from lakebench.k8s import get_k8s_client as _get_k8s

            _get_k8s(
                context=self.config.platform.kubernetes.context or "",
                namespace=namespace,
            )
            for n in _kclient.CoreV1Api().list_namespace().items:
                if n.metadata.name == namespace:
                    continue
                if getattr(n.metadata, "deletion_timestamp", None):
                    continue  # being deleted; its buckets go with it
                anns = n.metadata.annotations or {}
                labels = n.metadata.labels or {}
                if anns.get(ANNOTATION_DEPLOYMENT_NAME) == self.config.name:
                    return str(n.metadata.name)
                # Pre-annotation (legacy) deployments are identified by the
                # managed-by label and their namespace name.
                if (
                    not anns.get(ANNOTATION_DEPLOYMENT_NAME)
                    and labels.get("app.kubernetes.io/managed-by") == "lakebench"
                    and n.metadata.name == self.config.name
                ):
                    return str(n.metadata.name)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "Could not check that deployment name %r is unique on the cluster "
                "(%s). Two deployments with one name share buckets.",
                self.config.name,
                e,
            )
        return None

    def _deploy_namespace(self, force_legacy: bool = False) -> DeploymentResult:
        """Deploy namespace and stamp ownership annotations.

        The stamp is the anchor for the "delete A does not affect B"
        invariant: destroy compares it before touching any resource, and
        a foreign stamp is a hard refuse. See
        dev-artifacts/DESIGN-namespace-isolation.md.
        """
        import time

        start = time.time()

        namespace = self.config.get_namespace()

        if self.dry_run:
            return DeploymentResult(
                component="namespace",
                status=DeploymentStatus.SUCCESS,
                message=f"Would create namespace: {namespace}",
                elapsed_seconds=0,
            )

        # A deployment name must be unique across namespaces. Bucket ownership
        # is keyed on the name (a tag, or a name prefix on FlashBlade), so two
        # namespaces carrying the same name both claim the same buckets, and
        # destroying either one deletes the other's data.
        duplicate = self._namespace_already_using_name(namespace)
        if duplicate:
            return DeploymentResult(
                component="namespace",
                status=DeploymentStatus.FAILED,
                message=(
                    f"Deployment name {self.config.name!r} is already used by "
                    f"namespace {duplicate!r}. Deployment names must be unique on "
                    "a cluster because bucket ownership is keyed on the name; "
                    "choose a different `name:` in the config."
                ),
                elapsed_seconds=time.time() - start,
            )

        # Check if namespace exists and wait if it's terminating
        pre_existing = False
        if self.k8s.namespace_exists(namespace):
            phase = self.k8s.get_namespace_phase(namespace)
            if phase == "Terminating":
                # LB-157: an earlier destroy of this name is still finishing.
                # Creating into it fails with a confusing 403/409 from the API
                # server, so wait a bounded time and then say what is wrong.
                try:
                    self.k8s.wait_for_namespace_deleted(
                        namespace, timeout=_TERMINATING_NAMESPACE_WAIT_SECONDS
                    )
                except K8sResourceError:
                    blockers: list[str] = []
                    try:
                        _, blockers = self.k8s.get_namespace_termination_status(namespace)
                    except Exception:  # noqa: BLE001
                        pass
                    remaining = f" Remaining: {'; '.join(blockers)}." if blockers else ""
                    return DeploymentResult(
                        component="namespace",
                        status=DeploymentStatus.FAILED,
                        message=(
                            f"Namespace {namespace!r} is still terminating from an "
                            f"earlier destroy after waiting "
                            f"{_TERMINATING_NAMESPACE_WAIT_SECONDS}s.{remaining} "
                            "A namespace cannot be re-created while it is being "
                            f"deleted; wait until `kubectl get ns {namespace}` "
                            "returns NotFound, then re-run deploy."
                        ),
                        elapsed_seconds=time.time() - start,
                    )
            else:
                pre_existing = True

        # Create namespace when missing.
        created = False
        if not pre_existing:
            if not self.config.platform.kubernetes.create_namespace:
                return DeploymentResult(
                    component="namespace",
                    status=DeploymentStatus.FAILED,
                    message=(f"Namespace '{namespace}' does not exist and create_namespace=false"),
                    elapsed_seconds=time.time() - start,
                )
            yaml_content = self.renderer.render("namespace.yaml.j2", self.context)
            import yaml

            manifest = yaml.safe_load(yaml_content)
            # F-2: seed the tracking BEFORE apply. If K8s accepts the
            # create but the response is lost to a network reset, our
            # retry sees `namespace_exists=True` with the tracking bit
            # already set, so we correctly stamp with force_legacy.
            # If apply fails and K8s did NOT create, the retry sees no
            # namespace, creates fresh, and force_legacy stays correct.
            # If a foreign namespace with the same name appears between
            # attempts, stamp_namespace's foreign-identity refuse cold
            # branch still catches it before writing.
            self._namespace_created_this_run.add(namespace)
            try:
                self.k8s.apply_manifest(manifest)
            except Exception as e:
                # F2-A: on a non-transient failure (K8s definitively
                # rejected the create), un-seed the tracking so a
                # caller reusing this engine cannot silently claim a
                # legacy annotation-less namespace that appeared later
                # under the same name.
                if not self._is_transient_error(e):
                    self._namespace_created_this_run.discard(namespace)
                raise
            created = True

        # Stamp deployment identity onto the namespace. A pre-existing
        # legacy (annotation-less) namespace requires force_legacy=True to
        # claim; otherwise deploy refuses and points at admin migration
        # (which lands in PR-2). New namespaces we created ourselves are
        # stamped unconditionally.
        from kubernetes import client as _kclient

        from lakebench.deploy.ownership import (
            IdentityVerdict,
            build_identity_from_config,
            stamp_namespace,
        )

        identity = build_identity_from_config(
            self.config,
            context=self.config.platform.kubernetes.context or "",
        )
        core_v1 = _kclient.CoreV1Api()
        # PR-1-F1: force_legacy on the FIRST create OR on any retry after a
        # transient failure that saw us create the namespace earlier.
        we_own_it = created or namespace in self._namespace_created_this_run
        stamp = stamp_namespace(
            core_v1,
            namespace,
            deployment_name=identity.name,
            api_server=identity.api_server,
            committed_sha=identity.committed_sha,
            force_legacy=force_legacy or we_own_it,
        )

        if stamp.verdict is IdentityVerdict.MISMATCH:
            # R5: distinguish "raced a competing create" from "existing
            # foreign deployment" so the operator knows whether to
            # abandon this config name or reconcile. F1 covers the same
            # case on retry after a transient stamp failure.
            extra = ""
            if we_own_it:
                extra = (
                    " Your create raced a competing deploy that stamped "
                    "the namespace first. No lakebench resources were "
                    "created here (only the namespace label). It is safe "
                    "to abandon this config name and pick a different "
                    "deployment name."
                )
            return DeploymentResult(
                component="namespace",
                status=DeploymentStatus.FAILED,
                message=f"Namespace ownership refused: {stamp.hint}{extra}",
                elapsed_seconds=time.time() - start,
            )
        if stamp.verdict is IdentityVerdict.ABSENT:
            return DeploymentResult(
                component="namespace",
                status=DeploymentStatus.FAILED,
                message=(
                    f"Namespace '{namespace}' exists without lakebench "
                    "identity annotations. Pass --force-legacy on `lakebench "
                    "deploy` to claim it (destroy still refuses until "
                    "the admin migrate-deployment command ships)."
                ),
                elapsed_seconds=time.time() - start,
            )

        # Every deploy stamps a new nonce, so a destroy already running on
        # this namespace sees the redeploy and stops before it touches the
        # new deployment's jobs, tables or buckets.
        from lakebench.deploy.ownership import write_deploy_nonce

        try:
            write_deploy_nonce(core_v1, namespace)
        except Exception as e:  # noqa: BLE001
            return DeploymentResult(
                component="namespace",
                status=DeploymentStatus.FAILED,
                message=(
                    f"Could not stamp the deploy nonce on namespace {namespace!r} ({e}); "
                    "a concurrent destroy could not tell this deploy apart. Re-run deploy."
                ),
                elapsed_seconds=time.time() - start,
            )

        msg = (
            f"Created namespace: {namespace}"
            if created
            else f"Namespace '{namespace}' already exists"
        )
        return DeploymentResult(
            component="namespace",
            status=DeploymentStatus.SUCCESS,
            message=msg,
            elapsed_seconds=time.time() - start,
            label="Namespace",
            detail=namespace,
        )

    def _deploy_secrets(self) -> DeploymentResult:
        """Deploy secrets (S3 credentials, PostgreSQL credentials)."""
        import time

        import yaml

        start = time.time()

        if self.dry_run:
            return DeploymentResult(
                component="secrets",
                status=DeploymentStatus.SUCCESS,
                message="Would create secrets",
                elapsed_seconds=0,
            )

        namespace = self.config.get_namespace()

        # Render and apply secrets
        yaml_content = self.renderer.render("secrets.yaml.j2", self.context)

        # Parse multi-document YAML
        docs = list(yaml.safe_load_all(yaml_content))
        for doc in docs:
            if doc:  # Skip empty documents
                self.k8s.apply_manifest(doc, namespace=namespace)

        return DeploymentResult(
            component="secrets",
            status=DeploymentStatus.SUCCESS,
            message="Created S3 and PostgreSQL secrets",
            elapsed_seconds=time.time() - start,
            label="Secrets",
            detail="S3 + PostgreSQL",
        )

    def _deploy_buckets(self, force_legacy: bool = False) -> DeploymentResult:
        """Create S3 buckets if create_buckets is enabled.

        Uses S3Client.ensure_buckets() which is idempotent -- existing
        buckets are left untouched.
        """
        import time

        start = time.time()

        s3_cfg = self.config.platform.storage.s3
        if not s3_cfg.create_buckets:
            return DeploymentResult(
                component="s3-buckets",
                status=DeploymentStatus.SKIPPED,
                message="Bucket creation disabled (create_buckets=false)",
                elapsed_seconds=0,
            )

        if not s3_cfg.endpoint:
            return DeploymentResult(
                component="s3-buckets",
                status=DeploymentStatus.SKIPPED,
                message="No S3 endpoint configured",
                elapsed_seconds=0,
            )

        bucket_names = [
            s3_cfg.buckets.bronze,
            s3_cfg.buckets.silver,
            s3_cfg.buckets.gold,
        ]

        if self.dry_run:
            return DeploymentResult(
                component="s3-buckets",
                status=DeploymentStatus.SUCCESS,
                message=f"Would create buckets: {', '.join(bucket_names)}",
                elapsed_seconds=0,
            )

        from lakebench.s3 import S3Client

        s3 = S3Client(
            endpoint=s3_cfg.endpoint,
            access_key=s3_cfg.access_key,
            secret_key=s3_cfg.secret_key,
            region=s3_cfg.region,
            path_style=s3_cfg.path_style,
            ca_cert=s3_cfg.ca_cert,
            verify_ssl=s3_cfg.verify_ssl,
        )
        if s3._init_error:
            return DeploymentResult(
                component="s3-buckets",
                status=DeploymentStatus.FAILED,
                message=f"S3 client init failed: {s3._init_error}",
                elapsed_seconds=time.time() - start,
            )

        results = s3.ensure_buckets(bucket_names)
        created = [name for name, was_created in results.items() if was_created]
        existed = [name for name, was_created in results.items() if not was_created]

        # Ownership tag: write unconditionally on every ensure_buckets call
        # (not just on create) so a bucket someone else created can be
        # claimed only if unowned, and our redeployed bucket is always
        # correctly tagged. Any pre-existing bucket owned by another
        # deployment stops the deploy here.
        from lakebench.deploy.ownership import (
            TAG_CREATED_BY_LAKEBENCH,
            TAG_DEPLOYMENT_NAME,
            BucketOwnershipError,
            BucketTaggingUnsupported,
            IdentityVerdict,
            bucket_name_matches_deployment,
            build_identity_from_config,
            list_lakebench_deployment_names,
            read_bucket_ownership_tag,
            record_created_buckets,
            verify_bucket_ownership,
            write_bucket_ownership_tag,
        )

        identity = build_identity_from_config(
            self.config,
            context=self.config.platform.kubernetes.context or "",
        )
        boto = s3.raw_client  # boto3 client under the hood
        unsupported_warned = False  # log the tagging fallback once per deploy
        # Enumerate other lakebench deployments on the cluster once so
        # the UNSUPPORTED-fallback prefix check can enforce
        # longest-prefix-wins. ``None`` means "cannot enumerate": the
        # UNSUPPORTED branch below MUST refuse rather than fall back
        # to naive prefix (F2 finding, round-3 review). Uses the
        # convention already in this module -- get_k8s_client loads
        # the right kubeconfig context (F8) and CoreV1Api uses it.
        from kubernetes import client as _kclient

        from lakebench.k8s import get_k8s_client as _get_k8s

        _get_k8s(
            context=self.config.platform.kubernetes.context or "",
            namespace=self.config.get_namespace(),
        )
        # LB-159: the namespace records which buckets lakebench created, for
        # backends without tagging; destroy deletes only those. Recorded
        # before the ownership loop so a deploy that fails on a later bucket
        # still leaves a record for the ones it already created.
        creation_recorded = True
        if created:
            try:
                record_created_buckets(_kclient.CoreV1Api(), self.config.get_namespace(), created)
            except Exception as e:  # noqa: BLE001
                creation_recorded = False
                logger.warning(
                    "Could not record created buckets %s on namespace %s (%s); "
                    "destroy will empty but keep them.",
                    created,
                    self.config.get_namespace(),
                    e,
                )
        other_deployments = list_lakebench_deployment_names(
            _kclient.CoreV1Api(), exclude=self.config.get_namespace()
        )
        for name in bucket_names:
            v = verify_bucket_ownership(boto, name, identity.name)
            if v.verdict is IdentityVerdict.MISMATCH:
                return DeploymentResult(
                    component="s3-buckets",
                    status=DeploymentStatus.FAILED,
                    message=f"Bucket ownership refused: {v.hint}",
                    elapsed_seconds=time.time() - start,
                )
            # R2: an ABSENT (legacy, untagged) bucket must NOT be claimed
            # silently. Freshly-created buckets ARE our own untagged
            # buckets (ensure_buckets returned True for was_created);
            # those we tag. Pre-existing untagged buckets require
            # explicit --force-legacy so a user cannot silently take
            # over another team's untagged storage.
            if v.verdict is IdentityVerdict.ABSENT:
                was_created = results.get(name, False)
                if not was_created and not force_legacy:
                    return DeploymentResult(
                        component="s3-buckets",
                        status=DeploymentStatus.FAILED,
                        message=(
                            f"Bucket {name!r} exists without a lakebench "
                            "ownership tag. Refusing to claim it. Pass "
                            "--force-legacy on deploy to take ownership "
                            "(caution: this may collide with another "
                            "team's storage). " + (v.hint or "")
                        ),
                        elapsed_seconds=time.time() - start,
                    )
            if v.verdict is IdentityVerdict.NOT_FOUND:
                # Should not happen after ensure_buckets returned. Skip.
                continue
            if v.verdict is IdentityVerdict.UNSUPPORTED:
                # Backend does not implement PutBucketTagging /
                # GetBucketTagging (FlashBlade). Cannot stamp identity.
                # Fallback: require that the bucket name follows the
                # convention AND that no other lakebench deployment on
                # the cluster has a longer-prefix claim. Enumeration
                # must have succeeded: if it returned None (RBAC 403,
                # kubeconfig missing) we cannot enforce longest-prefix
                # and MUST refuse unless the operator has explicitly
                # asserted ownership with --force-legacy.
                was_created = results.get(name, False)
                enumeration_failed = other_deployments is None
                prefix_ok = not enumeration_failed and bucket_name_matches_deployment(
                    name, identity.name, other_deployments or ()
                )
                if not (prefix_ok or force_legacy):
                    if was_created:
                        _try_cleanup_orphan_bucket(boto, name)
                    if enumeration_failed:
                        msg = (
                            f"Bucket {name!r}: backend does not support "
                            "bucket tagging, and lakebench could not "
                            "enumerate other deployments on the cluster "
                            "(likely RBAC on `namespaces` list). "
                            "Cannot enforce sibling-collision safety. "
                            "Grant cluster-wide `list namespaces` to "
                            "this token, or pass --force-legacy after "
                            "confirming no other lakebench deployment "
                            "on this cluster shares a name prefix with "
                            f"{identity.name!r}."
                        )
                    else:
                        msg = (
                            f"Bucket {name!r}: backend does not support "
                            "bucket tagging. Name-prefix fallback "
                            f"cannot grant ownership to deployment "
                            f"{identity.name!r} (another deployment on "
                            "the cluster may have a longer-prefix "
                            "claim, or the bucket does not follow the "
                            f"{identity.name}- naming convention). "
                            "Rename the bucket, or pass --force-legacy "
                            "if you have confirmed this is yours."
                        )
                    return DeploymentResult(
                        component="s3-buckets",
                        status=DeploymentStatus.FAILED,
                        message=msg,
                        elapsed_seconds=time.time() - start,
                    )
                if not unsupported_warned:
                    logger.warning(
                        "S3 backend does not implement bucket tagging. "
                        "Ownership discipline falls back to bucket-name "
                        "prefix matching the deployment name %r. See "
                        "docs/storage-backends.md.",
                        identity.name,
                    )
                    unsupported_warned = True
                if was_created:
                    logger.info(
                        "bucket %s: created under name-prefix ownership; "
                        "no tag written (backend unsupported).",
                        name,
                    )
                continue
            # LB-159: keep the created-by-lakebench marker across redeploys
            # (the tag set is rewritten each time) and add it on create.
            created_here = bool(results.get(name, False))
            if not created_here:
                try:
                    prior = read_bucket_ownership_tag(boto, name) or {}
                except Exception:  # noqa: BLE001
                    prior = {}
                created_here = (
                    prior.get(TAG_DEPLOYMENT_NAME) == identity.name
                    and prior.get(TAG_CREATED_BY_LAKEBENCH) == "true"
                )
            try:
                write_bucket_ownership_tag(
                    boto,
                    name,
                    identity.name,
                    workload_schema=identity.workload_schema,
                    created=created_here,
                )
            except BucketTaggingUnsupported:
                # Rare race: verify said tags exist earlier in this
                # loop, then the write said Unsupported (permissions
                # rotated, or a transient backend upgrade dropped the
                # API). Fall back the same way as the pre-check path,
                # including refusal when enumeration failed.
                prefix_ok = other_deployments is not None and bucket_name_matches_deployment(
                    name, identity.name, other_deployments
                )
                if not (prefix_ok or force_legacy):
                    return DeploymentResult(
                        component="s3-buckets",
                        status=DeploymentStatus.FAILED,
                        message=(
                            f"Bucket {name!r}: PutBucketTagging returned "
                            "NotImplemented and name-prefix fallback "
                            "cannot grant ownership (name mismatch or "
                            "cluster-enumeration failure). Cannot "
                            "enforce ownership."
                        ),
                        elapsed_seconds=time.time() - start,
                    )
                if not unsupported_warned:
                    logger.warning(
                        "S3 backend does not implement PutBucketTagging. "
                        "Falling back to name-prefix ownership."
                    )
                    unsupported_warned = True
                continue
            except BucketOwnershipError as e:
                return DeploymentResult(
                    component="s3-buckets",
                    status=DeploymentStatus.FAILED,
                    message=f"Bucket ownership tag failed for {name}: {e}",
                    elapsed_seconds=time.time() - start,
                )

        parts = []
        if created:
            parts.append(f"created {', '.join(created)}")
            if not creation_recorded:
                parts.append("creation not recorded; destroy will keep them")
        if existed:
            parts.append(f"already existed: {', '.join(existed)}")

        return DeploymentResult(
            component="s3-buckets",
            status=DeploymentStatus.SUCCESS,
            message=f"S3 buckets ready ({'; '.join(parts)})",
            elapsed_seconds=time.time() - start,
            label="S3 Buckets",
            detail=f"{len(created)} created, {len(existed)} existed",
        )

    def _deploy_scratch_storageclass(self) -> DeploymentResult:
        """Preflight-verify the scratch StorageClass exists.

        StorageClass is Category 2 shared infrastructure: lakebench uses
        it but never creates or destroys it (a create would race with
        parallel deploys, a destroy would strip the class out from under
        every other user of the cluster). We verify presence at deploy
        preflight and refuse with a pointer to
        ``lakebench admin install-scratch-storage-class`` when the class
        is missing.
        """
        import time

        start = time.time()

        scratch_cfg = self.config.platform.storage.scratch
        if not scratch_cfg.enabled:
            return DeploymentResult(
                component="scratch-sc",
                status=DeploymentStatus.SKIPPED,
                message="Scratch storage disabled (scratch.enabled=false)",
                elapsed_seconds=0,
            )

        if self.dry_run:
            return DeploymentResult(
                component="scratch-sc",
                status=DeploymentStatus.SUCCESS,
                message=f"Would verify StorageClass: {scratch_cfg.storage_class}",
                elapsed_seconds=0,
            )

        from kubernetes import client as k8s_client
        from kubernetes.client.exceptions import ApiException

        storage_v1 = k8s_client.StorageV1Api()
        try:
            storage_v1.read_storage_class(scratch_cfg.storage_class)
        except ApiException as e:
            if e.status == 404:
                hint = (
                    f"StorageClass '{scratch_cfg.storage_class}' does not exist. "
                    "A cluster admin can install it with: "
                    "`lakebench admin install-scratch-storage-class`. "
                    "Or set scratch.enabled=false to disable scratch PVCs."
                )
                return DeploymentResult(
                    component="scratch-sc",
                    status=DeploymentStatus.FAILED,
                    message=hint,
                    elapsed_seconds=time.time() - start,
                )
            return DeploymentResult(
                component="scratch-sc",
                status=DeploymentStatus.FAILED,
                message=f"Cannot read StorageClass '{scratch_cfg.storage_class}': {e}",
                elapsed_seconds=time.time() - start,
            )

        return DeploymentResult(
            component="scratch-sc",
            status=DeploymentStatus.SUCCESS,
            message=f"StorageClass '{scratch_cfg.storage_class}' verified",
            elapsed_seconds=time.time() - start,
            label="StorageClass",
            detail=scratch_cfg.storage_class,
        )

    @staticmethod
    def _operator_rbac_exists(namespace: str) -> bool:
        """Check if the Spark Operator's Role exists in the target namespace.

        After a namespace is deleted and recreated, the operator's
        per-namespace Role/RoleBinding are lost even though the operator
        still claims to watch the namespace.
        """
        try:
            from kubernetes import client as k8s_client

            rbac_v1 = k8s_client.RbacAuthorizationV1Api()
            rbac_v1.read_namespaced_role("spark-operator-controller", namespace)
            return True
        except Exception:
            return False

    def _deploy_spark_operator(self) -> DeploymentResult:
        """Ensure the Spark Operator is ready and watches the target namespace.

        When ``install=true``, installs the operator if missing.
        When ``install=false``, still verifies the operator exists and
        watches the target namespace -- adds it via ``helm upgrade`` if
        needed.  Fails deployment if the operator is missing or broken,
        rather than silently skipping and letting ``run`` fail later.
        """
        import time

        start = time.time()

        spark_op_cfg = self.config.platform.compute.spark.operator
        job_ns = self.config.get_namespace()

        if self.dry_run:
            action = "install" if spark_op_cfg.install else "verify"
            return DeploymentResult(
                component="spark-operator",
                status=DeploymentStatus.SUCCESS,
                message=(
                    f"Would {action} Spark Operator v{spark_op_cfg.version} "
                    f"in namespace '{spark_op_cfg.namespace}'"
                ),
                elapsed_seconds=0,
            )

        try:
            from lakebench.spark import SparkOperatorManager

            operator = SparkOperatorManager(
                namespace=spark_op_cfg.namespace,
                version=spark_op_cfg.version,
                job_namespace=job_ns,
                kube_context=self.config.platform.kubernetes.context,
            )

            if spark_op_cfg.install:
                # Full install/upgrade path
                status = operator.ensure_installed()
            else:
                # install=false: don't install, but DO ensure the operator
                # watches the target namespace (add via helm upgrade).
                status = operator.ensure_namespace_watched(can_heal=True)

            if not status.ready:
                hint = ""
                if not spark_op_cfg.install:
                    hint = (
                        " (operator.install is false -- set to true for "
                        "auto-install, or install the operator manually)"
                    )
                return DeploymentResult(
                    component="spark-operator",
                    status=DeploymentStatus.FAILED,
                    message=f"Spark Operator not ready: {status.message}{hint}",
                    elapsed_seconds=time.time() - start,
                )

            if status.watching_namespace is False:
                return DeploymentResult(
                    component="spark-operator",
                    status=DeploymentStatus.FAILED,
                    message=status.message,
                    elapsed_seconds=time.time() - start,
                )

            # After destroy + re-deploy, the operator claims to watch our
            # namespace (it's in spark.jobNamespaces) but the per-namespace
            # Role/RoleBinding were lost when the namespace was deleted.
            # Detect this and force-recreate the RBAC (remove + re-add
            # the namespace, patch OpenShift SCC, restart controller).
            if not self._operator_rbac_exists(job_ns):
                logger.info(
                    "Spark Operator RBAC missing in '%s' -- recreating",
                    job_ns,
                )
                if not operator.recreate_namespace_rbac(job_ns):
                    return DeploymentResult(
                        component="spark-operator",
                        status=DeploymentStatus.FAILED,
                        message=(f"Spark Operator RBAC recreation failed for namespace '{job_ns}'"),
                        elapsed_seconds=time.time() - start,
                    )

            return DeploymentResult(
                component="spark-operator",
                status=DeploymentStatus.SUCCESS,
                message=(
                    f"Spark Operator ready "
                    f"(v{status.version or 'unknown'}, "
                    f"namespace: {status.namespace or spark_op_cfg.namespace})"
                ),
                elapsed_seconds=time.time() - start,
                label="Spark Operator",
                detail=status.version or "unknown",
            )
        except Exception as e:
            return DeploymentResult(
                component="spark-operator",
                status=DeploymentStatus.FAILED,
                message=f"Spark Operator deployment failed: {e}",
                elapsed_seconds=time.time() - start,
            )

    def destroy_all(
        self,
        progress_callback: Callable[[str, DeploymentStatus, str], None] | None = None,
        clean_buckets: bool = True,
        allow_unverified_cluster: bool = False,
        force_legacy: bool = False,
        namespace_wait_timeout: int | None = None,
        delete_buckets: bool = True,
    ) -> list[DeploymentResult]:
        """Destroy all deployed components.

        Delegates to deploy.destroy.destroy_all() -- see that module
        for the full implementation. ``namespace_wait_timeout=None`` uses
        the destroy module's default.
        """
        from lakebench.deploy.destroy import DEFAULT_NAMESPACE_WAIT_TIMEOUT
        from lakebench.deploy.destroy import destroy_all as _destroy_all

        return _destroy_all(
            engine=self,
            progress_callback=progress_callback,
            clean_buckets=clean_buckets,
            allow_unverified_cluster=allow_unverified_cluster,
            force_legacy=force_legacy,
            namespace_wait_timeout=(
                DEFAULT_NAMESPACE_WAIT_TIMEOUT
                if namespace_wait_timeout is None
                else namespace_wait_timeout
            ),
            delete_buckets=delete_buckets,
        )
