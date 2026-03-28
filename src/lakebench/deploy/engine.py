"""Deployment engine for Lakebench."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from lakebench._constants import POLARIS_CLIENT_SECRET
from lakebench.config import LakebenchConfig
from lakebench.k8s import K8sClient

logger = logging.getLogger(__name__)


def image_tag(image: str) -> str:
    """Extract the version tag from a container image reference.

    >>> image_tag("postgres:17")
    '17'
    >>> image_tag("apache/polaris:1.3.0-incubating")
    '1.3.0-incubating'
    >>> image_tag("myregistry.io/org/app")
    'latest'
    """
    # Handle digest references (image@sha256:...)
    if "@" in image:
        return image.split("@", 1)[1][:12]
    tag = image.rsplit(":", 1)[1] if ":" in image else "latest"
    return tag


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

        from lakebench.spark.job import _spark_compat

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
            "polaris_client_secret": POLARIS_CLIENT_SECRET,
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
            "spark_thrift_memory_k8s": self._spark_mem_to_k8s(
                cfg.architecture.query_engine.spark_thrift.memory
            ),
            "spark_thrift_catalog_name": cfg.architecture.query_engine.spark_thrift.catalog_name,
            "query_engine_type": cfg.architecture.query_engine.type.value,
            # Spark Thrift packages (computed from config versions)
            "spark_thrift_packages": self._build_spark_thrift_packages(cfg),
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
            # Observability
            "observability_enabled": cfg.observability.enabled,
        }

    def deploy_all(
        self,
        progress_callback: Callable[[str, DeploymentStatus, str], None] | None = None,
    ) -> list[DeploymentResult]:
        """Deploy all components in order.

        Args:
            progress_callback: Optional callback for progress updates
                               (component, status, message)

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
            ("namespace", "Creating namespace", self._deploy_namespace),
            ("secrets", "Creating secrets", self._deploy_secrets),
            ("s3-buckets", "Creating S3 buckets", self._deploy_buckets),
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

        for component, description, deploy_fn in steps:
            if progress_callback:
                progress_callback(component, DeploymentStatus.IN_PROGRESS, description)

            try:
                result = deploy_fn()
                self.results.append(result)

                if progress_callback:
                    progress_callback(component, result.status, result.message)

                # Stop on failure (unless it's a non-critical component)
                if result.status == DeploymentStatus.FAILED:
                    break

            except Exception as e:
                result = DeploymentResult(
                    component=component,
                    status=DeploymentStatus.FAILED,
                    message=str(e),
                )
                self.results.append(result)

                if progress_callback:
                    progress_callback(component, DeploymentStatus.FAILED, str(e))
                break

        return self.results

    def _deploy_namespace(self) -> DeploymentResult:
        """Deploy namespace."""
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

        # Check if namespace exists and wait if it's terminating
        if self.k8s.namespace_exists(namespace):
            phase = self.k8s.get_namespace_phase(namespace)
            if phase == "Terminating":
                self.k8s.wait_for_namespace_deleted(namespace, timeout=120)
            else:
                return DeploymentResult(
                    component="namespace",
                    status=DeploymentStatus.SUCCESS,
                    message=f"Namespace '{namespace}' already exists",
                    elapsed_seconds=time.time() - start,
                    label="Namespace",
                    detail=namespace,
                )

        # Create namespace
        if self.config.platform.kubernetes.create_namespace:
            yaml_content = self.renderer.render("namespace.yaml.j2", self.context)
            import yaml

            manifest = yaml.safe_load(yaml_content)
            self.k8s.apply_manifest(manifest)

            return DeploymentResult(
                component="namespace",
                status=DeploymentStatus.SUCCESS,
                message=f"Created namespace: {namespace}",
                elapsed_seconds=time.time() - start,
                label="Namespace",
                detail=namespace,
            )
        else:
            return DeploymentResult(
                component="namespace",
                status=DeploymentStatus.FAILED,
                message=f"Namespace '{namespace}' does not exist and create_namespace=false",
                elapsed_seconds=time.time() - start,
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

    def _deploy_buckets(self) -> DeploymentResult:
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

        parts = []
        if created:
            parts.append(f"created {', '.join(created)}")
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
        """Deploy the Spark scratch StorageClass (px-csi-scratch, repl=1).

        Non-fatal: if the SC already exists or creation fails (no cluster-admin),
        the pipeline continues -- Spark will use whatever SC is configured.
        """
        import time

        start = time.time()

        scratch_cfg = self.config.platform.storage.scratch
        if not scratch_cfg.enabled or not scratch_cfg.create_storage_class:
            reasons = []
            if not scratch_cfg.enabled:
                reasons.append("scratch.enabled=false")
            if not scratch_cfg.create_storage_class:
                reasons.append("scratch.create_storage_class=false")
            return DeploymentResult(
                component="scratch-sc",
                status=DeploymentStatus.SKIPPED,
                message=f"Scratch StorageClass creation disabled ({', '.join(reasons)})",
                elapsed_seconds=0,
            )

        if self.dry_run:
            return DeploymentResult(
                component="scratch-sc",
                status=DeploymentStatus.SUCCESS,
                message=f"Would create StorageClass: {scratch_cfg.storage_class}",
                elapsed_seconds=0,
            )

        try:
            from kubernetes import client as k8s_client

            storage_v1 = k8s_client.StorageV1Api()

            # Check if SC already exists
            try:
                storage_v1.read_storage_class(scratch_cfg.storage_class)
                return DeploymentResult(
                    component="scratch-sc",
                    status=DeploymentStatus.SUCCESS,
                    message=f"StorageClass '{scratch_cfg.storage_class}' already exists",
                    elapsed_seconds=time.time() - start,
                    label="StorageClass",
                    detail=scratch_cfg.storage_class,
                )
            except Exception:
                logger.debug("StorageClass '%s' not found, will create", scratch_cfg.storage_class)

            import yaml

            yaml_content = self.renderer.render("storageclass/px-csi-scratch.yaml.j2", self.context)
            manifest = yaml.safe_load(yaml_content)
            storage_v1.create_storage_class(body=manifest)

            return DeploymentResult(
                component="scratch-sc",
                status=DeploymentStatus.SUCCESS,
                message=f"Created StorageClass: {scratch_cfg.storage_class}",
                elapsed_seconds=time.time() - start,
                label="StorageClass",
                detail=scratch_cfg.storage_class,
            )
        except Exception as e:
            # Non-fatal -- cluster-admin may be needed
            logger.warning("Scratch StorageClass creation failed: %s", e)
            return DeploymentResult(
                component="scratch-sc",
                status=DeploymentStatus.SKIPPED,
                message=f"Scratch StorageClass creation failed (may need cluster-admin): {e}",
                elapsed_seconds=time.time() - start,
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
    ) -> list[DeploymentResult]:
        """Destroy all deployed components.

        Delegates to deploy.destroy.destroy_all() -- see that module
        for the full implementation.
        """
        from lakebench.deploy.destroy import destroy_all as _destroy_all

        return _destroy_all(
            engine=self,
            progress_callback=progress_callback,
            clean_buckets=clean_buckets,
        )
