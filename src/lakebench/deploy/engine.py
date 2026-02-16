"""Deployment engine for Lakebench."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from lakebench.config import LakebenchConfig
from lakebench.k8s import K8sClient

logger = logging.getLogger(__name__)


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
        """Build the ``spark.jars.packages`` string for Spark Thrift Server."""
        from lakebench.spark.job import _spark_compat

        iceberg_version = cfg.architecture.table_format.iceberg.version
        spark_major_minor = DeploymentEngine._get_spark_major_minor(cfg)
        scala_suffix, hadoop_version = _spark_compat(cfg.images.spark)
        packages = [
            f"org.apache.iceberg:iceberg-spark-runtime-{spark_major_minor}{scala_suffix}:{iceberg_version}",
            f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}",
            f"org.apache.hadoop:hadoop-aws:{hadoop_version}",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
        return ",".join(packages)

    def _build_context(self) -> dict[str, Any]:
        """Build template context from configuration."""
        cfg = self.config
        s3 = cfg.platform.storage.s3

        # Detect OpenShift for SCC-aware templates
        openshift_mode = self._detect_openshift()

        # Parse S3 endpoint for Stackable (needs host and port separately)
        from urllib.parse import urlparse

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
            "image_pull_policy": cfg.images.pull_policy.value,
            # S3
            "s3_endpoint": s3.endpoint,
            "s3_host": s3_host,  # For Stackable HiveCluster
            "s3_port": s3_port,  # For Stackable HiveCluster
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
            # Hive
            "hive_cpu_min": cfg.architecture.catalog.hive.resources.cpu_min,
            "hive_cpu_max": cfg.architecture.catalog.hive.resources.cpu_max,
            "hive_memory": cfg.architecture.catalog.hive.resources.memory,
            "hive_thrift_min_threads": cfg.architecture.catalog.hive.thrift.min_threads,
            "hive_thrift_max_threads": cfg.architecture.catalog.hive.thrift.max_threads,
            "hive_client_timeout": cfg.architecture.catalog.hive.thrift.client_timeout,
            # Polaris
            "polaris_version": cfg.architecture.catalog.polaris.version,
            "polaris_port": cfg.architecture.catalog.polaris.port,
            "polaris_cpu": cfg.architecture.catalog.polaris.resources.cpu,
            "polaris_memory": cfg.architecture.catalog.polaris.resources.memory,
            "polaris_client_secret": "lakebench-polaris-secret-2024",
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

        # Initialize deployers
        postgres = PostgresDeployer(self)
        hive = HiveDeployer(self)
        polaris = PolarisDeployer(self)
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
            ("scratch-sc", "Creating scratch StorageClass", self._deploy_scratch_storageclass),
            ("postgres", "Deploying PostgreSQL", postgres.deploy),
            ("hive", "Deploying Hive Metastore", hive.deploy),
            ("polaris", "Deploying Polaris Catalog", polaris.deploy),
            ("rbac", "Creating Spark RBAC", rbac.deploy),
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

    def destroy_all(
        self,
        progress_callback: Callable[[str, DeploymentStatus, str], None] | None = None,
        clean_buckets: bool = True,
    ) -> list[DeploymentResult]:
        """Destroy all deployed components.

        Cleanup pattern:
        1. Delete SparkApplications
        2. Clean up orphaned Spark pods
        3. Drop Iceberg tables (if Trino available)
        4. Clean S3 buckets (optional)
        5. Remove infrastructure in reverse order

        Args:
            progress_callback: Optional callback for progress updates
            clean_buckets: Whether to clean S3 bucket contents

        Returns:
            List of destruction results
        """
        import time

        from kubernetes import client as k8s_client
        from kubernetes.client.rest import ApiException

        results = []
        namespace = self.config.get_namespace()

        def report(component: str, status: DeploymentStatus, message: str) -> None:
            if progress_callback:
                progress_callback(component, status, message)

        # Step 1: Delete SparkApplications
        report("spark-jobs", DeploymentStatus.IN_PROGRESS, "Deleting SparkApplications...")
        try:
            custom_api = k8s_client.CustomObjectsApi()
            sparkapps = custom_api.list_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=namespace,
                plural="sparkapplications",
            )
            for app in sparkapps.get("items", []):
                name = app["metadata"]["name"]
                custom_api.delete_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=namespace,
                    plural="sparkapplications",
                    name=name,
                )
            results.append(
                DeploymentResult(
                    component="spark-jobs",
                    status=DeploymentStatus.SUCCESS,
                    message=f"Deleted {len(sparkapps.get('items', []))} SparkApplications",
                )
            )
            report("spark-jobs", DeploymentStatus.SUCCESS, "SparkApplications deleted")
        except Exception:
            results.append(
                DeploymentResult(
                    component="spark-jobs",
                    status=DeploymentStatus.SUCCESS,
                    message="No SparkApplications found",
                )
            )

        # Step 2: Clean up orphaned Spark pods
        report("spark-pods", DeploymentStatus.IN_PROGRESS, "Cleaning up Spark pods...")
        try:
            core_v1 = k8s_client.CoreV1Api()
            for label in [
                "component=driver",
                "component=executor",
                "spark-role=driver",
                "spark-role=executor",
            ]:
                pods = core_v1.list_namespaced_pod(namespace, label_selector=label)
                for pod in pods.items:
                    core_v1.delete_namespaced_pod(
                        pod.metadata.name,
                        namespace,
                        grace_period_seconds=0,
                    )
            results.append(
                DeploymentResult(
                    component="spark-pods",
                    status=DeploymentStatus.SUCCESS,
                    message="Orphaned Spark pods cleaned",
                )
            )
            report("spark-pods", DeploymentStatus.SUCCESS, "Spark pods cleaned")
        except Exception:
            results.append(
                DeploymentResult(
                    component="spark-pods",
                    status=DeploymentStatus.SUCCESS,
                    message="No orphaned pods found",
                )
            )

        # Step 2b: Delete datagen Batch Jobs and pods
        report("datagen-jobs", DeploymentStatus.IN_PROGRESS, "Cleaning up datagen jobs...")
        try:
            batch_v1 = k8s_client.BatchV1Api()
            core_v1 = k8s_client.CoreV1Api()
            # Delete all Jobs matching lakebench-datagen pattern
            jobs = batch_v1.list_namespaced_job(
                namespace, label_selector="app.kubernetes.io/managed-by=lakebench"
            )
            for job in jobs.items:
                batch_v1.delete_namespaced_job(
                    job.metadata.name,
                    namespace,
                    propagation_policy="Background",
                )
            # Also delete by name pattern (in case labels are missing)
            try:
                batch_v1.delete_namespaced_job(
                    "lakebench-datagen",
                    namespace,
                    propagation_policy="Background",
                )
            except ApiException as e:
                if e.status != 404:
                    raise
            # Force-delete datagen pods
            pods = core_v1.list_namespaced_pod(
                namespace, label_selector="job-name=lakebench-datagen"
            )
            for pod in pods.items:
                core_v1.delete_namespaced_pod(
                    pod.metadata.name,
                    namespace,
                    grace_period_seconds=0,
                )
            deleted_count = len(jobs.items) + len(pods.items)
            results.append(
                DeploymentResult(
                    component="datagen-jobs",
                    status=DeploymentStatus.SUCCESS,
                    message=f"Datagen jobs cleaned ({deleted_count} resources)",
                )
            )
            report("datagen-jobs", DeploymentStatus.SUCCESS, "Datagen jobs cleaned")
        except Exception:
            results.append(
                DeploymentResult(
                    component="datagen-jobs",
                    status=DeploymentStatus.SUCCESS,
                    message="No datagen jobs found",
                )
            )

        time.sleep(2)

        # Step 3: Drop Iceberg tables via Trino (if available)
        report("iceberg-tables", DeploymentStatus.IN_PROGRESS, "Dropping Iceberg tables...")
        try:
            catalog = self.config.architecture.query_engine.trino.catalog_name
            core_v1 = k8s_client.CoreV1Api()
            pods = core_v1.list_namespaced_pod(
                namespace, label_selector="app=lakebench-trino-coordinator"
            )
            if pods.items:
                pod_name = pods.items[0].metadata.name
                tables = self.config.architecture.tables
                tables_to_drop = [
                    f"{catalog}.{tables.bronze}",
                    f"{catalog}.{tables.silver}",
                    f"{catalog}.{tables.gold}",
                ]
                # Expire snapshots and remove orphan files
                # before dropping tables to ensure S3 is fully cleaned
                for table in tables_to_drop:
                    for maintenance_sql in [
                        f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '0s')",
                        f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '0s')",
                    ]:
                        try:
                            self.k8s.exec_in_pod(
                                pod_name,
                                ["trino", "--execute", maintenance_sql],
                                namespace,
                            )
                        except Exception as e:
                            logger.warning(
                                "Iceberg maintenance failed (table may not exist): %s", e
                            )
                # Now drop the tables
                for table in tables_to_drop:
                    self.k8s.exec_in_pod(
                        pod_name,
                        ["trino", "--execute", f"DROP TABLE IF EXISTS {table}"],
                        namespace,
                    )
                results.append(
                    DeploymentResult(
                        component="iceberg-tables",
                        status=DeploymentStatus.SUCCESS,
                        message="Iceberg tables dropped",
                    )
                )
                report("iceberg-tables", DeploymentStatus.SUCCESS, "Iceberg tables dropped")
            else:
                results.append(
                    DeploymentResult(
                        component="iceberg-tables",
                        status=DeploymentStatus.SUCCESS,
                        message="Trino not available, skipping table cleanup",
                    )
                )
        except Exception as e:
            results.append(
                DeploymentResult(
                    component="iceberg-tables",
                    status=DeploymentStatus.SUCCESS,
                    message=f"Table cleanup skipped: {e}",
                )
            )

        # Step 4: Clean S3 buckets (optional)
        if clean_buckets:
            report("s3-buckets", DeploymentStatus.IN_PROGRESS, "Cleaning S3 buckets...")
            try:
                from lakebench.s3 import S3Client

                s3_cfg = self.config.platform.storage.s3
                s3 = S3Client(
                    endpoint=s3_cfg.endpoint,
                    access_key=s3_cfg.access_key,
                    secret_key=s3_cfg.secret_key,
                    region=s3_cfg.region,
                    path_style=s3_cfg.path_style,
                )
                if s3._init_error:
                    bucket_names = (
                        f"{s3_cfg.buckets.bronze}, {s3_cfg.buckets.silver}, {s3_cfg.buckets.gold}"
                    )
                    results.append(
                        DeploymentResult(
                            component="s3-buckets",
                            status=DeploymentStatus.FAILED,
                            message=(
                                f"S3 client init failed: {s3._init_error}. "
                                f"Clean buckets manually: {bucket_names}"
                            ),
                        )
                    )
                    report(
                        "s3-buckets",
                        DeploymentStatus.FAILED,
                        f"S3 init failed: {s3._init_error}",
                    )
                else:
                    buckets = [
                        s3_cfg.buckets.bronze,
                        s3_cfg.buckets.silver,
                        s3_cfg.buckets.gold,
                    ]
                    total_deleted = 0
                    for bucket in buckets:
                        deleted = s3.empty_bucket(bucket)
                        total_deleted += deleted
                    results.append(
                        DeploymentResult(
                            component="s3-buckets",
                            status=DeploymentStatus.SUCCESS,
                            message=f"Cleaned {len(buckets)} S3 buckets ({total_deleted} objects)",
                        )
                    )
                    report(
                        "s3-buckets",
                        DeploymentStatus.SUCCESS,
                        f"S3 buckets cleaned ({total_deleted} objects)",
                    )
            except Exception as e:
                results.append(
                    DeploymentResult(
                        component="s3-buckets",
                        status=DeploymentStatus.FAILED,
                        message=f"S3 cleanup failed: {e}",
                    )
                )
                report("s3-buckets", DeploymentStatus.FAILED, f"S3 cleanup failed: {e}")

        # Step 5: Remove observability stack (kube-prometheus-stack)
        if self.config.observability.enabled:
            report("observability", DeploymentStatus.IN_PROGRESS, "Removing observability stack...")
            try:
                from .observability import ObservabilityDeployer

                obs_deployer = ObservabilityDeployer(self)
                obs_result = obs_deployer.destroy()
                results.append(obs_result)
                report("observability", obs_result.status, obs_result.message)
            except Exception as e:
                results.append(
                    DeploymentResult(
                        component="observability",
                        status=DeploymentStatus.SUCCESS,
                        message=f"Observability cleanup skipped: {e}",
                    )
                )
        else:
            results.append(
                DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.SKIPPED,
                    message="Observability not configured",
                )
            )

        # Step 6: Remove query engine (only the configured one)
        engine_type = self.config.architecture.query_engine.type.value
        catalog_type = self.config.architecture.catalog.type.value

        # Trino
        if engine_type == "trino":
            report("trino", DeploymentStatus.IN_PROGRESS, "Removing Trino...")
            try:
                apps_v1 = k8s_client.AppsV1Api()
                core_v1 = k8s_client.CoreV1Api()
                try:
                    apps_v1.delete_namespaced_deployment("lakebench-trino-coordinator", namespace)
                except ApiException as e:
                    if e.status != 404:
                        raise
                try:
                    apps_v1.delete_namespaced_stateful_set("lakebench-trino-worker", namespace)
                except ApiException as e:
                    if e.status != 404:
                        raise
                try:
                    pvcs = core_v1.list_namespaced_persistent_volume_claim(
                        namespace,
                        label_selector="app.kubernetes.io/component=trino-worker",
                    )
                    for pvc in pvcs.items:
                        core_v1.delete_namespaced_persistent_volume_claim(
                            pvc.metadata.name, namespace
                        )
                except ApiException:
                    pass
                for svc_name in [
                    "lakebench-trino",
                    "lakebench-trino-coordinator",
                    "lakebench-trino-worker",
                ]:
                    try:
                        core_v1.delete_namespaced_service(svc_name, namespace)
                    except ApiException as e:
                        if e.status != 404:
                            raise
                try:
                    core_v1.delete_namespaced_config_map("lakebench-trino-config", namespace)
                except ApiException:
                    pass
                results.append(
                    DeploymentResult(
                        component="trino",
                        status=DeploymentStatus.SUCCESS,
                        message="Trino removed",
                    )
                )
                report("trino", DeploymentStatus.SUCCESS, "Trino removed")
            except Exception as e:
                results.append(
                    DeploymentResult(
                        component="trino",
                        status=DeploymentStatus.FAILED,
                        message=str(e),
                    )
                )
        else:
            results.append(
                DeploymentResult(
                    component="trino",
                    status=DeploymentStatus.SKIPPED,
                    message="Trino not configured",
                )
            )

        # Spark Thrift Server
        if engine_type == "spark-thrift":
            report("spark-thrift", DeploymentStatus.IN_PROGRESS, "Removing Spark Thrift Server...")
            try:
                core_v1 = k8s_client.CoreV1Api()
                apps_v1 = k8s_client.AppsV1Api()
                try:
                    apps_v1.delete_namespaced_deployment("lakebench-spark-thrift", namespace)
                except ApiException as e:
                    if e.status != 404:
                        raise
                try:
                    core_v1.delete_namespaced_service("lakebench-spark-thrift", namespace)
                except ApiException as e:
                    if e.status != 404:
                        raise
                results.append(
                    DeploymentResult(
                        component="spark-thrift",
                        status=DeploymentStatus.SUCCESS,
                        message="Spark Thrift Server removed",
                    )
                )
                report("spark-thrift", DeploymentStatus.SUCCESS, "Spark Thrift Server removed")
            except Exception as e:
                results.append(
                    DeploymentResult(
                        component="spark-thrift",
                        status=DeploymentStatus.FAILED,
                        message=str(e),
                    )
                )
        else:
            results.append(
                DeploymentResult(
                    component="spark-thrift",
                    status=DeploymentStatus.SKIPPED,
                    message="Spark Thrift not configured",
                )
            )

        # DuckDB
        if engine_type == "duckdb":
            report("duckdb", DeploymentStatus.IN_PROGRESS, "Removing DuckDB...")
            try:
                core_v1 = k8s_client.CoreV1Api()
                apps_v1 = k8s_client.AppsV1Api()
                try:
                    apps_v1.delete_namespaced_deployment("lakebench-duckdb", namespace)
                except ApiException as e:
                    if e.status != 404:
                        raise
                try:
                    core_v1.delete_namespaced_service("lakebench-duckdb", namespace)
                except ApiException as e:
                    if e.status != 404:
                        raise
                results.append(
                    DeploymentResult(
                        component="duckdb",
                        status=DeploymentStatus.SUCCESS,
                        message="DuckDB removed",
                    )
                )
                report("duckdb", DeploymentStatus.SUCCESS, "DuckDB removed")
            except Exception as e:
                results.append(
                    DeploymentResult(
                        component="duckdb",
                        status=DeploymentStatus.FAILED,
                        message=str(e),
                    )
                )
        else:
            results.append(
                DeploymentResult(
                    component="duckdb",
                    status=DeploymentStatus.SKIPPED,
                    message="DuckDB not configured",
                )
            )

        # Step 7: Remove catalog (only the configured one)
        # Hive Metastore
        if catalog_type == "hive":
            report("hive", DeploymentStatus.IN_PROGRESS, "Removing Hive Metastore...")
            try:
                custom_api = k8s_client.CustomObjectsApi()
                apps_v1 = k8s_client.AppsV1Api()
                core_v1 = k8s_client.CoreV1Api()
                try:
                    custom_api.delete_namespaced_custom_object(
                        group="hive.stackable.tech",
                        version="v1alpha1",
                        namespace=namespace,
                        plural="hiveclusters",
                        name="lakebench-hive",
                    )
                except ApiException as e:
                    if e.status != 404:
                        raise
                try:
                    apps_v1.delete_namespaced_deployment("lakebench-hive-metastore", namespace)
                except ApiException:
                    pass
                try:
                    core_v1.delete_namespaced_service("lakebench-hive-metastore", namespace)
                except ApiException:
                    pass
                results.append(
                    DeploymentResult(
                        component="hive",
                        status=DeploymentStatus.SUCCESS,
                        message="Hive Metastore removed",
                    )
                )
                report("hive", DeploymentStatus.SUCCESS, "Hive Metastore removed")
            except Exception as e:
                results.append(
                    DeploymentResult(
                        component="hive",
                        status=DeploymentStatus.FAILED,
                        message=str(e),
                    )
                )
        else:
            results.append(
                DeploymentResult(
                    component="hive",
                    status=DeploymentStatus.SKIPPED,
                    message="Hive not configured",
                )
            )

        # Polaris
        if catalog_type == "polaris":
            report("polaris", DeploymentStatus.IN_PROGRESS, "Removing Polaris...")
            try:
                apps_v1 = k8s_client.AppsV1Api()
                core_v1 = k8s_client.CoreV1Api()
                batch_v1 = k8s_client.BatchV1Api()
                try:
                    apps_v1.delete_namespaced_deployment("lakebench-polaris", namespace)
                except ApiException as e:
                    if e.status != 404:
                        raise
                try:
                    core_v1.delete_namespaced_service("lakebench-polaris", namespace)
                except ApiException:
                    pass
                try:
                    batch_v1.delete_namespaced_job(
                        "lakebench-polaris-bootstrap",
                        namespace,
                        propagation_policy="Background",
                    )
                except ApiException:
                    pass
                try:
                    core_v1.delete_namespaced_config_map("lakebench-polaris-config", namespace)
                except ApiException:
                    pass
                results.append(
                    DeploymentResult(
                        component="polaris",
                        status=DeploymentStatus.SUCCESS,
                        message="Polaris removed",
                    )
                )
                report("polaris", DeploymentStatus.SUCCESS, "Polaris removed")
            except Exception as e:
                results.append(
                    DeploymentResult(
                        component="polaris",
                        status=DeploymentStatus.FAILED,
                        message=str(e),
                    )
                )
        else:
            results.append(
                DeploymentResult(
                    component="polaris",
                    status=DeploymentStatus.SKIPPED,
                    message="Polaris not configured",
                )
            )

        # Step 7: Remove PostgreSQL
        report("postgres", DeploymentStatus.IN_PROGRESS, "Removing PostgreSQL...")
        try:
            apps_v1 = k8s_client.AppsV1Api()
            core_v1 = k8s_client.CoreV1Api()
            try:
                apps_v1.delete_namespaced_stateful_set("lakebench-postgres", namespace)
            except ApiException as e:
                if e.status != 404:
                    raise
            try:
                core_v1.delete_namespaced_service("lakebench-postgres", namespace)
            except ApiException:
                pass
            # Delete PVCs
            pvcs = core_v1.list_namespaced_persistent_volume_claim(
                namespace,
                label_selector="app.kubernetes.io/component=postgres",
            )
            for pvc in pvcs.items:
                core_v1.delete_namespaced_persistent_volume_claim(pvc.metadata.name, namespace)
            results.append(
                DeploymentResult(
                    component="postgres",
                    status=DeploymentStatus.SUCCESS,
                    message="PostgreSQL removed",
                )
            )
            report("postgres", DeploymentStatus.SUCCESS, "PostgreSQL removed")
        except Exception as e:
            results.append(
                DeploymentResult(
                    component="postgres",
                    status=DeploymentStatus.FAILED,
                    message=str(e),
                )
            )

        # Step 8: Remove RBAC and Secrets
        report("rbac", DeploymentStatus.IN_PROGRESS, "Removing RBAC and secrets...")
        try:
            rbac_v1 = k8s_client.RbacAuthorizationV1Api()
            core_v1 = k8s_client.CoreV1Api()
            custom_api = k8s_client.CustomObjectsApi()
            from lakebench._constants import SPARK_SERVICE_ACCOUNT

            try:
                rbac_v1.delete_namespaced_role_binding(SPARK_SERVICE_ACCOUNT, namespace)
            except ApiException:
                pass
            try:
                rbac_v1.delete_namespaced_role(SPARK_SERVICE_ACCOUNT, namespace)
            except ApiException:
                pass
            try:
                core_v1.delete_namespaced_service_account(SPARK_SERVICE_ACCOUNT, namespace)
            except ApiException:
                pass
            for secret in ["lakebench-s3-credentials", "lakebench-postgres-secret"]:
                try:
                    core_v1.delete_namespaced_secret(secret, namespace)
                except ApiException:
                    pass
            # Delete SecretClass (cluster-scoped)
            try:
                custom_api.delete_cluster_custom_object(
                    group="secrets.stackable.tech",
                    version="v1alpha1",
                    plural="secretclasses",
                    name="lakebench-s3-credentials-class",
                )
            except ApiException:
                pass
            results.append(
                DeploymentResult(
                    component="rbac",
                    status=DeploymentStatus.SUCCESS,
                    message="RBAC and secrets removed",
                )
            )
            report("rbac", DeploymentStatus.SUCCESS, "RBAC and secrets removed")
        except Exception as e:
            results.append(
                DeploymentResult(
                    component="rbac",
                    status=DeploymentStatus.FAILED,
                    message=str(e),
                )
            )

        # Step 9: Remove scratch StorageClass (cluster-scoped, best-effort)
        scratch_cfg = self.config.platform.storage.scratch
        if scratch_cfg.enabled and scratch_cfg.create_storage_class:
            report("scratch-sc", DeploymentStatus.IN_PROGRESS, "Removing scratch StorageClass...")
            try:
                storage_v1 = k8s_client.StorageV1Api()
                storage_v1.delete_storage_class(scratch_cfg.storage_class)
                results.append(
                    DeploymentResult(
                        component="scratch-sc",
                        status=DeploymentStatus.SUCCESS,
                        message=f"Removed StorageClass: {scratch_cfg.storage_class}",
                    )
                )
                report("scratch-sc", DeploymentStatus.SUCCESS, "Scratch StorageClass removed")
            except Exception:
                results.append(
                    DeploymentResult(
                        component="scratch-sc",
                        status=DeploymentStatus.SUCCESS,
                        message="Scratch StorageClass cleanup skipped",
                    )
                )

        # Finally, delete namespace if we created it
        if self.config.platform.kubernetes.create_namespace:
            report("namespace", DeploymentStatus.IN_PROGRESS, f"Deleting namespace {namespace}...")
            try:
                self.k8s.delete_namespace(namespace)
                results.append(
                    DeploymentResult(
                        component="namespace",
                        status=DeploymentStatus.SUCCESS,
                        message=f"Deleted namespace: {namespace}",
                    )
                )
                report("namespace", DeploymentStatus.SUCCESS, f"Namespace {namespace} deleted")
            except Exception as e:
                results.append(
                    DeploymentResult(
                        component="namespace",
                        status=DeploymentStatus.FAILED,
                        message=str(e),
                    )
                )

        return results
