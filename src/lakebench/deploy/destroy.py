"""Destroy logic for Lakebench deployments.

Extracted from DeploymentEngine.destroy_all() to reduce engine.py LOC.
Called by DeploymentEngine.destroy_all() -- not used directly.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING

from lakebench.deploy.engine import DeploymentResult, DeploymentStatus

if TYPE_CHECKING:
    from lakebench.deploy.engine import DeploymentEngine

logger = logging.getLogger(__name__)


def destroy_all(
    engine: DeploymentEngine,
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

    from kubernetes import client as k8s_client
    from kubernetes.client.rest import ApiException

    results = []
    namespace = engine.config.get_namespace()

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
    except ApiException as e:
        if e.status == 404:
            results.append(
                DeploymentResult(
                    component="spark-jobs",
                    status=DeploymentStatus.SKIPPED,
                    message="No SparkApplications found",
                )
            )
        else:
            logger.warning("SparkApplication cleanup failed: %s", e)
            results.append(
                DeploymentResult(
                    component="spark-jobs",
                    status=DeploymentStatus.FAILED,
                    message=f"SparkApplication cleanup failed: {e}",
                )
            )
    except Exception as e:
        logger.warning("SparkApplication cleanup failed: %s", e, exc_info=True)
        results.append(
            DeploymentResult(
                component="spark-jobs",
                status=DeploymentStatus.SKIPPED,
                message=f"SparkApplication cleanup skipped: {e}",
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
    except ApiException as e:
        if e.status == 404:
            results.append(
                DeploymentResult(
                    component="spark-pods",
                    status=DeploymentStatus.SKIPPED,
                    message="No orphaned pods found",
                )
            )
        else:
            logger.warning("Spark pod cleanup failed: %s", e)
            results.append(
                DeploymentResult(
                    component="spark-pods",
                    status=DeploymentStatus.FAILED,
                    message=f"Spark pod cleanup failed: {e}",
                )
            )
    except Exception as e:
        logger.warning("Spark pod cleanup failed: %s", e, exc_info=True)
        results.append(
            DeploymentResult(
                component="spark-pods",
                status=DeploymentStatus.SKIPPED,
                message=f"Spark pod cleanup skipped: {e}",
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
        pods = core_v1.list_namespaced_pod(namespace, label_selector="job-name=lakebench-datagen")
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
    except ApiException as e:
        if e.status == 404:
            results.append(
                DeploymentResult(
                    component="datagen-jobs",
                    status=DeploymentStatus.SKIPPED,
                    message="No datagen jobs found",
                )
            )
        else:
            logger.warning("Datagen cleanup failed: %s", e)
            results.append(
                DeploymentResult(
                    component="datagen-jobs",
                    status=DeploymentStatus.FAILED,
                    message=f"Datagen cleanup failed: {e}",
                )
            )
    except Exception as e:
        logger.warning("Datagen cleanup failed: %s", e, exc_info=True)
        results.append(
            DeploymentResult(
                component="datagen-jobs",
                status=DeploymentStatus.SKIPPED,
                message=f"Datagen cleanup skipped: {e}",
            )
        )

    time.sleep(2)

    # Step 3: Drop tables via available engine (Trino or Spark Thrift)
    table_format = engine.config.architecture.table_format.type.value
    report("table-cleanup", DeploymentStatus.IN_PROGRESS, f"Dropping {table_format} tables...")
    try:
        from lakebench.deploy.iceberg import (
            build_drop_table_sql,
            exec_sql,
            find_maintenance_engine,
        )

        maint_engine, pod_name, catalog = find_maintenance_engine(
            engine.config,
            namespace,
        )
        if maint_engine and pod_name and catalog:
            tables = engine.config.architecture.tables
            tables_to_drop = [
                f"{catalog}.{tables.bronze}",
                f"{catalog}.{tables.silver}",
                f"{catalog}.{tables.gold}",
            ]
            # Run maintenance before dropping tables to clean S3
            for table in tables_to_drop:
                if table_format == "delta":
                    from lakebench.deploy.delta_maintenance import (
                        build_delta_maintenance_sql,
                    )

                    maint_sqls = build_delta_maintenance_sql(engine, catalog, table, 0.0)
                else:
                    from lakebench.deploy.iceberg import build_maintenance_sql

                    maint_sqls = build_maintenance_sql(maint_engine, catalog, table, "0s")
                for sql in maint_sqls:
                    try:
                        exec_sql(maint_engine, engine.k8s, pod_name, namespace, sql)
                    except Exception as e:
                        logger.warning(
                            "%s maintenance failed (table may not exist): %s",
                            table_format.title(),
                            e,
                        )
            # Now drop the tables
            for table in tables_to_drop:
                drop_sql = build_drop_table_sql(maint_engine, table)
                if drop_sql:
                    try:
                        exec_sql(maint_engine, engine.k8s, pod_name, namespace, drop_sql)
                    except Exception as e:
                        logger.warning("DROP TABLE failed for %s: %s", table, e)
            results.append(
                DeploymentResult(
                    component="table-cleanup",
                    status=DeploymentStatus.SUCCESS,
                    message=f"{table_format.title()} tables dropped (via {maint_engine})",
                )
            )
            report(
                "table-cleanup",
                DeploymentStatus.SUCCESS,
                f"{table_format.title()} tables dropped (via {maint_engine})",
            )
        else:
            engine_type = engine.config.architecture.query_engine.type.value
            msg = (
                "DuckDB cannot run table maintenance, skipping table cleanup"
                if engine_type == "duckdb"
                else "No capable engine pod found, skipping table cleanup"
            )
            results.append(
                DeploymentResult(
                    component="table-cleanup",
                    status=DeploymentStatus.SKIPPED,
                    message=msg,
                )
            )
    except Exception as e:
        logger.warning("Table cleanup failed: %s", e, exc_info=True)
        results.append(
            DeploymentResult(
                component="table-cleanup",
                status=DeploymentStatus.SKIPPED,
                message=f"Table cleanup skipped: {e}",
            )
        )

    # Step 4: Clean S3 buckets (optional)
    if clean_buckets:
        report("s3-buckets", DeploymentStatus.IN_PROGRESS, "Cleaning S3 buckets...")
        try:
            from lakebench.s3 import S3Client

            s3_cfg = engine.config.platform.storage.s3
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
    if engine.config.observability.enabled:
        report("observability", DeploymentStatus.IN_PROGRESS, "Removing observability stack...")
        try:
            from .observability import ObservabilityDeployer

            obs_deployer = ObservabilityDeployer(engine)
            obs_result = obs_deployer.destroy()
            results.append(obs_result)
            report("observability", obs_result.status, obs_result.message)
        except Exception as e:
            results.append(
                DeploymentResult(
                    component="observability",
                    status=DeploymentStatus.SKIPPED,
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
    engine_type = engine.config.architecture.query_engine.type.value
    catalog_type = engine.config.architecture.catalog.type.value

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
                    core_v1.delete_namespaced_persistent_volume_claim(pvc.metadata.name, namespace)
            except ApiException as e:
                if e.status != 404:
                    logger.warning("Trino PVC cleanup failed: %s", e)
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
            except ApiException as e:
                if e.status != 404:
                    logger.warning("Trino configmap cleanup failed: %s", e)
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
            except ApiException as e:
                logger.debug("Hive metastore deployment delete skipped: %s", e.reason)
            try:
                core_v1.delete_namespaced_service("lakebench-hive-metastore", namespace)
            except ApiException as e:
                logger.debug("Hive metastore service delete skipped: %s", e.reason)
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
            except ApiException as e:
                logger.debug("Polaris service delete skipped: %s", e.reason)
            try:
                batch_v1.delete_namespaced_job(
                    "lakebench-polaris-bootstrap",
                    namespace,
                    propagation_policy="Background",
                )
            except ApiException as e:
                logger.debug("Polaris bootstrap job delete skipped: %s", e.reason)
            try:
                core_v1.delete_namespaced_config_map("lakebench-polaris-config", namespace)
            except ApiException as e:
                logger.debug("Polaris configmap delete skipped: %s", e.reason)
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

    # Unity
    if catalog_type == "unity":
        report("unity", DeploymentStatus.IN_PROGRESS, "Removing Unity Catalog...")
        try:
            apps_v1 = k8s_client.AppsV1Api()
            core_v1 = k8s_client.CoreV1Api()
            batch_v1 = k8s_client.BatchV1Api()
            try:
                apps_v1.delete_namespaced_deployment("lakebench-unity", namespace)
            except ApiException as e:
                if e.status != 404:
                    raise
            try:
                core_v1.delete_namespaced_service("lakebench-unity", namespace)
            except ApiException as e:
                logger.debug("Unity service delete skipped: %s", e.reason)
            try:
                batch_v1.delete_namespaced_job(
                    "lakebench-unity-bootstrap",
                    namespace,
                    propagation_policy="Background",
                )
            except ApiException as e:
                logger.debug("Unity bootstrap job delete skipped: %s", e.reason)
            try:
                core_v1.delete_namespaced_config_map("lakebench-unity", namespace)
            except ApiException as e:
                logger.debug("Unity configmap delete skipped: %s", e.reason)
            results.append(
                DeploymentResult(
                    component="unity",
                    status=DeploymentStatus.SUCCESS,
                    message="Unity Catalog removed",
                )
            )
            report("unity", DeploymentStatus.SUCCESS, "Unity Catalog removed")
        except Exception as e:
            results.append(
                DeploymentResult(
                    component="unity",
                    status=DeploymentStatus.FAILED,
                    message=str(e),
                )
            )
    else:
        results.append(
            DeploymentResult(
                component="unity",
                status=DeploymentStatus.SKIPPED,
                message="Unity not configured",
            )
        )

    # Step 8: Remove PostgreSQL
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
        except ApiException as e:
            logger.debug("Postgres service delete skipped: %s", e.reason)
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
        except ApiException as e:
            logger.debug("RoleBinding delete skipped: %s", e.reason)
        try:
            rbac_v1.delete_namespaced_role(SPARK_SERVICE_ACCOUNT, namespace)
        except ApiException as e:
            logger.debug("Role delete skipped: %s", e.reason)
        try:
            core_v1.delete_namespaced_service_account(SPARK_SERVICE_ACCOUNT, namespace)
        except ApiException as e:
            logger.debug("ServiceAccount delete skipped: %s", e.reason)
        for secret in ["lakebench-s3-credentials", "lakebench-postgres-secret"]:
            try:
                core_v1.delete_namespaced_secret(secret, namespace)
            except ApiException as e:
                logger.debug("Secret %s delete skipped: %s", secret, e.reason)
        # Delete SecretClass (cluster-scoped)
        try:
            custom_api.delete_cluster_custom_object(
                group="secrets.stackable.tech",
                version="v1alpha1",
                plural="secretclasses",
                name="lakebench-s3-credentials-class",
            )
        except ApiException as e:
            logger.debug("SecretClass delete skipped: %s", e.reason)
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
    scratch_cfg = engine.config.platform.storage.scratch
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
        except Exception as e:
            logger.debug("Scratch StorageClass cleanup skipped: %s", e)
            results.append(
                DeploymentResult(
                    component="scratch-sc",
                    status=DeploymentStatus.SUCCESS,
                    message="Scratch StorageClass cleanup skipped",
                )
            )

    # Finally, delete namespace if we created it
    if engine.config.platform.kubernetes.create_namespace:
        report("namespace", DeploymentStatus.IN_PROGRESS, f"Deleting namespace {namespace}...")
        try:
            engine.k8s.delete_namespace(namespace)
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
