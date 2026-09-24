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


# Cluster-scoped resources shared by every lakebench deployment (Stackable
# SecretClass, the scratch StorageClass, etc.) must not be deleted while
# another lakebench namespace still uses them. Deleting them out from under
# a running parallel deploy has crashed other users' Hive Metastore pods
# and killed other users' Spark PVC provisioning. See findings in the
# deploy/destroy adversarial review.
LAKEBENCH_NAMESPACE_LABEL = "app.kubernetes.io/managed-by=lakebench"


def _is_last_lakebench_namespace(namespace_names: list[str], current: str) -> bool:
    """Return True iff `current` is the only lakebench-labeled namespace left.

    Extracted as a pure function so refcount behavior can be unit-tested
    without a live cluster.
    """
    others = [n for n in namespace_names if n != current]
    return not others


def _other_lakebench_namespaces_exist(core_v1, current_namespace: str) -> bool:
    """Return True if any lakebench-labeled namespace exists BESIDES the one
    being destroyed. Fail-safe: on any listing error, return True (assume
    others exist) so we don't delete a shared resource on flaky read.
    """
    try:
        ns_list = core_v1.list_namespace(label_selector=LAKEBENCH_NAMESPACE_LABEL)
        names = [ns.metadata.name for ns in ns_list.items]
    except Exception as e:
        logger.warning(
            "Could not list lakebench namespaces (%s); assuming others exist "
            "to avoid deleting a shared cluster-scoped resource.",
            e,
        )
        return True
    return not _is_last_lakebench_namespace(names, current_namespace)


def _buckets_hold_data(engine) -> bool | None:
    """True if any of the deployment's buckets exists and holds an object,
    False if none do, None if that cannot be determined. Read-only."""
    try:
        from lakebench.s3 import S3Client

        s3_cfg = engine.config.platform.storage.s3
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
            return None
        for bucket in (s3_cfg.buckets.bronze, s3_cfg.buckets.silver, s3_cfg.buckets.gold):
            if not s3.bucket_exists(bucket):
                continue
            resp = s3.raw_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
            if int(resp.get("KeyCount", 0)) > 0:
                return True
        return False
    except Exception as e:  # noqa: BLE001
        logger.debug("bucket contents check failed: %s", e)
        return None


def destroy_all(
    engine: DeploymentEngine,
    progress_callback: Callable[[str, DeploymentStatus, str], None] | None = None,
    clean_buckets: bool = True,
    allow_unverified_cluster: bool = False,
    force_legacy: bool = False,
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
        allow_unverified_cluster: Bypass the api-server fingerprint match
            when it cannot be computed on one or both sides. Only use
            when you know the current kubectl context is correct.
        force_legacy: Proceed on a namespace or bucket that has no
            lakebench ownership annotations/tags. Refused by default
            (the design invariant is "destroy refuses without proof
            of ownership"; a warn-and-proceed defeats it). Foreign
            annotations/tags are refused regardless of this flag.

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

    # Step 0: Identity check. Refuse if this namespace is owned by another
    # lakebench deployment or targets a different cluster. See
    # docs/design/namespace-isolation.md.
    #
    # Legacy (annotation-less) namespaces are REFUSED unless the caller
    # passes ``force_legacy=True``. Warn-and-proceed on legacy state
    # would defeat the whole invariant: a destroy pointed at a
    # namespace we cannot prove is ours would proceed to empty its
    # buckets. The supported path is ``lakebench admin
    # migrate-deployment`` first (which stamps the annotations), then
    # an ordinary destroy that verifies. `--force-legacy` is the
    # explicit escape hatch for operators who have confirmed the
    # namespace is theirs and cannot run migrate first.
    from lakebench.deploy.ownership import (
        IdentityVerdict,
        build_identity_from_config,
        verify_namespace_identity,
    )

    # Whether this destroy has proof that the namespace (and so the data it
    # names) belongs to the caller. Without the namespace there is no
    # identity record to check: a stale or wrong kubeconfig context makes a
    # live deployment's namespace look absent, while its buckets (on a shared
    # object store) and tables are still reachable by name. The data-touching
    # steps below therefore require a verified namespace or --force-legacy.
    ownership_proven = False
    namespace_present = engine.k8s.namespace_exists(namespace)
    if namespace_present:
        identity = build_identity_from_config(
            engine.config,
            context=engine.config.platform.kubernetes.context or "",
        )
        core_v1 = k8s_client.CoreV1Api()
        v = verify_namespace_identity(
            core_v1,
            namespace,
            identity.name,
            identity.api_server,
            allow_unverified_cluster=allow_unverified_cluster,
        )
        if v.verdict is IdentityVerdict.MISMATCH:
            results.append(
                DeploymentResult(
                    component="ownership-check",
                    status=DeploymentStatus.FAILED,
                    message=f"Namespace ownership refused: {v.hint}",
                )
            )
            report(
                "ownership-check",
                DeploymentStatus.FAILED,
                f"Refused: {v.hint}",
            )
            return results
        if v.verdict is IdentityVerdict.ABSENT:
            if not force_legacy:
                hint = (
                    f"Namespace {namespace!r} has no lakebench identity "
                    "annotations. Refusing to destroy without proof of "
                    "ownership. Run `lakebench admin migrate-deployment "
                    f"{namespace}` first to stamp identity, then re-run "
                    "destroy; or pass --force-legacy if you have "
                    "confirmed the namespace is yours and cannot migrate."
                )
                results.append(
                    DeploymentResult(
                        component="ownership-check",
                        status=DeploymentStatus.FAILED,
                        message=hint,
                    )
                )
                report("ownership-check", DeploymentStatus.FAILED, hint)
                return results
            report(
                "ownership-check",
                DeploymentStatus.SUCCESS,
                f"--force-legacy: proceeding on unannotated namespace {namespace!r}.",
            )
            logger.warning(
                "destroy --force-legacy: proceeding on unannotated namespace %s "
                "(no lakebench identity to verify against)",
                namespace,
            )
            ownership_proven = True
        else:
            # Only a MATCH is proof. NOT_FOUND here means the namespace
            # vanished between the exists check and the read (a concurrent
            # destroy), which proves nothing.
            ownership_proven = v.verdict is IdentityVerdict.MATCH
            report(
                "ownership-check",
                DeploymentStatus.SUCCESS,
                f"Verified deployment: {identity.name}",
            )

    from lakebench.deploy.ownership import check_data_ownership

    decision = check_data_ownership(
        k8s_client.CoreV1Api(),
        namespace=namespace,
        deployment_name=engine.config.name,
        namespace_present=namespace_present,
        namespace_verified=ownership_proven,
        force_legacy=force_legacy,
        context_name=engine.config.platform.kubernetes.context or "",
    )
    data_steps_allowed = decision.allowed
    no_ns_hint = decision.hint
    # A refusal is FAILED (printed, non-zero exit) because it leaves data
    # behind. With the namespace gone and the buckets absent or empty there is
    # nothing left to protect (a re-run after a complete destroy, or a deploy
    # that never got as far as creating them), so that is a plain skip.
    refusal_status = DeploymentStatus.FAILED
    if not data_steps_allowed and not namespace_present:
        if _buckets_hold_data(engine) is False:
            refusal_status = DeploymentStatus.SKIPPED
            no_ns_hint = (
                f"Namespace {namespace!r} and its buckets are already gone or empty; "
                "nothing to clean."
            )
    if not data_steps_allowed:
        logger.warning(no_ns_hint)
    elif decision.hint:
        # Printed, not just progress: a --force-legacy wipe by name, or a
        # shared-name check that could not run, must leave a visible record.
        logger.warning(decision.hint)
        report("ownership-check", DeploymentStatus.SUCCESS, decision.hint)

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
    if not data_steps_allowed:
        results.append(
            DeploymentResult(
                component="table-cleanup",
                status=refusal_status,
                message=no_ns_hint,
            )
        )
        report("table-cleanup", refusal_status, no_ns_hint)
    else:
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

                        maint_sqls = build_delta_maintenance_sql(maint_engine, catalog, table, 0.0)
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
    # Set when the bucket step failed for a reason a retry can fix (S3
    # unreachable, an error while emptying). The namespace is then kept as
    # the buckets' ownership record. Ownership refusals do not set it: those
    # buckets are not provably ours, so keeping the namespace gains nothing
    # and would block destroy forever.
    bucket_transient_failure = False
    if clean_buckets and not data_steps_allowed:
        results.append(
            DeploymentResult(
                component="s3-buckets",
                status=refusal_status,
                message=no_ns_hint,
            )
        )
        report("s3-buckets", refusal_status, no_ns_hint)
    elif clean_buckets:
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
                ca_cert=s3_cfg.ca_cert,
                verify_ssl=s3_cfg.verify_ssl,
            )
            if s3._init_error:
                bucket_transient_failure = True
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
                # Ownership check per bucket: refuse to empty a bucket
                # that carries another deployment's tag OR that has no
                # lakebench ownership tag at all (legacy). The design
                # invariant is "destroy refuses without proof of
                # ownership"; a warn-and-proceed on absent tags would
                # let a destroy empty a bucket that belongs to another
                # workload. `--force-legacy` is the explicit opt-in.
                from lakebench.deploy.ownership import (
                    IdentityVerdict,
                    bucket_name_matches_deployment,
                    list_lakebench_deployment_names,
                    verify_bucket_ownership,
                )

                identity_name = engine.config.name
                # Cluster-scan other lakebench deployments so the
                # UNSUPPORTED fallback can enforce longest-prefix-wins.
                # ``None`` means "cannot know" and the UNSUPPORTED
                # branch below MUST refuse rather than fall back to
                # naive prefix. F8 (round-3): load the kubeconfig via
                # get_k8s_client(context=...) so a stale ambient
                # KUBECONFIG cannot make CoreV1Api target the wrong
                # cluster.
                from kubernetes import client as _kclient

                from lakebench.k8s import get_k8s_client as _get_k8s

                _get_k8s(
                    context=engine.config.platform.kubernetes.context or "",
                    namespace=engine.config.get_namespace(),
                )
                other_deployments = list_lakebench_deployment_names(
                    _kclient.CoreV1Api(),
                    exclude=engine.config.get_namespace(),
                )
                mismatched: list[str] = []
                legacy_refused: list[str] = []
                legacy_forced: list[str] = []
                unsupported_refused: list[str] = []
                unsupported_forced: list[str] = []
                unsupported_by_prefix: list[str] = []
                for bucket in buckets:
                    v = verify_bucket_ownership(s3.raw_client, bucket, identity_name)
                    if v.verdict is IdentityVerdict.MISMATCH:
                        mismatched.append(f"{bucket} ({v.hint})")
                    elif v.verdict is IdentityVerdict.ABSENT:
                        if not force_legacy:
                            legacy_refused.append(bucket)
                        else:
                            legacy_forced.append(bucket)
                            logger.warning(
                                "destroy --force-legacy: emptying untagged "
                                "bucket %s (no lakebench ownership tag; "
                                "may contain data from another workload)",
                                bucket,
                            )
                            report(
                                "s3-buckets",
                                DeploymentStatus.IN_PROGRESS,
                                f"--force-legacy: untagged bucket {bucket}",
                            )
                    elif v.verdict is IdentityVerdict.UNSUPPORTED:
                        # Backend does not implement bucket tagging.
                        # Fallback: proceed only if the bucket name
                        # prefix-matches this deployment AND no other
                        # lakebench deployment on the cluster has a
                        # longer-prefix claim (longest-prefix-wins).
                        # ``other_deployments is None`` means the
                        # enumeration itself failed and we cannot
                        # enforce longest-prefix -- refuse rather than
                        # fall back to naive prefix (F2, round-3).
                        prefix_ok = (
                            other_deployments is not None
                            and bucket_name_matches_deployment(
                                bucket, identity_name, other_deployments
                            )
                        )
                        if prefix_ok:
                            unsupported_by_prefix.append(bucket)
                            logger.warning(
                                "destroy: bucket %s on a backend without "
                                "tagging support. Proceeding by "
                                "name-prefix match against deployment %r.",
                                bucket,
                                identity_name,
                            )
                            report(
                                "s3-buckets",
                                DeploymentStatus.IN_PROGRESS,
                                f"name-prefix ownership: {bucket}",
                            )
                        elif force_legacy:
                            unsupported_forced.append(bucket)
                            logger.warning(
                                "destroy --force-legacy: emptying bucket "
                                "%s on a backend without tagging support "
                                "AND without name-prefix match against "
                                "deployment %r. Operator has asserted "
                                "ownership.",
                                bucket,
                                identity_name,
                            )
                            # F6: mirror the IN_PROGRESS report on the
                            # forced branch so a --force-legacy wipe is
                            # visible in the progress stream, not just
                            # in the final summary.
                            report(
                                "s3-buckets",
                                DeploymentStatus.IN_PROGRESS,
                                f"--force-legacy (no tagging, no prefix match): {bucket}",
                            )
                        else:
                            unsupported_refused.append(bucket)
                if unsupported_refused and other_deployments is None:
                    # Could not list sibling deployments: a transient cluster
                    # or RBAC problem, not proof the buckets are someone
                    # else's. Keep the namespace so a re-run can finish.
                    bucket_transient_failure = True
                if mismatched or legacy_refused or unsupported_refused:
                    parts = []
                    if mismatched:
                        parts.append("owned by another deployment: " + "; ".join(mismatched))
                    if legacy_refused:
                        parts.append(
                            "no lakebench ownership tag: "
                            + ", ".join(legacy_refused)
                            + " (pass --force-legacy if you have "
                            "confirmed these are yours; otherwise use "
                            "`lakebench admin reclaim-bucket <name>` to "
                            "adopt an untagged bucket)"
                        )
                    if unsupported_refused:
                        if other_deployments is None:
                            unsupported_reason = (
                                "backend does not support bucket tagging, "
                                "and lakebench could not enumerate other "
                                "deployments on the cluster (likely RBAC "
                                "on `namespaces` list) so longest-prefix "
                                "safety cannot be enforced: "
                            )
                        else:
                            unsupported_reason = (
                                "backend does not support bucket tagging "
                                f"and bucket name does not grant "
                                f"deployment {identity_name!r} a "
                                "name-prefix claim (or another lakebench "
                                "deployment on this cluster has a longer "
                                "prefix): "
                            )
                        parts.append(
                            unsupported_reason
                            + ", ".join(unsupported_refused)
                            + " (rename buckets to start with the "
                            "deployment name, grant cluster-wide "
                            "`list namespaces`, or pass --force-legacy "
                            "if you have confirmed these are yours)"
                        )
                    msg = "Bucket ownership refused; " + " | ".join(parts)
                    results.append(
                        DeploymentResult(
                            component="s3-buckets",
                            status=DeploymentStatus.FAILED,
                            message=msg,
                        )
                    )
                    report("s3-buckets", DeploymentStatus.FAILED, msg)
                else:
                    legacy = legacy_forced  # preserved local name for summary below
                    total_deleted = 0
                    for bucket in buckets:
                        deleted = s3.empty_bucket(bucket)
                        total_deleted += deleted
                    notes = []
                    if legacy:
                        notes.append(
                            f"WARN: {len(legacy)} legacy untagged buckets: {', '.join(legacy)}"
                        )
                    if unsupported_by_prefix:
                        notes.append(
                            f"{len(unsupported_by_prefix)} buckets destroyed by "
                            "name-prefix (backend does not support tagging): "
                            + ", ".join(unsupported_by_prefix)
                        )
                    if unsupported_forced:
                        notes.append(
                            f"WARN: {len(unsupported_forced)} buckets destroyed "
                            "by --force-legacy on a backend without tagging "
                            "AND without name-prefix match: " + ", ".join(unsupported_forced)
                        )
                    summary_note = f" ({' | '.join(notes)})" if notes else ""
                    results.append(
                        DeploymentResult(
                            component="s3-buckets",
                            status=DeploymentStatus.SUCCESS,
                            message=(
                                f"Cleaned {len(buckets)} S3 buckets "
                                f"({total_deleted} objects)" + summary_note
                            ),
                        )
                    )
                    report(
                        "s3-buckets",
                        DeploymentStatus.SUCCESS,
                        f"S3 buckets cleaned ({total_deleted} objects)" + summary_note,
                    )
        except Exception as e:
            bucket_transient_failure = True
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
        # Delete SecretClasses (cluster-scoped, namespace-suffixed).
        # Both credentials and CA-cert SecretClasses carry the deploying
        # namespace as a suffix so parallel deployments do not collide;
        # destroy removes only the ones scoped to this deployment.
        for sc_name in (
            f"lakebench-s3-credentials-{namespace}",
            f"lakebench-s3-ca-cert-{namespace}",
        ):
            try:
                custom_api.delete_cluster_custom_object(
                    group="secrets.stackable.tech",
                    version="v1alpha1",
                    plural="secretclasses",
                    name=sc_name,
                )
            except ApiException as e:
                logger.debug("SecretClass %s delete skipped: %s", sc_name, e.reason)
        # ADR-F6: legacy fixed-name SecretClasses (`lakebench-s3-credentials-class`,
        # `lakebench-s3-ca-cert-class`) belong to pre-PR-2 deployments. They
        # only exist in cluster state if this or another deployment was
        # migrated from pre-PR-2 via `admin migrate-deployment` (which
        # copies but does not delete). It is safe to clean them up when
        # this destroy is the last remaining deployment that could
        # depend on them -- i.e. no other lakebench-annotated namespace
        # remains cluster-wide, and no annotationless legacy namespace
        # is still around either. If either is present we leave the
        # legacy names in place; the operator can reclaim them once the
        # final deployment migrates and destroys.
        try:
            all_ns = core_v1.list_namespace().items

            # ADR-F6b: a legacy pre-PR-1 deployment predates the
            # annotation. Detect it via the managed-by LABEL that both
            # PR-1 (annotated) and pre-PR-1 (annotationless) namespaces
            # carry. Also skip cleanup on any namespace still in
            # ``Active`` phase to avoid ripping the legacy SC out from
            # under a mid-run Terminating tenant.
            def _is_other_lakebench(n) -> bool:
                if n.metadata.name == namespace:
                    return False
                anns = n.metadata.annotations or {}
                labels = n.metadata.labels or {}
                if anns.get("lakebench.deployment/name"):
                    return True
                if labels.get("app.kubernetes.io/managed-by") == "lakebench":
                    return True
                if labels.get("app.kubernetes.io/name") == "lakebench":
                    return True
                return False

            other_lakebench = [n for n in all_ns if _is_other_lakebench(n)]
            if not other_lakebench:
                for legacy_name in (
                    "lakebench-s3-credentials-class",
                    "lakebench-s3-ca-cert-class",
                ):
                    try:
                        custom_api.delete_cluster_custom_object(
                            group="secrets.stackable.tech",
                            version="v1alpha1",
                            plural="secretclasses",
                            name=legacy_name,
                        )
                        logger.info(
                            "Removed legacy SecretClass %s (last migrated deployment)",
                            legacy_name,
                        )
                    except ApiException as e:
                        logger.debug(
                            "Legacy SecretClass %s delete skipped: %s",
                            legacy_name,
                            e.reason,
                        )
        except Exception as e:  # noqa: BLE001
            logger.debug("Legacy SecretClass cluster-wide check skipped: %s", e)
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

    # Step 9: StorageClass is Category 2 shared infrastructure. lakebench
    # never deletes it -- another parallel deployment on the same cluster
    # relies on it, and stripping it out from under them would break
    # every running Spark job that references it. A cluster admin who
    # genuinely wants it gone deletes it directly with
    # `kubectl delete storageclass <name>`.

    # Finally, delete namespace if we created it
    if engine.config.platform.kubernetes.create_namespace and bucket_transient_failure:
        # The namespace's identity annotations are the only proof that the
        # buckets belong to this deployment. Deleting it after the bucket step
        # failed would leave full buckets that no later destroy can prove it
        # owns. Keep the namespace (and its watch-list entry, so it still
        # works) until the bucket problem is fixed and destroy is re-run.
        keep_msg = (
            f"Namespace {namespace!r} NOT deleted because emptying the S3 "
            "buckets failed; the namespace is the ownership record for them. "
            "Fix the S3 error above and re-run destroy. Do not delete this "
            "namespace by hand: it is still in the Spark Operator watch list, "
            "and deleting a watched namespace crash-loops the operator for "
            "every deployment on the cluster."
        )
        logger.error(keep_msg)
        results.append(
            DeploymentResult(
                component="namespace",
                status=DeploymentStatus.FAILED,
                message=keep_msg,
            )
        )
        report("namespace", DeploymentStatus.FAILED, keep_msg)
        return results
    if engine.config.platform.kubernetes.create_namespace:
        # Drop the namespace from the Spark Operator's watch list FIRST. The
        # operator crash-loops on a watched namespace that does not exist
        # ("failed to wait for ... caches to sync"), which breaks
        # SparkApplication reconciliation for every other namespace on the
        # cluster. Doing this after the delete would leave exactly that
        # window open.
        #
        # strict=True lease-gates the mutation against parallel destroys
        # and raises WatchListMutationError on failure -- surfacing a
        # crash-looping operator explicitly rather than swallowing it as
        # a successful destroy. Any failure here is captured as a
        # DeploymentResult and reported in the destroy summary.
        from lakebench.modules.pipeline_engines.spark.operator import (
            WatchListMutationError,
        )
        from lakebench.spark import SparkOperatorManager

        spark_op_cfg = engine.config.platform.compute.spark.operator
        watch_list_ok = True
        try:
            SparkOperatorManager(
                namespace=spark_op_cfg.namespace,
                version=spark_op_cfg.version,
                job_namespace=namespace,
                kube_context=engine.config.platform.kubernetes.context,
            ).remove_namespace_from_watch(namespace, strict=True)
            report(
                "spark-operator-watch",
                DeploymentStatus.SUCCESS,
                f"Dropped {namespace} from Spark Operator watch list",
            )
        except WatchListMutationError as e:
            # ADR-F1: watch-list drop failed. Do NOT delete the namespace
            # after this: the operator would crash-loop on a watched
            # namespace that no longer exists ("failed to wait for
            # spark-application-controller caches to sync"), taking down
            # SparkApplication reconciliation for every namespace on the
            # cluster -- the exact failure this whole path is written to
            # prevent. Record the failure loudly and leave the namespace
            # in place until `lakebench admin repair-operator` succeeds.
            watch_list_ok = False
            logger.error("Spark Operator watch-list mutation failed: %s", e)
            results.append(
                DeploymentResult(
                    component="spark-operator-watch",
                    status=DeploymentStatus.FAILED,
                    message=str(e),
                )
            )
            report(
                "spark-operator-watch",
                DeploymentStatus.FAILED,
                "Watch-list mutation failed; run admin repair-operator",
            )

        if not watch_list_ok:
            # Preserve the invariant: never delete a namespace the operator
            # still watches. The namespace stays, the user runs
            # `admin repair-operator`, then re-runs destroy.
            skip_msg = (
                f"Namespace {namespace!r} NOT deleted because the operator "
                "watch-list mutation failed. Run "
                "`lakebench admin repair-operator` first, then re-run destroy."
            )
            results.append(
                DeploymentResult(
                    component="namespace",
                    status=DeploymentStatus.SKIPPED,
                    message=skip_msg,
                )
            )
            report("namespace", DeploymentStatus.SKIPPED, skip_msg)
            return results

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
