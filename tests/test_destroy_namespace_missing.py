"""Destroy must not touch tables or buckets when the namespace is missing.

Regression for a cross-deployment data-loss path found in the 2026-09-24 code
audit: the identity check only ran ``if namespace_exists``, and the table and
bucket steps ran regardless. With a stale or wrong kubeconfig context, a live
deployment's namespace looks absent while its buckets (on a shared object
store) are still reachable by name, so destroy emptied another deployment's
data. The namespace is the identity record; without it there is no proof of
ownership.

Contract:
- namespace absent, no --force-legacy: table-cleanup and s3-buckets are
  SKIPPED with a hint naming the context, and no S3 client is built.
- namespace absent, --force-legacy: the data steps run (the documented escape
  hatch for "a previous destroy removed the namespace but left the buckets").
- namespace present and verified: unchanged.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.deploy.engine import DeploymentStatus


def _engine(namespace_exists: bool) -> MagicMock:
    engine = MagicMock()
    cfg = engine.config
    cfg.get_namespace.return_value = "lb-test"
    cfg.name = "lb-test"
    cfg.platform.kubernetes.create_namespace = True
    cfg.platform.kubernetes.context = "stale-ctx"
    cfg.platform.compute.spark.operator.namespace = "spark-operator"
    cfg.platform.compute.spark.operator.version = "2.5.1"
    engine.k8s.namespace_exists.return_value = namespace_exists
    return engine


def _run(engine, force_legacy: bool):
    from lakebench.deploy.destroy import destroy_all

    with (
        patch("kubernetes.client.CoreV1Api"),
        patch("kubernetes.client.CustomObjectsApi"),
        patch("kubernetes.client.RbacAuthorizationV1Api"),
        patch("kubernetes.client.StorageV1Api"),
        patch("kubernetes.client.BatchV1Api"),
        patch("lakebench.deploy.destroy.logger"),
        patch("lakebench.s3.S3Client") as s3_cls,
        patch("lakebench.deploy.iceberg.find_maintenance_engine", return_value=(None, None, None)),
    ):
        results = destroy_all(engine, force_legacy=force_legacy, clean_buckets=True)
    return results, s3_cls


def _by_component(results, name):
    return [r for r in results if r.component == name]


def test_absent_namespace_leaves_tables_and_buckets_alone():
    results, s3_cls = _run(_engine(namespace_exists=False), force_legacy=False)
    s3 = _by_component(results, "s3-buckets")
    tables = _by_component(results, "table-cleanup")
    assert s3 and all(r.status is DeploymentStatus.SKIPPED for r in s3)
    assert tables and all(r.status is DeploymentStatus.SKIPPED for r in tables)
    assert "stale-ctx" in s3[0].message and "--force-legacy" in s3[0].message
    s3_cls.assert_not_called()


def test_absent_namespace_with_force_legacy_cleans_buckets():
    _results, s3_cls = _run(_engine(namespace_exists=False), force_legacy=True)
    s3_cls.assert_called_once()


def test_present_verified_namespace_still_cleans_buckets():
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    ok = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name="lb-test",
        expected_deployment="lb-test",
        hint="",
    )
    with patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=ok):
        _results, s3_cls = _run(_engine(namespace_exists=True), force_legacy=False)
    s3_cls.assert_called_once()


def test_namespace_kept_when_bucket_step_fails():
    """The namespace is the ownership record for the buckets; never delete
    it after the bucket step failed."""
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    ok = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name="lb-test",
        expected_deployment="lb-test",
        hint="",
    )
    engine = _engine(namespace_exists=True)
    s3 = MagicMock()
    s3._init_error = "endpoint unreachable"
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=ok),
        patch("kubernetes.client.CoreV1Api"),
        patch("kubernetes.client.CustomObjectsApi"),
        patch("kubernetes.client.RbacAuthorizationV1Api"),
        patch("kubernetes.client.StorageV1Api"),
        patch("kubernetes.client.BatchV1Api"),
        patch("lakebench.deploy.destroy.logger"),
        patch("lakebench.s3.S3Client", return_value=s3),
        patch("lakebench.deploy.iceberg.find_maintenance_engine", return_value=(None, None, None)),
        patch("lakebench.spark.SparkOperatorManager") as op,
    ):
        results = destroy_all(engine, clean_buckets=True)
    engine.k8s.delete_namespace.assert_not_called()
    op.return_value.remove_namespace_from_watch.assert_not_called()
    ns = [r for r in results if r.component == "namespace"]
    assert ns and ns[0].status is DeploymentStatus.SKIPPED
