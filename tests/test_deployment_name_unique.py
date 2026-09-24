"""Deployment names must be unique across namespaces (2026-09-24 audit).

Bucket ownership is keyed on the deployment name (a tag, or a name prefix on
FlashBlade). Two namespaces carrying one name both claim the same buckets, so
destroying either deletes the other's data. Deploy now refuses a name already
used by another namespace, and destroy leaves shared tables/buckets alone.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from lakebench.deploy.engine import DeploymentStatus
from lakebench.deploy.ownership import ANNOTATION_DEPLOYMENT_NAME


def _ns(name, dep_name=None):
    anns = {ANNOTATION_DEPLOYMENT_NAME: dep_name} if dep_name else {}
    return SimpleNamespace(metadata=SimpleNamespace(name=name, annotations=anns, labels={}))


def _engine_obj(name="team-a"):
    from lakebench.deploy.engine import DeploymentEngine

    eng = DeploymentEngine.__new__(DeploymentEngine)
    eng.config = MagicMock()
    eng.config.name = name
    eng.config.get_namespace.return_value = "ns-a"
    eng.config.platform.kubernetes.context = ""
    eng.dry_run = False
    eng.k8s = MagicMock()
    return eng


def test_deploy_refuses_name_used_by_other_namespace():
    eng = _engine_obj()
    with (
        patch("lakebench.k8s.get_k8s_client"),
        patch("kubernetes.client.CoreV1Api") as core,
    ):
        core.return_value.list_namespace.return_value.items = [
            _ns("ns-a", "team-a"),
            _ns("ns-b", "team-a"),
        ]
        result = eng._deploy_namespace()
    assert result.status is DeploymentStatus.FAILED
    assert "ns-b" in result.message and "unique" in result.message
    eng.k8s.namespace_exists.assert_not_called()


def test_uniqueness_check_ignores_own_namespace_and_other_names():
    eng = _engine_obj()
    with (
        patch("lakebench.k8s.get_k8s_client"),
        patch("kubernetes.client.CoreV1Api") as core,
    ):
        core.return_value.list_namespace.return_value.items = [
            _ns("ns-a", "team-a"),
            _ns("ns-c", "team-c"),
            _ns("kube-system"),
        ]
        assert eng._namespace_already_using_name("ns-a") is None


def test_destroy_skips_data_steps_when_name_is_shared():
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    engine = MagicMock()
    cfg = engine.config
    cfg.get_namespace.return_value = "ns-a"
    cfg.name = "team-a"
    cfg.platform.kubernetes.create_namespace = True
    cfg.platform.kubernetes.context = ""
    cfg.platform.compute.spark.operator.namespace = "spark-operator"
    cfg.platform.compute.spark.operator.version = "2.5.1"
    engine.k8s.namespace_exists.return_value = True
    ok = IdentityReport(
        verdict=IdentityVerdict.MATCH, resource_name="ns-a", expected_deployment="team-a", hint=""
    )
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=ok),
        patch("kubernetes.client.CoreV1Api") as core,
        patch("kubernetes.client.CustomObjectsApi"),
        patch("kubernetes.client.RbacAuthorizationV1Api"),
        patch("kubernetes.client.StorageV1Api"),
        patch("kubernetes.client.BatchV1Api"),
        patch("lakebench.deploy.destroy.logger"),
        patch("lakebench.s3.S3Client") as s3_cls,
    ):
        core.return_value.list_namespace.return_value.items = [
            _ns("ns-a", "team-a"),
            _ns("ns-b", "team-a"),
        ]
        results = destroy_all(engine, clean_buckets=True)
    s3_cls.assert_not_called()
    skipped = [r for r in results if r.component == "s3-buckets"]
    assert skipped and skipped[0].status is DeploymentStatus.FAILED
    assert "ns-b" in skipped[0].message


def test_force_legacy_does_not_override_a_live_shared_name():
    """--force-legacy waives only "namespace missing". Two live deployments
    sharing a name share buckets, so nothing makes cleaning them safe."""
    from lakebench.deploy.ownership import check_data_ownership

    core = MagicMock()
    core.list_namespace.return_value.items = [_ns("ns-b", "team-a")]
    d = check_data_ownership(
        core,
        namespace="ns-a",
        deployment_name="team-a",
        namespace_present=False,
        namespace_verified=False,
        force_legacy=True,
    )
    assert not d.allowed and "ns-b" in d.hint


def test_terminating_namespace_with_same_name_does_not_block():
    from lakebench.deploy.ownership import check_data_ownership

    ns_b = _ns("ns-b", "team-a")
    ns_b.metadata.deletion_timestamp = "2026-09-24T00:00:00Z"
    core = MagicMock()
    core.list_namespace.return_value.items = [ns_b]
    d = check_data_ownership(
        core,
        namespace="ns-a",
        deployment_name="team-a",
        namespace_present=True,
        namespace_verified=True,
        force_legacy=False,
    )
    assert d.allowed
