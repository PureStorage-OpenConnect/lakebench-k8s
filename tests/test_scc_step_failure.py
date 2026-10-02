"""DEP-4: a refused SCC grant fails the deploy step (fix-reverted).

Before DEP-4 the RBAC step reported SUCCESS when the grant failed, and the
PostgreSQL step logged a warning and carried on; the pods were then
admission-rejected on their UID much later. These tests import nothing DEP-4
added, so they run unchanged against the pre-DEP-4 tree and fail there.

Nothing reaches a cluster or runs ``oc``: the RBAC API is a fake and every
``pinned_oc`` is patched to a failing stub.
"""

from __future__ import annotations

import copy
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from kubernetes.client.rest import ApiException

from lakebench.deploy.engine import DeploymentStatus
from lakebench.k8s import PlatformType
from lakebench.k8s.security import SecurityVerifier

NS = "lb-ns"
SA = "lakebench-spark-runner"


class NotAllowed:
    """LocalSubjectAccessReview that says the SA may not use the SCC."""

    def create_namespaced_local_subject_access_review(self, ns, body):
        return SimpleNamespace(status=SimpleNamespace(allowed=False))


class RefusingRbac:
    """The ClusterRole exists; every RoleBinding write is forbidden."""

    api_client = SimpleNamespace(sanitize_for_serialization=copy.deepcopy)

    def read_cluster_role(self, name):
        return {"metadata": {"name": name}}

    def read_namespaced_role_binding(self, name, ns):
        raise ApiException(status=404)

    def create_namespaced_role_binding(self, ns, body):
        raise ApiException(status=403, reason="Forbidden")

    def replace_namespaced_role_binding(self, name, ns, body):
        raise ApiException(status=403, reason="Forbidden")


def _engine(context=None):
    renderer = MagicMock()
    renderer.render.return_value = "kind: ServiceAccount\nmetadata:\n  name: x\n"
    cfg = MagicMock()
    cfg.get_namespace.return_value = NS
    cfg.platform.kubernetes.context = ""
    cfg.images.spark = "apache/spark:4.0.1"
    return SimpleNamespace(
        config=cfg, k8s=MagicMock(), renderer=renderer, context=context or {}, dry_run=False
    )


def _refusing_cluster(monkeypatch):
    """OpenShift whose RBAC API refuses the bind; any oc call fails too."""
    monkeypatch.setattr(
        SecurityVerifier, "detect_platform", lambda self, strict=False: _openshift()
    )
    monkeypatch.setattr(
        "kubernetes.client.AuthorizationV1Api", lambda *a, **k: NotAllowed(), raising=False
    )
    monkeypatch.setattr(SecurityVerifier, "_SCC_ADD_BACKOFF_SECONDS", 0, raising=False)
    monkeypatch.setattr("lakebench.k8s.security.SCC_VERIFY_TIMEOUT_S", 0.0, raising=False)
    failing_oc = MagicMock(return_value=SimpleNamespace(returncode=1, stdout="", stderr="denied"))
    monkeypatch.setattr("lakebench.k8s.security.pinned_oc", failing_oc, raising=False)
    monkeypatch.setattr("lakebench.deploy.postgres.pinned_oc", failing_oc, raising=False)
    return patch("kubernetes.client.RbacAuthorizationV1Api", return_value=RefusingRbac())


def _openshift():

    return PlatformType.OPENSHIFT


def test_scc_failure_fails_step(monkeypatch):
    """Spark RBAC step: a refused grant is FAILED with the admin command.
    Fails reverted: the old path returned False and the step was SUCCESS."""
    from lakebench.modules.pipeline_engines.spark.rbac import RBACDeployer

    with _refusing_cluster(monkeypatch):
        result = RBACDeployer(_engine()).deploy()
    assert result.status is DeploymentStatus.FAILED
    assert result.message.startswith(f"cannot grant SCC anyuid to SA {SA} in namespace {NS}")
    assert "oc adm policy add-scc-to-user" in result.message


def test_scc_failure_fails_postgres_step(monkeypatch):
    """PostgreSQL step: same rule. Fails reverted: the old path logged a
    warning and went on to wait for the StatefulSet."""
    from lakebench.deploy import postgres as pg
    from lakebench.k8s import WaitStatus

    ready = SimpleNamespace(status=WaitStatus.READY, message="")
    monkeypatch.setattr(pg, "wait_for_statefulset_ready", lambda *a, **k: ready)
    monkeypatch.setattr(pg, "wait_for_postgres_ready", lambda *a, **k: ready)
    engine = _engine(context={"openshift_mode": True})
    engine.k8s.exec_in_pod.return_value = "PostgreSQL 16"
    with _refusing_cluster(monkeypatch):
        result = pg.PostgresDeployer(engine).deploy()
    assert result.status is DeploymentStatus.FAILED
    assert "cannot grant SCC anyuid to SA lakebench-postgres" in result.message
