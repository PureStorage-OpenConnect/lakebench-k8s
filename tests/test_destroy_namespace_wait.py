"""Destroy waits for the namespace to be gone before saying so (LB-157).

A namespace delete returns as soon as the API server accepts it. The namespace
then stays Terminating while its content drains (a PVC held by
kubernetes.io/pvc-protection until its pod stops can take minutes). Live UAT
S-P3/S-P4 saw destroy print "Namespace <ns> deleted" while ``kubectl get ns``
still found it, and two concurrent destroys both claimed the delete.

The wait runs on the fake clock from conftest (``_fake_destroy_namespace_clock``).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.rest import ApiException

from lakebench.deploy import destroy as destroy_mod
from lakebench.deploy.engine import DeploymentStatus
from lakebench.k8s.client import K8sClient, K8sResourceError, NamespaceTerminatingError

PVC_BLOCKER = (
    "Some content in the namespace has finalizers remaining: "
    "kubernetes.io/pvc-protection in 1 resource instances"
)


class FakeCluster:
    """One namespace shared by any number of destroys.

    ``drain_polls`` is how many status reads after the delete still see it
    Terminating; ``None`` means it never finishes.
    """

    def __init__(self, phase: str = "Active", drain_polls: int | None = 3):
        self.phase = phase
        self.drain_polls = drain_polls
        self.deletes = 0

    def get_namespace_termination_status(self, name):
        if self.phase == "Terminating":
            if self.drain_polls is not None:
                if self.drain_polls <= 0:
                    self.phase = ""
                else:
                    self.drain_polls -= 1
        blockers = [PVC_BLOCKER] if self.phase == "Terminating" else []
        return self.phase, blockers

    def delete_namespace(self, name):
        if self.phase == "":
            return False
        if self.phase == "Terminating":
            raise NamespaceTerminatingError(f"Namespace '{name}' is already being deleted")
        self.phase = "Terminating"
        self.deletes += 1
        return True


def _engine(cluster: FakeCluster) -> SimpleNamespace:
    return SimpleNamespace(k8s=cluster)


def _run(cluster, timeout=600):
    reports: list[tuple[str, DeploymentStatus, str]] = []
    result = destroy_mod._delete_namespace_and_wait(
        _engine(cluster), "ns-a", timeout, lambda c, s, m: reports.append((c, s, m))
    )
    return result, reports


class TestWait:
    def test_terminating_then_gone_reports_deleted(self):
        cluster = FakeCluster(drain_polls=3)
        result, _ = _run(cluster)
        assert result.status is DeploymentStatus.SUCCESS
        assert result.message == "Namespace ns-a deleted"
        assert cluster.phase == ""

    def test_long_drain_prints_progress_naming_the_blocker(self):
        # 3 s polls, 30 s progress interval: 40 polls is two progress lines.
        cluster = FakeCluster(drain_polls=40)
        result, reports = _run(cluster)
        assert result.status is DeploymentStatus.SUCCESS
        progress = [m for _, s, m in reports if s is DeploymentStatus.IN_PROGRESS]
        assert progress, "a multi-minute wait must print progress"
        assert all("Waiting for namespace ns-a" in m for m in progress)
        assert "pvc-protection" in progress[0]

    def test_never_gone_is_a_warning_not_deleted(self):
        cluster = FakeCluster(drain_polls=None)
        result, _ = _run(cluster, timeout=600)
        assert result.status is DeploymentStatus.SKIPPED
        assert "deleted" not in result.message.replace("NOT deleted", "")
        assert "still terminating after 600s" in result.message
        assert "pvc-protection" in result.message
        assert destroy_mod._monotonic() >= 600, "the wait must actually run to its bound"

    def test_zero_timeout_does_not_wait(self):
        cluster = FakeCluster(drain_polls=None)
        before = destroy_mod._monotonic()
        result, _ = _run(cluster, timeout=0)
        assert result.status is DeploymentStatus.SKIPPED
        assert destroy_mod._monotonic() - before < 1

    def test_already_gone_is_not_claimed(self):
        cluster = FakeCluster(phase="")
        result, _ = _run(cluster)
        assert result.status is DeploymentStatus.SUCCESS
        assert "already gone" in result.message
        assert cluster.deletes == 0

    def test_delete_error_is_failed(self):
        cluster = FakeCluster()
        cluster.delete_namespace = MagicMock(side_effect=K8sResourceError("forbidden"))
        result, _ = _run(cluster)
        assert result.status is DeploymentStatus.FAILED
        assert "forbidden" in result.message

    def test_status_read_errors_keep_waiting(self):
        """A transient read error during the wait is not proof of anything."""
        cluster = FakeCluster(drain_polls=2)
        real = cluster.get_namespace_termination_status
        calls = {"n": 0}

        def flaky(name):
            calls["n"] += 1
            if calls["n"] == 2:
                raise K8sResourceError("apiserver 503")
            return real(name)

        cluster.get_namespace_termination_status = flaky
        result, _ = _run(cluster)
        assert result.status is DeploymentStatus.SUCCESS
        assert result.message == "Namespace ns-a deleted"


class TestConcurrentDestroys:
    def test_second_destroy_sees_terminating_and_does_not_claim(self):
        """S-P4: the second destroy arrives while the first one's delete drains."""
        cluster = FakeCluster(drain_polls=None)
        first_started = cluster.delete_namespace("ns-a")
        assert first_started
        cluster.drain_polls = 2
        result, reports = _run(cluster)
        assert result.status is DeploymentStatus.SUCCESS
        assert "deletion started by another run" in result.message
        assert any("already being deleted" in m for _, _, m in reports)
        assert cluster.deletes == 1

    def test_race_where_both_saw_active_turns_409_into_already_deleting(self):
        """Both destroys read Active; the loser's delete gets 409 Conflict."""
        cluster = FakeCluster(drain_polls=2)
        cluster.get_namespace_termination_status = MagicMock(
            side_effect=[("Active", []), ("Terminating", []), ("", [])]
        )
        # The winner's delete lands between the loser's read and delete.
        cluster.phase = "Terminating"
        result, reports = _run(cluster)
        assert result.status is DeploymentStatus.SUCCESS
        assert "deletion started by another run" in result.message
        assert any("already being deleted" in m for _, _, m in reports)

    def test_second_destroy_after_namespace_gone(self):
        cluster = FakeCluster(drain_polls=0)
        _run(cluster)
        result, _ = _run(cluster)
        assert "already gone" in result.message
        assert cluster.deletes == 1


class TestDestroyAllWiring:
    def _run_destroy(self, cluster, timeout=600):
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        engine = MagicMock()
        cfg = engine.config
        cfg.get_namespace.return_value = "ns-a"
        cfg.platform.kubernetes.create_namespace = True
        cfg.platform.compute.spark.operator.namespace = "spark-operator"
        cfg.platform.compute.spark.operator.version = "2.5.1"
        engine.k8s.namespace_exists.return_value = True
        engine.k8s.get_namespace_termination_status.side_effect = (
            cluster.get_namespace_termination_status
        )
        engine.k8s.delete_namespace.side_effect = cluster.delete_namespace
        calls: list[str] = []
        manager = MagicMock()
        manager.remove_namespace_from_watch.side_effect = lambda ns, **_kw: calls.append(
            f"unwatch:{ns}"
        )
        match = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="ns-a",
            expected_deployment="ns-a",
            found_deployment="ns-a",
        )
        reports: list[tuple[str, DeploymentStatus, str]] = []
        with (
            patch("lakebench.spark.SparkOperatorManager", MagicMock(return_value=manager)),
            patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.destroy.logger"),
        ):
            results = destroy_mod.destroy_all(
                engine,
                clean_buckets=False,
                namespace_wait_timeout=timeout,
                progress_callback=lambda c, s, m: reports.append((c, s, m)),
            )
        return results, reports, calls, engine

    def test_unwatch_precedes_delete_and_result_reflects_the_wait(self):
        cluster = FakeCluster(drain_polls=None)
        results, reports, calls, engine = self._run_destroy(cluster, timeout=120)
        assert calls == ["unwatch:ns-a"]
        engine.k8s.delete_namespace.assert_called_once_with("ns-a")
        ns = [r for r in results if r.component == "namespace"]
        assert ns[-1].status is DeploymentStatus.SKIPPED
        assert "still terminating" in ns[-1].message
        final = [r for r in reports if r[0] == "namespace"][-1]
        assert final[1] is DeploymentStatus.SKIPPED

    def test_gone_reports_success(self):
        cluster = FakeCluster(drain_polls=1)
        results, _, _, _ = self._run_destroy(cluster)
        ns = [r for r in results if r.component == "namespace"]
        assert ns[-1].status is DeploymentStatus.SUCCESS
        assert ns[-1].message == "Namespace ns-a deleted"


class TestClient:
    def _client(self, core):
        c = K8sClient.__new__(K8sClient)
        c._core_v1 = core
        return c

    def test_delete_on_terminating_namespace_raises_terminating(self):
        core = MagicMock()
        core.delete_namespace.side_effect = ApiException(status=409, reason="Conflict")
        with pytest.raises(NamespaceTerminatingError):
            self._client(core).delete_namespace("ns-a")

    def test_delete_404_race_is_not_a_delete(self):
        core = MagicMock()
        core.delete_namespace.side_effect = ApiException(status=404, reason="NotFound")
        assert self._client(core).delete_namespace("ns-a") is False

    def test_termination_status_reads_blocking_conditions(self):
        core = MagicMock()
        cond = [
            SimpleNamespace(
                type="NamespaceContentRemaining",
                status="True",
                message="Some resources are remaining: pods. has 1 resource instances",
                reason="SomeResourcesRemain",
            ),
            SimpleNamespace(
                type="NamespaceDeletionDiscoveryFailure",
                status="False",
                message="All resources successfully discovered",
                reason="ResourcesDiscovered",
            ),
        ]
        core.read_namespace.return_value = SimpleNamespace(
            status=SimpleNamespace(phase="Terminating", conditions=cond)
        )
        phase, blockers = self._client(core).get_namespace_termination_status("ns-a")
        assert phase == "Terminating"
        assert blockers == ["Some resources are remaining: pods. has 1 resource instances"]

    def test_termination_status_gone(self):
        core = MagicMock()
        core.read_namespace.side_effect = ApiException(status=404, reason="NotFound")
        assert self._client(core).get_namespace_termination_status("ns-a") == ("", [])


class TestDeployIntoTerminatingNamespace:
    def test_fails_with_a_clear_message(self):
        from lakebench.deploy.engine import DeploymentEngine

        eng = DeploymentEngine.__new__(DeploymentEngine)
        eng.config = MagicMock()
        eng.config.name = "team-a"
        eng.config.get_namespace.return_value = "ns-a"
        eng.config.platform.kubernetes.context = ""
        eng.dry_run = False
        eng.k8s = MagicMock()
        eng.k8s.namespace_exists.return_value = True
        eng.k8s.get_namespace_phase.return_value = "Terminating"
        eng.k8s.wait_for_namespace_deleted.side_effect = K8sResourceError("still exists")
        eng.k8s.get_namespace_termination_status.return_value = ("Terminating", [PVC_BLOCKER])
        with (
            patch("lakebench.k8s.get_k8s_client"),
            patch("kubernetes.client.CoreV1Api") as core,
        ):
            core.return_value.list_namespace.return_value.items = []
            result = eng._deploy_namespace()
        assert result.status is DeploymentStatus.FAILED
        assert "still terminating from an earlier destroy" in result.message
        assert "pvc-protection" in result.message
        eng.k8s.apply_manifest.assert_not_called()
