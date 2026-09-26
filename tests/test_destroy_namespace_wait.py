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
from lakebench.k8s.client import (
    K8sClient,
    K8sResourceError,
    NamespaceReplacedError,
    NamespaceTerminatingError,
)

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

    def delete_namespace(self, name, uid=None):
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


class TestPreconditionRefusal:
    def test_replaced_namespace_is_left_alone(self):
        from lakebench.k8s.client import NamespaceReplacedError

        cluster = FakeCluster()
        cluster.delete_namespace = MagicMock(side_effect=NamespaceReplacedError("uid"))
        result = destroy_mod._delete_namespace_and_wait(
            _engine(cluster), "ns-a", 600, lambda *a: None, uid="uid-1"
        )
        # Not completed: the old incarnation's delete never happened here.
        assert result.status is DeploymentStatus.FAILED
        assert "newer" in result.message
        cluster.delete_namespace.assert_called_once_with("ns-a", uid="uid-1")


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

    def test_namespace_recreated_by_concurrent_deploy_is_not_waited_on(self):
        """S-P3: a deploy recreates the name while this destroy waits."""
        cluster = FakeCluster()
        cluster.get_namespace_termination_status = MagicMock(
            side_effect=[("Active", []), ("Terminating", [PVC_BLOCKER]), ("Active", [])]
        )
        before = destroy_mod._monotonic()
        result, _ = _run(cluster)
        assert result.status is DeploymentStatus.SUCCESS
        assert "new namespace with the same name" in result.message
        assert destroy_mod._monotonic() - before < 60, "must not wait out the timeout"
        assert cluster.deletes == 1

    def test_second_destroy_after_namespace_gone(self):
        cluster = FakeCluster(drain_polls=0)
        _run(cluster)
        result, _ = _run(cluster)
        assert "already gone" in result.message
        assert cluster.deletes == 1


class TestDestroyAllWiring:
    def _run_destroy(
        self,
        cluster,
        timeout=600,
        uids=None,
        uid_fn=None,
        on_lease=None,
        nonce_fn=None,
        after_unwatch=None,
    ):
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
        calls: list[str] = []

        def delete(ns, uid=None):
            calls.append("delete")
            return cluster.delete_namespace(ns, uid=uid)

        engine.k8s.delete_namespace.side_effect = delete
        uid_seq = iter(uids or [])
        engine.k8s.get_namespace_annotation.side_effect = nonce_fn or (lambda _ns, _k: "n-1")
        engine.k8s.get_namespace_uid.side_effect = uid_fn or (
            lambda _ns: next(uid_seq, uids[-1] if uids else "uid-1")
        )
        manager = MagicMock()

        def unwatch(ns, strict=False, precondition=None, then=None):
            # Mirrors the lease: precondition, helm change, then, release.
            if on_lease is not None:
                on_lease(engine)
            if precondition is not None:
                precondition()
            calls.append(f"unwatch:{ns}")
            if after_unwatch is not None:
                after_unwatch(engine)
            if then is not None:
                then()
            calls.append("release")

        manager.remove_namespace_from_watch.side_effect = unwatch
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
            patch("kubernetes.client.AppsV1Api") as apps,
            patch("kubernetes.client.CustomObjectsApi") as custom,
            patch("lakebench.deploy.destroy.logger"),
        ):
            self.apps = apps
            self.custom = custom
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
        # Finding 3 (race review): the delete is issued before the lease is
        # released, so a same-name deploy's add sees it Terminating.
        assert calls == ["unwatch:ns-a", "delete", "release"]
        engine.k8s.delete_namespace.assert_called_once_with("ns-a", uid="uid-1")
        ns = [r for r in results if r.component == "namespace"]
        assert ns[-1].status is DeploymentStatus.SKIPPED
        assert "still terminating" in ns[-1].message
        final = [r for r in reports if r[0] == "namespace"][-1]
        assert final[1] is DeploymentStatus.SKIPPED

    @pytest.mark.parametrize("uid_at_start", ["uid-1", ""])
    def test_newer_incarnation_is_neither_unwatched_nor_deleted(self, uid_at_start):
        """A concurrent destroy finished and a redeploy re-created the name
        (or, S-P3, a deploy created it after this destroy found it absent)."""
        cluster = FakeCluster(drain_polls=1)
        results, _, calls, engine = self._run_destroy(cluster, uids=[uid_at_start, "uid-2"])
        assert calls == [], "the new deployment's namespace must stay watched"
        engine.k8s.delete_namespace.assert_not_called()
        apps = self.apps.return_value
        assert not apps.delete_namespaced_stateful_set.called, "new postgres must survive"
        assert not apps.delete_namespaced_deployment.called, "new components must survive"
        ns = [r for r in results if r.component == "namespace"]
        assert "newer deployment" in ns[-1].message

    def test_recreated_during_infra_teardown_is_not_deleted(self):
        """The guard at the namespace step still catches a late redeploy."""
        cluster = FakeCluster(drain_polls=1)
        results, _, calls, engine = self._run_destroy(cluster, uids=["uid-1", "uid-1", "uid-2"])
        assert calls == []
        engine.k8s.delete_namespace.assert_not_called()

    def test_redeploy_during_infra_teardown_keeps_its_secretclass(self):
        """Race review finding 3: A2 finished and R redeployed while A1 was
        still tearing down; A1 must not delete R's cluster-scoped SecretClass."""
        cluster = FakeCluster(drain_polls=1)
        # start, four pre-bucket guards, post-bucket, query engine, catalog,
        # postgres; R lands before RBAC.
        results, _, calls, engine = self._run_destroy(cluster, uids=["uid-1"] * 9 + ["uid-2"])
        assert not self.custom.return_value.delete_cluster_custom_object.called
        assert calls == []
        ns = [r for r in results if r.component == "namespace"][-1]
        assert "stopped before RBAC" in ns.message
        assert ns.status is DeploymentStatus.FAILED, "a stopped destroy is not completed"

    def test_redeploy_while_waiting_for_the_lease_keeps_its_watch_entry(self):
        """R re-created the name between the pre-check and the lease: the
        in-lease check refuses, so R's watch entry is never removed."""
        cluster = FakeCluster(drain_polls=1)
        state = {"uid": "uid-1"}

        def redeploy(_engine):
            state["uid"] = "uid-2"

        results, _, calls, engine = self._run_destroy(
            cluster, uid_fn=lambda _ns: state["uid"], on_lease=redeploy
        )
        assert "unwatch:ns-a" not in calls, "the redeploy's watch entry must not be removed"
        engine.k8s.delete_namespace.assert_not_called()
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.FAILED
        assert "NOT completed" in ns.message
        assert "left alone" in ns.message

    def test_unreadable_uid_inside_the_lease_changes_nothing(self):
        cluster = FakeCluster(drain_polls=1)
        state = {"fail": False}

        def uid(_ns):
            if state["fail"]:
                raise K8sResourceError("apiserver 503")
            return "uid-1"

        results, _, calls, engine = self._run_destroy(
            cluster, uid_fn=uid, on_lease=lambda _e: state.update(fail=True)
        )
        assert "unwatch:ns-a" not in calls
        engine.k8s.delete_namespace.assert_not_called()
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.FAILED
        assert "watch-list entry NOT" in ns.message

    def test_in_lease_delete_refused_by_uid_precondition_is_left_alone(self):
        cluster = FakeCluster(drain_polls=1)

        def replaced(ns, uid=None):
            raise NamespaceReplacedError("uid precondition failed")

        cluster.delete_namespace = replaced
        results, _, calls, _ = self._run_destroy(cluster)
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.FAILED
        assert "NOT completed" in ns.message
        assert "left alone" in ns.message
        assert calls.count("delete") == 1

    def test_in_lease_delete_error_keeps_the_namespace(self):
        """Fix review: deleting outside the lease after an in-lease error would
        let a same-name deploy re-add the entry first. Fail closed instead."""
        cluster = FakeCluster(drain_polls=1)

        def flaky_delete(ns, uid=None):
            raise K8sResourceError("apiserver 500")

        cluster.delete_namespace = flaky_delete
        results, _, calls, engine = self._run_destroy(cluster)
        assert calls == ["unwatch:ns-a", "delete", "delete", "delete", "release"]
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.FAILED
        assert "re-run destroy" in ns.message

    def test_lost_reply_then_terminating_is_not_reported_as_kept(self):
        cluster = FakeCluster(drain_polls=1)
        real_delete = cluster.delete_namespace
        n = {"i": 0}

        def lost_reply(ns, uid=None):
            n["i"] += 1
            real_delete(ns, uid=uid)  # accepted by the API server...
            if n["i"] == 1:
                raise K8sResourceError("timeout")  # ...but the reply is lost

        cluster.delete_namespace = lost_reply
        results, _, _, _ = self._run_destroy(cluster)
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.SUCCESS
        assert "NOT deleted" not in ns.message

    def test_redeploy_into_the_same_namespace_during_the_helm_call_is_not_deleted(self):
        """The nonce changes (same UID) after the in-lease check, while helm
        runs: no delete, and the report points at repair-operator."""
        cluster = FakeCluster(drain_polls=1)
        state = {"nonce": "n-1"}

        def redeploy(eng):
            state["nonce"] = "n-2"

        results, _, calls, engine = self._run_destroy(
            cluster,
            nonce_fn=lambda _ns, _k: state["nonce"],
            after_unwatch=redeploy,
        )
        assert "unwatch:ns-a" in calls
        assert "delete" not in calls
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.FAILED
        assert "repair-operator" in ns.message

    def test_redeploy_during_a_backoff_sleep_is_not_deleted(self, monkeypatch):
        """Fourth review: the nonce can change while the retry sleeps; the
        next attempt must re-check before deleting."""
        cluster = FakeCluster(drain_polls=1)
        state = {"nonce": "n-1"}
        monkeypatch.setattr(destroy_mod, "_sleep", lambda _s: state.update(nonce="n-2"))

        def throttled_then_ok(ns, uid=None):
            if state["nonce"] == "n-1":
                raise K8sResourceError("429 Too Many Requests")
            return cluster.delete_namespace_real(ns, uid=uid)

        cluster.delete_namespace_real = cluster.delete_namespace
        cluster.delete_namespace = throttled_then_ok
        results, _, calls, _ = self._run_destroy(cluster, nonce_fn=lambda _ns, _k: state["nonce"])
        assert calls.count("delete") == 1, "no attempt after the redeploy"
        assert cluster.deletes == 0
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.FAILED

    def test_in_lease_retries_back_off(self, monkeypatch):
        """429s under parallel UAT: retry after 2 s, then 5 s, still in the lease."""
        cluster = FakeCluster(drain_polls=1)
        slept: list[float] = []
        monkeypatch.setattr(destroy_mod, "_sleep", lambda sec: slept.append(sec))

        def throttled(ns, uid=None):
            raise K8sResourceError("429 Too Many Requests")

        cluster.delete_namespace = throttled
        self._run_destroy(cluster)
        assert slept[:2] == [2.0, 5.0]

    def test_lost_reply_then_409_is_attributed_to_this_run(self):
        cluster = FakeCluster(drain_polls=1)
        real_delete = cluster.delete_namespace
        n = {"i": 0}

        def lost_then_409(ns, uid=None):
            n["i"] += 1
            if n["i"] == 1:
                real_delete(ns, uid=uid)
                raise K8sResourceError("timeout")
            raise NamespaceTerminatingError("already being deleted")

        cluster.delete_namespace = lost_then_409
        results, _, _, _ = self._run_destroy(cluster)
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.SUCCESS
        assert "another run" not in ns.message

    def test_every_reply_lost_but_terminating_is_not_reported_kept(self):
        cluster = FakeCluster(drain_polls=1)
        real_delete = cluster.delete_namespace

        def all_lost(ns, uid=None):
            if cluster.phase == "Active":
                real_delete(ns, uid=uid)
            raise K8sResourceError("timeout")

        cluster.delete_namespace = all_lost
        results, _, _, _ = self._run_destroy(cluster)
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.SUCCESS
        assert "NOT deleted" not in ns.message

    def test_in_lease_delete_finding_terminating_waits(self):
        cluster = FakeCluster(phase="Terminating", drain_polls=1)
        results, _, calls, _ = self._run_destroy(cluster)
        assert calls == ["unwatch:ns-a", "delete", "release"]
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.SUCCESS
        assert "another run" in ns.message

    def test_gone_mid_teardown_still_drops_the_watch_entry(self):
        """Namespace deleted by hand during infra teardown: skip the rest of
        the by-name teardown but still un-watch (else the operator
        crash-loops), and never report it as this run's delete."""
        cluster = FakeCluster(drain_polls=1)
        n = {"i": 0}

        def uid(_ns):
            n["i"] += 1
            if n["i"] >= 3:
                cluster.phase = ""
                return ""
            return "uid-1"

        results, _, calls, engine = self._run_destroy(cluster, uid_fn=uid)
        assert "unwatch:ns-a" in calls
        # The UID-guarded delete on a gone namespace is a harmless 404.
        assert cluster.deletes == 0
        apps = self.apps.return_value
        assert not apps.delete_namespaced_stateful_set.called
        ns = [r for r in results if r.component == "namespace"][-1]
        assert ns.status is DeploymentStatus.SUCCESS
        assert "already gone" in ns.message
        sc = [r for r in results if r.component == "secretclass"]
        assert sc and sc[0].status is DeploymentStatus.SKIPPED
        assert "lakebench-s3-credentials-ns-a" in sc[0].message

    def test_absent_at_start_is_never_deleted(self):
        """Start UID empty: a namespace that appears later is a new deploy."""
        cluster = FakeCluster(drain_polls=1)
        state = {"uid": ""}

        results, _, calls, engine = self._run_destroy(
            cluster,
            uid_fn=lambda _ns: state["uid"],
            on_lease=lambda _e: None,
        )
        engine.k8s.delete_namespace.assert_not_called()

    def test_delete_carries_the_start_uid_as_precondition(self):
        cluster = FakeCluster(drain_polls=1)
        _, _, _, engine = self._run_destroy(cluster)
        engine.k8s.delete_namespace.assert_called_once_with("ns-a", uid="uid-1")

    def test_unreadable_uid_at_start_refuses_before_touching_anything(self):
        cluster = FakeCluster(drain_polls=1)

        def boom(_ns):
            raise K8sResourceError("apiserver 503")

        results, _, calls, engine = self._run_destroy(cluster, uids=None, uid_fn=boom)
        assert results[-1].component == "ownership-check"
        assert results[-1].status is DeploymentStatus.FAILED
        assert calls == []
        engine.k8s.delete_namespace.assert_not_called()

    def test_unreadable_uid_at_namespace_step_keeps_namespace(self):
        cluster = FakeCluster(drain_polls=1)
        n = {"i": 0}

        def flaky(_ns):
            n["i"] += 1
            if n["i"] == 3:
                raise K8sResourceError("apiserver 503")
            return "uid-1"

        results, _, calls, engine = self._run_destroy(cluster, uid_fn=flaky)
        assert calls == []
        engine.k8s.delete_namespace.assert_not_called()
        assert results[-1].status is DeploymentStatus.FAILED

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

    def test_uid_precondition_is_sent_and_mismatch_raises_replaced(self):
        from lakebench.k8s.client import NamespaceReplacedError

        core = MagicMock()
        core.delete_namespace.side_effect = ApiException(status=409, reason="Conflict")
        core.delete_namespace.side_effect.body = (
            '{"message":"Precondition failed: UID in precondition: uid-1, '
            'UID in object meta: uid-2"}'
        )
        with pytest.raises(NamespaceReplacedError):
            self._client(core).delete_namespace("ns-a", uid="uid-1")
        body = core.delete_namespace.call_args.kwargs["body"]
        assert body.preconditions.uid == "uid-1"

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


class TestCliExitCode:
    """A namespace still Terminating at the deadline is not "Destroy Complete"."""

    def _invoke(self, results, monkeypatch, tmp_path):
        from pathlib import Path

        from typer.testing import CliRunner

        from lakebench.cli import app

        monkeypatch.chdir(tmp_path)
        fixture = Path(__file__).parent / "fixtures" / "v14user.yaml"
        engine = MagicMock()
        engine.destroy_all.return_value = results
        with patch("lakebench.deploy.DeploymentEngine", return_value=engine):
            return CliRunner().invoke(app, ["destroy", str(fixture), "--force"])

    def test_still_terminating_exits_3(self, monkeypatch, tmp_path):
        from lakebench.cli._destroy import EXIT_NAMESPACE_STILL_TERMINATING
        from lakebench.deploy.engine import DeploymentResult

        results = [
            DeploymentResult("postgres", DeploymentStatus.SUCCESS, "removed"),
            DeploymentResult(
                "namespace",
                DeploymentStatus.SKIPPED,
                "Namespace x is still terminating after 600s",
                details={"still_terminating": True},
            ),
        ]
        out = self._invoke(results, monkeypatch, tmp_path)
        assert EXIT_NAMESPACE_STILL_TERMINATING == 3
        assert out.exit_code == 3, out.output
        assert "Destroy Complete\n" not in out.output

    def test_clean_destroy_exits_0(self, monkeypatch, tmp_path):
        from lakebench.deploy.engine import DeploymentResult

        results = [DeploymentResult("namespace", DeploymentStatus.SUCCESS, "Namespace x deleted")]
        out = self._invoke(results, monkeypatch, tmp_path)
        assert out.exit_code == 0, out.output
