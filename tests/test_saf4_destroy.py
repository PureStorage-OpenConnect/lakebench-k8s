"""SAF-4 on destroy (SD-11, DESIGN ch01 3.2 and 3.3) and the lease holder id (LB-178).

- The legacy SecretClass cleanup runs inside the cluster lease, and skips
  (keeping them) when the lease stays held.
- Inside the lease, before the namespace delete, destroy waits for every
  Spark Operator pod that still lists the namespace in ``--namespaces=`` to
  go, and keeps the namespace (exit 1) when one is still there: the operator
  crash-loops on a watched namespace that does not exist, which stops
  SparkApplication reconciliation for every deployment on the cluster.
- ``build_holder_id`` is unique to each acquire, release matches the write
  nonce, and an interrupt inside the acquire releases a lease it wrote.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from unittest.mock import MagicMock, patch

import pytest

from tests.fixtures.recording_k8s import K8sRecorder, recording

NS = "u01"
OP_NS = "spark-operator"
LEGACY = ("lakebench-s3-credentials-class", "lakebench-s3-ca-cert-class")
ALLOW_LEGACY = [f"secretclasses/{n}" for n in LEGACY]


class _Clock:
    """Fake ``destroy._monotonic``/``_sleep``; ``on_sleep`` runs after each sleep."""

    def __init__(self, on_sleep: Callable[[int], None] | None = None) -> None:
        self.t = 1000.0
        self.sleeps: list[float] = []
        self.on_sleep = on_sleep

    def monotonic(self) -> float:
        return self.t

    def sleep(self, s: float) -> None:
        self.sleeps.append(s)
        self.t += s
        if self.on_sleep is not None:
            self.on_sleep(len(self.sleeps))


def _stale_pod(
    rec: K8sRecorder,
    name: str,
    args: list[str],
    *,
    phase: str = "Running",
    terminating: bool = True,
    labels: dict | None = None,
) -> None:
    """An operator pod with old args: terminating after a restart, say."""
    meta: dict = {
        "name": name,
        "labels": {"app.kubernetes.io/name": "spark-operator"} if labels is None else labels,
    }
    if terminating:
        meta["deletionTimestamp"] = "2026-10-01T00:00:00Z"
    rec.add(
        "pods",
        {
            "metadata": meta,
            "spec": {"containers": [{"name": "controller", "args": args}]},
            "status": {"phase": phase},
        },
        namespace=OP_NS,
    )


def _destroy(
    rec: K8sRecorder,
    *,
    legacy: bool = False,
    clock: _Clock | None = None,
    seed: Callable[[K8sRecorder], None] | None = None,
) -> list:
    from lakebench.deploy.destroy import destroy_all
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict
    from lakebench.k8s.client import K8sClient
    from tests.conftest import make_config

    cfg = make_config(name=NS)
    rec.for_config(cfg)
    rec.add_namespace(NS, annotations={"lakebench.deployment/name": NS})
    rec.add_spark_operator(watched=[NS, "u02"])
    rec.add_namespace("u02", annotations={"lakebench.deployment/name": "u02"})
    rec.add_stackable()
    for name in (f"lakebench-s3-credentials-{NS}", f"lakebench-s3-ca-cert-{NS}"):
        rec.add("secretclasses", {"metadata": {"name": name}})
    if legacy:
        for name in LEGACY:
            rec.add("secretclasses", {"metadata": {"name": name}})
    if seed is not None:
        seed(rec)
    engine = MagicMock()
    engine.config = cfg
    engine.k8s = K8sClient(namespace=NS)
    match = IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name=NS,
        expected_deployment=NS,
        found_deployment=NS,
    )
    clock = clock or _Clock()
    with (
        patch("lakebench.deploy.ownership.verify_namespace_identity", return_value=match),
        patch("lakebench.deploy.ownership.verify_bucket_ownership", return_value=match),
        patch("lakebench.deploy.destroy._monotonic", clock.monotonic),
        patch("lakebench.deploy.destroy._sleep", clock.sleep),
    ):
        return destroy_all(engine, clean_buckets=False)


def _ns_result(results: list):
    return next(r for r in results if r.component == "namespace")


def _namespace_deletes(rec: K8sRecorder) -> list:
    return [
        c for c in rec.mutations() if c.verb == "delete" and c.kind == "namespaces" and c.name == NS
    ]


# ---------------------------------------------------------------------------
# 3.3: the operator pod check inside the lease
# ---------------------------------------------------------------------------


class TestOperatorPodCheck:
    def test_destroy_refuses_while_pod_lists_namespace(self):
        """Named case (DESIGN ch01 3.3). Fails reverted: the delete is issued."""
        clock = _Clock()
        with recording() as rec:
            results = _destroy(
                rec,
                clock=clock,
                seed=lambda r: _stale_pod(
                    r, "spark-operator-controller-old", ["controller", "--namespaces=u02,u01"]
                ),
            )
            assert _namespace_deletes(rec) == []
            ns = _ns_result(results)
            assert ns.status.value == "failed"  # the CLI exits 1 on any FAILED result
            assert "spark-operator-controller-old" in ns.message
            assert "still watch it; they did not roll after a restart" in ns.message
            assert ("namespaces", None, NS) in rec.store
            # It polled inside the lease, every 3 s, for the 120 s budget.
            pod_lists = rec.assert_recorded(verb="list", kind="pods", namespace=OP_NS)
            assert all(c.lease_held for c in pod_lists)
            assert len(pod_lists) > 30
            assert sum(clock.sleeps) == pytest.approx(120.0)
            assert set(clock.sleeps) == {3.0}
            rec.assert_clean()

    def test_waits_for_a_terminating_pod_then_deletes(self):
        def roll(n: int) -> None:
            if n == 3:
                rec.store.pop(("pods", OP_NS, "spark-operator-controller-old"))

        clock = _Clock(on_sleep=roll)
        with recording() as rec:
            results = _destroy(
                rec,
                clock=clock,
                seed=lambda r: _stale_pod(
                    r, "spark-operator-controller-old", ["controller", "--namespaces", NS]
                ),
            )
            assert _ns_result(results).status.value == "success"
            (delete,) = _namespace_deletes(rec)
            assert delete.lease_held
            # The category1 step runs with create_namespace true too (one path).
            assert {r.component: r.status.value for r in results}["category1"] == "success"
            assert clock.sleeps[:3] == [3.0, 3.0, 3.0]
            rec.assert_clean()

    @pytest.mark.parametrize(
        ("args", "phase"),
        [
            (["controller", f"--namespaces={NS}"], "Failed"),  # evicted, runs nothing
            (["controller", f"--namespaces={NS}"], "Succeeded"),
            (["controller", "--namespaces="], "Running"),  # watch-all, not a listing
            (["controller", f"--namespaces={NS}-x,x-{NS}"], "Running"),  # look-alikes
        ],
        ids=["evicted", "succeeded", "watch-all", "look-alike"],
    )
    def test_pods_that_do_not_watch_it_do_not_block(self, args, phase):
        clock = _Clock()
        with recording() as rec:
            results = _destroy(
                rec, clock=clock, seed=lambda r: _stale_pod(r, "other", args, phase=phase)
            )
            assert _ns_result(results).status.value == "success"
            assert len(_namespace_deletes(rec)) == 1
            assert clock.sleeps == [] or set(clock.sleeps) == {0.0}

    def test_pod_list_error_keeps_namespace(self):
        with recording() as rec:
            rec.fail(verb="list", kind="pods", namespace=OP_NS, status=500)
            results = _destroy(rec)
            assert _namespace_deletes(rec) == []
            ns = _ns_result(results)
            assert ns.status.value == "failed"
            assert "could not list the Spark Operator pods" in ns.message
            assert ("namespaces", None, NS) in rec.store

    def test_pod_list_carries_a_request_timeout(self):
        from lakebench.deploy.cluster_lock import LEASE_REQUEST_TIMEOUT

        with recording() as rec:
            _destroy(rec)
            calls = rec.assert_recorded(verb="list", kind="pods", namespace=OP_NS)
            assert all(c.kwargs.get("_request_timeout") == LEASE_REQUEST_TIMEOUT for c in calls)

    def test_wait_is_clamped_to_the_hold_budget(self):
        """With 10 s of hold budget left the poll stops at 10 s, not 120 s."""
        from lakebench.deploy import destroy

        clock = _Clock()
        core = MagicMock()
        apps = MagicMock()
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.metadata.name = "p"
        pod.metadata.deletion_timestamp = "2026-10-01T00:00:00Z"
        pod.spec.containers = [MagicMock(command=None, args=[f"--namespaces={NS}"])]
        core.list_namespaced_pod.return_value.items = [pod]
        apps.list_namespaced_deployment.return_value.items = []
        with (
            patch("lakebench.deploy.cluster_lock.lease_clamp", lambda t: min(t, 10.0)),
            patch.object(destroy, "_monotonic", clock.monotonic),
            patch.object(destroy, "_sleep", clock.sleep),
        ):
            assert destroy._await_operator_unwatch(core, apps, OP_NS, NS) == ["p"]
        assert sum(clock.sleeps) == pytest.approx(10.0)

    def test_a_pod_without_the_chart_label_still_blocks(self):
        """The pods are listed without a label selector (a nameOverride changes it)."""
        with recording() as rec:
            results = _destroy(
                rec,
                seed=lambda r: _stale_pod(
                    r, "renamed-controller", [f"--namespaces={NS}"], labels={"app": "x"}
                ),
            )
            assert _namespace_deletes(rec) == []
            assert "renamed-controller" in _ns_result(results).message

    def test_a_live_stale_pod_gets_one_restart(self):
        """A stale pod nobody is replacing (a restart that failed, or a destroy re-run
        after the entry left the helm values): one restart, inside the lease."""

        def roll(n: int) -> None:
            rec.store.pop(("pods", OP_NS, "spark-operator-controller-old"), None)

        clock = _Clock(on_sleep=roll)
        with recording() as rec:
            results = _destroy(
                rec,
                clock=clock,
                seed=lambda r: _stale_pod(
                    r,
                    "spark-operator-controller-old",
                    [f"--namespaces={NS}"],
                    terminating=False,
                ),
            )
            restarts = [c for c in rec.mutations() if c.verb == "rollout restart"]
            # The watch-list removal restarts both Deployments, the SAF-4 check once more.
            assert len(restarts) == 4 and all(c.lease_held for c in restarts)
            assert _ns_result(results).status.value == "success"
            assert len(_namespace_deletes(rec)) == 1
            rec.assert_clean()

    def test_a_deployment_template_still_listing_it_fails_at_once(self):
        """New pods would watch it again: no wait, no restart, repair-operator named."""
        clock = _Clock()

        def drift(r: K8sRecorder) -> None:
            # A Deployment the watch-list upgrade does not rewrite (the fake's
            # helm upgrade rewrites the chart's two).
            r.add(
                "deployments",
                {
                    "metadata": {"name": "spark-operator-controller-2"},
                    "spec": {
                        "selector": {"matchLabels": {"a": "b"}},
                        "template": {
                            "metadata": {"labels": {"a": "b"}},
                            "spec": {
                                "containers": [
                                    {"name": "c", "args": ["controller", f"--namespaces={NS}"]}
                                ]
                            },
                        },
                    },
                },
                namespace=OP_NS,
            )

        with recording() as rec:
            results = _destroy(rec, clock=clock, seed=drift)
            assert _namespace_deletes(rec) == []
            ns = _ns_result(results)
            assert "deployment/spark-operator-controller-2" in ns.message
            assert "repair-operator" in ns.message
            assert clock.sleeps == [] or set(clock.sleeps) == {0.0}

    def test_pod_list_404_keeps_namespace(self):
        """Any read error fails closed (DESIGN ch01 3.3), a 404 included."""
        with recording() as rec:
            rec.fail(verb="list", kind="pods", namespace=OP_NS, status=404)
            results = _destroy(rec)
            assert _namespace_deletes(rec) == []
            assert "could not list the Spark Operator pods" in _ns_result(results).message


# ---------------------------------------------------------------------------
# 3.2: the legacy SecretClass cleanup under the lease
# ---------------------------------------------------------------------------


class TestLegacyCleanupLeased:
    def test_deletes_run_inside_the_lease(self):
        with recording(allow_delete=ALLOW_LEGACY) as rec:
            # Only this deployment: drop u02 from the seed after it is added.
            def only_us(r: K8sRecorder) -> None:
                r.store.pop(("namespaces", None, "u02"))

            _destroy(rec, legacy=True, seed=only_us)
            legacy = [c for c in rec.mutations() if c.name in LEGACY]
            assert len(legacy) == 2 and all(c.lease_held for c in legacy)
            # The refcount read, the last namespace list before the deletes,
            # ran inside that lease too.
            first = rec.calls.index(legacy[0])
            lists = [c for c in rec.calls[:first] if c.verb == "list" and c.kind == "namespaces"]
            assert lists and lists[-1].lease_held
            rec.assert_clean()

    def test_skipped_when_the_lease_stays_held(self, caplog):
        def only_us(r: K8sRecorder) -> None:
            r.store.pop(("namespaces", None, "u02"))

        # The held lease also stops the namespace step's watch-list removal
        # (it keeps the namespace); neither waits in this test.
        with (
            recording() as rec,
            patch("lakebench.deploy.destroy._LEGACY_CLEANUP_LOCK_TIMEOUT_S", 0),
            patch(
                "lakebench.modules.pipeline_engines.spark.operator._WATCH_LIST_LOCK_TIMEOUT_S", 0
            ),
        ):
            rec.seed_lease()
            with caplog.at_level(logging.WARNING, logger="lakebench.deploy.destroy"):
                results = _destroy(rec, legacy=True, seed=only_us)
            assert [c for c in rec.mutations() if c.name in LEGACY] == []
            assert {r.component: r.status.value for r in results}["rbac"] == "success"
            assert "Legacy SecretClass cleanup skipped: the cluster lease is held" in caplog.text
            assert ("secretclasses", None, LEGACY[0]) in rec.store

    def test_presence_check_fails_toward_the_leased_check(self):
        from kubernetes.client.rest import ApiException

        from lakebench.deploy.destroy import _legacy_secretclasses_present

        api = MagicMock()
        api.get_cluster_custom_object.side_effect = ApiException(status=404)
        assert _legacy_secretclasses_present(api) is False
        api.get_cluster_custom_object.side_effect = ApiException(status=403)
        assert _legacy_secretclasses_present(api) is True
        api.get_cluster_custom_object.side_effect = OSError("connection reset")
        assert _legacy_secretclasses_present(api) is True

    def test_a_transport_error_taking_the_lease_is_only_a_skip(self):
        from lakebench.deploy.destroy import _legacy_secretclass_cleanup

        api = MagicMock()
        core = MagicMock()
        core.read_namespace.side_effect = OSError("connection reset")
        assert _legacy_secretclass_cleanup(core, api, NS) == []
        api.delete_cluster_custom_object.assert_not_called()

    def test_no_lease_when_no_legacy_secretclass_exists(self):
        with recording() as rec:
            _destroy(rec)
            creates = [
                c
                for c in rec.calls
                if c.verb == "create"
                and c.kind == "configmaps"
                and c.name == "lakebench-cluster-lock"
            ]
            # One lease: the namespace step's watch-list removal.
            assert len(creates) == 1
            rec.assert_clean()


# ---------------------------------------------------------------------------
# LB-178: holder id, release by nonce, interrupted acquire
# ---------------------------------------------------------------------------


def _core():
    from kubernetes import client

    return client.CoreV1Api()


class TestHolderId:
    def test_names_the_process_and_is_unique_per_acquire(self):
        import os
        import re

        from lakebench.deploy.cluster_lock import build_holder_id

        a, b = build_holder_id(), build_holder_id()
        assert a != b
        assert re.fullmatch(rf"[^@]+@[^@]+@[^@#]+#{os.getpid()}-[0-9a-f]{{8}}", a), a

    def test_release_leaves_a_same_holder_lease_with_another_nonce(self):
        """Admin force-release, then a same-holder same-second acquire: ours must not go."""
        from lakebench.deploy.cluster_lock import (
            LOCK_CONFIGMAP_NAME,
            LOCK_NAMESPACE,
            acquire_cluster_lock,
            release_cluster_lock,
        )

        with recording(NS) as rec:
            core = _core()
            handle = acquire_cluster_lock(core, timeout=0, holder="h@u@s#1-aaaaaaaa")
            key = ("configmaps", LOCK_NAMESPACE, LOCK_CONFIGMAP_NAME)
            rec.store[key].data["write-nonce"] = "someone-else"
            release_cluster_lock(core, handle)
            assert key in rec.store
            rec.store[key].data["write-nonce"] = handle.write_nonce
            release_cluster_lock(core, handle)
            assert key not in rec.store

    def test_interrupt_inside_acquire_releases_the_written_lease(self):
        from lakebench.deploy import cluster_lock as cl

        real = cl._try_acquire_once

        def interrupted(*a, **kw):
            real(*a, **kw)  # the write lands
            raise KeyboardInterrupt  # before the handle reaches cluster_lock

        with recording(NS) as rec, patch.object(cl, "_try_acquire_once", interrupted):
            with pytest.raises(KeyboardInterrupt), cl.cluster_lock(_core(), timeout=0):
                pytest.fail("body must not run")
            assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) not in rec.store
            rec.assert_recorded(verb="delete", kind="configmaps", name=cl.LOCK_CONFIGMAP_NAME)

    def test_interrupt_while_waiting_leaves_the_other_holders_lease(self):
        from lakebench.deploy import cluster_lock as cl

        def interrupted(*a, **kw):
            raise KeyboardInterrupt

        with recording(NS) as rec:
            rec.seed_lease()
            with (
                patch.object(cl, "_try_acquire_once", interrupted),
                pytest.raises(KeyboardInterrupt),
            ):
                with cl.cluster_lock(_core(), timeout=0):
                    pytest.fail("body must not run")
            assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) in rec.store
            assert not [c for c in rec.mutations() if c.name == cl.LOCK_CONFIGMAP_NAME]


# ---------------------------------------------------------------------------
# Review round: a lease left to its 3600 s TTL
# ---------------------------------------------------------------------------


class TestNoLeaseLeftToTheTTL:
    def test_a_lost_reply_then_409_adopts_our_own_lease(self):
        """urllib3 re-sends a PUT whose reply timed out; the retry 409s on our own write."""
        from kubernetes.client.rest import ApiException

        from lakebench.deploy import cluster_lock as cl

        mine = "h@u@s#1-aaaaaaaa"
        ours = cl.LeaseState(
            holder=mine,
            acquired_at="2026-10-01T00:00:00+00:00",
            ttl_seconds=3600,
            expires_at_epoch=4e9,
            resource_version="7",
            write_nonce="n1",
        )
        core = MagicMock()
        with patch.object(cl, "read_cluster_lock", side_effect=[None, ours]):
            core.create_namespaced_config_map.side_effect = ApiException(status=409)
            got = cl._try_acquire_once(core, mine, 3600, adopt_own=True)
        assert isinstance(got, cl.LeaseHandle) and got.write_nonce == "n1"
        # A caller-chosen holder is not unique: it is never adopted.
        with patch.object(cl, "read_cluster_lock", side_effect=[None, ours]):
            got = cl._try_acquire_once(core, mine, 3600, adopt_own=False)
        assert isinstance(got, cl.LeaseState)

    def test_a_failed_acquire_releases_a_lease_it_wrote(self):
        """A 504 or transport error after the write committed: released, not left."""
        from lakebench.deploy import cluster_lock as cl

        real = cl._try_acquire_once

        def landed_then_failed(*a, **kw):
            real(*a, **kw)
            raise cl.ClusterLockError("cannot create lease: (504) request did not complete")

        with recording(NS) as rec, patch.object(cl, "_try_acquire_once", landed_then_failed):
            with pytest.raises(cl.ClusterLockError), cl.cluster_lock(_core(), timeout=0):
                pytest.fail("body must not run")
            assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) not in rec.store

    def test_a_signal_as_the_body_ends_still_releases(self):
        """The third interrupt lands at the start of the release: released, then aborted."""
        from lakebench.deploy import cluster_lock as cl

        real_quiet = cl._SignalDeferral.quiet
        calls = []

        def quiet(self):
            calls.append(1)
            if len(calls) == 1:
                raise cl.LeaseAbort(2)
            real_quiet(self)

        with recording(NS) as rec, patch.object(cl._SignalDeferral, "quiet", quiet):
            with pytest.raises(cl.LeaseAbort), cl.cluster_lock(_core(), timeout=0):
                pass
            assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) not in rec.store
            assert not cl.lease_held()

    def test_sigterm_during_the_acquire_releases_instead_of_killing(self):
        import signal

        from lakebench.deploy import cluster_lock as cl

        real = cl._try_acquire_once
        before = signal.getsignal(signal.SIGTERM)

        def landed_then_sigterm(*a, **kw):
            out = real(*a, **kw)
            signal.raise_signal(signal.SIGTERM)  # default would kill pytest
            return out

        with recording(NS) as rec, patch.object(cl, "_try_acquire_once", landed_then_sigterm):
            with pytest.raises(cl.LeaseAbort) as info, cl.cluster_lock(_core(), timeout=0):
                pytest.fail("body must not run")
            assert info.value.signum == signal.SIGTERM
            assert ("configmaps", cl.LOCK_NAMESPACE, cl.LOCK_CONFIGMAP_NAME) not in rec.store
        assert signal.getsignal(signal.SIGTERM) == before

    def test_leave_without_a_token_clears_the_lease_state(self):
        from lakebench.k8s import lease_state

        lease_state._LEASE.set(lease_state.HeldLease("h", 1.0, 0.0))
        lease_state.leave(None)
        assert not lease_state.lease_held()


def test_an_abort_during_the_grace_kills_the_child():
    """Popen.__exit__ would wait for a SIGTERM-ignoring child without a bound."""
    import signal

    from lakebench.deploy.cluster_lock import LeaseAbort
    from lakebench.k8s import _pinned

    sent = []

    class Proc:
        pid = 0

        def send_signal(self, sig):
            sent.append(sig)

        def communicate(self, timeout=None):
            raise LeaseAbort(signal.SIGINT)

    with pytest.raises(LeaseAbort):
        _pinned._stop_gently(Proc())
    assert sent == [signal.SIGTERM, signal.SIGKILL]
