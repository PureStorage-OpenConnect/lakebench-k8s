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


def _stale_pod(rec: K8sRecorder, name: str, args: list[str], *, phase: str = "Running") -> None:
    """An operator pod with old args: terminating after a restart, say."""
    rec.add(
        "pods",
        {
            "metadata": {
                "name": name,
                "labels": {"app.kubernetes.io/name": "spark-operator"},
                "deletionTimestamp": "2026-10-01T00:00:00Z",
            },
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
            assert "still watch it; re-run destroy after they roll" in ns.message
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
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.metadata.name = "p"
        pod.spec.containers = [MagicMock(command=None, args=[f"--namespaces={NS}"])]
        core.list_namespaced_pod.return_value.items = [pod]
        with (
            patch("lakebench.deploy.cluster_lock.lease_clamp", lambda t: min(t, 10.0)),
            patch.object(destroy, "_monotonic", clock.monotonic),
            patch.object(destroy, "_sleep", clock.sleep),
        ):
            assert destroy._await_operator_unwatch(core, OP_NS, NS) == ["p"]
        assert sum(clock.sleeps) == pytest.approx(10.0)


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
