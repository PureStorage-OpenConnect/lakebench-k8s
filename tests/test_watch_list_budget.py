"""SD-12 (DEP-5): watch-list mutations finish inside the lease's hold budget
and fail closed when they cannot.

Covers: the strict remove honours a failed operator restart; a spent budget
stops the remove before destroy's namespace delete; the phase budget sums to
the lease's hold budget and the waiter's timeout covers three holders; an
unreadable watch list fails deploy instead of passing it; and the
single-upgrade ``_set_watch_list_impl`` and release-state helpers repair uses.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy import cluster_lock as cl
from lakebench.k8s import lease_state
from lakebench.k8s.lease_state import LeaseHoldExceeded
from lakebench.modules.pipeline_engines.spark import operator as op
from lakebench.modules.pipeline_engines.spark.operator import (
    OperatorStatus,
    SparkOperatorManager,
    WatchListMutationError,
)


def _mgr(job_namespace: str | None = "my-ns") -> SparkOperatorManager:
    return SparkOperatorManager(
        namespace="spark-operator", version="2.5.1", job_namespace=job_namespace
    )


def _ok(stdout: str = "") -> SimpleNamespace:
    return SimpleNamespace(returncode=0, stdout=stdout, stderr="")


def _fail(stderr: str = "boom") -> SimpleNamespace:
    return SimpleNamespace(returncode=1, stdout="", stderr=stderr)


@contextmanager
def _lease(budget_s: float):
    """A fake cluster_lock that enters the real hold-budget state."""

    @contextmanager
    def lock(*_a, **_kw):
        _held, token = lease_state.enter("test@host@x", budget_s)
        try:
            yield
        finally:
            lease_state.leave(token)

    with (
        patch("kubernetes.client.CoreV1Api"),
        patch("lakebench.deploy.cluster_lock.cluster_lock", side_effect=lock),
    ):
        yield


def _remove_ready(mgr: SparkOperatorManager, run) -> list:
    """Patch the reads of the remove path; return the patchers to enter."""
    return [
        patch.object(mgr, "_get_watched_namespaces", return_value=["my-ns", "other"]),
        patch.object(mgr, "_watch_list_pin", return_value=["--version", "2.5.1"]),
        patch.object(mgr, "_is_openshift", return_value=False),
        patch.object(mgr, "_run", side_effect=run),
    ]


class TestRemoveHonoursRestart:
    def test_remove_honours_restart_failure(self):
        """A restart that fails leaves pods that may still list the namespace:
        the strict remove must fail and destroy's delete must not run.
        Reverted (bare restart call, return True), the delete runs."""
        mgr = _mgr()
        deleted: list[str] = []
        with _lease(750):
            ps = _remove_ready(mgr, lambda *a, **k: _ok())
            for p in ps:
                p.start()
            try:
                with patch.object(mgr, "_restart_operator", return_value=False):
                    with pytest.raises(WatchListMutationError) as ei:
                        mgr.remove_namespace_from_watch(
                            "my-ns", strict=True, then=lambda: deleted.append("my-ns")
                        )
            finally:
                for p in ps:
                    p.stop()
        assert deleted == []
        assert "repair-operator" in str(ei.value)

    def test_openshift_patch_rollout_is_awaited_before_the_restart(self):
        mgr = _mgr()
        calls: list[str] = []
        with (
            patch.object(mgr, "_get_watched_namespaces", return_value=["my-ns", "other"]),
            patch.object(mgr, "_watch_list_pin", return_value=["--version", "2.5.1"]),
            patch.object(mgr, "_is_openshift", return_value=True),
            patch.object(mgr, "_run", return_value=_ok()),
            patch.object(mgr, "_assign_openshift_scc"),
            patch.object(
                mgr, "_patch_openshift_deployments", side_effect=lambda: calls.append("patch")
            ),
            patch.object(
                mgr, "_wait_for_rollout", side_effect=lambda *a, **k: calls.append("wait") or False
            ),
            patch.object(
                mgr, "_restart_operator", side_effect=lambda: calls.append("restart") or True
            ),
        ):
            assert mgr._remove_namespace_from_watch_unlocked("my-ns") is False
        assert calls == ["patch", "wait"], "a failed patch rollout stops before the restart"


class TestHoldBudget:
    def test_hold_over_budget_fails_closed(self):
        """A slow helm upgrade spends the hold budget: the restart's rollout
        wait has no time left, raises, and the strict remove fails without
        running destroy's namespace delete. Reverted (fixed per-wait
        timeouts, restart result ignored), the delete runs past the budget."""
        clock = [1000.0]
        deleted: list[str] = []

        def run(cmd, **_kw):
            if cmd[:2] == ["helm", "upgrade"]:
                clock[0] += 749.5  # the upgrade takes almost the whole hold
            return _ok()

        mgr = _mgr()
        with (
            patch("time.monotonic", side_effect=lambda: clock[0]),
            _lease(750),
            patch.object(mgr, "_get_watched_namespaces", return_value=["my-ns", "other"]),
            patch.object(mgr, "_watch_list_pin", return_value=["--version", "2.5.1"]),
            patch.object(mgr, "_is_openshift", return_value=False),
            patch.object(mgr, "_run", side_effect=run),
        ):
            with pytest.raises(WatchListMutationError) as ei:
                mgr.remove_namespace_from_watch(
                    "my-ns", strict=True, then=lambda: deleted.append("my-ns")
                )
        assert deleted == []
        assert "namespace was NOT deleted" in str(ei.value)

    def test_lease_budget_fits_waiter_timeout(self):
        """The design's phase table sums to the hold budget, the table's pod
        poll and recovery rows are the constants the code uses, and a waiter
        outlasts three holders at that budget (the four-deployment limit).
        The table is the plan; the hold budget is what enforces it."""
        from lakebench.deploy import destroy
        from lakebench.k8s import _pinned

        assert sum(op.WATCH_PHASES_S) <= cl.LEASE_MAX_HOLD_S
        assert destroy._OPERATOR_POD_WAIT_S == op.WATCH_POD_POLL_S
        assert _pinned.HELM_RECOVERY_RESERVE_S == op.WATCH_RECOVERY_S
        assert op._WATCH_LIST_LOCK_TIMEOUT_S == 3 * cl.LEASE_MAX_HOLD_S
        assert cl.ADMIN_MAX_HOLD_S <= op._WATCH_LIST_LOCK_TIMEOUT_S
        assert cl.ADMIN_MAX_HOLD_S < cl.DEFAULT_TTL_SEC

    def test_phase_shares_one_deadline(self):
        clock = [0.0]
        with patch("time.monotonic", side_effect=lambda: clock[0]):
            phase = op._Phase(180)
            assert phase.wait_s("first") == 180
            clock[0] = 150
            assert phase.wait_s("second") == 30
            clock[0] = 179.5
            with pytest.raises(LeaseHoldExceeded):
                phase.wait_s("third")

    def test_phase_is_clamped_by_the_lease(self):
        clock = [0.0]
        with patch("time.monotonic", side_effect=lambda: clock[0]):
            _held, token = lease_state.enter("t", 40)
            try:
                phase = op._Phase(180)
                assert phase.wait_s("w") == 40
                with pytest.raises(LeaseHoldExceeded):
                    phase.helm_attempt_s("helm")  # under the 60 s floor: not started
            finally:
                lease_state.leave(token)

    def test_helm_attempt_is_capped_under_the_lease(self):
        clock = [0.0]
        with patch("time.monotonic", side_effect=lambda: clock[0]):
            _held, token = lease_state.enter("t", 750)
            try:
                assert op._Phase(180).helm_attempt_s("helm") == op._HELM_ATTEMPT_MAX_S
            finally:
                lease_state.leave(token)

    def test_helm_is_not_killed_outside_the_lease(self):
        """Outside the lease _pinned gives helm no SIGTERM-first stop and no
        own --timeout, so a subprocess timeout would SIGKILL it and leave
        the release pending for every deployment."""
        assert op._Phase(180).helm_attempt_s("helm") is None

    def test_rollout_wait_shares_one_budget(self):
        clock = [0.0]
        timeouts: list[str] = []

        def run(cmd, **_kw):
            timeouts.append(cmd[-1])
            clock[0] += 150
            return _ok()

        mgr = _mgr()
        with (
            patch("time.monotonic", side_effect=lambda: clock[0]),
            patch.object(mgr, "_run", side_effect=run),
        ):
            assert mgr._wait_for_rollout() is True
        assert timeouts == [f"--timeout={op.WATCH_ROLLOUT_PHASE_S}s", "--timeout=30s"]

    def test_rollout_waits_share_the_restart_budget(self):
        """The second Deployment's rollout gets what the first left."""
        clock = [0.0]
        timeouts: list[str] = []

        def run(cmd, **_kw):
            if cmd[:3] == ["kubectl", "rollout", "status"]:
                timeouts.append(cmd[-1])
                clock[0] += 100
            return _ok()

        mgr = _mgr()
        with (
            patch("time.monotonic", side_effect=lambda: clock[0]),
            patch.object(mgr, "_run", side_effect=run),
        ):
            assert mgr._restart_operator() is True
        assert timeouts == [f"--timeout={op.WATCH_RESTART_PHASE_S}s", "--timeout=80s"]


class TestUnknownWatchState:
    def _status(self, watching):
        return OperatorStatus(
            installed=True,
            version="2.5.1",
            namespace="spark-operator",
            ready=True,
            message="ok",
            watching_namespace=watching,
        )

    def test_unknown_watch_state_fails_ensure(self):
        """Reverted (a ready status returned as is), deploy passes with a
        namespace the operator may not watch."""
        mgr = _mgr()
        with patch.object(mgr, "check_status", return_value=self._status(None)):
            st = mgr.ensure_namespace_watched(can_heal=True)
        assert st.ready is False
        assert "could not be read" in st.message

    def test_unknown_after_heal_fails(self):
        mgr = _mgr()
        with (
            patch.object(
                mgr, "check_status", side_effect=[self._status(False), self._status(None)]
            ),
            patch.object(mgr, "_add_namespace_to_watch", return_value=True),
        ):
            st = mgr.ensure_namespace_watched(can_heal=True)
        assert st.ready is False

    def test_unknown_watch_state_fails_deploy(self):
        from lakebench.deploy.engine import DeploymentEngine, DeploymentStatus
        from tests.conftest import make_config

        engine = MagicMock()
        engine.config = make_config(name="sd12-t")
        engine.dry_run = False
        with patch.object(SparkOperatorManager, "check_status", return_value=self._status(None)):
            res = DeploymentEngine._deploy_spark_operator(engine)
        assert res.status == DeploymentStatus.FAILED
        assert "could not be read" in res.message
        assert "admin install" not in res.message

    def test_check_status_error_is_unknown_not_absent(self):
        mgr = _mgr()
        with patch.object(mgr, "_run", side_effect=RuntimeError("api down")):
            st = mgr.check_status()
        assert st.installed is None
        assert st.ready is False

    @pytest.mark.parametrize(
        "stderr",
        [
            "Unable to connect to the server: dial tcp 10.0.1.50:6443: i/o timeout",
            'Error from server (Forbidden): customresourcedefinitions "x" is forbidden',
        ],
    )
    def test_unreadable_crd_is_unknown(self, stderr):
        """kubectl returns non-zero (not an exception) when the API is down
        or the read is refused: that is not "not installed"."""
        mgr = _mgr()
        with patch.object(mgr, "_run", return_value=_fail(stderr)):
            st = mgr.check_status()
        assert st.installed is None

    def test_missing_crd_is_not_installed(self):
        mgr = _mgr()
        err = 'Error from server (NotFound): customresourcedefinitions "x" not found'
        with patch.object(mgr, "_run", return_value=_fail(err)):
            st = mgr.check_status()
        assert st.installed is False

    def test_unlistable_deployments_are_unknown(self):
        mgr = _mgr()
        with patch.object(mgr, "_run", side_effect=[_ok("crd"), _fail("Forbidden")]):
            st = mgr.check_status()
        assert st.installed is None


class TestSetWatchList:
    def test_refuses_an_empty_list(self):
        with pytest.raises(ValueError):
            _mgr()._set_watch_list_impl([])
        with pytest.raises(ValueError):
            _mgr()._set_watch_list_impl([""])

    def test_one_upgrade_then_verified(self):
        mgr = _mgr()
        cmds: list[list[str]] = []

        def run(cmd, **_kw):
            cmds.append(cmd)
            return _ok()

        with (
            patch.object(mgr, "_watch_list_pin", return_value=["--version", "2.5.1"]),
            patch.object(mgr, "_is_openshift", return_value=False),
            patch.object(mgr, "_run", side_effect=run),
            patch.object(mgr, "_restart_operator", return_value=True) as restart,
            patch.object(mgr, "_get_active_namespaces", return_value=["ns-b", "ns-a"]) as read,
        ):
            assert mgr._set_watch_list_impl(["ns-b", "ns-a", "ns-a"]) is True
        restart.assert_called_once()
        assert {c.kwargs["deployment"] for c in read.call_args_list} == {
            mgr.CONTROLLER_DEPLOYMENT,
            mgr.WEBHOOK_DEPLOYMENT,
        }
        upgrades = [c for c in cmds if c[:2] == ["helm", "upgrade"]]
        assert len(upgrades) == 1
        assert "spark.jobNamespaces={ns-a,ns-b}" in upgrades[0]
        assert "--reuse-values" in upgrades[0] and "--version" in upgrades[0]

    def test_unpinned_release_is_not_upgraded(self):
        mgr = _mgr()
        with (
            patch.object(mgr, "_watch_list_pin", return_value=None),
            patch.object(mgr, "_run") as run,
        ):
            assert mgr._set_watch_list_impl(["ns-a"]) is False
        run.assert_not_called()

    def _verify(self, controller, webhook, wanted):
        clock = [0.0]

        def tick(_s):
            clock[0] += 1

        def read(operator_ns=None, deployment=None):
            return webhook if deployment == "spark-operator-webhook" else controller

        mgr = _mgr()
        with (
            patch("time.monotonic", side_effect=lambda: clock[0]),
            patch("time.sleep", side_effect=tick),
            patch.object(mgr, "_watch_list_pin", return_value=["--version", "2.5.1"]),
            patch.object(mgr, "_is_openshift", return_value=False),
            patch.object(mgr, "_run", return_value=_ok()),
            patch.object(mgr, "_restart_operator", return_value=True),
            patch.object(mgr, "_get_active_namespaces", side_effect=read),
        ):
            return mgr._set_watch_list_impl(wanted)

    def test_controller_mismatch_fails(self):
        assert self._verify(["ns-a"], ["ns-a", "ns-b"], ["ns-a", "ns-b"]) is False

    def test_a_superset_is_not_the_list(self):
        assert self._verify(["ns-a", "ns-b", "ns-c"], ["ns-a", "ns-b"], ["ns-a", "ns-b"]) is False

    def test_webhook_mismatch_fails(self):
        assert self._verify(["ns-a"], ["ns-a", "ns-gone"], ["ns-a"]) is False

    def test_watch_all_deployment_is_not_the_list(self):
        assert self._verify(["ns-a"], None, ["ns-a"]) is False

    def test_failed_restart_fails(self):
        mgr = _mgr()
        with (
            patch.object(mgr, "_watch_list_pin", return_value=["--version", "2.5.1"]),
            patch.object(mgr, "_is_openshift", return_value=False),
            patch.object(mgr, "_run", return_value=_ok()),
            patch.object(mgr, "_restart_operator", return_value=False),
            patch.object(mgr, "_get_active_namespaces") as read,
        ):
            assert mgr._set_watch_list_impl(["ns-a"]) is False
        read.assert_not_called()

    def test_openshift_patch_is_rolled_out_before_the_restart(self):
        mgr = _mgr()
        calls: list[str] = []
        with (
            patch.object(mgr, "_watch_list_pin", return_value=["--version", "2.5.1"]),
            patch.object(mgr, "_is_openshift", return_value=True),
            patch.object(mgr, "_run", return_value=_ok()),
            patch.object(mgr, "_assign_openshift_scc", side_effect=lambda: calls.append("scc")),
            patch.object(
                mgr, "_patch_openshift_deployments", side_effect=lambda: calls.append("patch")
            ),
            patch.object(
                mgr, "_wait_for_rollout", side_effect=lambda *a, **k: calls.append("wait") or True
            ),
            patch.object(
                mgr, "_restart_operator", side_effect=lambda: calls.append("restart") or True
            ),
            patch.object(mgr, "_get_active_namespaces", return_value=["ns-a"]),
        ):
            assert mgr._set_watch_list_impl(["ns-a"]) is True
        assert calls == ["scc", "patch", "wait", "restart"]


class TestAddAwaitsPatchRollout:
    def test_add_stops_when_the_patch_rollout_fails(self):
        mgr = _mgr()
        calls: list[str] = []
        with (
            patch.object(mgr, "_get_watched_namespaces", return_value=["other"]),
            patch.object(mgr, "_namespace_is_terminating", return_value=False),
            patch.object(mgr, "_watch_list_pin", return_value=["--version", "2.5.1"]),
            patch.object(mgr, "_is_openshift", return_value=True),
            patch.object(mgr, "_run", return_value=_ok()),
            patch.object(mgr, "_assign_openshift_scc"),
            patch.object(
                mgr, "_patch_openshift_deployments", side_effect=lambda: calls.append("patch")
            ),
            patch.object(
                mgr, "_wait_for_rollout", side_effect=lambda *a, **k: calls.append("wait") or False
            ),
            patch.object(
                mgr, "_restart_operator", side_effect=lambda: calls.append("restart") or True
            ),
        ):
            assert mgr._add_namespace_to_watch_impl("my-ns") is False
        assert calls == ["patch", "wait"]


class TestReleaseState:
    def test_status_and_revision(self):
        mgr = _mgr()
        doc = {"info": {"status": "pending-upgrade"}, "version": 7}
        with patch.object(mgr, "_run", return_value=_ok(json.dumps(doc))):
            assert mgr.release_state() == ("pending-upgrade", 7)

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("2026-10-02T04:53:00Z", 1790916780.0),
            ("2026-10-02T06:53:00.5+02:00", 1790916780.5),
            ("2026-10-02T04:53:00.123456789Z", 1790916780.123456),
            ("yesterday", None),
            ("", None),
            (None, None),
        ],
    )
    def test_rfc3339(self, value, expected):
        got = op._rfc3339(value)
        if expected is None:
            assert got is None
        else:
            assert got == pytest.approx(expected, abs=1e-3)

    def test_revision_created_reads_the_release_secret(self):
        mgr = _mgr()
        with patch.object(mgr, "_run", return_value=_ok("2026-10-02T04:53:00Z")) as run:
            assert mgr.revision_created(5) == pytest.approx(1790916780.0)
        cmd = run.call_args.args[0]
        assert "sh.helm.release.v1.spark-operator.v5" in cmd
        assert "jsonpath={.metadata.creationTimestamp}" in cmd
        with patch.object(mgr, "_run", return_value=_fail("NotFound")):
            assert mgr.revision_created(5) is None

    def test_absent(self):
        mgr = _mgr()
        with patch.object(mgr, "_run", return_value=_fail("Error: release: not found")):
            assert mgr.release_state() == ("absent", 0)

    def test_unreadable(self):
        mgr = _mgr()
        with patch.object(mgr, "_run", return_value=_fail("connection refused")):
            assert mgr.release_state() is None
        with patch.object(mgr, "_run", return_value=_ok("not json")):
            assert mgr.release_state() is None

    def test_good_revisions_skip_failed_and_pending(self):
        rows = [
            {"revision": 3, "status": "superseded"},
            {"revision": 4, "status": "deployed"},
            {"revision": 5, "status": "failed"},
            {"revision": 6, "status": "pending-upgrade"},
        ]
        mgr = _mgr()
        with patch.object(mgr, "_run", return_value=_ok(json.dumps(rows))):
            assert mgr.good_revisions(6) == [4, 3]
            assert mgr.good_revisions(4) == [3]
            assert mgr.good_revisions(3) == []

    def test_good_revisions_unreadable(self):
        mgr = _mgr()
        with patch.object(mgr, "_run", return_value=_fail()):
            assert mgr.good_revisions(6) is None

    def test_values_at_a_revision(self):
        mgr = _mgr()
        cmds: list[list[str]] = []

        def run(cmd, **_kw):
            cmds.append(cmd)
            return _ok(json.dumps({"spark": {"jobNamespaces": ["ns-a"]}}))

        with patch.object(mgr, "_run", side_effect=run):
            assert mgr._get_watched_namespaces(revision=4) == ["ns-a"]
        assert "--revision" in cmds[0] and "4" in cmds[0]

    def test_an_empty_entry_means_every_namespace(self):
        """The chart renders --namespaces="" (watch all) for a list with "";
        reading it as the other entries would let repair narrow it."""
        mgr = _mgr()
        doc = {"spark": {"jobNamespaces": ["", "team-a"]}}
        with patch.object(mgr, "_run", return_value=_ok(json.dumps(doc))):
            assert mgr._get_watched_namespaces() is None

    def test_history_with_a_bad_revision_is_unreadable(self):
        mgr = _mgr()
        rows = [{"revision": "x", "status": "deployed"}]
        with patch.object(mgr, "_run", return_value=_ok(json.dumps(rows))):
            assert mgr.good_revisions(6) is None

    def test_rollback_names_the_revision_and_awaits_the_rollout(self):
        mgr = _mgr()
        cmds: list[list[str]] = []

        def run(cmd, **_kw):
            cmds.append(cmd)
            return _ok()

        with (
            patch.object(mgr, "_get_helm_version", return_value="2.5.1"),
            patch.object(mgr, "_run", side_effect=run),
            patch.object(mgr, "_is_openshift", return_value=False),
            patch.object(mgr, "_wait_for_rollout", return_value=False) as wait,
        ):
            assert mgr.rollback_to(4) is False
        wait.assert_called_once()
        assert cmds == [["helm", "rollback", "spark-operator", "4", "-n", "spark-operator"]]

    def test_rollback_on_openshift_patches_before_waiting(self):
        mgr = _mgr()
        calls: list[str] = []
        with (
            patch.object(mgr, "_get_helm_version", return_value="2.5.1"),
            patch.object(mgr, "_run", return_value=_ok()),
            patch.object(mgr, "_is_openshift", return_value=True),
            patch.object(mgr, "_assign_openshift_scc", side_effect=lambda: calls.append("scc")),
            patch.object(
                mgr, "_patch_openshift_deployments", side_effect=lambda: calls.append("patch")
            ),
            patch.object(
                mgr, "_wait_for_rollout", side_effect=lambda *a, **k: calls.append("wait") or True
            ),
        ):
            assert mgr.rollback_to(4) is True
        assert calls == ["scc", "patch", "wait"]

    def test_rollback_failure_is_reported(self):
        mgr = _mgr()
        with (
            patch.object(mgr, "_get_helm_version", return_value="2.5.1"),
            patch.object(mgr, "_run", return_value=_fail()),
            patch.object(mgr, "_wait_for_rollout") as wait,
        ):
            assert mgr.rollback_to(4) is False
        wait.assert_not_called()


class TestValidateReportsUnknown:
    """validate is read-only and never fails on watching (deploy adds the
    namespace), but an unreadable watch list is a warning now that deploy
    and run refuse on it, and an unreadable operator is not "not installed"."""

    def _validate(self, status, tmp_path, monkeypatch):
        import yaml
        from typer.testing import CliRunner

        from lakebench.cli import app

        monkeypatch.setenv("KUBECONFIG", "/nonexistent")
        monkeypatch.chdir(tmp_path)
        cfg = tmp_path / "lakebench.yaml"
        cfg.write_text(
            yaml.safe_dump(
                {
                    "name": "sd12-v",
                    "endpoint": "http://127.0.0.1:9",
                    "access_key": "k",
                    "secret_key": "s",
                    "scale": 1,
                }
            )
        )
        from lakebench.k8s import K8sConnectionError

        with (
            patch(
                "lakebench.cli.get_k8s_client",
                side_effect=K8sConnectionError("no cluster in tests"),
            ),
            patch.object(SparkOperatorManager, "check_status", return_value=status),
        ):
            r = CliRunner().invoke(app, ["validate", str(cfg)], env={"COLUMNS": "300"})
        return " ".join(r.output.split())

    def test_unreadable_watch_list_warns(self, tmp_path, monkeypatch):
        st = OperatorStatus(
            installed=True,
            version="2.5.1",
            namespace="spark-operator",
            ready=True,
            message="ok",
            watching_namespace=None,
        )
        out = self._validate(st, tmp_path, monkeypatch)
        assert "! Namespace watching unverified (watch list unreadable)" in out
        assert "deploy and run refuse" in out

    def test_unknown_install_is_not_reported_absent(self, tmp_path, monkeypatch):
        st = OperatorStatus(
            installed=None,
            version=None,
            namespace=None,
            ready=False,
            message="Error checking operator status: timeout",
        )
        out = self._validate(st, tmp_path, monkeypatch)
        assert "Could not check the Spark Operator" in out
        assert "Not installed" not in out
