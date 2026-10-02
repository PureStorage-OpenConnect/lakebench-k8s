"""Unit tests for the ``lakebench admin`` subcommand tree.

Focus on the ownership discipline these commands enforce: lease-gating
of mutations, idempotent migration, reclaim refusal on non-empty
buckets, and doctor / status read-only reports.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from kubernetes.client.exceptions import ApiException
from typer.testing import CliRunner

from lakebench.cli._admin import (
    _migrate_secretclass,
    admin_app,
)
from lakebench.deploy.cluster_lock import ClusterLockHeld
from lakebench.modules.pipeline_engines.spark.operator_scratch import TmpVolume

runner = CliRunner()


def _api_exc(status: int) -> ApiException:
    return ApiException(status=status, reason="test")


class TestReleaseLock:
    def test_expired_only_refuses_live(self):
        with (
            patch("lakebench.cli._admin._get_core_v1"),
            patch(
                "lakebench.deploy.cluster_lock.force_release_cluster_lock",
                side_effect=ClusterLockHeld(
                    holder="prod@host@abc",
                    acquired_at="2026-09-21T12:00:00+00:00",
                    ttl_seconds=3600,
                    expires_at="2026-09-21T13:00:00+00:00",
                ),
            ),
        ):
            r = runner.invoke(admin_app, ["release-lock"])
        assert r.exit_code == 3  # refused: the lease is live (lease.held)
        assert "prod@host@abc" in r.output

    def test_force_releases_live(self):
        state = MagicMock(holder="prod@host@abc", acquired_at="2026-09-21T12:00:00+00:00")
        with (
            patch("lakebench.cli._admin._get_core_v1"),
            patch(
                "lakebench.deploy.cluster_lock.force_release_cluster_lock",
                return_value=state,
            ) as mock_frc,
        ):
            r = runner.invoke(admin_app, ["release-lock", "--force"])
        assert r.exit_code == 0
        assert "prod@host@abc" in r.output
        # --force flips expired_only OFF at the call site.
        _, kwargs = mock_frc.call_args
        assert kwargs["expired_only"] is False

    def test_no_lease_returns_info(self):
        with (
            patch("lakebench.cli._admin._get_core_v1"),
            patch(
                "lakebench.deploy.cluster_lock.force_release_cluster_lock",
                return_value=None,
            ),
        ):
            r = runner.invoke(admin_app, ["release-lock"])
        assert r.exit_code == 0
        assert "no lease" in r.output.lower()


class TestReleaseLockErrors:
    def test_lock_error_is_a_message_not_a_traceback(self):
        from lakebench.deploy.cluster_lock import ClusterLockError

        with (
            patch("lakebench.cli._admin._get_core_v1"),
            patch(
                "lakebench.deploy.cluster_lock.force_release_cluster_lock",
                side_effect=ClusterLockError("cannot force-delete lease: (403) Forbidden"),
            ),
        ):
            r = runner.invoke(admin_app, ["release-lock"])
        assert r.exit_code == 1
        assert r.exception is None or isinstance(r.exception, SystemExit)
        assert "403" in r.output


class TestMigrateDeployment:
    def _fake_core_and_custom(self, namespace_exists=True, already_migrated=False):
        core = MagicMock()
        custom = MagicMock()
        if namespace_exists:
            ns = MagicMock()
            ns.metadata.annotations = (
                {"lakebench.deployment/name": "old-name"} if already_migrated else {}
            )
            core.read_namespace.return_value = ns
        else:
            core.read_namespace.side_effect = _api_exc(404)
        return core, custom

    def test_refuses_when_namespace_missing(self):
        core, custom = self._fake_core_and_custom(namespace_exists=False)
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch("kubernetes.client.CustomObjectsApi", return_value=custom),
        ):
            r = runner.invoke(admin_app, ["migrate-deployment", "gone"])
        assert r.exit_code == 1  # the namespace is not on the cluster (cluster state, as status)
        assert "does not exist" in r.output

    def test_noop_when_already_migrated(self):
        core, custom = self._fake_core_and_custom(already_migrated=True)
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch("kubernetes.client.CustomObjectsApi", return_value=custom),
        ):
            r = runner.invoke(admin_app, ["migrate-deployment", "already-there"])
        assert r.exit_code == 0
        assert "already stamped" in r.output

    def test_stamps_new_namespace_under_lease(self):
        core, custom = self._fake_core_and_custom()
        # Legacy SecretClass rename branches are 404 (nothing to copy).
        custom.get_cluster_custom_object.side_effect = _api_exc(404)

        stamp_report = MagicMock()
        stamp_report.verdict = __import__(
            "lakebench.deploy.ownership", fromlist=["IdentityVerdict"]
        ).IdentityVerdict.MATCH

        cm_ctx = MagicMock()
        cm_ctx.__enter__ = MagicMock(return_value=MagicMock())
        cm_ctx.__exit__ = MagicMock(return_value=False)

        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch("kubernetes.client.CustomObjectsApi", return_value=custom),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=cm_ctx),
            patch(
                "lakebench.deploy.ownership.stamp_namespace",
                return_value=stamp_report,
            ),
            patch(
                "lakebench.deploy.ownership.api_server_fingerprint",
                return_value="abcdef123456",
            ),
        ):
            r = runner.invoke(admin_app, ["migrate-deployment", "fresh-ns"])

        assert r.exit_code == 0, r.output
        # Verify lease was engaged.
        cm_ctx.__enter__.assert_called_once()


class TestMigrateSecretclassHelper:
    def test_404_on_legacy_is_noop(self):
        custom = MagicMock()
        custom.get_cluster_custom_object.side_effect = _api_exc(404)
        _migrate_secretclass(custom, "old", "new")
        custom.create_cluster_custom_object.assert_not_called()

    def test_skips_when_new_already_exists_and_matches(self):
        """ADR-F4: only skip when the existing new_name matches the legacy."""
        custom = MagicMock()
        matching_spec = {"backend": {"k8sSearch": {"searchNamespace": {"pod": {}}}}}
        custom.get_cluster_custom_object.side_effect = [
            {"spec": matching_spec, "metadata": {"labels": {"a": "b"}}},
            {"spec": matching_spec, "metadata": {"labels": {"a": "b"}}},
        ]
        _migrate_secretclass(custom, "old", "new")
        custom.create_cluster_custom_object.assert_not_called()

    def test_refuses_when_new_already_exists_with_different_spec(self):
        """ADR-F4: divergent existing new_name means aborted migration or
        manual edit -- must refuse loudly instead of silently binding
        Hive to potentially-foreign credentials."""
        import pytest as _pt

        custom = MagicMock()
        custom.get_cluster_custom_object.side_effect = [
            {"spec": {"backend": {"k8sSearch": {}}}, "metadata": {}},
            {"spec": {"backend": {"kerberos": {}}}, "metadata": {}},
        ]
        with _pt.raises(RuntimeError) as ei:
            _migrate_secretclass(custom, "old", "new")
        assert "does not match" in str(ei.value)
        custom.create_cluster_custom_object.assert_not_called()

    def test_copies_when_new_missing(self):
        custom = MagicMock()
        custom.get_cluster_custom_object.side_effect = [
            {"spec": {"backend": {"k": "v"}}, "metadata": {"labels": {"a": "b"}}},
            _api_exc(404),
        ]
        _migrate_secretclass(custom, "old", "new")
        custom.create_cluster_custom_object.assert_called_once()
        _, kwargs = custom.create_cluster_custom_object.call_args
        assert kwargs["body"]["metadata"]["name"] == "new"
        assert kwargs["body"]["spec"] == {"backend": {"k": "v"}}


class TestStatus:
    def test_reports_no_lease_and_no_namespaces(self):
        core = MagicMock()
        core.list_namespace.return_value = MagicMock(items=[])
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch("lakebench.deploy.cluster_lock.read_cluster_lock", return_value=None),
            patch("lakebench.cli._admin._read_operator_scratch", side_effect=RuntimeError("n/a")),
        ):
            r = runner.invoke(admin_app, ["status"])
        assert r.exit_code == 0
        assert "No lease held" in r.output
        assert "No lakebench-annotated namespaces" in r.output

    def test_reports_annotated_namespaces(self):
        core = MagicMock()
        ns1 = MagicMock()
        ns1.metadata.name = "prod-a"
        ns1.metadata.annotations = {
            "lakebench.deployment/name": "prod-a",
            "lakebench.deployment/api-server": "abc123",
            "lakebench.deployment/committed-sha": "def",
        }
        ns2 = MagicMock()
        ns2.metadata.name = "no-ann"
        ns2.metadata.annotations = None
        core.list_namespace.return_value = MagicMock(items=[ns1, ns2])
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch("lakebench.deploy.cluster_lock.read_cluster_lock", return_value=None),
            patch("lakebench.cli._admin._read_operator_scratch", side_effect=RuntimeError("n/a")),
        ):
            r = runner.invoke(admin_app, ["status"])
        assert r.exit_code == 0
        assert "prod-a" in r.output
        assert "no-ann" not in r.output


class TestReclaimBucket:
    def _mock_s3_and_cfg(self, key_count: int, load_cfg_patched: bool = True):
        s3 = MagicMock()
        s3._init_error = None
        s3.raw_client.list_objects_v2.return_value = {"KeyCount": key_count, "Contents": []}
        cfg = MagicMock()
        cfg.name = "new-owner"
        cfg.platform.storage.s3 = MagicMock()
        return s3, cfg

    def test_refuses_nonempty_bucket(self, tmp_path):
        s3, cfg = self._mock_s3_and_cfg(key_count=5)
        yaml_path = tmp_path / "cfg.yaml"
        yaml_path.write_text("name: x\n")
        with (
            patch("lakebench.cli._admin._get_core_v1"),
            patch("lakebench.cli._admin._load_cfg", return_value=cfg),
            patch("lakebench.s3.S3Client", return_value=s3),
        ):
            r = runner.invoke(admin_app, ["reclaim-bucket", "some-bucket", str(yaml_path)])
        assert r.exit_code == 3  # refused: the bucket holds objects
        assert "has objects" in r.output
        assert "--force-nonempty" in r.output

    def test_rewrites_tag_on_empty_bucket(self, tmp_path):
        s3, cfg = self._mock_s3_and_cfg(key_count=0)
        yaml_path = tmp_path / "cfg.yaml"
        yaml_path.write_text("name: x\n")

        cm_ctx = MagicMock()
        cm_ctx.__enter__ = MagicMock(return_value=MagicMock())
        cm_ctx.__exit__ = MagicMock(return_value=False)

        with (
            patch("lakebench.cli._admin._get_core_v1"),
            patch("lakebench.cli._admin._load_cfg", return_value=cfg),
            patch("lakebench.s3.S3Client", return_value=s3),
            patch("lakebench.deploy.cluster_lock.cluster_lock", return_value=cm_ctx),
            patch("lakebench.deploy.ownership.write_bucket_ownership_tag") as mock_tag,
        ):
            r = runner.invoke(admin_app, ["reclaim-bucket", "some-bucket", str(yaml_path)])
        assert r.exit_code == 0, r.output
        mock_tag.assert_called_once()
        cm_ctx.__enter__.assert_called_once()


def _ns(name: str, phase: str = "Active") -> MagicMock:
    n = MagicMock()
    n.metadata.name = name
    n.status.phase = phase
    return n


def _page(*names: str) -> MagicMock:
    page = MagicMock(items=[_ns(n) for n in names])
    page.metadata._continue = None
    page.metadata.continue_ = None
    return page


class _Repair:
    """A repair-operator harness: a fake lease that records when it is held,
    a manager mock whose reads record whether they ran under it, and Active
    namespaces that may differ inside and outside the lease."""

    def __init__(
        self,
        *,
        values=None,
        live=None,
        active=(),
        active_in_lease=None,
        state=("deployed", 3),
        tmp=TmpVolume(True, "8Gi"),
    ):
        from contextlib import contextmanager

        self.held = False
        self.reads: list[tuple[str, bool]] = []
        self.core = MagicMock()
        outside = _page(*active)
        inside = _page(*(active if active_in_lease is None else active_in_lease))

        def list_ns(**_kw):
            self.reads.append(("namespaces", self.held))
            return inside if self.held else outside

        self.core.list_namespace.side_effect = list_ns

        @contextmanager
        def lock(*_a, **_kw):
            self.held = True
            try:
                yield
            finally:
                self.held = False

        self.lock = MagicMock(side_effect=lock)
        self.mgr = MagicMock()
        self.mgr.HELM_RELEASE_NAME = "spark-operator"
        self.revision_lists: dict[int, list[str] | None] = {}
        states = list(state) if isinstance(state, list) else [state]

        def rec(name, value):
            def f(*_a, **_kw):
                self.reads.append((name, self.held))
                return value() if callable(value) else value

            return f

        def watched(revision=None):
            self.reads.append(("values", self.held))
            if revision is not None:
                return self.revision_lists.get(revision)
            return values

        self.mgr.release_state.side_effect = rec(
            "state", lambda: states.pop(0) if len(states) > 1 else states[0]
        )
        self.mgr._get_watched_namespaces.side_effect = watched
        self.mgr._get_active_namespaces.side_effect = rec("live", live)
        self.mgr.controller_tmp_volume.side_effect = rec("tmp", tmp)
        self.mgr._set_watch_list_impl.return_value = True
        self.mgr.rollback_to.return_value = True
        self.mgr.apply_controller_tmp_size.return_value = True

    def invoke(self, *args: str):
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=self.core),
            patch(
                "lakebench.modules.pipeline_engines.spark.operator.SparkOperatorManager",
                return_value=self.mgr,
            ),
            patch("lakebench.deploy.cluster_lock.cluster_lock", self.lock),
        ):
            return runner.invoke(admin_app, ["repair-operator", *args])


class TestRepairOperator:
    def test_all_watch_noop(self):
        h = _Repair(values=None, live=None, active=("ns-a",))
        r = h.invoke()
        assert r.exit_code == 0, r.output
        assert "watches all namespaces" in r.output
        h.mgr._set_watch_list_impl.assert_not_called()

    def test_reconcile_drops_only_deleted_namespaces_in_dry_run(self):
        """Keep every entry whose namespace still exists and is Active
        (annotated or not); drop only deleted ones. --dry-run takes no lease
        and applies nothing."""
        h = _Repair(
            values=["ns-a", "ns-stale", "ns-gone"],
            live=["ns-a", "ns-stale", "ns-gone"],
            active=("ns-a", "ns-stale"),
        )
        r = h.invoke("--dry-run")
        assert r.exit_code == 0, r.output
        assert "helm values: ['ns-a', 'ns-gone', 'ns-stale']" in r.output
        assert "after:       ['ns-a', 'ns-stale']" in r.output
        assert not h.lock.called
        h.mgr._set_watch_list_impl.assert_not_called()

    def test_repair_rereads_inside_lease(self):
        """A namespace a deploy re-created after an unlocked read must be kept:
         every read runs under the lease and the reconciled list comes from it
        . Reverted (read before the lease, drop one by one),
         ns-b is removed."""
        h = _Repair(
            values=["ns-a", "ns-b", "ns-gone"],
            live=["ns-a", "ns-b", "ns-gone"],
            active=("ns-a",),
            active_in_lease=("ns-a", "ns-b"),
        )
        r = h.invoke()
        assert r.exit_code == 0, r.output
        assert all(held for _, held in h.reads), h.reads
        h.mgr._set_watch_list_impl.assert_called_once_with(["ns-a", "ns-b"])
        h.mgr._remove_namespace_from_watch_impl.assert_not_called()

    def test_one_upgrade_unions_values_and_controller(self):
        """Helm values and the controller's --namespaces disagree after a
        killed upgrade: the target is their union, filtered to Active."""
        h = _Repair(values=["ns-a"], live=["ns-b", "ns-gone"], active=("ns-a", "ns-b"))
        r = h.invoke()
        assert r.exit_code == 0, r.output
        h.mgr._set_watch_list_impl.assert_called_once_with(["ns-a", "ns-b"])

    def test_empty_target_falls_back_to_default(self):
        """Never an empty list: the chart reads {} as every namespace."""
        h = _Repair(values=["ns-gone"], live=["ns-gone"], active=())
        r = h.invoke()
        assert r.exit_code == 0, r.output
        h.mgr._set_watch_list_impl.assert_called_once_with(["default"])

    def test_default_is_not_added_when_others_remain(self):
        h = _Repair(values=["ns-a", "ns-gone"], live=["ns-a", "ns-gone"], active=("ns-a",))
        r = h.invoke()
        assert r.exit_code == 0, r.output
        h.mgr._set_watch_list_impl.assert_called_once_with(["ns-a"])

    def test_unreadable_controller_fails_closed(self):
        from lakebench.modules.pipeline_engines.spark.operator import _DeploymentReadError

        h = _Repair(values=["ns-a", "ns-gone"], active=("ns-a",))
        h.mgr._get_active_namespaces.side_effect = _DeploymentReadError("timeout")
        r = h.invoke()
        assert r.exit_code == 1
        h.mgr._set_watch_list_impl.assert_not_called()

    def test_unreadable_release_state_fails_closed(self):
        h = _Repair(values=["ns-a"], live=["ns-a"], active=("ns-a",), state=None)
        r = h.invoke()
        assert r.exit_code == 1
        h.mgr._set_watch_list_impl.assert_not_called()

    def test_failed_set_exits_nonzero(self):
        h = _Repair(values=["ns-a", "ns-gone"], live=["ns-a", "ns-gone"], active=("ns-a",))
        h.mgr._set_watch_list_impl.return_value = False
        r = h.invoke()
        assert r.exit_code == 1

    def test_pending_release_rolls_back_to_a_safe_revision(self):
        """A pending-upgrade release blocks every later upgrade. Its last
        deployed revision watches only Active namespaces and keeps what the
        controller watches now, so repair rolls back, re-reads and then
        reconciles."""
        h = _Repair(
            values=["ns-a", "ns-gone"],
            live=["ns-a"],
            active=("ns-a",),
            state=[("pending-upgrade", 5), ("deployed", 6)],
        )
        h.mgr.last_good_revision.return_value = 4
        h.revision_lists[4] = ["ns-a"]
        r = h.invoke()
        assert r.exit_code == 0, r.output
        h.mgr.last_good_revision.assert_called_once_with(5)
        h.mgr.rollback_to.assert_called_once_with(4)
        assert all(held for _, held in h.reads), h.reads
        h.mgr._set_watch_list_impl.assert_called_once_with(["ns-a"])

    def test_pending_release_refuses_a_revision_naming_a_deleted_namespace(self):
        """Rolling back to a list naming a deleted namespace crash-loops the
        operator for every tenant: refuse and name the manual step."""
        h = _Repair(values=["ns-a"], live=["ns-a"], active=("ns-a",), state=("pending-upgrade", 5))
        h.mgr.last_good_revision.return_value = 4
        h.revision_lists[4] = ["ns-a", "ns-gone"]
        r = h.invoke()
        assert r.exit_code == 3, r.output
        assert "helm history" in " ".join(r.output.split())
        h.mgr.rollback_to.assert_not_called()
        h.mgr._set_watch_list_impl.assert_not_called()

    def test_pending_release_refuses_a_revision_dropping_a_watched_namespace(self):
        h = _Repair(
            values=["ns-a", "ns-b"],
            live=["ns-a", "ns-b"],
            active=("ns-a", "ns-b"),
            state=("pending-rollback", 7),
        )
        h.mgr.last_good_revision.return_value = 6
        h.revision_lists[6] = ["ns-a"]
        r = h.invoke()
        assert r.exit_code == 3, r.output
        h.mgr.rollback_to.assert_not_called()

    def test_pending_release_without_a_deployed_revision_refuses(self):
        h = _Repair(values=["ns-a"], live=["ns-a"], active=("ns-a",), state=("pending-upgrade", 1))
        h.mgr.last_good_revision.return_value = None
        r = h.invoke()
        assert r.exit_code == 3, r.output
        h.mgr.rollback_to.assert_not_called()

    def test_pending_install_refuses(self):
        h = _Repair(values=["ns-a"], live=["ns-a"], active=("ns-a",), state=("pending-install", 1))
        r = h.invoke()
        assert r.exit_code == 3, r.output
        h.mgr.rollback_to.assert_not_called()
        h.mgr._set_watch_list_impl.assert_not_called()

    def test_absent_release_is_a_prerequisite(self):
        h = _Repair(state=("absent", 0))
        r = h.invoke()
        assert r.exit_code == 4, r.output
        assert "admin install --component spark-operator" in " ".join(r.output.split())

    def test_waits_up_to_three_watch_list_holds(self):
        from lakebench.deploy.cluster_lock import ADMIN_MAX_HOLD_S
        from lakebench.modules.pipeline_engines.spark.operator import _WATCH_LIST_LOCK_TIMEOUT_S

        h = _Repair(values=["ns-a", "ns-gone"], live=["ns-a", "ns-gone"], active=("ns-a",))
        h.invoke()
        kw = h.lock.call_args.kwargs
        assert kw["timeout"] == _WATCH_LIST_LOCK_TIMEOUT_S
        assert kw["max_hold_s"] == ADMIN_MAX_HOLD_S
