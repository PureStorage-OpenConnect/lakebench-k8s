"""Unit tests for the ``lakebench admin`` subcommand tree.

Focus on the ownership discipline these commands enforce: lease-gating
of mutations, idempotent migration, reclaim refusal on non-empty
buckets, and doctor / status read-only reports.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from kubernetes.client.exceptions import ApiException
from typer.testing import CliRunner

from lakebench.cli._admin import (
    _migrate_secretclass,
    admin_app,
)
from lakebench.deploy.cluster_lock import ClusterLockHeld
from lakebench.exit_codes import ExitCode
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


class TestReclaimBucket:
    def _mock_s3_and_cfg(self, key_count: int, load_cfg_patched: bool = True):
        s3 = MagicMock()
        s3._init_error = None
        s3.raw_client.list_objects_v2.return_value = {"KeyCount": key_count, "Contents": []}
        s3.has_user_objects.return_value = key_count > 0
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
            patch("lakebench.deploy.ownership.api_server_fingerprint", return_value="fp1"),
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
            patch("lakebench.deploy.ownership.api_server_fingerprint", return_value="fp1"),
        ):
            r = runner.invoke(admin_app, ["reclaim-bucket", "some-bucket", str(yaml_path)])
        assert r.exit_code == 0, r.output
        mock_tag.assert_called_once()
        # SAF-10: the claim carries this cluster's stamp.
        assert mock_tag.call_args.kwargs["cluster"] == "fp1"
        cm_ctx.__enter__.assert_called_once()

    def test_refuses_without_a_cluster_fingerprint(self, tmp_path):
        s3, cfg = self._mock_s3_and_cfg(key_count=0)
        yaml_path = tmp_path / "cfg.yaml"
        yaml_path.write_text("name: x\n")
        with (
            patch("lakebench.cli._admin._get_core_v1"),
            patch("lakebench.cli._admin._load_cfg", return_value=cfg),
            patch("lakebench.s3.S3Client", return_value=s3),
            patch("lakebench.deploy.ownership.write_bucket_ownership_tag") as mock_tag,
            patch("lakebench.deploy.ownership.api_server_fingerprint", return_value=None),
        ):
            r = runner.invoke(admin_app, ["reclaim-bucket", "some-bucket", str(yaml_path)])
        assert r.exit_code == ExitCode.PREREQUISITE
        assert "fingerprint" in r.output
        mock_tag.assert_not_called()


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


_OLD = 1_000_000.0  # when the pending revision's Secret was created (server time)


def _state(status: str = "deployed", revision: int = 3):
    from lakebench.modules.pipeline_engines.spark.operator import ReleaseState

    return ReleaseState(status, revision)


_SENTINEL = object()


class _Repair:
    """A repair-operator harness: a fake lease that records when it is held,
    a manager mock whose reads and writes record whether they ran under it,
    and Active namespaces that may differ inside and outside the lease.

    ``live`` is both Deployments' ``--namespaces`` unless ``webhook`` is
    given; after a rollback the reads return ``after`` when given."""

    def __init__(
        self,
        *,
        values=None,
        live=None,
        webhook=_SENTINEL,
        active=(),
        active_in_lease=None,
        state=None,
        after=None,
        tmp=TmpVolume(True, "8Gi"),
    ):
        from contextlib import contextmanager

        self.held = False
        self.rolled_back = False
        self.calls: list[tuple[str, bool]] = []
        self.core = MagicMock()
        outside = _page(*active)
        inside = _page(*(active if active_in_lease is None else active_in_lease))

        def list_ns(**_kw):
            self.calls.append(("namespaces", self.held))
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
        self.mgr.CONTROLLER_DEPLOYMENT = "spark-operator-controller"
        self.mgr.WEBHOOK_DEPLOYMENT = "spark-operator-webhook"
        self.revision_lists: dict[int, list[str] | None] = {}
        before = {
            "values": values,
            "spark-operator-controller": live,
            "spark-operator-webhook": live if webhook is _SENTINEL else webhook,
        }
        after = after or {}
        st = state or _state()

        def now(key):
            if self.rolled_back and key in after:
                return after[key]
            return before[key]

        def release_state():
            self.calls.append(("state", self.held))
            if self.rolled_back:
                return after.get("state", _state("deployed", st.revision + 1))
            return st

        def watched(revision=None):
            self.calls.append(("values", self.held))
            if revision is not None:
                return self.revision_lists.get(revision)
            return now("values")

        def deployment(operator_ns=None, deployment=None):
            self.calls.append((deployment or "spark-operator-controller", self.held))
            return now(deployment or "spark-operator-controller")

        def tmp_volume():
            self.calls.append(("tmp", self.held))
            return tmp

        def write(name, result=True):
            def f(*_a, **_kw):
                self.calls.append((name, self.held))
                if name == "rollback":
                    self.rolled_back = True
                return result

            return f

        self.mgr.release_state.side_effect = release_state
        self.mgr.revision_created.return_value = _OLD
        self.mgr._get_watched_namespaces.side_effect = watched
        self.mgr._get_active_namespaces.side_effect = deployment
        self.mgr.controller_tmp_volume.side_effect = tmp_volume
        self.mgr._set_watch_list_impl.side_effect = write("set")
        self.mgr.rollback_to.side_effect = write("rollback")
        self.mgr.apply_controller_tmp_size.side_effect = write("resize")

    def invoke(self, *args: str, now: float | None = _OLD + 3600):
        from email.utils import formatdate

        headers = {} if now is None else {"Date": formatdate(now, usegmt=True)}
        self.core.read_namespace_with_http_info.return_value = (None, 200, headers)
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=self.core),
            patch(
                "lakebench.modules.pipeline_engines.spark.operator.SparkOperatorManager",
                return_value=self.mgr,
            ),
            patch("lakebench.deploy.cluster_lock.cluster_lock", self.lock),
        ):
            return runner.invoke(admin_app, ["repair-operator", *args])

    def writes(self) -> list[str]:
        return [n for n, _ in self.calls if n in ("set", "rollback", "resize")]


def _flat(r) -> str:
    return " ".join(r.output.split())


class TestRepairOperator:
    def test_all_watch_noop(self):
        h = _Repair(values=None, live=None, active=("ns-a",))
        r = h.invoke()
        assert r.exit_code == 0, r.output
        assert "watches all namespaces" in r.output
        assert h.writes() == []

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
        assert "helm values: ['ns-a', 'ns-gone', 'ns-stale']" in _flat(r)
        assert "after: ['ns-a', 'ns-stale']" in _flat(r)
        assert not h.lock.called
        assert h.writes() == []

    def test_repair_rereads_inside_lease(self):
        """A namespace a deploy re-created after an unlocked read must be kept:
        every read and write runs under the lease and the reconciled list
        comes from it. Reverted (read before the lease, drop one by one),
        ns-b is removed."""
        h = _Repair(
            values=["ns-a", "ns-b", "ns-gone"],
            live=["ns-a", "ns-b", "ns-gone"],
            active=("ns-a",),
            active_in_lease=("ns-a", "ns-b"),
        )
        r = h.invoke()
        assert r.exit_code == 0, r.output
        assert all(held for _, held in h.calls), h.calls
        h.mgr._set_watch_list_impl.assert_called_once_with(["ns-a", "ns-b"])
        h.mgr._remove_namespace_from_watch_impl.assert_not_called()

    @pytest.mark.parametrize(
        ("values", "live", "webhook", "active", "target"),
        [
            # controller template stale, values already right
            (["ns-a"], ["ns-a", "ns-gone"], None, ("ns-a",), ["ns-a"]),
            # only the webhook still lists the deleted namespace
            (["ns-a"], ["ns-a"], ["ns-a", "ns-gone"], ("ns-a",), ["ns-a"]),
            # one upgrade unions values and deployments
            (["ns-a"], ["ns-b", "ns-gone"], None, ("ns-a", "ns-b"), ["ns-a", "ns-b"]),
            # never an empty list: the chart reads {} as every namespace
            (["ns-gone"], ["ns-gone"], None, (), ["default"]),
            # default is not added when others remain
            (["ns-a", "ns-gone"], ["ns-a", "ns-gone"], None, ("ns-a",), ["ns-a"]),
        ],
    )
    def test_repair_sets_the_watch_list_once(self, values, live, webhook, active, target):
        kw = {"webhook": webhook} if webhook is not None else {}
        h = _Repair(values=values, live=live, active=active, **kw)
        r = h.invoke()
        assert r.exit_code == 0, r.output
        h.mgr._set_watch_list_impl.assert_called_once_with(target)

    def test_a_watch_all_controller_is_never_narrowed(self):
        """Values list namespaces but the controller watches all: setting the
        values' list would silently stop reconciling every other namespace."""
        h = _Repair(values=["ns-a"], live=None, active=("ns-a", "ns-b"))
        r = h.invoke()
        assert r.exit_code == 3, r.output
        assert "watch every namespace" in _flat(r)
        assert h.writes() == []

    def _revisions_unreadable(self, h):
        from lakebench.modules.pipeline_engines.spark.operator import _WatchListReadError

        h.mgr.good_revisions.return_value = [4]

        def watched(revision=None):
            if revision is not None:
                raise _WatchListReadError("helm get values failed")
            return ["ns-a"]

        h.mgr._get_watched_namespaces.side_effect = watched

    @pytest.mark.parametrize(
        ("values", "state", "setup", "now"),
        [
            # watch-all values while deployments still list namespaces
            (None, None, None, "default"),
            (["ns-a"], ("pending-install", 1), None, "default"),
            (["ns-a"], ("pending-upgrade", 5), "revisions-unreadable", "default"),
            (["ns-a"], ("pending-upgrade", 1), "no-deployed-revision", "default"),
            # no server time or no secret time: never roll back blind
            (["ns-a"], ("pending-rollback", 5), "good-revision-4", None),
            (["ns-a"], ("pending-rollback", 5), "no-secret-time", "default"),
        ],
    )
    def test_repair_refuses_and_writes_nothing(self, values, state, setup, now):
        kw = {"state": _state(*state)} if state else {}
        h = _Repair(values=values, live=["ns-a"], active=("ns-a",), **kw)
        if setup == "revisions-unreadable":
            self._revisions_unreadable(h)
        elif setup == "no-deployed-revision":
            h.mgr.good_revisions.return_value = []
        elif setup in ("good-revision-4", "no-secret-time"):
            h.mgr.good_revisions.return_value = [4]
            h.revision_lists[4] = ["ns-a"]
            if setup == "no-secret-time":
                h.mgr.revision_created.return_value = None
        r = h.invoke() if now == "default" else h.invoke(now=None)
        assert r.exit_code == 3, r.output
        assert h.writes() == []

    def test_unreadable_webhook_fails_closed(self):
        from lakebench.modules.pipeline_engines.spark.operator import _DeploymentReadError

        h = _Repair(values=["ns-a", "ns-gone"], live=["ns-a"], active=("ns-a",))

        def read(operator_ns=None, deployment=None):
            if deployment == "spark-operator-webhook":
                raise _DeploymentReadError("forbidden")
            return ["ns-a"]

        h.mgr._get_active_namespaces.side_effect = read
        r = h.invoke()
        assert r.exit_code == 1
        assert "spark-operator-webhook" in _flat(r)
        assert h.writes() == []

    def test_unreadable_release_state_fails_closed(self):
        h = _Repair(values=["ns-a"], live=["ns-a"], active=("ns-a",))
        h.mgr.release_state.side_effect = None
        h.mgr.release_state.return_value = None
        r = h.invoke()
        assert r.exit_code == 1
        assert h.writes() == []

    def test_namespace_list_error_is_a_message_not_a_traceback(self):
        h = _Repair(values=["ns-a"], live=["ns-a"], active=("ns-a",))
        h.core.list_namespace.side_effect = RuntimeError("apiserver timeout")
        r = h.invoke()
        assert r.exit_code == 1
        assert "Cannot list the cluster's namespaces" in _flat(r)
        assert not isinstance(r.exception, RuntimeError)

    def test_failed_set_exits_nonzero(self):
        h = _Repair(values=["ns-a", "ns-gone"], live=["ns-a", "ns-gone"], active=("ns-a",))
        h.mgr._set_watch_list_impl.side_effect = None
        h.mgr._set_watch_list_impl.return_value = False
        r = h.invoke()
        assert r.exit_code == 1

    def test_pending_release_rolls_back_then_sets_with_what_it_dropped(self):
        """A deploy's add of ns-b was killed after helm applied it: the
        release is pending, the Deployments list ns-b and the last deployed
        revision does not. Repair rolls back (that revision watches no
        deleted namespace) and then sets the list it read before, so ns-b is
        kept. Every step runs under the lease."""
        h = _Repair(
            values=["ns-a", "ns-b"],
            live=["ns-a", "ns-b"],
            active=("ns-a", "ns-b"),
            state=_state("pending-upgrade", 5),
            after={
                "values": ["ns-a"],
                "spark-operator-controller": ["ns-a"],
                "spark-operator-webhook": ["ns-a"],
            },
        )
        h.mgr.good_revisions.return_value = [4]
        h.revision_lists[4] = ["ns-a"]
        r = h.invoke()
        assert r.exit_code == 0, r.output
        h.mgr.good_revisions.assert_called_once_with(5)
        assert h.writes() == ["rollback", "set"]
        h.mgr.rollback_to.assert_called_once_with(4)
        h.mgr._set_watch_list_impl.assert_called_once_with(["ns-a", "ns-b"])
        assert all(held for _, held in h.calls), h.calls

    def test_a_landed_rollback_with_a_slow_rollout_still_sets(self):
        """rollback_to returns False when the rollout wait times out even
        though helm rolled back; the carried namespace must still be set."""
        h = _Repair(
            values=["ns-a", "ns-b"],
            live=["ns-a", "ns-b"],
            active=("ns-a", "ns-b"),
            state=_state("pending-upgrade", 5),
            after={
                "values": ["ns-a"],
                "spark-operator-controller": ["ns-a"],
                "spark-operator-webhook": ["ns-a"],
            },
        )
        h.mgr.good_revisions.return_value = [4]
        h.revision_lists[4] = ["ns-a"]
        h.mgr.rollback_to.side_effect = lambda *_a: setattr(h, "rolled_back", True) or False
        r = h.invoke()
        assert r.exit_code == 0, r.output
        h.mgr._set_watch_list_impl.assert_called_once_with(["ns-a", "ns-b"])

    def test_a_failed_helm_rollback_exits_without_a_set(self):
        h = _Repair(
            values=["ns-a"], live=["ns-a"], active=("ns-a",), state=_state("pending-upgrade", 5)
        )
        h.mgr.good_revisions.return_value = [4]
        h.revision_lists[4] = ["ns-a"]
        h.mgr.rollback_to.side_effect = lambda *_a: False
        r = h.invoke()
        assert r.exit_code == 1, r.output
        h.mgr._set_watch_list_impl.assert_not_called()

    def test_rollback_to_a_watch_all_revision_is_set_back(self):
        """A watch-all revision cannot crash-loop the operator; after it the
        pre-rollback list is set back rather than left watching everything."""
        h = _Repair(
            values=["ns-a"],
            live=["ns-a"],
            active=("ns-a",),
            state=_state("pending-upgrade", 5),
            after={
                "values": None,
                "spark-operator-controller": None,
                "spark-operator-webhook": None,
            },
        )
        h.mgr.good_revisions.return_value = [4]
        h.revision_lists[4] = None
        r = h.invoke()
        assert r.exit_code == 0, r.output
        assert h.writes() == ["rollback", "set"]
        h.mgr._set_watch_list_impl.assert_called_once_with(["ns-a"])

    def test_pending_release_refuses_a_revision_naming_a_deleted_namespace(self):
        """Rolling back to a list naming a deleted namespace crash-loops the
        operator for every tenant: refuse and name the manual recovery."""
        h = _Repair(
            values=["ns-a"], live=["ns-a"], active=("ns-a",), state=_state("pending-upgrade", 5)
        )
        h.mgr.good_revisions.return_value = [4]
        h.revision_lists[4] = ["ns-a", "ns-gone"]
        r = h.invoke()
        assert r.exit_code == 3, r.output
        assert "ns-gone" in _flat(r)
        assert "helm rollback spark-operator 4" in _flat(r)
        assert h.writes() == []

    def test_a_recent_pending_release_is_not_rolled_back(self):
        """A helm call outside the lease (an admin, an unlocked add) may still
        be running: rolling back under it makes two writers. The age is the
        API server's Date minus the revision Secret's creationTimestamp, both
        server time, so a skewed workstation clock cannot change it."""
        h = _Repair(
            values=["ns-a"], live=["ns-a"], active=("ns-a",), state=_state("pending-upgrade", 5)
        )
        h.mgr.good_revisions.return_value = [4]
        h.revision_lists[4] = ["ns-a"]
        with patch("time.time", return_value=_OLD + 99_999):  # the local clock is ignored
            r = h.invoke(now=_OLD + 30)
        assert r.exit_code == 3, r.output
        assert "may still be running" in _flat(r)
        h.mgr.revision_created.assert_called_once_with(5)
        assert h.writes() == []

    def test_an_older_safe_revision_is_used(self):
        """The newest deployed revision names a deleted namespace; an older
        one does not, so that one is the rollback target."""
        h = _Repair(
            values=["ns-a"],
            live=["ns-a"],
            active=("ns-a",),
            state=_state("pending-upgrade", 5),
            after={
                "values": ["ns-a"],
                "spark-operator-controller": ["ns-a"],
                "spark-operator-webhook": ["ns-a"],
            },
        )
        h.mgr.good_revisions.return_value = [4, 3]
        h.revision_lists[4] = ["ns-a", "ns-gone"]
        h.revision_lists[3] = ["ns-a"]
        r = h.invoke()
        assert r.exit_code == 0, r.output
        h.mgr.rollback_to.assert_called_once_with(3)

    def test_a_watch_all_operator_is_not_rolled_back_to_a_list(self):
        """A killed widening upgrade left every source watching all: a
        rollback to the listing revision would narrow the operator."""
        h = _Repair(
            values=None, live=None, active=("ns-a", "ns-b"), state=_state("pending-upgrade", 6)
        )
        h.mgr.good_revisions.return_value = [5]
        h.revision_lists[5] = ["ns-a"]
        r = h.invoke()
        assert r.exit_code == 3, r.output
        assert "stop reconciling" in _flat(r)
        assert h.writes() == []

    def test_a_watch_all_operator_rolls_back_to_a_watch_all_revision(self):
        h = _Repair(
            values=None,
            live=None,
            active=("ns-a",),
            state=_state("pending-upgrade", 6),
            after={
                "values": None,
                "spark-operator-controller": None,
                "spark-operator-webhook": None,
            },
        )
        h.mgr.good_revisions.return_value = [5, 4]
        h.revision_lists[5] = ["ns-a"]
        h.revision_lists[4] = None
        r = h.invoke()
        assert r.exit_code == 0, r.output
        assert h.writes() == ["rollback"]
        h.mgr.rollback_to.assert_called_once_with(4)

    def test_mixed_sources_refuse_before_any_rollback(self):
        """Deployments watch every namespace (the deployed revision had "")
        while a killed upgrade's values list one: rolling back and setting
        the carried list would narrow the operator for every tenant."""
        h = _Repair(
            values=["ns-a"], live=None, active=("ns-a", "ns-b"), state=_state("pending-upgrade", 5)
        )
        h.mgr.good_revisions.return_value = [4]
        h.revision_lists[4] = None
        r = h.invoke()
        assert r.exit_code == 3, r.output
        assert h.writes() == []
        r = h.invoke("--dry-run")
        assert "refuses (exit 3)" in _flat(r)
        assert "roll back to revision" not in _flat(r)

    def test_dry_run_reports_the_rollback_verdict(self):
        h = _Repair(
            values=["ns-a"], live=["ns-a"], active=("ns-a",), state=_state("pending-upgrade", 5)
        )
        h.mgr.good_revisions.return_value = [4]
        h.revision_lists[4] = ["ns-a", "ns-gone"]
        r = h.invoke("--dry-run")
        assert r.exit_code == 0, r.output
        assert "not rolling back" in _flat(r)
        assert "already reconciled" not in _flat(r)
        assert not h.lock.called
        assert h.writes() == []

    def test_absent_release_is_a_prerequisite(self):
        h = _Repair(state=_state("absent", 0))
        r = h.invoke()
        assert r.exit_code == 4, r.output
        assert "admin install --component spark-operator" in _flat(r)

    def test_waits_up_to_three_watch_list_holds(self):
        from lakebench.deploy.cluster_lock import ADMIN_MAX_HOLD_S
        from lakebench.modules.pipeline_engines.spark.operator import _WATCH_LIST_LOCK_TIMEOUT_S

        h = _Repair(values=["ns-a", "ns-gone"], live=["ns-a", "ns-gone"], active=("ns-a",))
        h.invoke()
        kw = h.lock.call_args.kwargs
        assert kw["timeout"] == _WATCH_LIST_LOCK_TIMEOUT_S
        assert kw["max_hold_s"] == ADMIN_MAX_HOLD_S
