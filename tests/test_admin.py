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
        assert r.exit_code == 1
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
        assert r.exit_code == 1
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
        assert r.exit_code == 1
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


class TestRepairOperator:
    def test_all_watch_noop(self):
        core = MagicMock()
        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch(
                "lakebench.modules.pipeline_engines.spark.operator.SparkOperatorManager"
            ) as mock_mgr,
        ):
            mgr_instance = mock_mgr.return_value
            mgr_instance._get_watched_namespaces.return_value = None
            r = runner.invoke(admin_app, ["repair-operator"])
        assert r.exit_code == 0
        assert "watches all namespaces" in r.output

    def test_reconcile_drops_only_deleted_namespaces_in_dry_run(self):
        """ADR-F2: keep every entry whose ns still exists (annotated or
        not); only drop entries pointing at DELETED namespaces. Legacy
        pre-PR-1 lakebench namespaces run active workloads and would
        stall silently if pruned."""
        core = MagicMock()
        ns_a = MagicMock()
        ns_a.metadata.name = "ns-a"
        ns_a.metadata.annotations = {"lakebench.deployment/name": "a"}
        ns_a.status.phase = "Active"
        ns_stale = MagicMock()
        ns_stale.metadata.name = "ns-stale"
        ns_stale.metadata.annotations = None  # legacy annotationless
        ns_stale.status.phase = "Active"
        page = MagicMock(items=[ns_a, ns_stale])
        page.metadata._continue = None
        page.metadata.continue_ = None
        core.list_namespace.return_value = page

        with (
            patch("lakebench.cli._admin._get_core_v1", return_value=core),
            patch(
                "lakebench.modules.pipeline_engines.spark.operator.SparkOperatorManager"
            ) as mock_mgr,
        ):
            mgr_instance = mock_mgr.return_value
            # Operator watches three: ns-a (live+annotated),
            # ns-stale (live but annotationless), ns-gone (deleted).
            mgr_instance._get_watched_namespaces.return_value = ["ns-a", "ns-stale", "ns-gone"]
            r = runner.invoke(admin_app, ["repair-operator", "--dry-run"])
        assert r.exit_code == 0
        assert "before: ['ns-a', 'ns-gone', 'ns-stale']" in r.output
        # ns-stale is kept (still exists cluster-side); only ns-gone drops.
        assert "after:  ['default', 'ns-a', 'ns-stale']" in r.output
