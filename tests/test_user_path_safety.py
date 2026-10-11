"""User-path safety: validate before deploy, watch-list edits, remedy hints."""

from __future__ import annotations

import pytest

from lakebench.cli import operator_watch_verdict


class TestValidateWatchListVerdict:
    """validate passes before deploy when the namespace add is possible."""

    @pytest.mark.parametrize("can_edit", [True, None])
    def test_pre_deploy_passes_when_the_add_is_possible_or_unknown(self, can_edit):
        level, _msg, _hint = operator_watch_verdict(
            "lb16-base", ["default"], namespace_exists=False, can_edit_release=can_edit
        )
        assert level == "ok"

    @pytest.mark.parametrize("exists", [True, False, None])
    def test_fails_when_credentials_cannot_make_the_add(self, exists):
        """Without credentials to edit the release, validate fails and names the operator namespace."""
        level, _msg, hint = operator_watch_verdict(
            "lb16-base",
            ["default"],
            namespace_exists=exists,
            can_edit_release=False,
            operator_namespace="spark-operator",
        )
        assert level == "fail"
        assert "spark-operator" in hint


class TestWatchListEditsKeepTheInstalledChart:
    """A namespace add/remove pins the installed chart, never the config's or latest."""

    def _mgr(self, target=None):
        from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

        return SparkOperatorManager(version=target, job_namespace="lb16")

    @staticmethod
    def _helm_list(chart):
        import json
        from unittest.mock import MagicMock

        def run(cmd, **_kw):
            if cmd[:2] == ["helm", "list"]:
                if chart is None:
                    return MagicMock(returncode=1, stdout="", stderr="Error: Unauthorized")
                rows = [{"name": "spark-operator", "chart": f"spark-operator-{chart}"}]
                return MagicMock(returncode=0, stdout=json.dumps(rows), stderr="")
            return MagicMock(returncode=0, stdout="", stderr="")

        return run

    def test_installed_chart_wins_over_config_and_latest(self, monkeypatch):
        """The pin is the installed chart, read afresh. With it unreadable the
        pin is None and the edit is refused; the config's version never stands in."""
        for target in ("2.4.0", None):
            m = self._mgr(target=target)
            monkeypatch.setattr(m, "_run", self._helm_list("2.5.1"))
            assert m._watch_list_pin()[:2] == ["--version", "2.5.1"]
            m = self._mgr(target=target)
            monkeypatch.setattr(m, "_run", self._helm_list(None))
            assert m._watch_list_pin() is None

    def test_remove_refuses_when_the_installed_chart_is_unreadable(self, monkeypatch):
        """A refused removal fails the strict destroy, which keeps the namespace."""
        from unittest.mock import patch

        from lakebench.modules.pipeline_engines.spark.operator import WatchListMutationError

        m = self._mgr(target="2.4.0")
        calls = []
        base = self._helm_list(None)

        def run(cmd, **kw):
            calls.append(cmd)
            return base(cmd, **kw)

        monkeypatch.setattr(m, "_run", run)
        monkeypatch.setattr(m, "_get_watched_namespaces", lambda: ["default", "lb16"])
        assert m._remove_namespace_from_watch_unlocked("lb16") is False
        assert not [c for c in calls if c[:2] == ["helm", "upgrade"]]
        deleted = []
        with (
            patch("kubernetes.client.CoreV1Api"),
            patch("lakebench.deploy.cluster_lock.cluster_lock"),
            pytest.raises(WatchListMutationError),
        ):
            m.remove_namespace_from_watch("lb16", strict=True, then=lambda: deleted.append(1))
        assert deleted == []  # destroy's namespace delete never ran

    def test_recreate_rbac_refuses_without_the_lease(self, monkeypatch):
        from unittest.mock import MagicMock

        m = self._mgr()
        run = MagicMock()
        monkeypatch.setattr(m, "_run", run)
        monkeypatch.setattr(m, "_acquire_watch_lease", lambda: (None, "refuse"))
        monkeypatch.setattr(m, "_get_watched_namespaces", lambda: ["default", "lb16"])
        assert m.recreate_namespace_rbac("lb16") is False
        run.assert_not_called()


@pytest.mark.parametrize("recorded", [True, False])
def test_continuous_reset_needs_the_record_on_tagless_backends(monkeypatch, recorded):
    """On tagless backends the continuous reset proceeds only with an ownership record."""
    from unittest.mock import MagicMock, patch

    from lakebench.cli import _sustained
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    cfg = MagicMock()
    cfg.name = "a"
    cfg.get_namespace.return_value = "a"
    b = cfg.platform.storage.s3.buckets
    b.bronze, b.silver, b.gold = "a-bronze", "a-silver", "a-gold"
    with (
        patch("lakebench.s3.S3Client"),
        patch(
            "lakebench.deploy.ownership.verify_bucket_ownership",
            side_effect=lambda _c, bucket, _n, **_k: IdentityReport(
                verdict=IdentityVerdict.UNSUPPORTED, resource_name=bucket, expected_deployment="a"
            ),
        ),
        patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=[]),
        patch("lakebench.deploy.ownership.tagless_contents_are_ours", return_value=recorded),
    ):
        problem = _sustained._bucket_ownership_problem(cfg, MagicMock())
    assert (problem is None) is recorded


class TestInstalledChartLookup:
    """Only the exact release in the operator namespace sets the watch-list pin."""

    def _mgr(self, stdout, rc=0):
        from unittest.mock import MagicMock

        from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

        m = SparkOperatorManager(namespace="spark-operator")
        m._run = MagicMock(return_value=MagicMock(returncode=rc, stdout=stdout))
        return m

    def test_only_the_exact_release_in_its_namespace_counts(self):
        m = self._mgr(
            '[{"name":"my-spark-operator","chart":"spark-operator-9.9.9"},'
            '{"name":"spark-operator","chart":"spark-operator-2.5.1"}]'
        )
        assert m._get_helm_version() == "2.5.1"
        cmd = m._run.call_args.args[0]
        assert cmd[cmd.index("-n") + 1] == "spark-operator"
        assert "-A" not in cmd
        assert cmd[cmd.index("-f") + 1] == "^spark-operator$"

    def test_unparseable_chart_gives_no_pin(self):
        assert (
            self._mgr('[{"name":"spark-operator","chart":"mirror-chart"}]')._get_helm_version()
            is None
        )
