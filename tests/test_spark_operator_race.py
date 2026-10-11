"""Tests for concurrent-safety of the Spark Operator namespace watch list.

``spark.jobNamespaces`` is cluster-scoped Helm state shared by every lakebench
deployment, so adding to it is a read-modify-write that two deploys can run at
once. A lost update and chart version drift on upgrade both
lived here undetected because there was no test file for this module.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager
from tests.fixtures.spark_operator_helpers import INSTALLED, operator_patcher


@pytest.fixture
def operator_run(monkeypatch):
    """Patches the manager's collaborators; returns a factory for the run mock."""
    yield from operator_patcher(monkeypatch)


def _ok() -> MagicMock:
    return MagicMock(returncode=0, stdout="", stderr="")


def _conflict() -> MagicMock:
    return MagicMock(
        returncode=1,
        stdout="",
        stderr="Error: UPGRADE FAILED: release: already exists",
    )


def _helm_cmds(mock_run) -> list[list[str]]:
    """Every helm upgrade argv the manager issued."""
    return [
        call.args[0]
        for call in mock_run.call_args_list
        if call.args and call.args[0][:2] == ["helm", "upgrade"]
    ]


class TestHelmConflictDetection:
    """Only contention retries. Real errors must fail fast."""

    @pytest.mark.parametrize(
        ("stderr", "is_conflict"),
        [
            ("Error: UPGRADE FAILED: release: already exists", True),
            ("another operation (install/upgrade/rollback) is in progress", True),
            ("Operation cannot be fulfilled: the object has been modified", True),
            (
                'Error: UPGRADE FAILED: secrets "sh.helm.release.v1.spark-operator.v32" not found',
                True,
            ),
            ('Error: UPGRADE FAILED: "spark-operator" has no deployed releases', False),
            ("Error: forbidden: User cannot patch resource", False),
            ("", False),
        ],
        ids=[
            "release-exists",
            "operation-in-progress",
            "object-modified",
            "release-secret-missing",
            "no-deployed-release",
            "rbac-denial",
            "empty",
        ],
    )
    def test_only_contention_is_a_conflict(self, stderr, is_conflict):
        assert SparkOperatorManager._is_helm_conflict(stderr) is is_conflict


class TestChartVersionPinnedOnUpgrade:
    """--reuse-values carries values forward but not chart version."""

    def test_upgrade_passes_the_installed_version_not_the_config_pin(self, operator_run):
        mock_run = operator_run()
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(version="2.4.0", job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is True

        cmd = _helm_cmds(mock_run)[0]
        assert "--version" in cmd, "namespace add must pin the chart version"
        assert cmd[cmd.index("--version") + 1] == INSTALLED

    def test_upgrade_refused_when_the_installed_chart_is_unreadable(self, operator_run):
        """No readable installed chart: refuse rather than let Helm pick the
        repo's latest (or the config's pin) for every deployment."""
        mock_run = operator_run()
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(version="2.4.0", job_namespace="u02")

        with patch.object(SparkOperatorManager, "_get_helm_version", return_value=None):
            assert mgr._add_namespace_to_watch("u02") is False
        assert _helm_cmds(mock_run) == []


class TestJobSubmitLatencyBucketsBackfill:
    """A version-pinned ``--reuse-values`` upgrade must backfill
    ``prometheus.metrics.jobSubmitLatencyBuckets``, or a release whose stored
    values predate the 2.5.0 chart renders it empty against the 2.5.1+ template
    and the controller rejects the empty string at startup.
    """

    @pytest.mark.parametrize("path", ["add", "remove"])
    def test_namespace_edit_backfills_when_version_pinned(self, operator_run, path):
        mock_run = operator_run(
            ["u01", "u02"] if path == "remove" else ["u01"],
            verify=None if path == "remove" else True,
            filter_existing=path == "add",
        )
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(version="2.5.1", job_namespace="u02")

        if path == "add":
            assert mgr._add_namespace_to_watch("u02") is True
        else:
            assert mgr.remove_namespace_from_watch("u02") is True

        cmd = _helm_cmds(mock_run)[0]
        sets = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "--set"]
        assert any("jobSubmitLatencyBuckets=" in s for s in sets), (
            f"pinned-version upgrade must backfill jobSubmitLatencyBuckets, got {cmd}"
        )


class TestLostUpdateRace:
    """Concurrent deploys must not drop each other's namespaces."""

    def test_retries_on_conflict_and_rereads_the_list(self, operator_run):
        """The retry must re-read. Reusing the stale read is the actual bug.

        Another deploy adds u09 between the first attempt and the retry. If the
        retry replayed the original list, u09 would be silently dropped.
        """
        mock_run = operator_run(watched_reads=[["u01"], ["u01", "u09"]], sleep=True)
        mock_run.side_effect = [_conflict(), _ok()]

        mgr = SparkOperatorManager(job_namespace="u02")
        assert mgr._add_namespace_to_watch("u02") is True

        cmds = _helm_cmds(mock_run)
        assert len(cmds) == 2, "expected one retry after the conflict"
        final = cmds[1][cmds[1].index("--set") + 1]
        assert final == "spark.jobNamespaces={u01,u09,u02}", (
            f"retry must build on the re-read list, got {final}"
        )

    def test_gives_up_after_retry_budget(self, operator_run):
        mock_run = operator_run(sleep=True)
        mock_run.return_value = _conflict()
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is False
        assert len(_helm_cmds(mock_run)) == SparkOperatorManager._HELM_CONFLICT_RETRIES

    def test_non_conflict_error_fails_immediately(self, operator_run):
        """A bad chart should not be retried five times before reporting."""
        mock_run = operator_run(sleep=True)
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Error: chart not found")
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is False
        assert len(_helm_cmds(mock_run)) == 1

    def test_re_adds_when_evicted_after_a_successful_upgrade(self, operator_run):
        """The silent half of the lost update.

        The upgrade succeeds, then a concurrent writer overwrites the list and
        drops this namespace. Nothing errors -- the job just never runs. The
        manager must notice and re-add rather than report success.
        """
        mock_run = operator_run(verify=[False, True], sleep=True)
        mock_run.return_value = _ok()

        mgr = SparkOperatorManager(job_namespace="u02")
        assert mgr._add_namespace_to_watch("u02") is True

        assert SparkOperatorManager._verify_namespace_watched.call_count == 2
        assert len(_helm_cmds(mock_run)) == 2, "eviction must trigger a re-add"

    def test_eviction_retry_is_bounded(self, operator_run):
        """Two deploys must not ping-pong forever re-adding themselves."""
        mock_run = operator_run(verify=False, sleep=True)
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is False
        assert len(_helm_cmds(mock_run)) == 2

    @pytest.mark.parametrize("watched", [None, ["u01", "u02"]], ids=["watch-all", "already"])
    def test_already_covered_needs_no_upgrade(self, operator_run, watched):
        """An operator watching all namespaces, or this one already, needs no upgrade."""
        mock_run = operator_run(watched)
        mgr = SparkOperatorManager(job_namespace="u02")
        assert mgr._add_namespace_to_watch("u02") is True
        assert _helm_cmds(mock_run) == []


class TestRemoveNamespaceFromWatch:
    """A watched namespace that no longer exists crash-loops the operator.

    The controller cannot establish a Pod watch on a missing namespace, so its
    cache never syncs and SparkApplication reconciliation stops for the whole
    cluster -- not just the namespace that was destroyed.
    """

    @pytest.mark.parametrize(
        ("watched", "target", "expected"),
        [
            (["u01", "u02", "u03"], "u02", "spark.jobNamespaces={u01,u03}"),
            # An empty jobNamespaces means "watch all", not "watch none".
            (["u01"], "u01", "spark.jobNamespaces={default}"),
        ],
        ids=["only-the-target", "last-namespace-keeps-scope"],
    )
    def test_removal_sets_the_remaining_list(self, operator_run, watched, target, expected):
        mock_run = operator_run(watched, verify=None, filter_existing=False)
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(job_namespace=target)

        assert mgr.remove_namespace_from_watch(target) is True

        cmd = _helm_cmds(mock_run)[0]
        assert cmd[cmd.index("--set") + 1] == expected

    def test_removal_keeps_the_installed_chart_version(self, operator_run):
        mock_run = operator_run(["u01", "u02"], verify=None, filter_existing=False)
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(version="2.4.0", job_namespace="u02")

        assert mgr.remove_namespace_from_watch("u02") is True

        cmd = _helm_cmds(mock_run)[0]
        assert cmd[cmd.index("--version") + 1] == INSTALLED

    @pytest.mark.parametrize("watched", [None, ["u01"]], ids=["watch-all", "absent"])
    def test_nothing_to_remove_needs_no_upgrade(self, operator_run, watched):
        mock_run = operator_run(watched, verify=None, filter_existing=False)
        mgr = SparkOperatorManager(job_namespace="u02")
        assert mgr.remove_namespace_from_watch("u02") is True
        assert _helm_cmds(mock_run) == []

    def test_retries_on_conflict(self, operator_run):
        """Removal contends for the same shared state as the add path."""
        mock_run = operator_run(["u01", "u02"], verify=None, filter_existing=False, sleep=True)
        mock_run.side_effect = [_conflict(), _ok()]
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr.remove_namespace_from_watch("u02") is True
        assert len(_helm_cmds(mock_run)) == 2

    def test_missing_helm_reports_failure_without_raising(self, operator_run):
        """Destroy must not abort because a shared operator is unreachable."""
        mock_run = operator_run(["u01", "u02"], verify=None, filter_existing=False)
        mock_run.side_effect = FileNotFoundError("helm")
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr.remove_namespace_from_watch("u02") is False
