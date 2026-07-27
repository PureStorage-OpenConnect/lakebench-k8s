"""Tests for concurrent-safety of the Spark Operator namespace watch list.

``spark.jobNamespaces`` is cluster-scoped Helm state shared by every lakebench
deployment, so adding to it is a read-modify-write that two deploys can run at
once. LB-063 (lost update) and LB-064 (chart version drift on upgrade) both
lived here undetected because there was no test file for this module.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from lakebench.modules.pipeline_engines.spark.operator import SparkOperatorManager

_RUN = "lakebench.modules.pipeline_engines.spark.operator.subprocess.run"


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

    def test_release_already_exists_is_a_conflict(self):
        assert SparkOperatorManager._is_helm_conflict(
            "Error: UPGRADE FAILED: release: already exists"
        )

    def test_another_operation_in_progress_is_a_conflict(self):
        assert SparkOperatorManager._is_helm_conflict(
            "another operation (install/upgrade/rollback) is in progress"
        )

    def test_object_modified_is_a_conflict(self):
        assert SparkOperatorManager._is_helm_conflict(
            "Operation cannot be fulfilled: the object has been modified"
        )

    def test_missing_release_is_not_a_conflict(self):
        assert not SparkOperatorManager._is_helm_conflict(
            'Error: UPGRADE FAILED: "spark-operator" has no deployed releases'
        )

    def test_rbac_denial_is_not_a_conflict(self):
        assert not SparkOperatorManager._is_helm_conflict(
            "Error: forbidden: User cannot patch resource"
        )

    def test_empty_stderr_is_not_a_conflict(self):
        assert not SparkOperatorManager._is_helm_conflict("")


class TestChartVersionPinnedOnUpgrade:
    """LB-064: --reuse-values carries values forward but not chart version."""

    @patch.object(SparkOperatorManager, "_verify_namespace_watched", return_value=True)
    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_filter_existing_namespaces", side_effect=lambda ns: ns)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01"])
    @patch(_RUN)
    def test_upgrade_passes_version_when_pinned(self, mock_run, *_):
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(version="2.4.0", job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is True

        cmd = _helm_cmds(mock_run)[0]
        assert "--version" in cmd, "namespace add must pin the chart version"
        assert cmd[cmd.index("--version") + 1] == "2.4.0"

    @patch.object(SparkOperatorManager, "_verify_namespace_watched", return_value=True)
    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_filter_existing_namespaces", side_effect=lambda ns: ns)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01"])
    @patch(_RUN)
    def test_upgrade_omits_version_when_unpinned(self, mock_run, *_):
        """No pin means latest is intentional -- do not invent a version."""
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is True
        assert "--version" not in _helm_cmds(mock_run)[0]


class TestLostUpdateRace:
    """LB-063: concurrent deploys must not drop each other's namespaces."""

    @patch.object(SparkOperatorManager, "_verify_namespace_watched", return_value=True)
    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_filter_existing_namespaces", side_effect=lambda ns: ns)
    @patch("lakebench.modules.pipeline_engines.spark.operator.time.sleep")
    @patch(_RUN)
    def test_retries_on_conflict_and_rereads_the_list(self, mock_run, _sleep, *_):
        """The retry must re-read. Reusing the stale read is the actual bug.

        Another deploy adds u09 between the first attempt and the retry. If the
        retry replayed the original list, u09 would be silently dropped.
        """
        mock_run.side_effect = [_conflict(), _ok()]
        reads = [["u01"], ["u01", "u09"]]

        with patch.object(SparkOperatorManager, "_get_watched_namespaces", side_effect=reads):
            mgr = SparkOperatorManager(job_namespace="u02")
            assert mgr._add_namespace_to_watch("u02") is True

        cmds = _helm_cmds(mock_run)
        assert len(cmds) == 2, "expected one retry after the conflict"
        final = cmds[1][cmds[1].index("--set") + 1]
        assert final == "spark.jobNamespaces={u01,u09,u02}", (
            f"retry must build on the re-read list, got {final}"
        )

    @patch.object(SparkOperatorManager, "_verify_namespace_watched", return_value=True)
    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_filter_existing_namespaces", side_effect=lambda ns: ns)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01"])
    @patch("lakebench.modules.pipeline_engines.spark.operator.time.sleep")
    @patch(_RUN)
    def test_gives_up_after_retry_budget(self, mock_run, _sleep, *_):
        mock_run.return_value = _conflict()
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is False
        assert len(_helm_cmds(mock_run)) == SparkOperatorManager._HELM_CONFLICT_RETRIES

    @patch.object(SparkOperatorManager, "_verify_namespace_watched", return_value=True)
    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_filter_existing_namespaces", side_effect=lambda ns: ns)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01"])
    @patch("lakebench.modules.pipeline_engines.spark.operator.time.sleep")
    @patch(_RUN)
    def test_non_conflict_error_fails_immediately(self, mock_run, _sleep, *_):
        """A bad chart should not be retried five times before reporting."""
        mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="Error: chart not found")
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is False
        assert len(_helm_cmds(mock_run)) == 1

    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_filter_existing_namespaces", side_effect=lambda ns: ns)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01"])
    @patch("lakebench.modules.pipeline_engines.spark.operator.time.sleep")
    @patch(_RUN)
    def test_re_adds_when_evicted_after_a_successful_upgrade(self, mock_run, _sleep, *_):
        """The silent half of LB-063.

        The upgrade succeeds, then a concurrent writer overwrites the list and
        drops this namespace. Nothing errors -- the job just never runs. The
        manager must notice and re-add rather than report success.
        """
        mock_run.return_value = _ok()

        with patch.object(
            SparkOperatorManager,
            "_verify_namespace_watched",
            side_effect=[False, True],
        ) as verify:
            mgr = SparkOperatorManager(job_namespace="u02")
            assert mgr._add_namespace_to_watch("u02") is True

        assert verify.call_count == 2
        assert len(_helm_cmds(mock_run)) == 2, "eviction must trigger a re-add"

    @patch.object(SparkOperatorManager, "_verify_namespace_watched", return_value=False)
    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_filter_existing_namespaces", side_effect=lambda ns: ns)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01"])
    @patch("lakebench.modules.pipeline_engines.spark.operator.time.sleep")
    @patch(_RUN)
    def test_eviction_retry_is_bounded(self, mock_run, _sleep, *_):
        """Two deploys must not ping-pong forever re-adding themselves."""
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr._add_namespace_to_watch("u02") is False
        assert len(_helm_cmds(mock_run)) == 2

    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=None)
    @patch(_RUN)
    def test_watch_all_short_circuits(self, mock_run, _watched):
        """An operator watching all namespaces needs no upgrade at all."""
        mgr = SparkOperatorManager(job_namespace="u02")
        assert mgr._add_namespace_to_watch("u02") is True
        assert _helm_cmds(mock_run) == []

    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01", "u02"])
    @patch(_RUN)
    def test_already_watched_short_circuits(self, mock_run, _watched):
        mgr = SparkOperatorManager(job_namespace="u02")
        assert mgr._add_namespace_to_watch("u02") is True
        assert _helm_cmds(mock_run) == []


class TestRemoveNamespaceFromWatch:
    """LB-066: a watched namespace that no longer exists crash-loops the operator.

    The controller cannot establish a Pod watch on a missing namespace, so its
    cache never syncs and SparkApplication reconciliation stops for the whole
    cluster -- not just the namespace that was destroyed.
    """

    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(
        SparkOperatorManager,
        "_get_watched_namespaces",
        return_value=["u01", "u02", "u03"],
    )
    @patch(_RUN)
    def test_removes_only_the_target(self, mock_run, *_):
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr.remove_namespace_from_watch("u02") is True

        cmd = _helm_cmds(mock_run)[0]
        assert cmd[cmd.index("--set") + 1] == "spark.jobNamespaces={u01,u03}"

    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01"])
    @patch(_RUN)
    def test_removing_the_last_namespace_does_not_widen_scope(self, mock_run, *_):
        """An empty jobNamespaces means "watch all", not "watch none"."""
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(job_namespace="u01")

        assert mgr.remove_namespace_from_watch("u01") is True

        cmd = _helm_cmds(mock_run)[0]
        assert cmd[cmd.index("--set") + 1] == "spark.jobNamespaces={default}"

    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01", "u02"])
    @patch(_RUN)
    def test_removal_keeps_the_chart_version_pinned(self, mock_run, *_):
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(version="2.4.0", job_namespace="u02")

        assert mgr.remove_namespace_from_watch("u02") is True

        cmd = _helm_cmds(mock_run)[0]
        assert cmd[cmd.index("--version") + 1] == "2.4.0"

    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=None)
    @patch(_RUN)
    def test_watch_all_needs_no_removal(self, mock_run, _watched):
        mgr = SparkOperatorManager(job_namespace="u02")
        assert mgr.remove_namespace_from_watch("u02") is True
        assert _helm_cmds(mock_run) == []

    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01"])
    @patch(_RUN)
    def test_absent_namespace_needs_no_removal(self, mock_run, _watched):
        mgr = SparkOperatorManager(job_namespace="u02")
        assert mgr.remove_namespace_from_watch("u02") is True
        assert _helm_cmds(mock_run) == []

    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01", "u02"])
    @patch("lakebench.modules.pipeline_engines.spark.operator.time.sleep")
    @patch(_RUN)
    def test_retries_on_conflict(self, mock_run, _sleep, *_):
        """Removal contends for the same shared state as the add path."""
        mock_run.side_effect = [_conflict(), _ok()]
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr.remove_namespace_from_watch("u02") is True
        assert len(_helm_cmds(mock_run)) == 2

    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01", "u02"])
    @patch(_RUN)
    def test_missing_helm_reports_failure_without_raising(self, mock_run, _watched):
        """Destroy must not abort because a shared operator is unreachable."""
        mock_run.side_effect = FileNotFoundError("helm")
        mgr = SparkOperatorManager(job_namespace="u02")

        assert mgr.remove_namespace_from_watch("u02") is False

    @patch.object(SparkOperatorManager, "_restart_operator", return_value=True)
    @patch.object(SparkOperatorManager, "_is_openshift", return_value=False)
    @patch.object(SparkOperatorManager, "_get_watched_namespaces", return_value=["u01", "u02"])
    @patch(_RUN)
    def test_restarts_so_the_change_takes_effect(self, mock_run, _watched, _os, restart):
        """The operator reads jobNamespaces at startup only."""
        mock_run.return_value = _ok()
        mgr = SparkOperatorManager(job_namespace="u02")

        mgr.remove_namespace_from_watch("u02")
        restart.assert_called_once()
