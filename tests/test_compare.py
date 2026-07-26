"""Tests for `lakebench compare`.

The interesting failures here are silent ones. A comparison that picks the
wrong run, or paints a faster engine red, still prints a confident table --
there is no error to notice.
"""

import json
from unittest import mock

import pytest

from lakebench.cli._compare import (
    _NOISE_FLOOR_PCT,
    _build_comparison,
    _higher_is_better,
    _is_neutral,
    _load_latest_metrics,
    _run_single,
)


class TestScoreDirection:
    """Which way is better differs per score, and guessing wrong inverts it."""

    @pytest.mark.parametrize(
        "metric",
        [
            "composite_qph",
            "pipeline_throughput_gb_per_second",
            "compute_efficiency_gb_per_core_hour",
        ],
    )
    def test_rate_scores_are_higher_is_better(self, metric):
        assert _higher_is_better(metric)

    @pytest.mark.parametrize(
        "metric",
        ["time_to_value_seconds", "total_elapsed_seconds", "data_freshness_seconds"],
    )
    def test_durations_are_lower_is_better(self, metric):
        assert not _higher_is_better(metric)

    def test_unknown_metric_defaults_to_lower_is_better(self):
        """Most of the scorecard is times and sizes, so that is the safe default."""
        assert not _higher_is_better("some_future_metric_seconds")

    @pytest.mark.parametrize("metric", ["scale_ratio", "total_data_processed_gb"])
    def test_descriptive_scores_are_neutral(self, metric):
        """scale_ratio is best at 1.0 in either direction, not simply higher."""
        assert _is_neutral(metric)


class TestLoadLatestMetrics:
    """Regression: the loader iterated newest-first order in reverse.

    list_runs() returns most-recent-first. Iterating it reversed returned the
    config's *first ever* run, so a repeated comparison silently reported stale
    numbers with no error.
    """

    def _storage(self, tmp_path, runs):
        storage = mock.Mock()
        storage.list_runs.return_value = [{"run_id": r["run_id"]} for r in runs]
        dirs = {}
        for r in runs:
            d = tmp_path / r["run_id"]
            d.mkdir()
            (d / "metrics.json").write_text(json.dumps(r))
            dirs[r["run_id"]] = d
        storage.run_dir.side_effect = lambda rid: dirs[rid]
        return storage

    def test_picks_the_newest_matching_run(self, tmp_path):
        runs = [
            {"run_id": "run-03", "deployment_name": "demo", "marker": "newest"},
            {"run_id": "run-02", "deployment_name": "demo", "marker": "middle"},
            {"run_id": "run-01", "deployment_name": "demo", "marker": "oldest"},
        ]
        cfg = mock.Mock(name_attr="demo")
        cfg.name = "demo"

        with (
            mock.patch(
                "lakebench.metrics.storage.MetricsStorage",
                return_value=self._storage(tmp_path, runs),
            ),
            mock.patch("lakebench.config.load_config", return_value=cfg),
        ):
            result = _load_latest_metrics(tmp_path / "cfg.yaml")

        assert result["marker"] == "newest"

    def test_skips_runs_from_other_deployments(self, tmp_path):
        runs = [
            {"run_id": "run-03", "deployment_name": "other", "marker": "wrong-config"},
            {"run_id": "run-02", "deployment_name": "demo", "marker": "right"},
        ]
        cfg = mock.Mock()
        cfg.name = "demo"

        with (
            mock.patch(
                "lakebench.metrics.storage.MetricsStorage",
                return_value=self._storage(tmp_path, runs),
            ),
            mock.patch("lakebench.config.load_config", return_value=cfg),
        ):
            result = _load_latest_metrics(tmp_path / "cfg.yaml")

        assert result["marker"] == "right"


class TestLocalRunSingle:
    """Local mode needs a deploy first and a data-removing destroy after."""

    def _run(self, tmp_path, **kwargs):
        with (
            mock.patch("subprocess.run") as sp,
            mock.patch(
                "lakebench.cli._compare._load_latest_metrics", return_value={"run_id": "r1"}
            ),
        ):
            sp.return_value = mock.Mock(returncode=0)
            _run_single(tmp_path / "c.yaml", timeout=60, skip_benchmark=False, keep=False, **kwargs)
        return [c.args[0] for c in sp.call_args_list]

    def test_local_deploys_before_running(self, tmp_path):
        """`run --local` refuses without an existing stack."""
        calls = self._run(tmp_path, local=True)
        assert "deploy" in calls[0]
        assert "--local" in calls[0]
        assert "run" in calls[1]
        assert "--local" in calls[1]

    def test_local_destroy_removes_data(self, tmp_path):
        """Surviving buckets would feed stale data to the next run."""
        calls = self._run(tmp_path, local=True)
        destroy = [c for c in calls if "destroy" in c][0]
        assert "--local" in destroy
        assert "--remove-data" in destroy

    def test_cluster_path_does_not_pass_local_flags(self, tmp_path):
        calls = self._run(tmp_path, local=False)
        assert all("--local" not in c for c in calls)
        assert all("deploy" not in c for c in calls)

    def test_generate_is_forwarded(self, tmp_path):
        calls = self._run(tmp_path, local=True, generate=True)
        run_cmd = [c for c in calls if "run" in c][0]
        assert "--generate" in run_cmd

    def test_failed_deploy_reports_rather_than_running(self, tmp_path):
        with mock.patch("subprocess.run") as sp:
            sp.return_value = mock.Mock(returncode=1)
            result = _run_single(
                tmp_path / "c.yaml", timeout=60, skip_benchmark=False, keep=False, local=True
            )
        assert "error" in result
        assert sp.call_count == 1, "must not run the pipeline after a failed deploy"


class TestBuildComparison:
    def test_run_ids_are_recorded(self):
        """A saved comparison that cannot name its runs cannot be traced back."""
        comparison = _build_comparison(
            "a",
            {"run_id": "run-a", "pipeline_benchmark": {"scores": {"composite_qph": 100}}},
            "b",
            {"run_id": "run-b", "pipeline_benchmark": {"scores": {"composite_qph": 200}}},
        )
        assert comparison["config_a"]["run_id"] == "run-a"
        assert comparison["config_b"]["run_id"] == "run-b"

    def test_noise_floor_is_published(self):
        """Readers need to know which differences the run can resolve."""
        comparison = _build_comparison("a", {}, "b", {})
        assert comparison["noise_floor_pct"] == _NOISE_FLOOR_PCT

    def test_scores_from_both_configs_appear(self):
        comparison = _build_comparison(
            "a",
            {"pipeline_benchmark": {"scores": {"only_in_a": 1, "shared": 2}}},
            "b",
            {"pipeline_benchmark": {"scores": {"shared": 3, "only_in_b": 4}}},
        )
        metrics = {r["metric"] for r in comparison["metrics"]}
        assert metrics == {"only_in_a", "shared", "only_in_b"}

    def test_failed_run_yields_no_scores(self):
        comparison = _build_comparison("a", {"error": "boom"}, "b", {})
        assert comparison["config_a"]["error"] == "boom"
        assert comparison["metrics"] == []
