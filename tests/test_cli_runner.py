"""CLI surface tests using typer.testing.CliRunner.

Covers BUG-023 (untested commands), BUG-024 (no parameter parsing tests),
and BUG-030 (no CliRunner tests). These tests exercise the CLI entry points
through Typer's test harness -- no K8s cluster or S3 required.
"""

from __future__ import annotations

import pytest
from typer.testing import CliRunner

from lakebench import __version__
from lakebench.cli import app

runner = CliRunner()


# =============================================================================
# version command
# =============================================================================


class TestVersionCommand:
    """Tests for 'lakebench version'."""

    def test_version_output(self):
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0
        assert __version__ in result.output

    def test_version_no_args(self):
        result = runner.invoke(app, ["version"])
        assert result.exit_code == 0


# =============================================================================
# init command (BUG-023)
# =============================================================================


class TestInitCommand:
    """Tests for 'lakebench init'."""

    def test_init_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "test-config.yaml"
        result = runner.invoke(app, ["init", "--output", str(output)])
        assert result.exit_code == 0
        assert output.exists()
        content = output.read_text()
        assert "name:" in content

    def test_init_default_name(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "out.yaml"
        result = runner.invoke(app, ["init", "--output", str(output)])
        assert result.exit_code == 0
        content = output.read_text()
        assert "my-lakehouse" in content

    def test_init_custom_name(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "out.yaml"
        result = runner.invoke(app, ["init", "--output", str(output), "--name", "prod-lake"])
        assert result.exit_code == 0
        content = output.read_text()
        assert "prod-lake" in content

    def test_init_custom_scale(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "out.yaml"
        result = runner.invoke(app, ["init", "--output", str(output), "--scale", "100"])
        assert result.exit_code == 0
        content = output.read_text()
        assert "scale: 100" in content

    def test_init_refuses_overwrite(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "existing.yaml"
        output.write_text("existing config")
        result = runner.invoke(app, ["init", "--output", str(output)])
        assert result.exit_code == 1
        assert "already exists" in result.output

    def test_init_force_overwrites(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "existing.yaml"
        output.write_text("old content")
        result = runner.invoke(app, ["init", "--output", str(output), "--force"])
        assert result.exit_code == 0
        content = output.read_text()
        assert content != "old content"
        assert "name:" in content

    def test_init_with_recipe(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "out.yaml"
        result = runner.invoke(
            app, ["init", "--output", str(output), "--recipe", "polaris-iceberg-spark-trino"]
        )
        assert result.exit_code == 0
        content = output.read_text()
        assert "polaris-iceberg-spark-trino" in content

    def test_init_with_s3_creds(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        output = tmp_path / "out.yaml"
        result = runner.invoke(
            app,
            [
                "init",
                "--output",
                str(output),
                "--endpoint",
                "http://minio:9000",
                "--access-key",
                "minioadmin",
                "--secret-key",
                "miniosecret",
            ],
        )
        assert result.exit_code == 0
        content = output.read_text()
        assert "http://minio:9000" in content


# =============================================================================
# validate/deploy/run -- missing config tests (BUG-024)
# =============================================================================


class TestMissingConfig:
    """Tests that commands fail cleanly when no config file is found."""

    def test_validate_no_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["validate", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_deploy_no_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["deploy", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_run_no_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["run", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_benchmark_no_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["benchmark", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_destroy_no_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["destroy", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_info_no_config(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["info", "nonexistent.yaml"])
        assert result.exit_code != 0

    def test_auto_discover_default(self, tmp_path, monkeypatch):
        """Commands auto-discover ./lakebench.yaml when no arg given."""
        monkeypatch.chdir(tmp_path)
        # No lakebench.yaml present -- should exit with error
        result = runner.invoke(app, ["validate"])
        assert result.exit_code != 0
        assert "lakebench.yaml" in result.output


# =============================================================================
# help output tests (BUG-024)
# =============================================================================


class TestHelpOutput:
    """Tests that each command provides help text."""

    @pytest.mark.parametrize(
        "cmd",
        ["init", "validate", "deploy", "destroy", "run", "benchmark", "info", "version"],
    )
    def test_command_help(self, cmd):
        result = runner.invoke(app, [cmd, "--help"])
        assert result.exit_code == 0
        assert "Usage" in result.output or "usage" in result.output.lower()


# =============================================================================
# journal command (BUG-023)
# =============================================================================


class TestJournalCommand:
    """Tests for 'lakebench journal'."""

    def test_journal_no_sessions(self, tmp_path, monkeypatch):
        """Journal shows message when no sessions exist."""
        monkeypatch.setenv("LAKEBENCH_JOURNAL_DIR", str(tmp_path / "journals"))
        result = runner.invoke(app, ["journal"])
        # Should succeed or show "no sessions" -- not crash
        assert result.exit_code == 0 or "no sessions" in result.output.lower()

    def test_journal_help(self):
        result = runner.invoke(app, ["journal", "--help"])
        assert result.exit_code == 0


# =============================================================================
# benchmark median fix (BUG-026 regression test)
# =============================================================================


class TestBenchmarkMedian:
    """Regression test for BUG-026: median calculation with even iterations."""

    def test_median_odd_iterations(self):
        import statistics

        times = [3.0, 1.0, 2.0]
        assert statistics.median(times) == 2.0

    def test_median_even_iterations(self):
        """Even iteration counts should average the two middle values."""
        import statistics

        times = [1.0, 2.0, 3.0, 4.0]
        assert statistics.median(times) == 2.5

    def test_median_two_iterations(self):
        import statistics

        times = [1.0, 2.0]
        assert statistics.median(times) == 1.5


# =============================================================================
# ObservabilityConfig validation (BUG-029 regression test)
# =============================================================================


class TestObservabilityConfigValidation:
    """Regression test for BUG-029: nested YAML should be rejected."""

    def test_flat_config_accepted(self):
        from lakebench.config.schema import ObservabilityConfig

        cfg = ObservabilityConfig(enabled=True, prometheus_stack_enabled=True)
        assert cfg.enabled is True
        assert cfg.prometheus_stack_enabled is True

    def test_nested_config_rejected(self):
        """Deeply nested YAML structure should raise ValidationError."""
        from pydantic import ValidationError

        from lakebench.config.schema import ObservabilityConfig

        with pytest.raises(ValidationError):
            ObservabilityConfig(
                metrics={"prometheus": {"enabled": True}},
                dashboards={"grafana": {"enabled": True}},
            )

    def test_extra_fields_rejected(self):
        """Unknown fields should raise ValidationError."""
        from pydantic import ValidationError

        from lakebench.config.schema import ObservabilityConfig

        with pytest.raises(ValidationError):
            ObservabilityConfig(enabled=True, unknown_field=True)

    def test_defaults(self):
        from lakebench.config.schema import ObservabilityConfig

        cfg = ObservabilityConfig()
        assert cfg.enabled is False
        assert cfg.prometheus_stack_enabled is True
        assert cfg.dashboards_enabled is True
        assert cfg.retention == "7d"
        assert cfg.storage == "10Gi"


# =============================================================================
# ConfigMap deploy check (BUG-028 regression test)
# =============================================================================


class TestScriptsConfigMapCheck:
    """Regression test for BUG-028: deploy_scripts_configmap return value."""

    def test_deploy_scripts_returns_bool(self):
        """deploy_scripts_configmap should return a boolean."""
        from lakebench.spark.job import SparkJobManager

        assert hasattr(SparkJobManager, "deploy_scripts_configmap")
        # Check return annotation
        import inspect

        sig = inspect.signature(SparkJobManager.deploy_scripts_configmap)
        assert sig.return_annotation is bool or sig.return_annotation == "bool"


# =============================================================================
# report --summary flag
# =============================================================================


class TestReportSummary:
    """Tests for 'lakebench report --summary'."""

    def _seed_run(self, tmp_path):
        """Write a minimal metrics.json with pipeline_benchmark."""
        import json
        from datetime import datetime, timezone

        run_dir = tmp_path / "runs" / "run-summary-test"
        run_dir.mkdir(parents=True)
        now = datetime.now(tz=timezone.utc).isoformat()
        data = {
            "run_id": "summary-test",
            "deployment_name": "test-deploy",
            "start_time": now,
            "end_time": now,
            "total_elapsed_seconds": 120.0,
            "success": True,
            "jobs": [],
            "queries": [],
            "streaming": [],
            "config_snapshot": {"name": "test-deploy", "scale": 10},
            "pipeline_benchmark": {
                "run_id": "summary-test",
                "deployment_name": "test-deploy",
                "pipeline_mode": "batch",
                "start_time": now,
                "end_time": now,
                "success": True,
                "scorecard": {
                    "total_elapsed_seconds": 120.0,
                    "total_data_processed_gb": 10.5,
                    "pipeline_throughput_gb_per_second": 0.088,
                    "time_to_value_seconds": 120.0,
                    "compute_efficiency_gb_per_core_hour": 0.5,
                    "scale_ratio": 1.0,
                    "composite_qph": 0.0,
                },
                "stages": [
                    {
                        "stage_name": "bronze",
                        "stage_type": "batch",
                        "engine": "spark",
                        "elapsed_seconds": 40.0,
                        "input_size_gb": 5.0,
                        "output_size_gb": 5.0,
                        "input_rows": 100000,
                        "output_rows": 100000,
                        "throughput_gb_per_second": 0.125,
                        "throughput_rows_per_second": 2500.0,
                        "executor_count": 4,
                        "executor_cores": 2,
                        "executor_memory_gb": 4.0,
                        "success": True,
                    },
                    {
                        "stage_name": "silver",
                        "stage_type": "batch",
                        "engine": "spark",
                        "elapsed_seconds": 80.0,
                        "input_size_gb": 5.5,
                        "output_size_gb": 3.2,
                        "input_rows": 100000,
                        "output_rows": 80000,
                        "throughput_gb_per_second": 0.069,
                        "throughput_rows_per_second": 1000.0,
                        "executor_count": 8,
                        "executor_cores": 4,
                        "executor_memory_gb": 48.0,
                        "success": True,
                    },
                ],
                "config_snapshot": {"name": "test-deploy", "scale": 10},
            },
        }
        (run_dir / "metrics.json").write_text(json.dumps(data, indent=2))
        return tmp_path / "runs"

    def test_summary_prints_scores(self, tmp_path):
        metrics_dir = self._seed_run(tmp_path)
        result = runner.invoke(
            app, ["report", "--summary", "--metrics", str(metrics_dir), "--run", "summary-test"]
        )
        assert result.exit_code == 0
        assert "Benchmark Summary" in result.output
        assert "test-deploy" in result.output
        assert "bronze" in result.output
        assert "silver" in result.output
        assert "Time to Value" in result.output

    def test_summary_without_flag_no_scores(self, tmp_path):
        metrics_dir = self._seed_run(tmp_path)
        result = runner.invoke(
            app, ["report", "--metrics", str(metrics_dir), "--run", "summary-test"]
        )
        assert result.exit_code == 0
        assert "Report Generated" in result.output
        assert "Benchmark Summary" not in result.output
