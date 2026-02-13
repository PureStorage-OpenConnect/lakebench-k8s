"""Tests for CLI auto-discovery and resolve_config_path."""

from pathlib import Path

import pytest
from click.exceptions import Exit as ClickExit

from lakebench.cli import DEFAULT_CONFIG, resolve_config_path


class TestResolveConfigPath:
    """Tests for config file auto-discovery."""

    def test_explicit_path_returned(self, tmp_path):
        """Explicit path is returned as-is, even if it doesn't exist."""
        p = tmp_path / "custom.yaml"
        assert resolve_config_path(p) == p

    def test_none_finds_default(self, tmp_path, monkeypatch):
        """None resolves to ./lakebench.yaml when it exists."""
        monkeypatch.chdir(tmp_path)
        default = tmp_path / DEFAULT_CONFIG
        default.write_text("name: test\n")
        result = resolve_config_path(None)
        assert result == Path(DEFAULT_CONFIG)

    def test_none_exits_when_missing(self, tmp_path, monkeypatch):
        """None raises typer.Exit when ./lakebench.yaml is absent."""
        monkeypatch.chdir(tmp_path)
        with pytest.raises(ClickExit):
            resolve_config_path(None)

    def test_default_config_constant(self):
        """DEFAULT_CONFIG is lakebench.yaml."""
        assert DEFAULT_CONFIG == "lakebench.yaml"


class TestRecommendCommand:
    """Tests for the lakebench recommend command."""

    def test_compute_requirements_scale_1(self):
        """Scale 1 requires minimal cluster resources."""
        from lakebench.config.scale import customer360_dimensions, full_compute_guidance

        scale = 1
        dims = customer360_dimensions(scale)
        guidance = full_compute_guidance(scale)

        assert dims.approx_bronze_gb == 10.0
        assert guidance.spark.tier_name == "minimal"
        assert guidance.spark.recommended_executors == 2
        assert guidance.datagen.parallelism == 2

    def test_compute_requirements_scale_100(self):
        """Scale 100 requires performance tier resources."""
        from lakebench.config.scale import customer360_dimensions, full_compute_guidance

        scale = 100
        dims = customer360_dimensions(scale)
        guidance = full_compute_guidance(scale)

        assert dims.approx_bronze_gb == 1000.0  # 1 TB
        assert guidance.spark.tier_name == "performance"
        assert guidance.spark.recommended_executors >= 8

    def test_compute_requirements_scale_500(self):
        """Scale 500 requires significant cluster resources."""
        from lakebench.config.scale import customer360_dimensions, full_compute_guidance

        scale = 500
        dims = customer360_dimensions(scale)
        guidance = full_compute_guidance(scale)

        assert dims.approx_bronze_gb == 5000.0  # 5 TB
        assert guidance.spark.tier_name == "performance"
        assert guidance.spark.recommended_executors >= 16

    def test_scale_tiers_are_monotonic(self):
        """Higher scales require more resources."""
        from lakebench.config.scale import full_compute_guidance

        prev_executors = 0
        for scale in [1, 10, 50, 100, 500, 1000]:
            guidance = full_compute_guidance(scale)
            current = guidance.spark.recommended_executors
            assert current >= prev_executors, (
                f"Scale {scale} should need >= executors than lower scales"
            )
            prev_executors = current

    def test_cluster_requirements_include_all_components(self):
        """Cluster requirements include Spark, Trino, Datagen, and infra."""
        from lakebench.config.scale import full_compute_guidance

        scale = 100
        guidance = full_compute_guidance(scale)

        # All components should have non-zero resources
        assert guidance.spark.recommended_executors > 0
        assert guidance.trino.worker_replicas > 0
        assert guidance.datagen.parallelism > 0

        # Memory strings should be valid
        assert guidance.spark.recommended_memory.endswith("g")
        assert guidance.trino.worker_memory.endswith("Gi")
        assert guidance.datagen.memory.endswith("Gi")
