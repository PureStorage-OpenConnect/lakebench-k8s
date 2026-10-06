"""Tests for configuration loading and validation."""

import pytest

from lakebench.config import (
    LakebenchConfig,
    parse_size_to_bytes,
)


class TestParseSize:
    """Tests for size parsing utilities."""

    def test_parse_invalid(self):
        with pytest.raises(ValueError):
            parse_size_to_bytes("invalid")
        with pytest.raises(ValueError):
            parse_size_to_bytes("100xyz")


class TestParseSparkMemory:
    """Tests for Spark memory parsing."""


class TestLakebenchConfig:
    """Tests for LakebenchConfig model."""

    def test_missing_name_raises(self):
        """Test that missing name raises validation error."""
        with pytest.raises(ValueError, match="'name' is required"):
            LakebenchConfig(name="")

    def test_s3_credentials_check(self):
        """Test S3 credential detection."""
        # No credentials
        config = LakebenchConfig(name="test")
        assert not config.has_inline_s3_credentials()

        # Inline credentials
        config = LakebenchConfig(
            name="test", platform={"storage": {"s3": {"access_key": "key", "secret_key": "secret"}}}
        )
        assert config.has_inline_s3_credentials()

        # secret_ref was removed: built without a load purpose it is dropped
        # with a DeprecationWarning; see test_config_honesty_v16.
        with pytest.warns(DeprecationWarning, match="secret_ref"):
            config = LakebenchConfig(
                name="test",
                platform={
                    "storage": {
                        "s3": {"access_key": "k", "secret_key": "s", "secret_ref": "my-secret"}
                    }
                },
            )
        assert config.has_inline_s3_credentials()
        assert not hasattr(config.platform.storage.s3, "secret_ref")

    def test_s3_verify_ssl_false(self):
        """Test S3Config verify_ssl can be set to false."""
        config = LakebenchConfig(
            name="test",
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "https://flashblade:443",
                        "verify_ssl": False,
                        "access_key": "key",
                        "secret_key": "secret",
                    }
                }
            },
        )
        assert config.platform.storage.s3.verify_ssl is False

    def test_dirty_data_ratio_validation(self):
        """Test dirty data ratio must be 0-1."""
        with pytest.raises(ValueError, match="between 0 and 1"):
            LakebenchConfig(
                name="test", architecture={"workload": {"datagen": {"dirty_data_ratio": 1.5}}}
            )


class TestStackableOperatorConfig:
    """Tests for StackableOperatorConfig defaults and override."""


class TestConfigLoader:
    """Tests for configuration file loading."""


class TestScaleConfig:
    """Tests for scale factor configuration."""

    def test_target_size_backward_compat(self):
        """Legacy target_size is converted to scale."""
        with pytest.warns(DeprecationWarning, match="target_size is deprecated"):
            config = LakebenchConfig(
                name="test", architecture={"workload": {"datagen": {"target_size": "100gb"}}}
            )
        assert config.architecture.workload.datagen.scale == 10


class TestPipelineModeConfig:
    """Tests for pipeline mode configuration."""


class TestScratchStorageConfig:
    """Tests for scratch storage configuration."""

    def test_scratch_legacy_create_sc_field_ignored(self):
        """create_storage_class is a legacy field.

        StorageClass is Category 2 shared infrastructure; lakebench no
        longer creates it. Config models reject unknown keys, but this one
        is listed in ScratchStorageConfig._removed_keys, so YAML that still
        carries it loads with a DeprecationWarning and the field is dropped.
        """
        with pytest.warns(DeprecationWarning, match="create_storage_class"):
            config = LakebenchConfig(
                name="test",
                platform={"storage": {"scratch": {"create_storage_class": False}}},
            )
        assert not hasattr(config.platform.storage.scratch, "create_storage_class")


class TestTrinoWorkerStorageConfig:
    """Tests for Trino worker storage configuration."""


class TestGenerateConfig:
    """Tests for configuration generation."""


class TestPerJobExecutorOverrides:
    """Tests for per-job executor count overrides."""

    def test_override_with_scale(self):
        """Per-job overrides coexist with scale factor."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 100}}},
            platform={"compute": {"spark": {"silver_executors": 24}}},
        )
        assert config.architecture.workload.datagen.scale == 100
        assert config.platform.compute.spark.silver_executors == 24
        assert config.platform.compute.spark.bronze_executors is None


class TestComponentValidation:
    """Tests for component combination validation."""

    def test_polaris_without_secret_loads_but_deploy_rejects(self):
        """LB-090 and SAF-8: config load succeeds with no secret, and nothing
        generates one at load. A run with neither a config value nor the
        Secret deploy stores refuses, naming deploy."""
        from lakebench.config.schema import PolarisClientSecretMissing
        from lakebench.deploy.deployment_secrets import polaris_client_secret
        from tests.test_deployment_secrets import FakeCore

        cfg = LakebenchConfig(
            name="a",
            architecture={
                "catalog": {"type": "polaris"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            },
        )
        assert cfg.architecture.catalog.polaris.client_secret == ""
        with pytest.raises(PolarisClientSecretMissing, match="run lakebench deploy first"):
            polaris_client_secret(cfg, FakeCore())

    def test_polaris_supplied_secret_survives_reload(self):
        """The LB-090 core invariant: two independent loads of the same
        config give the same secret, so `deploy` and `run` never diverge."""
        from lakebench.deploy.deployment_secrets import polaris_client_secret
        from tests.test_deployment_secrets import FakeCore

        args = {
            "name": "a",
            "architecture": {
                "catalog": {
                    "type": "polaris",
                    "polaris": {"client_secret": "user-supplied-value"},
                },
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            },
        }
        cfg_a = LakebenchConfig(**args)
        cfg_b = LakebenchConfig(**args)
        assert polaris_client_secret(cfg_a, FakeCore()) == "user-supplied-value"
        assert polaris_client_secret(cfg_b, FakeCore()) == "user-supplied-value"

    def test_unity_iceberg_rejected(self):
        """unity + iceberg is not a supported combination (Unity is Delta-only)."""
        with pytest.raises(ValueError, match="Unsupported component combination"):
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": "unity"},
                    "table_format": {"type": "iceberg"},
                    "query_engine": {"type": "trino"},
                },
            )

    def test_polaris_delta_rejected(self):
        """polaris + delta is rejected (Polaris is Iceberg-native)."""
        with pytest.raises(ValueError, match="Unsupported component combination"):
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": "polaris"},
                    "table_format": {"type": "delta"},
                    "query_engine": {"type": "trino"},
                },
            )

    def test_invalid_format_rejected(self):
        """Invalid table format is rejected by Pydantic validation."""
        with pytest.raises(ValueError):
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": "hive"},
                    "table_format": {"type": "hudi"},
                    "query_engine": {"type": "trino"},
                },
            )

    def test_error_message_lists_supported(self):
        """Error message lists all supported combinations."""
        with pytest.raises(
            ValueError, match="catalog=hive, table_format=iceberg, engine=spark, query_engine=trino"
        ):
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": "polaris"},
                    "table_format": {"type": "delta"},
                    "query_engine": {"type": "trino"},
                },
            )


class TestRecipeName:
    """Tests for recipe name derivation."""


class TestSustainedThroughputConfig:
    """Tests for sustained streaming throughput tuning fields."""


class TestSustainedBenchmarkConfig:
    """Tests for benchmark_interval and benchmark_warmup on SustainedConfig."""

    def test_warmup_clamped_to_gold_refresh(self):
        """Warmup below gold_refresh_interval is clamped up."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {
                    "sustained": {
                        "gold_refresh_interval": "10 minutes",
                        "benchmark_warmup": 300,
                    },
                },
            },
        )
        c = config.architecture.pipeline.sustained
        assert c.benchmark_warmup == 600  # clamped to gold_refresh (10 min)

    def test_warmup_with_short_gold_refresh(self):
        """With a 5-minute gold refresh, warmup of 300s is valid (matches floor)."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {
                    "sustained": {
                        "gold_refresh_interval": "5 minutes",
                        "benchmark_warmup": 300,
                    },
                },
            },
        )
        c = config.architecture.pipeline.sustained
        assert c.benchmark_warmup == 300  # 300s >= 300s floor, matches gold_refresh

    def test_interval_clamped_to_gold_refresh(self):
        """Interval below gold_refresh_interval is clamped up."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {
                    "sustained": {
                        "gold_refresh_interval": "10 minutes",
                        "benchmark_interval": 300,
                    },
                },
            },
        )
        c = config.architecture.pipeline.sustained
        assert c.benchmark_interval == 600  # clamped to gold_refresh (10 min)

    def test_interval_with_short_gold_refresh(self):
        """With a 5-minute gold refresh, interval of 300s is valid (matches floor)."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {
                    "sustained": {
                        "gold_refresh_interval": "5 minutes",
                        "benchmark_interval": 300,
                        "benchmark_warmup": 300,
                    },
                },
            },
        )
        c = config.architecture.pipeline.sustained
        assert c.benchmark_interval == 300  # 300s >= 300s floor, matches gold_refresh


class TestSustainedRetentionConfig:
    """Tests for retention_interval and retention_threshold on SustainedConfig."""


class TestBenchmarkConfig:
    """Tests for BenchmarkConfig in schema."""

    def test_cache_validation(self):
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={"benchmark": {"cache": "warm"}},
            )


# ---------------------------------------------------------------------------
# Batch Cycles Config (v1.1.0)
# ---------------------------------------------------------------------------


class TestBatchCyclesConfig:
    """Tests for pipeline.cycles and pre_benchmark_maintenance fields."""

    def test_cycles_gt1_requires_batch_mode(self):
        """cycles > 1 is invalid with sustained mode."""
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={"processing": {"mode": "sustained", "cycles": 3}},
            )


# ---------------------------------------------------------------------------
# Sustained Compaction Config (v1.1.0)
# ---------------------------------------------------------------------------


class TestSustainedCompactionConfig:
    """Tests for compaction_enabled and compaction_interval on SustainedConfig."""

    def test_compaction_interval_zero_resolves_to_2x_retention(self):
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {
                    "sustained": {
                        "retention_interval": 600,
                        "compaction_interval": 0,
                    },
                },
            },
        )
        assert config.architecture.pipeline.sustained.effective_compaction_interval() == 1200


class TestFinancialW1MaxVertices:
    """LB-119/LB-120: the W1 connected-components vertex cap is a config
    field so the graph detector runs at scale 10 by default and can be
    raised for larger scales without a code edit."""

    def test_rejects_above_ceiling(self):
        from pydantic import ValidationError

        from lakebench.config.schema import WorkloadConfig

        with pytest.raises(ValidationError):
            WorkloadConfig(w1_max_vertices=200_000_001)


def test_job_injects_w1_max_vertices_env():
    """job.py must inject LB_FINANCIAL_W1_MAX_VERTICES for financial so
    gold_finalize can thread the configured cap into W1."""
    from pathlib import Path

    p = Path(__file__).resolve().parents[1] / (
        "src/lakebench/modules/pipeline_engines/spark/job.py"
    )
    body = p.read_text()
    assert "LB_FINANCIAL_W1_MAX_VERTICES" in body
    assert "cfg.architecture.workload.w1_max_vertices" in body
