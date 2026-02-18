"""Tests for configuration loading and validation."""

import pytest

from lakebench.config import (
    CatalogType,
    ConfigFileNotFoundError,
    ConfigValidationError,
    LakebenchConfig,
    generate_default_config,
    generate_example_config_yaml,
    load_config,
    parse_size_to_bytes,
    parse_spark_memory,
    save_config,
)


class TestParseSize:
    """Tests for size parsing utilities."""

    def test_parse_bytes(self):
        assert parse_size_to_bytes("1024") == 1024
        assert parse_size_to_bytes("100b") == 100

    def test_parse_kilobytes(self):
        assert parse_size_to_bytes("1kb") == 1024
        assert parse_size_to_bytes("10KB") == 10240

    def test_parse_megabytes(self):
        assert parse_size_to_bytes("1mb") == 1024**2
        assert parse_size_to_bytes("512MB") == 512 * 1024**2

    def test_parse_gigabytes(self):
        assert parse_size_to_bytes("1gb") == 1024**3
        assert parse_size_to_bytes("100GB") == 100 * 1024**3

    def test_parse_terabytes(self):
        assert parse_size_to_bytes("1tb") == 1024**4
        assert parse_size_to_bytes("5TB") == 5 * 1024**4

    def test_parse_invalid(self):
        with pytest.raises(ValueError):
            parse_size_to_bytes("invalid")
        with pytest.raises(ValueError):
            parse_size_to_bytes("100xyz")


class TestParseSparkMemory:
    """Tests for Spark memory parsing."""

    def test_parse_gigabytes(self):
        assert parse_spark_memory("8g") == 8 * 1024**3
        assert parse_spark_memory("48G") == 48 * 1024**3

    def test_parse_megabytes(self):
        assert parse_spark_memory("512m") == 512 * 1024**2
        assert parse_spark_memory("4096M") == 4096 * 1024**2

    def test_parse_kilobytes(self):
        assert parse_spark_memory("1024k") == 1024 * 1024


class TestLakebenchConfig:
    """Tests for LakebenchConfig model."""

    def test_minimal_config(self):
        """Test creating config with just the required field."""
        config = LakebenchConfig(name="test-deployment")
        assert config.name == "test-deployment"
        assert config.version == 1

    def test_missing_name_raises(self):
        """Test that missing name raises validation error."""
        with pytest.raises(ValueError, match="'name' is required"):
            LakebenchConfig(name="")

    def test_default_values(self):
        """Test that default values are set correctly."""
        config = LakebenchConfig(name="test")

        # Platform defaults
        assert config.platform.kubernetes.context == ""
        assert config.platform.kubernetes.create_namespace is True
        assert config.platform.storage.s3.region == "us-east-1"
        assert config.platform.storage.s3.path_style is True

        # Compute defaults (proven values)
        assert config.platform.compute.spark.driver.cores == 4
        assert config.platform.compute.spark.driver.memory == "8g"
        assert config.platform.compute.spark.executor.instances == 8
        assert config.platform.compute.spark.executor.cores == 4
        assert config.platform.compute.spark.executor.memory == "48g"
        assert config.platform.compute.spark.executor.memory_overhead == "12g"

        # Architecture defaults
        assert config.architecture.catalog.type.value == "hive"
        assert config.architecture.table_format.type.value == "iceberg"
        assert config.architecture.query_engine.type.value == "trino"
        assert config.architecture.pipeline.pattern.value == "medallion"

        # Spark conf defaults (S3A tuning)
        assert config.spark.conf["spark.hadoop.fs.s3a.connection.maximum"] == "500"
        assert config.spark.conf["spark.hadoop.fs.s3a.fast.upload"] == "true"

    def test_get_namespace_defaults_to_name(self):
        """Test that namespace defaults to deployment name."""
        config = LakebenchConfig(name="my-deployment")
        assert config.get_namespace() == "my-deployment"

    def test_get_namespace_explicit(self):
        """Test that explicit namespace is used."""
        config = LakebenchConfig(
            name="my-deployment", platform={"kubernetes": {"namespace": "custom-ns"}}
        )
        assert config.get_namespace() == "custom-ns"

    def test_s3_credentials_check(self):
        """Test S3 credential detection."""
        # No credentials
        config = LakebenchConfig(name="test")
        assert not config.has_inline_s3_credentials()
        assert not config.has_s3_secret_ref()

        # Inline credentials
        config = LakebenchConfig(
            name="test", platform={"storage": {"s3": {"access_key": "key", "secret_key": "secret"}}}
        )
        assert config.has_inline_s3_credentials()
        assert not config.has_s3_secret_ref()

        # Secret ref
        config = LakebenchConfig(
            name="test", platform={"storage": {"s3": {"secret_ref": "my-secret"}}}
        )
        assert not config.has_inline_s3_credentials()
        assert config.has_s3_secret_ref()

    def test_quality_distribution_validation(self):
        """Test that quality distribution must sum to 1.0."""
        with pytest.raises(ValueError, match="must sum to 1.0"):
            LakebenchConfig(
                name="test",
                architecture={
                    "workload": {
                        "customer360": {
                            "quality_distribution": {
                                "clean": 0.5,
                                "duplicate_suspected": 0.1,
                                "incomplete": 0.1,
                                "format_inconsistent": 0.1,
                            }
                        }
                    }
                },
            )

    def test_dirty_data_ratio_validation(self):
        """Test dirty data ratio must be 0-1."""
        with pytest.raises(ValueError, match="between 0 and 1"):
            LakebenchConfig(
                name="test", architecture={"workload": {"datagen": {"dirty_data_ratio": 1.5}}}
            )


class TestConfigLoader:
    """Tests for configuration file loading."""

    def test_load_nonexistent_file(self):
        """Test loading a file that doesn't exist."""
        with pytest.raises(ConfigFileNotFoundError):
            load_config("/nonexistent/path/config.yaml")

    def test_load_valid_config(self, tmp_path):
        """Test loading a valid configuration file."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text("""
name: test-deployment
platform:
  storage:
    s3:
      endpoint: http://localhost:9000
      access_key: minioadmin
      secret_key: minioadmin
""")
        config = load_config(config_path)
        assert config.name == "test-deployment"
        assert config.platform.storage.s3.endpoint == "http://localhost:9000"

    def test_load_invalid_yaml(self, tmp_path):
        """Test loading invalid YAML."""
        config_path = tmp_path / "bad.yaml"
        config_path.write_text("name: [invalid yaml")
        with pytest.raises(Exception):  # ConfigParseError  # noqa: B017
            load_config(config_path)

    def test_load_missing_required(self, tmp_path):
        """Test loading config with missing required fields."""
        config_path = tmp_path / "config.yaml"
        config_path.write_text("version: 1")
        with pytest.raises(ConfigValidationError):
            load_config(config_path)

    def test_save_and_load_roundtrip(self, tmp_path):
        """Test that saving and loading preserves config."""
        config = LakebenchConfig(
            name="roundtrip-test",
            description="Test description",
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://localhost:9000",
                        "buckets": {
                            "bronze": "test-bronze",
                            "silver": "test-silver",
                            "gold": "test-gold",
                        },
                    }
                }
            },
        )

        config_path = tmp_path / "config.yaml"
        save_config(config, config_path)

        loaded = load_config(config_path)
        assert loaded.name == config.name
        assert loaded.description == config.description
        assert loaded.platform.storage.s3.buckets.bronze == "test-bronze"


class TestScaleConfig:
    """Tests for scale factor configuration."""

    def test_scale_default(self):
        """Default scale is 10."""
        config = LakebenchConfig(name="test")
        assert config.architecture.workload.datagen.scale == 10

    def test_scale_explicit(self):
        """Setting scale explicitly."""
        config = LakebenchConfig(
            name="test", architecture={"workload": {"datagen": {"scale": 100}}}
        )
        assert config.architecture.workload.datagen.scale == 100

    def test_target_size_backward_compat(self):
        """Legacy target_size is converted to scale."""
        with pytest.warns(DeprecationWarning, match="target_size is deprecated"):
            config = LakebenchConfig(
                name="test", architecture={"workload": {"datagen": {"target_size": "100gb"}}}
            )
        assert config.architecture.workload.datagen.scale == 10

    def test_target_size_1tb(self):
        """1 TB target_size -> scale ~102."""
        with pytest.warns(DeprecationWarning, match="target_size is deprecated"):
            config = LakebenchConfig(
                name="test", architecture={"workload": {"datagen": {"target_size": "1tb"}}}
            )
        # 1 TB = 1024 GB -> scale = round(1024 / 10) = 102
        assert config.architecture.workload.datagen.scale == 102

    def test_scale_min_validation(self):
        """Scale 0 is rejected."""
        with pytest.raises(ValueError):
            LakebenchConfig(name="test", architecture={"workload": {"datagen": {"scale": 0}}})

    def test_get_effective_scale(self):
        """get_effective_scale returns current scale."""
        config = LakebenchConfig(name="test", architecture={"workload": {"datagen": {"scale": 50}}})
        assert config.architecture.workload.datagen.get_effective_scale() == 50

    def test_get_scale_dimensions(self):
        """LakebenchConfig.get_scale_dimensions returns correct dimensions."""
        config = LakebenchConfig(name="test", architecture={"workload": {"datagen": {"scale": 10}}})
        dims = config.get_scale_dimensions()
        assert dims.scale == 10
        assert dims.customers == 1_000_000

    def test_get_compute_guidance(self):
        """LakebenchConfig.get_compute_guidance returns guidance."""
        config = LakebenchConfig(name="test", architecture={"workload": {"datagen": {"scale": 10}}})
        guidance = config.get_compute_guidance()
        assert guidance.tier_name == "balanced"

    def test_payload_size_removed_from_config(self):
        """payload_size is no longer a user-configurable field."""
        # Should not raise -- unknown fields are ignored by default
        config = LakebenchConfig(name="test")
        assert not hasattr(config.architecture.workload.datagen, "payload_size")

    def test_datagen_cpu_memory_defaults(self):
        """Datagen cpu and memory have sensible defaults."""
        config = LakebenchConfig(name="test")
        assert config.architecture.workload.datagen.cpu == "2"
        assert config.architecture.workload.datagen.memory == "4Gi"

    def test_datagen_cpu_memory_override(self):
        """Datagen cpu and memory can be overridden."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"cpu": "8", "memory": "16Gi"}}},
        )
        assert config.architecture.workload.datagen.cpu == "8"
        assert config.architecture.workload.datagen.memory == "16Gi"

    def test_datagen_image_default(self):
        """BUG-010: Default datagen image is docker.io/sillidata/lb-datagen:v2."""
        config = LakebenchConfig(name="test")
        assert config.images.datagen == "docker.io/sillidata/lb-datagen:v2"

    def test_datagen_mode_defaults_auto(self):
        """Datagen mode defaults to 'auto'."""
        from lakebench.config.schema import DatagenMode

        config = LakebenchConfig(name="test")
        assert config.architecture.workload.datagen.mode == DatagenMode.AUTO

    def test_datagen_mode_batch(self):
        """Datagen mode can be set to 'batch'."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"mode": "batch"}}},
        )
        assert config.architecture.workload.datagen.mode.value == "batch"

    def test_datagen_mode_continuous(self):
        """Datagen mode can be set to 'continuous'."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"mode": "continuous"}}},
        )
        assert config.architecture.workload.datagen.mode.value == "continuous"

    def test_datagen_generators_uploaders_defaults(self):
        """Datagen generators/uploaders default to 0 (auto-resolved)."""
        config = LakebenchConfig(name="test")
        assert config.architecture.workload.datagen.generators == 0
        assert config.architecture.workload.datagen.uploaders == 0


class TestPipelineModeConfig:
    """Tests for pipeline mode configuration."""

    def test_pipeline_mode_defaults_batch(self):
        """Pipeline mode defaults to 'batch'."""
        from lakebench.config.schema import PipelineMode

        config = LakebenchConfig(name="test")
        assert config.architecture.pipeline.mode == PipelineMode.BATCH

    def test_pipeline_mode_continuous(self):
        """Pipeline mode can be set to 'continuous'."""
        config = LakebenchConfig(
            name="test",
            architecture={"pipeline": {"mode": "continuous"}},
        )
        assert config.architecture.pipeline.mode.value == "continuous"

    def test_pipeline_mode_invalid_rejected(self):
        """Invalid pipeline mode is rejected by Pydantic."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            LakebenchConfig(
                name="test",
                architecture={"pipeline": {"mode": "invalid"}},
            )


class TestScratchStorageConfig:
    """Tests for scratch storage configuration."""

    def test_scratch_storage_defaults(self):
        """Default scratch config: disabled, px-csi-scratch, 100Gi."""
        config = LakebenchConfig(name="test")
        scratch = config.platform.storage.scratch
        assert scratch.enabled is False
        assert scratch.storage_class == "px-csi-scratch"
        assert scratch.size == "100Gi"

    def test_scratch_create_sc_field(self):
        """create_storage_class defaults to True."""
        config = LakebenchConfig(name="test")
        assert config.platform.storage.scratch.create_storage_class is True

    def test_scratch_override(self):
        """Override scratch config values."""
        config = LakebenchConfig(
            name="test",
            platform={
                "storage": {
                    "scratch": {
                        "enabled": True,
                        "storage_class": "my-sc",
                        "size": "200Gi",
                        "create_storage_class": False,
                    }
                }
            },
        )
        scratch = config.platform.storage.scratch
        assert scratch.enabled is True
        assert scratch.storage_class == "my-sc"
        assert scratch.size == "200Gi"
        assert scratch.create_storage_class is False

    def test_scratch_provisioner_default(self):
        """BUG-001: Default scratch provisioner is Portworx."""
        config = LakebenchConfig(name="test")
        scratch = config.platform.storage.scratch
        assert scratch.provisioner == "pxd.portworx.com"
        assert scratch.parameters == {
            "repl": "1",
            "io_profile": "auto",
            "priority_io": "high",
        }

    def test_scratch_provisioner_override(self):
        """BUG-001: Scratch provisioner and parameters can be overridden."""
        config = LakebenchConfig(
            name="test",
            platform={
                "storage": {
                    "scratch": {
                        "enabled": True,
                        "provisioner": "ebs.csi.aws.com",
                        "parameters": {"type": "gp3", "iopsPerGB": "50"},
                    }
                }
            },
        )
        scratch = config.platform.storage.scratch
        assert scratch.provisioner == "ebs.csi.aws.com"
        assert scratch.parameters == {"type": "gp3", "iopsPerGB": "50"}


class TestTrinoWorkerStorageConfig:
    """Tests for Trino worker storage configuration."""

    def test_trino_worker_storage_defaults(self):
        """Default Trino worker storage: spill on, 50Gi PVC."""
        config = LakebenchConfig(name="test")
        worker = config.architecture.query_engine.trino.worker
        assert worker.spill_enabled is True
        assert worker.spill_max_per_node == "40Gi"
        assert worker.storage == "50Gi"
        assert worker.storage_class == ""

    def test_trino_worker_storage_override(self):
        """Override Trino worker storage fields."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "query_engine": {
                    "trino": {
                        "worker": {
                            "replicas": 4,
                            "storage": "100Gi",
                            "storage_class": "px-csi-db",
                            "spill_enabled": False,
                            "spill_max_per_node": "80Gi",
                        }
                    }
                }
            },
        )
        worker = config.architecture.query_engine.trino.worker
        assert worker.replicas == 4
        assert worker.storage == "100Gi"
        assert worker.storage_class == "px-csi-db"
        assert worker.spill_enabled is False
        assert worker.spill_max_per_node == "80Gi"


class TestGenerateConfig:
    """Tests for configuration generation."""

    def test_generate_default_config(self):
        """Test generating default config with minimal inputs."""
        config = generate_default_config(name="test")
        assert config.name == "test"
        assert "test" in config.description

    def test_generate_config_with_s3(self):
        """Test generating config with S3 settings."""
        config = generate_default_config(
            name="test",
            s3_endpoint="http://localhost:9000",
            s3_access_key="accesskey",
            s3_secret_key="secretkey",
        )
        assert config.platform.storage.s3.endpoint == "http://localhost:9000"
        assert config.platform.storage.s3.access_key == "accesskey"
        assert config.platform.storage.s3.secret_key == "secretkey"

    def test_generate_example_yaml(self):
        """Test generating example YAML."""
        yaml_content = generate_example_config_yaml()
        assert "name:" in yaml_content
        assert "platform:" in yaml_content
        assert "architecture:" in yaml_content
        assert "observability:" in yaml_content
        assert "spark:" in yaml_content
        # Check for proven defaults in comments
        assert "spark.hadoop.fs.s3a" in yaml_content


class TestGeneratedYamlDrift:
    """BUG-008: Verify generated config YAML stays in sync with schema."""

    def test_duckdb_mentioned(self):
        """Generated YAML should mention duckdb as a query engine option."""
        yaml_content = generate_example_config_yaml()
        assert "duckdb" in yaml_content

    def test_recipe_field_present(self):
        """Generated YAML should include a recipe field with valid names."""
        yaml_content = generate_example_config_yaml()
        assert "recipe:" in yaml_content
        # At least one known recipe name should appear
        assert "hive-iceberg-trino" in yaml_content

    def test_datagen_image_matches_schema(self):
        """Generated YAML datagen image should match schema default."""
        yaml_content = generate_example_config_yaml()
        default_image = LakebenchConfig(name="t").images.datagen
        assert default_image in yaml_content

    def test_scratch_provisioner_present(self):
        """Generated YAML should document scratch provisioner field."""
        yaml_content = generate_example_config_yaml()
        assert "provisioner:" in yaml_content

    def test_legend_present(self):
        """Generated YAML should start with a usage legend."""
        yaml_content = generate_example_config_yaml()
        assert "LEGEND" in yaml_content

    def test_spark_operator_note(self):
        """Generated YAML should document that spark operator defaults to false."""
        yaml_content = generate_example_config_yaml()
        assert "default is false" in yaml_content.lower()

    def test_benchmark_section_engine_agnostic(self):
        """Benchmark section should not be Trino-specific."""
        yaml_content = generate_example_config_yaml()
        # Should NOT say "Trino query benchmark" (was the old header)
        assert "Trino query benchmark" not in yaml_content
        # Should have a generic benchmark header
        assert "benchmark" in yaml_content.lower()


class TestPerJobExecutorOverrides:
    """Tests for per-job executor count overrides."""

    def test_overrides_default_none(self):
        """Per-job executor overrides default to None (auto from scale)."""
        config = LakebenchConfig(name="test")
        spark = config.platform.compute.spark
        assert spark.bronze_executors is None
        assert spark.silver_executors is None
        assert spark.gold_executors is None

    def test_override_single_job(self):
        """Can override a single job's executor count."""
        config = LakebenchConfig(
            name="test",
            platform={"compute": {"spark": {"silver_executors": 25}}},
        )
        spark = config.platform.compute.spark
        assert spark.bronze_executors is None
        assert spark.silver_executors == 25
        assert spark.gold_executors is None

    def test_override_all_jobs(self):
        """Can override all job executor counts."""
        config = LakebenchConfig(
            name="test",
            platform={
                "compute": {
                    "spark": {
                        "bronze_executors": 6,
                        "silver_executors": 20,
                        "gold_executors": 12,
                    }
                }
            },
        )
        spark = config.platform.compute.spark
        assert spark.bronze_executors == 6
        assert spark.silver_executors == 20
        assert spark.gold_executors == 12

    def test_override_with_scale(self):
        """Per-job overrides coexist with scale factor."""
        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 100}}},
            platform={"compute": {"spark": {"silver_executors": 30}}},
        )
        assert config.architecture.workload.datagen.scale == 100
        assert config.platform.compute.spark.silver_executors == 30
        assert config.platform.compute.spark.bronze_executors is None


class TestComponentValidation:
    """Tests for component combination validation."""

    def test_default_combination_valid(self):
        """Default combination (hive + iceberg + trino) passes validation."""
        config = LakebenchConfig(name="test")
        assert config.architecture.catalog.type.value == "hive"
        assert config.architecture.table_format.type.value == "iceberg"
        assert config.architecture.query_engine.type.value == "trino"

    def test_hive_iceberg_trino(self):
        """hive + iceberg + trino is supported."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "catalog": {"type": "hive"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            },
        )
        assert config.architecture.catalog.type.value == "hive"

    def test_hive_delta_trino(self):
        """hive + delta + trino is supported."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "catalog": {"type": "hive"},
                "table_format": {"type": "delta"},
                "query_engine": {"type": "trino"},
            },
        )
        assert config.architecture.table_format.type.value == "delta"

    def test_hive_iceberg_none_query_engine(self):
        """hive + iceberg + none query engine is supported (deploy-only)."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "catalog": {"type": "hive"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "none"},
            },
        )
        assert config.architecture.query_engine.type.value == "none"

    def test_polaris_accepted(self):
        """polaris + iceberg + trino is a valid combination."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "catalog": {"type": "polaris"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            },
        )
        assert config.architecture.catalog.type == CatalogType.POLARIS

    def test_polaris_config_defaults(self):
        """Polaris config should have sensible defaults."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "catalog": {"type": "polaris"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            },
        )
        assert config.architecture.catalog.polaris.port == 8181
        assert config.architecture.catalog.polaris.version == "1.3.0-incubating"
        assert config.architecture.catalog.polaris.resources.cpu == "1"
        assert config.architecture.catalog.polaris.resources.memory == "2Gi"

    def test_polaris_delta_rejected(self):
        """polaris + delta is rejected (Polaris is Iceberg-only)."""
        with pytest.raises(ValueError, match="Unsupported component combination"):
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": "polaris"},
                    "table_format": {"type": "delta"},
                    "query_engine": {"type": "trino"},
                },
            )

    def test_unity_rejected(self):
        """unity + iceberg + trino is rejected (not yet implemented)."""
        with pytest.raises(ValueError, match="Unsupported component combination"):
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": "unity"},
                    "table_format": {"type": "iceberg"},
                    "query_engine": {"type": "trino"},
                },
            )

    def test_hudi_rejected(self):
        """hive + hudi + trino is rejected (not yet implemented)."""
        with pytest.raises(ValueError, match="Unsupported component combination"):
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
            ValueError, match="catalog=hive, table_format=iceberg, query_engine=trino"
        ):
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": "unity"},
                    "table_format": {"type": "iceberg"},
                    "query_engine": {"type": "trino"},
                },
            )


class TestRecipeName:
    """Tests for recipe name derivation."""

    def test_recipe_customer360_batch(self):
        """Default recipe is customer360-batch (scale <= 10, mode auto -> batch)."""
        from lakebench.config.autosizer import _resolve_datagen_mode

        config = LakebenchConfig(name="test")
        mode = _resolve_datagen_mode(config)
        recipe = f"{config.architecture.workload.schema_type.value}-{mode}"
        assert recipe == "customer360-batch"

    def test_recipe_customer360_continuous(self):
        """Scale > 10 with auto mode -> customer360-continuous."""
        from lakebench.config.autosizer import _resolve_datagen_mode

        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 50}}},
        )
        mode = _resolve_datagen_mode(config)
        recipe = f"{config.architecture.workload.schema_type.value}-{mode}"
        assert recipe == "customer360-continuous"

    def test_recipe_explicit_batch_mode(self):
        """Explicit batch mode at high scale -> customer360-batch."""
        from lakebench.config.autosizer import _resolve_datagen_mode

        config = LakebenchConfig(
            name="test",
            architecture={"workload": {"datagen": {"scale": 100, "mode": "batch"}}},
        )
        mode = _resolve_datagen_mode(config)
        recipe = f"{config.architecture.workload.schema_type.value}-{mode}"
        assert recipe == "customer360-batch"


class TestContinuousThroughputConfig:
    """Tests for continuous streaming throughput tuning fields."""

    def test_defaults(self):
        config = LakebenchConfig(name="test")
        c = config.architecture.pipeline.continuous
        assert c.max_files_per_trigger == 50
        assert c.bronze_target_file_size_mb == 512
        assert c.silver_target_file_size_mb == 512
        assert c.gold_target_file_size_mb == 128

    def test_custom_values(self):
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {
                    "continuous": {
                        "max_files_per_trigger": 200,
                        "bronze_target_file_size_mb": 256,
                        "silver_target_file_size_mb": 1024,
                        "gold_target_file_size_mb": 64,
                    },
                },
            },
        )
        c = config.architecture.pipeline.continuous
        assert c.max_files_per_trigger == 200
        assert c.bronze_target_file_size_mb == 256
        assert c.silver_target_file_size_mb == 1024
        assert c.gold_target_file_size_mb == 64

    def test_max_files_per_trigger_minimum(self):
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={
                    "processing": {
                        "continuous": {"max_files_per_trigger": 0},
                    },
                },
            )

    def test_target_file_size_minimum(self):
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={
                    "processing": {
                        "continuous": {"bronze_target_file_size_mb": 10},
                    },
                },
            )


class TestBenchmarkConfig:
    """Tests for BenchmarkConfig in schema."""

    def test_defaults(self):
        cfg = LakebenchConfig(name="test")
        b = cfg.architecture.benchmark
        assert b.mode.value == "power"
        assert b.streams == 4
        assert b.cache == "hot"
        assert b.iterations == 1

    def test_yaml_parse(self, tmp_path):
        yaml_content = """
name: bench-test
architecture:
  benchmark:
    mode: throughput
    streams: 8
    cache: cold
    iterations: 3
"""
        f = tmp_path / "bench.yaml"
        f.write_text(yaml_content)
        cfg = load_config(f)
        assert cfg.architecture.benchmark.mode.value == "throughput"
        assert cfg.architecture.benchmark.streams == 8
        assert cfg.architecture.benchmark.cache == "cold"
        assert cfg.architecture.benchmark.iterations == 3

    def test_mode_enum_validation(self):
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={"benchmark": {"mode": "invalid"}},
            )

    def test_streams_bounds(self):
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={"benchmark": {"streams": 0}},
            )
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={"benchmark": {"streams": 65}},
            )

    def test_cache_validation(self):
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={"benchmark": {"cache": "warm"}},
            )

    def test_iterations_bounds(self):
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={"benchmark": {"iterations": 0}},
            )
        with pytest.raises(Exception):  # noqa: B017
            LakebenchConfig(
                name="test",
                architecture={"benchmark": {"iterations": 101}},
            )
