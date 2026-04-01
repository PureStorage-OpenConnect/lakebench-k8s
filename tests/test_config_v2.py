"""Tests for Config Schema v2 features (env vars, flat fields).

Phase 7 of v1.3 modularization.
"""

from __future__ import annotations

import textwrap

import pytest

from lakebench.config.loader import (
    ConfigError,
    _apply_flat_fields,
    _substitute_env_vars,
    load_config,
)

# ===========================================================================
# Env var substitution
# ===========================================================================


class TestEnvVarSubstitution:
    """Tests for ${VAR} and ${VAR:-default} substitution."""

    def test_simple_substitution(self, monkeypatch):
        monkeypatch.setenv("MY_ENDPOINT", "http://s3:9000")
        result = _substitute_env_vars("endpoint: ${MY_ENDPOINT}")
        assert result == "endpoint: http://s3:9000"

    def test_default_value_used(self):
        result = _substitute_env_vars("scale: ${MISSING_VAR:-10}")
        assert result == "scale: 10"

    def test_default_value_not_used_when_set(self, monkeypatch):
        monkeypatch.setenv("MY_SCALE", "50")
        result = _substitute_env_vars("scale: ${MY_SCALE:-10}")
        assert result == "scale: 50"

    def test_unresolved_var_raises(self):
        with pytest.raises(ConfigError, match="Unresolved environment variables.*MY_SECRET"):
            _substitute_env_vars("key: ${MY_SECRET}")

    def test_multiple_vars(self, monkeypatch):
        monkeypatch.setenv("HOST", "s3.local")
        monkeypatch.setenv("PORT", "9000")
        result = _substitute_env_vars("endpoint: http://${HOST}:${PORT}")
        assert result == "endpoint: http://s3.local:9000"

    def test_no_vars_unchanged(self):
        text = "name: my-lakehouse\nscale: 10"
        assert _substitute_env_vars(text) == text

    def test_empty_default(self):
        result = _substitute_env_vars("region: ${AWS_REGION:-}")
        assert result == "region: "

    def test_default_with_special_chars(self):
        result = _substitute_env_vars("endpoint: ${EP:-http://10.0.0.1:80}")
        assert result == "endpoint: http://10.0.0.1:80"


# ===========================================================================
# Flat field mapping
# ===========================================================================


class TestFlatFieldMapping:
    """Tests for flat top-level field promotion."""

    def test_endpoint_promoted(self):
        data = {"name": "test", "endpoint": "http://s3:9000"}
        result = _apply_flat_fields(data)
        assert result["platform"]["storage"]["s3"]["endpoint"] == "http://s3:9000"
        assert "endpoint" not in result

    def test_scale_promoted(self):
        data = {"name": "test", "scale": 50}
        result = _apply_flat_fields(data)
        assert result["architecture"]["workload"]["datagen"]["scale"] == 50
        assert "scale" not in result

    def test_multiple_flat_fields(self):
        data = {
            "name": "test",
            "endpoint": "http://s3:9000",
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "scale": 10,
            "namespace": "lb-test",
            "mode": "batch",
        }
        result = _apply_flat_fields(data)
        assert result["platform"]["storage"]["s3"]["endpoint"] == "http://s3:9000"
        assert result["platform"]["storage"]["s3"]["access_key"] == "minioadmin"
        assert result["platform"]["kubernetes"]["namespace"] == "lb-test"
        assert result["architecture"]["pipeline"]["mode"] == "batch"
        assert result["architecture"]["workload"]["datagen"]["scale"] == 10

    def test_flat_overrides_nested(self):
        data = {
            "name": "test",
            "endpoint": "http://flat:9000",
            "platform": {"storage": {"s3": {"endpoint": "http://nested:9000"}}},
        }
        result = _apply_flat_fields(data)
        assert result["platform"]["storage"]["s3"]["endpoint"] == "http://flat:9000"

    def test_no_flat_fields_passthrough(self):
        data = {
            "name": "test",
            "platform": {"storage": {"s3": {"endpoint": "http://s3:9000"}}},
        }
        result = _apply_flat_fields(data)
        assert result["platform"]["storage"]["s3"]["endpoint"] == "http://s3:9000"

    def test_spark_image_promoted(self):
        data = {"name": "test", "spark_image": "apache/spark:4.1.1-python3"}
        result = _apply_flat_fields(data)
        assert result["images"]["spark"] == "apache/spark:4.1.1-python3"
        assert "spark_image" not in result


# ===========================================================================
# Integration: flat config file loads correctly
# ===========================================================================


class TestFlatConfigIntegration:
    """Test that a flat v2 config file loads end-to-end."""

    def test_minimal_flat_config(self, tmp_path, monkeypatch):
        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            name: flat-test
            endpoint: http://minio:9000
            access_key: minioadmin
            secret_key: minioadmin
            scale: 10
            """)
        )
        config = load_config(cfg_file)
        assert config.name == "flat-test"
        assert config.platform.storage.s3.endpoint == "http://minio:9000"
        assert config.architecture.workload.datagen.scale == 10

    def test_flat_with_env_vars(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEST_ENDPOINT", "http://s3:9000")
        monkeypatch.setenv("TEST_KEY", "testkey")
        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            name: env-test
            endpoint: ${TEST_ENDPOINT}
            access_key: ${TEST_KEY}
            secret_key: ${TEST_KEY}
            scale: ${MISSING_SCALE:-5}
            """)
        )
        config = load_config(cfg_file)
        assert config.platform.storage.s3.endpoint == "http://s3:9000"
        assert config.platform.storage.s3.access_key == "testkey"
        assert config.architecture.workload.datagen.scale == 5

    def test_flat_with_recipe(self, tmp_path):
        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            name: recipe-test
            recipe: polaris-iceberg-spark-trino
            endpoint: http://minio:9000
            access_key: minioadmin
            secret_key: minioadmin
            """)
        )
        config = load_config(cfg_file)
        assert config.architecture.catalog.type.value == "polaris"
        assert config.architecture.query_engine.type.value == "trino"

    def test_auto_generated_name(self, tmp_path):
        """Config without name field gets auto-generated lb-YYYYMMDD-HHMMSS name."""
        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            endpoint: http://minio:9000
            access_key: minioadmin
            secret_key: minioadmin
            """)
        )
        config = load_config(cfg_file)
        assert config.name.startswith("lb-")
        assert len(config.name) == 18  # lb-YYYYMMDD-HHMMSS

    def test_auto_name_persists_to_state_json(self, tmp_path):
        """Auto-generated name is persisted to .lakebench/state.json."""
        import json

        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            endpoint: http://minio:9000
            access_key: minioadmin
            secret_key: minioadmin
            """)
        )
        config1 = load_config(cfg_file)
        state_file = tmp_path / ".lakebench" / "state.json"
        assert state_file.exists()
        state = json.loads(state_file.read_text())
        assert state["name"] == config1.name

        # Second load reuses the same name
        config2 = load_config(cfg_file)
        assert config2.name == config1.name

    def test_explicit_name_overrides_auto(self, tmp_path):
        """Explicit name in config takes precedence over state.json."""
        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            name: my-explicit-name
            endpoint: http://minio:9000
            access_key: minioadmin
            secret_key: minioadmin
            """)
        )
        config = load_config(cfg_file)
        assert config.name == "my-explicit-name"

    def test_backward_compat_nested_config(self, tmp_path):
        """A v1.2-style nested config still works."""
        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            name: nested-test
            platform:
              storage:
                s3:
                  endpoint: http://minio:9000
                  access_key: minioadmin
                  secret_key: minioadmin
            architecture:
              workload:
                datagen:
                  scale: 10
            """)
        )
        config = load_config(cfg_file)
        assert config.platform.storage.s3.endpoint == "http://minio:9000"
        assert config.architecture.workload.datagen.scale == 10
