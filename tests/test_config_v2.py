"""Tests for Config Schema v2 features (env vars, flat fields).

Phase 7 of v1.3 modularization.
"""

from __future__ import annotations

import textwrap

import pytest

from lakebench.config.loader import (
    _apply_flat_fields,
    load_config,
)

# ===========================================================================
# Env var substitution
# ===========================================================================


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
        assert result["workload"]["datagen"]["scale"] == 50
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
        assert result["workload"]["datagen"]["scale"] == 10

    def test_flat_overrides_nested(self):
        data = {
            "name": "test",
            "endpoint": "http://flat:9000",
            "platform": {"storage": {"s3": {"endpoint": "http://nested:9000"}}},
        }
        result = _apply_flat_fields(data)
        assert result["platform"]["storage"]["s3"]["endpoint"] == "http://flat:9000"

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

    def test_nameless_config_refused_for_mutating_load(self, tmp_path):
        """A config without a name cannot change data (SAF-2); v1.6 auto-named it."""
        from lakebench.config.loader import ConfigNameRequired

        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            endpoint: http://minio:9000
            access_key: minioadmin
            secret_key: minioadmin
            """)
        )
        with pytest.raises(ConfigNameRequired, match="cannot change data"):
            load_config(cfg_file)
        assert not (tmp_path / ".lakebench").exists()

    def test_nameless_read_refuses_v16_state_name_without_writing(self, tmp_path):
        """A read-only load names the v1.6 state.json name, refuses, writes nothing."""
        import json

        from lakebench.config.loader import ConfigNameRequired, LoadPurpose

        cfg_file = tmp_path / "lakebench.yaml"
        cfg_file.write_text(
            textwrap.dedent("""\
            endpoint: http://minio:9000
            access_key: minioadmin
            secret_key: minioadmin
            """)
        )
        config = load_config(cfg_file, purpose=LoadPurpose.READ)
        assert config.name.startswith("lb-")
        assert not (tmp_path / ".lakebench").exists()

        state_file = tmp_path / ".lakebench" / "state.json"
        state_file.parent.mkdir()
        state_file.write_text(json.dumps({"name": "lb-20260101-120000", "created": "x"}))
        with pytest.raises(ConfigNameRequired, match="name: lb-20260101-120000"):
            load_config(cfg_file, purpose=LoadPurpose.READ)
        assert sorted(p.name for p in (tmp_path / ".lakebench").iterdir()) == ["state.json"]
