"""Flat top-level config fields and env var substitution load end to end."""

from __future__ import annotations

import textwrap

import pytest
import yaml

from lakebench.config.loader import load_config


@pytest.mark.parametrize(
    ("flat", "nested", "read"),
    [
        (
            {"endpoint": "http://flat:9000"},
            {"platform": {"storage": {"s3": {"endpoint": "http://nested:9000"}}}},
            lambda c: c.platform.storage.s3.endpoint,
        ),
        (
            {"scale": 50},
            {"workload": {"datagen": {"scale": 7}}},
            lambda c: c.architecture.workload.datagen.scale,
        ),
    ],
    ids=["endpoint", "scale"],
)
def test_flat_field_overrides_nested(tmp_path, flat, nested, read):
    data = {
        "name": "test",
        "endpoint": "http://minio:9000",
        "access_key": "minioadmin",
        "secret_key": "minioadmin",
    }
    data.update(nested)
    data.update(flat)
    cfg_file = tmp_path / "lakebench.yaml"
    cfg_file.write_text(yaml.safe_dump(data))
    assert read(load_config(cfg_file)) == next(iter(flat.values()))


@pytest.mark.parametrize(
    ("key", "value", "read"),
    [
        ("namespace", "lb-flat", lambda c: c.platform.kubernetes.namespace),
        ("mode", "continuous", lambda c: c.architecture.pipeline.mode.value),
        ("spark_image", "apache/spark:3.5.4-python3", lambda c: c.images.spark),
        ("access_key", "flat-ak", lambda c: c.platform.storage.s3.access_key),
        ("secret_key", "flat-sk", lambda c: c.platform.storage.s3.secret_key),
    ],
)
def test_flat_field_is_promoted(tmp_path, key, value, read):
    cfg_file = tmp_path / "lakebench.yaml"
    cfg_file.write_text(
        yaml.safe_dump({"name": "test", "recipe": "hive-iceberg-spark-trino", key: value})
    )
    with pytest.warns(DeprecationWarning):
        cfg = load_config(cfg_file)
    assert read(cfg) == value


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

    def test_nameless_read_writes_nothing_and_refuses_when_state_holds_a_name(self, tmp_path):
        """A read-only load writes nothing, and refuses once state.json holds a name."""
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
        load_config(cfg_file, purpose=LoadPurpose.READ)
        assert not (tmp_path / ".lakebench").exists()

        state_file = tmp_path / ".lakebench" / "state.json"
        state_file.parent.mkdir()
        state_file.write_text(json.dumps({"name": "lb-20260101-120000", "created": "x"}))
        with pytest.raises(ConfigNameRequired):
            load_config(cfg_file, purpose=LoadPurpose.READ)
        assert sorted(p.name for p in (tmp_path / ".lakebench").iterdir()) == ["state.json"]
