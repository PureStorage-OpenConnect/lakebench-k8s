"""Configs written for older lakebench releases keep loading under the strict
schema: keys removed since v1.0 warn and are dropped, and ambiguous double
spellings are refused instead of one being dropped silently."""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.config import load_config
from lakebench.config.loader import ConfigValidationError
from lakebench.config.schema import LakebenchConfig

FIXTURE = Path(__file__).parent / "fixtures" / "v14user.yaml"
runner = CliRunner()

# Every config key a released version accepted that the current schema does
# not have as a field (from the field sets of tags v1.0.0 to v1.5.0), apart
# from pipeline.continuous, which is migrated rather than dropped.
REMOVED = [
    ("images", "pull_secrets", ["regcred"]),
    ("platform.storage.scratch", "create_storage_class", False),
    ("architecture.table_format", "hudi", {"version": "0.14.0", "properties": {}}),
    ("architecture.pipeline.medallion.silver", "strategy", {"enable_salting": True}),
    ("architecture.workload.customer360", "channels", ["web"]),
    ("architecture.workload.customer360", "event_types", ["purchase"]),
    ("architecture.workload.customer360", "quality_distribution", {"clean": 0.92}),
]


def _nest(dotted: str, key: str, value) -> dict:
    d: dict = {key: value}
    for part in reversed(dotted.split(".")):
        d = {part: d}
    return {"name": "t", **d}


@pytest.mark.parametrize(("where", "key", "value"), REMOVED, ids=[k for _, k, _ in REMOVED])
def test_removed_key_warns_and_is_dropped(where, key, value):
    with pytest.warns(DeprecationWarning, match=key):
        cfg = LakebenchConfig.model_validate(_nest(where, key, value))
    obj = cfg
    for part in where.split("."):
        obj = getattr(obj, part)
    assert not hasattr(obj, key)


def test_v14_user_config_loads():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        cfg = load_config(FIXTURE)
    assert cfg.name == "v14user"
    messages = " ".join(str(w.message) for w in caught)
    for key in ("pull_secrets", "create_storage_class", "channels", "quality_distribution"):
        assert key in messages


def test_v14_user_config_reaches_destroy_confirmation(monkeypatch, tmp_path):
    # Without --yes and off a terminal, destroy refuses at the confirmation
    # step, which comes after the config loads and before any cluster call.
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    monkeypatch.chdir(tmp_path)  # journal output lands here, not in the repo
    result = runner.invoke(app, ["destroy", str(FIXTURE)])
    assert "Extra inputs" not in result.output
    assert "validation failed" not in result.output.lower()
    assert "Refusing to destroy" in result.output


def test_v14_user_config_reaches_status(monkeypatch, tmp_path):
    monkeypatch.setenv("KUBECONFIG", "/nonexistent/kubeconfig")
    monkeypatch.chdir(tmp_path)  # journal output lands here, not in the repo
    result = runner.invoke(app, ["status", str(FIXTURE)])
    assert "Extra inputs" not in result.output
    assert "validation failed" not in result.output.lower()


# -- double spellings are refused, not silently resolved ---------------------


def _write(tmp_path, data) -> Path:
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(data))
    return p


def test_processing_and_pipeline_together_is_an_error(tmp_path):
    data = {
        "name": "t",
        "architecture": {"processing": {"cycles": 3}, "pipeline": {"mode": "batch"}},
    }
    with pytest.raises(ConfigValidationError, match="processing.*pipeline"):
        load_config(_write(tmp_path, data))


def test_flat_mode_with_processing_keeps_processing_settings(tmp_path):
    data = {"name": "t", "mode": "batch", "architecture": {"processing": {"cycles": 3}}}
    with pytest.warns(DeprecationWarning, match="processing"):
        cfg = load_config(_write(tmp_path, data))
    assert cfg.architecture.pipeline.cycles == 3


def test_processing_block_is_still_checked_for_unknown_keys(tmp_path):
    data = {"name": "t", "architecture": {"processing": {"cycles": 3, "typo_key": 1}}}
    with pytest.raises(ConfigValidationError, match="typo_key"):
        load_config(_write(tmp_path, data))


def test_continuous_and_sustained_together_is_an_error(tmp_path):
    data = {
        "name": "t",
        "architecture": {
            "pipeline": {"continuous": {"run_duration": 600}, "sustained": {"run_duration": 900}}
        },
    }
    with pytest.raises(ConfigValidationError, match="continuous.*sustained"):
        load_config(_write(tmp_path, data))


def test_schema_and_schema_type_together_is_a_clear_error(tmp_path):
    data = {
        "name": "t",
        "architecture": {"workload": {"schema": "financial", "schema_type": "financial"}},
    }
    with pytest.raises(ConfigValidationError, match="set only 'schema'"):
        load_config(_write(tmp_path, data))


@pytest.mark.parametrize("processing", [None, {}])
def test_flat_mode_with_empty_processing_block(tmp_path, processing):
    data = {"name": "t", "mode": "batch", "cycles": 2, "architecture": {"processing": processing}}
    with pytest.warns(DeprecationWarning, match="processing"):
        cfg = load_config(_write(tmp_path, data))
    assert cfg.architecture.pipeline.cycles == 2


def test_flat_field_under_empty_architecture(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("name: t\nmode: batch\narchitecture:\n")
    assert load_config(p).architecture.pipeline.mode.value == "batch"


def test_flat_field_under_non_mapping_is_a_clear_error(tmp_path):
    from lakebench.config.loader import ConfigError

    with pytest.raises(ConfigError, match="not a mapping"):
        load_config(_write(tmp_path, {"name": "t", "mode": "batch", "architecture": "oops"}))
