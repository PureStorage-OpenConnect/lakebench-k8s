"""Unknown config keys are rejected, documented deprecated spellings still load,
and every shipped example validates (GOALS P5.3, WORKPLAN G1).

Before this, a misspelt key was dropped silently and the run went ahead on the
default. The shipped local AML example carried three whole blocks that no code
read."""

from __future__ import annotations

import warnings
from pathlib import Path

import pytest
import yaml

from lakebench.config import load_config
from lakebench.config.loader import ConfigValidationError, generate_example_config_yaml
from lakebench.config.schema import ConfigModel, LakebenchConfig, PipelineMode

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = sorted((ROOT / "examples").glob("*.yaml"))


@pytest.fixture
def example_env(monkeypatch):
    for var in (
        "LAKEBENCH_POLARIS_CLIENT_SECRET",
        "LAKEBENCH_S3_ACCESS_KEY",
        "LAKEBENCH_S3_SECRET_KEY",
    ):
        monkeypatch.setenv(var, "placeholder")


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_example_validates_without_deprecations(path, example_env):
    # Examples teach the current spelling, so a deprecated key is a failure.
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        cfg = load_config(path)
    assert cfg.name


def test_examples_exist():
    assert len(EXAMPLES) >= 11


def _write(tmp_path, data) -> Path:
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(data))
    return p


@pytest.mark.parametrize(
    ("data", "bad_loc"),
    [
        ({"name": "t", "nmae": "x"}, "nmae"),
        ({"name": "t", "platform": {"kubernetes": {"namespce": "x"}}}, "namespce"),
        (
            {"name": "t", "architecture": {"workload": {"datagen": {"scael": 10}}}},
            "architecture.workload.datagen.scael",
        ),
        ({"name": "t", "platform": {"compute": {"spark": {"image": "x"}}}}, "spark.image"),
        ({"name": "t", "architecture": {"workloads": {"excluded": []}}}, "workloads"),
    ],
)
def test_unknown_key_is_rejected_and_named(tmp_path, data, bad_loc):
    with pytest.raises(ConfigValidationError) as exc:
        load_config(_write(tmp_path, data))
    assert bad_loc in str(exc.value)
    assert "Extra inputs are not permitted" in str(exc.value)


def test_every_schema_model_forbids_extras():
    def walk(model):
        yield model
        for field in model.model_fields.values():
            ann = field.annotation
            for t in getattr(ann, "__args__", (ann,)):
                if isinstance(t, type) and issubclass(t, ConfigModel):
                    yield from walk(t)

    for model in walk(LakebenchConfig):
        assert model.model_config.get("extra") == "forbid", model.__name__


# -- documented back-compat spellings ---------------------------------------


def _load_warns(tmp_path, data):
    with pytest.warns(DeprecationWarning):
        return load_config(_write(tmp_path, data))


def test_mode_continuous_value(tmp_path):
    cfg = _load_warns(tmp_path, {"name": "t", "architecture": {"pipeline": {"mode": "continuous"}}})
    assert cfg.architecture.pipeline.mode == PipelineMode.SUSTAINED


def test_pipeline_continuous_key(tmp_path):
    cfg = _load_warns(
        tmp_path,
        {"name": "t", "architecture": {"pipeline": {"continuous": {"run_duration": 600}}}},
    )
    assert cfg.architecture.pipeline.sustained.run_duration == 600


def test_processing_key(tmp_path):
    cfg = _load_warns(
        tmp_path, {"name": "t", "architecture": {"processing": {"mode": "sustained"}}}
    )
    assert cfg.architecture.pipeline.mode == PipelineMode.SUSTAINED


def test_scratch_create_storage_class_is_dropped_with_warning(tmp_path):
    cfg = _load_warns(
        tmp_path,
        {"name": "t", "platform": {"storage": {"scratch": {"create_storage_class": False}}}},
    )
    assert not hasattr(cfg.platform.storage.scratch, "create_storage_class")


def test_flat_top_level_fields_still_promote(tmp_path):
    cfg = load_config(_write(tmp_path, {"name": "t", "scale": 5, "mode": "batch"}))
    assert cfg.architecture.workload.datagen.scale == 5


def test_workload_schema_alias_and_field_name(tmp_path):
    a = load_config(
        _write(tmp_path, {"name": "t", "architecture": {"workload": {"schema": "financial"}}})
    )
    b = LakebenchConfig(name="t", architecture={"workload": {"schema_type": "financial"}})
    assert a.architecture.workload.schema_type == b.architecture.workload.schema_type


def test_dump_roundtrip_revalidates():
    cfg = LakebenchConfig(name="t", architecture={"workload": {"schema": "financial"}})
    again = LakebenchConfig.model_validate(cfg.model_dump(mode="json"))
    assert again == cfg


def test_init_template_validates(tmp_path):
    p = tmp_path / "init.yaml"
    p.write_text(generate_example_config_yaml())
    assert load_config(p).name
