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
from lakebench.config.loader import ConfigValidationError

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
def test_example_validates_without_deprecations(example_env, path):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        cfg = load_config(path)
    assert cfg.name


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
        # Top-level workload: the error names the key the user wrote.
        ({"name": "t", "workload": {"datagen": {"scael": 10}}}, "  - workload.datagen.scael"),
        ({"name": "t", "platform": {"compute": {"spark": {"image": "x"}}}}, "spark.image"),
        ({"name": "t", "architecture": {"workloads": {"excluded": []}}}, "workloads"),
        ({"name": "t", "platform": {"storage": {"s3": {"bogusx": 1}}}}, "s3.bogusx"),
        ({"name": "t", "platform": {"storage": {"scratch": {"bogusx": 1}}}}, "scratch.bogusx"),
        ({"name": "t", "images": {"bogusx": 1}}, "images.bogusx"),
        ({"name": "t", "observability": {"bogusx": 1}}, "observability.bogusx"),
        ({"name": "t", "architecture": {"catalog": {"hive": {"bogusx": 1}}}}, "hive.bogusx"),
        ({"name": "t", "architecture": {"catalog": {"polaris": {"bogusx": 1}}}}, "polaris.bogusx"),
        ({"name": "t", "architecture": {"pipeline": {"bogusx": 1}}}, "pipeline.bogusx"),
        (
            {"name": "t", "architecture": {"pipeline": {"continuous": {"bogusx": 1}}}},
            "continuous.bogusx",
        ),
        ({"name": "t", "architecture": {"query_engine": {"trino": {"bogusx": 1}}}}, "trino.bogusx"),
        ({"name": "t", "platform": {"compute": {"spark": {"bogusx": 1}}}}, "spark.bogusx"),
    ],
)
def test_unknown_key_is_rejected_and_named(tmp_path, data, bad_loc):
    with pytest.raises(ConfigValidationError) as exc:
        load_config(_write(tmp_path, data))
    assert bad_loc in str(exc.value)
    assert "unknown key" in str(exc.value)


# -- documented back-compat spellings ---------------------------------------


def _load_warns(tmp_path, data):
    with pytest.warns(DeprecationWarning):
        return load_config(_write(tmp_path, data))


def _load_quiet(tmp_path, data):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        return load_config(_write(tmp_path, data))


_RECIPE = "hive-iceberg-spark-trino"


@pytest.mark.parametrize(
    ("pipeline", "top", "deprecated", "mode", "run_duration"),
    [
        ({"mode": "continuous"}, {}, False, "continuous", None),
        ({"mode": "sustained"}, {}, True, "continuous", None),
        ({"continuous": {"run_duration": 600}}, {}, False, "batch", 600),
        ({"sustained": {"run_duration": 600}}, {}, True, "batch", 600),
        ({}, {"processing": {"mode": "sustained"}}, True, "continuous", None),
    ],
    ids=[
        "mode-continuous",
        "mode-sustained",
        "continuous-key",
        "sustained-key",
        "processing-key",
    ],
)
def test_continuous_spellings_resolve_to_one_mode(
    tmp_path, pipeline, top, deprecated, mode, run_duration
):
    arch = {**top}
    if pipeline:
        arch["pipeline"] = pipeline
    data = {"name": "t", "recipe": _RECIPE, "architecture": arch}
    default = load_config(_write(tmp_path, {"name": "t"})).architecture.pipeline
    if deprecated:
        cfg = _load_warns(tmp_path, data)
    else:
        cfg = _load_quiet(tmp_path, data)
    resolved = cfg.architecture.pipeline
    assert resolved.mode.value == mode
    assert resolved.sustained.run_duration == (run_duration or default.sustained.run_duration)


def test_pipeline_continuous_and_sustained_keys_together_refused(tmp_path):
    data = {
        "name": "t",
        "architecture": {"pipeline": {"continuous": {}, "sustained": {"run_duration": 600}}},
    }
    with pytest.raises(ConfigValidationError, match="both 'pipeline.continuous'"):
        load_config(_write(tmp_path, data))


def test_scratch_create_storage_class_is_dropped_with_warning(tmp_path):
    # Dropped with a warning for the read and teardown commands; refused by
    # the commands that change data (CFG-1, LoadPurpose).
    from lakebench.config.loader import LoadPurpose

    data = {"name": "t", "platform": {"storage": {"scratch": {"create_storage_class": False}}}}
    with pytest.warns(DeprecationWarning):
        cfg = load_config(_write(tmp_path, data), purpose=LoadPurpose.TEARDOWN)
    assert not hasattr(cfg.platform.storage.scratch, "create_storage_class")
    with pytest.raises(ConfigValidationError, match="'create_storage_class' was removed"):
        load_config(_write(tmp_path, data))
