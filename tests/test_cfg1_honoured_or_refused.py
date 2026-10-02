"""CFG-1 (CC-11): every recorded setting is honoured or refused at load.

Each test fails with its fix reverted:

- ``platform.compute.spark.driver`` / ``.executor`` and
  ``platform.storage.scratch.size`` sized nothing; a command that changes data
  refuses them with fix text, destroy and status still load them;
- ``operator.install: true`` is refused (C24);
- ``lakebench run`` refuses benchmark settings it does not honour, and its
  snapshot records the power pass it runs;
- a validation error never echoes the config it failed on (LB-230).
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import LakebenchConfig, LoadPurpose, load_config
from lakebench.config.loader import ConfigValidationError, load_notes

_BASE = {
    "name": "cc11-test",
    "platform": {
        "storage": {
            "s3": {
                "endpoint": "http://minio:9000",
                "access_key": "minioadmin",
                "secret_key": "minioadmin",
            }
        }
    },
}


def _write(tmp_path: Path, extra: dict) -> Path:
    data = yaml.safe_load(yaml.safe_dump(_BASE))

    def merge(dst: dict, src: dict) -> None:
        for k, v in src.items():
            if isinstance(v, dict) and isinstance(dst.get(k), dict):
                merge(dst[k], v)
            else:
                dst[k] = v

    merge(data, extra)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


_EXECUTOR = {"platform": {"compute": {"spark": {"executor": {"instances": 16, "memory": "8g"}}}}}
_DRIVER = {"platform": {"compute": {"spark": {"driver": {"cores": 2}}}}}
_SCRATCH = {"platform": {"storage": {"scratch": {"enabled": True, "size": "200Gi"}}}}


# -- removed sizing blocks -----------------------------------------------------


@pytest.mark.parametrize("purpose", [LoadPurpose.RUN, LoadPurpose.MUTATE])
@pytest.mark.parametrize(
    ("extra", "key"),
    [(_EXECUTOR, "'executor' was removed"), (_DRIVER, "'driver' was removed")],
)
def test_removed_executor_block_refused_on_run(tmp_path, purpose, extra, key):
    path = _write(tmp_path, extra)
    with pytest.raises(ConfigValidationError) as e:
        load_config(path, purpose=purpose, print_notes=False)
    text = str(e.value)
    assert key in text
    assert "job profiles" in text and "<job>_executors" in text


@pytest.mark.parametrize("purpose", [LoadPurpose.TEARDOWN, LoadPurpose.READ])
def test_removed_executor_block_loads_for_destroy(tmp_path, purpose):
    path = _write(
        tmp_path,
        {
            "platform": {
                "compute": _EXECUTOR["platform"]["compute"],
                "storage": _SCRATCH["platform"]["storage"],
            }
        },
    )
    cfg = load_config(path, purpose=purpose, print_notes=False)
    notes = " ".join(load_notes(cfg).texts())
    assert "'executor' (SparkComputeConfig) is no longer used" in notes
    assert "'size' (ScratchStorageConfig) is no longer used" in notes
    assert not hasattr(cfg.platform.compute.spark, "executor")
    assert not hasattr(cfg.platform.storage.scratch, "size")


def test_removed_scratch_size_refused_on_run(tmp_path):
    path = _write(tmp_path, _SCRATCH)
    with pytest.raises(ConfigValidationError, match="'size' was removed: per-job scratch"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)


# -- operator install keys (C24) ----------------------------------------------

_SPARK_OP = {"platform": {"compute": {"spark": {"operator": {"install": True}}}}}
_HIVE_OP = {"architecture": {"catalog": {"hive": {"operator": {"install": True}}}}}


@pytest.mark.parametrize(
    ("extra", "names"),
    [
        (_SPARK_OP, "lakebench admin install-spark-operator"),
        (_HIVE_OP, "docs/component-hive.md"),
    ],
)
@pytest.mark.parametrize("purpose", [LoadPurpose.RUN, LoadPurpose.MUTATE])
def test_operator_install_true_refused_names_admin_install(tmp_path, extra, names, purpose):
    path = _write(tmp_path, extra)
    with pytest.raises(ConfigValidationError) as e:
        load_config(path, purpose=purpose, print_notes=False)
    assert "install: true' is refused" in str(e.value)
    assert names in str(e.value)


@pytest.mark.parametrize("value", ["true", "yes", 1, "on"])
def test_operator_install_coerced_true_refused(tmp_path, value):
    # Pydantic reads these as True; the refusal runs after that coercion,
    # so none of them reaches deploy's install branch.
    for extra in (
        {"platform": {"compute": {"spark": {"operator": {"install": value}}}}},
        {"architecture": {"catalog": {"hive": {"operator": {"install": value}}}}},
    ):
        path = _write(tmp_path, extra)
        with pytest.raises(ConfigValidationError, match="install: true' is refused"):
            load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)


@pytest.mark.parametrize("extra", [_SPARK_OP, _HIVE_OP])
def test_operator_install_true_loads_false_for_teardown(tmp_path, extra):
    path = _write(tmp_path, extra)
    cfg = load_config(path, purpose=LoadPurpose.TEARDOWN, print_notes=False)
    assert cfg.platform.compute.spark.operator.install is False
    assert cfg.architecture.catalog.hive.operator.install is False
    assert "install: true' is ignored" in " ".join(load_notes(cfg).texts())


def test_operator_install_false_loads_quietly(tmp_path):
    path = _write(
        tmp_path,
        {
            "platform": {"compute": {"spark": {"operator": {"install": False}}}},
            "architecture": {"catalog": {"hive": {"operator": {"install": False}}}},
        },
    )
    cfg = load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    assert not [t for t in load_notes(cfg).texts() if "install" in t]


# -- run-only benchmark refusals ----------------------------------------------


@pytest.mark.parametrize(
    ("bench", "needle"),
    [
        ({"cache": "cold"}, "lakebench benchmark --cold"),
        ({"mode": "throughput"}, "lakebench benchmark --mode"),
        ({"mode": "composite"}, "lakebench benchmark --mode"),
        ({"streams": 4}, "lakebench benchmark --streams"),
    ],
    ids=["cold", "throughput", "composite", "streams"],
)
def test_run_refuses_what_it_does_not_run(tmp_path, bench, needle):
    path = _write(tmp_path, {"architecture": {"benchmark": bench}})
    with pytest.raises(ConfigValidationError) as e:
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    assert needle in str(e.value)
    # `lakebench benchmark` (MUTATE) honours all of them.
    cfg = load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)
    assert cfg.architecture.benchmark.model_dump(include=set(bench)) == bench


def test_run_refuses_cold_cache(tmp_path):
    path = _write(tmp_path, {"architecture": {"benchmark": {"cache": "cold"}}})
    with pytest.raises(ConfigValidationError, match="benchmark.cache cold"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)


def test_run_refuses_throughput(tmp_path):
    path = _write(tmp_path, {"architecture": {"benchmark": {"mode": "throughput"}}})
    with pytest.raises(ConfigValidationError, match="benchmark.mode throughput"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)


def test_run_refuses_explicit_streams(tmp_path):
    # An explicit value above 1 is refused for every workload and mode; the
    # schema default (4, used by `lakebench benchmark`) is not.
    for schema, mode in (("customer360", "batch"), ("financial", "continuous")):
        path = _write(
            tmp_path,
            {
                "architecture": {
                    "benchmark": {"streams": 2},
                    "workload": {"schema": schema},
                    "pipeline": {"mode": mode},
                },
            },
        )
        with pytest.raises(ConfigValidationError, match="benchmark.streams 2"):
            load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    # A benchmark block without streams keeps the default 4 unset: allowed.
    unset = _write(tmp_path, {"architecture": {"benchmark": {"iterations": 3}}})
    cfg = load_config(unset, purpose=LoadPurpose.RUN, print_notes=False)
    assert cfg.architecture.benchmark.streams == 4
    one = _write(tmp_path, {"architecture": {"benchmark": {"streams": 1}}})
    load_config(one, purpose=LoadPurpose.RUN, print_notes=False)


def test_saved_config_loads_for_run(tmp_path):
    # save_config writes every field; the default streams count would then
    # read as set and run would refuse its own saved config.
    from lakebench.config.loader import save_config

    cfg = load_config(_write(tmp_path, {}), purpose=LoadPurpose.MUTATE, print_notes=False)
    out = tmp_path / "saved.yaml"
    save_config(cfg, out)
    assert "streams" not in yaml.safe_load(out.read_text())["architecture"]["benchmark"]
    load_config(out, purpose=LoadPurpose.RUN, print_notes=False)
    # An explicit value is kept.
    set_cfg = load_config(
        _write(tmp_path, {"architecture": {"benchmark": {"streams": 8}}}),
        purpose=LoadPurpose.MUTATE,
        print_notes=False,
    )
    save_config(set_cfg, out)
    assert yaml.safe_load(out.read_text())["architecture"]["benchmark"]["streams"] == 8


@pytest.mark.parametrize("mode", ["standard", "extended", "power"])
def test_run_accepts_power_modes(tmp_path, mode):
    path = _write(tmp_path, {"architecture": {"benchmark": {"mode": mode}}})
    load_config(path, purpose=LoadPurpose.RUN, print_notes=False)


def test_run_snapshot_records_power_hot_1(tmp_path):
    from lakebench.metrics.collector import build_config_snapshot

    path = _write(
        tmp_path,
        {"architecture": {"benchmark": {"mode": "extended", "iterations": 5}}},
    )
    cfg = load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
    bench = build_config_snapshot(cfg, run_mode="batch")["benchmark"]
    assert {k: bench[k] for k in ("mode", "streams", "cache", "iterations")} == {
        "mode": "power",
        "streams": 1,
        "cache": "hot",
        "iterations": 5,
    }
    # A batch config run with --continuous records the in-stream rounds.
    cont = build_config_snapshot(cfg, run_mode="continuous")["benchmark"]
    assert cont == {"mode": "power", "streams": 1, "cache": "hot", "iterations": 1}


# -- LB-230: errors never echo their input ------------------------------------

_SEED = 4343434343


def _all_error_text(exc: BaseException) -> str:
    """Every text a caller prints: the message, the repr and, for the
    loader's error, the error dicts the CLI iterates."""
    parts = [str(exc), repr(exc)]
    errors = getattr(exc, "errors", None)
    if isinstance(errors, list):
        parts.append(repr(errors))
    return "\n".join(parts)


@pytest.mark.parametrize(
    "bad",
    [
        # A list where the workload block belongs: the error's input is the
        # whole list, seed included.
        {"architecture": {"workload": [{"datagen": {"seed": _SEED}}]}},
        # A model-level failure on the architecture block (an unsupported
        # combination), whose input is every architecture key.
        {
            "architecture": {
                "workload": {"datagen": {"seed": _SEED}},
                "catalog": {"type": "unity"},
                "table_format": {"type": "iceberg"},
            }
        },
    ],
    ids=["type-error", "model-validator"],
)
def test_validation_error_never_echoes_a_seed(tmp_path, bad):
    data = {**_BASE, **bad}
    with pytest.raises(ValidationError) as e:
        LakebenchConfig.model_validate(data)
    assert str(_SEED) not in _all_error_text(e.value)

    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump(data))
    with pytest.raises(ConfigValidationError) as ce:
        load_config(path, purpose=LoadPurpose.READ, print_notes=False)
    assert str(_SEED) not in _all_error_text(ce.value)
    for err in ce.value.errors:
        assert "input" not in err
    # The pydantic error (which keeps its input) is not chained on.
    assert ce.value.__cause__ is None and ce.value.__suppress_context__
