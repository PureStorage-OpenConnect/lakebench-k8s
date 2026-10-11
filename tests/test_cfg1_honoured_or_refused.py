"""Config settings that nothing honours are refused on commands that change data and tolerated on teardown and read."""

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
_SPARK_OP = {"platform": {"compute": {"spark": {"operator": {"install": True}}}}}
_HIVE_OP = {"architecture": {"catalog": {"hive": {"operator": {"install": True}}}}}


# -- removed sizing blocks -----------------------------------------------------


@pytest.mark.parametrize("purpose", [LoadPurpose.RUN, LoadPurpose.MUTATE])
@pytest.mark.parametrize(
    ("extra", "loc"),
    [
        (_EXECUTOR, ("platform", "compute", "spark")),
        (_DRIVER, ("platform", "compute", "spark")),
        (_SPARK_OP, ("platform", "compute", "spark", "operator")),
        (_HIVE_OP, ("architecture", "catalog", "hive", "operator")),
    ],
)
def test_unhonoured_setting_refused_on_data_changing_commands(tmp_path, extra, loc, purpose):
    with pytest.raises(ConfigValidationError) as e:
        load_config(_write(tmp_path, extra), purpose=purpose, print_notes=False)
    assert [err["loc"] for err in e.value.errors] == [loc]


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
    assert "'executor'" in notes
    assert "'size'" in notes
    assert not hasattr(cfg.platform.compute.spark, "executor")
    assert not hasattr(cfg.platform.storage.scratch, "size")


def test_removed_scratch_size_refused_on_run(tmp_path):
    path = _write(tmp_path, _SCRATCH)
    with pytest.raises(ConfigValidationError, match="'size' was removed: per-job scratch"):
        load_config(path, purpose=LoadPurpose.RUN, print_notes=False)


# -- operator install keys (C24) ----------------------------------------------


def test_operator_install_coerced_true_refused(tmp_path):
    for value in ["true", "yes", 1, "on"]:
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


# -- run-only benchmark refusals ----------------------------------------------


def test_run_refuses_what_it_does_not_run(tmp_path):
    for bench, needle in [
        ({"cache": "cold"}, "lakebench benchmark --cold"),
        ({"mode": "throughput"}, "lakebench benchmark --mode"),
        ({"mode": "composite"}, "lakebench benchmark --mode"),
        ({"streams": 4}, "lakebench benchmark --streams"),
    ]:
        path = _write(tmp_path, {"architecture": {"benchmark": bench}})
        with pytest.raises(ConfigValidationError) as e:
            load_config(path, purpose=LoadPurpose.RUN, print_notes=False)
        assert needle in str(e.value)
        # `lakebench benchmark` (MUTATE) honours all of them.
        cfg = load_config(path, purpose=LoadPurpose.MUTATE, print_notes=False)
        assert cfg.architecture.benchmark.model_dump(include=set(bench)) == bench


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


# -- Errors never echo their input ------------------------------------

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
