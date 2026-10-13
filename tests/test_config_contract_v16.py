"""v1.6 config contract (design-contradictions #4, #7, #12, #13, #18, #19,
outcome 5).

Each test here fails with its fix reverted: AML on Delta and the Java/Iceberg
pairing used to load, `custom` used to run the Customer 360 queries, the
workload block lived only under architecture, `sustained` was the canonical
mode, dead fields loaded silently, a benchmark exception left the run
successful, and bucket names were fixed and global.
"""

from __future__ import annotations

import ast
import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import load_config
from lakebench.config.loader import ConfigValidationError, save_config
from lakebench.config.schema import (
    PipelineMode,
    is_continuous_mode,
)
from tests.conftest import make_config

ROOT = Path(__file__).resolve().parents[1]


def _write(tmp_path, data) -> Path:
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(data))
    return p


def _quiet(fn, *a, **kw):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        return fn(*a, **kw)


# -- #4: workload x format compatibility, Java/Iceberg pairing ---------------


def test_aml_on_delta_is_refused_on_the_architecture():
    with pytest.raises(ValidationError) as exc:
        make_config(recipe="hive-delta-spark-trino", workload={"schema": "financial"})
    assert [e["loc"] for e in exc.value.errors()] == [("architecture",)]
    make_config(recipe="hive-iceberg-spark-trino", workload={"schema": "financial"})


def test_iceberg_111_on_java11_spark_image_is_refused_at_load():
    with pytest.raises(ValueError, match="requires Java 17"):
        make_config(
            images={"spark": "apache/spark:3.5.4-python3"},
            architecture={"table_format": {"iceberg": {"version": "1.11.0"}}},
        )


# -- #13: custom is refused ---------------------------------------------------


def test_custom_workload_is_refused(tmp_path):
    with pytest.raises(ConfigValidationError) as exc:
        load_config(_write(tmp_path, {"name": "t", "workload": {"schema": "custom"}}))
    assert [e["loc"] for e in exc.value.errors] == [("workload", "schema")]


# -- #12: top-level workload --------------------------------------------------


def test_top_level_workload_is_canonical(tmp_path):
    cfg = _quiet(
        load_config,
        _write(
            tmp_path,
            {
                "name": "t",
                "recipe": "hive-iceberg-spark-trino",
                "workload": {"schema": "financial"},
            },
        ),
    )
    assert cfg.workload.schema_type.value == "financial"
    assert cfg.architecture.workload is cfg.workload
    # The financial table defaults still follow the workload.
    assert cfg.architecture.tables.silver == "silver.transactions"


def test_architecture_workload_warns_and_names_new_location(tmp_path):
    with pytest.warns(DeprecationWarning, match="top-level 'workload:' key"):
        cfg = load_config(
            _write(tmp_path, {"name": "t", "architecture": {"workload": {"schema": "financial"}}})
        )
    assert cfg.workload.schema_type.value == "financial"


def test_both_locations_that_agree_are_merged(tmp_path):
    data = {
        "name": "t",
        "workload": {"schema": "financial"},
        "architecture": {"workload": {"schema": "financial", "datagen": {"scale": 3}}},
    }
    with pytest.warns(DeprecationWarning):
        cfg = load_config(_write(tmp_path, data))
    assert cfg.workload.schema_type.value == "financial"
    assert cfg.workload.datagen.scale == 3


def test_both_locations_that_disagree_are_refused(tmp_path):
    data = {
        "name": "t",
        "workload": {"datagen": {"scale": 5}},
        "architecture": {"workload": {"datagen": {"scale": 3}}},
    }
    with pytest.raises(ConfigValidationError, match="workload.datagen.scale: 5 at the top level"):
        load_config(_write(tmp_path, data))


def test_flat_scale_lands_in_the_top_level_block(tmp_path):
    # Flat keys are deprecated in v1.7 (CFG-4 notes): still promoted, with a note.
    with pytest.warns(DeprecationWarning, match="flat 'scale' is deprecated"):
        cfg = load_config(
            _write(tmp_path, {"name": "t", "scale": 7, "workload": {"schema": "customer360"}})
        )
    assert cfg.workload.datagen.scale == 7


def test_flat_scale_with_legacy_block_does_not_create_a_conflict(tmp_path):
    data = {"name": "t", "scale": 7, "architecture": {"workload": {"schema": "financial"}}}
    with pytest.warns(DeprecationWarning):
        cfg = load_config(_write(tmp_path, data))
    assert cfg.workload.datagen.scale == 7
    assert cfg.workload.schema_type.value == "financial"


def test_saved_config_reloads_without_deprecations(tmp_path):
    cfg = make_config(
        workload={"schema": "financial"},
        architecture={"pipeline": {"mode": "continuous"}},
    )
    path = tmp_path / "saved.yaml"
    save_config(cfg, path)
    data = yaml.safe_load(path.read_text())
    assert "workload" in data and "workload" not in data["architecture"]
    assert "continuous" in data["architecture"]["pipeline"]
    again = _quiet(load_config, path)
    assert again.workload.schema_type.value == "financial"
    assert again.architecture.pipeline.mode == PipelineMode.CONTINUOUS


# -- #18: continuous is canonical ---------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("continuous", True),
        ("sustained", True),  # metrics files still record this
        (PipelineMode.CONTINUOUS, True),
        ("batch", False),
        (None, False),
    ],
)
def test_is_continuous_mode(value, expected):
    assert is_continuous_mode(value) is expected


def test_old_metrics_with_sustained_mode_still_read_as_continuous(tmp_path):
    from lakebench.reports.generator import ReportGenerator

    pb = SimpleNamespace(pipeline_mode="sustained")
    metrics = SimpleNamespace(pipeline_benchmark=pb, streaming=[])
    gen = ReportGenerator.__new__(ReportGenerator)
    assert gen._is_sustained(metrics) is True


# -- Outcome 5: bucket names ----------------------------------------------


@pytest.mark.parametrize(
    ("buckets", "expected"),
    [
        pytest.param(None, ("lab-a-bronze", "lab-a-silver", "lab-a-gold"), id="derived"),
        pytest.param(
            {"bronze": "shared-bronze"},
            ("shared-bronze", "lab-a-silver", "lab-a-gold"),
            id="explicit-wins",
        ),
    ],
)
def test_buckets_derive_from_the_deployment_name_unless_explicit(buckets, expected):
    s3 = {"endpoint": "http://minio:9000", "access_key": "k", "secret_key": "s"}
    if buckets:
        s3["buckets"] = buckets
    cfg = make_config(name="lab-a", platform={"storage": {"s3": s3}})
    b = cfg.platform.storage.s3.buckets
    assert (b.bronze, b.silver, b.gold) == expected


# -- #7: a benchmark exception fails the run ----------------------------------


def _benchmark_except_handler() -> ast.ExceptHandler:
    src = (ROOT / "src/lakebench/cli/_run.py").read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and "Benchmark did not complete" in ast.unparse(
            node
        ):
            return node
    raise AssertionError("benchmark exception handler not found")


def test_benchmark_exception_fails_the_run_and_withholds_qph():
    body = ast.unparse(_benchmark_except_handler())
    assert "pipeline_success = False" in body
    assert "benchmark_qph = None" in body
    assert "collector.current_run.benchmark = None" in body
    assert "benchmark_error" in body
    assert "success=False" in body  # journal event


def test_benchmark_error_round_trips_and_reaches_the_report(tmp_path):
    from datetime import datetime

    from lakebench.metrics.collector import PipelineMetrics
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.generator import ReportGenerator

    run = PipelineMetrics(
        run_id="r1",
        deployment_name="d",
        start_time=datetime.now(),
        success=False,
        benchmark_error="RuntimeError: trino down",
    )
    assert run.to_dict()["benchmark_error"] == "RuntimeError: trino down"
    storage = MetricsStorage(tmp_path)
    storage.save_run(run)
    loaded = storage.load_run("r1")
    assert loaded.benchmark_error == "RuntimeError: trino down"
    gen = ReportGenerator.__new__(ReportGenerator)
    ok, reasons, _ = gen._compute_overall_status(loaded)
    assert not ok
    assert any("Benchmark did not complete" in r for r in reasons)


@pytest.mark.parametrize(
    ("value", "ok"),
    [
        ("0 seconds", True),
        ("5 minutes", True),
        ("1 hour", True),
        ("0", False),
        ("1.5 minutes", False),
    ],
)
def test_trigger_intervals_need_a_whole_number_and_a_unit(value, ok):
    """A bare "0" went to Spark unchanged for Customer360 and became a 10 s
    trigger for AML: an interval is refused at load unless both read it."""
    for key in ("bronze_trigger_interval", "silver_trigger_interval", "gold_refresh_interval"):
        cfg = {"architecture": {"pipeline": {"continuous": {key: value}}}}
        if ok:
            make_config(**cfg)
        else:
            with pytest.raises(ValueError):
                make_config(**cfg)
