"""Tests for configuration loading and validation.

AML on Delta and the Java/Iceberg pairing are refused, `custom` is refused,
the workload block is top level, `continuous` is the canonical mode, a
benchmark exception fails the run, and bucket names follow the deployment.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError

from lakebench.config import LakebenchConfig, load_config
from lakebench.config.loader import ConfigValidationError, save_config
from lakebench.config.schema import PipelineMode, is_continuous_mode
from tests.conftest import make_config


class TestComponentValidation:
    """Tests for component combination validation."""

    @pytest.mark.parametrize(
        ("catalog", "fmt", "loc"),
        [
            ("unity", "iceberg", ("architecture",)),
            ("polaris", "delta", ("architecture",)),
            ("hive", "hudi", ("architecture", "table_format", "type")),
        ],
    )
    def test_unsupported_combination_rejected(self, catalog, fmt, loc):
        with pytest.raises(ValidationError) as exc:
            LakebenchConfig(
                name="test",
                architecture={
                    "catalog": {"type": catalog},
                    "table_format": {"type": fmt},
                    "query_engine": {"type": "trino"},
                },
            )
        assert [e["loc"] for e in exc.value.errors()] == [loc]


class TestSustainedBenchmarkConfig:
    """Tests for benchmark_interval and benchmark_warmup on SustainedConfig."""

    @pytest.mark.parametrize(
        ("gold_refresh", "settings", "field", "expected"),
        [
            ("10 minutes", {"benchmark_warmup": 300}, "benchmark_warmup", 600),
            ("5 minutes", {"benchmark_warmup": 300}, "benchmark_warmup", 300),
            ("10 minutes", {"benchmark_interval": 300}, "benchmark_interval", 600),
            (
                "5 minutes",
                {"benchmark_interval": 300, "benchmark_warmup": 300},
                "benchmark_interval",
                300,
            ),
        ],
    )
    def test_warmup_and_interval_never_below_gold_refresh(
        self, gold_refresh, settings, field, expected
    ):
        """The measurement window must not start before gold refreshes."""
        config = LakebenchConfig(
            name="test",
            architecture={
                "processing": {"sustained": {"gold_refresh_interval": gold_refresh, **settings}}
            },
        )
        assert getattr(config.architecture.pipeline.sustained, field) == expected


def _write(tmp_path, data) -> Path:
    p = tmp_path / "c.yaml"
    p.write_text(yaml.safe_dump(data))
    return p


def _quiet(fn, *a, **kw):
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        return fn(*a, **kw)


# -- workload x format compatibility, Java/Iceberg pairing ------------------


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


# -- custom is refused ------------------------------------------------------


def test_custom_workload_is_refused(tmp_path):
    with pytest.raises(ConfigValidationError) as exc:
        load_config(_write(tmp_path, {"name": "t", "workload": {"schema": "custom"}}))
    assert [e["loc"] for e in exc.value.errors] == [("workload", "schema")]


# -- top-level workload -----------------------------------------------------


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
    # Flat keys are deprecated in v1.7: still promoted, with a note.
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


# -- continuous is canonical ------------------------------------------------


def test_is_continuous_mode_reads_sustained():
    # Metrics files still record "sustained".
    assert is_continuous_mode("sustained") is True


def test_old_metrics_with_sustained_mode_still_read_as_continuous(tmp_path):
    from lakebench.reports.generator import ReportGenerator

    pb = SimpleNamespace(pipeline_mode="sustained")
    metrics = SimpleNamespace(pipeline_benchmark=pb, streaming=[])
    gen = ReportGenerator.__new__(ReportGenerator)
    assert gen._is_sustained(metrics) is True


# -- bucket names -----------------------------------------------------------


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


# -- a benchmark exception fails the run ------------------------------------


def test_benchmark_exception_fails_the_run_and_withholds_qph(tmp_path, monkeypatch):
    """The real batch ``run`` through the run harness, its one timed
    benchmark raising: the run fails, no QpH is recorded and the journal
    says why."""
    import dataclasses
    import json

    from tests.harness import run_harness

    def broken(self, *args, **kwargs):
        self._rec.add("Benchmark", "run_power", "raised")
        raise RuntimeError("trino down")

    monkeypatch.setattr(run_harness.FakeBenchmark, "run_power", broken)
    base = run_harness.SCENARIOS["batch_c360"]
    # No maintenance, so no pre-compaction round: the timed benchmark is
    # the only run_power call.
    scenario = dataclasses.replace(base, argv=[*base.argv, "--skip-maintenance"])
    trace, rec = run_harness.run_scenario_full(scenario, tmp_path, monkeypatch)
    record = run_harness.saved_record(tmp_path)

    assert trace["unscripted"] == []
    assert ["Benchmark", "run_power", "raised"] in rec.calls
    assert trace["exit_code"] != 0
    assert record["success"] is False
    assert record.get("benchmark") is None
    assert record["benchmark_error"] == "RuntimeError: trino down"
    assert record["verdict"]["status"] != "PASSED"
    assert any("Benchmark did not complete" in r for r in record["verdict"]["reasons"])
    assert record["pipeline_benchmark"]["scores"].get("composite_qph") is None

    events = [
        json.loads(line)
        for f in (tmp_path / "lakebench-output" / "journal").glob("*.jsonl")
        for line in f.read_text().splitlines()
    ]
    done = [e for e in events if e["event_type"] == "benchmark.complete"]
    assert len(done) == 1
    assert done[0]["success"] is False
    assert done[0]["details"]["qph"] is None


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


@pytest.mark.parametrize(("value", "ok"), [("5 minutes", True), ("0", False)])
def test_trigger_intervals_need_a_whole_number_and_a_unit(value, ok):
    """A bare "0" went to Spark unchanged for Customer360 and became a 10 s
    trigger for AML: an interval is refused at load unless both read it."""
    cfg = {"architecture": {"pipeline": {"continuous": {"bronze_trigger_interval": value}}}}
    if ok:
        make_config(**cfg)
    else:
        with pytest.raises(ValueError):
            make_config(**cfg)
