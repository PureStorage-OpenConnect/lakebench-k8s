"""Shared test helpers moved from tests/test_experiment.py (imported by several test files)."""

from __future__ import annotations

from lakebench.benchmark.fingerprint import fingerprint_rows
from lakebench.metrics.collector import (
    BenchmarkMetrics,
    JobMetrics,
    MetricsCollector,
    build_config_snapshot,
)
from tests.conftest import make_config


def _cfg(schema="customer360", mode="batch", engine="trino", fmt="iceberg", **datagen):
    return make_config(
        architecture={
            "workload": {"schema": schema, "datagen": {"scale": 1, **datagen}},
            "pipeline": {"mode": mode},
            "query_engine": {"type": engine},
            "table_format": {"type": fmt},
        }
    )


def _fp(value: int = 1) -> dict:
    return fingerprint_rows([(value, "x")], engine="trino", adapted_sql="SELECT 1")


def _metrics(cfg, fingerprints: dict | None = None, fleet: dict | None = None):
    run = MetricsCollector().start_run(
        "20260926-120000-aaaaaa", cfg.name, build_config_snapshot(cfg)
    )
    fps = fingerprints if fingerprints is not None else {"Q1_full_aggregation_scan": _fp()}
    run.benchmark = BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1,
        qph=100.0,
        total_seconds=10.0,
        queries=[
            {"name": n, "elapsed_seconds": 1.0, "success": True, "result_fingerprint": f}
            for n, f in fps.items()
        ],
    )
    run.datagen_fleet = fleet
    # A2b wiring: compare and perf_gate now refuse a run whose verdict is
    # FAILED. PipelineMetrics defaults success to False (the "run in
    # progress" shape), so a synthetic collector object without an
    # end_run(success=True) call would compute a FAILED verdict and be
    # refused by compare. These tests build a synthetic completed run to
    # exercise the comparability ladder itself, not to test a failed run;
    # mark it complete so the verdict computes PASSED. Every layer has rows,
    # so the verdict's layer_rows gate (EVD-1) passes too.
    run.success = True
    run.jobs = [
        JobMetrics(job_name=f"lakebench-{s}", job_type=s, success=True, output_rows=100)
        for s in ("bronze-verify", "silver-build", "gold-finalize")
    ]
    return run
