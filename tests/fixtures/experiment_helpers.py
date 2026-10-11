"""Config and metrics builders for the experiment and comparability tests."""

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
    # A completed run with rows in every layer so its verdict computes PASSED.
    run.success = True
    run.jobs = [
        JobMetrics(job_name=f"lakebench-{s}", job_type=s, success=True, output_rows=100)
        for s in ("bronze-verify", "silver-build", "gold-finalize")
    ]
    return run
