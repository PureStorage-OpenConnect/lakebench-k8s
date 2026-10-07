"""Shared test helpers moved from tests/test_reproduce.py (imported by several test files)."""

from __future__ import annotations

from types import SimpleNamespace

from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID
from tests.conftest import stub_experiment

# The fixture packages carry one sample per query, so the verify config must
# ask for one or the sample-count check refuses before anything else runs.
_ONE_SAMPLE_CFG = "name: x\narchitecture:\n  benchmark:\n    iterations: 1\n"


def _stage(name: str, elapsed: float) -> SimpleNamespace:
    return SimpleNamespace(stage_name=name, elapsed_seconds=elapsed)


def _pb(**overrides):
    """Build a PipelineBenchmark-shaped SimpleNamespace with sensible defaults."""
    defaults = {
        "pipeline_mode": "batch",
        "time_to_value_seconds": 405.0,
        "pipeline_throughput_gb_per_second": 0.030,
        "compute_efficiency_gb_per_core_hour": 1.2,
        "scale_ratio": 0.992,
        "data_freshness_seconds": None,
        "sustained_throughput_rps": 0.0,
        "ingest_ratio": 0.0,
        "post_compaction_qph": 0.0,
        "query_benchmark": SimpleNamespace(qph=1305.8),
        "stages": [
            _stage("bronze-verify", 240.0),
            _stage("silver-build", 90.0),
            _stage("gold-finalize", 75.0),
        ],
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _metrics(**overrides):
    defaults = {
        "run_id": "20260920-210120-9d4b92",
        "deployment_name": "c360-scale-0-1",
        "pipeline_benchmark": _pb(),
        "config_snapshot": {"name": "c360-scale-0-1", "scale": 0.1},
        # A run from this code carries the current policy (PipelineMetrics default).
        "maintenance_policy_id": MAINTENANCE_POLICY_ID,
        "experiment": stub_experiment(["Q1"]),
        "datagen_fleet": {
            "pods_reported": 2,
            "aggregate_mbps": 154.4,
            "cpu_hr_per_tb": 14.36,
            "data_quality": "complete",
        },
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)
