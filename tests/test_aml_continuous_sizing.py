"""AML continuous silver-stream and gold-refresh profile overrides.

run-20260925-135005-4b7a97 (AML, continuous, scale 10, 1800 s): ingest 100%,
freshness 1,271 s, time to detect p50 1,280 s. silver-stream on the base
4 executors x 4 cores ran 4 micro-batches of 66.7M rows at 299 s each against
a 60 s trigger; gold-refresh on the base 2 x 4 cores ran 4 ticks at 349.5 s
against a 300 s refresh interval, with path searches ~80% of a tick.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from lakebench.config import LakebenchConfig
from lakebench.k8s.client import ClusterCapacity
from lakebench.modules.pipeline_engines.spark.job import (
    _JOB_PROFILES,
    _MAX_EXECUTORS_SAFE,
    _streaming_concurrent_budget,
    compute_peak_requirements,
    get_executor_count,
    get_job_profile,
)
from lakebench.spark.job import JobType, SparkJobManager

# Silver, measured: rows per batch, seconds per batch, cores.
_SILVER_ROWS, _SILVER_S, _SILVER_CORES = 66_666_600, 299.1, 16
_SILVER_TRIGGER_S = 60
# Bronze intake while the scale-10 corpus drains: 9,523,800 rows per 30 s
# trigger (the bronze batch, 28.7 s, fits inside it).
_BRONZE_RPS = 9_523_800 / 30
# Gold, measured: mean tick seconds on 8 cores; share of a tick in path
# searches, taken as the only part that scales with cores.
_GOLD_TICK_S, _GOLD_CORES, _GOLD_PARALLEL = 349.5, 8, 0.8
_GOLD_REFRESH_S = 300

_STAGES = (JobType.BRONZE_INGEST, JobType.SILVER_STREAM, JobType.GOLD_REFRESH)


def _config(schema: str, scale: float) -> LakebenchConfig:
    return LakebenchConfig(
        name="t",
        platform={
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "a",
                    "secret_key": "b",
                    "buckets": {"bronze": "b", "silver": "s", "gold": "g"},
                }
            }
        },
        architecture={
            "workload": {"schema": schema, "datagen": {"scale": scale}},
            "pipeline": {"mode": "sustained"},
        },
    )


def _cores(job: str, scale: float = 10) -> int:
    return (
        get_executor_count(job, scale, "financial")
        * get_job_profile(job, "financial")["executor_cores"]
    )


def test_silver_batch_fits_its_trigger_at_scale_10():
    """A caught-up batch holds one trigger of bronze intake. At the measured
    per-core rate (which already folds in per-batch overhead) it must leave
    at least a third of the trigger for fixed per-batch cost."""
    per_core_rps = _SILVER_ROWS / _SILVER_S / _SILVER_CORES
    batch_rows = _BRONZE_RPS * _SILVER_TRIGGER_S
    work_s = batch_rows / (_cores("silver-stream") * per_core_rps)
    assert work_s <= _SILVER_TRIGGER_S * 2 / 3
    # Silver outruns bronze with margin, so a backlog cannot build.
    assert _cores("silver-stream") * per_core_rps >= 1.5 * _BRONZE_RPS


def test_the_base_silver_profile_could_not_keep_up():
    """The measurement the override answers: 16 cores fall behind bronze."""
    per_core_rps = _SILVER_ROWS / _SILVER_S / _SILVER_CORES
    base = _JOB_PROFILES["silver-stream"]
    assert base["base_executors"] * base["executor_cores"] * per_core_rps < _BRONZE_RPS


def test_gold_tick_fits_the_refresh_interval_at_scale_10():
    ratio = _GOLD_CORES / _cores("gold-refresh")
    tick_s = _GOLD_TICK_S * ((1 - _GOLD_PARALLEL) + _GOLD_PARALLEL * ratio)
    assert tick_s < _GOLD_REFRESH_S * 0.6


def test_per_executor_sizing_is_the_base():
    for job in ("silver-stream", "gold-refresh"):
        aml, base = get_job_profile(job, "financial"), _JOB_PROFILES[job]
        for field in ("executor_cores", "executor_memory", "executor_memory_overhead"):
            assert aml[field] == base[field], (job, field)
        assert aml["scratch_size"] == base["scratch_size"]


def test_c360_is_unchanged():
    assert get_executor_count("silver-stream", 10, "customer360") == 4
    assert get_executor_count("gold-refresh", 10, "customer360") == 2


@pytest.mark.parametrize(
    ("scale", "silver", "gold"),
    [(1, 10, 6), (10, 10, 6), (50, 13, 9), (100, 17, 13), (1000, 28, 28)],
)
def test_counts_scale_and_respect_the_cap(scale, silver, gold):
    assert get_executor_count("silver-stream", scale, "financial") == silver
    assert get_executor_count("gold-refresh", scale, "financial") == gold
    assert silver <= _MAX_EXECUTORS_SAFE and gold <= _MAX_EXECUTORS_SAFE


@pytest.mark.parametrize(
    ("scale", "cores", "memory"),
    [(1, 94, 740), (10, 94, 740), (100, 162, 1348)],
)
def test_peak_requirements(scale, cores, memory):
    """Gotcha 34: the preflight and the docs read compute_peak_requirements."""
    peak = compute_peak_requirements(scale, "sustained", "financial")
    assert (peak.cpu_cores, peak.memory_gb) == (cores, memory)


# Budget split per cluster size: (bronze, silver, gold) executors.
_BUDGET = [
    (1, 60, (1, 6, 2)),
    (1, 80, (2, 8, 3)),
    (1, 100, (4, 9, 5)),
    (1, 150, (5, 10, 6)),
    (1, 434, (5, 10, 6)),
    (10, 60, (1, 6, 2)),
    (10, 80, (2, 8, 3)),
    (10, 100, (4, 9, 5)),
    (10, 150, (5, 10, 6)),
    (10, 434, (5, 10, 6)),
    (100, 60, (1, 6, 2)),
    (100, 80, (1, 8, 4)),
    (100, 100, (2, 10, 6)),
    (100, 150, (5, 14, 10)),
    (100, 434, (8, 17, 13)),
]


def _budget_cores(cfg, cores):
    from lakebench.config.autosizer import _parse_cpu_millicores

    trino = cfg.architecture.query_engine.trino
    co = (
        _parse_cpu_millicores(trino.coordinator.cpu)
        + trino.worker.replicas * _parse_cpu_millicores(trino.worker.cpu)
        + 1000
    )
    dg = cfg.architecture.workload.datagen
    dg_m = dg.parallelism * _parse_cpu_millicores(dg.cpu)
    return int(max(0, cores * 1000 - co - dg_m) * 0.9) // 1000


@pytest.mark.parametrize(("scale", "cores", "expected"), _BUDGET)
def test_budget_split(scale, cores, expected):
    cfg = _config("financial", scale)
    got = _streaming_concurrent_budget(cfg, cores * 1000)
    assert tuple(got[j] for j in _STAGES) == expected
    base = _streaming_concurrent_budget(_config("customer360", scale), cores * 1000)
    base_total = sum(base[j] * _JOB_PROFILES[j.value]["executor_cores"] for j in _STAGES)
    total = sum(got[j] * 4 for j in _STAGES)
    assert total <= max(_budget_cores(cfg, cores), base_total)
    for j in _STAGES:
        floor = max(1, base[j] * _JOB_PROFILES[j.value]["executor_cores"] // 4)
        assert floor <= got[j] <= get_executor_count(j.value, scale, "financial")


@pytest.mark.parametrize("scale", [1, 10, 50, 100, 200, 500, 1000])
@pytest.mark.parametrize("cores", range(20, 700, 3))
def test_budget_never_overspends_and_uses_what_fits(scale, cores):
    """Total within the budget (or the old split, when floors alone exceed
    it); and no whole 4-core executor left unspent while a stage still
    wants one."""
    cfg = _config("financial", scale)
    got = _streaming_concurrent_budget(cfg, cores * 1000)
    base = _streaming_concurrent_budget(_config("customer360", scale), cores * 1000)
    base_total = sum(base[j] * _JOB_PROFILES[j.value]["executor_cores"] for j in _STAGES)
    budget = _budget_cores(cfg, cores)
    total = sum(got[j] * 4 for j in _STAGES)
    assert total <= max(budget, base_total)
    wants = any(got[j] < get_executor_count(j.value, scale, "financial") for j in _STAGES)
    if wants:
        assert budget - total < 4


def _capacity_k8s(cores):
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        total_cpu_millicores=cores * 1000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=cores * 1000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    return k8s


@pytest.mark.parametrize(
    ("job", "instances"), [(JobType.SILVER_STREAM, 10), (JobType.GOLD_REFRESH, 6)]
)
def test_manifest_deploys_the_override(job, instances):
    mgr = SparkJobManager(_config("financial", 10), _capacity_k8s(434))
    ex = mgr._build_manifest(job)["spec"]["executor"]
    assert ex["instances"] == instances
    assert ex["cores"] == 4
    assert mgr.budget_warnings == []
