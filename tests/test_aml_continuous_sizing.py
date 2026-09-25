"""AML continuous silver-stream and gold-refresh profile overrides.

run-20260925-135005-4b7a97 (AML, continuous, scale 10, 1800 s): ingest 100%,
freshness 1,271 s, time to detect p50 1,280 s. silver-stream on the base
4 executors x 4 cores ran 4 micro-batches of 66.7M rows at 299 s each against
a 60 s trigger; gold-refresh on the base 2 x 4 cores ran 4 ticks at 349.5 s
against a 300 s refresh interval, reading 58.4M silver rows on average (silver
was lagging; the corpus is 266.7M).
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
# Gold, measured: mean tick seconds on 8 cores over a mean silver of 58.4M
# rows; fixed per-tick cost from lane U's local ticks (84 s at 3.4M rows,
# 122 s at 6.7M); the full scale-10 silver once it keeps up.
_GOLD_TICK_S, _GOLD_CORES, _GOLD_MEAN_ROWS_M = 349.5, 8, 58.4
_GOLD_FIXED_S = 45
_SILVER_FULL_ROWS_M_S10 = 266.7
_GOLD_REFRESH_S = 300

_KEEP_UP = get_job_profile("silver-stream", "financial")["keep_up_executors"]

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


def _gold_tick_s(cores: int, rows_m: float) -> float:
    per_m_rows_s = (_GOLD_TICK_S - _GOLD_FIXED_S) / _GOLD_MEAN_ROWS_M
    return _GOLD_FIXED_S + per_m_rows_s * rows_m * _GOLD_CORES / cores


def test_the_local_ticks_give_the_fixed_cost():
    slope = (122 - 84) / (6.7 - 3.4)
    assert abs((84 - slope * 3.4) - _GOLD_FIXED_S) < 3


def test_a_full_silver_gold_tick_fits_the_refresh_interval_at_scale_10():
    """Ticks recompute all of silver, so size for the whole corpus, not the
    lagging silver the measured ticks read."""
    assert _gold_tick_s(_cores("gold-refresh"), _SILVER_FULL_ROWS_M_S10) < _GOLD_REFRESH_S
    # One executor fewer would not: the count is the smallest that fits.
    fewer = _cores("gold-refresh") - get_job_profile("gold-refresh", "financial")["executor_cores"]
    assert _gold_tick_s(fewer, _SILVER_FULL_ROWS_M_S10) >= _GOLD_REFRESH_S - 25


@pytest.mark.parametrize(("job", "partitions"), [("silver-stream", "80"), ("gold-refresh", "96")])
def test_shuffle_partitions_cover_the_cores_at_scale_10(job, partitions):
    """The base 32 partitions would idle cores in every shuffle stage."""
    from lakebench.modules.pipeline_engines.spark.job import _scale_partitions

    prof = get_job_profile(job, "financial")
    n = get_executor_count(job, 10, "financial")
    got = _scale_partitions(prof, 10, n, prof["executor_cores"])
    assert got == partitions
    assert int(got) >= 2 * n * prof["executor_cores"]


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
    [(1, 10, 12), (10, 10, 12), (20, 10, 24), (50, 13, 28), (100, 17, 28), (1000, 28, 28)],
)
def test_counts_scale_and_respect_the_cap(scale, silver, gold):
    assert get_executor_count("silver-stream", scale, "financial") == silver
    assert get_executor_count("gold-refresh", scale, "financial") == gold
    assert silver <= _MAX_EXECUTORS_SAFE and gold <= _MAX_EXECUTORS_SAFE


@pytest.mark.parametrize(
    ("scale", "cores", "memory"),
    [(1, 118, 980), (10, 118, 980), (100, 222, 1948)],
)
def test_peak_requirements(scale, cores, memory):
    """Gotcha 34: the preflight and the docs read compute_peak_requirements."""
    peak = compute_peak_requirements(scale, "sustained", "financial")
    assert (peak.cpu_cores, peak.memory_gb) == (cores, memory)


# Budget split per cluster size: (bronze, silver, gold) executors.
_BUDGET = [
    (1, 60, (1, 4, 2)),
    (1, 80, (5, 4, 2)),
    (1, 100, (5, 7, 3)),
    (1, 150, (5, 10, 12)),
    (1, 434, (5, 10, 12)),
    (10, 60, (1, 4, 2)),
    (10, 80, (5, 4, 2)),
    (10, 100, (5, 7, 3)),
    (10, 150, (5, 10, 12)),
    (10, 434, (5, 10, 12)),
    (100, 60, (1, 5, 2)),
    (100, 80, (1, 8, 3)),
    (100, 100, (2, 10, 4)),
    (100, 150, (8, 11, 8)),
    (100, 434, (8, 17, 28)),
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
    # The overridden stages share the budget net of the three drivers
    # (2 + 4 + 4 cores under the AML profiles).
    return int(max(0, cores * 1000 - co - dg_m - 10_000) * 0.9) // 1000


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


@pytest.mark.parametrize("scale", [1, 10, 50, 100, 500])
@pytest.mark.parametrize("cores", range(20, 700, 3))
def test_spare_cores_go_upstream_first(scale, cores):
    """A stage runs no faster than its input: gold gets cores above its
    floor only once bronze is full and silver is at its keep-up count, and
    silver goes past keep-up only once bronze and gold are full."""
    got = _streaming_concurrent_budget(_config("financial", scale), cores * 1000)
    base = _streaming_concurrent_budget(_config("customer360", scale), cores * 1000)
    want = {j: get_executor_count(j.value, scale, "financial") for j in _STAGES}
    floor = {j: max(1, base[j] * _JOB_PROFILES[j.value]["executor_cores"] // 4) for j in _STAGES}
    keep_up = min(want[JobType.SILVER_STREAM], _KEEP_UP)
    if got[JobType.SILVER_STREAM] > floor[JobType.SILVER_STREAM]:
        assert got[JobType.BRONZE_INGEST] == want[JobType.BRONZE_INGEST]
    if got[JobType.GOLD_REFRESH] > floor[JobType.GOLD_REFRESH]:
        assert got[JobType.BRONZE_INGEST] == want[JobType.BRONZE_INGEST]
        assert got[JobType.SILVER_STREAM] >= keep_up
    if got[JobType.SILVER_STREAM] > max(floor[JobType.SILVER_STREAM], keep_up):
        assert got[JobType.GOLD_REFRESH] == want[JobType.GOLD_REFRESH]


def test_silver_keep_up_count_holds_bronze_intake():
    per_core_rps = _SILVER_ROWS / _SILVER_S / _SILVER_CORES
    assert _KEEP_UP * 4 * per_core_rps >= 1.2 * _BRONZE_RPS
    assert (_KEEP_UP - 1) * 4 * per_core_rps < 1.2 * _BRONZE_RPS


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
    ("job", "instances"), [(JobType.SILVER_STREAM, 10), (JobType.GOLD_REFRESH, 12)]
)
def test_manifest_deploys_the_override(job, instances):
    mgr = SparkJobManager(_config("financial", 10), _capacity_k8s(434))
    ex = mgr._build_manifest(job)["spec"]["executor"]
    assert ex["instances"] == instances
    assert ex["cores"] == 4
    assert mgr.budget_warnings == []


class TestPreflightBetweenOldAndNewMinimum:
    """A cluster between the old AML continuous minimum (54 cores) and the
    new one (118) runs degraded with a warning naming the capped stages,
    rather than failing preflight."""

    GIB = 1024**3

    def _check(self, cores, memory_gb=4000, node_cores=64, node_gb=256):
        from unittest import mock

        from lakebench.cli._prerequisites import _check_cluster_capacity

        cap = ClusterCapacity(
            cores * 1000, memory_gb * self.GIB, 8, node_cores * 1000, node_gb * self.GIB
        )
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            return _check_cluster_capacity(_config("financial", 10))

    @pytest.mark.parametrize("cores", [60, 80, 100, 117])
    def test_runs_degraded_with_a_warning(self, cores):
        r = self._check(cores)
        assert r.passed
        assert r.message.startswith("WARNING")
        assert "silver-stream" in r.message or "gold-refresh" in r.message

    def test_the_old_minimum_fails_once_trino_and_datagen_are_counted(self):
        """At 54 cores the capped streams plus Trino, Hive/Postgres and
        datagen need 57: the run would hang Pending, so preflight fails."""
        assert not self._check(54).passed
        assert self._check(57).passed

    @pytest.mark.parametrize("cores", [30, 37])
    def test_c360_below_the_capped_request_still_fails(self, cores):
        from unittest import mock

        from lakebench.cli._prerequisites import _check_cluster_capacity

        cap = ClusterCapacity(cores * 1000, 4000 * self.GIB, 8, 64_000, 256 * self.GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            assert not _check_cluster_capacity(_config("customer360", 10)).passed

    def test_an_explicit_count_is_counted_uncapped(self):
        """The manifest applies gold_refresh_executors after the budget."""
        from unittest import mock

        from lakebench.cli._prerequisites import _check_cluster_capacity

        cfg = _config("financial", 10)
        cfg.platform.compute.spark.gold_refresh_executors = 28
        cap = ClusterCapacity(80_000, 4000 * self.GIB, 8, 64_000, 256 * self.GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            assert not _check_cluster_capacity(cfg).passed

    def test_a_failing_estimate_keeps_the_hard_failure(self):
        from unittest import mock

        with mock.patch(
            "lakebench.modules.pipeline_engines.spark.job.streaming_request_under_budget",
            side_effect=RuntimeError("boom"),
        ):
            assert not self._check(80).passed

    def test_full_cluster_is_not_warned(self):
        r = self._check(434)
        assert r.passed and not r.message.startswith("WARNING")

    def test_too_small_even_capped_still_fails(self):
        assert not self._check(20).passed

    def test_memory_short_even_capped_still_fails(self):
        assert not self._check(80, memory_gb=200).passed

    def test_a_pod_that_fits_no_node_still_fails(self):
        assert not self._check(100, node_gb=30).passed

    def test_batch_mode_is_unchanged(self):
        from unittest import mock

        from lakebench.cli._prerequisites import _check_cluster_capacity

        cfg = _config("financial", 10)
        cfg.architecture.pipeline.mode = "batch"
        cap = ClusterCapacity(20_000, 4000 * self.GIB, 8, 64_000, 256 * self.GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            assert not _check_cluster_capacity(cfg).passed
