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
    stage_profile,
    streaming_executor_count,
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


def _gold_tick_s(cores: int, rows_m: float) -> float:
    per_m_rows_s = (_GOLD_TICK_S - _GOLD_FIXED_S) / _GOLD_MEAN_ROWS_M
    return _GOLD_FIXED_S + per_m_rows_s * rows_m * _GOLD_CORES / cores


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


def test_counts_scale_and_respect_the_cap():
    for scale, silver, gold in [
        (1, 10, 12),
        (10, 10, 12),
        (20, 10, 24),
        (50, 13, 28),
        (100, 17, 28),
        (1000, 28, 28),
    ]:
        assert get_executor_count("silver-stream", scale, "financial") == silver
        assert get_executor_count("gold-refresh", scale, "financial") == gold
        assert silver <= _MAX_EXECUTORS_SAFE and gold <= _MAX_EXECUTORS_SAFE


def _continuous_financial(scale: float):
    cfg = _config("financial", scale)
    cfg.architecture.pipeline.mode = "continuous"
    return cfg


def test_silver_cap_memory_ceiling_floors_at_the_schema_default():
    """The memory-ceiling bound never drops below the schema default, so a run
    left at the default is never silently re-capped, and the floor constant
    tracks the schema field default (the "moves no existing run" guarantee)."""
    from lakebench.config.schema import SustainedConfig
    from lakebench.config.sizing import (
        SILVER_CAP_FLOOR,
        _financial_silver_cap_memory_ceiling,
    )

    assert SILVER_CAP_FLOOR == SustainedConfig.model_fields["silver_max_rows_per_trigger"].default
    for scale in (0.1, 1, 10, 48, 100, 1000):
        assert (
            _financial_silver_cap_memory_ceiling(_continuous_financial(scale)) >= SILVER_CAP_FLOOR
        )


def test_silver_cap_implies_a_safe_per_executor_load():
    """The ceiling is sized against the BASE silver-stream geometry (4 cores /
    32g, the ``streaming_executor_count`` within the 28 cap) as a conservative
    proxy: the per-executor row load it implies on that 32g geometry stays below
    the one on-cluster point that ran without OOM (a 113.3M-row batch on 28
    executors at 32g, ~4.05M rows per executor). The stage actually reshapes to
    larger 64g executors at scale (measured 8 cores / 64g), where that same cap
    is well under the ~2x higher load a 64g executor allows, so the
    base-geometry sizing is conservative. The implied load is set by the heap
    fractions, not the count,
    so it is uniform across scale and one measured point bounds every scale. A
    margin is kept for run-to-run noise and the unmeasured true peak."""
    from lakebench.config.sizing import _financial_silver_cap_memory_ceiling as ceiling
    from lakebench.modules.pipeline_engines.spark.job import (
        _resolve_job_profile,
        streaming_executor_count,
    )

    profile = _resolve_job_profile("silver-stream", "financial")
    proven_safe_rows_per_exec = 113_304_204 / 28
    loads = []
    for scale in (10, 48, 100, 300):
        cfg = _continuous_financial(scale)
        execs = streaming_executor_count("silver-stream", profile, cfg, scale=scale)
        loads.append(ceiling(cfg) / execs)
    # Uniform per-executor load across scale (set by the fractions, not count).
    assert max(loads) - min(loads) < 1.0
    # Safely under the proven-safe point, with margin (not fit to it).
    assert loads[0] < proven_safe_rows_per_exec * 0.85


def test_financial_bronze_bytes_per_row_matches_the_measured_band():
    """Rows and bronze bytes are both linear in scale, so bytes/row is near
    constant: the measured 8.47/9.36 GiB-per-unit corpus over 26.7M rows/unit,
    read with the binary GiB convention the scale module uses."""
    from lakebench.config.sizing import _financial_bronze_bytes_per_row as bpr

    assert bpr(1) == pytest.approx(341, abs=2)
    assert bpr(10) == pytest.approx(377, abs=2)
    # Flat outside the two measured points and monotone between them.
    assert bpr(0.5) == pytest.approx(bpr(1), abs=1)
    assert bpr(100) == pytest.approx(bpr(10), abs=1)
    assert bpr(1) <= bpr(5) <= bpr(10)


def _resolved_silver_cap(schema: str, scale: float, mode: str = "sustained", cap=...):
    """``silver_max_rows_per_trigger`` after ``resolve_auto_sizing``; *cap*
    left as ``...`` means the user did not set one."""
    from lakebench.config.autosizer import resolve_auto_sizing

    cfg = _config(schema, scale)
    cfg.architecture.pipeline.mode = mode
    if cap is not ...:
        cfg.architecture.pipeline.sustained.silver_max_rows_per_trigger = cap
    resolve_auto_sizing(cfg)
    return cfg.architecture.pipeline.sustained.silver_max_rows_per_trigger


def test_autosizer_derives_the_silver_cap_for_financial_continuous():
    """A defaulted financial continuous run gets the memory-ceiling cap, above
    the 20M floor, so it rises with the executor envelope."""
    from lakebench.config.sizing import _financial_silver_cap_memory_ceiling

    for scale in (10, 48, 100):
        assert _resolved_silver_cap("financial", scale) == _financial_silver_cap_memory_ceiling(
            _continuous_financial(scale)
        )
        assert _resolved_silver_cap("financial", scale) > 20_000_000


def test_autosizer_keeps_an_explicit_silver_cap():
    """An explicitly configured cap always wins over the derivation."""
    assert _resolved_silver_cap("financial", 48, cap=80_000_000) == 80_000_000
    # Even an explicit value below the derived ceiling is honoured.
    assert _resolved_silver_cap("financial", 48, cap=5_000_000) == 5_000_000


def test_autosizer_leaves_the_silver_cap_at_default_off_the_financial_path():
    """customer360 continuous and batch financial keep the flat 20M default:
    only financial continuous has the world-sized MERGE the derivation targets,
    so no other run's recorded cap moves."""
    assert _resolved_silver_cap("customer360", 48) == 20_000_000
    assert _resolved_silver_cap("financial", 48, mode="batch") == 20_000_000


def _advisories(schema: str, scale: float, mode: str = "sustained", dur: int = 1800, cap=...):
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.config.sizing import _silver_cap_advisories

    cfg = _config(schema, scale)
    cfg.architecture.pipeline.mode = mode
    cfg.architecture.pipeline.sustained.run_duration = dur
    if cap is not ...:
        cfg.architecture.pipeline.sustained.silver_max_rows_per_trigger = cap
    resolve_auto_sizing(cfg)
    return _silver_cap_advisories(cfg)


def test_silver_cap_advisory_describes_a_raised_cap():
    """A financial continuous run whose cap was raised gets an advisory naming
    the derived cap and the amortize-vs-commit-cadence trade-off."""
    from lakebench.config.sizing import _financial_silver_cap_memory_ceiling

    adv = _advisories("financial", 48)
    assert any("rows per micro-batch" in a for a in adv)
    # The advisory prints the actual derived cap value, formatted with commas.
    derived = f"{_financial_silver_cap_memory_ceiling(_continuous_financial(48)):,}"
    assert any(derived in a for a in adv)


def test_silver_cap_advisory_warns_only_on_a_short_window():
    """The window/commit confirmation line fires below the 1800 s default, not
    at or above it; it never claims an estimated commit count (no in-repo
    batch-duration model)."""
    short = _advisories("financial", 48, dur=900)
    default = _advisories("financial", 48, dur=1800)
    assert any("2 silver commits" in a for a in short)
    assert not any("silver commits" in a for a in default)
    for a in short + default:
        assert "estimated" not in a.lower()


def test_silver_cap_advisory_silent_off_the_financial_path_and_at_the_floor():
    """No advisory for customer360, batch, or a cap left at/below the floor:
    the advisory only speaks when the cap was actually raised."""
    assert _advisories("customer360", 48) == []
    assert _advisories("financial", 48, mode="batch") == []
    assert _advisories("financial", 48, cap=20_000_000) == []
    assert _advisories("financial", 48, cap=5_000_000) == []


def test_silver_cap_advisory_does_not_trip_the_datagen_queue_tag():
    """The '(datagen pods queue)' tag in recommend fires on a warning starting
    'datagen:'; the silver-cap advisory must not start that way, or a raised-cap
    financial run would be mislabelled as queuing datagen pods."""
    for a in _advisories("financial", 48, dur=900):
        assert not a.startswith("datagen:")


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


def test_budget_properties_over_the_core_and_scale_grid():
    """Over scales 1..1000 and 20..697 cores: the total stays within the
    budget (or one executor per stage, when that alone exceeds it); no whole
    executor is left unspent while a stage still wants one; and no stage
    holds a larger share of what it wants than another would with one more
    executor, when that executor could have moved, so the stage that binds is
    not starved for a larger one."""
    for scale in (1, 10, 50, 100, 200, 500, 1000):
        cfg = _config("financial", scale)
        shape = {j: stage_profile(j.value, cfg) for j in _STAGES}
        cpe = {j: shape[j]["executor_cores"] for j in _STAGES}
        want = {j: streaming_executor_count(j.value, shape[j], cfg) for j in _STAGES}
        for cores in range(20, 700, 3):
            at = f"scale={scale} cores={cores}"
            got = _streaming_concurrent_budget(cfg, cores * 1000)
            budget = _budget_cores(cfg, cores)
            total = sum(got[j] * cpe[j] for j in _STAGES)
            assert all(1 <= got[j] <= want[j] for j in _STAGES), at
            assert total <= max(budget, sum(cpe.values())), at
            wanting = [cpe[j] for j in _STAGES if got[j] < want[j]]
            if wanting:
                assert budget - total < min(wanting), at
            for low in _STAGES:
                for high in _STAGES:
                    # Moving one executor from high to low was possible.
                    movable = cpe[low] <= budget - total + cpe[high]
                    if got[low] < want[low] and got[high] > 1 and movable:
                        assert (got[high] - 1) / want[high] <= got[low] / want[low], at


def _capacity_k8s(cores):
    k8s = MagicMock()
    k8s.get_cluster_capacity.return_value = ClusterCapacity(
        total_cpu_millicores=cores * 1000,
        total_memory_bytes=8 * 432 * 1024**3,
        node_count=8,
        largest_node_cpu_millicores=cores * 1000 // 8,
        largest_node_memory_bytes=432 * 1024**3,
    )
    _free_from_total(k8s)
    return k8s


@pytest.mark.parametrize(
    "job", [JobType.BRONZE_INGEST, JobType.SILVER_STREAM, JobType.GOLD_REFRESH]
)
def test_executor_cores_set_in_config_are_deployed_exactly(job):
    """Outcome 6: ``*_executor_cores: 8`` deploys 8-core executors with the
    profile's memory per core and keeps the scale's executor count: the
    size changes, not the count, on a cluster with room."""
    from lakebench.config.schema import parse_spark_memory

    plain = SparkJobManager(_config("financial", 10), _capacity_k8s(434))._build_manifest(job)
    cfg = _config("financial", 10)
    setattr(cfg.platform.compute.spark, f"{job.value.split('-')[0]}_executor_cores", 8)
    ex = SparkJobManager(cfg, _capacity_k8s(434))._build_manifest(job)["spec"]["executor"]
    base = get_job_profile(job.value, "financial")
    per_core = {
        k: parse_spark_memory(base[k]) / base["executor_cores"]
        for k in ("executor_memory", "executor_memory_overhead")
    }
    assert ex["cores"] == 8
    assert parse_spark_memory(ex["memory"]) == 8 * per_core["executor_memory"]
    assert parse_spark_memory(ex["memoryOverhead"]) == 8 * per_core["executor_memory_overhead"]
    assert ex["instances"] == plain["spec"]["executor"]["instances"]


@pytest.mark.parametrize(
    "job", [JobType.BRONZE_INGEST, JobType.SILVER_STREAM, JobType.GOLD_REFRESH]
)
def test_executor_memory_set_in_config_is_deployed(job):
    """``*_executor_memory`` deploys that heap per executor, with the overhead
    at the profile's ratio to the heap; the cores and, on a cluster with room
    for the larger pods, the count are unchanged."""
    from lakebench.config.schema import parse_spark_memory

    plain = SparkJobManager(_config("financial", 10), _capacity_k8s(434))._build_manifest(job)
    base = plain["spec"]["executor"]
    gib = 1024**3
    heap_g = parse_spark_memory(base["memory"]) * 3 // 2 // gib
    cfg = _config("financial", 10)
    setattr(cfg.platform.compute.spark, f"{job.value.split('-')[0]}_executor_memory", f"{heap_g}g")
    ex = SparkJobManager(cfg, _capacity_k8s(434))._build_manifest(job)["spec"]["executor"]
    ratio = parse_spark_memory(base["memoryOverhead"]) / parse_spark_memory(base["memory"])
    assert parse_spark_memory(ex["memory"]) == heap_g * gib
    assert parse_spark_memory(ex["memoryOverhead"]) == pytest.approx(heap_g * gib * ratio, abs=gib)
    assert ex["cores"] == base["cores"]
    assert ex["instances"] == base["instances"]


def test_executor_memory_is_counted_in_the_peak_and_ignored_by_batch_jobs():
    """The larger pod is what the capacity plan counts; a batch job keeps its
    profile's memory, as with the cores key."""
    from lakebench.config.schema import parse_spark_memory

    cfg = _continuous_financial(10)
    before = compute_peak_requirements(10, "continuous", "financial", config=cfg)
    cfg.platform.compute.spark.silver_executor_memory = "96g"
    after = compute_peak_requirements(10, "continuous", "financial", config=cfg)
    assert after.memory_gb > before.memory_gb
    assert after.max_pod_memory_gb >= 96
    built = stage_profile("silver-build", cfg)
    assert parse_spark_memory(built["executor_memory"]) == parse_spark_memory(
        get_job_profile("silver-build", "financial")["executor_memory"]
    )


def test_executor_memory_that_is_not_a_spark_size_is_refused():
    with pytest.raises(ValueError, match="silver_executor_memory"):
        _config("financial", 10).platform.compute.spark.__class__(silver_executor_memory="lots")


class TestPreflightBetweenOldAndNewMinimum:
    """A cluster between the capped AML continuous request and the full one
    runs degraded with a warning naming the capped stages, rather than
    failing preflight.

    The preflight sizes the config as ``run`` does (CC-22: auto-sizing on a
    copy against the cluster), so datagen is the cores the scale's offered
    load needs, even though this test's config is not resolved in place.
    """

    GIB = 1024**3

    def _check(self, cores, memory_gb=4000, node_cores=64, node_gb=256, schema="financial"):
        from unittest import mock

        from lakebench.cli._prerequisites import _check_cluster_capacity

        cap = ClusterCapacity(
            cores * 1000, memory_gb * self.GIB, 8, node_cores * 1000, node_gb * self.GIB
        )
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            _free_from_total(get_client.return_value)
            return _check_cluster_capacity(_config(schema, 10))

    def _min_cores(self, schema="financial", **kw):
        """The smallest cluster preflight admits (the check is monotonic)."""
        return next(c for c in range(1, 1000) if self._check(c, schema=schema, **kw).passed)

    def test_runs_degraded_below_the_planned_floor(self):
        """Below the floor the plan prints, preflight admits the run degraded
        (capped streams, with a warning) instead of refusing it; at the floor
        it admits it clean, so the plan and the preflight agree."""
        from lakebench.config.sizing import plan_requirements

        floor = plan_requirements(_config("financial", 10)).floor.cpu_cores
        low = self._min_cores()
        assert low < floor
        for cores in (low, (low + floor) // 2, floor - 1):
            r = self._check(cores)
            assert r.passed and r.message.startswith("WARNING"), cores
        assert self._check(floor).passed and not self._check(floor).message.startswith("WARNING")

    def test_an_explicit_count_is_counted_uncapped(self):
        """The manifest applies gold_executors after the budget."""
        from unittest import mock

        from lakebench.cli._prerequisites import _check_cluster_capacity

        cfg = _config("financial", 10)
        cfg.platform.compute.spark.gold_executors = 28
        cap = ClusterCapacity(80_000, 4000 * self.GIB, 8, 64_000, 256 * self.GIB)
        with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
            get_client.return_value.get_cluster_capacity.return_value = cap
            _free_from_total(get_client.return_value)
            assert not _check_cluster_capacity(cfg).passed

    def test_a_driver_override_is_counted(self):
        from unittest import mock

        from lakebench.cli._prerequisites import _check_cluster_capacity

        low = self._min_cores()

        def run(driver_cores):
            cfg = _config("financial", 10)
            cfg.platform.compute.spark.driver_cores = driver_cores
            cap = ClusterCapacity(low * 1000, 4000 * self.GIB, 8, 64_000, 256 * self.GIB)
            with mock.patch("lakebench.k8s.get_k8s_client") as get_client:
                get_client.return_value.get_cluster_capacity.return_value = cap
                _free_from_total(get_client.return_value)
                return _check_cluster_capacity(cfg).passed

        assert run(None)
        assert not run(8)

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
        assert not self._check(80, memory_gb=160).passed

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
            _free_from_total(get_client.return_value)
            assert not _check_cluster_capacity(cfg).passed


# -- driver pod overhead ---------------------------------------------


@pytest.mark.parametrize(
    ("heap", "mib"),
    [("32g", 32768 + 13107), ("8g", 8192 + 3276), ("2g", 2048 + 819), ("512m", 512 + 384)],
)
def test_driver_pod_counts_spark_non_jvm_overhead(heap, mib):
    """Spark on Kubernetes gives a Python driver max(0.4 x heap, 384 MiB) of
    overhead when the manifest sets none (BasicDriverFeatureStep)."""
    from lakebench.modules.pipeline_engines.spark.job import _driver_pod_bytes

    assert _driver_pod_bytes(heap) == mib * 1024**2


@pytest.mark.parametrize("schema", ["financial", "customer360"])
def test_manifest_sets_no_driver_overhead(schema):
    # The count above holds only while the manifest leaves the overhead to Spark.
    cfg = _config(schema, 1)
    for jt in _STAGES + (JobType.BRONZE_VERIFY, JobType.SILVER_BUILD, JobType.GOLD_FINALIZE):
        m = SparkJobManager(cfg, _capacity_k8s(434))._build_manifest(jt)
        assert m["spec"]["type"] == "Python"
        assert "memoryOverheadFactor" not in m["spec"]
        assert "memoryOverhead" not in m["spec"]["driver"]
        conf = m["spec"]["sparkConf"]
        assert not [k for k in conf if "memoryOverhead" in k and "driver" in k]
        assert "spark.kubernetes.memoryOverheadFactor" not in conf


@pytest.mark.parametrize("schema", ["financial", "customer360"])
def test_continuous_budget_counts_driver_overhead(schema, monkeypatch):
    from lakebench.config.schema import parse_spark_memory
    from lakebench.modules.pipeline_engines.spark import job as job_mod

    cfg = _config(schema, 1)
    with_overhead = job_mod.streaming_request_under_budget(cfg, 10_000_000).memory_gb
    monkeypatch.setattr(job_mod, "_driver_pod_bytes", parse_spark_memory)
    heap_only = job_mod.streaming_request_under_budget(cfg, 10_000_000).memory_gb
    overhead = (
        sum(
            max(
                int(
                    0.4
                    * parse_spark_memory(get_job_profile(jt.value, schema)["driver_memory"])
                    / 2**20
                ),
                384,
            )
            for jt in _STAGES
        )
        * 2**20
    )
    assert with_overhead - heap_only in (overhead // 1024**3, -(-overhead // 1024**3))
    assert with_overhead > heap_only


def test_peak_rounds_driver_overhead_up():
    # silver-build at scale 1: 8 x 60 GiB executors + a 44.8 GiB driver pod.
    peak = compute_peak_requirements(1, "batch")
    sb = next(r for r in peak.per_job if r.job_type == "silver-build")
    assert sb.memory_gb == 525  # 524.8 GiB, never rounded down


def _free_from_total(k8s_mock):
    """The preflight reads free capacity: make the mock report the capacity
    its get_cluster_capacity returns as both free and allocatable (one node
    with the largest node's resources free), with no published scratch."""
    from lakebench.k8s.client import FreeCapacity, ScratchCapacity

    def _free(**_kw):
        cap = k8s_mock.get_cluster_capacity.return_value
        node = (cap.largest_node_cpu_millicores, cap.largest_node_memory_bytes)
        return FreeCapacity(free=cap, allocatable=cap, free_by_node=(node,))

    k8s_mock.get_free_capacity.side_effect = _free
    k8s_mock.get_scratch_capacity.return_value = ScratchCapacity(None, "none published (test)")
