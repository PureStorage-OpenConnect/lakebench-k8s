"""Performance-regression gate: pinned configs, baseline store, compare.

Every test builds real metrics.json files on disk from the pinned configs'
own snapshots and runs the gate over them; nothing is mocked.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml

from lakebench.metrics import perf_gate as pg
from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID
from tests.conftest import stub_experiment

ROOT = Path(__file__).resolve().parents[1]
PERF = ROOT / "benchmarks" / "perf"


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def _snapshot(config: Path) -> dict:
    """The config_snapshot a faithful run of *config* records."""
    import os

    from lakebench.config import load_config
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.metrics.collector import build_config_snapshot

    env = {k: v for k, v in pg._PLACEHOLDER_ENV.items() if not os.environ.get(k)}
    os.environ.update(env)
    try:
        cfg = load_config(config)
        resolve_auto_sizing(cfg, None)
        # A run records the sha256 of the file it loaded.
        return build_config_snapshot(cfg, config_path=config)
    finally:
        for k in env:
            os.environ.pop(k, None)


QUERIES = {"Q1_scan": 4.0, "Q2_filter": 3.0, "Q3_join": 6.0}

# The dependency pinset every synthetic run records (provenance.deps); the
# gate refuses a run on another set and records no baseline without one.
PINSET = "a1" * 32


def _provenance(pinset: str | None = PINSET) -> dict:
    return {"deps": {"pinset_sha256": pinset}} if pinset else {}


def _batch_run(snapshot: dict, run_id: str, **over) -> dict:
    ttv = over.get("ttv", 600.0)
    qph = over.get("qph", 400.0)
    stage_s = over.get(
        "stage_s", {"datagen": 120.0, "bronze": 100.0, "silver": 200.0, "gold": 150.0}
    )
    executors = over.get("executors", {"bronze": 4, "silver": 8, "gold": 4})
    q_scale = over.get("query_scale", 1.0)
    # Batch pinned configs score the median of 3 samples per query (LB-150).
    n_samples = over.get("samples", 3)
    queries = [
        {
            "name": n,
            "elapsed_seconds": s * q_scale,
            "success": n not in over.get("failed", ()),
            **({"samples": [s * q_scale] * n_samples} if n_samples else {}),
        }
        for n, s in QUERIES.items()
    ]
    scores = {
        "time_to_value_seconds": ttv,
        "pipeline_throughput_gb_per_second": over.get("gbps", 0.2),
        "compute_efficiency_gb_per_core_hour": over.get("gbch", 5.0),
        "composite_qph": qph,
        "scale_ratio": over.get("scale_ratio", 1.02),
    }
    if "maintenance_value_pct" in over:
        scores["pre_compaction_qph"] = 300.0
        scores["post_compaction_qph"] = 330.0
        scores["maintenance_value_pct"] = over["maintenance_value_pct"]
    stages = [
        {
            "stage_name": name,
            "stage_type": "datagen" if name == "datagen" else "batch",
            "elapsed_seconds": secs,
            "success": True,
            "executor_count": executors.get(name, 0),
        }
        for name, secs in stage_s.items()
    ]
    if over.get("timed", True):
        # Pipeline stages run back to back from 10:00 and the last one ends
        # at 10:00 + ttv, as the collector records them. The datagen stage
        # carries no timestamps (collector.build_pipeline_benchmark).
        t0 = datetime(2026, 9, 24, 10, 0, 0)
        cursor = t0
        timed = [st for st in stages if st["stage_name"] != "datagen"]
        for st in timed:
            st["start_time"] = cursor.isoformat()
            cursor += timedelta(seconds=st["elapsed_seconds"])
            st["end_time"] = cursor.isoformat()
        if timed:
            last_end = max(cursor, t0 + timedelta(seconds=ttv))
            timed[-1]["end_time"] = last_end.isoformat()
    pods = over.get("pods", 4)
    return {
        "run_id": run_id,
        "deployment_name": snapshot["name"],
        "start_time": "2026-09-24T10:00:00",
        "success": over.get("success", True),
        "config_snapshot": snapshot,
        "experiment": over.get(
            "experiment", stub_experiment(QUERIES, failed=over.get("failed", ()))
        ),
        "maintenance_policy_id": over.get("policy", MAINTENANCE_POLICY_ID),
        "provenance": _provenance(over.get("pinset", PINSET)),
        "pipeline_benchmark": {
            "run_id": run_id,
            "pipeline_mode": "batch",
            "start_time": "2026-09-24T10:00:00",
            "success": True,
            "scorecard": scores,
            "stages": stages,
            "config_snapshot": snapshot,
            "query_benchmark": {"mode": "power", "qph": qph, "queries": queries},
        },
        "datagen_fleet": {
            "pods_expected": pods,
            "pods_reported": pods,
            "data_quality": "complete",
            "aggregate_mbps": over.get("dg_mbps", 2000.0),
            "cpu_hr_per_tb": 6.0,
            # The collector takes the datagen stage's seconds from here when
            # the run did not generate itself.
            "wall_elapsed_max_s": stage_s.get("datagen", 0.0),
            "per_pod": [{"throughput_mbps": over.get("dg_mbps", 2000.0) / pods}] * pods,
        },
    }


def _cont_run(
    snapshot: dict,
    run_id: str,
    *,
    rps: float,
    drained: bool,
    window: float = 1800.0,
    ingest: float | None = None,
    executors: tuple[int, int, int] = (2, 4, 2),
) -> dict:
    scores = {
        "data_freshness_seconds": 120.0,
        "sustained_throughput_rps": rps,
        "ingest_ratio": ingest if ingest is not None else (1.0 if drained else 0.97),
        "corpus_drained": drained,
        "compute_efficiency_gb_per_core_hour": 3.0,
        "composite_qph": None,
    }
    # Continuous runs name their stages bronze/silver/gold (collector
    # _STREAMING_MAP), each lasting the whole window.
    stages = [
        {"stage_name": n, "stage_type": "streaming", "elapsed_seconds": window, "executor_count": c}
        for n, c in zip(("bronze", "silver", "gold"), executors, strict=True)
    ]
    return {
        "run_id": run_id,
        "deployment_name": snapshot["name"],
        "start_time": "2026-09-24T10:00:00",
        "success": True,
        "config_snapshot": snapshot,
        # A continuous run with its end-of-run result check (a settled corpus).
        "experiment": stub_experiment(QUERIES, mode="sustained"),
        "maintenance_policy_id": MAINTENANCE_POLICY_ID,
        "provenance": _provenance(),
        "pipeline_benchmark": {
            "run_id": run_id,
            "pipeline_mode": "sustained",
            "start_time": "2026-09-24T10:00:00",
            "success": True,
            "scorecard": scores,
            "stages": stages,
            "config_snapshot": snapshot,
        },
    }


@pytest.fixture
def env(tmp_path):
    """A store with the repo's pinned configs copied in, and a runs dir."""
    store_dir = tmp_path / "perf"
    store_dir.mkdir()
    for f in PERF.glob("*.yaml"):
        shutil.copy(f, store_dir / f.name)
    # Start every test from pending baselines, whatever the checked-in store
    # has accepted since, so recording is exercised from scratch.
    store = yaml.safe_load((store_dir / "baselines.yaml").read_text())
    for name, entry in store["baselines"].items():
        for key in [k for k in entry if k not in ("config", "required", "notes")]:
            del entry[key]
        entry["status"] = "pending first run"
        # The gate-logic tests need required and optional configs whatever the
        # checked-in store currently requires.
        entry["required"] = name in ("c360-batch-s10", "c360-continuous-s10")
    (store_dir / "baselines.yaml").write_text(yaml.safe_dump(store, sort_keys=False))
    runs = tmp_path / "runs"
    runs.mkdir()
    snaps = {
        "c360-batch-s10": _snapshot(store_dir / "c360-batch-s10.yaml"),
        "c360-continuous-s10": _snapshot(store_dir / "c360-continuous-s10.yaml"),
    }

    def write_run(data: dict) -> Path:
        d = runs / f"run-{data['run_id']}"
        d.mkdir()
        (d / "metrics.json").write_text(json.dumps(data))
        return d / "metrics.json"

    class Env:
        pass

    e = Env()
    e.store_path = store_dir / "baselines.yaml"
    e.store_dir = store_dir
    e.runs = runs
    e.snaps = snaps
    e.write_run = write_run
    e.store = lambda: pg.load_store(e.store_path)
    return e


def _record(env, name: str, data: dict):
    store = env.store()
    run = pg.load_run(env.write_run(data))
    pg.record_baseline(store, name, run, "abc1234")
    store.save()
    return env.store()


def _compare(env, name: str, data: dict) -> pg.Comparison:
    return pg.compare_run(env.store(), name, pg.load_run(env.write_run(data)))


def _row(c: pg.Comparison, metric: str) -> pg.Row:
    return next(r for r in c.rows if r.metric == metric)


# -- the four named behaviours ----------------------------------------------


def test_regression_detected(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", ttv=690.0))
    assert c.verdict == pg.REGRESSION
    assert _row(c, "time_to_value_seconds").status == "regression"
    assert round(_row(c, "time_to_value_seconds").drift_pct, 1) == 15.0


def test_different_query_results_are_refused_not_gated(env):
    """Invariant 1: a run whose benchmark returned other results than the
    baseline's is not a performance comparison at all."""
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    other = stub_experiment(QUERIES)
    from lakebench.benchmark.fingerprint import fingerprint_rows

    other["results"]["fingerprints"]["Q2_filter"] = fingerprint_rows([("Q2_filter", 2)])
    c = _compare(
        env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", experiment=other)
    )
    assert c.verdict == pg.REFUSED
    assert any("Q2_filter results not shown equal" in r for r in c.reasons), c.reasons


def test_different_experiment_or_conditions_refused(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    for over, field in (({"seed": 7}, "seed"), ({"maintenance": "x"}, "effective maintenance")):
        run_id = f"20260924-11000{len(field) % 10}-bbbbbb"
        run = _batch_run(snap, run_id, experiment=stub_experiment(QUERIES, **over))
        c = _compare(env, "c360-batch-s10", run)
        assert c.verdict == pg.REFUSED and any(r.startswith(field) for r in c.reasons), c.reasons


def test_config_hash_mismatch_refused(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    pinned = env.store_dir / "c360-batch-s10.yaml"
    pinned.write_text(pinned.read_text().replace("replicas: 2", "replicas: 8"))
    new_snap = _snapshot(pinned)
    c = _compare(env, "c360-batch-s10", _batch_run(new_snap, "20260924-110000-bbbbbb"))
    assert c.verdict == pg.REFUSED
    assert any("changed since the baseline" in r for r in c.reasons)
    assert c.rows == []


def test_run_with_different_sizing_refused(env):
    """The false scare: a 'baseline' with 4x the Trino workers."""
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    bigger = copy.deepcopy(snap)
    bigger["trino"]["worker"]["replicas"] = 8
    c = _compare(env, "c360-batch-s10", _batch_run(bigger, "20260924-110000-bbbbbb", ttv=300.0))
    assert c.verdict == pg.REFUSED
    assert any("trino.worker.replicas: pinned 2, run 8" in r for r in c.reasons)


def test_drained_run_rows_per_second_excluded(env):
    snap = env.snaps["c360-continuous-s10"]
    _record(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-100000-aaaaaa", rps=9000.0, drained=True),
    )
    # A drained run reports corpus rows / window, far below the real rate.
    # Its rows/s is never compared, however low it is.
    c = _compare(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-110000-bbbbbb", rps=10.0, drained=True),
    )
    assert c.verdict == pg.PASS, pg.format_comparison(c)
    assert all(r.metric != "sustained_throughput_rps" for r in c.rows)
    numbers, excluded = pg.extract_metrics(pg.load_run(env.runs / "run-20260924-110000-bbbbbb"))
    assert "sustained_throughput_rps" not in numbers
    assert "LB-145" in excluded["sustained_throughput_rps"]


def test_drained_run_refused_against_undrained_baseline(env):
    snap = env.snaps["c360-continuous-s10"]
    _record(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-100000-aaaaaa", rps=50000.0, drained=False),
    )
    c = _compare(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-110000-bbbbbb", rps=9000.0, drained=True),
    )
    assert c.verdict == pg.REFUSED
    assert any("corpus_drained" in r for r in c.reasons)


def test_undrained_rows_per_second_regression_detected(env):
    snap = env.snaps["c360-continuous-s10"]
    _record(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-100000-aaaaaa", rps=50000.0, drained=False),
    )
    c = _compare(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-110000-bbbbbb", rps=40000.0, drained=False),
    )
    assert c.verdict == pg.REGRESSION
    assert _row(c, "sustained_throughput_rps").status == "regression"


# -- honest-number guards ----------------------------------------------------


@pytest.mark.parametrize("ratio", [0.0, 0.5, 0.94])
def test_low_scale_ratio_refused(env, ratio):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(
        env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", scale_ratio=ratio)
    )
    assert c.verdict == pg.REFUSED
    assert any("scale_ratio" in r for r in c.reasons)


def test_failed_query_is_missing_and_fails(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(
        env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", failed=("Q3_join",))
    )
    assert c.verdict == pg.REGRESSION
    assert _row(c, "query_qph_Q3_join").status == "missing"


def test_failed_run_refused(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", success=False))
    assert c.verdict == pg.REFUSED


# -- release gate ------------------------------------------------------------


def test_release_check_fails_on_missing_required_baseline(env):
    passed, lines = pg.release_check(env.store(), env.runs)
    assert not passed
    assert any(ln.startswith("FAIL c360-batch-s10 [required]: no baseline") for ln in lines)
    assert any(ln.startswith("warn aml-batch-s1 [optional]") for ln in lines)


def _accept_both_c360(env, extra_runs: bool = True):
    """Record both c360 baselines and, by default, one later run of each."""
    b, c = env.snaps["c360-batch-s10"], env.snaps["c360-continuous-s10"]
    _record(env, "c360-batch-s10", _batch_run(b, "20260924-100000-aaaaaa"))
    _record(
        env, "c360-continuous-s10", _cont_run(c, "20260924-100001-aaaaaa", rps=5e4, drained=False)
    )
    if extra_runs:
        env.write_run(_batch_run(b, "20260924-200000-eeeeee", ttv=610.0))
        env.write_run(_cont_run(c, "20260924-200001-eeeeee", rps=5.1e4, drained=False))


# -- the checked-in pinned configs and store ---------------------------------

# Every knob that changes a performance number, per mode. The pinned file must
# set each one itself: defaults are not part of config_hash, so a default
# that moved would otherwise change the run without changing the hash.
_ALWAYS = [
    "images.datagen",
    "images.spark",
    "architecture.pipeline.mode",
    "architecture.workload.schema",
    "architecture.workload.datagen.scale",
    "architecture.workload.datagen.mode",
    "architecture.workload.datagen.parallelism",
    "architecture.workload.datagen.cpu",
    "architecture.workload.datagen.file_size",
    "architecture.workload.datagen.generators",
    "architecture.workload.datagen.timestamp_start",
    "architecture.workload.datagen.timestamp_end",
    "architecture.query_engine.type",
    "architecture.pipeline.pre_benchmark_maintenance",
    "architecture.benchmark.mode",
    "architecture.benchmark.cache",
    "architecture.benchmark.iterations",
    "platform.storage.scratch.storage_class",
]
_BATCH = [
    "platform.compute.spark.bronze_executors",
    "platform.compute.spark.silver_executors",
    "platform.compute.spark.gold_executors",
]
_SUSTAINED = [
    "platform.compute.spark.bronze_ingest_executors",
    "platform.compute.spark.silver_stream_executors",
    "platform.compute.spark.gold_refresh_executors",
    "architecture.pipeline.sustained.bronze_trigger_interval",
    "architecture.pipeline.sustained.silver_trigger_interval",
    "architecture.pipeline.sustained.gold_refresh_interval",
    "architecture.pipeline.sustained.run_duration",
    "architecture.pipeline.sustained.max_files_per_trigger",
    "architecture.pipeline.sustained.benchmark_interval",
    "architecture.pipeline.sustained.benchmark_warmup",
    "architecture.pipeline.sustained.retention_interval",
    "architecture.pipeline.sustained.retention_threshold",
    "architecture.pipeline.sustained.compaction_enabled",
    "architecture.pipeline.sustained.compaction_interval",
]
_TRINO = [
    "images.trino",
    "architecture.query_engine.trino.coordinator.cpu",
    "architecture.query_engine.trino.coordinator.memory",
    "architecture.query_engine.trino.worker.replicas",
    "architecture.query_engine.trino.worker.cpu",
    "architecture.query_engine.trino.worker.memory",
]
_THRIFT = [
    "architecture.query_engine.spark_thrift.cores",
    "architecture.query_engine.spark_thrift.memory",
]


def _has(data: dict, dotted: str) -> bool:
    cur = data
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False
        cur = cur[part]
    return cur is not None


@pytest.mark.parametrize(
    "path", sorted(p for p in PERF.glob("*.yaml") if p.name != "baselines.yaml")
)
def test_pinned_executor_counts_match_todays_auto_counts(path):
    """Pinning must not change sizing: overrides equal get_executor_count."""
    from lakebench.modules.pipeline_engines.spark.job import get_executor_count

    data = yaml.safe_load(path.read_text())
    spark = data["platform"]["compute"]["spark"]
    scale = data["architecture"]["workload"]["datagen"]["scale"]
    schema = data["architecture"]["workload"]["schema"]
    jobs = {
        "bronze_executors": "bronze-verify",
        "silver_executors": "silver-build",
        "gold_executors": "gold-finalize",
        "bronze_ingest_executors": "bronze-ingest",
        "silver_stream_executors": "silver-stream",
        "gold_refresh_executors": "gold-refresh",
    }
    for key, job in jobs.items():
        if key in spark:
            assert spark[key] == get_executor_count(job, scale, schema), (path.name, key)


#: The v1.7 re-baseline: AML batch scale 10 and Customer 360 batch scale 10
#: on Hive and on Polaris; each baseline is one run (n=1) of a three-run series.
REBASELINE_V17 = {"aml-batch-s10", "c360-batch-s10", "c360-batch-s10-polaris"}


def test_checked_in_store_requires_exactly_the_v17_rebaseline_set(tmp_path):
    # The v1.7 re-baseline set is pinned. Until the post-freeze data commit
    # accepts its baselines nothing is required; from that commit on exactly
    # the set is required, each with a fingerprint-v2 baseline, and the
    # release check fails without their runs.
    store = pg.load_store(PERF / "baselines.yaml")
    assert REBASELINE_V17 <= set(store.baselines)
    required = {n for n, b in store.baselines.items() if b.required}
    assert required in (set(), REBASELINE_V17), required
    if required:
        for name in REBASELINE_V17:
            b = store.baselines[name]
            assert b.accepted and b.fingerprint_version == 2, name
    passed, lines = pg.release_check(store, tmp_path)
    assert passed == (not required), lines
    assert len(lines) == len(store.baselines)
    if not required:
        assert all(ln.startswith(("warn", "ok")) for ln in lines)


# -- review fixes (adversarial pass on d9860b9) ------------------------------


def test_continuous_run_with_no_data_cannot_be_recorded(env):
    snap = env.snaps["c360-continuous-s10"]
    store = env.store()
    data = _cont_run(snap, "20260924-100000-aaaaaa", rps=0.0, drained=False, ingest=0.0)
    run = pg.load_run(env.write_run(data))
    with pytest.raises(pg.PerfGateError, match="no data flowed"):
        pg.record_baseline(store, "c360-continuous-s10", run, "abc")


def test_scale_ratio_above_band_refused(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(
        env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", scale_ratio=2.05)
    )
    assert c.verdict == pg.REFUSED
    assert any("more data than the scale" in r for r in c.reasons)


def test_ttv_regression_caught_without_datagen_stage(env):
    """Review probe F: datagen on one side only must not hide a 40% TTV loss."""
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    run = _batch_run(
        snap,
        "20260926-120000-dddddd",
        ttv=840.0,
        gbps=0.2 * 600 / 840,
        stage_s={"bronze": 100.0, "silver": 200.0, "gold": 150.0},
    )
    c = _compare(env, "c360-batch-s10", run)
    assert c.verdict == pg.REGRESSION, pg.format_comparison(c)
    assert _row(c, "time_to_value_seconds").status == "regression"
    assert _row(c, "time_to_value_seconds").actual == pytest.approx(840.0)
    assert _row(c, "pipeline_throughput_gb_per_second").status == "regression"


def test_continuous_ingest_ratio_above_band_refused(env):
    snap = env.snaps["c360-continuous-s10"]
    _record(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-100000-aaaaaa", rps=5e4, drained=False),
    )
    run = _cont_run(snap, "20260924-110000-bbbbbb", rps=1.2e5, drained=False, ingest=2.4)
    c = _compare(env, "c360-continuous-s10", run)
    assert c.verdict == pg.REFUSED
    assert any("ingest_ratio 2.40" in r for r in c.reasons)


def test_stale_datagen_metrics_excluded_not_attributed(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    stale = _batch_run(snap, "20260924-110000-bbbbbb", dg_mbps=500.0)
    stale["datagen_fleet"]["written_at"] = "2026-09-22T10:00:00+00:00"
    c = _compare(env, "c360-batch-s10", stale)
    # A much slower datagen from an earlier generate is not this run's number.
    assert c.verdict == pg.PASS, pg.format_comparison(c)
    assert "earlier generate" in _row(c, "datagen_mbps_per_pod").status
    for metric in ("time_to_value_seconds", "compute_efficiency_gb_per_core_hour"):
        assert _row(c, metric).status == "ok", metric
    fresh = _batch_run(snap, "20260924-120000-cccccc", dg_mbps=500.0)
    fresh["datagen_fleet"]["written_at"] = "2026-09-24T09:55:00+00:00"
    c = _compare(env, "c360-batch-s10", fresh)
    assert c.verdict == pg.REGRESSION
    assert _row(c, "datagen_mbps_per_pod").status == "regression"


def test_stale_datagen_does_not_hide_ttv_regression(env):
    """Review probe E2: a stale sidecar must not drop time to value."""
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    run = _batch_run(snap, "20260926-110000-cccccc", ttv=840.0)
    run["start_time"] = "2026-09-26T11:00:00"
    run["datagen_fleet"]["written_at"] = "2026-09-24T09:00:00+00:00"
    c = _compare(env, "c360-batch-s10", run)
    assert c.verdict == pg.REGRESSION, pg.format_comparison(c)
    assert _row(c, "time_to_value_seconds").status == "regression"
    assert "earlier generate" in _row(c, "datagen_seconds").status


def test_record_refuses_a_batch_run_without_fresh_datagen(env):
    """A baseline with no datagen numbers would turn datagen gating off."""
    snap = env.snaps["c360-batch-s10"]
    stale = _batch_run(snap, "20260924-100000-aaaaaa")
    stale["datagen_fleet"]["written_at"] = "2026-09-20T10:00:00+00:00"
    with pytest.raises(pg.PerfGateError, match="earlier generate"):
        pg.record_baseline(env.store(), "c360-batch-s10", pg.load_run(env.write_run(stale)), "x")
    none = _batch_run(
        snap, "20260924-110000-bbbbbb", stage_s={"bronze": 100.0, "silver": 200.0, "gold": 150.0}
    )
    del none["datagen_fleet"]
    with pytest.raises(pg.PerfGateError, match="no datagen stage"):
        pg.record_baseline(env.store(), "c360-batch-s10", pg.load_run(env.write_run(none)), "x")


def test_multi_cycle_batch_run_refused(env):
    """Cycles 2..N generate between cycles, inside first-start..last-end."""
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    run = _batch_run(snap, "20260924-110000-bbbbbb")
    run["cycles"] = [{"cycle": 1}, {"cycle": 2}]
    c = _compare(env, "c360-batch-s10", run)
    assert c.verdict == pg.REFUSED
    assert any("multi-cycle" in r for r in c.reasons)
    dup = _batch_run(snap, "20260924-120000-cccccc")
    dup["pipeline_benchmark"]["stages"].append(dict(dup["pipeline_benchmark"]["stages"][2]))
    c = _compare(env, "c360-batch-s10", dup)
    assert any("multi-cycle" in r for r in c.reasons)


def _real_run(snap: dict, run_id: str, silver_s: float = 200.0):
    """A run built by the real collector and saved by MetricsStorage."""
    from lakebench.metrics.collector import (
        BenchmarkMetrics,
        JobMetrics,
        PipelineMetrics,
        build_pipeline_benchmark,
    )

    t = datetime(2026, 9, 24, 10, 0, 0)
    ov = snap["spark"]["executor_overrides"]
    pm = PipelineMetrics(
        run_id=run_id,
        deployment_name=snap["name"],
        start_time=t,
        success=True,
        bronze_size_gb=100.0,
        silver_size_gb=90.0,
        gold_size_gb=40.0,
        config_snapshot=snap,
        # What SD-5c's run start fills from the deployment's manifest.
        provenance=_provenance(),
    )
    t += timedelta(seconds=300)  # generate inside the run
    for jt, secs, gb, ex in (
        ("bronze-verify", 100.0, 100.0, ov["bronze"]),
        ("silver-build", silver_s, 90.0, ov["silver"]),
        ("gold-finalize", 150.0, 40.0, ov["gold"]),
    ):
        start, t = t, t + timedelta(seconds=secs)
        pm.jobs.append(
            JobMetrics(
                job_name=f"lakebench-{jt}",
                job_type=jt,
                start_time=start,
                end_time=t,
                elapsed_seconds=secs,
                success=True,
                input_size_gb=gb,
                output_rows=1000,  # rows in every layer (the verdict's layer_rows gate)
                executor_count=ex,
                executor_cores=4,
            )
        )
    pm.benchmark = BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=10,
        qph=400.0,
        total_seconds=30.0,
        queries=[
            {
                "name": "Q1",
                "elapsed_seconds": 30.0,
                "success": True,
                "samples": [29.0, 30.0, 31.0],
                "result_fingerprint": stub_experiment(["Q1"])["results"]["fingerprints"]["Q1"],
            }
        ],
        iterations=3,
    )
    pm.end_time = t + timedelta(seconds=30)
    pods = snap["datagen"]["parallelism"]
    fleet = {
        "pods_expected": pods,
        "pods_reported": pods,
        "data_quality": "complete",
        "aggregate_mbps": 2000.0,
        "cpu_hr_per_tb": 6.0,
        "written_at": "2026-09-24T09:00:00+00:00",
        "wall_elapsed_max_s": 300.0,
        "total_bytes_written": 100e9,
        "cores_total": pods * 4,
    }
    pm.datagen_fleet = fleet
    pm.pipeline_benchmark = build_pipeline_benchmark(pm, datagen_elapsed=300.0, datagen_fleet=fleet)
    return pm


def test_real_collector_run_matches_scorecard_and_gates(env):
    """Sized stages from the real collector: the gate's numbers are the report's."""
    from lakebench.metrics.storage import MetricsStorage

    snap = env.snaps["c360-batch-s10"]
    storage = MetricsStorage(env.runs)
    base = pg.load_run(storage.save_run(_real_run(snap, "20260924-100000-aaaaaa")))
    assert pg.run_refusals(base, env.store().pinned("c360-batch-s10")) == []
    numbers, _ = pg.extract_metrics(base)
    for key in ("time_to_value_seconds", "pipeline_throughput_gb_per_second"):
        assert numbers[key] == pytest.approx(base.scores[key], rel=1e-3), key
    store = env.store()
    pg.record_baseline(store, "c360-batch-s10", base, "abc")
    store.save()
    slow = pg.load_run(storage.save_run(_real_run(snap, "20260925-100000-bbbbbb", silver_s=380.0)))
    c = pg.compare_run(env.store(), "c360-batch-s10", slow)
    assert c.verdict == pg.REGRESSION, pg.format_comparison(c)
    for metric in ("time_to_value_seconds", "pipeline_throughput_gb_per_second"):
        assert _row(c, metric).status == "regression", metric


def test_pre_compaction_qph_regression_detected(env):
    snap = env.snaps["c360-batch-s10"]
    base = _batch_run(snap, "20260924-100000-aaaaaa", maintenance_value_pct=10.0)
    _record(env, "c360-batch-s10", base)
    worse = _batch_run(snap, "20260924-110000-bbbbbb", maintenance_value_pct=10.0)
    worse["pipeline_benchmark"]["scorecard"]["pre_compaction_qph"] = 200.0
    c = _compare(env, "c360-batch-s10", worse)
    assert c.verdict == pg.REGRESSION
    assert _row(c, "pre_compaction_qph").status == "regression"


# -- stopped pre-benchmark maintenance (lane Y follow-up) ---------------------


def _stopped(data: dict, pre_qph: float | None = None) -> dict:
    sc = data["pipeline_benchmark"]["scorecard"]
    sc["maintenance_stopped"] = True
    sc["maintenance_stop_reason"] = "OPTIMIZE lakehouse.gold.t timed out after 1800s"
    if pre_qph is not None:
        sc["pre_compaction_qph"] = pre_qph
    return data


def test_stopped_maintenance_excludes_post_maintenance_qph(env):
    snap = env.snaps["c360-batch-s10"]
    base = _batch_run(snap, "20260924-100000-aaaaaa")
    base["pipeline_benchmark"]["scorecard"]["pre_compaction_qph"] = 300.0
    _record(env, "c360-batch-s10", base)
    # A rewrite still running halves QpH; that is not a regression.
    slow = _stopped(
        _batch_run(snap, "20260924-110000-bbbbbb", qph=200.0, query_scale=2.0), pre_qph=300.0
    )
    c = _compare(env, "c360-batch-s10", slow)
    assert c.verdict != pg.REGRESSION, pg.format_comparison(c)
    for metric in ("composite_qph", "query_qph_Q1_scan"):
        row = _row(c, metric)
        assert row.status.startswith("excluded"), row
        assert "maintenance stopped" in row.status
    assert _row(c, "pre_compaction_qph").status == "ok"


def _live(data: dict, pre_qph: float | None = None) -> dict:
    sc = data["pipeline_benchmark"]["scorecard"]
    sc["maintenance_live_streams"] = True
    sc["maintenance_live_streams_reason"] = (
        "stream apps present or unreadable: lakebench-silver-stream"
    )
    if pre_qph is not None:
        sc["pre_compaction_qph"] = pre_qph
    return data


def _simulate_continuous_loop(run, ri, ci, bench_s, warmup=300, bench_interval=300):
    """(maintenance rounds, compaction rounds) the _run_sustained loop fires.

    Worst case: every maintenance and compaction round uses its whole budget
    plus the statement grace,
    every maintenance round ends on a timeout (so compaction is held one
    statement timeout), and each in-stream benchmark round takes bench_s and
    wins the loop pass (it `continue`s). Mirrors the order in _run_sustained.
    """
    from lakebench.cli._sustained import _BUDGET_GRACE_SECONDS, continuous_round_bounds

    t, next_round, next_maint, next_comp = 0.0, float(warmup), float(ri), float(ci)
    last_round = hold = 0.0
    maint = comp = 0
    while t < run:
        elapsed = t
        if bench_s and elapsed >= next_round and run - elapsed >= max(60, last_round * 1.2):
            t += bench_s
            last_round = bench_s
            next_round = t + bench_interval
            continue
        if elapsed >= next_maint:
            b = continuous_round_bounds(ri, run - t)
            if b:
                # The last statement may overrun the budget by the grace.
                t += b[1] + _BUDGET_GRACE_SECONDS
                maint += 1
                hold = t + b[0]
            next_maint = t + ri
        if elapsed >= next_comp:
            if t < hold:
                next_comp = hold
            else:
                b = continuous_round_bounds(ci, run - t)
                if b:
                    t += b[1] + _BUDGET_GRACE_SECONDS
                    comp += 1
                next_comp = t + ci
        wake = min(elapsed + 30, next_round if bench_s else run, next_maint, next_comp, run)
        t = max(t + 1.0, wake)  # real time moves on even when nothing sleeps
    return maint, comp


def test_continuous_pinned_config_fires_maintenance_and_compaction():
    """At least two maintenance rounds and one compaction inside run_duration."""
    for path in PERF.glob("*.yaml"):
        data = yaml.safe_load(path.read_text())
        if path.name == "baselines.yaml" or data["architecture"]["pipeline"]["mode"] != "sustained":
            continue
        s = data["architecture"]["pipeline"]["sustained"]
        assert s["compaction_enabled"] is True, path.name
        for bench_s in range(0, 301, 30):
            maint, comp = _simulate_continuous_loop(
                s["run_duration"],
                s["retention_interval"],
                s["compaction_interval"],
                bench_s,
                s["benchmark_warmup"],
                s["benchmark_interval"],
            )
            assert maint >= 2 and comp >= 1, (path.name, bench_s, maint, comp)


def test_continuous_run_without_a_result_check_is_not_a_baseline(env):
    """The continuous deviation is gone: a continuous run whose corpus never
    settled has no checked results and cannot be a baseline."""
    snap = env.snaps["c360-continuous-s10"]
    data = _cont_run(snap, "20260924-100000-aaaaaa", rps=5e4, drained=False)
    # What a continuous record carried before the result check: rounds not
    # checked "by design", which the gate used to accept.
    data["experiment"]["results"] = {
        "query_set_id": None,
        "fingerprints": {},
        "not_checked": "continuous: in-stream benchmark rounds read tables still being written",
        "by_design": True,
    }
    run = pg.load_run(env.write_run(data))
    with pytest.raises(pg.PerfGateError, match="comparability not established"):
        pg.record_baseline(env.store(), "c360-continuous-s10", run, "abc")


def _cluster_timed(data: dict) -> dict:
    for st in data["pipeline_benchmark"]["stages"]:
        if st["stage_type"] == "batch":
            st["timing_source"] = "driver_container"
    return data


# -- LB-269: a held-out corpus is never a baseline or a gated run ------------
# The held-out record is the synthetic test fixture (tests/fixtures/
# heldout_test.json, test values), never a pre-registration value.


@pytest.fixture
def held(monkeypatch):
    from tests.fixtures import protected_corpus as pc

    pc.use_heldout(monkeypatch)
    return pc


def test_a_held_out_seed_run_is_refused_with_that_reason_alone(env, held):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    run = _batch_run(
        snap, "20260924-110000-bbbbbb", experiment=stub_experiment(QUERIES, seed=held.EV)
    )
    c = _compare(env, "c360-batch-s10", run)
    assert c.verdict == pg.REFUSED
    assert c.reasons == [
        "the run's corpus is a protected AML corpus (its seed is the registered evaluation seed)"
    ]
    assert held.seed_tokens(" ".join(c.reasons)) == []


def test_a_held_out_seed_run_cannot_be_recorded(env, held):
    snap = env.snaps["c360-batch-s10"]
    run = _batch_run(
        snap, "20260924-100000-aaaaaa", experiment=stub_experiment(QUERIES, seed=held.RB)
    )
    with pytest.raises(pg.PerfGateError) as e:
        _record(env, "c360-batch-s10", run)
    assert "registered robustness seed" in str(e.value)
    assert held.seed_tokens(str(e.value)) == []


def test_a_baseline_with_a_held_out_seed_refuses_every_run(env, held):
    snap = env.snaps["c360-batch-s10"]
    store = _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    store.baselines["c360-batch-s10"].experiment_identity["seed"] = held.EV
    c = pg.compare_run(
        store,
        "c360-batch-s10",
        pg.load_run(env.write_run(_batch_run(snap, "20260924-110000-bbbbbb"))),
    )
    assert c.verdict == pg.REFUSED
    assert c.reasons == ["the baseline's seed is the registered evaluation seed"]


def test_a_seed_difference_never_prints_the_seeds(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    run = _batch_run(
        snap, "20260924-110000-bbbbbb", experiment=stub_experiment(QUERIES, seed=987654321)
    )
    c = _compare(env, "c360-batch-s10", run)
    assert "seed differs (values withheld) from the baseline" in c.reasons
    assert not any("987654321" in r for r in c.reasons)


def test_release_check_line_carries_no_held_out_seed(env, held):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    env.write_run(
        _batch_run(
            snap, "20260924-110000-bbbbbb", experiment=stub_experiment(QUERIES, seed=held.EV)
        )
    )
    ok, lines = pg.release_check(
        env.store(), [env.runs], {"c360-batch-s10": "20260924-110000-bbbbbb"}
    )
    assert not ok
    text = "\n".join(lines)
    assert "protected AML corpus" in text and held.seed_tokens(text) == []


def _aml_raw(seed, *, schema="financial"):
    exp = stub_experiment(QUERIES)
    exp["workload"]["name"] = "financial" if schema == "financial" else "customer360"
    exp["corpus"]["schema"] = schema
    exp["corpus"]["seed"] = seed
    return {"run_id": "r", "experiment": exp}


def test_protected_run_refusal_passes_calibration_and_c360(held):
    assert pg.protected_run_refusal(_aml_raw(held.CALIBRATION)) is None
    assert pg.protected_run_refusal(_aml_raw(None, schema="customer360")) is None
    assert "unidentified" in pg.protected_run_refusal(_aml_raw(None))
    assert "registered evaluation seed" in pg.protected_run_refusal(_aml_raw(held.EV))


def test_baseline_seed_refusal_is_fail_closed_for_aml_only(held, monkeypatch):
    from lakebench.config import datagen_seed as ds

    assert pg.baseline_seed_refusal({"workload": "financial", "seed": held.CALIBRATION}) is None
    assert pg.baseline_seed_refusal({"workload": "financial", "seed": None})
    assert pg.baseline_seed_refusal({"workload": "financial", "seed": "ab" * 32})
    assert pg.baseline_seed_refusal({"workload": "customer360", "seed": None}) is None

    def boom():
        raise OSError("no hash file")

    monkeypatch.setattr(ds, "_heldout", boom)
    assert pg.baseline_seed_refusal({"workload": "customer360", "seed": 42}) is None
    assert "cannot be checked" in pg.baseline_seed_refusal({"workload": "financial", "seed": 43})


def test_a_newer_protected_run_never_displaces_the_gating_run(env, held):
    snap = env.snaps["c360-batch-s10"]
    env.write_run(_batch_run(snap, "20260924-110000-bbbbbb"))
    env.write_run(
        _batch_run(
            snap, "20260924-120000-cccccc", experiment=stub_experiment(QUERIES, seed=held.EV)
        )
    )
    pinned = env.store().pinned("c360-batch-s10")
    assert pg.latest_candidate(pinned, env.runs).run_id == "20260924-110000-bbbbbb"
