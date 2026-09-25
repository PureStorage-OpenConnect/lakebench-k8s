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
from pathlib import Path

import pytest
import yaml

from lakebench.metrics import perf_gate as pg

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
        return build_config_snapshot(cfg)
    finally:
        for k in env:
            os.environ.pop(k, None)


QUERIES = {"Q1_scan": 4.0, "Q2_filter": 3.0, "Q3_join": 6.0}


def _batch_run(snapshot: dict, run_id: str, **over) -> dict:
    ttv = over.get("ttv", 600.0)
    qph = over.get("qph", 400.0)
    stage_s = over.get(
        "stage_s", {"datagen": 120.0, "bronze": 100.0, "silver": 200.0, "gold": 150.0}
    )
    executors = over.get("executors", {"bronze": 4, "silver": 8, "gold": 4})
    q_scale = over.get("query_scale", 1.0)
    queries = [
        {"name": n, "elapsed_seconds": s * q_scale, "success": n not in over.get("failed", ())}
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
            "stage_type": "batch",
            "elapsed_seconds": secs,
            "success": True,
            "executor_count": executors.get(name, 0),
        }
        for name, secs in stage_s.items()
    ]
    pods = over.get("pods", 4)
    return {
        "run_id": run_id,
        "deployment_name": snapshot["name"],
        "start_time": "2026-09-24T10:00:00",
        "success": over.get("success", True),
        "config_snapshot": snapshot,
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


def test_higher_is_better_drop_is_a_regression(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", dg_mbps=1600.0))
    assert c.verdict == pg.REGRESSION
    assert _row(c, "datagen_mbps_per_pod").status == "regression"
    assert _row(c, "datagen_aggregate_mbps").status == "regression"


def test_improvement_accepted(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    faster = _batch_run(
        snap,
        "20260924-110000-bbbbbb",
        ttv=400.0,
        qph=700.0,
        query_scale=0.5,
        stage_s={"datagen": 60.0, "bronze": 50.0, "silver": 100.0, "gold": 75.0},
        dg_mbps=4000.0,
    )
    c = _compare(env, "c360-batch-s10", faster)
    assert c.verdict == pg.PASS, pg.format_comparison(c)
    assert _row(c, "time_to_value_seconds").status == "improved"
    assert _row(c, "query_qph_Q1_scan").status == "improved"
    assert _row(c, "silver_seconds").status == "improved"


def test_drift_inside_tolerance_passes(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", ttv=650.0))
    assert c.verdict == pg.PASS
    assert _row(c, "time_to_value_seconds").status == "ok"


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


def test_comment_edit_keeps_config_hash(env):
    pinned = env.store_dir / "c360-batch-s10.yaml"
    before = pg.config_file_hash(pinned)
    pinned.write_text("# a new comment\n" + pinned.read_text())
    assert pg.config_file_hash(pinned) == before


def test_run_with_different_sizing_refused(env):
    """The false scare: a 'baseline' with 4x the Trino workers."""
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    bigger = copy.deepcopy(snap)
    bigger["trino"]["worker"]["replicas"] = 8
    c = _compare(env, "c360-batch-s10", _batch_run(bigger, "20260924-110000-bbbbbb", ttv=300.0))
    assert c.verdict == pg.REFUSED
    assert any("trino.worker.replicas: pinned 2, run 8" in r for r in c.reasons)


def test_record_refuses_mismatched_run(env):
    bigger = copy.deepcopy(env.snaps["c360-batch-s10"])
    bigger["datagen"]["parallelism"] = 16
    store = env.store()
    run = pg.load_run(env.write_run(_batch_run(bigger, "20260924-100000-aaaaaa")))
    with pytest.raises(pg.PerfGateError, match="datagen.parallelism"):
        pg.record_baseline(store, "c360-batch-s10", run, "abc")


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


def test_drained_run_never_becomes_the_rows_per_second_baseline(env):
    snap = env.snaps["c360-continuous-s10"]
    store = _record(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-100000-aaaaaa", rps=9000.0, drained=True),
    )
    assert "sustained_throughput_rps" not in store.baselines["c360-continuous-s10"].metrics
    assert "data_freshness_seconds" in store.baselines["c360-continuous-s10"].metrics


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


def test_maintenance_value_none_is_not_measured(env):
    snap = env.snaps["c360-batch-s10"]
    _record(
        env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa", maintenance_value_pct=8.0)
    )
    c = _compare(
        env,
        "c360-batch-s10",
        _batch_run(snap, "20260924-110000-bbbbbb", maintenance_value_pct=None),
    )
    assert c.verdict == pg.PASS
    row = _row(c, "maintenance_value_pct")
    assert row.status == "excluded: not measured" and row.actual is None


def test_maintenance_value_compared_in_points(env):
    snap = env.snaps["c360-batch-s10"]
    _record(
        env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa", maintenance_value_pct=8.0)
    )
    # 8 -> 1 is -87% relative but only 7 points; inside the 10-point band.
    ok = _compare(
        env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", maintenance_value_pct=1.0)
    )
    assert _row(ok, "maintenance_value_pct").status == "ok"
    bad = _compare(
        env,
        "c360-batch-s10",
        _batch_run(snap, "20260924-120000-cccccc", maintenance_value_pct=-5.0),
    )
    assert _row(bad, "maintenance_value_pct").status == "regression"


def test_failed_query_is_missing_and_fails(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(
        env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", failed=("Q3_join",))
    )
    assert c.verdict == pg.REGRESSION
    assert _row(c, "query_qph_Q3_join").status == "missing"


def test_realised_executor_count_mismatch_refused(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(
        env,
        "c360-batch-s10",
        _batch_run(
            snap, "20260924-110000-bbbbbb", executors={"bronze": 4, "silver": 16, "gold": 4}
        ),
    )
    assert c.verdict == pg.REFUSED
    assert any("silver ran 16 executors, pinned 8" in r for r in c.reasons)


def test_failed_run_refused(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", success=False))
    assert c.verdict == pg.REFUSED


def test_record_needs_replace_for_an_accepted_baseline(env):
    snap = env.snaps["c360-batch-s10"]
    store = _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    b = store.baselines["c360-batch-s10"]
    assert (b.run_id, b.git_sha, b.status) == (
        "20260924-100000-aaaaaa",
        "abc1234",
        pg.STATUS_ACCEPTED,
    )
    assert b.config_hash == pg.config_file_hash(env.store_dir / "c360-batch-s10.yaml")
    run = pg.load_run(env.write_run(_batch_run(snap, "20260924-110000-bbbbbb")))
    with pytest.raises(pg.PerfGateError, match="--replace"):
        pg.record_baseline(store, "c360-batch-s10", run, "def")
    pg.record_baseline(store, "c360-batch-s10", run, "def", replace=True)
    assert store.baselines["c360-batch-s10"].run_id == "20260924-110000-bbbbbb"


def test_pending_baseline_gives_no_baseline(env):
    snap = env.snaps["c360-batch-s10"]
    c = _compare(env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb"))
    assert c.verdict == pg.NO_BASELINE


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


def test_release_check_passes_and_then_fails_on_regression(env):
    _accept_both_c360(env)
    passed, lines = pg.release_check(env.store(), env.runs)
    assert passed, lines  # the optional AML config is pending but does not fail
    env.write_run(_batch_run(env.snaps["c360-batch-s10"], "20260925-100000-cccccc", ttv=900.0))
    passed, lines = pg.release_check(env.store(), env.runs)
    assert not passed
    assert any(
        "REGRESSION run 20260925-100000-cccccc" in ln and "time_to_value_seconds" in ln
        for ln in lines
    )


def test_release_check_ignores_runs_of_other_configs(env):
    _accept_both_c360(env)
    other = copy.deepcopy(env.snaps["c360-batch-s10"])
    other["trino"]["worker"]["replicas"] = 8
    env.write_run(_batch_run(other, "20260925-100000-dddddd", ttv=900.0))
    passed, lines = pg.release_check(env.store(), env.runs)
    assert passed, lines
    # Named explicitly, the same run is refused rather than skipped.
    passed, lines = pg.release_check(
        env.store(), env.runs, {"c360-batch-s10": "20260925-100000-dddddd"}
    )
    assert not passed
    assert any("REFUSED" in ln and "trino.worker.replicas" in ln for ln in lines)


def test_release_check_fails_when_no_run_found(env, tmp_path):
    _accept_both_c360(env)
    empty = tmp_path / "empty"
    empty.mkdir()
    passed, lines = pg.release_check(env.store(), empty)
    assert not passed
    assert any("no successful run" in ln for ln in lines)


def test_release_gate_script_check(env):
    rg = _load_script("release_gate")
    res = rg.make_perf_check(store_path=env.store_path, runs_dirs=[env.runs])()
    assert res.status == rg.FAIL and "no baseline" in res.detail
    _accept_both_c360(env)
    res = rg.make_perf_check(store_path=env.store_path, runs_dirs=[env.runs])()
    assert res.status == rg.PASS, res.detail
    env.write_run(_batch_run(env.snaps["c360-batch-s10"], "20260925-100000-cccccc", qph=100.0))
    res = rg.make_perf_check(store_path=env.store_path, runs_dirs=[env.runs])()
    assert res.status == rg.FAIL and "composite_qph" in res.detail


def test_perf_gate_cli_exit_codes(env, capsys):
    cli = _load_script("perf_gate")
    snap = env.snaps["c360-batch-s10"]
    base = ["--store", str(env.store_path), "--runs-dir", str(env.runs)]
    env.write_run(_batch_run(snap, "20260924-100000-aaaaaa"))
    assert cli.main([*base, "record", "c360-batch-s10", "--run", "20260924-100000-aaaaaa"]) == 2
    assert (
        cli.main(
            [
                *base,
                "record",
                "c360-batch-s10",
                "--run",
                "20260924-100000-aaaaaa",
                "--git-sha",
                "abc",
            ]
        )
        == 0
    )
    env.write_run(_batch_run(snap, "20260924-110000-bbbbbb", ttv=800.0))
    assert cli.main([*base, "compare", "c360-batch-s10", "--run", "20260924-110000-bbbbbb"]) == 1
    assert "REGRESSION" in capsys.readouterr().out
    env.write_run(_batch_run(snap, "20260924-120000-cccccc", scale_ratio=0.5))
    assert cli.main([*base, "compare", "c360-batch-s10", "--run", "20260924-120000-cccccc"]) == 2
    env.write_run(_batch_run(snap, "20260924-130000-dddddd", ttv=610.0))
    assert cli.main([*base, "compare", "c360-batch-s10", "--run", "20260924-130000-dddddd"]) == 0


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
    "architecture.workload.datagen.uploaders",
    "architecture.workload.datagen.timestamp_start",
    "architecture.workload.datagen.timestamp_end",
    "architecture.query_engine.type",
    "architecture.benchmark.mode",
    "architecture.benchmark.cache",
    "architecture.benchmark.iterations",
    "architecture.benchmark.streams",
    "platform.storage.scratch.storage_class",
    "platform.storage.scratch.size",
    "platform.compute.spark.driver.cores",
    "platform.compute.spark.driver.memory",
    "platform.compute.spark.executor.instances",
    "platform.compute.spark.executor.cores",
    "platform.compute.spark.executor.memory",
    "platform.compute.spark.executor.memory_overhead",
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
def test_pinned_config_sets_every_knob(path):
    data = yaml.safe_load(path.read_text())
    required = list(_ALWAYS)
    mode = data["architecture"]["pipeline"]["mode"]
    required += _SUSTAINED if mode == "sustained" else _BATCH
    engine = data["architecture"]["query_engine"]["type"]
    required += {"trino": _TRINO, "spark-thrift": _THRIFT}.get(engine, [])
    missing = [k for k in required if not _has(data, k)]
    assert not missing, f"{path.name} leaves these to defaults: {missing}"
    assert ":latest" not in data["images"]["datagen"]


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


def test_checked_in_store_loads_and_references_pinned_configs():
    store = pg.load_store(PERF / "baselines.yaml")
    assert {"c360-batch-s10", "c360-continuous-s10", "aml-batch-s1"} <= set(store.baselines)
    assert store.baselines["c360-batch-s10"].required
    assert store.baselines["c360-continuous-s10"].required
    for name in store.baselines:
        pinned = store.pinned(name)
        assert pinned.config_hash and pinned.fingerprint_hash
    assert store.pinned("c360-continuous-s10").mode == "sustained"


def test_store_rejects_accepted_entry_without_provenance(tmp_path):
    p = tmp_path / "baselines.yaml"
    p.write_text(
        yaml.safe_dump(
            {
                "schema_version": 1,
                "baselines": {"x": {"config": "x.yaml", "status": "accepted", "metrics": {"a": 1}}},
            }
        )
    )
    with pytest.raises(pg.PerfGateError, match="run_id"):
        pg.load_store(p)


# -- review fixes (adversarial pass on d9860b9) ------------------------------


def test_release_check_fails_when_only_the_baseline_run_exists(env):
    _accept_both_c360(env, extra_runs=False)
    passed, lines = pg.release_check(env.store(), env.runs)
    assert not passed
    assert any("no run newer than the baseline" in ln for ln in lines)


def test_continuous_window_override_refused(env):
    snap = env.snaps["c360-continuous-s10"]
    _record(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-100000-aaaaaa", rps=5e4, drained=False),
    )
    run = _cont_run(snap, "20260924-110000-bbbbbb", rps=5e4, drained=False, window=300.0)
    c = _compare(env, "c360-continuous-s10", run)
    assert c.verdict == pg.REFUSED
    assert any("run window 300s" in r for r in c.reasons)


def test_continuous_stage_seconds_are_not_compared(env):
    snap = env.snaps["c360-continuous-s10"]
    store = _record(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-100000-aaaaaa", rps=5e4, drained=False),
    )
    metrics = store.baselines["c360-continuous-s10"].metrics
    assert "data_freshness_seconds" in metrics
    assert not [k for k in metrics if k.endswith("_seconds") and k != "data_freshness_seconds"]


def test_continuous_run_with_no_data_cannot_be_recorded(env):
    snap = env.snaps["c360-continuous-s10"]
    store = env.store()
    data = _cont_run(snap, "20260924-100000-aaaaaa", rps=0.0, drained=False, ingest=0.0)
    run = pg.load_run(env.write_run(data))
    with pytest.raises(pg.PerfGateError, match="no data flowed"):
        pg.record_baseline(store, "c360-continuous-s10", run, "abc")


def test_continuous_executor_mismatch_refused(env):
    snap = env.snaps["c360-continuous-s10"]
    _record(
        env,
        "c360-continuous-s10",
        _cont_run(snap, "20260924-100000-aaaaaa", rps=5e4, drained=False),
    )
    run = _cont_run(snap, "20260924-110000-bbbbbb", rps=5e4, drained=False, executors=(2, 8, 2))
    c = _compare(env, "c360-continuous-s10", run)
    assert c.verdict == pg.REFUSED
    assert any("silver ran 8 executors, pinned 4" in r for r in c.reasons)


def test_scale_ratio_above_band_refused(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    c = _compare(
        env, "c360-batch-s10", _batch_run(snap, "20260924-110000-bbbbbb", scale_ratio=2.05)
    )
    assert c.verdict == pg.REFUSED
    assert any("more data than the scale" in r for r in c.reasons)


def test_missing_stage_refused_not_compared(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    no_datagen = _batch_run(
        snap, "20260924-110000-bbbbbb", stage_s={"bronze": 100.0, "silver": 200.0, "gold": 150.0}
    )
    c = _compare(env, "c360-batch-s10", no_datagen)
    assert c.verdict == pg.REFUSED
    assert any("stages differ" in r for r in c.reasons)


def test_stale_datagen_metrics_refused(env):
    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    stale = _batch_run(snap, "20260924-110000-bbbbbb")
    stale["datagen_fleet"]["written_at"] = "2026-09-20T10:00:00+00:00"
    c = _compare(env, "c360-batch-s10", stale)
    assert c.verdict == pg.REFUSED
    assert any("earlier generate" in r for r in c.reasons)
    # Written during the same run: start_time is naive local, written_at UTC.
    fresh = _batch_run(snap, "20260924-120000-cccccc")
    fresh["datagen_fleet"]["written_at"] = "2026-09-24T16:00:00+00:00"
    assert _compare(env, "c360-batch-s10", fresh).verdict == pg.PASS


def test_recorded_config_file_sha_must_match(env):
    import hashlib

    snap = env.snaps["c360-batch-s10"]
    _record(env, "c360-batch-s10", _batch_run(snap, "20260924-100000-aaaaaa"))
    edited = copy.deepcopy(snap)
    edited["config_sha256"] = "0" * 64
    c = _compare(env, "c360-batch-s10", _batch_run(edited, "20260924-110000-bbbbbb"))
    assert c.verdict == pg.REFUSED
    assert any("different config file" in r for r in c.reasons)
    same = copy.deepcopy(snap)
    pinned_bytes = (env.store_dir / "c360-batch-s10.yaml").read_bytes()
    same["config_sha256"] = hashlib.sha256(pinned_bytes).hexdigest()
    assert _compare(env, "c360-batch-s10", _batch_run(same, "20260924-120000-cccccc")).ok


def test_pre_compaction_qph_regression_detected(env):
    snap = env.snaps["c360-batch-s10"]
    base = _batch_run(snap, "20260924-100000-aaaaaa", maintenance_value_pct=10.0)
    _record(env, "c360-batch-s10", base)
    worse = _batch_run(snap, "20260924-110000-bbbbbb", maintenance_value_pct=10.0)
    worse["pipeline_benchmark"]["scorecard"]["pre_compaction_qph"] = 200.0
    c = _compare(env, "c360-batch-s10", worse)
    assert c.verdict == pg.REGRESSION
    assert _row(c, "pre_compaction_qph").status == "regression"


def test_relative_tolerance_on_signed_metric_rejected(tmp_path):
    p = tmp_path / "baselines.yaml"
    entry = {"config": "x.yaml", "tolerances": {"maintenance_value_pct": {"pct": 10}}}
    p.write_text(yaml.safe_dump({"schema_version": 1, "baselines": {"x": entry}}))
    with pytest.raises(pg.PerfGateError, match="signed percentage"):
        pg.load_store(p)
