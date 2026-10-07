"""Continuous-mode credibility (mission invariant 3, DESIGN 5).

The 2026-09-27 discovery run (20260926-215221-65567b, integrate f8b29eb)
exited 0 on a degenerate window: silver-stream and gold-refresh failed to
submit for nine minutes, bronze drained the corpus before the window opened,
rows/s was corpus rows / 600 s, and the gate passed on one silver and one
gold commit. These tests pin each fix to that shape.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
import typer

from lakebench.metrics.collector import (
    BenchmarkRoundMeta,
    MetricsCollector,
    PipelineBenchmark,
    StageMetrics,
    StreamingJobMetrics,
    build_config_snapshot,
)
from lakebench.metrics.continuous_window import (
    StreamEvent,
    arrival_seconds,
    classify_submission_failure,
    parse_events,
    settle_state,
    window_gate_problems,
    window_stats,
)

W0 = datetime(2026, 9, 27, 4, 4, 0)
W1 = W0 + timedelta(seconds=600)


def _ts(offset_s: float) -> str:
    return (W0 + timedelta(seconds=offset_s)).isoformat()


def _line(offset_s: float, msg: str) -> str:
    return f"[lb] {_ts(offset_s)} - {msg}"


def bronze_log(batches: list[tuple[float, int]]) -> str:
    out = []
    for i, (t, rows) in enumerate(batches):
        out.append(_line(t, f"Batch {i}: writing {rows:,} rows to ice.bronze.raw"))
        out.append(_line(t + 5, f"Batch {i}: committed in 5.0s"))
    return "\n".join(out) + "\n"


def silver_log(batches: list[tuple[float, int, int]]) -> str:
    out = []
    for i, (t, rows_in, rows_out) in enumerate(batches):
        out.append(_line(t, f"Batch {i}: transforming {rows_in:,} rows"))
        out.append(_line(t + 2, f"Batch {i}: {rows_out:,} rows after transforms (filtered ~2%)"))
        out.append(_line(t + 8, f"Batch {i}: committed to ice.silver.t in 8.0s"))
    return "\n".join(out) + "\n"


def gold_log(cycles: list[tuple[float, int, float, bool]]) -> str:
    out = []
    for i, (t, silver_rows, fresh, idle) in enumerate(cycles, start=1):
        out.append(_line(t, f"Refresh cycle {i} (batch {i - 1})"))
        out.append(_line(t + 1, f"Cycle {i}: aggregating {silver_rows:,} Silver records"))
        out.append(_line(t + 3, f"Cycle {i}: generated 14 daily KPI records"))
        tag = " (silver idle)" if idle else ""
        out.append(_line(t + 5, f"Cycle {i}: data freshness {fresh:.0f}s{tag}"))
        out.append(_line(t + 6, f"Cycle {i}: refreshed ice.gold.g in 6.0s (14 KPI records)"))
    return "\n".join(out) + "\n"


# The discovery run: bronze took the whole corpus ~9 min before the window
# (the other two streams were retrying submission), silver committed once,
# gold refreshed once on new data and then idled.
DISCOVERY = {
    "bronze-ingest": bronze_log(
        [(-540, 775_000), (-510, 775_000), (-480, 775_000), (-465, 153_560)]
    ),
    "silver-stream": silver_log([(20, 2_478_560, 2_428_561)]),
    "gold-refresh": gold_log([(60, 2_428_561, 322, False), (360, 2_428_561, 622, True)]),
}

# A healthy window: bronze takes a trickle every 30 s for the whole window,
# silver commits every 60 s, gold refreshes every 300 s on new data.
HEALTHY = {
    "bronze-ingest": bronze_log([(t, 26_000) for t in range(0, 600, 30)]),
    "silver-stream": silver_log([(t, 52_000, 51_000) for t in range(10, 600, 60)]),
    "gold-refresh": gold_log([(100, 300_000, 40, False), (400, 600_000, 45, False)]),
}


def _stats(logs: dict[str, str]) -> dict:
    return {job: window_stats(parse_events(log, job), job, W0, W1) for job, log in logs.items()}


# ---------------------------------------------------------------- item 3


def test_discovery_window_fails_the_continuous_gate():
    probs = window_gate_problems(_stats(DISCOVERY))
    text = "\n".join(probs)
    assert "before the window opened" in text  # drained pre-window
    assert "silver committed 1" in text
    assert "gold refreshed on new silver data 1" in text


def test_healthy_window_passes_the_continuous_gate():
    assert window_gate_problems(_stats(HEALTHY)) == []


def test_idle_gold_cycles_do_not_count_as_continuous():
    logs = dict(HEALTHY)
    logs["gold-refresh"] = gold_log(
        [(100, 300_000, 40, False), (400, 300_000, 340, True), (550, 300_000, 490, True)]
    )
    probs = window_gate_problems(_stats(logs))
    assert any("gold refreshed on new silver data 1" in p for p in probs)


def test_unreadable_log_fails_the_gate():
    stats = _stats(HEALTHY)
    stats["gold-refresh"] = None
    assert any("no gold-refresh batch lines" in p for p in window_gate_problems(stats))


def test_gold_freshness_must_be_measured_inside_the_window():
    logs = dict(HEALTHY)
    logs["gold-refresh"] = "\n".join(
        line for line in HEALTHY["gold-refresh"].splitlines() if "freshness" not in line
    )
    assert any("freshness was not measured" in p for p in window_gate_problems(_stats(logs)))


# ---------------------------------------------------------------- item 2


def test_rows_before_the_window_are_not_window_rows():
    b = _stats(DISCOVERY)["bronze-ingest"]
    assert b["window_input_rows"] == 0
    assert b["pre_window_input_rows"] == 2_478_560
    assert b["last_write_offset_seconds"] is None


def test_arrival_stops_one_trigger_after_the_last_write_of_a_drained_corpus():
    assert arrival_seconds(600, 90, True, 30) == 120
    assert arrival_seconds(600, 590, True, 30) == 600  # capped at the window
    assert arrival_seconds(600, 90, False, 30) == 600  # corpus left: the whole window
    assert arrival_seconds(600, None, True, 30) == 0.0  # nothing arrived


def _pb(stages, datagen_rows, trigger="30 seconds"):
    t0 = datetime(2026, 9, 27, tzinfo=timezone.utc)
    pb = PipelineBenchmark(
        run_id="t",
        deployment_name="t",
        pipeline_mode="sustained",
        start_time=t0,
        end_time=t0 + timedelta(seconds=600),
        success=True,
        stages=stages,
        config_snapshot={
            "datagen_output_rows": datagen_rows,
            "sustained": {
                "bronze_trigger_interval": trigger,
                "silver_trigger_interval": "60 seconds",
            },
        },
    )
    pb.compute_aggregates()
    return pb


def _stage(name, **kw):
    return StageMetrics(
        stage_name=name, stage_type="streaming", engine="spark", elapsed_seconds=600, **kw
    )


def test_throughput_counts_only_window_rows_over_arrival_time():
    # The discovery shape: 2,478,560 rows, all before the window.
    pb = _pb(
        [
            _stage(
                "bronze",
                input_rows=2_478_560,
                window_input_rows=0,
                pre_window_input_rows=2_478_560,
                last_write_offset_seconds=None,
            ),
            _stage("silver", input_rows=2_478_560, committed_rows=2_478_560),
        ],
        datagen_rows=2_478_560,
    )
    assert pb.sustained_throughput_rps == 0.0  # was 2,478,560 / 600 = 4,130.9
    assert pb.arrival_seconds == 0.0
    assert pb.pre_window_rows == 2_478_560


def test_a_corpus_that_drains_mid_window_is_not_averaged_over_idle_time():
    pb = _pb(
        [
            _stage(
                "bronze",
                input_rows=1_000_000,
                window_input_rows=900_000,
                pre_window_input_rows=100_000,
                last_write_offset_seconds=270,
            ),
            _stage("silver", input_rows=1_000_000, committed_rows=1_000_000),
        ],
        datagen_rows=1_000_000,
    )
    assert pb.arrival_seconds == 300
    assert pb.sustained_throughput_rps == pytest.approx(3000.0)
    assert pb.window_arrival_fraction == pytest.approx(0.5)
    scores = pb.to_dict()["scores"]
    assert scores["arrival_seconds"] == 300 and scores["pre_window_rows"] == 100_000


def test_a_record_without_a_window_is_scored_as_before():
    pb = _pb([_stage("bronze", input_rows=600_000)], datagen_rows=0)
    assert pb.sustained_throughput_rps == pytest.approx(1000.0)
    assert pb.arrival_seconds is None
    assert "arrival_seconds" not in pb.to_dict()["scores"]


def test_ratio_one_with_silver_caught_up_is_drained():
    # Discovery: ingest_ratio 1.0 next to corpus_drained false.
    pb = _pb(
        [
            _stage("bronze", input_rows=2_478_560),
            _stage("silver", input_rows=2_478_560, committed_rows=2_478_560),
            _stage("gold", input_rows=2_428_561, trailing_idle_cycles=0),
        ],
        datagen_rows=2_478_560,
    )
    assert pb.ingest_ratio == 1.0 and pb.corpus_drained is True


# ---------------------------------------------------------------- item 4


def test_stage_fields_are_measured_or_absent():
    c = MetricsCollector()
    bronze = c.parse_streaming_logs(HEALTHY["bronze-ingest"], "bronze-ingest")
    silver = c.parse_streaming_logs(DISCOVERY["silver-stream"], "silver-stream")
    gold = c.parse_streaming_logs(DISCOVERY["gold-refresh"], "gold-refresh")
    assert bronze.freshness_seconds is None and silver.freshness_seconds is None
    assert gold.unique_rows_processed is None
    silver.apply_window(DISCOVERY["silver-stream"], W0, W1)
    gold.apply_window(DISCOVERY["gold-refresh"], W0, W1)
    # committed_rows is the bronze rows silver consumed; output_rows is
    # what the table holds after the transforms' filter.
    assert silver.committed_rows == 2_478_560
    assert silver.output_rows == 2_428_561
    assert gold.output_rows == 14


def test_gold_freshness_comes_from_cycles_inside_the_window():
    logs = gold_log([(-300, 100, 900, False), (60, 200, 40, False), (360, 300, 45, False)])
    g = StreamingJobMetrics(job_name="g", job_type="gold-refresh")
    g.apply_window(logs, W0, W1)
    assert g.freshness_seconds == 45  # the 900 s cycle ran before the window
    assert g.window_commits == 2


def test_unmeasured_round_freshness_and_table_health_are_absent():
    meta = BenchmarkRoundMeta(round_index=1, gold_data_file_count=12)
    d = meta.to_dict()
    assert d["gold_event_age_seconds"] is None
    assert d["table_health"] == {"gold_data_file_count": 12}  # no -1, no stand-in 0


def test_continuous_snapshot_records_the_benchmark_the_rounds_run():
    from tests.conftest import make_config

    cfg = make_config()
    cfg.architecture.pipeline.mode = type(cfg.architecture.pipeline.mode)("continuous")
    cfg.architecture.benchmark.iterations = 3
    cfg.architecture.benchmark.streams = 4
    bench = build_config_snapshot(cfg)["benchmark"]
    assert bench["iterations"] == 1 and bench["streams"] == 1


# ---------------------------------------------------------------- items 1, 6

_MAVEN = (
    "failed to run spark-submit: ... [FAILED     ] "
    "org.apache.iceberg#iceberg-aws-bundle;1.11.0!iceberg-aws-bundle.jar: "
    "Downloaded file size (0) doesn't match expected Content Length (63613165) ..."
)


def test_a_truncated_maven_download_is_named():
    reason = classify_submission_failure(_MAVEN)
    assert "org.apache.iceberg#iceberg-aws-bundle;1.11.0" in reason
    assert "0 of 63,613,165 bytes" in reason


def test_submission_failures_are_reported_while_the_operator_retries(capsys):
    from lakebench.cli._sustained import StreamStartWatch
    from lakebench.spark.job import JobState, JobStatus

    journal = MagicMock()
    watch = StreamStartWatch("silver-stream", journal)
    for attempt in (1, 1, 2):
        watch(
            JobStatus(
                name="s",
                state=JobState.SUBMISSION_FAILED,
                message=_MAVEN,
                submission_attempts=attempt,
            ),
            60.0 * attempt,
        )
    watch(JobStatus(name="s", state=JobState.RUNNING, message=""), 600.0)
    assert [f["attempt"] for f in watch.failures] == [1, 2]  # repeats are not re-reported
    assert watch.running_at is not None
    out = "".join(capsys.readouterr())
    assert "submission attempt 1 failed" in out and "iceberg-aws-bundle" in out
    assert journal.record.call_count == 2


def test_wait_until_running_reports_every_polled_status():
    from lakebench.modules.pipeline_engines.spark import monitor as mon
    from lakebench.spark.job import JobState, JobStatus

    jm = MagicMock()
    jm.get_job_status.side_effect = [
        JobStatus(name="j", state=s, message="m")
        for s in (JobState.SUBMISSION_FAILED, JobState.RUNNING)
    ]
    m = mon.SparkJobMonitor.__new__(mon.SparkJobMonitor)
    m.job_manager, m.namespace = jm, "ns"
    seen = []
    mon.time.sleep, _sleep = (lambda s: None), mon.time.sleep
    try:
        assert m.wait_until_running(
            "j", poll_interval=0, on_status=lambda st, el: seen.append(st.state)
        ).success
    finally:
        mon.time.sleep = _sleep
    assert seen == [JobState.SUBMISSION_FAILED, JobState.RUNNING]


def test_a_stream_not_running_at_window_end_fails():
    from lakebench.cli._sustained import end_of_window_problems
    from lakebench.spark.job import JobState, JobStatus

    jm = MagicMock()
    jm.get_job_status.side_effect = lambda name: JobStatus(
        name=name,
        state=JobState.FAILED if "gold" in name else JobState.RUNNING,
        message="driver exited",
    )
    probs = end_of_window_problems(jm, ["bronze-ingest", "silver-stream", "gold-refresh"])
    assert len(probs) == 1 and "gold-refresh was FAILED" in probs[0]


# ---------------------------------------------------------------- item 5


def test_benchmark_runner_failure_fails_before_any_stream(monkeypatch, tmp_path, capsys):
    from lakebench.cli import _sustained
    from tests.conftest import make_config

    monkeypatch.chdir(tmp_path)
    op = MagicMock()
    op.check_status.return_value = MagicMock(ready=True, version="2.5.1")
    op.ensure_namespace_watched.return_value = MagicMock(watching_namespace=True)
    monkeypatch.setattr("lakebench.spark.SparkOperatorManager", lambda **kw: op)
    monkeypatch.setattr(_sustained, "get_k8s_client", lambda **kw: MagicMock())
    jm = MagicMock()
    monkeypatch.setattr("lakebench.engine.get_engine", lambda c, k: jm)
    monkeypatch.setattr("lakebench.spark.SparkJobMonitor", lambda *a, **kw: MagicMock())
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: MagicMock())
    monkeypatch.setattr(_sustained, "_collect_platform_metrics", lambda *a, **kw: None)
    monkeypatch.setattr("lakebench.metrics.MetricsStorage", lambda *a, **kw: MagicMock())
    monkeypatch.setattr(_sustained, "write_run_report", lambda *a, **kw: None)

    def broken(cfg, **kw):
        raise RuntimeError("no query engine executor")

    monkeypatch.setattr("lakebench.benchmark.BenchmarkRunner", broken)
    cfg = make_config(
        architecture={
            "pipeline": {
                "mode": "continuous",
                "continuous": {"gold_refresh_interval": "30 seconds"},
            }
        }
    )
    with pytest.raises(typer.Exit) as exc:
        _sustained._run_sustained(cfg, tmp_path / "cfg.yaml", 60, False, 120)
    assert exc.value.exit_code == 1
    assert "Could not create the benchmark runner" in "".join(capsys.readouterr())
    jm.submit_job.assert_not_called()
    jm.deploy_scripts_configmap.assert_not_called()


# ---------------------------------------------------- result check (deviation)


def test_settled_needs_every_row_through_silver_and_a_gold_read_after():
    ev = {job: parse_events(log, job) for job, log in HEALTHY.items()}
    bronze_rows = 20 * 26_000
    ok, why = settle_state(ev, bronze_rows + 1)
    assert not ok and "bronze has" in why
    ok, why = settle_state(ev, bronze_rows)
    assert not ok and "no gold refresh has read silver" in why  # last gold read at 401 s
    short = silver_log([(t, 52_000, 51_000) for t in range(10, 540, 60)])
    ok, why = settle_state(
        {**ev, "silver-stream": parse_events(short, "silver-stream")}, bronze_rows
    )
    assert not ok and "silver has committed" in why
    ev["gold-refresh"] = parse_events(gold_log([(599, 510_000, 40, False)]), "gold-refresh")
    ok, why = settle_state(ev, bronze_rows)
    assert ok, why
    # A drain: bronze stopped (its log gone) after taking its rows. Silver
    # must still have committed every one of them.
    drained = {k: v for k, v in ev.items() if k != "bronze-ingest"}
    assert settle_state(drained, 0, bronze_rows=bronze_rows)[0]
    ok, why = settle_state(drained, 0, bronze_rows=bronze_rows + 1)
    assert not ok and "silver has committed" in why
    # A last bronze batch written while it stopped: silver is transforming it.
    last = drained["silver-stream"][-1]
    in_flight = StreamEvent(last.at, "transform", 999, rows=1_000)
    drained["silver-stream"] = [*drained["silver-stream"], in_flight]
    ok, why = settle_state(drained, 0, bronze_rows=bronze_rows)
    assert not ok and "not committed yet" in why


def test_continuous_results_are_established_only_by_a_result_check():
    from lakebench.metrics.experiment import _continuous_results

    run = MagicMock(continuous=None)
    assert "no end-of-run result check" in _continuous_results(run)["not_checked"]
    run.continuous = {"result_check": {"not_checked": "the corpus did not settle: x"}}
    assert "did not settle" in _continuous_results(run)["not_checked"]
    run.continuous = {"result_check": {"query_set_id": "qs8-x", "fingerprints": {"Q1": {"a": 1}}}}
    res = _continuous_results(run)
    assert res["fingerprints"] == {"Q1": {"a": 1}} and "not_checked" not in res


def test_stored_reference_refuses_a_continuous_run_without_checked_results():
    from lakebench.metrics.experiment import identity, stored_identity_refusals
    from tests.conftest import stub_experiment

    ref = stub_experiment(["Q1"], mode="sustained")
    run = stub_experiment(["Q1"], mode="sustained")
    assert (
        stored_identity_refusals(
            identity(ref), {"Q1": ref["results"]["fingerprints"]["Q1"]}, run, "baseline"
        )
        == []
    )
    run["results"] = {"fingerprints": {}, "not_checked": "continuous: the corpus did not settle"}
    reasons = stored_identity_refusals(
        identity(ref), ref["results"]["fingerprints"], run, "baseline"
    )
    assert any("comparability not established" in r for r in reasons)


# ------------------------------------------------ end to end (mocked cluster)


class _Clock:
    """time.time / time.sleep / datetime.now on one fake clock."""

    def __init__(self):
        self.t = 1_000_000.0
        self.epoch = datetime(2026, 9, 27, 4, 0, 0, tzinfo=timezone.utc)

    def time(self):
        return self.t

    def sleep(self, s):
        self.t += max(0.0, float(s))

    def now_utc_naive(self):
        return (self.epoch + timedelta(seconds=self.t - 1_000_000.0)).replace(tzinfo=None)


def _drive(
    monkeypatch, tmp_path, make_logs, *, duration=120, clock_offset=None, runner=None, dg_rows=0
):
    """_run_sustained on a c360 config with every cluster edge mocked and a
    fake clock. *make_logs(job, window_end)* builds each driver log when the
    CLI reads it (right after the window closes). Returns (exit code or
    None, the run's continuous record)."""
    from lakebench.cli import _sustained
    from lakebench.modules.pipeline_engines.spark.job import JobState, JobStatus
    from tests.conftest import make_config

    monkeypatch.chdir(tmp_path)
    clock = _Clock()

    class _DT(datetime):
        @classmethod
        def now(cls, tz=None):
            n = clock.now_utc_naive()
            return n.replace(tzinfo=timezone.utc).astimezone(tz) if tz else n

    monkeypatch.setattr(_sustained, "time", clock)
    monkeypatch.setattr(_sustained, "datetime", _DT)
    op = MagicMock()
    op.check_status.return_value = MagicMock(ready=True, version="2.5.1")
    op.ensure_namespace_watched.return_value = MagicMock(watching_namespace=True)
    monkeypatch.setattr("lakebench.spark.SparkOperatorManager", lambda **kw: op)
    monkeypatch.setattr(_sustained, "get_k8s_client", lambda **kw: MagicMock())
    jm = MagicMock()
    jm.deploy_scripts_configmap.return_value = True
    jm.budget_warnings = []
    jm.submit_job.return_value = MagicMock(state=JobState.RUNNING, executor_count=2)
    jm.get_job_status.side_effect = lambda n: JobStatus(name=n, state=JobState.RUNNING, message="")
    monkeypatch.setattr("lakebench.engine.get_engine", lambda c, k: jm)
    mon = MagicMock()
    mon.wait_for_completion.return_value = MagicMock(success=True, elapsed_seconds=5.0)

    def until_running(name, timeout_seconds=0, on_status=None):
        if on_status is not None:
            on_status(JobStatus(name=name, state=JobState.RUNNING, message=""), 0.0)
        return MagicMock(success=True)

    mon.wait_until_running.side_effect = until_running
    mon._get_driver_logs.side_effect = lambda name, tail_lines=None: make_logs(
        name.removeprefix("lakebench-"), clock.now_utc_naive()
    )
    monkeypatch.setattr("lakebench.spark.SparkJobMonitor", lambda *a, **kw: mon)
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda c, **kw: MagicMock())
    dg = MagicMock()
    from lakebench.deploy import DeploymentStatus

    dg.deploy.return_value = MagicMock(status=DeploymentStatus.SUCCESS)
    monkeypatch.setattr("lakebench.deploy.DatagenDeployer", lambda e, **kw: dg)
    monkeypatch.setattr("lakebench.deploy.datagen.stop_previous_datagen", lambda c: None)
    monkeypatch.setattr("lakebench.deploy.datagen.end_continuous_datagen", lambda c: True)
    # The window opens at datagen's first file in bronze.
    monkeypatch.setattr(_sustained, "_wait_for_bronze_data", lambda *a, **kw: True)
    monkeypatch.setattr(_sustained, "_require_reset_ownership", lambda c: None)
    monkeypatch.setattr(_sustained, "_stop_leftover_streams", lambda *a: None)
    monkeypatch.setattr(_sustained, "_reset_continuous_state", lambda c, clear_raw: None)
    monkeypatch.setattr(_sustained, "_c360_existing_state", lambda c, clear_raw: [])
    monkeypatch.setattr(_sustained, "_datagen_released", lambda *a, **kw: True)
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: MagicMock())
    monkeypatch.setattr(_sustained, "_collect_platform_metrics", lambda *a, **kw: None)
    storage = MagicMock()
    monkeypatch.setattr("lakebench.metrics.MetricsStorage", lambda *a, **kw: storage)
    monkeypatch.setattr(_sustained, "write_run_report", lambda *a, **kw: None)
    monkeypatch.setattr(_sustained, "cluster_clock_offset_seconds", lambda: clock_offset)
    if runner is not None:
        monkeypatch.setattr("lakebench.benchmark.BenchmarkRunner", lambda c, **kw: runner)
    monkeypatch.setattr(
        "lakebench.metrics.datagen_aggregator.collect_from_k8s",
        lambda **kw: MagicMock(data_quality="complete", total_rows_written=dg_rows),
    )
    cfg = make_config(
        name="c360-window",
        architecture={
            "pipeline": {
                "mode": "continuous",
                "continuous": {"gold_refresh_interval": "30 seconds"},
            }
        },
    )
    code = None
    try:
        _sustained._run_sustained(cfg, tmp_path / "cfg.yaml", 60, runner is None, duration)
    except typer.Exit as e:
        code = e.exit_code
    saved = storage.save_run.call_args[0][0]
    return code, saved


def _rel(end, offset_s):
    return (end + timedelta(seconds=offset_s)).isoformat()


def _logs_relative_to(end, spec):
    """Rebuild a HEALTHY/DISCOVERY-shaped log with times relative to *end*,
    the window's end (offsets in seconds, negative = earlier)."""
    job, rows = spec
    lines = []
    if job == "bronze-ingest":
        for i, (t, n) in enumerate(rows):
            lines += [
                f"[lb] {_rel(end, t)} - Batch {i}: writing {n:,} rows to ice.bronze.raw",
                f"[lb] {_rel(end, t + 1)} - Batch {i}: committed in 1.0s",
            ]
    elif job == "silver-stream":
        for i, (t, n) in enumerate(rows):
            lines += [
                f"[lb] {_rel(end, t)} - Batch {i}: transforming {n:,} rows",
                f"[lb] {_rel(end, t + 1)} - Batch {i}: {n - 10:,} rows after transforms",
                f"[lb] {_rel(end, t + 2)} - Batch {i}: committed to ice.silver.t in 2.0s",
            ]
    else:
        for i, (t, n, fresh, idle) in enumerate(rows, start=1):
            tag = " (silver idle)" if idle else ""
            lines += [
                f"[lb] {_rel(end, t)} - Cycle {i}: aggregating {n:,} Silver records",
                f"[lb] {_rel(end, t + 1)} - Cycle {i}: data freshness {fresh}s{tag}",
                f"[lb] {_rel(end, t + 2)} - Cycle {i}: refreshed ice.gold.g in 2.0s (14 KPI records)",
            ]
    return "\n".join(lines) + "\n"


def test_degenerate_run_exits_nonzero_and_records_why(monkeypatch, tmp_path):
    def logs(job, end):
        return _logs_relative_to(
            end,
            {
                "bronze-ingest": (job, [(-700, 775_000), (-670, 775_000)]),
                "silver-stream": (job, [(-100, 1_550_000)]),
                "gold-refresh": (job, [(-60, 1_549_990, 322, False), (-10, 1_549_990, 372, True)]),
            }[job],
        )

    code, saved = _drive(monkeypatch, tmp_path, logs)
    assert code == 1
    problems = saved.continuous["gate_problems"]
    assert any("before the window opened" in p for p in problems)
    assert saved.success is False
    bronze = next(s for s in saved.streaming if s.job_type == "bronze-ingest")
    assert bronze.window_input_rows == 0 and bronze.pre_window_input_rows == 1_550_000


def test_healthy_run_passes_with_window_scores(monkeypatch, tmp_path):
    def logs(job, end):
        return _logs_relative_to(
            end,
            {
                "bronze-ingest": (job, [(t, 10_000) for t in range(-115, 0, 10)]),
                "silver-stream": (job, [(t, 20_000) for t in range(-110, 0, 20)]),
                "gold-refresh": (
                    job,
                    [(-90, 40_000, 20, False), (-50, 80_000, 25, False), (-10, 110_000, 22, False)],
                ),
            }[job],
        )

    code, saved = _drive(monkeypatch, tmp_path, logs)
    assert code is None, saved.continuous.get("gate_problems")
    assert saved.success is True
    assert saved.continuous["gate_problems"] == []
    assert saved.continuous["result_check"] == {"not_checked": "no benchmark (--skip-benchmark)"}
    gold = next(s for s in saved.streaming if s.job_type == "gold-refresh")
    assert gold.window_new_data_cycles == 3 and gold.freshness_seconds == 25


# ------------------------------------------------------ review fixes (pinned)


def test_one_late_bronze_batch_after_a_drained_corpus_fails():
    # Reviewer shape: 14.7M rows before the window, one 1,000-row batch at
    # +5 s, silver chewing its backlog, gold growing on it.
    logs = {
        "bronze-ingest": bronze_log([(-300, 14_700_000), (5, 1_000)]),
        "silver-stream": silver_log([(t, 3_000_000, 2_900_000) for t in (10, 70, 130, 190)]),
        "gold-refresh": gold_log([(100, 6_000_000, 40, False), (400, 12_000_000, 45, False)]),
    }
    probs = window_gate_problems(_stats(logs), 600)
    assert any("data stopped arriving" in p for p in probs)
    assert window_gate_problems(_stats(HEALTHY), 600) == []


def test_silver_backlog_before_bronze_arrives_is_not_continuous():
    logs = dict(HEALTHY)
    logs["bronze-ingest"] = bronze_log([(t, 26_000) for t in range(400, 600, 30)])
    logs["silver-stream"] = silver_log([(t, 52_000, 51_000) for t in (10, 70, 130, 450)])
    probs = window_gate_problems(_stats(logs))
    assert any("silver committed 1 micro-batch" in p for p in probs)


def test_empty_silver_gold_ticks_do_not_count_as_new_data():
    # AML: "Silver table is empty, skipping" logs no aggregate line but the
    # tick still logs "refreshed".
    lines = []
    for i, t in enumerate((60, 360), start=1):
        lines.append(_line(t, f"Cycle {i}: Silver table is empty, skipping"))
        lines.append(_line(t + 2, f"Cycle {i}: refreshed gold.alerts in 2.0s"))
    lines.append(_line(500, "Cycle 3: aggregating 5,000 Silver records"))
    lines.append(_line(501, "Cycle 3: data freshness 30s"))
    lines.append(_line(502, "Cycle 3: refreshed gold.alerts in 2.0s"))
    st = window_stats(parse_events("\n".join(lines), "gold-refresh"), "gold-refresh", W0, W1)
    assert st["window_commits"] == 3 and st["window_new_data_cycles"] == 1


def test_aml_freshness_drops_cycles_after_silver_stops_growing():
    # AML gold never tags "(silver idle)": the trailing cycles whose silver
    # count did not grow are the idle tail.
    logs = gold_log(
        [(60, 1000, 40, False), (160, 2000, 45, False), (260, 2000, 145, False)]
    ).replace(" (silver idle)", "")
    st = window_stats(parse_events(logs, "gold-refresh"), "gold-refresh", W0, W1)
    assert st["trailing_idle_cycles"] == 1
    assert st["freshness_active_seconds"] == 45


def test_totals_are_cut_at_the_window_end():
    # A stall from 450 s; the last rows land after the window closed but
    # before the logs were read. They must not make the run "drained".
    log = bronze_log([(t, 10_000) for t in range(0, 450, 30)] + [(610, 200_000)])
    b = StreamingJobMetrics(job_name="b", job_type="bronze-ingest")
    b.apply_window(log, W0, W1)
    assert b.total_rows_processed == 15 * 10_000
    s = StreamingJobMetrics(job_name="s", job_type="silver-stream")
    s.apply_window(silver_log([(100, 50_000, 49_000), (700, 100_000, 99_000)]), W0, W1)
    assert s.total_rows_processed == 50_000 and s.committed_rows == 50_000


def test_stage_rate_is_windowed_after_compute_derived():
    st = _stage("bronze", input_rows=1_000_000, window_input_rows=600_000)
    st.compute_derived()
    assert st.throughput_rows_per_second == pytest.approx(1000.0)


def test_drained_rps_is_gated_when_arrival_lasted_the_window():
    from lakebench.metrics.continuous_window import drained_rps_excluded

    assert drained_rps_excluded(True, 0.95) is None
    assert "short arrival" in drained_rps_excluded(True, 0.5)
    assert "lower bound" in drained_rps_excluded(True, None)  # pre-window record
    assert drained_rps_excluded(False, 0.1) is None


def test_a_stream_restarted_inside_the_window_fails():
    from lakebench.cli._sustained import end_of_window_problems
    from lakebench.spark.job import JobState, JobStatus

    jm = MagicMock()
    jm.get_job_status.side_effect = lambda name: JobStatus(
        name=name, state=JobState.RUNNING, message="", driver_pod=f"{name}-driver-2"
    )
    opened = {"silver-stream": ("lakebench-silver-stream-driver-1", 1, None)}
    probs = end_of_window_problems(jm, ["silver-stream"], opened)
    assert len(probs) == 1 and "restarted inside the window" in probs[0]


def test_a_window_too_short_for_two_gold_refreshes_is_refused():
    from lakebench.cli._sustained import short_window_problem
    from tests.conftest import make_config

    cfg = make_config()  # gold_refresh_interval 5 minutes
    assert "at least 900s" in short_window_problem(cfg, 600)
    assert short_window_problem(cfg, 900) is None


def test_aml_continuous_results_are_not_established(monkeypatch, tmp_path):
    # Settling is not attempted for AML: detection and TM passes are timed.
    from lakebench.cli import _sustained

    src = __import__("inspect").getsource(_sustained._run_sustained)
    assert "not a function of the corpus alone" in src


def test_an_error_after_the_window_fails_the_run_and_stops_streams(monkeypatch, tmp_path):
    from lakebench.cli import _sustained

    def logs(job, end):
        return _logs_relative_to(
            end,
            {
                "bronze-ingest": (job, [(t, 10_000) for t in range(-115, 0, 10)]),
                "silver-stream": (job, [(t, 20_000) for t in range(-110, 0, 20)]),
                "gold-refresh": (
                    job,
                    [(-90, 40_000, 20, False), (-50, 80_000, 25, False), (-10, 110_000, 22, False)],
                ),
            }[job],
        )

    stopped = []
    monkeypatch.setattr(_sustained, "_stop_streams", lambda k, ns, sub: stopped.append(len(sub)))

    def boom(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr("lakebench.metrics.continuous_window.window_gate_problems", boom)
    # An error that is not an interrupt still propagates and still stops
    # the streams (by name, in the finally). An interrupt is sealed and its
    # streams stopped by uid instead: tests/test_run_interrupt.py.
    with pytest.raises(RuntimeError, match="boom"):
        _drive(monkeypatch, tmp_path, logs)
    assert stopped == [3]


def test_the_window_is_shifted_to_the_cluster_clock(monkeypatch, tmp_path):
    # Pods 60 s ahead of this host. Bronze wrote 30 s before the window
    # opened (true time), which the pod logged 30 s after the unshifted
    # window's start: without the shift those rows would count as arriving.
    def logs(job, end):
        return _logs_relative_to(
            end,
            {
                "bronze-ingest": (job, [(-90, 775_000), (-85, 775_000)]),
                "silver-stream": (job, [(-100, 1_550_000)]),
                "gold-refresh": (job, [(-60, 1_549_990, 322, False)]),
            }[job],
        )

    code, saved = _drive(monkeypatch, tmp_path, logs, clock_offset=60.0)
    assert code == 1
    assert saved.continuous["window"]["cluster_clock_offset_seconds"] == 60.0
    bronze = next(s for s in saved.streaming if s.job_type == "bronze-ingest")
    assert bronze.window_input_rows == 0
    _, unshifted = _drive(monkeypatch, tmp_path, logs, clock_offset=None)
    b2 = next(s for s in unshifted.streaming if s.job_type == "bronze-ingest")
    assert b2.window_input_rows == 1_550_000


def _settling_logs(job, end):
    return _logs_relative_to(
        end,
        {
            "bronze-ingest": (job, [(t, 10_000) for t in range(-115, 0, 10)]),
            "silver-stream": (job, [(t, 20_000) for t in range(-110, 0, 20)]),
            "gold-refresh": (
                job,
                [(-90, 40_000, 20, False), (-50, 80_000, 25, False), (-5, 119_940, 22, False)],
            ),
        }[job],
    )


def _runner(fail=()):
    from lakebench.benchmark.fingerprint import fingerprint_rows

    queries = []
    for name in ("Q1_a", "Q9_b"):
        ok = name not in fail
        q = MagicMock(success=ok, result_fingerprint=fingerprint_rows([(name, 1)]) if ok else None)
        q.query.name = name
        q.to_dict.return_value = {"name": name, "success": ok, "rows_returned": 1 if ok else 0}
        queries.append(q)
    r = MagicMock()
    r.run_power.return_value = MagicMock(queries=queries)
    return r


def test_settled_run_records_fingerprinted_results(monkeypatch, tmp_path):
    code, saved = _drive(
        monkeypatch, tmp_path, _settling_logs, runner=_runner(), dg_rows=12 * 10_000
    )
    assert code is None, saved.continuous
    assert saved.continuous["settle"]["settled"] is True
    fps = saved.continuous["result_check"]["fingerprints"]
    assert set(fps) == {"Q1_a", "Q9_b"}
    results = saved.experiment_block()["results"]
    assert results["fingerprints"] == fps and "not_checked" not in results


def test_a_failed_result_check_query_fails_the_run(monkeypatch, tmp_path):
    code, saved = _drive(
        monkeypatch, tmp_path, _settling_logs, runner=_runner(fail=("Q1_a",)), dg_rows=120_000
    )
    assert code == 1 and saved.success is False


def test_an_unsettled_corpus_leaves_results_not_established(monkeypatch, tmp_path):
    monkeypatch.setattr("lakebench.cli._sustained.SETTLE_MAX_SECONDS", 1)
    code, saved = _drive(monkeypatch, tmp_path, _settling_logs, runner=_runner(), dg_rows=10**9)
    assert code is None
    assert "settle limit" in saved.continuous["result_check"]["not_checked"]
    assert "not_checked" in saved.experiment_block()["results"]


def test_a_rerun_with_the_same_pod_name_is_caught_by_submission_time():
    from lakebench.cli._sustained import end_of_window_problems
    from lakebench.spark.job import JobState, JobStatus

    jm = MagicMock()
    jm.get_job_status.side_effect = lambda name: JobStatus(
        name=name,
        state=JobState.RUNNING,
        message="",
        driver_pod="lakebench-silver-stream-driver",
        submission_attempts=1,
        start_time="2026-09-27T04:14:00Z",
    )
    opened = {"silver-stream": ("lakebench-silver-stream-driver", 1, "2026-09-27T04:03:00Z")}
    probs = end_of_window_problems(jm, ["silver-stream"], opened)
    assert len(probs) == 1 and "resubmitted inside the window" in probs[0]
    same = {"silver-stream": ("lakebench-silver-stream-driver", 1, "2026-09-27T04:14:00Z")}
    assert end_of_window_problems(jm, ["silver-stream"], same) == []


# ------------------------------------------- default path keeps data arriving


def _cont_cfg(scale=1, schema="customer360", **sustained):
    from tests.conftest import make_config

    return make_config(
        workload={"schema": schema, "datagen": {"scale": scale}},
        architecture={"pipeline": {"mode": "continuous", "continuous": sustained}},
    )


@pytest.mark.parametrize(
    "scale,schema,want",
    [(1, "customer360", 2), (10, "customer360", 22), (100, "customer360", 50), (1, "financial", 1)],
)
def test_default_trickle_keeps_data_arriving_through_the_window(scale, schema, want):
    from lakebench.cli._sustained import resolve_trickle

    t = resolve_trickle(_cont_cfg(scale, schema), 1800)
    assert t["source"] == "auto" and t["value"] == want and t["problem"] is None
    assert t["arrival_seconds"] >= 1800


def test_an_explicit_trickle_that_drains_early_is_refused_with_the_setting():
    from lakebench.cli._sustained import resolve_trickle

    t = resolve_trickle(_cont_cfg(1, max_files_per_trigger=50), 600)
    assert "max_files_per_trigger to 6 or lower" in t["problem"]
    assert resolve_trickle(_cont_cfg(1, max_files_per_trigger=6), 600)["problem"] is None


def test_a_refused_trickle_starts_nothing(monkeypatch, tmp_path, capsys):
    from lakebench.cli import _sustained

    monkeypatch.chdir(tmp_path)
    jm = MagicMock()
    monkeypatch.setattr("lakebench.engine.get_engine", lambda c, k: jm)
    op = MagicMock()
    monkeypatch.setattr("lakebench.spark.SparkOperatorManager", lambda **kw: op)
    with pytest.raises(typer.Exit) as exc:
        # A finite corpus (--skip-generate) is the one a trickle reads.
        _sustained._run_sustained(
            _cont_cfg(1, max_files_per_trigger=50),
            tmp_path / "c.yaml",
            60,
            True,
            900,
            skip_generate=True,
        )
    assert exc.value.exit_code == 2  # usage: the window and trickle do not fit
    op.check_status.assert_not_called()
    assert "max_files_per_trigger" in "".join(capsys.readouterr())


def test_the_runs_own_datagen_sets_no_trickle(monkeypatch, tmp_path):
    """Datagen generates for the whole window, so bronze reads with no
    per-trigger limit, and the record says so."""
    code, saved = _drive(monkeypatch, tmp_path, _settling_logs)
    trickle = saved.continuous["trickle"]
    assert trickle["source"] == "none" and trickle["value"] is None
    assert saved.config_snapshot["sustained"]["max_files_per_trigger"] is None
    assert saved.config_snapshot["datagen_continuous"] is True


# ------------------------------------- ingest_ratio against what was released


def _trickle_pb(bronze_rows, trickle_start, datagen_rows=1_000_000, files=100, mpt=2):
    t0 = datetime(2026, 9, 27, tzinfo=timezone.utc)
    pb = PipelineBenchmark(
        run_id="t",
        deployment_name="t",
        pipeline_mode="sustained",
        start_time=t0,
        end_time=t0 + timedelta(seconds=1800),
        success=True,
        stages=[
            StageMetrics(
                stage_name="bronze",
                stage_type="streaming",
                engine="spark",
                elapsed_seconds=1800,
                input_rows=bronze_rows,
                window_input_rows=bronze_rows,
                pre_window_input_rows=0,
                last_write_offset_seconds=1790,
                trickle_start_offset_seconds=trickle_start,
            )
        ],
        config_snapshot={
            "datagen_output_rows": datagen_rows,
            "datagen_output_files": files,
            "sustained": {"bronze_trigger_interval": "30 seconds", "max_files_per_trigger": mpt},
        },
    )
    pb.compute_aggregates()
    return pb


def test_a_trickle_that_has_not_released_the_corpus_is_not_saturation():
    # A default-shaped run: the trickle (2 files per 30 s from t=0) had
    # released 61 x 2 = 122 of 150 files (1,220,000 rows) by the window's
    # end, and bronze took all of them: 81% of the corpus, 100% of arrival.
    pb = _trickle_pb(1_220_000, 0.0, datagen_rows=1_500_000, files=150)
    assert pb.corpus_ingest_ratio == pytest.approx(0.8133, abs=1e-3)
    assert pb.released_rows == 1_220_000
    assert pb.ingest_ratio == pytest.approx(1.0)
    assert pb.pipeline_saturated is False
    scores = pb.to_dict()["scores"]
    assert scores["corpus_ingest_ratio"] == pytest.approx(0.8133, abs=1e-3)


def test_bronze_behind_what_was_released_is_saturation():
    pb = _trickle_pb(900_000, 0.0, datagen_rows=1_500_000, files=150)
    assert pb.ingest_ratio == pytest.approx(900_000 / 1_220_000)
    assert pb.pipeline_saturated is True


def test_without_a_file_count_the_corpus_ratio_stands():
    pb = _trickle_pb(1_220_000, 0.0, datagen_rows=1_500_000, files=0)
    assert pb.released_rows is None
    assert pb.ingest_ratio == pytest.approx(pb.corpus_ingest_ratio)


def _datagen_pb(bronze_rows, datagen_rows, datagen_elapsed_s):
    """The run's own continuous datagen: no trickle, a 900 s window, 30 s
    bronze trigger (the shape of a scale-1 matrix run, 2026-10-07)."""
    t0 = datetime(2026, 10, 7, tzinfo=timezone.utc)
    pb = PipelineBenchmark(
        run_id="t",
        deployment_name="t",
        pipeline_mode="sustained",
        start_time=t0,
        end_time=t0 + timedelta(seconds=900),
        success=True,
        stages=[
            StageMetrics(
                stage_name="bronze",
                stage_type="streaming",
                engine="spark",
                elapsed_seconds=900,
                input_rows=bronze_rows,
                window_input_rows=bronze_rows,
                pre_window_input_rows=0,
                last_write_offset_seconds=890,
                trickle_start_offset_seconds=5.0,
            )
        ],
        config_snapshot={
            "datagen_output_rows": datagen_rows,
            "datagen_output_files": 1404,
            "datagen_continuous": True,
            "datagen_elapsed_s": datagen_elapsed_s,
            "sustained": {"bronze_trigger_interval": "30 seconds", "max_files_per_trigger": None},
        },
    )
    pb.compute_aggregates()
    return pb


def test_continuous_datagen_bronze_one_trigger_behind_kept_up():
    # Datagen wrote 21.75M rows over 906.6 s; bronze's last trigger and the
    # seconds before the pods saw the stop marker left 1.25M rows untaken.
    # Against datagen's total that read 0.94, a saturated pipeline.
    pb = _datagen_pb(20_494_593, 21_749_364, 906.625)
    assert pb.released_rows == round(21_749_364 / 906.625 * 870)
    assert pb.ingest_ratio == pytest.approx(0.982, abs=1e-3)
    assert pb.pipeline_saturated is False
    assert pb.datagen_ahead is False
    assert pb.backlog_rows == 21_749_364 - 20_494_593


def test_continuous_datagen_a_real_backlog_is_still_ahead():
    pb = _datagen_pb(15_000_000, 21_749_364, 906.625)
    assert pb.ingest_ratio < 0.95
    assert pb.datagen_ahead is True


def test_every_streams_submission_failures_are_recorded_while_another_is_waited_on():
    """lb16-cf: the streams are waited on one at a time, so silver-stream's
    and gold-refresh's failures happened while bronze-ingest was waited on
    and were never recorded. The callback polls every stream not yet running."""
    from lakebench.cli._sustained import StreamStartWatch, watch_all_streams
    from lakebench.spark.job import JobState, JobStatus

    now = [0.0]
    watches = {
        n: StreamStartWatch(n, clock=lambda: now[0])
        for n in ("bronze-ingest", "silver-stream", "gold-refresh")
    }
    failed = JobState.SUBMISSION_FAILED

    def st(state, attempt=0):
        return JobStatus(name="x", state=state, message=_MAVEN, submission_attempts=attempt)

    others = {
        "lakebench-silver-stream": [st(failed, 1), st(JobState.SUBMITTED), st(JobState.RUNNING)],
        "lakebench-gold-refresh": [st(failed, 1), st(failed, 2), st(JobState.RUNNING)],
    }
    jm = MagicMock()
    jm.get_job_status.side_effect = lambda name: others[name].pop(0)
    cb = watch_all_streams(jm, watches, "bronze-ingest")
    for t, own in ((0.0, JobState.SUBMITTED), (10.0, JobState.SUBMITTED), (20.0, JobState.RUNNING)):
        now[0] = t
        cb(st(own), t)
    assert watches["bronze-ingest"].failures == []
    silver, gold = watches["silver-stream"], watches["gold-refresh"]
    assert [f["attempt"] for f in silver.failures] == [1]
    assert [f["attempt"] for f in gold.failures] == [1, 2]
    assert silver.failures[0]["lost_seconds"] == 10.0
    assert [f["lost_seconds"] for f in gold.failures] == [10.0, 10.0]
    assert gold.submission_retry_seconds == 20.0
    assert silver.running_at and gold.running_at
    # A stream seen running stays polled: a failed resubmission is recorded
    # and running_at moves to when it ran again.
    first_up = silver.running_at
    others["lakebench-silver-stream"] = [st(failed, 2), st(JobState.RUNNING)]
    others["lakebench-gold-refresh"] = [st(JobState.RUNNING), st(JobState.RUNNING)]
    now[0] = 30.0
    cb(st(JobState.RUNNING), 30.0)
    assert [f["attempt"] for f in silver.failures] == [1, 2] and silver.running_at is None
    now[0] = 40.0
    cb(st(JobState.RUNNING), 40.0)
    assert silver.failures[1]["lost_seconds"] == 10.0
    assert silver.running_at is not None and silver.running_at >= first_up


def test_a_failure_open_when_the_wait_ends_is_closed_with_its_time():
    from lakebench.cli._sustained import StreamStartWatch
    from lakebench.spark.job import JobState, JobStatus

    now = [100.0]
    w = StreamStartWatch("gold-refresh", clock=lambda: now[0])
    w(JobStatus(name="g", state=JobState.SUBMISSION_FAILED, message="m", submission_attempts=1), 0)
    now[0] = 145.0
    w.close()
    assert w.failures[0]["lost_seconds"] == 45.0 and w.submission_retry_seconds == 45.0


def test_stream_submission_failures_reach_the_stage_and_the_report(tmp_path):
    from lakebench.metrics.collector import (
        PipelineMetrics,
        StreamingJobMetrics,
        build_pipeline_benchmark,
    )
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.generator import ReportGenerator

    m = PipelineMetrics(
        run_id="20260927-000000-dddddd",
        deployment_name="d",
        start_time=datetime(2026, 9, 27),
        success=True,
        config_snapshot={"mode": "continuous", "table_format": "iceberg"},
    )
    fail = {"at": "t", "attempt": 1, "reason": "r", "lost_seconds": 42.0}
    m.streaming = [
        StreamingJobMetrics(job_name="lakebench-bronze-ingest", job_type="bronze-ingest"),
        StreamingJobMetrics(
            job_name="lakebench-gold-refresh",
            job_type="gold-refresh",
            submission_failures=[fail, {**fail, "attempt": 2, "lost_seconds": 18.0}],
        ),
    ]
    pb = build_pipeline_benchmark(m)
    gold = next(s for s in pb.stages if s.submission_failures)
    assert gold.submission_retry_seconds == 60.0 and len(gold.submission_failures) == 2
    m.pipeline_benchmark = pb
    MetricsStorage(tmp_path).save_run(m)
    html = (
        ReportGenerator(metrics_dir=tmp_path, output_dir=tmp_path)
        .generate_report(m.run_id)
        .read_text()
    )
    assert "2 failed submissions, 60s before it ran" in html


def test_stream_failures_recorded_without_lost_seconds_say_so(tmp_path):
    """A record from before lost_seconds was kept reads unknown, not 0s."""
    from lakebench.metrics.collector import (
        PipelineMetrics,
        StreamingJobMetrics,
        build_pipeline_benchmark,
    )
    from lakebench.metrics.storage import MetricsStorage
    from lakebench.reports.generator import ReportGenerator

    m = PipelineMetrics(
        run_id="20260927-000000-eeeeee",
        deployment_name="d",
        start_time=datetime(2026, 9, 27),
        success=True,
        config_snapshot={"mode": "continuous", "table_format": "iceberg"},
    )
    m.streaming = [
        StreamingJobMetrics(
            job_name="lakebench-bronze-ingest",
            job_type="bronze-ingest",
            submission_failures=[{"at": "t", "attempt": 1, "reason": "r"}],
        )
    ]
    m.pipeline_benchmark = build_pipeline_benchmark(m)
    MetricsStorage(tmp_path).save_run(m)
    html = (
        ReportGenerator(metrics_dir=tmp_path, output_dir=tmp_path)
        .generate_report(m.run_id)
        .read_text()
    )
    assert "1 failed submission, time lost not recorded" in html
    assert "0s before it ran" not in html


def _header(fmt: str, engine: str, **kw) -> list[tuple[str, str]]:
    from lakebench.cli._sustained import maintenance_schedule_lines

    args = {
        "skip_maintenance": False,
        "retention_interval": 600,
        "retention_source": "auto",
        "retention_threshold": "30m",
        "compaction_configured": True,
        "compaction_interval": 1200,
        "compaction_source": "auto",
        **kw,
    }
    return maintenance_schedule_lines(fmt, engine, **args)


def test_delta_run_header_names_delta_maintenance_not_iceberg():
    """lb16-cf: Delta runs printed "Iceberg retention: every 600s"."""
    trino = _header("delta", "trino")
    text = " | ".join(t for _, t in trino)
    assert "Iceberg" not in text
    assert "Delta VACUUM: every 600s (auto; retention 168h" in text
    assert "Delta OPTIMIZE: not run" in text
    assert trino[-1][0] == "warning" and "owner decision #46" in trino[-1][1]
    thrift = " | ".join(t for _, t in _header("delta", "spark-thrift"))
    assert "Delta VACUUM: not run (it OOMs Spark Thrift" in thrift and "Iceberg" not in thrift
    skipped = _header("delta", "trino", skip_maintenance=True)
    assert skipped[0][1] == "Delta VACUUM: disabled (--skip-maintenance)"
    assert "DuckDB" in _header("delta", "duckdb")[0][1]


def test_iceberg_run_header_is_unchanged():
    assert _header("iceberg", "trino") == [
        ("info", "Iceberg retention: every 600s (auto; threshold: 30m)"),
        ("info", "Iceberg compaction: every 1200s (auto)"),
    ]
    assert _header("iceberg", "trino", skip_maintenance=True) == [
        ("info", "Iceberg retention: disabled (--skip-maintenance)"),
        ("info", "Iceberg compaction: disabled (--skip-maintenance)"),
    ]
    assert _header("iceberg", "trino", compaction_configured=False) == [
        ("info", "Iceberg retention: every 600s (auto; threshold: 30m)"),
    ]
    assert "not run" in _header("iceberg", "duckdb")[0][1]


def test_result_check_counts_an_unfingerprinted_result_as_failed():
    """A query that ran but whose result could not be fingerprinted
    ({spec, error}: DuckDB lost its S3 connection re-reading silver,
    2026-10-07) has no answer to compare: it is failed, not fingerprinted."""
    from types import SimpleNamespace

    from lakebench.cli._sustained import continuous_result_check

    def qr(name, success, fp):
        return SimpleNamespace(
            query=SimpleNamespace(name=name),
            success=success,
            result_fingerprint=fp,
            to_dict=lambda: {"name": name},
        )

    ok = {"spec": "rf2", "rows": 1, "cols": 1, "exact": "ab"}
    result = SimpleNamespace(
        queries=[
            qr("Q1", True, ok),
            qr("Q2", True, {"spec": "rf2", "error": "IOException: IO Error"}),
            qr("Q6", False, None),
        ]
    )
    runner = SimpleNamespace(run_power=lambda **kw: result)
    record, _ = continuous_result_check(runner)
    assert record["failed"] == ["Q2", "Q6"]


def test_result_check_retries_a_transient_store_error_once_and_records_it():
    from types import SimpleNamespace

    from lakebench.cli._sustained import continuous_result_check

    ok = {"spec": "rf2", "rows": 1, "cols": 1, "exact": "ab"}

    def qr(name, success, fp=None, err=""):
        return SimpleNamespace(
            query=SimpleNamespace(name=name),
            success=success,
            result_fingerprint=fp,
            error_message=err,
            to_dict=lambda: {"name": name},
        )

    result = SimpleNamespace(
        queries=[
            qr("Q1", True, ok),
            # Ran, then the fingerprint run lost the S3 connection.
            qr("Q2", True, {"spec": "rf2", "error": "IO Error: Could not connect to server"}),
            # The engine pod died (OOM): not transient, not retried.
            qr("Q6", False, err='unable to upgrade connection: container not found ("duckdb")'),
        ]
    )
    calls = []

    def repeat(query, cache, iterations, timeout):
        calls.append(query.name)
        return qr(query.name, True)

    def fingerprint(results, timeout):
        for r in results:
            r.result_fingerprint = ok

    runner = SimpleNamespace(
        run_power=lambda **kw: result, _repeat_query=repeat, fingerprint_results=fingerprint
    )
    record, _ = continuous_result_check(runner)
    assert calls == ["Q2"]
    assert record["retried"] == ["Q2"]
    assert record["failed"] == ["Q6"]


def test_stream_marker_is_cleared_only_once_the_driver_pod_is_gone(monkeypatch):
    """A clean stop clears the AML stream's _STARTED marker so a later batch
    may rebuild silver; while the driver pod still exists it may write, so
    the marker stays."""
    from types import SimpleNamespace

    import lakebench.cli._sustained as sus
    import lakebench.deploy.datagen as dg

    s3 = MagicMock()
    monkeypatch.setattr(dg, "_s3_client_for", lambda cfg: s3)
    monkeypatch.setattr(sus, "_STREAM_POD_GONE_WAIT_S", 0)
    monkeypatch.setattr(sus.time, "sleep", lambda s: None)
    cfg = MagicMock()
    cfg.architecture.pipeline.sustained.checkpoint_base = "checkpoints"
    cfg.platform.storage.s3.buckets.silver = "ns-silver"
    k8s = MagicMock()

    k8s.get_pod_status.return_value = SimpleNamespace(exists=True)
    assert sus._clear_silver_stream_marker(cfg, k8s, "ns") is False
    s3.raw_client.delete_object.assert_not_called()

    k8s.get_pod_status.side_effect = RuntimeError("api down")
    assert sus._clear_silver_stream_marker(cfg, k8s, "ns") is False
    s3.raw_client.delete_object.assert_not_called()

    k8s.get_pod_status.side_effect = None
    k8s.get_pod_status.return_value = SimpleNamespace(exists=False)
    assert sus._clear_silver_stream_marker(cfg, k8s, "ns") is True
    s3.raw_client.delete_object.assert_called_once_with(
        Bucket="ns-silver", Key="checkpoints/silver-stream/_STARTED"
    )
