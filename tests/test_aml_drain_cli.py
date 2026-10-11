"""AML-6, CLI side: the drain request, the tick records, the covered score
call, `stop`'s drain and the window-end order (cli/_aml_post.py,
metrics/tick_records.py, cli/_sustained.py, cli/_cluster_ops.py)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from lakebench.cli import _aml_post as post
from lakebench.metrics import tick_records as tr
from lakebench.modules.pipeline_engines.spark.job import JobState, JobStatus

RUN = "20261003-120000-abc123"


def _cfg(schema="financial", base="lakebench-checkpoints"):
    return SimpleNamespace(
        name="t",
        get_namespace=lambda: "ns-t",
        platform=SimpleNamespace(
            storage=SimpleNamespace(
                s3=SimpleNamespace(
                    endpoint="http://10.0.1.50",
                    access_key="k",
                    secret_key="s",
                    region="us-east-1",
                    path_style=True,
                    ca_cert=None,
                    verify_ssl=False,
                    buckets=SimpleNamespace(bronze="b-bronze", silver="b-silver", gold="b-gold"),
                )
            )
        ),
        architecture=SimpleNamespace(
            pipeline=SimpleNamespace(sustained=SimpleNamespace(checkpoint_base=base)),
            workload=SimpleNamespace(schema_type=SimpleNamespace(value=schema)),
        ),
    )


def _lb(msg, ts="2026-10-03T12:00:00.000"):
    return f"[lb] {ts} - {msg}"


def _tick_lines(cycle, run=RUN, *, pins=("11", "12", "13", "14"), commit=("21", "22")):
    t, e, a, v = pins
    return [
        _lb(
            f"Cycle {cycle}: pinned txns={t} entities={e} accounts={a} versions={v} at=1.5 run={run}"
        ),
        _lb(f"Cycle {cycle}: committed alerts={commit[0]} status={commit[1]} run={run}"),
        _lb("Tick complete in 3.0s (gold.alerts rows: 9)"),
        _lb(f"Cycle {cycle}: completed run={run}", ts=f"2026-10-03T12:0{cycle}:00.000"),
    ]


BANNER = _lb("Gold Refresh (Financial) -- baseline + periodic detection")


def _drained_log(n=2, run=RUN):
    lines = [BANNER]
    for c in range(1, n + 1):
        lines += _tick_lines(c, run, pins=(str(10 + c), "12", "13", "14"))
    lines.append(_lb(f"Drain complete: last completed cycle {n} run={run}"))
    lines += ["INFO SparkContext: Successfully stopped SparkContext"] * 3
    return "\n".join(lines)


class _Calls:
    def __init__(self):
        self.calls: list[tuple] = []


class FakeLog:
    """Driver log reads: ``logs`` is the list of tails served in turn (the
    last repeats); the full read returns ``full`` (default: the tail). An
    entry that is an exception is raised instead."""

    def __init__(self, rec, logs, full=None):
        self.rec, self.logs, self.full, self.i = rec, logs, full, 0

    def __call__(self, tail, timeout=None):
        self.rec.calls.append(("logs", tail))
        if tail is None:
            out = self.full if self.full is not None else self.logs[min(self.i, len(self.logs) - 1)]
        else:
            out = self.logs[min(self.i, len(self.logs) - 1)]
            self.i += 1
        if isinstance(out, BaseException):
            raise out
        return out


class FakeS3:
    def __init__(self, rec, error=None):
        self.rec, self.error = rec, error
        self.raw_client = self

    def put_object(self, Bucket, Key, Body):  # noqa: N803 -- boto3's names
        self.rec.calls.append(("put", Bucket, Key, Body))
        if self.error:
            raise self.error


def _drain(
    monkeypatch,
    logs,
    *,
    full=None,
    state="RUNNING",
    s3_error=None,
    budget=60,
    before=None,
    make_log=None,
):
    rec = _Calls()
    monkeypatch.setattr(post, "_s3_client", lambda cfg: FakeS3(rec, s3_error))
    clock = {"t": 0.0}

    def sleep(s):
        rec.calls.append(("sleep", s))
        clock["t"] += s

    def app_state():
        if isinstance(state, BaseException):
            raise state
        return state

    result = post.request_drain(
        _cfg(),
        None,
        budget,
        run_id=RUN,
        read_log=(make_log or FakeLog)(rec, logs, full),
        app_state=app_state,
        before_write=before,
        poll_s=10,
        sleep=sleep,
        clock=lambda: clock["t"],
    )
    return result, rec


# --- request_drain ------------------------------------------------------------


def test_drain_writes_the_marker_then_polls_then_reads_the_full_log(monkeypatch):
    full = _drained_log(2)
    result, rec = _drain(monkeypatch, ["still ticking", full], full=full)
    assert result.state == "drained" and result.last_cycle == 2
    assert result.logs == full
    kinds = [c[0] for c in rec.calls]
    assert kinds.index("put") < kinds.index("logs")  # the marker comes first
    assert rec.calls[0] == (
        "put",
        "b-gold",
        "lakebench-checkpoints/gold-refresh/_lb_stop",
        RUN.encode(),
    )
    assert rec.calls[-1] == ("logs", None)  # full log once, at the end


def test_drain_ignores_another_runs_drain_line(monkeypatch):
    result, _rec = _drain(monkeypatch, [_drained_log(2, run="other-run")], budget=30)
    assert result.state == "timeout"


def test_drain_times_out_at_the_budget(monkeypatch):
    result, rec = _drain(monkeypatch, ["Cycle 4: pinned ..."], budget=30)
    assert result.state == "timeout" and result.waited_s == 30
    assert sum(1 for c in rec.calls if c[0] == "sleep") == 3


def test_failed_marker_write_is_no_marker_and_waits_for_nothing(monkeypatch):
    result, rec = _drain(monkeypatch, [_drained_log(1)], s3_error=RuntimeError("403"))
    assert result.state == "no_marker" and "403" in result.reason
    assert [c[0] for c in rec.calls] == ["put"]


def test_deleted_application_ends_the_wait(monkeypatch):
    result, _rec = _drain(monkeypatch, ["boom"], state=None)
    assert result.state == "driver_gone" and "deleted" in result.reason


def test_a_failed_driver_is_waited_for(monkeypatch):
    """Under restartPolicy Always a failed driver restarts and drains at
    start; FAILING or FAILED does not end the wait."""
    result, _rec = _drain(monkeypatch, ["boom"], state="FAILING", budget=30)
    assert result.state == "timeout"


def test_read_errors_are_not_yet_and_never_end_the_wait(monkeypatch):
    full = _drained_log(1)
    err = ConnectionResetError("api server rollout")
    result, _rec = _drain(monkeypatch, [err, err, full], full=full, state=TimeoutError("slow"))
    assert result.state == "drained" and result.last_cycle == 1


def test_a_failed_full_read_keeps_polling(monkeypatch):
    full = _drained_log(1)
    reads = {"n": 0}

    class Flaky(FakeLog):
        def __call__(self, tail, timeout=None):
            if tail is None:
                reads["n"] += 1
                if reads["n"] == 1:
                    raise ConnectionResetError("reset")
            return super().__call__(tail, timeout)

    result, _rec = _drain(monkeypatch, [full], full=full, make_log=Flaky)
    assert result.state == "drained" and reads["n"] == 2


def test_namespace_check_runs_before_the_write_and_propagates(monkeypatch):
    class Gone(Exception):
        pass

    def before():
        raise Gone()

    rec = _Calls()
    monkeypatch.setattr(post, "_s3_client", lambda cfg: FakeS3(rec))
    with pytest.raises(Gone):
        post.request_drain(
            _cfg(),
            None,
            60,
            run_id=RUN,
            read_log=FakeLog(rec, [""]),
            app_state=lambda: "RUNNING",
            before_write=before,
        )
    assert rec.calls == []


def test_marker_key_matches_the_driver_checkpoint():
    from lakebench.modules.pipeline_engines.spark.job import (
        gold_refresh_checkpoint_uri,
        gold_refresh_stop_marker,
    )

    for base in ("lakebench-checkpoints", "/x/y/", "a//b"):
        cfg = _cfg(base=base)
        uri = gold_refresh_checkpoint_uri(cfg)
        bucket, key = gold_refresh_stop_marker(cfg)
        # The driver reads CHECKPOINT_LOCATION.rstrip("/") + "/_lb_stop", and
        # Hadoop's Path drops empty segments.
        path = uri.rstrip("/") + "/_lb_stop"
        assert path.startswith(f"s3a://{bucket}/")
        want = "/".join(p for p in path[len(f"s3a://{bucket}/") :].split("/") if p)
        assert key == want


# --- tick records ---------------------------------------------------------------


def test_scored_tick_is_the_drain_cycle():
    parsed = tr.parse_tick_records(_drained_log(3), RUN)
    tick, why = tr.scored_tick(parsed)
    assert why == "" and tick["cycle"] == 3 and tick["pinned_txns"] == 13
    assert tick["completed_at"] == "2026-10-03T12:03:00.000Z"
    assert tr.ticks_unpinned(parsed["ticks"]) == 0
    assert [t["cycle"] for t in tr.tick_list(parsed["ticks"])] == [1, 2, 3]


def test_cycle_numbers_restart_per_driver_start():
    lines = [
        BANNER,
        *_tick_lines(1),
        *_tick_lines(2),
        BANNER,
        *_tick_lines(1, pins=("91", "12", "13", "14")),
    ]
    lines.append(_lb(f"Drain complete: last completed cycle 1 run={RUN}"))
    tick, why = tr.scored_tick(tr.parse_tick_records("\n".join(lines), RUN))
    assert why == "" and tick["pinned_txns"] == 91 and tick["start"] == 1


def test_drain_tick_scored_from_its_records_repeated_at_the_drain():
    """A driver log rotated during the drain tick keeps only its end; gold
    repeats the tick's records with each drain line, so its pins survive."""
    pinned, committed, completed = _tick_lines(3)[0], _tick_lines(3)[1], _tick_lines(3)[3]
    status = _lb(
        f"Cycle 3: rule-status W4_risk_propagation=error W3_round_tripping=skipped:path-cap run={RUN}"
    )
    # The log kept nothing of the tick itself; only the repeated records.
    lines = [
        pinned,
        status,
        committed,
        completed,
        _lb(f"Drain complete: last completed cycle 3 run={RUN}"),
    ]
    parsed = tr.parse_tick_records("\n".join(lines), RUN)
    tick, why = tr.scored_tick(parsed)
    assert why == "" and tick["cycle"] == 3 and tick["pinned_txns"] == 11
    assert tr.tick_list(parsed["ticks"])[-1]["rule_status"] == {
        "W4_risk_propagation": "error",
        "W3_round_tripping": "skipped:path-cap",
    }


@pytest.mark.parametrize(
    ("lines", "reason"),
    [
        ([BANNER, *_tick_lines(1)], "no drain line"),
        (
            [
                BANNER,
                _lb(
                    f"Drain complete: stop marker present at start; last completed cycle 0 run={RUN}"
                ),
            ],
            "before its first tick",
        ),
        (
            [
                BANNER,
                *_tick_lines(1),
                *_tick_lines(2)[:1],
                _lb(f"Drain complete: last completed cycle 2 run={RUN}"),
            ],
            "no completed tick record",
        ),
        (
            [
                BANNER,
                *_tick_lines(1, pins=("11", "12", "13", "unknown")),
                _lb(f"Drain complete: last completed cycle 1 run={RUN}"),
            ],
            "silver.silver_batch_versions snapshot unknown at the last completed tick",
        ),
        (
            [
                BANNER,
                *_tick_lines(1, pins=("11", "none", "13", "14")),
                _lb(f"Drain complete: last completed cycle 1 run={RUN}"),
            ],
            "silver.entities snapshot unknown",
        ),
        (
            [
                BANNER,
                *_tick_lines(1, commit=("unknown", "22")),
                _lb(f"Drain complete: last completed cycle 1 run={RUN}"),
            ],
            "gold.alerts snapshot unknown",
        ),
    ],
)
def test_scored_tick_refuses(lines, reason):
    tick, why = tr.scored_tick(tr.parse_tick_records("\n".join(lines), RUN))
    assert tick is None and reason in why


def test_ticks_unpinned_counts_txns_or_versions_tokens():
    lines = [
        BANNER,
        *_tick_lines(1, pins=("unknown", "1", "1", "1")),
        *_tick_lines(2, pins=("1", "1", "1", "none")),
        *_tick_lines(3, pins=("1", "unknown", "1", "1")),
    ]
    assert tr.ticks_unpinned(tr.parse_tick_records("\n".join(lines), RUN)["ticks"]) == 2


# --- covered scoring call -----------------------------------------------------


class FakeJobs:
    def __init__(self, rec):
        self.rec = rec

    def submit_job(self, job_type, arguments=None, cycle_env=None):
        self.rec.calls.append(("submit", job_type.value, list(arguments), dict(cycle_env or {})))
        return JobStatus(name="lakebench-score-financial", state=JobState.SUBMITTED, message="")


class FakeWait:
    def __init__(self, ok=True):
        self.ok = ok

    def wait_for_completion(self, name, timeout_seconds, poll_interval):
        return SimpleNamespace(success=self.ok, message="x", driver_logs=None)


def _score(monkeypatch, body, *, ok=True, drain=None, tick=None, reason="", failed=False):
    import json

    rec = _Calls()

    class S3:
        raw_client = None

        def __init__(self):
            self.raw_client = self

        def get_object(self, Bucket, Key):  # noqa: N803
            rec.calls.append(("get", Bucket, Key))
            return {"Body": SimpleNamespace(read=lambda: json.dumps(body).encode())}

    monkeypatch.setattr(post, "_s3_client", lambda cfg: S3())
    monkeypatch.setattr("lakebench.deploy.datagen.bronze_datagen_prefix", lambda cfg: "pacs008/")
    out = post.continuous_scoring(
        _cfg(), RUN, drain, tick, reason, FakeJobs(rec), FakeWait(ok), 600, run_failed=failed
    )
    return out, rec


def _tick():
    return tr.scored_tick(tr.parse_tick_records(_drained_log(2), RUN))[0]


def test_covered_score_passes_the_six_snapshots_and_the_run_id(monkeypatch):
    body = {
        "mode": "covered",
        "status": "scored",
        "covered": {"covered_instances": 3, "corpus_instances": 5},
    }
    out, rec = _score(monkeypatch, body, drain=post.DrainResult("drained"), tick=_tick())
    submit = next(c for c in rec.calls if c[0] == "submit")
    args = submit[2]
    pairs = dict(zip(args[4::2], args[5::2], strict=True))
    assert pairs == {
        "--covered-txns-snapshot": "12",
        "--covered-entities-snapshot": "12",
        "--covered-accounts-snapshot": "13",
        "--covered-versions-snapshot": "14",
        "--covered-alerts-snapshot": "21",
        "--covered-status-snapshot": "22",
    }
    assert submit[3] == {"LB_RUN_ID": RUN}
    assert ("get", "b-gold", f"scoring/{RUN}/recall.json") in rec.calls
    assert out["status"] == "scored" and out["tick"]["cycle"] == 2


@pytest.mark.parametrize(
    ("kwargs", "submits"),
    [
        pytest.param({"drain": None}, False, id="drain-did-not-run"),
        pytest.param({"drain": post.DrainResult("timeout", reason="x")}, False, id="drain-timeout"),
        pytest.param(
            {"drain": post.DrainResult("no_marker", reason="403")}, False, id="drain-no-marker"
        ),
        pytest.param(
            {"drain": post.DrainResult("drained"), "failed": True}, False, id="run-failed"
        ),
        pytest.param(
            {"drain": post.DrainResult("drained"), "tick": None, "reason": "no drain line"},
            False,
            id="no-tick",
        ),
        pytest.param(
            {"drain": post.DrainResult("drained"), "tick": _tick(), "ok": False},
            True,
            id="score-job-incomplete",
        ),
    ],
)
def test_covered_score_not_scored_reasons(monkeypatch, kwargs, submits):
    out, rec = _score(monkeypatch, {}, **kwargs)
    assert out["mode"] == "covered" and out["status"] == "not_scored" and out["reason"]
    assert bool([c for c in rec.calls if c[0] == "submit"]) is submits


def test_a_summary_without_covered_mode_is_not_a_covered_score(monkeypatch):
    out, _rec = _score(
        monkeypatch, {"typologies": []}, drain=post.DrainResult("drained"), tick=_tick()
    )
    assert out["status"] == "not_scored" and "covered mode" in out["reason"]


def test_alert_set_continuous_only_from_a_covered_score():
    from lakebench.metrics.experiment import _continuous_results

    aset = {"spec": "as1", "rows": 4, "h": "9"}
    m = SimpleNamespace(
        continuous={}, financial_scoring={"mode": "covered", "status": "scored", "alert_set": aset}
    )
    assert _continuous_results(m)["alert_set_continuous"] == aset
    m.financial_scoring = {"mode": "covered", "status": "not_scored", "alert_set": aset}
    assert "alert_set_continuous" not in _continuous_results(m)
    m.financial_scoring = None
    assert "alert_set_continuous" not in _continuous_results(m)


# --- stop -----------------------------------------------------------------------


class FakeCustom:
    def __init__(self, app=None, error=None):
        self.app, self.error = app, error

    def get_namespaced_custom_object(
        self, group, version, namespace, plural, name, _request_timeout=None
    ):
        if self.error is not None:
            raise self.error
        return self.app


def _app(state="RUNNING", run=RUN):
    env = [{"name": "LB_RUN_ID", "value": run}] if run else []
    return {"status": {"applicationState": {"state": state}}, "spec": {"driver": {"env": env}}}


def test_stop_drains_a_running_financial_gold_refresh(monkeypatch):
    seen = []
    monkeypatch.setattr(
        post,
        "request_drain",
        lambda cfg, k8s, b, run_id: seen.append((b, run_id)) or post.DrainResult("drained"),
    )
    r = post.stop_drain(_cfg(), None, custom_api=FakeCustom(_app()))
    assert r.state == "drained" and seen == [(post.STOP_DRAIN_BUDGET_S, RUN)]


@pytest.mark.parametrize(
    ("cfg", "custom"),
    [
        (_cfg(schema="customer360"), FakeCustom(_app())),
        (_cfg(), FakeCustom(_app(state="COMPLETED"))),
        (_cfg(), FakeCustom(_app(run=""))),
        (_cfg(), FakeCustom(error=type("E", (Exception,), {"status": 404})())),
    ],
)
def test_stop_has_nothing_to_drain(monkeypatch, cfg, custom):
    monkeypatch.setattr(post, "request_drain", lambda *a, **k: pytest.fail("drained"))
    assert post.stop_drain(cfg, None, custom_api=custom) is None


def test_pre_stop_raises_when_the_drain_is_not_confirmed(monkeypatch):
    from lakebench.cli import _cluster_ops as ops

    monkeypatch.setattr(
        post, "stop_drain", lambda cfg, k8s: post.DrainResult("timeout", reason="slow")
    )
    with pytest.raises(RuntimeError, match="gold drain not confirmed"):
        ops.pre_stop(_cfg(), None)
    monkeypatch.setattr(post, "stop_drain", lambda cfg, k8s: post.DrainResult("drained"))
    ops.pre_stop(_cfg(), None)
    monkeypatch.setattr(post, "stop_drain", lambda cfg, k8s: None)
    ops.pre_stop(_cfg(), None)


# --- drain_gold_refresh: the window end's record and gate -------------------------


def _window_drain(monkeypatch, result):
    from datetime import datetime

    from lakebench.cli import _sustained as sus

    monkeypatch.setattr(post, "request_drain", lambda *a, **k: result)
    collector = SimpleNamespace(current_run=SimpleNamespace(continuous={"gate_problems": []}))
    out = sus.drain_gold_refresh(
        _cfg(), None, RUN, collector, window_end=datetime(1970, 1, 1, 0, 0, 1)
    )
    return out, collector.current_run.continuous


def test_window_drain_records_ticks_and_the_scored_tick(monkeypatch):
    log = _drained_log(3)
    (drain, tick, why, problem), cont = _window_drain(
        monkeypatch, post.DrainResult("drained", 12.0, logs=log, last_cycle=3)
    )
    assert problem == "" and why == "" and tick["cycle"] == 3
    # pinned_at=1.5 in the fixture; the window ended at epoch 1.
    assert tick["pinned_after_window_end_s"] == 0.5
    assert [t["cycle"] for t in cont["ticks"]] == [1, 2, 3]
    assert cont["ticks_unpinned"] == 0
    assert cont["drain"]["state"] == "drained" and cont["drain"]["log_from_driver_start"] is True
    assert cont["drain"]["ticks_scope"]
    assert cont["gate_problems"] == []


def test_window_drain_records_the_time_travel_ticks(monkeypatch):
    """AML-9: each tick's tt-record line becomes continuous.time_travel.ticks[]
    (metadata counts, never the live table's), beside continuous.ticks."""
    lines = [BANNER]
    for c in (1, 2):
        pinned, *rest = _tick_lines(c, pins=(str(10 + c), "12", "13", "14"))
        count = "null" if c == 2 else "40"
        source = "unavailable" if c == 2 else "summary"
        lines += [
            pinned,
            _lb(
                f"Cycle {c}: tt-record table=silver.transactions snapshot={10 + c} "
                f"committed_at=2026-10-03T12:0{c}:00.000000Z total_records={count} "
                f"pos_deletes=0 eq_deletes=0 count_source={source} run={RUN}"
            ),
            *rest,
        ]
    lines.append(_lb(f"Drain complete: last completed cycle 2 run={RUN}"))
    (_d, tick, _why, problem), cont = _window_drain(
        monkeypatch, post.DrainResult("drained", 5.0, logs="\n".join(lines), last_cycle=2)
    )
    assert problem == "" and tick["cycle"] == 2
    tt = cont["time_travel"]["ticks"]
    assert [(t["cycle"], t["snapshot"], t["total_records"]) for t in tt] == [
        (1, 11, 40),
        (2, 12, None),
    ]
    assert tt[1]["count_source"] == "unavailable" and tt[0]["start"] == 0
    assert all(t["completed"] for t in tt)
    assert tt[0]["committed_at"] == "2026-10-03T12:01:00.000000Z"
    # continuous.ticks keeps its own keys.
    assert "tt" not in cont["ticks"][0]


def test_window_drain_problems_fail_the_run(monkeypatch):
    for result, problem in [
        (post.DrainResult("timeout", 1800.0, reason="x"), "gold drain timed out"),
        (post.DrainResult("driver_gone", 5.0, reason="x"), "deleted before it drained"),
        (
            post.DrainResult(
                "drained",
                1.0,
                logs="\n".join(
                    [
                        BANNER,
                        _lb(
                            "Drain complete: stop marker present at start; last completed "
                            f"cycle 0 run={RUN}"
                        ),
                    ]
                ),
                last_cycle=0,
            ),
            "restarted gold-refresh driver",
        ),
    ]:
        (_d, tick, _why, got), cont = _window_drain(monkeypatch, result)
        assert problem in got and tick is None
        assert cont["gate_problems"] == [got]


def _kept(tmp_path, lines):
    """A capturer holding *lines* as ``kubectl logs -f --timestamps`` wrote
    them since the gold-refresh driver started (None: nothing kept)."""
    from lakebench.cli._driver_log_capture import DriverLogCapturer

    cap = DriverLogCapturer("ns", tmp_path)
    if lines is not None:
        path = tmp_path / "gold.driver.log"
        path.write_text("".join(f"2026-10-03T12:00:00.123456789Z {ln}\n" for ln in lines))
        cap._captures["lakebench-gold-refresh"] = [("uid", None, path)]
    return lambda: cap.text("lakebench-gold-refresh")


@pytest.mark.parametrize(
    ("kept", "live", "cycles", "problem"),
    [
        # Rotation trimmed the pod log's front; the kept copy has it.
        ("1-2", "2-3", [1, 2, 3], ""),
        # The follow stream dropped after cycle 1; the pod log has the rest.
        ("1", "2-3", [1, 2, 3], ""),
        # Nothing kept and the pod log was trimmed: figures from part of the run.
        (None, "2-3", [2, 3], "driver log incomplete"),
        # Nothing trimmed: the kept copy adds nothing and duplicates nothing.
        ("1-3", "1-3", [1, 2, 3], ""),
    ],
)
def test_window_drain_reads_the_log_kept_since_the_driver_started(
    monkeypatch, tmp_path, kept, live, cycles, problem
):
    """Kubelet rotation trims the gold driver's pod log, so the drain
    reads the copy kept since the driver started, continued by the pod log;
    a log that still misses the first tick fails the run."""
    from datetime import datetime

    from lakebench.cli import _sustained as sus

    def span(spec, drain=False):
        lo, _, hi = spec.partition("-")
        out = [BANNER] if lo == "1" else []
        for c in range(int(lo), int(hi or lo) + 1):
            out += _tick_lines(c, pins=(str(10 + c), "12", "13", "14"))
        if drain:
            out.append(_lb(f"Drain complete: last completed cycle 3 run={RUN}"))
        return out

    monkeypatch.setattr(
        post,
        "request_drain",
        lambda *a, **k: post.DrainResult(
            "drained", 5.0, logs="\n".join(span(live, drain=True)), last_cycle=3
        ),
    )
    collector = SimpleNamespace(current_run=SimpleNamespace(continuous={"gate_problems": []}))
    _d, tick, _why, got = sus.drain_gold_refresh(
        _cfg(),
        None,
        RUN,
        collector,
        window_end=datetime(1970, 1, 1, 0, 0, 1),
        kept_log=_kept(tmp_path, span(kept) if kept else None),
    )
    cont = collector.current_run.continuous
    assert [t["cycle"] for t in cont["ticks"]] == cycles
    assert tick["cycle"] == 3
    assert (problem in got) if problem else got == ""


def test_window_drain_without_a_marker_is_not_a_gate_problem(monkeypatch):
    (_d, tick, _why, got), cont = _window_drain(
        monkeypatch, post.DrainResult("no_marker", reason="403")
    )
    assert got == "" and tick is None and cont["drain"]["state"] == "no_marker"
    assert "ticks" not in cont and "time_travel" not in cont


def test_pre_stop_ctrl_c_still_lets_stop_delete(monkeypatch):
    from lakebench.cli import _cluster_ops as ops

    def interrupted(cfg, k8s):
        raise KeyboardInterrupt

    monkeypatch.setattr(post, "stop_drain", interrupted)
    with pytest.raises(RuntimeError, match="drain interrupted"):
        ops.pre_stop(_cfg(), None)


def test_finally_settles_financial_scoring():
    from lakebench.cli._sustained import _settle_financial_scoring

    run = SimpleNamespace(financial_scoring=None)
    col = SimpleNamespace(current_run=run)
    _settle_financial_scoring(_cfg(), col, False, {"reason": "namespace deleted"})
    assert run.financial_scoring == post.not_scored(
        "the run ended before scoring (namespace deleted)"
    )
    run.financial_scoring = {"mode": "covered", "status": "scored"}
    _settle_financial_scoring(_cfg(), col, True, None)
    assert run.financial_scoring["status"] == "scored"
    _settle_financial_scoring(_cfg(), col, False, None)
    assert run.financial_scoring == post.not_scored("the run failed its gates")
    c360 = SimpleNamespace(current_run=SimpleNamespace(financial_scoring=None))
    _settle_financial_scoring(_cfg(schema="customer360"), c360, False, None)
    assert c360.current_run.financial_scoring is None


def test_full_log_read_is_bounded_by_the_budget_left(monkeypatch):
    full = _drained_log(1)
    seen = []

    class Timed(FakeLog):
        def __call__(self, tail, timeout=None):
            if tail is None:
                seen.append(timeout)
            return super().__call__(tail, timeout)

    result, rec = _drain(monkeypatch, ["x", "x", full], full=full, budget=300, make_log=Timed)
    elapsed = sum(c[1] for c in rec.calls if c[0] == "sleep")
    assert result.state == "drained" and elapsed > 0
    assert len(seen) == 1 and 0 < seen[0] <= 300 - elapsed
