"""Time-travel reads of a continuous AML run.

The job script's passes run here on fakes for the Spark reads (the Spark
tier runs them on a real Iceberg table); the record, the expiry attribution
and the verdict are pure; the CLI step runs on a fake job manager, monitor
and S3 client.
"""

from __future__ import annotations

import json
import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lakebench.metrics import time_travel as tt

T1, T2, T3 = 101, 102, 103


def _rec(cycle, snapshot, total=10, *, source="summary", pos=0, eq=0, committed=None):
    return {
        "start": 0,
        "cycle": cycle,
        "table": "silver.transactions",
        "snapshot": snapshot,
        "committed_at": committed or f"2026-10-03T10:{cycle:02d}:00.000000Z",
        "total_records": total,
        "pos_deletes": pos,
        "eq_deletes": eq,
        "count_source": source,
        "completed": True,
    }


# --- the job script on fakes --------------------------------------------------


@pytest.fixture
def job(load_script, monkeypatch):
    """time_travel_financial with its Spark reads faked: snapshots T2 and T3
    are live (T1 expired), each holds 10 rows; storage is a dict."""
    mod = load_script("time_travel_financial")
    store: dict[str, str] = {}
    live = {T2, T3}
    scans: list[int | None] = []

    def fingerprint_at(spark, fq, sid):
        scans.append(sid)
        return {"rows": 10, "fp": f"fp-{sid}", "cols_sha": "c" * 16}

    monkeypatch.setattr(mod, "live_snapshots", lambda spark, fq: set(live))
    monkeypatch.setattr(mod, "fingerprint_at", fingerprint_at)
    monkeypatch.setattr(mod, "table_provider", lambda spark, fq: "iceberg")
    monkeypatch.setattr(mod, "current_snapshot", lambda spark, fq: T3)
    monkeypatch.setattr(mod, "_write_text", lambda spark, uri, text: store.__setitem__(uri, text))
    monkeypatch.setattr(mod, "_read_text", lambda spark, uri: store[uri])
    return SimpleNamespace(mod=mod, store=store, live=live, scans=scans)


def _run(job, records, budget=None):
    inputs = {
        "run_id": "r",
        "nonce": "n1",
        "exclude_columns": sorted(tt.SENTINEL_COLUMNS),
        "records": records,
    }
    return job.mod.time_travel(None, inputs, "mem://tt_hashes.json", job.mod.Budget(budget))


def _states(result):
    return [t["state"] for t in result["ticks"]]


def test_expired_verified_and_current_read(job):
    out = _run(job, [_rec(1, T1), _rec(2, T2), _rec(3, T3)])
    assert _states(out) == ["expired", "verified", "verified"]
    assert out["ticks"][1]["count_match"] is True and out["ticks"][1]["fp_match"] is True
    assert out["ticks"][1]["read_s"] is not None
    assert out["current"]["snapshot"] == T3 and out["current"]["read_s"] is not None
    assert out["incomplete"] is False
    # Hash pass once per live snapshot, read pass once per live snapshot, current once.
    assert sorted(s for s in job.scans if s is not None) == [T2, T2, T3, T3, T3]


def test_hashes_are_re_read_from_storage_and_a_tampered_fp_is_a_mismatch(job, monkeypatch):
    real_hash_pass = job.mod.hash_pass

    def tamper(spark, records, uri, nonce, budget):
        done = real_hash_pass(spark, records, uri, nonce, budget)
        doc = json.loads(job.store[uri])
        for h in doc["hashes"]:
            if h["snapshot"] == T2:
                h["fp"] = "tampered"
        job.store[uri] = json.dumps(doc)
        return done

    monkeypatch.setattr(job.mod, "hash_pass", tamper)
    out = _run(job, [_rec(2, T2), _rec(3, T3)])
    assert _states(out) == ["mismatch", "verified"]
    assert out["ticks"][0]["fp_match"] is False
    rec = {"time_travel": {"ticks": [_rec(2, T2), _rec(3, T3)]}}
    got = tt.merge(rec, {**out, "status": "read"}, {"configured": "30m", "applied_expire": "1h"})
    assert got["verdict"] == "fail" and "mismatch x1" in got["reason"]


def test_hashes_of_another_submission_are_not_trusted(job, monkeypatch):
    real_hash_pass = job.mod.hash_pass

    def other(spark, records, uri, nonce, budget):
        return real_hash_pass(spark, records, uri, "someone-else", budget)

    monkeypatch.setattr(job.mod, "hash_pass", other)
    out = _run(job, [_rec(2, T2)])
    assert _states(out) == ["mismatch"]
    assert "no hash-pass entry" in out["ticks"][0]["reason"]


def test_recorded_count_one_off_is_a_mismatch(job):
    out = _run(job, [_rec(2, T2, total=11)])
    assert _states(out) == ["mismatch"]
    assert out["ticks"][0]["count_match"] is False and out["ticks"][0]["fp_match"] is True


@pytest.mark.parametrize(
    "kw",
    [
        pytest.param({"source": "unavailable", "total": None}, id="no-summary-count"),
        pytest.param({"pos": 3}, id="position-deletes"),
        pytest.param({"eq": 1}, id="equality-deletes"),
    ],
)
def test_no_live_row_count_compares_on_the_hash_alone(job, kw):
    out = _run(job, [_rec(2, T2, **kw)])
    assert _states(out) == ["verified_hash_only"]
    assert out["ticks"][0]["count_match"] is None


def test_a_snapshot_read_by_two_ticks_is_scanned_once(job):
    out = _run(job, [_rec(2, T2), _rec(3, T2)])
    assert _states(out) == ["verified", "verified"]
    assert job.scans.count(T2) == 2  # one hash pass, one read pass


def test_budget_spent_leaves_records_not_read(job):
    out = _run(job, [_rec(1, T1), _rec(2, T2)], budget=0)
    assert _states(out) == ["expired", "not_read"]
    assert out["incomplete"] is True
    assert out["current"] == {
        "table": "silver.transactions",
        "not_read": "the time-travel budget ran out",
    }


def test_unreadable_live_snapshot_is_an_error(job, monkeypatch):
    def boom(spark, fq, sid):
        raise RuntimeError("no such file")

    monkeypatch.setattr(job.mod, "fingerprint_at", boom)
    out = _run(job, [_rec(2, T2)])
    assert _states(out) == ["error"] and "no such file" in out["ticks"][0]["reason"]


def test_a_hash_pass_read_failure_is_an_error_not_a_mismatch(job, monkeypatch):
    """A transient failure in the hash pass is a read error, even when the
    read pass's scan of the same snapshot succeeds."""
    real = job.mod.fingerprint_at
    calls = {"n": 0}

    def first_fails(spark, fq, sid):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("S3 503")
        return real(spark, fq, sid)

    monkeypatch.setattr(job.mod, "fingerprint_at", first_fails)
    out = _run(job, [_rec(2, T2)])
    assert _states(out) == ["error"] and "hash pass" in out["ticks"][0]["reason"]
    assert json.loads(job.store["mem://tt_hashes.json"])["errors"][0]["snapshot"] == T2


def test_a_table_that_cannot_be_read_gives_every_record_an_error(job, monkeypatch):
    def gone(spark, fq):
        raise RuntimeError("Table not found")

    monkeypatch.setattr(job.mod, "live_snapshots", gone)
    out = _run(job, [_rec(1, T1), _rec(2, T2)])
    assert _states(out) == ["error", "error"]
    assert "Table not found" in out["ticks"][0]["reason"] and out["current"] is None


def test_the_newest_snapshot_is_read_first(job):
    _run(job, [_rec(2, T2), _rec(3, T3)])
    assert job.scans[:2] == [T3, T2]  # hash pass, newest first


def test_a_scan_starts_only_when_the_time_left_can_hold_it(job):
    clock = {"t": 100.0}
    budget_cls = job.mod.Budget
    bud = budget_cls(110.0, clock=lambda: clock["t"])
    assert bud.can_start()
    bud.took(5.0)
    assert bud.can_start()  # 10 s left > 7.5 s
    clock["t"] = 103.0
    assert not bud.can_start()  # 7 s left < 7.5 s
    half = budget_cls(110.0, clock=lambda: 100.0).split()
    assert half.deadline == 105.0
    assert budget_cls(None).can_start()


def test_a_record_without_a_snapshot_id_is_a_mismatch(job):
    out = _run(job, [_rec(2, "unknown")])
    assert _states(out) == ["mismatch"]


def test_delta_table_is_not_supported(job, monkeypatch):
    monkeypatch.setattr(job.mod, "table_provider", lambda spark, fq: "delta")
    out = _run(job, [_rec(2, T2)])
    assert out["status"] == "not_supported" and _states(out) == ["not_supported"]
    assert job.scans == [] and job.store == {}
    rec = {"time_travel": {"ticks": [_rec(2, T2)]}}
    assert tt.merge(rec, out, {"configured": "30m"})["verdict"] == "fail"


# --- expiry attribution and the verdict ---------------------------------------


def _round(n, ended, applied="1h", tables=("lakehouse.silver.transactions",)):
    return {
        "round": n,
        "started_at": ended,
        "ended_at": ended,
        "applied_expire": applied,
        "expired_tables": list(tables),
    }


def _continuous(ticks, rounds, offset=0.0):
    return {
        "window": {"cluster_clock_offset_seconds": offset},
        "retention": {"configured": "30m", "applied_expire": "1h", "rounds": rounds},
        "time_travel": {"ticks": ticks},
    }


POLICY = {"configured": "30m", "applied_expire": "1h"}


def _result(states):
    return {
        "status": "read",
        "ticks": [{"start": 0, "cycle": c, "state": s} for c, s in states],
        "current": {"read_s": 1.5},
        "incomplete": False,
    }


def test_expired_snapshot_explained_by_a_covering_round():
    ticks = [_rec(1, T1, committed="2026-10-03T10:00:00Z"), _rec(2, T2)]
    cont = _continuous(
        ticks, [_round(1, "2026-10-03T10:30:00Z"), _round(2, "2026-10-03T11:05:00Z")]
    )
    got = tt.merge(cont, _result([(1, "expired"), (2, "verified")]), POLICY)
    assert got["verdict"] == "pass"
    by = got["ticks"][0]["expired_by"]
    # Round 1 (10:30 - 1h) is before the commit; round 2 (11:05 - 1h) covers it.
    assert by == {
        "round": 2,
        "basis": "the earliest maintenance round that could have expired it",
        "ran_at": "2026-10-03T11:05:00Z",
        "ran_at_clock": "host",
        "configured": "30m",
        "applied": "1h",
        "reason": "live-stream floor",
    }
    assert got["policy"] == POLICY and got["current_read_s"] == 1.5


def test_expired_snapshot_no_round_explains_is_missing_unexplained():
    ticks = [_rec(1, T1, committed="2026-10-03T10:00:00Z"), _rec(2, T2)]
    cont = _continuous(ticks, [_round(1, "2026-10-03T10:30:00Z")])
    got = tt.merge(cont, _result([(1, "expired"), (2, "verified")]), POLICY)
    assert got["ticks"][0]["state"] == "missing_unexplained"
    assert got["verdict"] == "fail" and "missing_unexplained" in got["reason"]


def test_a_round_that_did_not_expire_the_table_explains_nothing():
    ticks = [_rec(1, T1, committed="2026-10-03T10:00:00Z"), _rec(2, T2)]
    cont = _continuous(
        ticks, [_round(1, "2026-10-03T12:00:00Z", tables=("lakehouse.gold.alerts",))]
    )
    got = tt.merge(cont, _result([(1, "expired"), (2, "verified")]), POLICY)
    assert got["ticks"][0]["state"] == "missing_unexplained"


def test_the_cluster_clock_offset_moves_the_round_end():
    # Host clock round end 11:00; the cluster is 10 minutes behind, so the
    # cutoff on the cluster clock is 09:50 and a 09:55 commit is not covered.
    ticks = [_rec(1, T1, committed="2026-10-03T09:55:00Z"), _rec(2, T2)]
    rounds = [_round(1, "2026-10-03T11:00:00Z")]
    assert (
        tt.merge(
            _continuous(ticks, rounds, 0.0), _result([(1, "expired"), (2, "verified")]), POLICY
        )["ticks"][0]["state"]
        == "expired"
    )
    got = tt.merge(
        _continuous([dict(t) for t in ticks], rounds, -600.0),
        _result([(1, "expired"), (2, "verified")]),
        POLICY,
    )
    assert got["ticks"][0]["state"] == "missing_unexplained"


@pytest.mark.parametrize(
    ("configured", "reason"),
    [
        ("1h", "configured retention"),
        ("60m", "configured retention"),
        (None, "configured retention unknown"),
    ],
)
def test_expiry_reason_names_the_configured_retention(configured, reason):
    by = tt.expired_by(
        _rec(1, T1, committed="2026-10-03T10:00:00Z"),
        [_round(1, "2026-10-03T12:00:00Z", applied="1h")],
        configured,
        None,
    )
    assert by is not None and by["reason"] == reason
    assert by["ran_at_clock"] == "host"


def test_maintenance_skipped_leaves_every_expiry_unexplained():
    ticks = [_rec(1, T1, committed="2026-10-03T10:00:00Z"), _rec(2, T2)]
    cont = {"retention": {"skipped": "--skip-maintenance"}, "time_travel": {"ticks": ticks}}
    pol = tt.policy(cont["retention"], "30m")
    assert pol == {"configured": "30m", "applied_expire": None, "skipped": "--skip-maintenance"}
    got = tt.merge(cont, _result([(1, "expired"), (2, "verified")]), pol)
    assert got["ticks"][0]["state"] == "missing_unexplained" and got["verdict"] == "fail"
    assert "maintenance skipped" in tt.line(got)


@pytest.mark.parametrize(
    ("states", "incomplete", "verdict"),
    [
        ([], False, "fail"),
        (["expired", "expired"], False, "fail"),
        (["verified", "mismatch"], False, "fail"),
        (["verified", "error"], False, "fail"),
        (["verified", "not_read"], False, "incomplete"),
        (["verified"], True, "incomplete"),
        (["verified_hash_only"], False, "fail"),
        (["verified", "verified_hash_only"], False, "pass"),
        (["expired", "verified"], False, "pass"),
        (["verified", "something_new"], False, "fail"),
    ],
)
def test_verdict(states, incomplete, verdict):
    got, reason = tt.verdict_of([{"state": s} for s in states], incomplete)
    assert got == verdict
    assert (reason == "") == (verdict == "pass")


def test_spark_thrift_cutoff_is_on_the_host_clock():
    # The cluster is 10 minutes behind; Spark Thrift's cutoff literal is the
    # host's time, so the offset does not apply and a 09:55 commit is covered.
    ticks = [_rec(1, T1, committed="2026-10-03T09:55:00Z"), _rec(2, T2)]
    rounds = [{**_round(1, "2026-10-03T11:00:00Z"), "engine": "spark-thrift"}]
    got = tt.merge(
        _continuous(ticks, rounds, -600.0), _result([(1, "expired"), (2, "verified")]), POLICY
    )
    assert got["ticks"][0]["state"] == "expired"


def test_a_recorded_tick_with_no_result_is_an_error():
    cont = _continuous([_rec(1, T1), _rec(2, T2)], [])
    got = tt.merge(cont, _result([(2, "verified")]), POLICY)
    assert got["ticks"][0]["state"] == "error" and got["verdict"] == "fail"


def test_line_distinguishes_a_failed_check_from_a_pass():
    bad = tt.merge(_continuous([_rec(1, T1)], []), _result([(1, "mismatch")]), POLICY)
    ok = tt.merge(_continuous([_rec(1, T1)], []), _result([(1, "verified")]), POLICY)
    assert tt.line(bad).startswith("time-travel check: FAIL")
    assert "FAIL" not in tt.line(ok) and tt.line(ok).startswith("time-travel check: pass")


# --- the CLI step ---------------------------------------------------------------


class _S3:
    def __init__(self, result=None):
        self.objects: dict[str, bytes] = {}
        self.result = result
        self.raw_client = self

    def put_object(self, Bucket, Key, Body):  # noqa: N803 -- boto3's names
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):  # noqa: N803 -- boto3's names
        body = self.result(self.objects) if callable(self.result) else self.result
        return {"Body": SimpleNamespace(read=lambda: json.dumps(body).encode())}


def _cfg():
    from tests.conftest import make_config

    return make_config(
        recipe="hive-iceberg-spark-trino",
        workload={"schema": "financial"},
        architecture={"pipeline": {"mode": "continuous"}},
    )


def _drained():
    from lakebench.cli._aml_post import DrainResult

    return DrainResult("drained", last_cycle=3)


def _step(cont, s3, *, drain=None, submit_state=None, success=True, run_failed=False):
    from lakebench.cli import _aml_post
    from lakebench.spark.job import JobState

    jm = MagicMock()
    jm.submit_job.return_value = SimpleNamespace(
        state=submit_state or JobState.SUBMITTED, message="m"
    )
    mon = MagicMock()
    mon.wait_for_completion.return_value = SimpleNamespace(success=success, message="timed out")
    with patch.object(_aml_post, "_s3_client", return_value=s3):
        out = _aml_post.run_time_travel(
            _cfg(),
            "run-1",
            cont,
            drain or _drained(),
            jm,
            mon,
            1200,
            run_failed=run_failed,
            wall_clock=lambda: 1_000_000.0,
        )
    return out, jm, mon


def test_step_submits_the_job_with_the_records_and_merges_its_result():
    from lakebench.spark.job import JobType

    def result(objects):
        sent = json.loads(objects["scoring/run-1/tt_input.json"])
        return {
            "nonce": sent["nonce"],
            "status": "read",
            "incomplete": False,
            "current": {"read_s": 2.0},
            "ticks": [{"start": 0, "cycle": 2, "state": "verified", "read_s": 1.0, "rows": 10}],
        }

    s3 = _S3(result)
    cont = _continuous([_rec(2, T2)], [])
    out, jm, mon = _step(cont, s3)
    assert out is cont["time_travel"] and out["verdict"] == "pass"
    sent = json.loads(s3.objects["scoring/run-1/tt_input.json"])
    assert [r["snapshot"] for r in sent["records"]] == [T2]
    assert sent["exclude_columns"] == ["_batch_id", "_stream_id", "committed_at", "ingest_ts"]
    assert "completed" not in sent["records"][0]
    args = jm.submit_job.call_args
    assert args.args[0] == JobType.TIME_TRAVEL_FINANCIAL
    argv = args.kwargs["arguments"]
    # Deadline: submit time + (1200 - 120) s, moved to the cluster clock
    # (_continuous's offset is 0 here).
    assert argv[argv.index("--deadline-epoch") + 1] == f"{1_000_000.0 + 1080:.3f}"
    assert out["budget"]["budget_s"] == 1080 and out["budget"]["label"].startswith("BOUNDED BY")
    assert args.kwargs["cycle_env"] == {"LB_RUN_ID": "run-1"}
    assert mon.wait_for_completion.call_args.kwargs["timeout_seconds"] == 1200


def test_step_deadline_follows_the_cluster_clock():
    cont = _continuous([_rec(2, T2)], [], offset=-30.0)
    _out, jm, _mon = _step(cont, _S3({"nonce": "x"}))
    argv = jm.submit_job.call_args.kwargs["arguments"]
    assert argv[argv.index("--deadline-epoch") + 1] == f"{1_000_000.0 + 1080 - 30:.3f}"


def test_a_wait_that_ends_first_deletes_the_job():
    out, jm, _mon = _step(_continuous([_rec(2, T2)], []), _S3(), success=False)
    assert out["verdict"] == "not_run"
    jm._delete_job.assert_called_once_with("lakebench-time-travel-financial")


def test_a_failed_run_is_not_read():
    out, jm, _mon = _step(_continuous([_rec(2, T2)], []), _S3(), run_failed=True)
    assert out["verdict"] == "not_run" and out["reason"] == "the run failed its gates"
    jm.submit_job.assert_not_called()


def test_step_refuses_a_result_of_another_submission():
    s3 = _S3({"nonce": "stale", "status": "read", "ticks": []})
    out, _jm, _mon = _step(_continuous([_rec(2, T2)], []), s3)
    assert out["verdict"] == "not_run" and "not this submission's" in out["reason"]


@pytest.mark.parametrize(
    ("kw", "verdict", "needle"),
    [
        ({"drain": "timeout"}, "not_run", "drain timeout"),
        ({"success": False}, "not_run", "did not complete"),
        ({"submit_state": "FAILED"}, "not_run", "not submitted"),
    ],
)
def test_step_not_run(kw, verdict, needle):
    from lakebench.cli._aml_post import DrainResult
    from lakebench.spark.job import JobState

    if "drain" in kw:
        kw["drain"] = DrainResult(kw["drain"], reason="x")
    if "submit_state" in kw:
        kw["submit_state"] = JobState.FAILED
    out, _jm, _mon = _step(_continuous([_rec(2, T2)], []), _S3(), **kw)
    assert out["verdict"] == verdict and needle in out["reason"]
    assert out["policy"]["configured"] == "30m"


def test_step_zero_records_fails_without_a_job():
    out, jm, _mon = _step(_continuous([], []), _S3())
    assert out["verdict"] == "fail" and out["reason"] == "zero recorded snapshots"
    jm.submit_job.assert_not_called()


def test_step_never_raises():
    class Broken(_S3):
        def put_object(self, **kw):
            raise OSError("S3 down")

    out, _jm, _mon = _step(_continuous([_rec(2, T2)], []), Broken())
    assert out["verdict"] == "not_run" and "S3 down" in out["reason"]


def test_settle_gives_an_unfinished_run_a_verdict():
    from lakebench.cli._aml_post import settle_time_travel

    run = SimpleNamespace(continuous={"time_travel": {"ticks": [_rec(2, T2)]}})
    settle_time_travel(_cfg(), run, "interrupted before the time-travel reads")
    assert run.continuous["time_travel"]["verdict"] == "not_run"
    assert run.continuous["time_travel"]["ticks"] == [_rec(2, T2)]
    done = SimpleNamespace(continuous={"time_travel": {"verdict": "pass"}})
    settle_time_travel(_cfg(), done, "x")
    assert done.continuous["time_travel"] == {"verdict": "pass"}


# --- maintenance rounds -------------------------------------------------------


def test_expire_round_record_lists_tables_whose_expire_ran():
    from lakebench.cli._sustained import expire_round_record

    plan = [
        ("c.silver.transactions", "ALTER TABLE c.silver.transactions EXECUTE expire_snapshots"),
        ("c.silver.transactions", "ALTER TABLE c.silver.transactions EXECUTE remove_orphan_files"),
        ("c.gold.alerts", "ALTER TABLE c.gold.alerts EXECUTE expire_snapshots"),
        ("c.silver.accounts", "ALTER TABLE c.silver.accounts EXECUTE expire_snapshots"),
    ]
    out = {"status": ["ok", "ok", "timed_out", "failed"]}
    rec = expire_round_record(3, "a", "b", "1h", plan, out, engine="trino")
    assert rec == {
        "round": 3,
        "started_at": "a",
        "ended_at": "b",
        "applied_expire": "1h",
        "engine": "trino",
        "expired_tables": ["c.silver.transactions", "c.gold.alerts"],
    }


def test_a_continuous_maintenance_round_records_its_time_and_tables():
    from rich.console import Console

    from lakebench.cli._sustained import _run_iceberg_maintenance, continuous_retention_record

    cfg = _cfg()
    rounds: list = []
    with (
        patch(
            "lakebench.deploy.iceberg.find_maintenance_engine",
            return_value=("trino", "trino-0", "lakehouse"),
        ),
        patch("lakebench.deploy.iceberg.exec_sql"),
    ):
        _run_iceberg_maintenance(
            cfg,
            MagicMock(),
            Console(quiet=True),
            MagicMock(),
            "30m",
            live_streams=True,
            rounds=rounds,
        )
    assert len(rounds) == 1 and rounds[0]["round"] == 1
    assert rounds[0]["applied_expire"] == "1h"  # the live-stream floor
    assert rounds[0]["engine"] == "trino"
    assert "lakehouse.silver.transactions" in rounds[0]["expired_tables"]
    assert re.fullmatch(r"\d{4}-\d\d-\d\dT[\d:.]+Z", rounds[0]["ended_at"])
    rec = continuous_retention_record(cfg, rounds=rounds)
    assert rec["rounds"] == rounds and rec["applied_expire"] == "1h"
    assert "rounds" not in continuous_retention_record(cfg)


# --- profile ---------------------------------------


@pytest.mark.parametrize("schema", ["customer360", "financial"])
def test_profile_equals_the_scorer_and_one_attempt(schema):
    from lakebench.modules.pipeline_engines.spark import job as job_mod
    from lakebench.spark.job import JobType, SparkJobManager
    from tests.test_spark import _mock_k8s

    assert job_mod._resolve_job_profile("time-travel-financial", schema) == (
        job_mod._resolve_job_profile("score-financial", schema)
    )
    mgr = SparkJobManager(_cfg(), _mock_k8s())
    manifest = mgr._build_manifest(JobType.TIME_TRAVEL_FINANCIAL, arguments=["--input", "x"])
    spec = manifest["spec"]
    assert spec["mainApplicationFile"].endswith("/time_travel_financial.py")
    assert spec["restartPolicy"]["onFailureRetries"] == 0
    assert spec["restartPolicy"]["onSubmissionFailureRetries"] == 5


def test_the_verdict_shows_the_time_travel_check_and_never_fails_on_it():
    from lakebench.metrics.verdict import compute_verdict
    from lakebench.reports.front_matter import _qualifier_lines
    from tests.test_experiment import _cfg, _metrics

    m = _metrics(_cfg(schema="financial", mode="continuous"))
    before = compute_verdict(m)
    assert "time_travel" not in before.qualifiers
    # Tick records alone (no verdict yet) give no line.
    m.continuous = {"time_travel": {"ticks": [_rec(1, T1)]}}
    assert "time_travel" not in compute_verdict(m).qualifiers
    got = tt.merge(_continuous([_rec(1, T1)], []), _result([(1, "mismatch")]), POLICY)
    m.continuous = {"time_travel": got}
    after = compute_verdict(m)
    text = after.qualifiers["time_travel"]
    assert text == tt.line(got) and text.startswith("time-travel check: FAIL")
    assert after.status == before.status
    assert _qualifier_lines({"time_travel": text}) == [text]


def test_a_hash_pass_cut_short_leaves_the_rest_not_read_not_mismatched(job, monkeypatch):
    """The hash pass reached only the newest snapshot; the read pass, with
    time left, reads that one and leaves the other not_read (incomplete),
    never a mismatch for a hash it never tried to take."""
    real = job.mod.hash_pass

    def first_only(spark, keys, uri, nonce, budget):
        return real(spark, keys[:1], uri, nonce, budget)

    monkeypatch.setattr(job.mod, "hash_pass", first_only)
    out = _run(job, [_rec(2, T2), _rec(3, T3)])
    assert _states(out) == ["not_read", "verified"]
    assert out["incomplete"] is True


def test_spark_thrift_rounds_use_their_start():
    # Spark Thrift's cutoff literal was taken before the round started: a
    # commit after start - 1h but before end - 1h is not covered.
    tick = _rec(1, T1, committed="2026-10-03T10:02:00Z")
    rnd = {**_round(1, "2026-10-03T11:05:00Z"), "started_at": "2026-10-03T11:00:00Z"}
    assert tt.expired_by(tick, [rnd], "30m", None) is not None  # Trino: end - 1h
    assert tt.expired_by(tick, [{**rnd, "engine": "spark-thrift"}], "30m", None) is None


def test_a_run_that_failed_after_the_reads_does_not_keep_their_verdict():
    from lakebench.cli._aml_post import settle_time_travel

    run = SimpleNamespace(continuous={"time_travel": {"verdict": "pass", "ticks": [_rec(2, T2)]}})
    settle_time_travel(_cfg(), run, "x", pipeline_success=False)
    tt_rec = run.continuous["time_travel"]
    assert tt_rec["verdict"] == "not_run" and tt_rec["reason"] == "the run failed its gates"
    assert tt_rec["ticks"] == [_rec(2, T2)]


def test_a_wait_that_raises_deletes_the_job():
    from lakebench.cli import _aml_post
    from lakebench.spark.job import JobState

    jm = MagicMock()
    jm.submit_job.return_value = SimpleNamespace(state=JobState.SUBMITTED, message="m")
    mon = MagicMock()
    mon.wait_for_completion.side_effect = RuntimeError("status read 500")
    with patch.object(_aml_post, "_s3_client", return_value=_S3()):
        out = _aml_post.run_time_travel(
            _cfg(), "run-1", _continuous([_rec(2, T2)], []), _drained(), jm, mon, 1200
        )
    assert out["verdict"] == "not_run" and "status read 500" in out["reason"]
    jm._delete_job.assert_called_once_with("lakebench-time-travel-financial")


# --- the hash covers the business columns ---


def test_the_job_hashes_business_columns_only(job):
    _run(job, [_rec(2, T2)])
    cols = ["txn_id", "_batch_id", "txn_amount", "_stream_id", "ingest_ts"]
    assert job.mod.business_columns(cols) == ["txn_id", "txn_amount"]


@pytest.mark.parametrize("excluded", [None, [], [1]])
def test_the_job_refuses_an_input_with_no_column_definition(job, excluded):
    inputs = {"run_id": "r", "nonce": "n1", "records": [_rec(2, T2)]}
    if excluded is not None:
        inputs["exclude_columns"] = excluded
    with pytest.raises(SystemExit, match="exclude_columns"):
        job.mod.time_travel(None, inputs, "mem://h", job.mod.Budget(None))
    assert job.scans == []


def test_the_record_says_which_columns_were_hashed():
    got = tt.merge(
        _continuous([_rec(1, T1)], []),
        {**_result([(1, "verified")]), "excluded_columns": sorted(tt.SENTINEL_COLUMNS)},
        POLICY,
    )
    assert got["hashed_columns"]["excluded"] == sorted(tt.SENTINEL_COLUMNS)
    assert got["hashed_columns"]["basis"].startswith("business columns")
    # No job result names its columns: nothing claims what was hashed.
    none = tt.merge(_continuous([_rec(1, T1)], []), _result([(1, "verified")]), POLICY)
    assert "hashed_columns" not in none


# --- order of the post-window steps ---


def test_the_reads_follow_the_drain_the_stop_and_the_scorer(monkeypatch, tmp_path):
    """The reads compare against the tables the scorer saw: drain, stop the
    streams, score, then read, and the run outcome goes to the reads."""
    import time as real_time

    from lakebench.cli import _aml_post, _sustained
    from lakebench.spark.job import JobState
    from tests.conftest import make_config

    order: list[str] = []
    seen: dict = {}
    cfg = make_config(
        recipe="hive-iceberg-spark-trino",
        workload={"schema": "financial"},
        architecture={"pipeline": {"mode": "continuous"}},
    )
    monkeypatch.chdir(tmp_path)
    op = MagicMock()
    op.check_status.return_value = MagicMock(ready=True, version="2.5.1")
    op.ensure_namespace_watched.return_value = MagicMock(watching_namespace=True)
    monkeypatch.setattr("lakebench.spark.SparkOperatorManager", lambda **kw: op)
    monkeypatch.setattr(_sustained, "get_k8s_client", lambda **kw: MagicMock())
    jm = MagicMock()
    jm.deploy_scripts_configmap.return_value = True
    jm.submit_job.return_value = MagicMock(state=JobState.RUNNING)
    monkeypatch.setattr("lakebench.engine.get_engine", lambda c, k: jm)
    mon = MagicMock()
    mon.wait_for_completion.return_value = SimpleNamespace(
        success=True, message="", elapsed_seconds=1.0
    )
    mon._get_driver_logs.return_value = ""
    monkeypatch.setattr("lakebench.spark.SparkJobMonitor", lambda *a, **kw: mon)
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", lambda c, **kw: MagicMock())
    monkeypatch.setattr("lakebench.deploy.DatagenDeployer", lambda e, **kw: MagicMock())
    monkeypatch.setattr("lakebench.s3.S3Client", lambda **kw: MagicMock())
    monkeypatch.setattr(_sustained, "_wait_for_bronze_data", lambda *a, **kw: True)
    monkeypatch.setattr(_sustained, "_collect_platform_metrics", lambda *a, **kw: None)
    monkeypatch.setattr(_sustained, "_require_reset_ownership", lambda c: None)
    monkeypatch.setattr(_sustained, "_stop_leftover_streams", lambda jm_, ns: None)
    monkeypatch.setattr(_sustained, "_reset_continuous_state", lambda c, clear_raw: None)
    monkeypatch.setattr(_sustained, "_datagen_job_state", lambda ns: ("none", ""))
    clock = [real_time.time()]

    def sleep(seconds):
        clock[0] += seconds

    monkeypatch.setattr(
        _sustained,
        "time",
        SimpleNamespace(
            time=lambda: clock[0],
            sleep=sleep,
            monotonic=lambda: clock[0],
            strftime=real_time.strftime,
        ),
    )

    def drain(*a, **kw):
        order.append("drain")
        return _drained(), None, "", None

    def stop(k8s, ns, subs):
        order.append("stop")
        return list(subs)

    def score(*a, **kw):
        order.append("score")
        return {"status": "scored"}

    def reads(*a, **kw):
        order.append("reads")
        seen.update(kw)

    real_record = _sustained.continuous_retention_record

    def record(cfg, rounds=None):
        seen["rounds"] = rounds
        return real_record(cfg, rounds=rounds)

    monkeypatch.setattr(_sustained, "continuous_retention_record", record)
    monkeypatch.setattr(_sustained, "drain_gold_refresh", drain)
    monkeypatch.setattr(_sustained, "_stop_streams", stop)
    monkeypatch.setattr(_aml_post, "continuous_scoring", score)
    monkeypatch.setattr(_aml_post, "run_time_travel", reads)
    monkeypatch.setattr(_aml_post, "settle_time_travel", lambda *a, **kw: None)
    with pytest.raises(BaseException) as ei:  # noqa: B017, PT011 -- the exit is not asserted
        _sustained._run_sustained(cfg, tmp_path / "cfg.yaml", 60, True, 60, skip_generate=True)
    assert order == ["drain", "stop", "score", "reads"], repr(ei.value)
    assert "run_failed" in seen
    assert seen["rounds"] == []
