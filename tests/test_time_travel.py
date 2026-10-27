"""Time-travel retention check of a continuous AML run.

The check runs after the window through the query engine (no Spark): a
metadata read says whether each recorded snapshot is present, expired or
missing, and a present snapshot gets one point read of a case's evidence.
The record, the expiry attribution and the verdict are pure; the fake
``run_tsv`` below stands in for the executor.
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lakebench.aml import time_travel_queries as ttq
from lakebench.metrics import time_travel as tt

T1, T2, T3 = 101, 102, 103
#: gold.alerts snapshot a tick committed (so the point read can pin alerts).
A1, A2, A3 = 201, 202, 203


def _rec(cycle, snapshot, *, committed=None, alerts=A2):
    return {
        "start": 0,
        "cycle": cycle,
        "table": "silver.transactions",
        "snapshot": snapshot,
        "committed_at": committed or f"2026-10-03T10:{cycle:02d}:00.000000Z",
        "total_records": 10,
        "committed_alerts": alerts,
        "completed": True,
    }


class _Engine:
    """A fake run_tsv: snapshots in *live* are present; a present snapshot's
    point read returns (found, evidence) from *case*, keyed by the
    alerts snapshot the statement pins. A table in *fails* raises on its
    snapshot listing."""

    def __init__(self, live, case=None, fails=()):
        self.live = {str(s) for s in live}
        self.case = case or {}
        self.fails = set(fails)
        self.reads: list[str] = []

    def run_tsv(self, sql, timeout):
        self.reads.append(sql)
        if "$snapshots" in sql:
            table = re.search(r'silver\."(\w+)\$snapshots"', sql).group(1)
            if table in self.fails:
                raise RuntimeError("snapshot list 500")
            return [[s] for s in sorted(self.live)]
        # The point read: find which alerts snapshot it pinned.
        pin = re.search(r"gold\.alerts FOR VERSION AS OF (\d+)", sql)
        key = int(pin.group(1)) if pin else None
        found, evidence = self.case.get(key, (3, 3))
        return [[_cell(found), _cell(evidence)]]


def _cell(v):
    return None if v is None else str(v)


def _continuous(ticks, rounds, offset=0.0):
    return {
        "window": {"cluster_clock_offset_seconds": offset},
        "retention": {"configured": "30m", "applied_expire": "1h", "rounds": rounds},
        "time_travel": {"ticks": ticks},
    }


POLICY = {"configured": "30m", "applied_expire": "1h"}


def _record(cont, engine, pol=POLICY):
    return ttq.retention_record(cont, engine.run_tsv, "lakehouse", pol)


def _states(record):
    return [t["state"] for t in record["ticks"]]


# --- the retention check -----------------------------------------------------


def test_present_snapshot_whose_case_reads_back_is_verified():
    cont = _continuous([_rec(2, T2, alerts=A2)], [])
    eng = _Engine(live=[T2], case={A2: (3, 3)})
    got = _record(cont, eng)
    assert _states(got) == ["verified"] and got["verdict"] == "pass"
    assert got["ticks"][0]["rows"] == 3 and "read_s" in got["ticks"][0]


def test_a_short_case_read_is_a_mismatch():
    cont = _continuous([_rec(2, T2, alerts=A2)], [])
    got = _record(cont, _Engine(live=[T2], case={A2: (1, 3)}))
    assert _states(got) == ["mismatch"] and got["verdict"] == "fail"
    assert "1 of 3" in got["ticks"][0]["reason"]


def test_a_present_snapshot_with_no_case_evidence_is_verified_hash_only():
    cont = _continuous([_rec(2, T2, alerts=A2), _rec(3, T3, alerts=A3)], [])
    # T2 has a readable case; T3's alert has no payments (evidence 0).
    got = _record(cont, _Engine(live=[T2, T3], case={A2: (3, 3), A3: (0, 0)}))
    assert _states(got) == ["verified", "verified_hash_only"]
    assert got["verdict"] == "pass"


def test_a_tick_with_no_pinned_alerts_snapshot_is_verified_hash_only():
    cont = _continuous([_rec(2, T2, alerts="none")], [])
    got = _record(cont, _Engine(live=[T2]))
    assert _states(got) == ["verified_hash_only"]
    assert "no pinned alerts snapshot" in got["ticks"][0]["reason"]


def test_a_snapshot_listing_failure_is_an_error_for_its_ticks():
    cont = _continuous([_rec(2, T2)], [])
    got = _record(cont, _Engine(live=[T2], fails={"transactions"}))
    assert _states(got) == ["error"] and got["verdict"] == "fail"
    assert "snapshot list 500" in got["ticks"][0]["reason"]


def test_a_snapshot_is_listed_once_per_table():
    cont = _continuous([_rec(2, T2, alerts=A2), _rec(3, T3, alerts=A3)], [])
    eng = _Engine(live=[T2, T3])
    _record(cont, eng)
    # One $snapshots read for the shared transactions table, two point reads.
    assert sum("$snapshots" in s for s in eng.reads) == 1


# --- expiry attribution (an absent snapshot) ---------------------------------


def _round(n, ended, applied="1h", tables=("lakehouse.silver.transactions",)):
    return {
        "round": n,
        "started_at": ended,
        "ended_at": ended,
        "applied_expire": applied,
        "expired_tables": list(tables),
    }


def test_expired_snapshot_explained_by_a_covering_round():
    ticks = [_rec(1, T1, committed="2026-10-03T10:00:00Z"), _rec(2, T2)]
    cont = _continuous(
        ticks, [_round(1, "2026-10-03T10:30:00Z"), _round(2, "2026-10-03T11:05:00Z")]
    )
    got = _record(cont, _Engine(live=[T2], case={A2: (3, 3)}))
    assert got["verdict"] == "pass" and _states(got)[0] == "expired"
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
    assert got["policy"] == POLICY


def test_expired_snapshot_no_round_explains_is_missing_unexplained():
    ticks = [_rec(1, T1, committed="2026-10-03T10:00:00Z"), _rec(2, T2)]
    cont = _continuous(ticks, [_round(1, "2026-10-03T10:30:00Z")])
    got = _record(cont, _Engine(live=[T2], case={A2: (3, 3)}))
    assert got["ticks"][0]["state"] == "missing_unexplained"
    assert got["verdict"] == "fail" and "missing_unexplained" in got["reason"]


def test_a_round_that_did_not_expire_the_table_explains_nothing():
    ticks = [_rec(1, T1, committed="2026-10-03T10:00:00Z"), _rec(2, T2)]
    cont = _continuous(
        ticks, [_round(1, "2026-10-03T12:00:00Z", tables=("lakehouse.gold.alerts",))]
    )
    got = _record(cont, _Engine(live=[T2], case={A2: (3, 3)}))
    assert got["ticks"][0]["state"] == "missing_unexplained"


def test_the_cluster_clock_offset_moves_the_round_end():
    # Host clock round end 11:00; the cluster is 10 minutes behind, so the
    # cutoff on the cluster clock is 09:50 and a 09:55 commit is not covered.
    ticks = [_rec(1, T1, committed="2026-10-03T09:55:00Z"), _rec(2, T2)]
    rounds = [_round(1, "2026-10-03T11:00:00Z")]
    eng = _Engine(live=[T2], case={A2: (3, 3)})
    assert _record(_continuous(ticks, rounds, 0.0), eng)["ticks"][0]["state"] == "expired"
    got = _record(
        _continuous([dict(t) for t in ticks], rounds, -600.0),
        _Engine(live=[T2], case={A2: (3, 3)}),
    )
    assert got["ticks"][0]["state"] == "missing_unexplained"


@pytest.mark.parametrize(
    ("configured", "reason"),
    [("1h", "configured retention"), (None, "configured retention unknown")],
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
    got = _record(cont, _Engine(live=[T2], case={A2: (3, 3)}), pol)
    assert got["ticks"][0]["state"] == "missing_unexplained" and got["verdict"] == "fail"
    assert "maintenance skipped" in tt.line(got)


# --- verdict, kept pure ------------------------------------------------------


@pytest.mark.parametrize(
    ("states", "verdict"),
    [
        ([], "fail"),
        (["verified", "mismatch"], "fail"),
        (["verified_hash_only"], "fail"),
        (["verified", "verified_hash_only"], "pass"),
        (["expired", "verified"], "pass"),
        (["verified", "something_new"], "fail"),
    ],
)
def test_verdict(states, verdict):
    got, reason = tt.verdict_of([{"state": s} for s in states], incomplete=False)
    assert got == verdict
    assert (reason == "") == (verdict == "pass")


# --- the CLI step ------------------------------------------------------------


def _cfg():
    from tests.conftest import make_config

    return make_config(
        recipe="hive-iceberg-spark-trino",
        workload={"schema": "financial"},
        architecture={"pipeline": {"mode": "continuous"}},
    )


def _step(cont, engine, *, run_failed=False):
    from lakebench.cli import _aml_post

    executor = SimpleNamespace(query_rows=engine.run_tsv, catalog_name="lakehouse")
    with patch("lakebench.benchmark.executor.get_executor", return_value=executor):
        return _aml_post.run_time_travel(_cfg(), "run-1", cont, timeout=600, run_failed=run_failed)


def test_step_runs_the_queries_and_records_the_verdict():
    cont = _continuous([_rec(2, T2, alerts=A2)], [])
    out = _step(cont, _Engine(live=[T2], case={A2: (3, 3)}))
    assert out is cont["time_travel"] and out["verdict"] == "pass"
    assert _states(out) == ["verified"]


def test_a_failed_run_is_not_read():
    out = _step(_continuous([_rec(2, T2)], []), _Engine(live=[T2]), run_failed=True)
    assert out["verdict"] == "not_run" and out["reason"] == "the run failed its gates"


def test_step_zero_records_fails():
    out = _step(_continuous([], []), _Engine(live=[]))
    assert out["verdict"] == "fail" and out["reason"] == "zero recorded snapshots"


def test_step_never_raises():
    class Broken(_Engine):
        def run_tsv(self, sql, timeout):
            raise OSError("engine down")

    out = _step(_continuous([_rec(2, T2)], []), Broken(live=[T2]))
    assert out["verdict"] == "fail"  # every tick errors, so the check fails


def test_settle_time_travel_verdicts():
    from lakebench.cli._aml_post import settle_time_travel

    # An unfinished run gets a not_run verdict, keeping its ticks.
    run = SimpleNamespace(continuous={"time_travel": {"ticks": [_rec(2, T2)]}})
    settle_time_travel(_cfg(), run, "interrupted before the time-travel reads")
    assert run.continuous["time_travel"]["verdict"] == "not_run"
    assert run.continuous["time_travel"]["ticks"] == [_rec(2, T2)]

    # A finished run's verdict is left untouched.
    done = SimpleNamespace(continuous={"time_travel": {"verdict": "pass"}})
    settle_time_travel(_cfg(), done, "x")
    assert done.continuous["time_travel"] == {"verdict": "pass"}

    # A run that failed its gates after the reads loses the reads' verdict but
    # keeps the ticks.
    failed = SimpleNamespace(
        continuous={"time_travel": {"verdict": "pass", "ticks": [_rec(2, T2)]}}
    )
    settle_time_travel(_cfg(), failed, "x", pipeline_success=False)
    tt_rec = failed.continuous["time_travel"]
    assert tt_rec["verdict"] == "not_run" and tt_rec["reason"] == "the run failed its gates"
    assert tt_rec["ticks"] == [_rec(2, T2)]


# --- maintenance rounds ------------------------------------------------------


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
            "lakebench.modules.table_formats.iceberg.maintenance.find_maintenance_engine",
            return_value=("trino", "trino-0", "lakehouse"),
        ),
        patch("lakebench.modules.table_formats.iceberg.maintenance.exec_sql"),
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


# --- verdict integration -----------------------------------------------------


def test_the_verdict_shows_the_time_travel_check_and_never_fails_on_it():
    from lakebench.metrics.verdict import compute_verdict
    from lakebench.reports.front_matter import _qualifier_lines
    from tests.fixtures.experiment_helpers import _cfg as _exp_cfg
    from tests.fixtures.experiment_helpers import _metrics

    m = _metrics(_exp_cfg(schema="financial", mode="continuous"))
    before = compute_verdict(m)
    assert "time_travel" not in before.qualifiers
    # Tick records alone (no verdict yet) give no line.
    m.continuous = {"time_travel": {"ticks": [_rec(1, T1)]}}
    assert "time_travel" not in compute_verdict(m).qualifiers
    got = _record(_continuous([_rec(2, T2, alerts=A2)], []), _Engine(live=[], case={A2: (3, 3)}))
    assert got["verdict"] == "fail"  # T2 is not live -> missing_unexplained
    m.continuous = {"time_travel": got}
    after = compute_verdict(m)
    text = after.qualifiers["time_travel"]
    assert text == tt.line(got) and text.startswith("time-travel check: FAIL")
    assert after.status == before.status
    assert _qualifier_lines({"time_travel": text}) == [text]
