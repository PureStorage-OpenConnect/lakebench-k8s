"""AML investigator sessions under load: the extra round (AM-14).

N concurrent sessions, one open case each in IQ1's queue order, run IQ1, IQ2
and IQ3 bound to their case and IQ4 unchanged, once each, right after the
first in-stream round that included the investigator queries. The round is
recorded as ``continuous.investigators``, never as a benchmark round; ticks
that overlap its window are labelled.
"""

from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from lakebench.benchmark import BenchmarkRunner
from lakebench.benchmark import investigator_sessions as inv
from lakebench.benchmark.queries import (
    INVESTIGATOR_QUERIES,
    SESSION_SUFFIX,
    bind_case,
    get_benchmark_queries,
    query_set_id,
)
from lakebench.benchmark.result import QueryExecutorResult
from lakebench.config.schema import WorkloadSchema
from lakebench.metrics import MetricsCollector
from lakebench.metrics.collector import parse_tick_timing
from lakebench.metrics.tick_records import investigator_tick_overlap
from tests.conftest import make_config

RUN = "20261003-120000-abc123"
CASES = ["case-" + c * 24 for c in "abc"]


# --- bind_case and the default SQL ------------------------------------------


def test_bind_case_gives_each_case_its_own_sql():
    by_query = {q.name: [bind_case(q, c) for c in CASES] for q in INVESTIGATOR_QUERIES}
    for name in ("IQ1_customer_360", "IQ2_case_activity_12m", "IQ3_counterparty_two_hop"):
        bound = by_query[name]
        assert len({b.sql for b in bound}) == 3, name
        for b, case in zip(bound, CASES, strict=True):
            assert b.name == name + SESSION_SUFFIX
            assert f"AND case_id = '{case}'" in b.sql
            assert "LIMIT 1\n)" not in b.sql  # the queue pick is gone
    # IQ4 takes no case.
    iq4 = by_query["IQ4_open_cases_over_60_days"]
    assert all(b is INVESTIGATOR_QUERIES[3] for b in iq4)
    # IQ2 bound to a case may still be empty (no escalation case).
    assert by_query["IQ2_case_activity_12m"][0].allow_empty
    assert "customer_id, opened_date" in by_query["IQ2_case_activity_12m"][0].sql


@pytest.mark.parametrize("bad", ["case-xyz", "x' OR '1'='1", "case-" + "a" * 24 + "'", ""])
def test_bind_case_takes_only_a_case_id(bad):
    with pytest.raises(ValueError, match="not a case id"):
        bind_case(INVESTIGATOR_QUERIES[0], bad)


#: The default investigator SQL, pinned: the subject constants render it
#: byte for byte as before (the qs12 id did not move).
PINNED = {
    "IQ1_customer_360": "377015e8166e3946c814cc4e1cf2e2af941640a84197150d1020bcb3d8e5ab76",
    "IQ2_case_activity_12m": "b0846dddea2d8c3a38688f1e4216e06c93ed22dcef12f366d13fe351c48c40db",
    "IQ3_counterparty_two_hop": "418ac05cbc52a04e14297914e13f77ac352a6a4a26e914aedf512ac9595b3bf0",
    "IQ4_open_cases_over_60_days": "63109f35baa824da5dd217907707257e9f508d60829f086483b9dddcac7a4a92",
}


def test_default_investigator_sql_is_unchanged():
    got = {q.name: hashlib.sha256(q.sql.encode()).hexdigest() for q in INVESTIGATOR_QUERIES}
    assert got == PINNED
    names = [q.name for q in get_benchmark_queries(WorkloadSchema.FINANCIAL)]
    assert query_set_id(names) == "qs12-4bd2d9416abb"


def test_the_case_query_orders_as_iq1():
    runner = SimpleNamespace(
        _extra_tables={"gold_cases": "gold.cases"},
        tm_run_id=RUN,
        catalog="lakehouse",
        executor=SimpleNamespace(adapt_query=lambda s: s),
    )
    sql = inv.case_query(runner, 8)
    iq1 = INVESTIGATOR_QUERIES[0].sql
    order = iq1[iq1.index("ORDER BY") + len("ORDER BY") : iq1.index("LIMIT 1")]
    assert " ".join(order.split()) in sql
    assert sql.endswith("LIMIT 8") and f"base_run_id = '{RUN}'" in sql


# --- run_throughput with one list per stream -----------------------------------


class _Exec:
    """Answers every query after a short wait; the case query answers
    *cases*; queries whose SQL holds *fail_on* fail."""

    catalog_name = "lakehouse"

    def __init__(self, cases=(), rows=3, fail_on=None, empty_on=None):
        self.cases = list(cases)
        self.rows = rows
        self.fail_on = fail_on
        self.empty_on = empty_on
        self.lock = threading.Lock()
        self.sql: list[str] = []

    def engine_name(self):
        return "trino"

    def flush_cache(self):
        pass

    def adapt_query(self, sql):
        return sql

    def execute_query(self, sql, timeout=300):
        with self.lock:
            self.sql.append(sql)
        if sql.startswith("SELECT case_id FROM"):
            out = "".join(f'"{c}"\n' for c in self.cases)
            return QueryExecutorResult(sql, "trino", 0.01, len(self.cases), out)
        error = (
            "Query exceeded per-node memory limit" if self.fail_on and self.fail_on in sql else None
        )
        rows = 0 if (error or (self.empty_on and self.empty_on in sql)) else self.rows
        return QueryExecutorResult(sql, "trino", 0.01, rows, "x\n" * rows, error=error)


def _runner(executor):
    cfg = make_config(
        workload={"schema": "financial"},
        architecture={"pipeline": {"mode": "continuous"}},
    )
    with patch("lakebench.benchmark.executor.get_executor", return_value=executor):
        return BenchmarkRunner(cfg, tm_run_id=RUN)


def test_each_stream_runs_exactly_its_list_in_order():
    runner = _runner(_Exec())
    lists = [inv.session_queries(c) for c in CASES]
    result = runner.run_throughput(
        cache="hot", iterations=1, fingerprint=False, stream_queries=lists, shuffle=False
    )
    assert result.streams == 3 and len(result.stream_results) == 3
    for stream, want in zip(result.stream_results, lists, strict=True):
        assert [r.query.name for r in stream.queries] == [q.name for q in want]
        assert [r.query.sql for r in stream.queries] == [q.sql for q in want]
    # Every query ran once (iterations=1) and nothing was fingerprinted.
    assert len(runner.executor.sql) == 12


def test_the_default_throughput_path_still_shuffles():
    runner = _runner(_Exec())
    with patch("lakebench.benchmark.runner.random.shuffle") as shuffled:
        runner.run_throughput(streams=2, fingerprint=False)
    assert shuffled.call_count == 2
    with patch("lakebench.benchmark.runner.random.shuffle") as shuffled:
        runner.run_throughput(streams=2, fingerprint=False, shuffle=False)
    assert shuffled.call_count == 0


def test_nearest_rank():
    xs = [5.0, 1.0, 4.0, 2.0, 3.0, 9.0, 8.0, 7.0, 6.0, 10.0]
    assert inv.nearest_rank(xs, 50) == 5.0
    assert inv.nearest_rank(xs, 95) == 10.0
    assert inv.nearest_rank([2.0], 95) == 2.0
    assert inv.nearest_rank([], 50) is None


def _clock():
    t = [datetime(2026, 10, 3, 12, 0, 0, tzinfo=timezone.utc)]

    def now():
        t[0] += timedelta(seconds=30)
        return t[0]

    return now


# --- the record ----------------------------------------------------------------


def test_sessions_record_on_success():
    runner = _runner(_Exec(cases=CASES))
    rec = inv.run_sessions(runner, 3, {"IQ1_customer_360": 1.5}, now=_clock(), remaining_s=10_000)
    assert rec["status"] == "pass" and rec["sessions_run"] == 3
    assert rec["lowered_reason"] is None and rec["case_ids"] == CASES
    assert set(rec["rows_per_session"]) == set(CASES)
    assert rec["rows_per_session"][CASES[0]] == {q.name: 3 for q in INVESTIGATOR_QUERIES}
    assert rec["latency"]["IQ1_customer_360"]["n"] == 3
    assert rec["latency"]["IQ3_counterparty_two_hop"]["p95_s"] is not None
    assert rec["baseline"] == {"IQ1_customer_360": 1.5}
    assert rec["window"] == {
        "start": "2026-10-03T12:00:30Z",
        "end": "2026-10-03T12:01:00Z",
        "clock": "lakebench host (UTC)",
    }
    assert set(rec["seconds_per_session"][CASES[1]]) == {q.name for q in INVESTIGATOR_QUERIES}
    assert rec["latency"]["IQ2_case_activity_12m"]["failed"] == 0
    assert rec["query_timeout_s"] == 300
    assert set(rec["session_sql"]) == {q.name for q in INVESTIGATOR_QUERIES}
    assert "n=1 per arm" in rec["labels"] and "shared S3 contention" in rec["labels"]


def test_fewer_cases_lower_the_sessions():
    rec = inv.run_sessions(_runner(_Exec(cases=CASES[:2])), 8, {}, now=_clock(), remaining_s=1200)
    assert (rec["sessions_run"], rec["lowered_reason"]) == (2, "fewer cases than sessions")


def test_no_case_skips_the_round():
    rec = inv.run_sessions(_runner(_Exec(cases=[])), 8, {}, now=_clock(), remaining_s=1200)
    assert (rec["status"], rec["sessions_run"]) == ("no_cases", 0)


def test_an_empty_iq1_or_iq3_fails_the_check():
    rec = inv.run_sessions(
        _runner(_Exec(cases=CASES, empty_on="hop1 AS")), 3, {}, now=_clock(), remaining_s=1200
    )
    assert rec["status"] == "fail" and rec["failed"] == []
    assert {e["query"] for e in rec["empty"]} == {"IQ3_counterparty_two_hop"}


def test_an_empty_iq2_or_iq4_does_not():
    rec = inv.run_sessions(
        _runner(_Exec(cases=CASES, empty_on="activity_month")),
        3,
        {},
        now=_clock(),
        remaining_s=1200,
    )
    assert rec["status"] == "pass", rec


def test_a_memory_failure_fails_the_check_and_names_the_bound():
    rec = inv.run_sessions(
        _runner(_Exec(cases=CASES, fail_on="hop2 AS")), 3, {}, now=_clock(), remaining_s=1200
    )
    assert rec["status"] == "fail" and len(rec["failed"]) == 3
    assert inv.MEMORY_BOUNDS["trino"] in rec["labels"]
    assert rec["latency"]["IQ3_counterparty_two_hop"] == {
        "p50_s": None,
        "p95_s": None,
        "n": 0,
        "failed": 3,
    }


def test_a_failed_case_query_is_recorded():
    runner = _runner(_Exec())
    runner.executor.execute_query = lambda sql, timeout=300: QueryExecutorResult(
        sql, "trino", 0.0, 0, "", error="boom"
    )
    rec = inv.run_sessions(runner, 4, {}, now=_clock(), remaining_s=1200)
    assert rec["status"] == "case_query_failed" and "boom" in rec["reason"]


# --- tick overlap ----------------------------------------------------------------


def _tick(end, total):
    return {"ended_at": end, "phases": {"total": total}}


def test_overlap_classifier_at_49_50_and_0_percent():
    rec = {"window": {"start": "2026-10-03T12:00:00Z", "end": "2026-10-03T12:01:00Z"}}
    ticks = [
        _tick("2026-10-03T12:00:05Z", 10.0),  # 5 of 10 s inside: 50%, overlaps
        _tick("2026-10-03T12:00:04.9Z", 10.0),  # 4.9 of 10 s inside: 49%, neither
        _tick("2026-10-03T11:59:50Z", 10.0),  # ends at the window start: clean
        _tick("2026-10-03T12:00:30Z", 20.0),  # all inside
    ]
    out = investigator_tick_overlap(rec, ticks, None)
    assert out["tick_delta"]["overlapping"] == {"n": 2, "median_total_s": 15.0}
    assert out["tick_delta"]["clean"] == {"n": 1, "median_total_s": 10.0}
    assert out["load_label"].endswith(": 2 of 4 ticks overlap")
    # The continuous window keeps only the ticks that ended inside it: the
    # clean tick ended before the window opened (a warm-up tick).
    inside = investigator_tick_overlap(
        rec, ticks, None, window={"start": "2026-10-03T12:00:00Z", "end": "2026-10-03T13:00:00Z"}
    )
    assert inside["tick_delta"]["clean"] == {"n": 0, "median_total_s": None}
    assert inside["load_label"].endswith(": 2 of 3 ticks overlap")


def test_overlap_shifts_ticks_to_the_cli_clock():
    """A cluster clock 30 s ahead: a tick that ended at 12:00:35 cluster
    time ended at 12:00:05 on the CLI clock."""
    rec = {"window": {"start": "2026-10-03T12:00:00Z", "end": "2026-10-03T12:00:10Z"}}
    out = investigator_tick_overlap(rec, [_tick("2026-10-03T12:00:35Z", 5.0)], 30.0)
    assert out["tick_delta"]["overlapping"]["n"] == 1


def test_tick_timing_lines_carry_their_end_time():
    line = (
        "[lb] 2026-10-03T12:00:05.123456 - Cycle 4: tick timing silver_rows=10 "
        "probe=0.5s total=3.0s"
    )
    tt = parse_tick_timing(line)
    assert tt["ended_at"] == "2026-10-03T12:00:05.123456Z" and tt["phases"]["total"] == 3.0
    assert tt["started_at"] == "2026-10-03T12:00:02.123456Z"
    assert parse_tick_timing("Cycle 4: tick timing silver_rows=1 total=1.0s")["ended_at"] is None


# --- the CLI hooks -----------------------------------------------------------------


def _collector_with_round(state):
    collector = MetricsCollector()
    collector.start_run(RUN, "t", {})
    rnd = SimpleNamespace(
        round_record={"investigator_queries": state},
        queries=[{"name": "IQ1_customer_360", "elapsed_seconds": 2.0, "success": True}],
    )
    collector.current_run.benchmark_rounds.append(rnd)
    return collector


def test_sessions_wait_for_a_round_with_a_case():
    from lakebench.cli._sustained import investigator_sessions_after_round

    collector = _collector_with_round("absent_no_cases")
    pending = investigator_sessions_after_round(
        MagicMock(), collector, MagicMock(), MagicMock(), 8, remaining_s=900, baseline_round_s=60
    )
    assert pending is True and collector.current_run.continuous is None


def test_sessions_need_two_baseline_rounds_left():
    from lakebench.cli._sustained import investigator_sessions_after_round

    collector = _collector_with_round("included")
    runner = MagicMock()
    pending = investigator_sessions_after_round(
        runner, collector, MagicMock(), MagicMock(), 8, remaining_s=119, baseline_round_s=60
    )
    rec = collector.current_run.continuous["investigators"]
    assert pending is False and rec["status"] == "no_time" and rec["sessions_run"] == 0
    runner.run_throughput.assert_not_called()


def test_sessions_run_after_the_baseline_round(monkeypatch):
    from lakebench.cli._sustained import investigator_sessions_after_round

    collector = _collector_with_round("included")
    seen = {}

    def fake(runner, requested, baseline, *, now, remaining_s):
        seen.update(requested=requested, baseline=baseline, remaining_s=remaining_s)
        return {"status": "pass", "sessions_run": 8, "sessions_requested": 8}

    monkeypatch.setattr(inv, "run_sessions", fake)
    pending = investigator_sessions_after_round(
        MagicMock(), collector, MagicMock(), MagicMock(), 8, remaining_s=120, baseline_round_s=60
    )
    assert pending is False
    assert seen == {"requested": 8, "baseline": {"IQ1_customer_360": 2.0}, "remaining_s": 120}
    assert collector.current_run.continuous["investigators"]["status"] == "pass"
    # Not a benchmark round: the round count does not move.
    assert len(collector.current_run.benchmark_rounds) == 1


@pytest.mark.parametrize(
    ("rounds_ran", "status", "why"),
    [(True, "no_cases", "found a case"), (False, "no_rounds", "--skip-benchmark")],
)
def test_window_close_records_a_round_that_never_ran(rounds_ran, status, why):
    from lakebench.cli._sustained import finish_investigator_sessions

    cont: dict = {}
    finish_investigator_sessions(
        cont, 8, pending=True, rounds_ran=rounds_ran, ticks=[], clock_offset_s=None
    )
    assert cont["investigators"]["status"] == status and why in cont["investigators"]["reason"]


def test_window_close_adds_the_tick_overlap():
    from lakebench.cli._sustained import finish_investigator_sessions

    cont = {
        "investigators": {
            "status": "pass",
            "window": {"start": "2026-10-03T12:00:00Z", "end": "2026-10-03T12:01:00Z"},
        }
    }
    finish_investigator_sessions(
        cont,
        8,
        pending=False,
        rounds_ran=True,
        ticks=[_tick("2026-10-03T12:00:30Z", 10.0), _tick("2026-10-03T11:58:00Z", 10.0)],
        clock_offset_s=0.0,
    )
    rec = cont["investigators"]
    assert rec["tick_delta"]["overlapping"]["n"] == 1 and rec["tick_delta"]["clean"]["n"] == 1
    assert "1 of 2 ticks overlap" in rec["load_label"]


def test_the_query_timeout_keeps_the_round_inside_the_window():
    """Four queries per stream, each at most the timeout: they end inside
    the time left, and too little time skips the round."""
    assert inv.query_timeout(10_000) == 300
    assert inv.query_timeout(400) == 90  # 400 / 4 less the 10 s cleanup margin
    assert inv.query_timeout(-5) == 0
    assert inv.case_query_timeout(10_000) == 120 and inv.case_query_timeout(200) == 40
    rec = inv.run_sessions(_runner(_Exec(cases=CASES)), 3, {}, now=_clock(), remaining_s=150)
    assert rec["status"] == "no_time" and rec["sessions_run"] == 0


def test_the_case_pick_spends_from_the_same_budget(monkeypatch):
    """A slow case pick leaves less for the sessions: their timeout is taken
    from what is left after it, and too little left skips them."""
    import time as _time

    runner = _runner(_Exec(cases=CASES))
    real = runner.executor.execute_query
    seen = {}
    clock = [1000.0]
    monkeypatch.setattr(_time, "monotonic", lambda: clock[0])

    def slow_pick(sql, timeout=300):
        if sql.startswith("SELECT case_id"):
            seen["case_timeout"] = timeout
            clock[0] += 60  # the pick took a minute
        else:
            seen.setdefault("query_timeouts", set()).add(timeout)
        return real(sql, timeout)

    runner.executor.execute_query = slow_pick
    rec = inv.run_sessions(runner, 3, {}, now=_clock(), remaining_s=400)
    assert seen["case_timeout"] == 80  # a fifth of 400
    assert rec["query_timeout_s"] == 75 and seen["query_timeouts"] == {75}  # (400-60)/4-10
    clock[0] = 1000.0
    rec = inv.run_sessions(runner, 3, {}, now=_clock(), remaining_s=200)
    assert rec["status"] == "no_time"  # 40 s per query before the pick, (200-60)/4-10 = 25 after


def test_a_timed_out_query_names_the_lakebench_timeout():
    runner = _runner(_Exec(cases=CASES))
    real = runner.executor.execute_query

    def slow(sql, timeout=300):
        if "hop2 AS" in sql:
            return QueryExecutorResult(
                sql, "trino", timeout, 0, "", error=f"Query timed out ({timeout}s)"
            )
        return real(sql, timeout)

    runner.executor.execute_query = slow
    rec = inv.run_sessions(runner, 3, {}, now=_clock(), remaining_s=522)
    assert rec["query_timeout_s"] == 120
    assert "BOUNDED BY Lakebench per-query timeout (120s)" in rec["labels"]


def test_trinos_own_run_time_limit_is_the_lakebench_timeout():
    """The Trino executor sets query_max_run_time just under the timeout,
    so the server fails the query first, with its own message."""
    runner = _runner(_Exec(cases=CASES))
    real = runner.executor.execute_query

    def limited(sql, timeout=300):
        if "hop2 AS" in sql:
            return QueryExecutorResult(
                sql, "trino", timeout, 0, "", error="Query exceeded maximum time limit of 1.92m"
            )
        return real(sql, timeout)

    runner.executor.execute_query = limited
    rec = inv.run_sessions(runner, 3, {}, now=_clock(), remaining_s=522)
    assert "BOUNDED BY Lakebench per-query timeout (120s)" in rec["labels"]


def test_the_verdict_shows_the_investigators_check_and_the_load():
    from lakebench.metrics.verdict import investigators_qualifier
    from lakebench.reports.front_matter import _qualifier_lines

    rec = {
        "status": "fail",
        "sessions_run": 8,
        "sessions_requested": 8,
        "failed": [{"query": "IQ3_counterparty_two_hop"}],
        "empty": [],
        "load_label": "investigator load A-B: 3 of 9 ticks overlap",
        "labels": ["n=1 per arm", inv.MEMORY_BOUNDS["trino"]],
    }
    text = investigators_qualifier(rec)
    assert text.startswith("investigators check: FAIL (8 of 8 sessions; 1 failed")
    assert "not a run FAIL" in text
    assert "3 of 9 ticks overlap: time to detect and continuous throughput" in text
    assert inv.MEMORY_BOUNDS["trino"] in text
    assert _qualifier_lines({"investigators": text}) == [text]
    skipped = investigators_qualifier(inv.skipped(8, "no_time", "40s left"))
    assert skipped == "investigators check: no_time (40s left)"


def test_a_run_without_sessions_gets_no_qualifier():
    from lakebench.metrics.verdict import compute_verdict
    from tests.test_experiment import _cfg, _metrics

    m = _metrics(_cfg(schema="financial", mode="continuous"))
    assert "investigators" not in compute_verdict(m).qualifiers
    m.continuous = {"investigators": inv.skipped(8, "no_cases", "no case")}
    assert (
        compute_verdict(m).qualifiers["investigators"].startswith("investigators check: no_cases")
    )
