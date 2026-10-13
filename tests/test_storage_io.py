"""Storage I/O in the run record: what the engines report, and what is not measured.

Trino bytes come from ``system.runtime.tasks`` after a stream has finished
(``TrinoExecutor.query_io``), travel on the per-query dicts, and become the
record's ``io`` block (``metrics.io.build_io``). Nothing here talks to a
cluster.
"""

from __future__ import annotations

import copy
import re
import subprocess
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from lakebench.benchmark.queries import BenchmarkQuery
from lakebench.benchmark.result import QueryExecutorResult
from lakebench.benchmark.runner import BenchmarkRunner
from lakebench.metrics.io import (
    BASIS_DATAGEN,
    BASIS_TRINO_READ,
    BASIS_TRINO_WRITE,
    REASON_ENGINE,
    REASON_NOT_RECORDED,
    bucket_series,
    build_io,
)
from lakebench.metrics.storage import MetricsStorage
from lakebench.modules.query_engines.trino.executor import TrinoExecutor
from tests.fixtures.stored_records import load_record

_SRC = re.compile(r"^lakebench-[0-9a-f]{16}$")
_Q = BenchmarkQuery(name="Q1", display_name="Q1", query_class="scan", sql="select 1")


def _trino() -> TrinoExecutor:
    ex = TrinoExecutor(namespace="ns", catalog_name="lakehouse")
    ex._pod = "coordinator-0"
    return ex


def _src(i: int) -> str:
    return f"lakebench-{i:016x}"


def _row(src: str, tasks=2, read=100, write=0, start=1000.0, end=1010.5) -> str:
    return f"{src}\t{tasks}\t{read}\t{write}\t{start}\t{end}"


# -- TrinoExecutor: the tag and the lookup ---------------------------------------


def test_every_result_carries_the_source_the_query_ran_under():
    ex = _trino()
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, "1\n", "")
        ok = ex.execute_query("SELECT 1")
        run.return_value = subprocess.CompletedProcess([], 1, "", "Query q failed: boom")
        failed = ex.execute_query("SELECT 2")
    for res, call in zip((ok, failed), run.call_args_list, strict=True):
        cmd = call.args[0]
        assert _SRC.match(res.engine_source)
        assert res.engine_source == cmd[cmd.index("--source") + 1]


def test_timed_out_query_still_reports_its_source():
    ex = _trino()

    def fake_run(cmd, **kwargs):
        if "SELECT 1" in cmd:
            raise subprocess.TimeoutExpired(cmd, kwargs["timeout"])
        return subprocess.CompletedProcess(cmd, 0, "", "")

    with patch("subprocess.run", side_effect=fake_run):
        res = ex.execute_query("SELECT 1", timeout=30)
    assert res.error and _SRC.match(res.engine_source)


def test_query_io_reads_one_row_per_source():
    ex = _trino()
    a, b = _src(1), _src(2)
    out = "\n".join(
        [_row(a, read=100, write=7), _row(b, read=0, write=0, start=1020.0, end=1021.0)]
    )
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, out + "\n", "WARNING: dumb terminal")
        got = ex.query_io([a, b, a])
    assert run.call_count == 1
    cmd = run.call_args.args[0]
    sql = cmd[cmd.index("--execute") + 1]
    assert cmd[cmd.index("--source") + 1] == "lakebench-io"
    assert f"'{a}'" in sql and f"'{b}'" in sql and sql.count(f"'{a}'") == 1
    assert "system.runtime.tasks" in sql and "physical_input_bytes" in sql
    # The storage columns only: never the decoded, output or exchange sizes.
    for not_storage in (
        "processed_input_bytes",
        "raw_input_bytes",
        "internal_network",
        "output_bytes",
    ):
        assert not_storage not in sql
    assert got[a] == {
        "tasks": 2,
        "read_bytes": 100,
        "write_bytes": 7,
        "start": 1000.0,
        "end": 1010.5,
    }
    assert got[b]["read_bytes"] == 0 and got[b]["write_bytes"] == 0


def test_query_io_is_empty_without_sources_and_ignores_unsafe_ones():
    ex = _trino()
    with patch("subprocess.run") as run:
        assert ex.query_io([]) == {}
        assert ex.query_io(["x'; DROP TABLE t; --", "lakebench-zz"]) == {}
    run.assert_not_called()


def test_query_io_skips_rows_it_cannot_read_and_null_cells():
    ex = _trino()
    a = _src(3)
    out = f"{a}\t1\t\\N\t5\t\t\nnot a row\n"
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, out, "")
        got = ex.query_io([a])
    assert got[a]["read_bytes"] is None and got[a]["write_bytes"] == 5
    assert got[a]["start"] is None and got[a]["end"] is None
    assert list(got) == [a]


def test_query_io_never_raises():
    ex = _trino()
    with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("kubectl", 10)):
        assert ex.query_io([_src(4)]) == {}
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 1, "", "Query failed: no such table")
        assert ex.query_io([_src(4)]) == {}
    ex2 = TrinoExecutor(namespace="ns", catalog_name="lakehouse")
    with patch.object(TrinoExecutor, "_discover_pod", side_effect=RuntimeError("no coordinator")):
        assert ex2.query_io([_src(4)]) == {}


def test_query_io_batches_long_source_lists():
    ex = _trino()
    sources = [_src(i) for i in range(90)]
    with patch("subprocess.run") as run:
        run.return_value = subprocess.CompletedProcess([], 0, "", "")
        ex.query_io(sources)
    assert run.call_count == 3  # 40 + 40 + 10


# -- the runner: attach the bytes after the stream -------------------------------


def _runner(executor) -> BenchmarkRunner:
    runner = BenchmarkRunner.__new__(BenchmarkRunner)
    runner.executor = executor
    runner.catalog = "c"
    runner.silver_table = "s"
    runner.gold_table = "g"
    runner._extra_tables = {}
    runner.tm_run_id = None
    return runner


def _executor(lookup=None):
    n = iter(range(1, 1000))

    def _exec(sql, timeout=300):
        return QueryExecutorResult(sql, "trino", 1.5, 1, "", engine_source=_src(next(n)))

    ex = MagicMock()
    ex.execute_query.side_effect = _exec
    ex.adapt_query.side_effect = lambda s: s
    ex.flush_cache.return_value = None
    if lookup is None:
        del ex.query_io  # an engine with no per-query report
    else:
        ex.query_io = lookup
    return ex


def test_samples_of_a_query_are_summed_and_windowed():
    seen: list[list[str]] = []

    def lookup(sources):
        seen.append(list(sources))
        return {
            _src(1): {"read_bytes": 10, "write_bytes": 0, "start": 100.0, "end": 101.0},
            _src(2): {"read_bytes": 30, "write_bytes": 5, "start": 103.0, "end": 104.5},
        }

    runner = _runner(_executor(lookup))
    [res] = runner._run_query_stream([_Q], "hot", 2)
    assert len(seen) == 1 and seen[0] == [_src(1), _src(2)]  # one lookup for the stream
    assert res.engine_io == {
        "read_bytes": 40,
        "write_bytes": 5,
        "start_epoch": 100.0,
        "end_epoch": 104.5,
        "samples": 2,
        "captured": 2,
    }
    assert res.to_dict()["io"] == res.engine_io


def test_a_missing_report_shows_as_a_short_capture_not_as_zero():
    runner = _runner(_executor(lambda sources: {_src(1): {"read_bytes": 10, "write_bytes": 0}}))
    [res] = runner._run_query_stream([_Q], "hot", 2)
    assert res.engine_io["samples"] == 2 and res.engine_io["captured"] == 1
    assert res.engine_io["read_bytes"] == 10


def test_no_report_at_all_is_recorded_as_none_not_zero():
    runner = _runner(_executor(lambda sources: {}))
    [res] = runner._run_query_stream([_Q], "hot", 1)
    assert res.engine_io["captured"] == 0
    assert res.engine_io["read_bytes"] is None and res.engine_io["write_bytes"] is None


def test_a_failing_lookup_never_fails_the_benchmark():
    def boom(sources):
        raise OSError("kubectl gone")

    runner = _runner(_executor(boom))
    [res] = runner._run_query_stream([_Q], "hot", 1)
    assert res.success and res.engine_io["captured"] == 0


def test_an_engine_without_a_report_leaves_io_unset():
    runner = _runner(_executor(None))
    [res] = runner._run_query_stream([_Q], "hot", 1)
    assert res.engine_io is None
    assert "io" not in res.to_dict()


def test_a_failed_sample_keeps_its_source_for_the_lookup():
    ex = _executor(lambda sources: {})
    ex.execute_query.side_effect = None
    ex.execute_query.return_value = QueryExecutorResult(
        "select 1", "trino", 300.0, 0, "", error="timeout", engine_source=_src(9)
    )
    runner = _runner(ex)
    [res] = runner._run_query_stream([_Q], "hot", 3)
    assert not res.success and res.engine_sources == [_src(9)]


def test_probe_timing_does_not_look_anything_up():
    lookup = MagicMock(return_value={})
    runner = _runner(_executor(lookup))
    res = runner.time_query(_Q, iterations=2)
    lookup.assert_not_called()
    assert res.engine_io is None


# -- build_io: the record's io block ---------------------------------------------


def _bench(queries, engine="trino"):
    return {"engine": engine, "queries": queries}


def _q(name, read, write, start=1_800_000_000.0, end=1_800_000_004.0, samples=1, captured=1):
    return {
        "name": name,
        "io": {
            "read_bytes": read,
            "write_bytes": write,
            "start_epoch": start,
            "end_epoch": end,
            "samples": samples,
            "captured": captured,
        },
    }


def test_trino_queries_become_intervals_with_their_basis():
    rec = {"benchmark": _bench([_q("Q1", 1000, 0), _q("Q2", 500, 20)])}
    io = build_io(rec)
    assert io["version"] == 1
    first = io["intervals"][0]
    assert (
        first["source"] == "trino" and first["stage"] == "query" and first["label"] == "benchmark"
    )
    assert first["read_basis"] == BASIS_TRINO_READ and first["write_basis"] == BASIS_TRINO_WRITE
    assert first["start"].endswith("+00:00") and first["clock"] == "trino_coordinator"
    assert io["totals"]["trino"] == {"read_bytes": 1500, "write_bytes": 20}
    assert io["problems"] == []


def test_rounds_are_labelled_and_the_mirrored_benchmark_is_not_read_twice():
    rec = {
        "benchmark": _bench([_q("Q1", 10, 0)]),
        "pipeline_benchmark": {
            "query_benchmark": _bench([_q("Q1", 10, 0)]),  # mirrors "benchmark"
            "pre_compaction_benchmark": _bench([_q("Q1", 7, 0)]),
        },
        "benchmark_rounds": [
            {**_bench([_q("Q1", 3, 0)]), "round_meta": {"round_index": 1}},
            {**_bench([_q("Q1", 4, 0)]), "round_meta": {"round_index": 2}},
        ],
    }
    io = build_io(rec)
    assert [i["label"] for i in io["intervals"]] == [
        "pre_compaction",
        "benchmark",
        "round_1",
        "round_2",
    ]
    assert io["totals"]["trino"]["read_bytes"] == 7 + 10 + 3 + 4


def test_none_stays_none_and_zero_stays_zero():
    rec = {"benchmark": _bench([_q("Q1", None, 0, captured=0)])}
    io = build_io(rec)
    iv = io["intervals"][0]
    assert iv["read_bytes"] is None and iv["write_bytes"] == 0 and iv["complete"] is False
    assert io["totals"]["trino"] == {"read_bytes": None, "write_bytes": 0}
    assert io["problems"] == ["trino: 1 query sample(s) had no I/O report"]


def test_an_older_trino_record_says_not_recorded():
    rec = {"benchmark": _bench([{"name": "Q1", "elapsed_seconds": 1.0}])}
    io = build_io(rec)
    assert io["intervals"] == []
    assert io["not_measured"] == [
        {
            "source": "trino",
            "stage": "query",
            "label": "benchmark",
            "direction": "both",
            "reason": REASON_NOT_RECORDED,
        }
    ]


def test_other_query_engines_are_not_instrumented():
    rec = {"benchmark": _bench([{"name": "Q1"}], engine="duckdb")}
    io = build_io(rec)
    assert io["not_measured"] == [
        {
            "source": "duckdb",
            "stage": "query",
            "label": "benchmark",
            "direction": "both",
            "reason": REASON_ENGINE,
        }
    ]


def test_datagen_bytes_come_from_the_fleet_and_are_unplaced_without_a_window():
    rec = {
        "datagen_fleet": {"total_bytes_written": 12345, "pods_missing": 0},
        "pipeline_benchmark": {"stages": [{"stage_name": "datagen"}]},
    }
    io = build_io(rec)
    [iv] = io["intervals"]
    assert iv["source"] == "datagen" and iv["write_bytes"] == 12345 and iv["read_bytes"] is None
    assert iv["write_basis"] == BASIS_DATAGEN and iv["start"] is None and iv["complete"] is True
    assert io["totals"]["datagen"] == {"read_bytes": None, "write_bytes": 12345}
    rec["datagen_fleet"]["pods_missing"] = 2
    assert build_io(rec)["intervals"][0]["complete"] is False


def test_a_record_with_nothing_to_say_has_no_io_block():
    assert build_io({}) is None
    assert build_io({"benchmark": {"engine": "trino", "queries": []}}) is None


# -- bucket_series ---------------------------------------------------------------

_T0 = "2026-01-01T00:00:00+00:00"
_T60 = "2026-01-01T00:01:00+00:00"


def _iv(start, end, read=None, write=None):
    return {"start": start, "end": end, "read_bytes": read, "write_bytes": write}


def test_bytes_are_spread_evenly_over_the_interval():
    s = bucket_series(
        [_iv("2026-01-01T00:00:10+00:00", "2026-01-01T00:00:40+00:00", read=300)],
        step_seconds=15,
        start=_T0,
        end=_T60,
    )
    assert s["read"] == pytest.approx([50.0, 150.0, 100.0, 0.0])
    assert sum(s["read"]) == pytest.approx(300.0) and s["unplaced"] == {"read": 0, "write": 0}


def test_series_conserves_bytes_and_keeps_directions_apart():
    ivs = [
        _iv("2026-01-01T00:00:00+00:00", "2026-01-01T00:00:20+00:00", read=1000, write=0),
        _iv("2026-01-01T00:00:30+00:00", "2026-01-01T00:00:59+00:00", write=777),
    ]
    s = bucket_series(ivs, step_seconds=10, start=_T0, end=_T60)
    assert sum(s["read"]) == pytest.approx(1000) and sum(s["write"]) == pytest.approx(777)
    assert len(s["read"]) == len(s["write"]) == 6


def test_zero_length_interval_lands_in_one_bucket():
    s = bucket_series(
        [_iv("2026-01-01T00:00:31+00:00", "2026-01-01T00:00:31+00:00", write=50)],
        step_seconds=15,
        start=_T0,
        end=_T60,
    )
    assert s["write"] == [0.0, 0.0, 50.0, 0.0]


def test_unplaced_and_out_of_window_bytes_are_reported_not_dropped():
    ivs = [
        _iv(None, None, write=900),
        _iv("2025-12-31T23:00:00+00:00", "2025-12-31T23:30:00+00:00", read=40),
        _iv("2025-12-31T23:59:30+00:00", "2026-01-01T00:00:30+00:00", read=600),  # half inside
    ]
    s = bucket_series(ivs, step_seconds=30, start=_T0, end=_T60)
    assert sum(s["read"]) == pytest.approx(300)
    assert s["unplaced"] == {"read": 340, "write": 900}


def test_not_measured_adds_nothing_to_the_series():
    s = bucket_series([_iv(_T0, _T60, read=None, write=None)], step_seconds=30, start=_T0, end=_T60)
    assert s["read"] == [0.0, 0.0] and s["unplaced"] == {"read": 0, "write": 0}


# -- the stored record -----------------------------------------------------------


def _load(rec):
    with tempfile.TemporaryDirectory() as tmp:
        return MetricsStorage(tmp)._dict_to_metrics(rec)


def test_stored_batch_record_gets_an_io_block_from_what_it_holds():
    rec = load_record("1320bd")
    assert "io" not in rec
    io = _load(rec).to_dict()["io"]
    assert io["version"] == 1
    sources = {i["source"] for i in io["intervals"]}
    assert "datagen" in sources  # this record has a fleet
    assert any(
        n["source"] == "trino" and n["label"] == "benchmark" and n["reason"] == REASON_NOT_RECORDED
        for n in io["not_measured"]
    )
    assert any(n["source"] == "spark" and n["stage"] == "silver" for n in io["not_measured"])


def test_io_survives_a_load_and_save():
    rec = load_record("1320bd")
    q = rec["benchmark"]["queries"][0]
    q["io"] = {
        "read_bytes": 4096,
        "write_bytes": 0,
        "start_epoch": 1_800_000_000.0,
        "end_epoch": 1_800_000_003.0,
        "samples": 1,
        "captured": 1,
    }
    first = _load(copy.deepcopy(rec)).to_dict()
    trino = [i for i in first["io"]["intervals"] if i["source"] == "trino"]
    assert len(trino) == 1 and trino[0]["read_bytes"] == 4096 and trino[0]["query"] == q["name"]
    assert first["io"]["totals"]["trino"]["read_bytes"] == 4096
    # a second pass over the saved record gives the same block
    assert _load(first).to_dict()["io"] == first["io"]


# -- the report section ----------------------------------------------------------


def _inject_trino_io(rec, *, windows=True):
    """*rec* with Trino I/O on every benchmark query; returns (read, written) totals."""
    read = written = 0
    for k, q in enumerate(rec["benchmark"]["queries"]):
        q["io"] = {
            "read_bytes": 40_000_000 + k * 3_000_000,
            "write_bytes": 0,
            "start_epoch": 1_800_000_000.0 + k * 6 if windows else None,
            "end_epoch": 1_800_000_000.0 + k * 6 + 4.5 if windows else None,
            "samples": 3,
            "captured": 3,
        }
        read += q["io"]["read_bytes"]
        written += q["io"]["write_bytes"]
    return read, written


def _section(html: str) -> str:
    start = html.index("<h2>Storage I/O</h2>")
    return html[start : html.index("</section>", start)]


def _render(rec) -> str:
    from tests.fixtures.report_goldens import render_metrics

    return render_metrics(_load(copy.deepcopy(rec)))


def test_report_totals_name_the_record_fields_they_come_from():
    from tests.fixtures.report_consistency_helpers import mismatches

    rec = load_record("1320bd")
    read, _ = _inject_trino_io(rec)
    html = _render(rec)
    section = _section(html)
    assert 'data-lb-inputs="benchmark.queries[*].io.read_bytes*/1048576"' in section
    assert f"{read / 1024**2:.2f} MiB" in section
    # the datagen total points at the fleet field, and the page agrees with the record
    assert 'data-lb-inputs="datagen_fleet.total_bytes_written*/1073741824"' in section
    assert mismatches(rec, html) == []


def test_report_draws_a_rate_chart_only_when_bytes_have_a_window():
    rec = load_record("1320bd")
    _inject_trino_io(rec, windows=True)
    assert "<svg" in _section(_render(rec))
    assert "MB/s per" in _section(_render(rec))
    rec = load_record("1320bd")
    _inject_trino_io(rec, windows=False)
    section = _section(_render(rec))
    assert "<svg" not in section
    assert "no time window recorded" in section


def test_report_says_not_measured_rather_than_zero():
    section = _section(_render(load_record("1320bd")))
    assert "not measured" in section and "n/a" in section  # datagen reads nothing
    assert "Not measured, Spark (bronze, gold, silver; read and write)" in section
    assert "0.00" not in section


def test_a_zero_that_was_measured_is_shown_as_zero():
    rec = load_record("1320bd")
    _inject_trino_io(rec)
    assert "0.00 MiB" in _section(_render(rec))  # Trino wrote nothing during the benchmark


def test_rate_chart_is_empty_without_bytes():
    from datetime import datetime, timezone

    from lakebench.reports.generator import _rate_chart_svg

    t = datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert _rate_chart_svg([0.0, 0.0], [0.0, 0.0], 5.0, t, t) == ""
    svg = _rate_chart_svg([5e6, 0.0], [0.0, 1e6], 5.0, t, t)
    assert svg.startswith("<svg") and "read" in svg and "written" in svg


def test_datagen_window_is_recorded_and_time_to_value_does_not_count_it() -> None:
    """A run that records when datagen ran places the stage on the timeline;
    time to value is still first pipeline stage start to last stage end."""
    from datetime import datetime, timedelta, timezone

    from lakebench.metrics.collector import (
        JobMetrics,
        PipelineMetrics,
        build_pipeline_benchmark,
    )

    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run = PipelineMetrics(run_id="r", deployment_name="d", start_time=t0, end_time=t0)
    run.jobs = [
        JobMetrics(
            job_name="b",
            job_type="bronze-verify",
            start_time=t0 + timedelta(seconds=100),
            end_time=t0 + timedelta(seconds=160),
            elapsed_seconds=60.0,
            success=True,
        )
    ]
    bare = build_pipeline_benchmark(run, datagen_elapsed=90.0, datagen_output_rows=10)
    timed = build_pipeline_benchmark(
        run,
        datagen_elapsed=90.0,
        datagen_output_rows=10,
        datagen_start=t0,
        datagen_end=t0 + timedelta(seconds=90),
    )
    dg = next(s for s in timed.stages if s.stage_name == "datagen")
    assert dg.to_dict()["start_time"] == t0.isoformat()
    assert dg.to_dict()["end_time"] == (t0 + timedelta(seconds=90)).isoformat()
    assert timed.time_to_value_seconds == bare.time_to_value_seconds == 60.0
