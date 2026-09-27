"""Result fingerprints (benchmark.fingerprint) and the engine counting fixes.

A fingerprint must be equal exactly when two engines returned the same rows,
whatever each engine's output format. Both failure directions are silent: a
false match lets a comparison of different work through, a false mismatch
refuses a valid one.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest import mock

import pytest

from lakebench.benchmark import fingerprint as fpm
from lakebench.benchmark.fingerprint import (
    Unsupported,
    canonical_cell,
    fingerprint_rows,
    mismatch,
    rows_from_beeline_tsv2,
    rows_from_trino_json,
)

# One result, as each engine hands it back on the fingerprint path.
# Columns: date, count, decimal revenue, double average, timestamptz, nullable
# string, boolean.
TRINO_JSON = (
    '{"d":"2024-01-01","n":455,"rev":123.40,"avg":2.5,"ts":"2024-01-01 10:00:00.000000 UTC",'
    '"s":null,"b":true}\n'
    '{"d":"2024-01-02","n":7,"rev":1.2345E7,"avg":0.1,"ts":"2024-01-02 00:00:00.000000 UTC",'
    '"s":"web","b":false}\n'
)
BEELINE_TSV2 = (
    "d\tn\trev\tavg\tts\ts\tb\n"
    "2024-01-02\t7\t12345000.00\t0.1\t2024-01-02 00:00:00\tweb\tfalse\n"
    "2024-01-01\t455\t123.4000\t2.5\t2024-01-01 10:00:00\tNULL\ttrue\n"
)
DUCKDB_ROWS = [
    # DuckDB casts timestamptz to text in the pod (no pytz); its rendering
    # carries the session offset.
    (date(2024, 1, 1), 455, Decimal("123.40"), 2.5, "2024-01-01 10:00:00+00", None, True),
    (date(2024, 1, 2), 7, Decimal("12345000.00"), 0.1, "2024-01-02 00:00:00+00", "web", False),
]


def _fp(rows, approx=None):
    return fingerprint_rows(rows, approx)


class TestCrossEngineEquality:
    def test_the_same_result_fingerprints_equally_from_all_three_engines(self):
        t = _fp(rows_from_trino_json(TRINO_JSON))
        s = _fp(rows_from_beeline_tsv2(BEELINE_TSV2))
        d = _fp(DUCKDB_ROWS)
        assert t["rows"] == s["rows"] == d["rows"] == 2
        assert t["cols"] == s["cols"] == d["cols"] == 7
        assert mismatch(t, s) is None
        assert mismatch(t, d) is None

    def test_a_changed_value_changes_the_fingerprint(self):
        changed = BEELINE_TSV2.replace("\t455\t", "\t454\t")
        assert mismatch(
            _fp(rows_from_trino_json(TRINO_JSON)), _fp(rows_from_beeline_tsv2(changed))
        ) == ("exact differs")

    def test_row_order_does_not_matter_but_duplicates_do(self):
        a = _fp(DUCKDB_ROWS)
        assert mismatch(a, _fp(list(reversed(DUCKDB_ROWS)))) is None
        doubled = _fp(DUCKDB_ROWS + [DUCKDB_ROWS[0]])
        assert mismatch(a, doubled) == "rows differs"
        # XOR would cancel a pair; the sum keeps it.
        pair = _fp([DUCKDB_ROWS[0], DUCKDB_ROWS[0]])
        assert pair["exact"] != _fp([])["exact"]

    def test_a_missing_row_differs(self):
        assert mismatch(_fp(DUCKDB_ROWS), _fp(DUCKDB_ROWS[:1])) == "rows differs"

    @pytest.mark.parametrize(
        "a,b",
        [
            ("2263.0", "2263"),
            ("123.40", "123.4000"),
            ("1.2345E7", "12345000"),
            (Decimal("0.10"), 0.1),
            ("-0.0", "0"),
            # Java's non-shortest Double.toString: 17 digits, same double.
            ("0.30000000000000004", 0.30000000000000004),
        ],
    )
    def test_numeric_renderings_of_one_value_agree(self, a, b):
        assert canonical_cell(a) == canonical_cell(b)

    def test_integers_compare_exactly(self):
        assert canonical_cell("123456789012345") != canonical_cell("123456789012346")

    @pytest.mark.parametrize(
        "text",
        [
            "2024-01-01 10:00:00.000000 UTC",
            "2024-01-01 10:00:00",
            "2024-01-01 10:00:00+00",
            "2024-01-01T10:00:00Z",
            "2024-01-01 12:00:00+02:00",
            datetime(2024, 1, 1, 10, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 5, tzinfo=timezone(timedelta(hours=-5))),
        ],
    )
    def test_timestamp_renderings_normalise_to_utc(self, text):
        assert canonical_cell(text) == "2024-01-01T10:00:00.000000Z"

    def test_null_and_empty_string_differ(self):
        assert canonical_cell(None) != canonical_cell("")
        assert rows_from_beeline_tsv2("s\nNULL\n\n")[0] == [None]


class TestApproximateColumns:
    def test_double_sum_noise_within_the_quantum_matches(self):
        a = _fp([("web", 1000.004), ("app", 2.0)], {1: 0.01})
        b = _fp([("app", 2.0), ("web", 1000.0)], {1: 0.01})
        assert mismatch(a, b) is None

    def test_a_real_difference_in_an_approximate_column_differs(self):
        a = _fp([("web", 1000.0)], {1: 0.01})
        b = _fp([("web", 1003.0)], {1: 0.01})
        assert "column 1 sums differ" in mismatch(a, b)

    def test_nullness_of_an_approximate_column_is_exact(self):
        a = _fp([("web", None)], {1: 0.01})
        b = _fp([("web", 0.0)], {1: 0.01})
        assert mismatch(a, b) == "exact differs"

    def test_the_exact_columns_still_decide(self):
        a = _fp([("web", 1.0)], {1: 0.01})
        b = _fp([("app", 1.0)], {1: 0.01})
        assert mismatch(a, b) == "exact differs"


class TestUnsupported:
    def test_arrays_are_not_guessed(self):
        with pytest.raises(Unsupported):
            _fp([(1, [1, 2])])

    def test_a_tab_inside_a_thrift_string_is_detected(self):
        with pytest.raises(Unsupported):
            rows_from_beeline_tsv2("a\tb\nx\ty\tz\n")

    def test_an_unusable_fingerprint_matches_nothing(self):
        bad = fpm.unusable("error", "boom")
        assert mismatch(bad, bad) is not None
        assert mismatch(None, _fp([])) is not None


class TestDuckDBInPodPath:
    """The DuckDB executor ships this module's source into the pod and
    fingerprints there; run that exact script against a local DuckDB."""

    def test_in_pod_script_matches_the_host_fingerprint(self):
        pytest.importorskip("duckdb")
        from lakebench.modules.query_engines.duckdb.executor import (
            DuckDBExecutor,
            fingerprint_from_payload,
            fingerprint_statement,
        )

        sql = (
            "SELECT * FROM (VALUES (DATE '2024-01-01', 455, 123.40::DECIMAL(18,2), 2.5::DOUBLE, "
            "TIMESTAMPTZ '2024-01-01 10:00:00+00', NULL::VARCHAR, true), "
            "(DATE '2024-01-02', 7, 12345000.00::DECIMAL(18,2), 0.1::DOUBLE, "
            "TIMESTAMPTZ '2024-01-02 00:00:00+00', 'web', false)) t(d, n, rev, avg, ts, s, b)"
        )
        script = DuckDBExecutor("ns", "lakehouse", s3_endpoint="http://x:80")._build_python_script(
            sql, timeout=60, result_statement=fingerprint_statement(None)
        )
        # Drop the S3 and extension setup (no object store here); keep the
        # session settings, the query, the fetch and the fingerprint.
        tail = script.split("conn.execute('SET enable_progress_bar", 1)[1]
        code = (
            "import sys; sys.modules['pytz'] = None; import duckdb, json; "
            "conn = duckdb.connect(); conn.execute('SET enable_progress_bar" + tail
        )
        # A host zone other than UTC must not leak into the rendering.
        env = {"TZ": "America/Denver", "PATH": "/usr/bin:/bin"}
        proc = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, env=env, check=False
        )
        assert proc.returncode == 0, proc.stderr
        fp, error = fingerprint_from_payload(proc.stdout, sql)
        assert error is None
        assert mismatch(fp, _fp(rows_from_trino_json(TRINO_JSON))) is None
        assert fp["adapted_sql_sha"]


# ---------------------------------------------------------------------------
# Counting fixes (divergence report items 1 and 2)
# ---------------------------------------------------------------------------


def _completed(stdout: str, rc: int = 0, stderr: str = ""):
    return subprocess.CompletedProcess(args=[], returncode=rc, stdout=stdout, stderr=stderr)


class TestSparkThriftCounting:
    def test_beeline_options_come_before_e(self):
        """After -e they were read as more -e statements and dropped: beeline
        fell back to table format and the count read n + 3 x ceil(n/100)."""
        from lakebench.modules.query_engines.spark_thrift.executor import beeline_argv

        argv = beeline_argv("SELECT 1")
        e = argv.index("-e")
        assert argv[e + 1] == "SELECT 1" and len(argv) == e + 2
        for opt in ("--silent=true", "--outputformat=tsv2", "--nullemptystring=false"):
            assert argv.index(opt) < e

    def test_executor_sends_that_argv(self):
        from lakebench.modules.query_engines.spark_thrift.executor import SparkThriftExecutor

        ex = SparkThriftExecutor("ns", "lakehouse")
        ex._pod = "thrift-0"
        with mock.patch("subprocess.run", return_value=_completed("a\n1\n")) as run:
            ex.execute_query("SELECT 1")
        cmd = run.call_args[0][0]
        assert cmd.index("--outputformat=tsv2") < cmd.index("-e")

    def test_row_count_is_lines_after_the_header(self):
        from lakebench.modules.query_engines.spark_thrift.executor import SparkThriftExecutor

        ex = SparkThriftExecutor("ns", "lakehouse")
        ex._pod = "thrift-0"
        body = "id\n" + "\n".join(str(i) for i in range(250)) + "\n"
        with mock.patch("subprocess.run", return_value=_completed(body)):
            assert ex.execute_query("q").rows_returned == 250
        with mock.patch("subprocess.run", return_value=_completed("id\n")):
            # A header alone is an empty result, not one row.
            assert ex.execute_query("q").rows_returned == 0

    def test_maintenance_beeline_calls_put_options_first(self):
        from lakebench.modules.table_formats.iceberg import maintenance as m

        k8s = mock.Mock()
        k8s.exec_in_pod.return_value = (0, "", "")
        m.exec_sql("spark-thrift", k8s, "pod", "ns", "SELECT 1")
        m.query_sql("spark-thrift", k8s, "pod", "ns", "SELECT 1")
        for call in k8s.exec_in_pod.call_args_list:
            argv = call[0][1]
            assert argv[-2:] == ["-e", "SELECT 1"]
            assert argv.index("--silent=true") < argv.index("-e")


class TestDuckDBCounting:
    def _ex(self):
        from lakebench.modules.query_engines.duckdb.executor import DuckDBExecutor

        ex = DuckDBExecutor("ns", "lakehouse", s3_endpoint="http://x:80")
        ex._pod = "duckdb-0"
        return ex

    def test_progress_bar_before_the_payload_does_not_become_the_count(self):
        out = '\r100% ▕███▏\n{"rows": 455, "data": []}\n'
        with mock.patch("subprocess.run", return_value=_completed(out)):
            r = self._ex().execute_query("q")
        assert r.success and r.rows_returned == 455

    def test_an_unreadable_payload_is_an_error_not_a_line_count(self):
        with mock.patch("subprocess.run", return_value=_completed("garbage\nmore\n")):
            r = self._ex().execute_query("q")
        assert not r.success
        assert r.rows_returned == 0

    def test_script_turns_the_progress_bar_off_and_pins_utc(self):
        script = self._ex()._build_python_script("SELECT 1")
        assert "enable_progress_bar = false" in script
        assert "TimeZone = 'UTC'" in script
        assert script.index("enable_progress_bar") < script.index("conn.sql(")


class TestTrinoSession:
    def test_every_trino_call_pins_utc(self):
        from lakebench.modules.query_engines.trino.executor import TrinoExecutor

        ex = TrinoExecutor("ns", "lakehouse")
        ex._pod = "coord-0"
        with mock.patch("subprocess.run", return_value=_completed('"1"\n')) as run:
            ex.execute_query("SELECT 1")
        cmd = run.call_args[0][0]
        assert cmd[cmd.index("--timezone") + 1] == "UTC"

    def test_fingerprint_path_reads_json_and_the_timed_path_does_not(self):
        from lakebench.modules.query_engines.trino.executor import TrinoExecutor

        ex = TrinoExecutor("ns", "lakehouse")
        ex._pod = "coord-0"
        with mock.patch("subprocess.run", return_value=_completed(TRINO_JSON)) as run:
            r = ex.fingerprint_query("SELECT 1")
        cmd = run.call_args[0][0]
        assert cmd[cmd.index("--output-format") + 1] == "JSON"
        assert r.fingerprint["rows"] == 2 and fpm.usable(r.fingerprint)
        with mock.patch("subprocess.run", return_value=_completed('"1"\n')) as run:
            ex.execute_query("SELECT 1")
        assert "--output-format" not in run.call_args[0][0]

    def test_thrift_session_zone_is_pinned_in_the_template(self):
        from pathlib import Path

        import lakebench

        tpl = (
            Path(lakebench.__file__).parent / "templates/spark-thrift/sparkapplication.yaml.j2"
        ).read_text()
        assert "spark.sql.session.timeZone=UTC" in tpl


class TestRunnerFingerprintPass:
    """The fingerprint execution is untimed and follows the timed samples."""

    def _runner(self, executor):
        from lakebench.benchmark.runner import BenchmarkRunner

        with mock.patch("lakebench.benchmark.executor.get_executor", return_value=executor):
            return BenchmarkRunner(make_cfg())

    def _executor(self, calls, with_fp=True):
        from lakebench.benchmark.result import QueryExecutorResult

        class Ex:
            catalog_name = "lakehouse"

            def engine_name(self):
                return "trino"

            def adapt_query(self, sql):
                return sql

            def flush_cache(self):
                pass

            def execute_query(self, sql, timeout=300):
                calls.append("timed")
                return QueryExecutorResult(sql, "trino", 2.0, 1, "x")

        if with_fp:

            def fingerprint_query(self, sql, timeout=300, approx_columns=None):
                calls.append("fingerprint")
                return QueryExecutorResult(
                    sql, "trino", 99.0, 1, "", fingerprint=fingerprint_rows([(1,)])
                )

            Ex.fingerprint_query = fingerprint_query
        return Ex()

    def test_fingerprints_come_after_every_timed_sample_and_are_not_timed(self):
        calls: list[str] = []
        runner = self._runner(self._executor(calls))
        result = runner.run_power(iterations=2)
        n = len(result.queries)
        assert calls[: 2 * n] == ["timed"] * (2 * n)
        assert calls[2 * n :] == ["fingerprint"] * n
        assert all(q.elapsed_seconds == 2.0 for q in result.queries)
        assert all(q.to_dict()["result_fingerprint"]["rows"] == 1 for q in result.queries)

    def test_fingerprint_can_be_turned_off(self):
        calls: list[str] = []
        result = self._runner(self._executor(calls)).run_power(fingerprint=False)
        assert "fingerprint" not in calls
        assert all(q.result_fingerprint is None for q in result.queries)

    def test_an_engine_without_a_fingerprint_path_is_recorded_unusable(self):
        calls: list[str] = []
        result = self._runner(self._executor(calls, with_fp=False)).run_power()
        fp = result.queries[0].result_fingerprint
        assert fp["unsupported"] and not fpm.usable(fp)


def make_cfg():
    from tests.conftest import make_config

    return make_config()


class TestQueryTiebreakers:
    """Each LIMIT or ROW_NUMBER over tied keys ends in a total order
    (divergence report section 2); the texts are pinned so a revert fails."""

    @pytest.mark.parametrize(
        "name,fragment",
        [
            ("Q4_churn_risk_analysis", "at_risk_customers DESC, churn_risk_indicator"),
            ("FQ2_top_corridors_window", "volume_usd DESC, originator_bank_bic"),
            ("FQ3_entity_edge_risk", "total_out_usd DESC, e.entity_id"),
            ("FQ4_running_balance_window", "ORDER BY COUNT(*) DESC, account_id"),
            ("FQ4_running_balance_window", "ORDER BY s.book_ts, s.entry_seq"),
            ("FQ6_structuring_scan", "txn_count DESC, originator_id, txn_currency"),
            ("FQ7_cross_border_concentration", "xborder_usd DESC, originator_bank_bic"),
            ("FQ8_alert_to_entity_join", "ORDER BY alert_ts DESC, entity_id, rule_id, alert_id"),
            ("FQ8_alert_to_entity_join", "ORDER BY a.alert_ts DESC, a.alert_id"),
            ("IQ3_counterparty_two_hop", "h2.hop2_entity_id, h2.via_entity_id"),
        ],
    )
    def test_tiebreak_present(self, name, fragment):
        from lakebench.benchmark.queries import BENCHMARK_QUERIES_BY_DOMAIN

        sql = {q.name: q.sql for qs in BENCHMARK_QUERIES_BY_DOMAIN.values() for q in qs}[name]
        assert fragment in " ".join(sql.split())
