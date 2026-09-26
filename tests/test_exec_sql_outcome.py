"""exec_sql must report the real outcome of a statement (review of b01854c).

``K8sClient.exec_in_pod`` never raises: a failed Trino CLI or beeline call,
a kubectl error and a timeout all come back as ``rc != 0``. ``exec_sql``
used to drop that tuple, so every failed maintenance, compaction or DROP read
as success. These tests pin the fixed contract, the destroy classifier that
decides which failures mean "table was never there", and what each caller
outside destroy does with a real failure.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from lakebench.deploy import destroy as destroy_mod
from lakebench.modules.table_formats.iceberg.maintenance import exec_sql

# -- exec_sql -----------------------------------------------------------------


class TestExecSql:
    def test_success_returns_quietly(self):
        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (0, "", "")
        exec_sql("trino", k8s, "trino-0", "ns", "SELECT 1")
        assert k8s.exec_in_pod.call_args.kwargs["timeout"] == 30

    def test_trino_failure_raises_with_stdout_and_stderr(self):
        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (
            1,
            "",
            "Query 20260926_101010_00001_abcde failed: line 1:13: "
            "Table 'lakehouse.gold.t' does not exist",
        )
        with pytest.raises(RuntimeError) as ei:
            exec_sql("trino", k8s, "trino-0", "ns", "ALTER TABLE lakehouse.gold.t EXECUTE x")
        assert "rc=1" in str(ei.value)
        assert "does not exist" in str(ei.value)

    def test_beeline_failure_raises(self):
        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (2, "Error: [TABLE_OR_VIEW_NOT_FOUND] ...", "")
        with pytest.raises(RuntimeError) as ei:
            exec_sql("spark-thrift", k8s, "thrift-0", "ns", "CALL x", timeout=600)
        assert "TABLE_OR_VIEW_NOT_FOUND" in str(ei.value)
        assert k8s.exec_in_pod.call_args.kwargs["container"] == "spark-thrift"
        assert k8s.exec_in_pod.call_args.kwargs["timeout"] == 600

    def test_timeout_raises(self):
        from lakebench.modules.table_formats.iceberg.maintenance import ExecSqlTimeout

        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (1, "", "Command timed out")
        with pytest.raises(ExecSqlTimeout, match="may still be running"):
            exec_sql("trino", k8s, "trino-0", "ns", "ALTER TABLE t EXECUTE optimize")

    def test_unknown_engine_raises(self):
        with pytest.raises(ValueError):
            exec_sql("duckdb", MagicMock(), "p", "ns", "SELECT 1")


# -- destroy's classifier ------------------------------------------------------

TRINO_TABLE_MISSING = (
    "Query 20260926_101010_00001_abcde failed: line 1:13: "
    "Table 'lakehouse.gold.customer_executive_dashboard' does not exist"
)
TRINO_SCHEMA_MISSING = (
    "Query 20260926_101010_00002_abcde failed: line 1:13: Schema 'gold' does not exist"
)
TRINO_CATALOG_MISSING = (
    "Query 20260926_101010_00003_abcde failed: line 1:13: Catalog 'lakehouse' not found"
)
TRINO_CATALOG_MISSING_OLD = (
    "Query 20260926_101010_00004_abcde failed: line 1:13: Catalog 'lakehouse' does not exist"
)
# The echoed statement mentions a table and "does not exist" appears later on
# a different, unrelated error: free-text matching would call this "missing".
ECHOED_SQL_THEN_CATALOG = (
    "ALTER TABLE lakehouse.gold.t EXECUTE remove_orphan_files -- table cleanup\n"
    "Query 20260926_101010_00005_abcde failed: Catalog 'lakehouse' does not exist"
)
# Same line: the statement text sits next to a catalog error.
SAME_LINE_SQL_THEN_CATALOG = (
    "Query 20260926_101010_00008_abcde failed: ALTER TABLE lakehouse.gold.t EXECUTE "
    "expire_snapshots: Catalog 'lakehouse' does not exist"
)
TRINO_PROCEDURE_MISSING = (
    "Query 20260926_101010_00006_abcde failed: line 1:1: "
    "Table procedure not registered: remove_orphan_files"
)
TRINO_METASTORE_DOWN = (
    "Query 20260926_101010_00007_abcde failed: Failed connecting to Hive metastore: "
    "[lakebench-hive-metastore.ns.svc.cluster.local:9083]"
)
BEELINE_TABLE_MISSING = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "[TABLE_OR_VIEW_NOT_FOUND] The table or view `lakehouse`.`gold`.`t` cannot be found. "
    "Verify the spelling and correctness of the schema and catalog. (state=42P01,code=0)"
)
BEELINE_SCHEMA_MISSING = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "[SCHEMA_NOT_FOUND] The schema `lakehouse`.`gold` cannot be found. (state=42704,code=0)"
)
BEELINE_CATALOG_MISSING = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "[CATALOG_NOT_FOUND] The catalog `lakehouse` not found. (state=42P08,code=0)"
)
BEELINE_PROCEDURE_MISSING = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "java.lang.IllegalArgumentException: Cannot find procedure: system.remove_orphan_filez"
)
BEELINE_METASTORE_DOWN = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "org.apache.thrift.transport.TTransportException: java.net.ConnectException: "
    "Connection refused (state=08S01,code=0)"
)
TIMEOUT = "exec_sql failed (rc=1): Command timed out"
# Confirmed live on 2026-09-26 (Trino 483, Spark 4.0.2 / Iceberg 1.10 Thrift).
LIVE_TRINO_TABLE_MISSING = (
    "Query 20260926_093512_00042_x7k2p failed: line 1:7: Table 'lakehouse.exp.nope' does not exist"
)
LIVE_SPARK_PROCEDURE_TABLE_MISSING = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "java.lang.IllegalArgumentException: Couldn't load table 'exp.nope' in catalog 'lakehouse'"
)
LIVE_SPARK_DML_TABLE_MISSING = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "[TABLE_OR_VIEW_NOT_FOUND] The table or view `lakehouse`.`exp`.`nope` cannot be found."
)
LIVE_TRINO_MIN_RETENTION = (
    "Query 20260926_093601_00043_x7k2p failed: Retention specified (30.00m) is shorter than "
    "the minimum retention configured in the system (7.00d). Minimum retention can be changed "
    "with iceberg.expire_snapshots_min_retention configuration property or "
    "iceberg.expire_snapshots_min_retention session property"
)
LIVE_SPARK_ORPHAN_INTERVAL = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "java.lang.IllegalArgumentException: Cannot remove orphan files with an interval less "
    "than 24 hours. Executing this procedure with a short interval may corrupt the table if "
    "other operations are happening at the same time."
)
# Trino table procedures (ALTER TABLE ... EXECUTE, CALL system.vacuum) raise
# TableNotFoundException, whose message differs from the analyzer's.
TRINO_PROCEDURE_TABLE_MISSING = (
    "Query 20260926_101010_00009_abcde failed: Table 'gold.customer_executive_dashboard' not found"
)
TRINO_CATALOG_NOT_FOUND_BARE = (
    "Query 20260926_101010_00010_abcde failed: Catalog 'lakehouse' not found"
)
# Iceberg Spark procedures wrap NoSuchTableException.
BEELINE_ICEBERG_PROCEDURE_TABLE_MISSING = (
    "Error: org.apache.hive.service.cli.HiveSQLException: Error running query: "
    "java.lang.IllegalArgumentException: Couldn't load table 'gold.t' in catalog 'lakehouse' "
    "(state=,code=0)"
)


def _err(text: str) -> RuntimeError:
    return RuntimeError(f"exec_sql failed (rc=1): {text}")


@pytest.mark.parametrize(
    ("output", "table_missing", "schema_missing"),
    [
        (TRINO_TABLE_MISSING, True, False),
        (TRINO_SCHEMA_MISSING, True, True),
        (BEELINE_TABLE_MISSING, True, False),
        (BEELINE_SCHEMA_MISSING, True, True),
        (TRINO_CATALOG_MISSING, False, False),
        (TRINO_CATALOG_MISSING_OLD, False, False),
        (ECHOED_SQL_THEN_CATALOG, False, False),
        (SAME_LINE_SQL_THEN_CATALOG, False, False),
        (TRINO_PROCEDURE_MISSING, False, False),
        (TRINO_METASTORE_DOWN, False, False),
        (BEELINE_CATALOG_MISSING, False, False),
        (BEELINE_PROCEDURE_MISSING, False, False),
        (BEELINE_METASTORE_DOWN, False, False),
        (TIMEOUT, False, False),
        (LIVE_TRINO_TABLE_MISSING, True, False),
        (LIVE_SPARK_PROCEDURE_TABLE_MISSING, True, False),
        (LIVE_SPARK_DML_TABLE_MISSING, True, False),
        (LIVE_TRINO_MIN_RETENTION, False, False),
        (LIVE_SPARK_ORPHAN_INTERVAL, False, False),
        (TRINO_PROCEDURE_TABLE_MISSING, True, False),
        (TRINO_CATALOG_NOT_FOUND_BARE, False, False),
        (BEELINE_ICEBERG_PROCEDURE_TABLE_MISSING, True, False),
    ],
)
def test_classifier(output, table_missing, schema_missing):
    e = _err(output)
    assert destroy_mod._is_table_missing(e) is table_missing
    assert destroy_mod._is_schema_missing(e) is schema_missing


# -- callers outside destroy: a failure is recorded, never fatal ---------------


def _cfg(engine_type="trino"):
    from lakebench.config.schema import TableNamesConfig

    cfg = MagicMock()
    cfg.get_namespace.return_value = "lakebench-test"
    cfg.architecture.query_engine.type.value = engine_type
    cfg.architecture.query_engine.trino.catalog_name = "lakehouse"
    cfg.architecture.table_format.type.value = "iceberg"
    cfg.architecture.tables = TableNamesConfig()
    cfg.architecture.workload.schema_type.value = "customer360"
    return cfg


def _journal_details(j, message):
    for call in j.record.call_args_list:
        if call.kwargs.get("message") == message:
            return call.kwargs["details"]
    raise AssertionError(f"no journal record {message!r}")


@pytest.fixture
def _engine_pod():
    with patch(
        "lakebench.deploy.iceberg.find_maintenance_engine",
        return_value=("trino", "trino-coordinator-0", "lakehouse"),
    ):
        yield


@pytest.mark.usefixtures("_engine_pod")
class TestSustainedCallers:
    def test_continuous_maintenance_records_real_failures(self):
        from lakebench.cli._sustained import _run_iceberg_maintenance

        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (1, "", TRINO_METASTORE_DOWN)
        j = MagicMock()
        _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m")
        d = _journal_details(j, "Iceberg maintenance")
        assert d["operations_total"] > 0
        assert d["operations_succeeded"] == 0
        assert d["operations_failed"] == d["operations_total"]
        assert "metastore" in d["failures"][0]

    def test_continuous_maintenance_success_counts(self):
        from lakebench.cli._sustained import _run_iceberg_maintenance

        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (0, "", "")
        j = MagicMock()
        _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m")
        d = _journal_details(j, "Iceberg maintenance")
        assert d["operations_failed"] == 0
        assert d["operations_succeeded"] == d["operations_total"]

    def test_pre_benchmark_compaction_records_real_failures(self):
        from lakebench.cli._sustained import _run_iceberg_compaction

        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (1, "", TRINO_METASTORE_DOWN)
        j = MagicMock()
        _run_iceberg_compaction(_cfg(), k8s, Console(quiet=True), j)
        d = _journal_details(j, "Iceberg compaction")
        assert d["operations_total"] == 2
        assert d["operations_succeeded"] == 0
        assert d["operations_failed"] == 2

    def test_continuous_maintenance_counts_timeouts_apart_from_failures(self):
        from lakebench.cli._sustained import _run_iceberg_maintenance

        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (1, "", "Command timed out")
        j = MagicMock()
        _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m")
        d = _journal_details(j, "Iceberg maintenance")
        assert d["operations_failed"] == 0
        assert d["operations_timed_out"] == d["operations_total"] > 0

    def test_compaction_passes_its_timeout_and_journals_elapsed(self):
        from lakebench.cli._sustained import _run_iceberg_compaction

        k8s = MagicMock()
        k8s.exec_in_pod.return_value = (0, "", "")
        j = MagicMock()
        _run_iceberg_compaction(_cfg(), k8s, Console(quiet=True), j, timeout=1800)
        assert {c.kwargs["timeout"] for c in k8s.exec_in_pod.call_args_list} == {1800}
        d = _journal_details(j, "Iceberg compaction")
        assert "elapsed_seconds" in d and d["statement_timeout_seconds"] == 1800


def test_pre_benchmark_compaction_waits_for_completion():
    """The benchmark must not start while rewrite_data_files still runs."""
    import inspect

    import lakebench.cli._run as run_mod

    src = inspect.getsource(run_mod)
    assert "timeout=PRE_BENCHMARK_COMPACTION_TIMEOUT" in src
    assert run_mod.PRE_BENCHMARK_COMPACTION_TIMEOUT >= 1800


def test_delta_vacuum_reaches_trino_in_one_execute():
    """SET SESSION and CALL vacuum must share one `trino --execute` process."""
    from lakebench.deploy.delta_maintenance import build_delta_maintenance_sql

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    for sql in build_delta_maintenance_sql("trino", "lakehouse", "lakehouse.gold.t", 0.0):
        exec_sql("trino", k8s, "trino-0", "ns", sql)
    assert k8s.exec_in_pod.call_count == 1
    cmd = k8s.exec_in_pod.call_args.args[1]
    assert cmd[:2] == ["trino", "--execute"]
    assert cmd[2].index("SET SESSION") < cmd[2].index("CALL lakehouse.system.vacuum")


def test_pre_benchmark_maintenance_waits_for_completion():
    """expire_snapshots / orphan removal must not overlap the benchmark."""
    import inspect

    import lakebench.cli._run as run_mod

    src = inspect.getsource(run_mod)
    assert "timeout=PRE_BENCHMARK_MAINTENANCE_TIMEOUT" in src
    assert run_mod.PRE_BENCHMARK_MAINTENANCE_TIMEOUT >= 1800


@pytest.mark.usefixtures("_engine_pod")
def test_maintenance_passes_its_timeout_and_journals_elapsed():
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m", timeout=1800)
    assert {c.kwargs["timeout"] for c in k8s.exec_in_pod.call_args_list} == {1800}
    d = _journal_details(j, "Iceberg maintenance")
    assert "elapsed_seconds" in d and d["statement_timeout_seconds"] == 1800


# -- brief review of 9a940b7+59d7e40 ------------------------------------------


@pytest.mark.usefixtures("_engine_pod")
def test_pre_benchmark_budget_stops_at_first_timeout_across_both_steps():
    """Financial: 51 statements x 1800 s must not become a 25 h wait."""
    from lakebench.cli._sustained import (
        MaintenanceBudget,
        _run_iceberg_compaction,
        _run_iceberg_maintenance,
    )

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (1, "", "Command timed out")
    j = MagicMock()
    budget = MaintenanceBudget(1800)
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m", budget=budget)
    _run_iceberg_compaction(_cfg(), k8s, Console(quiet=True), j, budget=budget)
    assert k8s.exec_in_pod.call_count == 1, "the first timeout stops everything"
    assert "timed out" in budget.stopped
    m = _journal_details(j, "Iceberg maintenance")
    c = _journal_details(j, "Iceberg compaction")
    assert m["operations_timed_out"] == 1
    assert m["operations_not_attempted"] == m["operations_total"] - 1
    assert c["operations_not_attempted"] == c["operations_total"] == 2
    assert c["not_attempted"] and c["stopped"]


@pytest.mark.usefixtures("_engine_pod")
def test_pre_benchmark_budget_has_an_overall_cap(monkeypatch):
    from lakebench.cli._sustained import MaintenanceBudget, _run_iceberg_maintenance

    clock = {"t": 0.0}
    budget = MaintenanceBudget(1800)
    budget._clock = lambda: clock["t"]
    budget.deadline = 1800

    k8s = MagicMock()

    def slow(*_a, **_kw):
        clock["t"] += 1000  # each statement takes 1000 s
        return (0, "", "")

    k8s.exec_in_pod.side_effect = slow
    j = MagicMock()
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m", budget=budget)
    d = _journal_details(j, "Iceberg maintenance")
    assert k8s.exec_in_pod.call_count == 2
    assert d["operations_not_attempted"] == d["operations_total"] - 2
    assert "cap" in budget.stopped


def test_run_shares_one_budget_between_maintenance_and_compaction():
    import inspect

    import lakebench.cli._run as run_mod

    src = inspect.getsource(run_mod)
    assert "maint_budget = MaintenanceBudget(PRE_BENCHMARK_MAINTENANCE_CAP)" in src
    assert src.count("budget=maint_budget") == 2
    assert run_mod.PRE_BENCHMARK_MAINTENANCE_CAP <= 1800


def _delta_cfg():
    cfg = _cfg()
    cfg.architecture.table_format.type.value = "delta"
    return cfg


@pytest.mark.usefixtures("_engine_pod")
def test_continuous_delta_vacuum_keeps_default_retention():
    """Policy: no retention override while streams are live."""
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    _run_iceberg_maintenance(_delta_cfg(), k8s, Console(quiet=True), j, "30m", live_streams=True)
    sent = [c.args[1][2] for c in k8s.exec_in_pod.call_args_list]
    assert sent and all("vacuum_min_retention" not in q for q in sent)
    assert all("168.0h" in q for q in sent)
    _journal_details(j, "Delta VACUUM at default 7d retention (live streams)")


@pytest.mark.usefixtures("_engine_pod")
def test_batch_delta_vacuum_still_honours_short_retention():
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    _run_iceberg_maintenance(_delta_cfg(), k8s, Console(quiet=True), MagicMock(), "30m")
    sent = [c.args[1][2] for c in k8s.exec_in_pod.call_args_list]
    assert sent and all("vacuum_min_retention" in q for q in sent)


def test_continuous_loop_passes_live_streams_to_maintenance():
    from tests.test_continuous_maintenance_timeout import loop_call_keywords

    assert loop_call_keywords("_run_iceberg_maintenance")["live_streams"] == "True"


@pytest.mark.usefixtures("_engine_pod")
def test_statement_timeout_is_clipped_to_the_budget():
    """Together maintenance and compaction cannot exceed the cap plus grace."""
    from lakebench.cli._sustained import (
        _BUDGET_GRACE_SECONDS,
        MaintenanceBudget,
        _run_iceberg_maintenance,
    )

    clock = {"t": 0.0}
    budget = MaintenanceBudget(1800)
    budget._clock = lambda: clock["t"]
    budget.deadline = 1800
    k8s = MagicMock()
    timeouts: list[int] = []

    def run(*_a, **kw):
        timeouts.append(kw["timeout"])
        clock["t"] += 1500  # the first statement uses most of the budget
        return (0, "", "")

    k8s.exec_in_pod.side_effect = run
    _run_iceberg_maintenance(
        _cfg(), k8s, Console(quiet=True), MagicMock(), "30m", timeout=1800, budget=budget
    )
    assert timeouts[0] == 1800
    assert timeouts[1] == 300 + _BUDGET_GRACE_SECONDS


# -- live evidence 2026-09-26: maintenance SQL that actually runs -------------


def test_trino_maintenance_sets_the_session_minimum_in_the_same_submission():
    from lakebench.deploy.iceberg import build_maintenance_sql

    exp, orph = build_maintenance_sql("trino", "lakehouse", "lakehouse.silver.t", "30m")
    assert exp == (
        "SET SESSION lakehouse.expire_snapshots_min_retention = '30m'; "
        "ALTER TABLE lakehouse.silver.t EXECUTE expire_snapshots(retention_threshold => '30m')"
    )
    # Orphans never below 24 h + 10 min, even when asked for less.
    assert orph == (
        "SET SESSION lakehouse.remove_orphan_files_min_retention = '1450m'; "
        "ALTER TABLE lakehouse.silver.t EXECUTE remove_orphan_files(retention_threshold => '1450m')"
    )
    # The catalog comes from config, not a hard-coded name.
    exp2, orph2 = build_maintenance_sql("trino", "iceberg_cat", "iceberg_cat.s.t", "1h", "48h")
    assert exp2.startswith("SET SESSION iceberg_cat.expire_snapshots_min_retention = '1h'; ")
    assert "remove_orphan_files_min_retention = '48h'" in orph2


def test_trino_maintenance_reaches_one_execute_each():
    """SET SESSION and the ALTER travel in one `trino --execute`. Verified live
    2026-09-26: `trino --execute "SET SESSION
    lakehouse.expire_snapshots_min_retention = '0s'; ALTER TABLE ... EXECUTE
    expire_snapshots(...)"` succeeded as one submission, and the same for
    remove_orphan_files."""
    from lakebench.deploy.iceberg import build_maintenance_sql

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    for sql in build_maintenance_sql("trino", "lakehouse", "lakehouse.silver.t", "30m"):
        exec_sql("trino", k8s, "trino-0", "ns", sql)
    assert k8s.exec_in_pod.call_count == 2
    for call in k8s.exec_in_pod.call_args_list:
        cmd = call.args[1][2]
        assert cmd.index("SET SESSION") < cmd.index("ALTER TABLE")


def test_spark_maintenance_uses_a_timestamp_literal():
    from datetime import datetime, timezone

    from lakebench.deploy.iceberg import build_maintenance_sql

    now = datetime(2026, 9, 26, 12, 0, 0, tzinfo=timezone.utc)
    exp, orph = build_maintenance_sql(
        "spark-thrift", "lakehouse", "lakehouse.silver.t", "30m", "24h", now=now
    )
    # An explicit offset, so the Thrift session time zone cannot shift it.
    assert exp == (
        "CALL lakehouse.system.expire_snapshots(table => 'lakehouse.silver.t', "
        "older_than => TIMESTAMP '2026-09-26 11:30:00+00:00')"
    )
    # 24 h asked, 24 h 10 min enforced.
    assert "older_than => TIMESTAMP '2026-09-25 11:50:00+00:00'" in orph
    assert "UNIX_TIMESTAMP" not in exp + orph


def _sent(k8s):
    return [
        c.args[1][-2] if c.args[1][0] != "trino" else c.args[1][2]
        for c in k8s.exec_in_pod.call_args_list
    ]


@pytest.mark.usefixtures("_engine_pod")
def test_continuous_floors_expire_at_1h_and_orphans_at_24h10m():
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "30m", live_streams=True)
    sent = _sent(k8s)
    assert any("expire_snapshots(retention_threshold => '1h')" in q for q in sent)
    assert any("remove_orphan_files(retention_threshold => '1450m')" in q for q in sent)
    d = _journal_details(j, "Iceberg maintenance")
    assert d["expire_retention"] == "1h" and d["orphan_retention"] == "1450m"


@pytest.mark.usefixtures("_engine_pod")
def test_batch_trino_orphans_are_floored_too():
    """Stream apps may be writing even in batch (restartPolicy Always)."""
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    _run_iceberg_maintenance(_cfg(), k8s, Console(quiet=True), j, "0s")
    sent = _sent(k8s)
    assert any("expire_snapshots(retention_threshold => '0s')" in q for q in sent)
    assert all("'0s'" not in q for q in sent if "remove_orphan_files" in q)
    d = _journal_details(j, "Iceberg maintenance")
    assert d["expire_retention"] == "0s" and d["orphan_retention"] == "1450m"


def test_batch_spark_orphans_never_below_24h10m():
    from lakebench.cli._sustained import _run_iceberg_maintenance

    k8s = MagicMock()
    k8s.exec_in_pod.return_value = (0, "", "")
    j = MagicMock()
    with patch(
        "lakebench.deploy.iceberg.find_maintenance_engine",
        return_value=("spark-thrift", "thrift-0", "lakehouse"),
    ):
        _run_iceberg_maintenance(_cfg("spark-thrift"), k8s, Console(quiet=True), j, "30m")
    d = _journal_details(j, "Iceberg maintenance")
    assert d["expire_retention"] == "30m" and d["orphan_retention"] == "1450m"


def test_continuous_loop_passes_live_streams():
    from tests.test_continuous_maintenance_timeout import loop_call_keywords

    assert loop_call_keywords("_run_iceberg_compaction")["live_streams"] == "True"


def test_spark_timestamp_is_utc_whatever_the_input_zone():
    from datetime import datetime, timedelta, timezone

    from lakebench.modules.table_formats.iceberg.maintenance import _spark_timestamp

    cest = timezone(timedelta(hours=2))
    now = datetime(2026, 9, 26, 14, 0, 0, tzinfo=cest)  # 12:00 UTC
    assert _spark_timestamp(1800, now) == "TIMESTAMP '2026-09-26 11:30:00+00:00'"


# -- data-safety review: parser, live-stream detection, loop resilience -------


@pytest.mark.parametrize("bad", ["7D ", "30", "1.5h", "30min", "", "m", "-1h", "1w"])
def test_parser_refuses_anything_unvalidated(bad):
    from lakebench.modules.table_formats.iceberg.maintenance import _parse_threshold_seconds

    if bad == "7D ":
        assert _parse_threshold_seconds(bad) == 7 * 86400  # case-insensitive, never 7 min
        return
    with pytest.raises(ValueError):
        _parse_threshold_seconds(bad)


@pytest.mark.parametrize(
    ("given", "stored"), [("30m", "30m"), (" 7D ", "7d"), ("1 H", "1h"), ("0s", "0s")]
)
def test_config_normalises_valid_thresholds(given, stored):
    from lakebench.config.schema import SustainedConfig

    assert SustainedConfig(retention_threshold=given).retention_threshold == stored


@pytest.mark.parametrize("bad", ["30", "1.5h", "30min", "7days", "1w", ""])
def test_config_rejects_bad_thresholds(bad):
    from pydantic import ValidationError

    from lakebench.config.schema import SustainedConfig

    with pytest.raises(ValidationError, match="whole number and one unit"):
        SustainedConfig(retention_threshold=bad)


def test_live_stream_apps_detects_present_apps_and_fails_safe():
    from kubernetes.client.rest import ApiException

    from lakebench.cli._sustained import _STREAM_APPS, _live_stream_apps

    timeouts = []

    def get(_g, _v, _ns, _p, name, _request_timeout=None):
        timeouts.append(_request_timeout)
        if name == "lakebench-silver-stream":
            return {"metadata": {"name": name}}
        if name == "lakebench-gold-refresh":
            raise ApiException(status=503)
        raise ApiException(status=404)

    with patch("kubernetes.client.CustomObjectsApi") as api:
        api.return_value.get_namespaced_custom_object.side_effect = get
        live, errors = _live_stream_apps("ns")
    assert live == ["lakebench-silver-stream", "lakebench-gold-refresh"]
    assert errors == ["lakebench-gold-refresh: HTTP 503"]
    assert set(live) <= set(_STREAM_APPS)
    assert timeouts and all(t == 10 for t in timeouts), "every read is bounded"


def test_live_stream_probe_timeout_counts_as_live():
    from urllib3.exceptions import ReadTimeoutError

    from lakebench.cli._sustained import _STREAM_APPS, _live_stream_apps

    def slow(*_a, **_kw):
        raise ReadTimeoutError(None, "/apis", "Read timed out. (read timeout=10)")

    with patch("kubernetes.client.CustomObjectsApi") as api:
        api.return_value.get_namespaced_custom_object.side_effect = slow
        live, errors = _live_stream_apps("ns")
    assert live == list(_STREAM_APPS)
    assert all("ReadTimeoutError" in e for e in errors)


def test_pre_benchmark_maintenance_uses_live_settings_when_streams_exist():
    import inspect

    import lakebench.cli._run as run_mod

    src = inspect.getsource(run_mod)
    assert "live_apps, live_errors = _live_stream_apps(cfg.get_namespace())" in src
    assert '"read_errors": live_errors' in src
    assert src.count("live_streams=bool(live_apps)") == 2
    assert "Pre-benchmark maintenance with live streams" in src


def test_continuous_loop_survives_a_maintenance_error():
    import ast
    import inspect

    import lakebench.cli._sustained as sus

    src = inspect.getsource(sus._run_sustained)
    guarded = [
        t
        for t in ast.walk(ast.parse(src))
        if isinstance(t, ast.Try)
        and any(
            isinstance(n, ast.Call) and getattr(n.func, "id", "") == "_run_iceberg_maintenance"
            for stmt in t.body
            for n in ast.walk(stmt)
        )
    ]
    assert any(
        ast.unparse(h.type) == "Exception" and "Maintenance round failed" in ast.unparse(h)
        for t in guarded
        for h in t.handlers
        if h.type is not None
    )


def test_run_records_live_streams_for_the_scorecard():
    import inspect

    import lakebench.cli._run as run_mod

    src = inspect.getsource(run_mod)
    assert "live_streams_reason=maint_live_reason" in src
    assert "pb.maintenance_live_streams = True" in src


def test_live_streams_nulls_the_maintenance_value():
    from lakebench.cli._run import _maintenance_value

    value, n, reason = _maintenance_value(
        [], [], 66, 61, 180.0, None, live_streams_reason="stream apps present: x"
    )
    assert value is None and "streams were live" in reason


def test_finished_leftover_stream_apps_are_not_live():
    from lakebench.cli._sustained import _live_stream_apps

    states = {
        "lakebench-bronze-ingest": "COMPLETED",
        "lakebench-silver-stream": "FAILED",
        "lakebench-gold-refresh": "RUNNING",
    }

    def get(_g, _v, _ns, _p, name, _request_timeout=None):
        return {"status": {"applicationState": {"state": states[name]}}}

    with patch("kubernetes.client.CustomObjectsApi") as api:
        api.return_value.get_namespaced_custom_object.side_effect = get
        live, errors = _live_stream_apps("ns")
    assert live == ["lakebench-gold-refresh"] and errors == []
