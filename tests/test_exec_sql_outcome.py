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
