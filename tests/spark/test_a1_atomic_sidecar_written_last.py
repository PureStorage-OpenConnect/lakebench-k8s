"""A1-atomic: the I10 sealed_batch_versions marker is written LAST.

A silver_build_financial run must not seal a cycle unless every silver
table for that cycle has been written and its row-count assertion has
passed. The sealed ('batch', cycle) MERGE INTO silver_batch_versions
happens after silver.entity_profiles has been written and asserted; any
mid-chain SilverAbort must leave the versions table untouched for that
cycle, so a downstream reader filtered by the sealed_txns semi-join
sees zero rows.

Two mid-chain failure surfaces need coverage:

1. Pre-flight abort (empty entities): SilverAbort fires before any
   ``_replace_data`` call. The sealed MERGE is never emitted.
2. In-sequence abort (empty statements after write): SilverAbort fires
   after silver.transactions/entities/accounts have been overwritten
   but before edges/profiles. The in-sequence assertion re-raises via
   the finally-cleanup path that truncates silver.transactions; the
   sealed MERGE is never emitted, and downstream readers filtered by
   the sealed_txns semi-join see zero rows for the crashed cycle.

Both paths converge on the same evidence: ``MERGE INTO ... silver_batch_versions``
is absent from the SQL statements Spark ever executes for the cycle.

Requires pyspark (skipped locally when absent). No Iceberg backend is
needed: DDL, ALTER TABLE and MERGE INTO are intercepted, and the write
side effect (``_replace_data``) is a spy.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("lb-a1-atomic-sidecar-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    yield s
    s.stop()


def _bronze_df(spark):
    from decimal import Decimal

    from pyspark.sql import Row

    row = Row(
        txn_id="T1",
        uetr="U1",
        dbtr=Row(
            nm="ACME LTD",
            ctry_of_res="US",
            pstl_adr=Row(twn_nm="NYC", strt_nm="MAIN ST"),
            id=Row(lei="L-ACME"),
        ),
        cdtr=Row(
            nm="BETA INC",
            ctry_of_res="GB",
            pstl_adr=Row(twn_nm="LON", strt_nm="OXFORD ST"),
            id=Row(lei="L-BETA"),
        ),
        dbtr_agt=Row(bicfi="MERIUS2L"),
        cdtr_agt=Row(bicfi="NRTHGB3X"),
        dbtr_acct=Row(iban="US01", ccy="USD"),
        cdtr_acct=Row(iban="GB02", ccy="GBP"),
        intrmy_agt_1=Row(bicfi=None),
        intrmy_agt_2=Row(bicfi=None),
        intrmy_agt_3=Row(bicfi=None),
        intr_bk_sttlm_amt=Decimal("100.00"),
        intr_bk_sttlm_ccy="USD",
        cre_dt_tm=None,
        purp_cd="SALA",
        rgltry_rptg=[],
        msg_id="MSG-1",
    )
    return spark.createDataFrame([row])


def _install_common_mocks(monkeypatch, spark):
    """Base wiring: intercepts _replace_data + spark.sql/table + Iceberg DDL."""
    import silver_build_financial as sbf

    bronze = _bronze_df(spark)
    replace_calls: list[tuple[str, int]] = []
    sql_calls: list[str] = []

    def _fake_replace_data(_spark, df, table):
        rows = df.count()
        replace_calls.append((table, int(rows)))

    monkeypatch.setattr(sbf, "_replace_data", _fake_replace_data)
    monkeypatch.setattr(sbf, "refuse_batch_while_stream_active", lambda **_kw: None)
    monkeypatch.setattr(sbf, "ensure_namespaces_for_ddl", lambda *_a, **_kw: None)
    monkeypatch.setattr(sbf, "ensure_column", lambda *_a, **_kw: None)
    monkeypatch.setattr(sbf, "ensure_partition_transform", lambda *_a, **_kw: None)
    monkeypatch.setattr(sbf, "_read_reference", lambda _s: (None, None))
    monkeypatch.setattr(sbf, "log_job_metrics", lambda *_a, **_kw: None)

    empty_df = spark.range(0)

    def _fake_table(name):
        if str(name).endswith(sbf.BRONZE_TABLE):
            return bronze
        return empty_df

    monkeypatch.setattr(spark, "table", _fake_table)

    def _fake_sql(stmt, *a, **kw):
        sql_calls.append(str(stmt))

        class _R:
            def collect(self):
                return []

        return _R()

    monkeypatch.setattr(spark, "sql", _fake_sql)

    class _Builder:
        def appName(self, *_a, **_kw):
            return self

        def getOrCreate(self):
            return spark

    monkeypatch.setattr(sbf.SparkSession, "builder", _Builder())

    return sbf, replace_calls, sql_calls


def test_sidecar_absent_after_preflight_abort(monkeypatch, spark):
    """Empty entities -> SilverAbort in pre-flight -> no sealed MERGE."""
    from common import SilverAbort

    sbf, replace_calls, sql_calls = _install_common_mocks(monkeypatch, spark)

    def _empty_entities(*_a, **_kw):
        return spark.range(0)

    monkeypatch.setattr(sbf, "build_entities", _empty_entities)
    monkeypatch.setattr(sbf, "iceberg_table_stats", lambda _s, _t: (0, 0.0))

    with pytest.raises(SilverAbort):
        sbf.main()

    # No write happened, so no sealed marker was emitted.
    assert replace_calls == [], (
        f"pre-flight abort must precede every _replace_data call; saw {replace_calls}"
    )
    _assert_no_sealed_marker(sql_calls)


def test_sidecar_absent_after_in_sequence_abort(monkeypatch, spark):
    """In-sequence failure post-statements-write leaves no sealed marker.

    build_entities returns >= 1 row so pre-flight passes and the writes
    start. iceberg_table_stats returns 0 rows for silver.account_statements,
    which trips the in-sequence assertion. The finally-cleanup truncates
    silver.transactions and re-raises. The sealed MERGE is NOT emitted,
    and downstream readers filtered by the sealed_txns semi-join see zero
    rows for the aborted cycle.
    """
    from common import SilverAbort

    sbf, replace_calls, sql_calls = _install_common_mocks(monkeypatch, spark)

    # iceberg_table_stats returns 0 for silver.account_statements. Any other
    # table returns a passing count so we do not spuriously trip a different
    # gate before the one under test.
    def _stats(_spark, table):
        if str(table).endswith(sbf.SILVER_STATEMENTS):
            return (0, 0.0)
        return (1, 0.0)

    monkeypatch.setattr(sbf, "iceberg_table_stats", _stats)

    with pytest.raises(SilverAbort) as excinfo:
        sbf.main()

    msg = str(excinfo.value)
    assert "silver.account_statements" in msg
    assert "A1-atomic" in msg or "F2" in msg

    # Pre-flight passed: silver.transactions, silver.entities and
    # silver.accounts writes happened, followed by silver.account_statements.
    # The in-sequence assertion trips right after that write; the cleanup
    # then truncates silver.transactions. So we expect to see:
    # - transactions, entities, accounts, account_statements (in some order)
    # - a second silver.transactions write with 0 rows (the truncate)
    # We DO NOT expect edges, profiles, or the sealed MERGE.
    written = [t for (t, _) in replace_calls]
    assert sbf.SILVER_TRANSACTIONS in written
    assert sbf.SILVER_STATEMENTS in written
    assert sbf.SILVER_EDGES not in written, (
        "edges must NOT be written after an in-sequence abort; saw {written}"
    )
    assert sbf.SILVER_PROFILES not in written, (
        "profiles must NOT be written after an in-sequence abort; saw {written}"
    )
    # The truncate cleanup wrote silver.transactions with 0 rows.
    truncate_writes = [n for (t, n) in replace_calls if t == sbf.SILVER_TRANSACTIONS and n == 0]
    assert truncate_writes, (
        f"cleanup after in-sequence abort must truncate silver.transactions; "
        f"saw writes {replace_calls}"
    )
    _assert_no_sealed_marker(sql_calls)


def _assert_no_sealed_marker(sql_calls):
    """The sealed ('batch', cycle) MERGE must be absent from the SQL log.

    Down-stream readers filtered by the sealed_txns semi-join see zero rows
    for the cycle when the marker is absent -- which is exactly the
    observability contract the I10 sidecar established and A1-atomic
    preserves for the batch mode.
    """
    merges = [s for s in sql_calls if "MERGE INTO" in s and "silver_batch_versions" in s.lower()]
    assert merges == [], (
        f"silver_batch_versions sealed marker must NOT be emitted when a "
        f"mid-chain SilverAbort fires; saw: {merges}"
    )
