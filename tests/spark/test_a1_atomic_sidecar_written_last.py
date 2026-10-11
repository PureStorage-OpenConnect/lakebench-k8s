"""A1-atomic: the I10 sealed_batch_versions marker is written LAST.

A silver_build_financial run must not seal a cycle unless every silver
table for that cycle has been written and its row-count assertion has
passed. Any SilverAbort must leave the versions table untouched for that
cycle, so a downstream reader filtered by the sealed_txns semi-join sees
zero rows.

Two abort surfaces:

1. Pre-flight abort (empty entities): SilverAbort fires before any
   ``_replace_data`` call.
2. In-sequence abort (empty statements after write): SilverAbort fires
   after silver.transactions/entities/accounts/account_statements have
   been written but before edges/profiles; the cleanup truncates
   silver.transactions.

In both, ``MERGE INTO ... silver_batch_versions`` is absent from the SQL
Spark executes for the cycle.

Requires pyspark (skipped locally when absent). No Iceberg backend is
needed: DDL, ALTER TABLE and MERGE INTO are intercepted, and the write
side effect (``_replace_data``) is a spy.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.usefixtures("load_script")


# Positional tuple + explicit DDL: a bare Row(**kwargs) with rgltry_rptg=[]
# has no inferable element type. ccy is on the accounts because
# silver_build_financial reads dbtr_acct.ccy / cdtr_acct.ccy.
_BRONZE_SCHEMA = (
    "txn_id string, uetr string, "
    "dbtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "cdtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "dbtr_agt struct<bicfi:string>, cdtr_agt struct<bicfi:string>, "
    "dbtr_acct struct<iban:string, ccy:string>, "
    "cdtr_acct struct<iban:string, ccy:string>, "
    "intrmy_agt_1 struct<bicfi:string>, "
    "intrmy_agt_2 struct<bicfi:string>, "
    "intrmy_agt_3 struct<bicfi:string>, "
    "intr_bk_sttlm_amt decimal(18,2), intr_bk_sttlm_ccy string, "
    "cre_dt_tm timestamp, purp_cd string, "
    "rgltry_rptg array<string>, msg_id string"
)

_ALL_SILVER = (
    "SILVER_TRANSACTIONS",
    "SILVER_ENTITIES",
    "SILVER_ACCOUNTS",
    "SILVER_STATEMENTS",
    "SILVER_EDGES",
    "SILVER_PROFILES",
)


def _bronze_df(spark):
    from decimal import Decimal

    row = (
        "T1",
        "U1",
        ("ACME LTD", "US", ("NYC", "MAIN ST"), ("L-ACME",)),
        ("BETA INC", "GB", ("LON", "OXFORD ST"), ("L-BETA",)),
        ("MERIUS2L",),
        ("NRTHGB3X",),
        ("US01", "USD"),
        ("GB02", "GBP"),
        (None,),
        (None,),
        (None,),
        Decimal("100.00"),
        "USD",
        None,
        "SALA",
        [],
        "MSG-1",
    )
    return spark.createDataFrame([row], _BRONZE_SCHEMA)


def _install_mocks(monkeypatch, spark):
    """Intercept _replace_data, spark.sql/table and the Iceberg DDL helpers."""
    import silver_build_financial as sbf

    bronze = _bronze_df(spark)
    replace_calls: list[tuple[str, int]] = []
    sql_calls: list[str] = []

    def _fake_replace_data(_spark, df, table):
        replace_calls.append((table, int(df.count())))

    monkeypatch.setattr(sbf, "_replace_data", _fake_replace_data)
    monkeypatch.setattr(sbf, "refuse_batch_while_stream_active", lambda **_kw: None)
    monkeypatch.setattr(sbf, "ensure_namespaces_for_ddl", lambda *_a, **_kw: None)
    monkeypatch.setattr(sbf, "ensure_column", lambda *_a, **_kw: None)
    monkeypatch.setattr(sbf, "ensure_partition_transform", lambda *_a, **_kw: None)
    monkeypatch.setattr(sbf, "_read_reference", lambda _s: (None, None))
    monkeypatch.setattr(sbf, "log_job_metrics", lambda *_a, **_kw: None)

    empty_df = spark.range(0)
    # build_statements reads silver.accounts back and selects account_id + iban.
    empty_accounts = spark.createDataFrame(
        [], "account_id bigint, iban string, current_balance decimal(18,2)"
    )

    def _fake_table(name):
        if str(name).endswith(sbf.BRONZE_TABLE):
            return bronze
        if str(name).endswith(sbf.SILVER_ACCOUNTS):
            return empty_accounts
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


def _stub_empty_entities(monkeypatch, sbf, spark):
    def _empty_entities(*_a, **_kw):
        return spark.range(0).selectExpr(
            "CAST(id AS BIGINT) AS entity_id",
            "CAST(NULL AS STRING) AS name",
            "CAST(NULL AS STRING) AS entity_type",
        )

    monkeypatch.setattr(sbf, "build_entities", _empty_entities)


def _stub_empty_statements(monkeypatch, sbf):
    # Zero rows for silver.account_statements only, so no other gate trips first.
    def _stats(_spark, table):
        return (0, 0.0) if str(table).endswith(sbf.SILVER_STATEMENTS) else (1, 0.0)

    monkeypatch.setattr(sbf, "iceberg_table_stats", _stats)


@pytest.mark.parametrize(
    "abort_at, must_write, never_write, truncates_transactions",
    [
        pytest.param("entities", (), _ALL_SILVER, False, id="empty-entities-preflight"),
        pytest.param(
            "statements",
            ("SILVER_TRANSACTIONS", "SILVER_STATEMENTS"),
            ("SILVER_EDGES", "SILVER_PROFILES"),
            True,
            id="empty-statements-in-sequence",
        ),
    ],
)
def test_aborted_cycle_writes_no_sealed_marker(
    monkeypatch, spark, abort_at, must_write, never_write, truncates_transactions
):
    from common import SilverAbort

    sbf, replace_calls, sql_calls = _install_mocks(monkeypatch, spark)
    if abort_at == "entities":
        _stub_empty_entities(monkeypatch, sbf, spark)
    else:
        _stub_empty_statements(monkeypatch, sbf)

    with pytest.raises(SilverAbort):
        sbf.main()

    written = [t for (t, _) in replace_calls]
    for name in must_write:
        assert getattr(sbf, name) in written, f"{name} should have been written; saw {written}"
    for name in never_write:
        assert getattr(sbf, name) not in written, (
            f"{name} must NOT be written after the abort; saw {written}"
        )
    if not must_write:
        assert replace_calls == [], (
            f"pre-flight must raise before any write; _replace_data saw {replace_calls}"
        )
    truncates = [n for (t, n) in replace_calls if t == sbf.SILVER_TRANSACTIONS and n == 0]
    assert bool(truncates) == truncates_transactions, (
        f"silver.transactions truncate expected={truncates_transactions}; saw {replace_calls}"
    )

    merges = [
        s for s in sql_calls if "merge into" in s.lower() and "silver_batch_versions" in s.lower()
    ]
    assert merges == [], (
        f"silver_batch_versions sealed marker must NOT be emitted after an abort; saw: {merges}"
    )
