"""A1-atomic: silver_build_financial.main() stages every bronze-derived
silver frame BEFORE it writes any silver table.

The contract the test locks in place:

* Every bronze-derived silver frame (silver.transactions,
  silver.entities, silver.accounts, silver.counterparty_edges,
  silver.entity_profiles) is built and row-counted BEFORE the first
  ``_replace_data`` call.
* If any pre-flight row-count assertion fails, ``SilverAbort`` is raised
  and NO ``_replace_data`` runs. The cycle leaves an untouched silver
  set rather than a partial one.
* The I10 sealed_batch_versions marker (the sealed ('batch', cycle)
  MERGE) is never emitted for a pre-flight-aborted cycle, so a
  downstream consumer filtered by the sealed_txns semi-join sees zero
  rows for that cycle even if silver.transactions had been left with
  data from an earlier successful cycle.

build_statements and update_accounts_balance CANNOT be pre-flighted
because they read durable silver tables. Their in-sequence assertion +
truncate-transactions cleanup is exercised by
``test_a1_atomic_sidecar_written_last.py``.

We patch build_entities to return an empty DataFrame regardless of the
bronze it is given -- the plan's canonical proxy for a bronze that
should never have progressed past pre-flight.

Requires pyspark (skipped locally when absent). The Spark session runs
in-process with no Iceberg backend; every DDL and _replace_data call is
intercepted, so the test does not need iceberg-spark-runtime jars.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

pytestmark = pytest.mark.usefixtures("load_script")


@pytest.fixture
def spark(tmp_path):
    """Local Spark session with no catalog, no Iceberg."""
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("lb-a1-atomic-preflight-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    yield s
    s.stop()


# Positional tuple + explicit DDL, the pattern every passing spark test in
# this dir uses (e.g. test_aml_batch_stream_profiles_parity.py). A bare
# Row(**kwargs) with rgltry_rptg=[] has no inferable element type, so
# createDataFrame raised CANNOT_DETERMINE_TYPE and these A1 tests were red
# in CI from the day the lane merged (3fb1259). The schema names the empty
# array as array<string>; ccy is on the accounts because
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


def _make_bronze_df(spark):
    """One-row bronze DataFrame with the columns build_transactions reads."""
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


def _install_mocks(monkeypatch, spark, *, entities_rows):
    """Wire the monkeypatches every A1-atomic test needs.

    Returns the spy list ``replace_calls`` and the intercepted SQL statements.
    """
    import silver_build_financial as sbf

    bronze = _make_bronze_df(spark)
    replace_calls: list[tuple[str, int]] = []
    sql_calls: list[str] = []

    def _fake_replace_data(_spark, df, table):
        rows = df.count()
        replace_calls.append((table, int(rows)))

    monkeypatch.setattr(sbf, "_replace_data", _fake_replace_data)

    # Guard: never touch a real deployment's stream marker.
    monkeypatch.setattr(sbf, "refuse_batch_while_stream_active", lambda **_kw: None)

    # DDL bootstrap + column-migration helpers are Iceberg-shaped; stub them.
    monkeypatch.setattr(sbf, "ensure_namespaces_for_ddl", lambda *_a, **_kw: None)
    monkeypatch.setattr(sbf, "ensure_column", lambda *_a, **_kw: None)
    monkeypatch.setattr(sbf, "ensure_partition_transform", lambda *_a, **_kw: None)

    # No KYC master; the WARNING branch is fine.
    monkeypatch.setattr(sbf, "_read_reference", lambda _s: (None, None))

    # bronze.count() succeeds. spark.table(bronze) returns the fixture; any
    # spark.table(silver.*) returns an empty DataFrame with a compatible
    # schema (only .count() and .limit(0) are ever called on it).
    empty_df = spark.range(0)

    def _fake_table(name):
        if str(name).endswith(sbf.BRONZE_TABLE):
            return bronze
        return empty_df

    monkeypatch.setattr(spark, "table", _fake_table)

    # DDL, ALTER TABLE, MERGE INTO -- record but do not execute.
    def _fake_sql(stmt, *a, **kw):
        sql_calls.append(str(stmt))

        class _R:
            def collect(self):
                return []

        return _R()

    monkeypatch.setattr(spark, "sql", _fake_sql)

    # iceberg_table_stats is only used post-write; stub with a plausible value
    # in case anything reaches it.
    monkeypatch.setattr(sbf, "iceberg_table_stats", lambda _s, _t: (0, 0.0))
    monkeypatch.setattr(sbf, "log_job_metrics", lambda *_a, **_kw: None)

    # Make main() reuse the shared Spark session; do not spawn a new one.
    class _Builder:
        def appName(self, *_a, **_kw):
            return self

        def getOrCreate(self):
            return spark

    monkeypatch.setattr(sbf.SparkSession, "builder", _Builder())

    # Override the specific build_* helper for this test to force an empty frame
    # for entities. The other build_* functions run normally against bronze.
    real_build_entities = sbf.build_entities

    def _empty_entities(*_a, **_kw):
        # Return an empty DataFrame with a plausible schema. Only .persist()
        # and .count() are called on this in the pre-flight; a real writeTo
        # would fail on any DataFrame, but the fake _replace_data does not
        # write.
        return spark.range(0).selectExpr(
            "CAST(id AS BIGINT) AS entity_id",
            "CAST(NULL AS STRING) AS name",
            "CAST(NULL AS STRING) AS entity_type",
        )

    monkeypatch.setattr(sbf, "build_entities", _empty_entities)
    if entities_rows > 0:
        # Convenience escape hatch: if a variant of this test asks for a
        # non-empty entities frame, keep the real builder. Not used by the
        # empty-entities test but kept so the helper is reusable.
        monkeypatch.setattr(sbf, "build_entities", real_build_entities)

    return sbf, replace_calls, sql_calls


def test_empty_entities_aborts_before_any_replace_data(monkeypatch, spark):
    """Pre-flight refuses to write once build_entities returns 0 rows."""
    from common import SilverAbort

    sbf, replace_calls, sql_calls = _install_mocks(monkeypatch, spark, entities_rows=0)

    with pytest.raises(SilverAbort) as excinfo:
        sbf.main()

    msg = str(excinfo.value)
    # SilverAbort names the failing silver frame -- that is how the
    # operator diagnoses which builder needs attention.
    assert "silver.entities" in msg
    # And carries the observed row count and the F2 grep-marker so the
    # log line documents the gate directly.
    assert "0 rows" in msg
    assert "expected >= 1" in msg
    assert "A1-atomic" in msg or "F2" in msg

    # No silver table was overwritten: the atomic contract holds. Even
    # silver.transactions (which under the pre-fix flow was written
    # before entities) is untouched.
    assert replace_calls == [], (
        f"pre-flight was supposed to raise before any write; _replace_data "
        f"was called with {replace_calls}"
    )

    # No sealed marker MERGE was emitted, so downstream readers filtered
    # by the sealed_txns semi-join see zero rows for this cycle.
    merge_calls = [s for s in sql_calls if "MERGE INTO" in s and "silver_batch_versions" in s]
    assert merge_calls == [], (
        f"silver_batch_versions marker must NOT be written when a pre-flight "
        f"assertion aborts the run; saw: {merge_calls}"
    )
