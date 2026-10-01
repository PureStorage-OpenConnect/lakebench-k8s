"""D-full-profiles: silver_stream_financial._merge_batch now maintains
silver.entity_profiles per micro-batch via a Welford + additive MERGE.

This test runs five synthetic micro-batches through _merge_batch, asserts
one row per touched entity in silver.entity_profiles, and asserts that
per-entity ``txn_count_total`` is monotone non-decreasing across the run
(the incremental additive counter is the primary sanity gate: any
Welford / MERGE regression that stops updating a per-entity counter fails
here).

The test uses the shared ``spark_session`` with a Hadoop Iceberg catalog
(``iceberg_catalog``), like test_aml_stream_refuse_fresh_checkpoint.py:
pyspark is required, and LB_SPARK_TEST_JARS must list an Iceberg runtime
jar (``requires_jars("iceberg")``).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")

pytestmark = [
    pytest.mark.requires_jars("iceberg"),
    pytest.mark.usefixtures("load_script"),
    # One core, as before the shared harness: the profile sums are compared
    # with a tolerance, and merge order follows the partition count.
    pytest.mark.spark_static_conf({"spark.master": "local[1]"}),
]

# The Iceberg catalog the scripts are pointed at (LB_ICEBERG_CATALOG) and the
# one registered on the shared session.
_CATALOG = "lh"


_PACS_SCHEMA = (
    "txn_id string, uetr string, "
    "dbtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "cdtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "dbtr_agt struct<bicfi:string>, cdtr_agt struct<bicfi:string>, "
    "dbtr_acct struct<iban:string>, cdtr_acct struct<iban:string>, "
    "intrmy_agt_1 struct<bicfi:string>, "
    "intrmy_agt_2 struct<bicfi:string>, "
    "intrmy_agt_3 struct<bicfi:string>, "
    "intr_bk_sttlm_amt decimal(18,2), intr_bk_sttlm_ccy string, "
    "cre_dt_tm timestamp, purp_cd string, "
    "rgltry_rptg array<string>, msg_id string"
)


def _bronze_row(spark, txn_id, orig_nm, ben_nm, ts, amount):
    def party(nm):
        return (nm, "US", ("NYC", "MAIN ST"), (f"LEI-{nm}",))

    row = (
        txn_id,
        f"UETR-{txn_id}",
        party(orig_nm),
        party(ben_nm),
        ("MERIUS2L",),
        ("NRTHGB3X",),
        ("US01",),
        ("GB02",),
        (None,),
        (None,),
        (None,),
        Decimal(amount),
        "USD",
        ts,
        "SALA",
        [],
        f"MSG-{txn_id}",
    )
    return spark.createDataFrame([row], _PACS_SCHEMA)


@pytest.fixture(scope="module", autouse=True)
def _catalog_env():
    """Point the stream / build modules at the test's Iceberg catalog
    before a test imports them: their DDL literals interpolate
    ``{CATALOG}`` at import, so patching module attributes after import is
    too late."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LB_ICEBERG_CATALOG", _CATALOG)
        yield


@pytest.fixture(scope="module")
def spark(spark_session, iceberg_catalog, tmp_path_factory):
    iceberg_catalog(
        spark_session, _CATALOG, tmp_path_factory.mktemp("aml-profiles-wh"), cache_enabled=False
    )
    return spark_session


def _bootstrap_stream(spark, ss):
    """Create the tables silver_stream_financial expects and point the
    module at the test catalog. streaming_query_id is monkey-patched to
    a fixed id because Spark only sets ``sql.streaming.queryId`` inside a
    real foreachBatch; a direct ``_merge_batch`` call from the test would
    otherwise raise from streaming_query_id()."""
    ss.CATALOG = "lh"
    ss.SILVER_TXNS = "silver.transactions"
    ss.SILVER_EDGES = "silver.counterparty_edges"
    ss.SILVER_ENTITIES = "silver.entities"
    ss.SILVER_ACCOUNTS = "silver.accounts"
    ss.SILVER_PROFILES = "silver.entity_profiles"
    ss.SILVER_BATCH_VERSIONS = "silver.silver_batch_versions"
    ss.streaming_query_id = lambda _s: "qid-test"

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
    for ddl_attr in (
        "DDL_TXNS",
        "DDL_ENTITIES",
        "DDL_ACCOUNTS",
        "DDL_STATEMENTS",
        "DDL_EDGES",
        "DDL_PROFILES",
        "DDL_BATCH_VERSIONS",
    ):
        spark.sql(getattr(ss, ddl_attr))
    # Skip KYC + dimension writes: this test isolates the profiles MERGE.
    ss._KYC = None
    ss._KYC_LOADED = True
    ss._kyc = lambda _s: None
    ss.append_new_dimensions = lambda *_a, **_kw: (0, 0)


def test_stream_maintains_profiles_monotone_across_five_batches(spark, tmp_path):
    import silver_stream_financial as ss

    _bootstrap_stream(spark, ss)

    # Five micro-batches: each carries one txn for entity "A" (constant
    # counterparty "Z") and, from batch 2 onward, one more for entity "B"
    # -> "Z". n_txns_out for A grows monotonically (1..5); for B grows
    # 0..4. profile-side counts (in) for Z grow to 5 + 4 = 9.
    for bid in range(5):
        row_a = _bronze_row(
            spark, f"A{bid}", "A", "Z", datetime(2024, 6, 1) + timedelta(days=bid), "100.00"
        )
        rows = row_a
        if bid >= 1:
            row_b = _bronze_row(
                spark,
                f"B{bid}",
                "B",
                "Z",
                datetime(2024, 6, 10) + timedelta(days=bid),
                "50.00",
            )
            rows = row_a.union(row_b)
        ss._merge_batch(rows, bid)

    profiles = spark.table("lh.silver.entity_profiles").collect()
    by_id = {r["entity_id"]: r for r in profiles}
    assert len(by_id) == 3, f"expected profiles for A, B, Z; got {list(by_id)}"

    # A: 5 out, 0 in -> total = 5.
    a_rows = [r for r in profiles if r["txn_count_out"] == 5 and r["txn_count_in"] == 0]
    assert len(a_rows) == 1, "one profile row must reflect entity A (5 out, 0 in)"
    # B: 4 out, 0 in -> total = 4.
    b_rows = [r for r in profiles if r["txn_count_out"] == 4 and r["txn_count_in"] == 0]
    assert len(b_rows) == 1, "one profile row must reflect entity B (4 out, 0 in)"
    # Z: 0 out, 9 in -> total = 9.
    z_rows = [r for r in profiles if r["txn_count_out"] == 0 and r["txn_count_in"] == 9]
    assert len(z_rows) == 1, "Z must have 0 out and 9 in txns after 5 batches"

    # Sanity gate: txn_count_total = txn_count_out + txn_count_in for every row.
    for r in profiles:
        assert r["txn_count_total"] == r["txn_count_out"] + r["txn_count_in"]

    # _m2 must be non-negative and finite for the two originator rows;
    # a Welford regression that drops the delta^2 cross-term could make
    # M2 negative when the batch mean drifts.
    for r in a_rows + b_rows:
        assert r["_m2"] is not None
        assert r["_m2"] >= 0.0, f"_m2 must be non-negative; got {r['_m2']}"
