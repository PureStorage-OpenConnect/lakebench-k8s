"""silver_stream_financial._merge_batch maintains silver.entity_profiles per
micro-batch via a Welford + additive MERGE.

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
        "DDL_PAIRS",
    ):
        spark.sql(getattr(ss, ddl_attr))
    # Skip KYC + dimension writes: this test isolates the profiles MERGE.
    ss._KYC = None
    ss._KYC_LOADED = True
    ss._kyc = lambda _s: None
    ss.append_new_dimensions = lambda *_a, **_kw: (0, 0)


def test_stream_distinct_counterparties_equal_a_full_count(spark):
    """distinct_counterparties_out/in are maintained from new pairs only
    (silver.counterparty_pairs), not a recount of every sealed transaction.
    They must still equal count_distinct over all of silver.transactions:
    with pairs repeated across batches, a batch applied twice (a replay),
    and an unsealed pairs row left by another stream, which must not hide
    its pair."""
    import silver_stream_financial as ss
    from pyspark.sql.functions import countDistinct

    for t in ("transactions", "entity_profiles", "counterparty_pairs", "silver_batch_versions"):
        spark.sql(f"DROP TABLE IF EXISTS lh.silver.{t}")
    _bootstrap_stream(spark, ss)
    ss.SILVER_PAIRS = "silver.counterparty_pairs"

    def batch(bid, pairs):
        rows = None
        for k, (o, b) in enumerate(pairs):
            r = _bronze_row(
                spark, f"T{bid}-{k}", o, b, datetime(2024, 6, 1) + timedelta(days=bid), "10.00"
            )
            rows = r if rows is None else rows.union(r)
        return rows

    batches = [
        [("A", "Z"), ("A", "Y")],
        [("A", "Z"), ("B", "Z")],  # A->Z repeats batch 0
        [("A", "X"), ("B", "Z"), ("Z", "A")],
        [("A", "W"), ("A", "Y"), ("B", "Y")],  # A->W first sealed here
    ]
    ss._merge_batch(batch(0, batches[0]), 0)
    ss._merge_batch(batch(1, batches[1]), 1)
    ss._merge_batch(batch(2, batches[2]), 2)
    ss._merge_batch(batch(2, batches[2]), 2)  # replay of batch 2
    # A crashed attempt of another stream left B->Y (first sealed in batch
    # 3) unsealed in the pairs table: it must not hide the pair.
    ids = {
        r["txn_id"]: (r["originator_id"], r["beneficiary_id"])
        for r in spark.table("lh.silver.transactions").collect()
    }
    b_id, y_id = ids["T1-1"][0], ids["T0-1"][1]
    spark.sql(f"INSERT INTO lh.silver.counterparty_pairs VALUES ({b_id}, {y_id}, 'qid-other', 99)")
    ss._merge_batch(batch(3, batches[3]), 3)

    txns = spark.table("lh.silver.transactions")
    want_out = {
        r[0]: r[1]
        for r in txns.groupBy("originator_id").agg(countDistinct("beneficiary_id")).collect()
    }
    want_in = {
        r[0]: r[1]
        for r in txns.groupBy("beneficiary_id").agg(countDistinct("originator_id")).collect()
    }
    got = {r["entity_id"]: r for r in spark.table("lh.silver.entity_profiles").collect()}
    assert set(got) == set(want_out) | set(want_in)
    for e, r in got.items():
        assert r["distinct_counterparties_out"] == want_out.get(e, 0), (e, "out")
        assert r["distinct_counterparties_in"] == want_in.get(e, 0), (e, "in")
    # A: Z, Y, X, W; B: Z, Y (Y despite the stale row).
    assert sorted(want_out.values(), reverse=True)[:2] == [4, 2]
