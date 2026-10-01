"""D-full-profiles: batch and stream must produce equivalent
silver.entity_profiles for the same bronze.

Batch mode computes the aggregates in one pass:
    stddev = sample stddev over originator amounts
    _m2 = variance * (n - 1)
Stream mode maintains the same aggregates incrementally via the Welford
parallel merge. The two paths must converge byte-identically on additive,
LEAST/GREATEST and derived columns; ``stddev_amount_usd`` and ``_m2`` are
allowed a small floating-point tolerance (Welford accumulates rounding
error linearly in the number of merges; a strict byte-identical assertion
would reject a numerically-correct implementation).

Any regression that lets batch and stream diverge -- for example, a
Welford implementation that drops the ``delta^2 * n_a * n_b / n``
cross-term, or a MERGE UPDATE SET that references the post-update t.x
instead of the pre-update value -- fails at least one of the exact-match
assertions below.
"""

from __future__ import annotations

import glob
import math
import os
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

# Point the stream / build modules at the test's Iceberg catalog before
# import so their `{CATALOG}` f-string interpolation resolves to "lh".
os.environ.setdefault("LB_ICEBERG_CATALOG", "lh")

import pytest

pytest.importorskip("pyspark")

_JARS = os.environ.get("LB_SPARK_TEST_JARS", "")


def _have_jars() -> bool:
    if not _JARS or not Path(_JARS).is_dir():
        return False
    names = [p.name for p in Path(_JARS).glob("*.jar")]
    return any(n.startswith("iceberg-spark-runtime") for n in names)


pytestmark = [
    pytest.mark.skipif(not _have_jars(), reason="LB_SPARK_TEST_JARS with Iceberg jars not set"),
    pytest.mark.usefixtures("load_script"),
]


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


def _party(nm):
    return (nm, "US", ("NYC", "MAIN ST"), (f"LEI-{nm}",))


def _row(txn_id, orig, ben, ts, amount):
    return (
        txn_id,
        f"UETR-{txn_id}",
        _party(orig),
        _party(ben),
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


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    wh = tmp_path_factory.mktemp("aml-parity-wh")
    jars = ",".join(sorted(glob.glob(os.path.join(_JARS, "*.jar"))))
    s = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lh.type", "hadoop")
        .config("spark.sql.catalog.lh.cache-enabled", "false")
        .config("spark.sql.catalog.lh.warehouse", f"file://{wh}")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield s
    s.stop()


def test_batch_and_stream_produce_equivalent_profiles(spark):

    import silver_stream_financial as ss

    # Build a shared bronze corpus: 8 transactions from three originators
    # to two beneficiaries; amounts spread enough that stddev is non-zero.
    base = datetime(2024, 6, 1)
    corpus = [
        _row("T1", "A", "Z", base + timedelta(days=0), "100.00"),
        _row("T2", "A", "Z", base + timedelta(days=1), "150.00"),
        _row("T3", "A", "Y", base + timedelta(days=2), "200.00"),
        _row("T4", "B", "Z", base + timedelta(days=3), "50.00"),
        _row("T5", "B", "Y", base + timedelta(days=4), "75.00"),
        _row("T6", "B", "Z", base + timedelta(days=5), "300.00"),
        _row("T7", "C", "Z", base + timedelta(days=6), "125.00"),
        _row("T8", "C", "Y", base + timedelta(days=7), "175.00"),
    ]
    bronze = spark.createDataFrame(corpus, _PACS_SCHEMA)

    # --- Batch path: build_entity_profiles directly.
    from datetime import date

    import silver_build_financial as sbf

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver_batch")
    spark.sql(sbf.DDL_PROFILES.replace("silver.entity_profiles", "silver_batch.entity_profiles"))

    from pyspark.sql.functions import lit as _lit

    batch_txns = sbf.build_transactions(bronze)
    batch_profiles = sbf.build_entity_profiles(batch_txns, data_clock=date(2024, 7, 1))
    batch_profiles.writeTo("lh.silver_batch.entity_profiles").overwrite(_lit(True))

    # --- Stream path: feed the same corpus as one micro-batch through
    # _merge_batch (proxy for the streaming trigger; the MERGE code path
    # is identical). streaming_query_id is monkey-patched: Spark only
    # binds the query id inside a real foreachBatch and a direct call
    # would otherwise raise from streaming_query_id().
    ss.CATALOG = "lh"
    ss.SILVER_TXNS = "silver_stream.transactions"
    ss.SILVER_EDGES = "silver_stream.counterparty_edges"
    ss.SILVER_ENTITIES = "silver_stream.entities"
    ss.SILVER_ACCOUNTS = "silver_stream.accounts"
    ss.SILVER_PROFILES = "silver_stream.entity_profiles"
    ss.SILVER_BATCH_VERSIONS = "silver_stream.silver_batch_versions"
    ss.streaming_query_id = lambda _s: "qid-parity"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver_stream")
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
    ss._KYC = None
    ss._KYC_LOADED = True
    ss._kyc = lambda _s: None
    ss.append_new_dimensions = lambda *_a, **_kw: (0, 0)
    ss._merge_batch(bronze, 0)

    # --- Compare per-entity rows.
    batch_rows = {
        r["entity_id"]: r for r in spark.table("lh.silver_batch.entity_profiles").collect()
    }
    stream_rows = {
        r["entity_id"]: r for r in spark.table("lh.silver_stream.entity_profiles").collect()
    }
    assert set(batch_rows) == set(stream_rows), (
        f"entity coverage drift: batch={set(batch_rows)}, stream={set(stream_rows)}"
    )
    for eid, b in batch_rows.items():
        s = stream_rows[eid]
        # Additive + count columns must be exact.
        for c in (
            "txn_count_out",
            "txn_count_in",
            "txn_count_total",
            "distinct_counterparties_out",
            "distinct_counterparties_in",
        ):
            assert b[c] == s[c], f"{eid}.{c}: batch={b[c]}, stream={s[c]}"
        for c in ("total_sent_usd", "total_received_usd"):
            b_v = b[c]
            s_v = s[c]
            # Decimal(38,2) comparison is exact.
            assert b_v == s_v, f"{eid}.{c}: batch={b_v}, stream={s_v}"
        # LEAST / GREATEST timestamps must be exact.
        for c in ("first_seen_ts", "last_seen_ts"):
            assert b[c] == s[c], f"{eid}.{c}: batch={b[c]}, stream={s[c]}"
        for c in ("active_span_days",):
            # DATEDIFF returns integer -> DOUBLE; exact-eq is safe.
            assert b[c] == s[c] or (b[c] is None and s[c] is None), (
                f"{eid}.{c}: batch={b[c]}, stream={s[c]}"
            )
        # Welford accumulators: within a small relative tolerance because
        # incremental merges accrue rounding error compared with a single
        # pass. Corpus is small so absolute tolerance suffices too.
        b_m2 = float(b["_m2"] or 0.0)
        s_m2 = float(s["_m2"] or 0.0)
        assert math.isclose(b_m2, s_m2, rel_tol=1e-9, abs_tol=1e-9), (
            f"{eid}._m2 drifted: batch={b_m2}, stream={s_m2}"
        )
        if b["stddev_amount_usd"] is None:
            assert s["stddev_amount_usd"] is None, (
                f"{eid}.stddev: batch NULL vs stream {s['stddev_amount_usd']}"
            )
        else:
            assert math.isclose(
                float(b["stddev_amount_usd"]),
                float(s["stddev_amount_usd"]),
                rel_tol=1e-9,
                abs_tol=1e-9,
            ), (
                f"{eid}.stddev drifted: batch={b['stddev_amount_usd']}, "
                f"stream={s['stddev_amount_usd']}"
            )
        # avg_amount_usd is a mean; tolerance same as stddev.
        if b["avg_amount_usd"] is None:
            assert s["avg_amount_usd"] is None
        else:
            assert math.isclose(
                float(b["avg_amount_usd"]),
                float(s["avg_amount_usd"]),
                rel_tol=1e-9,
                abs_tol=1e-9,
            )
