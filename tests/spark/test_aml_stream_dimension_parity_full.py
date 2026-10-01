"""E1: 5-batch stream produces the same silver.entities and silver.accounts
as a single batch build against the same bronze.

Batch mode runs ``silver_build_financial.build_entities`` /
``build_accounts`` over the full corpus and picks per-column ``min()``
across every row. Continuous mode, pre-E1, appended dimension rows the
FIRST batch introduced them in and never updated them; a scale-10
stream against identical bronze produced a different silver.entities
from batch. E1's MERGE with ``coalesce(least(t, s), t, s)`` on the
dimension columns collapses that gap.

Split the same bronze corpus into 5 chunks, run the stream MERGE 5
times, and assert row-hash equality against a single-shot batch build
on the MAINTAINED tables. D-safe skips ``silver.account_statements``,
``silver.accounts.current_balance`` and ``silver.entity_profiles`` in
continuous mode -- those are covered by the D-safe refusal test, not
here.

Runs in a Spark child (``spark_subprocess``) with the Iceberg jar from
``LB_SPARK_TEST_JARS`` and the Iceberg SQL extension on the JVM from launch.
"""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

pytest.importorskip("pyspark")


@pytest.mark.known_bug(
    "LB-193",
    match="No plan for TableReference",
    legs=("4.1",),
    reason="MERGE from a temp view over Iceberg: No plan for TableReference",
)
@pytest.mark.requires_jars("iceberg")
def test_batch_and_stream_dimensions_are_row_hash_identical(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Every maintained entity column matches between batch and stream on
    # every row (sorted by entity_id).
    assert out["entities_match"] is True, out
    # Same for the maintained account columns (sorted by iban).
    assert out["accounts_match"] is True, out
    # Row counts also match: batch's DISTINCT and stream's MERGE
    # both end up with one row per key.
    assert out["entities_batch_count"] == out["entities_stream_count"], out
    assert out["accounts_batch_count"] == out["accounts_stream_count"], out
    # Guard against a silent no-op where both sides read the same table.
    assert out["entities_batch_count"] > 0, out
    assert out["accounts_batch_count"] > 0, out


# ---------------------------------------------------------------------------
# Subprocess payload
# ---------------------------------------------------------------------------

_PACS_SCHEMA = (
    "txn_id string, uetr string, "
    "dbtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "cdtr struct<nm:string, ctry_of_res:string, "
    "pstl_adr:struct<twn_nm:string, strt_nm:string>, id:struct<lei:string>>, "
    "dbtr_agt struct<bicfi:string>, cdtr_agt struct<bicfi:string>, "
    "dbtr_acct struct<iban:string, ccy:string>, cdtr_acct struct<iban:string, ccy:string>, "
    "intrmy_agt_1 struct<bicfi:string>, "
    "intrmy_agt_2 struct<bicfi:string>, "
    "intrmy_agt_3 struct<bicfi:string>, "
    "intr_bk_sttlm_amt decimal(18,2), intr_bk_sttlm_ccy string, "
    "cre_dt_tm timestamp, purp_cd string, "
    "rgltry_rptg array<string>, msg_id string"
)


# Six distinct real entities; each contributes multiple rows with the reported
# name drifting slightly (spelling variants) so batch's per-column min() has
# non-trivial work to do, and so does stream's LEAST() MERGE. The full corpus
# has 20 rows spread across 5 chunks; each chunk is one micro-batch.
_ENTITIES = [
    ("ACME LTD", "US", "BOSTON", "LEI-ACME"),
    ("ACME CORP", "US", "BOSTON", "LEI-ACME"),  # same real entity, name drift
    ("ACME AG", "US", "BOSTON", "LEI-ACME"),
    ("BETA GMBH", "DE", "BERLIN", "LEI-BETA"),
    ("BETA GROUP", "DE", "BERLIN", "LEI-BETA"),  # name drift
    ("CHARLIE INC", "GB", "LONDON", "LEI-CHARLIE"),
    ("DELTA PLC", "GB", "MANCHESTER", "LEI-DELTA"),
    ("MARIA GARCIA", "ES", "MADRID", None),  # no LEI -> name-hash entity_id
    ("JOSE PEREZ", "ES", "MADRID", None),
    ("ZULU BANK", "SG", "SINGAPORE", "LEI-ZULU"),
]

# Fixed BICs / accounts / currencies per entity so build_accounts sees a
# stable holder_entity -> iban mapping. IBANs are keyed by index.
_IBANS = [f"IBAN-{i:03d}" for i in range(len(_ENTITIES))]


def _row(spark, txn_id, ts, dbtr_i, cdtr_i, iban_dbtr=None, iban_cdtr=None):
    """One pacs.008 row. ``iban_dbtr`` / ``iban_cdtr`` override the default
    entity-index iban so the corpus can carry the same iban across dbtr and
    cdtr sides on different entities (E1 BLOCKER 2 cross-side blending
    fixture)."""
    d_name, d_ctry, d_city, d_lei = _ENTITIES[dbtr_i]
    c_name, c_ctry, c_city, c_lei = _ENTITIES[cdtr_i]
    dbtr = (d_name, d_ctry, (d_city, "MAIN ST"), (d_lei,))
    cdtr = (c_name, c_ctry, (c_city, "HIGH ST"), (c_lei,))
    return (
        txn_id,
        f"UETR-{txn_id}",
        dbtr,
        cdtr,
        ("MERIUS2L",),
        ("NRTHGB3X",),
        (iban_dbtr or _IBANS[dbtr_i], "USD"),
        (iban_cdtr or _IBANS[cdtr_i], "USD"),
        (None,),
        (None,),
        (None,),
        Decimal("100.00"),
        "USD",
        ts,
        "SALA",
        [],
        f"MSG-{txn_id}",
    )


# 20 payments across the 10 entities, plus 4 cross-side-blending fixtures.
# Pairs cycle so each entity appears in both roles and multiple chunks
# (batch's per-column min() is meaningful only when an entity_id appears
# multiple times). Format: (dbtr_i, cdtr_i, iban_dbtr_override,
# iban_cdtr_override). None overrides use the entity-index default iban.
_PAIRS = [
    (0, 3, None, None),
    (1, 4, None, None),  # ACME (name variant) -> BETA (variant)
    (2, 3, None, None),  # ACME AG -> BETA GMBH
    (0, 5, None, None),
    (5, 0, None, None),
    (3, 6, None, None),
    (6, 3, None, None),
    (7, 8, None, None),
    (8, 7, None, None),
    (9, 0, None, None),
    (0, 9, None, None),
    (4, 5, None, None),
    (5, 4, None, None),
    (6, 7, None, None),
    (7, 6, None, None),
    (2, 5, None, None),
    (5, 2, None, None),
    (1, 3, None, None),
    (3, 1, None, None),
    (8, 9, None, None),
    # BLOCKER 2 fixture (E1 review). Two ibans (SHARED-A, SHARED-B)
    # appear on BOTH dbtr and cdtr sides across DIFFERENT entities, so
    # each iban's target row is a genuine tiebreak between two observed
    # rows with DIFFERENT holder_entity_id AND different bank_bic. Per-
    # column LEAST would synthesise a row that appears on neither side
    # (debtor's holder + creditor's bank_bic + a third row's opened_date);
    # row_number-1 by (holder, bic, opened_date) asc_nulls_last picks
    # exactly one actual observed row, which is what batch does.
    #
    # For SHARED-A: dbtr=0 (LEI-ACME) with bank_bic MERIUS2L; then
    # cdtr=8 (name-hash of JOSE PEREZ) with bank_bic NRTHGB3X. The two
    # rows differ on ALL four dimension columns.
    (0, 3, "SHARED-A", None),
    (7, 8, None, "SHARED-A"),
    # SHARED-B: cdtr=0 first, dbtr=5 (LEI-CHARLIE) later; different
    # holder, different bank_bic, different opened_date.
    (5, 0, None, "SHARED-B"),
    (5, 3, "SHARED-B", None),
]


def _full_bronze(spark):
    from pyspark.sql import Row  # noqa: F401 -- readability

    ts0 = datetime(2024, 6, 1)
    rows = [_row(spark, f"T{ix}", ts0 + timedelta(hours=ix), *tup) for ix, tup in enumerate(_PAIRS)]
    return spark.createDataFrame(rows, _PACS_SCHEMA)


def _chunks(bronze, n=5):
    """Split by cre_dt_tm hour into n contiguous groups. ``per`` is rounded
    UP so the last chunk absorbs any remainder -- otherwise a corpus of 24
    rows with n=5 would drop the last 4 rows and silently skip the E1
    BLOCKER 2 fixture."""
    import math

    rows = bronze.collect()
    per = max(1, math.ceil(len(rows) / n))
    return [rows[i : i + per] for i in range(0, len(rows), per)]


def _run(jars):
    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as work:
        spark = (
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
            .config("spark.sql.catalog.lh.warehouse", f"file://{work}/wh")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        import silver_build_financial as sb
        import silver_stream_financial as ss

        ss.CATALOG = "lh"

        # Two catalogs' worth of silver.entities / silver.accounts, one for
        # batch and one for stream, so the comparison is against two live
        # tables written by the two paths.
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver_stream")
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver_batch")
        for schema in ("silver_stream", "silver_batch"):
            for name, ddl in (
                ("entities", sb.DDL_ENTITIES),
                ("accounts", sb.DDL_ACCOUNTS),
            ):
                spark.sql(f"CREATE TABLE lh.{schema}.{name} ({ddl.split('(', 1)[1]}")

        bronze = _full_bronze(spark).cache()

        # ---- BATCH: single pass over the full corpus.
        txns_full = sb.build_transactions(bronze)
        ents_batch = sb.build_entities(txns_full, bronze, kyc=None)
        accts_batch = sb.build_accounts(bronze, kyc=None)
        ents_batch.writeTo("lh.silver_batch.entities").append()
        accts_batch.writeTo("lh.silver_batch.accounts").append()

        # ---- STREAM: five micro-batches, MERGE-per-batch.
        ss.SILVER_ENTITIES = "silver_stream.entities"
        ss.SILVER_ACCOUNTS = "silver_stream.accounts"

        chunks = _chunks(bronze)
        for chunk_rows in chunks:
            chunk = spark.createDataFrame(chunk_rows, _PACS_SCHEMA)
            txns_chunk = sb.build_transactions(chunk)
            ss.append_new_dimensions(spark, chunk, txns_chunk, None)

        # ---- Compare on the maintained columns only. KYC and screening
        # columns are NULL on both sides (kyc=None), so the row-hash on
        # (entity_id, entity_type, name, legal_name, country) captures the
        # E1 semantics. Row order fixed by ORDER BY so a partition shuffle
        # cannot make the comparison flap.
        ent_cols = "entity_id, entity_type, name, legal_name, country"
        ents_batch_rows = spark.sql(
            f"SELECT {ent_cols} FROM lh.silver_batch.entities ORDER BY entity_id"
        ).collect()
        ents_stream_rows = spark.sql(
            f"SELECT {ent_cols} FROM lh.silver_stream.entities ORDER BY entity_id"
        ).collect()

        acct_cols = "iban, holder_entity_id, bank_bic, currency, opened_date"
        # account_id is xxhash64(iban), same for both sides; skip it and use
        # iban as the join key to keep the comparison stable.
        accts_batch_rows = spark.sql(
            f"SELECT {acct_cols} FROM lh.silver_batch.accounts ORDER BY iban"
        ).collect()
        accts_stream_rows = spark.sql(
            f"SELECT {acct_cols} FROM lh.silver_stream.accounts ORDER BY iban"
        ).collect()

        # tuple(row) is the Spark Row tuple; equal Rows equate to equal tuples.
        entities_match = [tuple(r) for r in ents_batch_rows] == [tuple(r) for r in ents_stream_rows]
        accounts_match = [tuple(r) for r in accts_batch_rows] == [
            tuple(r) for r in accts_stream_rows
        ]

        out = {
            "entities_batch_count": len(ents_batch_rows),
            "entities_stream_count": len(ents_stream_rows),
            "entities_match": entities_match,
            "accounts_batch_count": len(accts_batch_rows),
            "accounts_stream_count": len(accts_stream_rows),
            "accounts_match": accounts_match,
        }
        if not entities_match:
            out["entities_batch_sample"] = [tuple(r) for r in ents_batch_rows[:5]]
            out["entities_stream_sample"] = [tuple(r) for r in ents_stream_rows[:5]]
        if not accounts_match:
            out["accounts_batch_sample"] = [tuple(r) for r in accts_batch_rows[:5]]
            out["accounts_stream_sample"] = [tuple(r) for r in accts_stream_rows[:5]]
        print(json.dumps(out, default=str))
        spark.stop()


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts on PYTHONPATH; argv[1]
    # is the comma-separated jar classpath.
    _run(sys.argv[1])
