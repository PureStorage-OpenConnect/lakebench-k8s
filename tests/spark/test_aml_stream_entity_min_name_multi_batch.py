"""E1: multi-batch stream picks LEAST(name) and re-derives entity_type.

Four micro-batches, each carrying a different name ("ACME LTD",
"ACME CORP", "ACME AG", "ACME BV") for the same entity (same LEI on
both sides so ``_entity_id_from`` collapses to one entity_id). Batch
mode's ``build_entities`` picks ``min("name") = "ACME AG"`` and
re-derives ``entity_type`` from that. The pre-E1 stream picked the
first-batch name ("ACME LTD") and typed the entity from that, so the
same bronze produced different silver.entities depending on which
mode ran. The E1 MERGE replicates batch's LEAST() and re-derives
entity_type from the merged name via the shared regex helper.

Runs in a Spark child (``spark_subprocess``) so the JVM launches with the
Iceberg runtime jar from ``LB_SPARK_TEST_JARS`` on its classpath and the
Iceberg SQL extensions enabled -- MERGE INTO on Iceberg is a SQL-extension
surface, not a base Spark surface.
"""

from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime
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
def test_min_name_across_batches_in_a_fresh_jvm(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=600)
    # Only the final JSON blob is asserted; the driver logs precede it.
    payload = json.loads(res.stdout.strip().splitlines()[-1])
    # One row per entity, one entity across all four batches (same LEI).
    assert payload["entities_rows"] == 1, payload
    # LEAST() over the four names is "ACME AG" (lexicographic; A < B < C < L).
    assert payload["final_name"] == "ACME AG", payload
    # Re-derived from "ACME AG": the trailing "AG" matches the corporate
    # suffix regex, so entity_type is "Company".
    assert payload["final_entity_type"] == "Company", payload
    # The four-name-progression assertion: after each batch the stored
    # name is the LEAST of the target and the arriving batch's name.
    # This makes an off-by-one in the MERGE UPDATE clause (e.g. using
    # s.name unconditionally instead of LEAST) visible.
    assert payload["progression"] == [
        "ACME LTD",  # batch 1 insert
        "ACME CORP",  # batch 2 update: LEAST(ACME LTD, ACME CORP) = ACME CORP
        "ACME AG",  # batch 3 update: LEAST(ACME CORP, ACME AG)  = ACME AG
        "ACME AG",  # batch 4 update: LEAST(ACME AG,   ACME BV)  = ACME AG
    ], payload
    # BLOCKER 3 regression guard: batch enforces ``legal_name = name`` at
    # build_entities. The MERGE must set legal_name to the SAME expression
    # as name, not an independent LEAST -- otherwise a target whose two
    # columns disagreed for any reason would drift further apart on each
    # update. Assert both after every batch, not just the last one.
    assert payload["legal_name_progression"] == payload["progression"], payload


# ---------------------------------------------------------------------------
# Subprocess payload: runs when this file is invoked directly with the jar
# classpath (comma-separated) as its argument.
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


def _bronze_row(spark, txn_id, ts, dbtr_name, cdtr_name):
    """One pacs.008 row. Both parties carry a fixed LEI so ``_entity_id_from``
    resolves to the same entity_id across batches even as the reported name
    changes; the entity_id of the dbtr side is what the test tracks.
    """
    dbtr = (
        dbtr_name,
        "US",
        ("BOSTON", "MAIN ST"),
        ("LEI-ACME",),  # same LEI -> same entity_id across all batches
    )
    cdtr = (
        cdtr_name,
        "GB",
        ("LONDON", "HIGH ST"),
        ("LEI-CDTR",),
    )
    row = (
        txn_id,
        f"UETR-{txn_id}",
        dbtr,
        cdtr,
        ("MERIUS2L",),
        ("NRTHGB3X",),
        ("US01", "USD"),
        ("GB02", "GBP"),
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
    return spark.createDataFrame([row], _PACS_SCHEMA)


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

        # Point the module at the test's Iceberg catalog and tables.
        ss.CATALOG = "lh"
        ss.SILVER_ENTITIES = "silver.entities"
        ss.SILVER_ACCOUNTS = "silver.accounts"

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
        # The job's own DDLs, re-scoped to this test's catalog/table.
        for name, ddl in (
            ("silver.entities", sb.DDL_ENTITIES),
            ("silver.accounts", sb.DDL_ACCOUNTS),
        ):
            spark.sql(f"CREATE TABLE lh.{name} ({ddl.split('(', 1)[1]}")

        # The four names the dbtr side reports over the four batches. Same
        # LEI everywhere, so entity_id is the same and the MERGE hits the
        # WHEN MATCHED path on batches 2..4.
        names = ["ACME LTD", "ACME CORP", "ACME AG", "ACME BV"]
        progression: list[str] = []
        legal_name_progression: list[str] = []
        for bid, dbtr_name in enumerate(names):
            bronze = _bronze_row(
                spark,
                f"T{bid}",
                datetime(2024, 6, 1 + bid, 12, 0),
                dbtr_name=dbtr_name,
                cdtr_name=f"OTHER-{bid}",  # cdtr is unique so it just inserts
            )
            txns = sb.build_transactions(bronze)
            # kyc=None so KYC columns stay NULL and only the E1 target fields
            # (name, entity_type, legal_name, country) move.
            ss.append_new_dimensions(spark, bronze, txns, None)

            # After each batch, read the stored name for the pinned entity_id.
            from pyspark.sql.functions import col as _col
            from silver_build_financial import _entity_id_from

            target_id_df = bronze.select(
                _entity_id_from(
                    _col("dbtr.nm"),
                    _col("dbtr.ctry_of_res"),
                    _col("dbtr.pstl_adr.twn_nm"),
                    _col("dbtr.id.lei"),
                ).alias("entity_id")
            )
            target_id = target_id_df.collect()[0]["entity_id"]
            row = spark.sql(
                f"SELECT name, legal_name FROM lh.silver.entities WHERE entity_id = {target_id}"
            ).collect()[0]
            progression.append(row["name"])
            legal_name_progression.append(row["legal_name"])

        # Count rows the pinned entity_id owns (must be exactly 1: MERGE
        # UPDATE, not INSERT, for batches 2..4).
        entity_rows = spark.sql(
            f"SELECT COUNT(*) AS n FROM lh.silver.entities WHERE entity_id = {target_id}"
        ).collect()[0]["n"]
        final_row = spark.sql(
            f"SELECT name, entity_type, legal_name FROM lh.silver.entities "
            f"WHERE entity_id = {target_id}"
        ).collect()[0]

        out = {
            "entities_rows": int(entity_rows),
            "final_name": final_row["name"],
            "final_entity_type": final_row["entity_type"],
            "final_legal_name": final_row["legal_name"],
            "progression": progression,
            "legal_name_progression": legal_name_progression,
        }
        print(json.dumps(out))
        spark.stop()


if __name__ == "__main__":
    # Run by spark_subprocess, which puts the scripts on PYTHONPATH; argv[1]
    # is the comma-separated jar classpath.
    _run(sys.argv[1])
