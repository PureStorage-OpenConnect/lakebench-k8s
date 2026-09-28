"""I5: silver_stream_financial._merge_batch only DELETEs on the first
micro-batch of each stream run, so Iceberg records one delete snapshot per
restart instead of one per trigger.

Runs the check in a child process because Iceberg jars must be on the
driver classpath at JVM launch and an earlier test module in the same
pytest run may already have started a plain JVM.
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parents[1] / "src/lakebench/spark/scripts"


def _iceberg_jar() -> str | None:
    """LB_TEST_ICEBERG_JAR, or an iceberg-spark-runtime-4.0 jar in a local ivy cache."""
    env = os.environ.get("LB_TEST_ICEBERG_JAR")
    if env and Path(env).exists():
        return env
    hits = sorted(
        glob.glob(str(Path.home() / ".lakebench/local/*/ivy/cache/org.apache.iceberg/*/jars/*.jar"))
        + glob.glob(str(Path.home() / ".ivy2*/cache/org.apache.iceberg/*/jars/*.jar"))
    )
    return next((h for h in hits if "spark-runtime-4.0" in h), None)


def test_one_delete_per_restart_in_a_fresh_jvm():
    jar = _iceberg_jar()
    if jar is None:
        pytest.skip("no iceberg-spark-runtime-4.0 jar available (set LB_TEST_ICEBERG_JAR)")
    res = subprocess.run(
        [sys.executable, __file__, jar], capture_output=True, text=True, timeout=600
    )
    assert res.returncode == 0, res.stdout[-4000:] + res.stderr[-4000:]
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # Run 1: one query run, three non-empty micro-batches. Pre-fix, every
    # micro-batch commits a DELETE, so run 1 leaves 3 delete snapshots per
    # table. Post-fix, only the run's first micro-batch does, so 1 delete
    # snapshot per table.
    assert out["run1_delete_snapshots_txns"] == 1, out
    assert out["run1_delete_snapshots_edges"] == 1, out
    assert out["run1_append_snapshots_txns"] == 3, out
    assert out["run1_append_snapshots_edges"] == 3, out
    # Run 2: a restart-from-checkpoint style new run adds one delete per
    # table (its first micro-batch) plus three appends.
    assert out["run2_delete_snapshots_txns"] == 2, out
    assert out["run2_delete_snapshots_edges"] == 2, out
    assert out["run2_append_snapshots_txns"] == 6, out
    assert out["run2_append_snapshots_edges"] == 6, out


# ---------------------------------------------------------------------------
# Subprocess payload: runs when this file is invoked directly with a jar arg.
# ---------------------------------------------------------------------------


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


def _bronze_row(spark, txn_id, ts):
    def party(nm):
        return (nm, "US", ("NYC", "MAIN ST"), (f"LEI-{nm}",))

    row = (
        txn_id,
        f"UETR-{txn_id}",
        party(f"O-{txn_id}"),
        party(f"B-{txn_id}"),
        ("MERIUS2L",),
        ("NRTHGB3X",),
        ("US01",),
        ("GB02",),
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


_TXNS_DDL = """
CREATE TABLE lh.silver.transactions (
    txn_id                  STRING NOT NULL,
    uetr                    STRING NOT NULL,
    originator_id           BIGINT NOT NULL,
    beneficiary_id          BIGINT NOT NULL,
    originator_bank_bic     STRING,
    beneficiary_bank_bic    STRING,
    txn_amount              DECIMAL(18, 2) NOT NULL,
    txn_currency            STRING NOT NULL,
    txn_amount_usd          DECIMAL(18, 2),
    txn_timestamp           TIMESTAMP NOT NULL,
    txn_type                STRING NOT NULL,
    purpose_code            STRING,
    correspondent_chain     ARRAY<STRING>,
    cross_border            BOOLEAN,
    regulatory_reported     BOOLEAN NOT NULL,
    rptd_originator_name    STRING,
    rptd_originator_address STRING,
    rptd_beneficiary_name   STRING,
    rptd_beneficiary_address STRING,
    source_message_ref      STRING,
    _batch_id               BIGINT,
    ingest_ts               TIMESTAMP
) USING iceberg PARTITIONED BY (months(txn_timestamp))
"""

_EDGES_DDL = """
CREATE TABLE lh.silver.counterparty_edges (
    source_entity_id       BIGINT NOT NULL,
    target_entity_id       BIGINT NOT NULL,
    first_seen_ts          TIMESTAMP NOT NULL,
    last_seen_ts           TIMESTAMP NOT NULL,
    cumulative_amount_usd  DECIMAL(38, 2) NOT NULL,
    txn_count              BIGINT NOT NULL,
    _batch_id              BIGINT
) USING iceberg PARTITIONED BY (bucket(64, source_entity_id))
"""


def _snapshot_counts(spark, fq_table):
    rows = spark.sql(
        f"SELECT operation, count(*) AS n FROM {fq_table}.snapshots GROUP BY operation"
    ).collect()
    return {r["operation"]: r["n"] for r in rows}


def _run(jar):
    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as work:
        spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.jars", jar)
            .config("spark.sql.shuffle.partitions", "2")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.lh", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lh.type", "hadoop")
            # The clone that runs inside the FE handler has its own catalog
            # instance; caching would show a stale snapshot list back in the
            # driver session.
            .config("spark.sql.catalog.lh.cache-enabled", "false")
            .config("spark.sql.catalog.lh.warehouse", f"file://{work}/wh")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        import common
        import silver_stream_financial as ss

        # Point the module at the test's Iceberg catalog and tables.
        ss.CATALOG = "lh"
        ss.SILVER_TXNS = "silver.transactions"
        ss.SILVER_EDGES = "silver.counterparty_edges"

        spark.sql("CREATE NAMESPACE IF NOT EXISTS lh.silver")
        spark.sql(_TXNS_DDL)
        spark.sql(_EDGES_DDL)

        # Bypass KYC and dimension writes: the test isolates the DELETE gate
        # on the two batch-keyed tables. append_new_dimensions writes to
        # tables the test does not create; skip it entirely.
        ss._KYC = None
        ss._KYC_LOADED = True
        ss._kyc = lambda _s: None
        ss.append_new_dimensions = lambda *_a, **_kw: (0, 0)

        def bronze(bid):
            return _bronze_row(
                spark,
                f"T{bid}-o-b",
                datetime(2024, 6, 1) + timedelta(days=bid),
            )

        # ---- Run 1: three micro-batches, one query run.
        spark.sparkContext.setJobGroup("stream-run-1", "test")
        for bid in range(3):
            ss._merge_batch(bronze(bid), bid)

        r1_txns = _snapshot_counts(spark, "lh.silver.transactions")
        r1_edges = _snapshot_counts(spark, "lh.silver.counterparty_edges")

        # ---- Run 2: restart with a new query run id.
        spark.sparkContext.setJobGroup("stream-run-2", "test")
        for bid in range(3, 6):
            ss._merge_batch(bronze(bid), bid)

        r2_txns = _snapshot_counts(spark, "lh.silver.transactions")
        r2_edges = _snapshot_counts(spark, "lh.silver.counterparty_edges")

        out = {
            "run1_delete_snapshots_txns": r1_txns.get("delete", 0) + r1_txns.get("overwrite", 0),
            "run1_delete_snapshots_edges": r1_edges.get("delete", 0) + r1_edges.get("overwrite", 0),
            "run1_append_snapshots_txns": r1_txns.get("append", 0),
            "run1_append_snapshots_edges": r1_edges.get("append", 0),
            "run2_delete_snapshots_txns": r2_txns.get("delete", 0) + r2_txns.get("overwrite", 0),
            "run2_delete_snapshots_edges": r2_edges.get("delete", 0) + r2_edges.get("overwrite", 0),
            "run2_append_snapshots_txns": r2_txns.get("append", 0),
            "run2_append_snapshots_edges": r2_edges.get("append", 0),
        }
        # Guard against a silent no-op that lets the test pass because
        # replay_possible is never referenced.
        assert common.replay_possible is not None
        print(json.dumps(out))
        spark.stop()


if __name__ == "__main__":
    sys.path[:0] = [str(SCRIPTS)]
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    _run(sys.argv[1])
