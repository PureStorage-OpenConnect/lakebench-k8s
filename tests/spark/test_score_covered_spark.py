"""AML-6 covered scoring (``score_financial.py`` covered mode) on Iceberg.

One fixture, scored at recorded snapshots and then changed after them:

- fan_out has 5 instances. f1, f2 and f3 are fully present at the tick's
  snapshots. f4's only transaction is in a batch whose versions row lands
  after the versions snapshot (L2: the current versions table would show it
  sealed). f5's transaction never reaches silver.
- f3's participant account is remapped in silver.accounts after the
  accounts snapshot, to a key no entity has: the pinned read keeps f3
  covered.
- stack has one instance, not covered: its recall_covered is null.
- An alert written after the alerts snapshot is not scored.
- W2's alert on f4's transaction is on target: false positives count the
  full manifest, so it is no false positive though f4 is not covered.
- random r1 is covered and r2 is not: the chance floor uses covered random
  instances only.

Runs in a Spark child with the Iceberg jar on the driver classpath.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

import pytest

pytest.importorskip("pyspark")

RUN = "run-cov"


@pytest.mark.requires_jars("iceberg")
def test_covered_score(spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, spark_jars.classpath, timeout=900, env={"LB_RUN_ID": RUN})
    out = json.loads(res.stdout.strip().splitlines()[-1])
    s = out["summary"]
    assert s["mode"] == "covered" and s["status"] == "scored", s
    cov = s["covered"]
    typ = {t["typology_type"]: t for t in cov["typologies"]}
    fan = typ["fan_out"]
    assert fan["covered_instances"] == 3 and fan["corpus_instances"] == 5, fan
    assert fan["coverage"] == pytest.approx(0.6)
    assert fan["recall_covered"] == pytest.approx(1 / 3), fan
    assert typ["stack"]["covered_instances"] == 0 and typ["stack"]["recall_covered"] is None
    # No key that a renderer could show as the batch recall.
    assert "recall" not in fan and "typologies" not in s and "fp_rate" not in s
    assert out["parquet_columns"] and "recall" not in out["parquet_columns"]
    # FP over the full manifest: a1 (f1) and a2 (f4) on target, a3 and a4 off.
    assert cov["fp_rate_by_rule"]["W2_structuring"] == pytest.approx(0.5), cov
    # Chance over covered random instances: r1 of {r1}.
    assert cov["chance_by_rule"]["W2_structuring"] == pytest.approx(1.0), cov
    # The alert written after the alerts snapshot is not scored.
    assert cov["total_alerts"] == 4 and s["alert_set"]["rows"] == 4, s["alert_set"]
    assert set(s["alert_set"]["by_rule"]) == {"W2_structuring"}
    assert {"typology_type": "cross_border", "reason": "mode-excluded"} in cov[
        "excluded_typologies"
    ]
    # The L2 and remap cases, each against the current tables: without the
    # pins f4 would count as covered and f3 would not.
    assert out["current_versions_would_cover_f4"] is True
    assert out["current_accounts_would_drop_f3"] is True
    # Not scored, never a fallback.
    assert out["expired"]["status"] == "not_scored"
    assert "silver.transactions" in out["expired"]["reason"]
    # An unknown snapshot raises NotScored rather than falling back to current state.
    assert out["unknown"]["reason"] is not None
    assert "silver_batch_versions" in out["unknown"]["reason"]
    assert "recall" not in out["expired"]


_DDL = {
    "lh.silver.transactions": (
        "uetr STRING, _stream_id STRING, _batch_id BIGINT, ingest_ts TIMESTAMP"
    ),
    "lh.silver.silver_batch_versions": (
        "stream_id STRING, batch_id BIGINT, committed_at TIMESTAMP"
    ),
    "lh.silver.entities": "entity_id BIGINT, is_customer BOOLEAN",
    "lh.silver.accounts": "iban STRING, holder_entity_id BIGINT",
    "lh.gold.alerts": (
        "alert_id STRING, rule_id STRING, entity_id BIGINT, related_txn_ids ARRAY<STRING>, "
        "alert_ts TIMESTAMP, run_id STRING"
    ),
    "lh.gold.detection_status": (
        "rule_id STRING, status STRING, reason STRING, target_typology STRING, "
        "alert_count BIGINT, run_id STRING"
    ),
}


def _snap(spark, fq):
    return int(
        spark.sql(
            f"SELECT snapshot_id FROM {fq}.history WHERE is_current_ancestor "
            "ORDER BY made_current_at DESC LIMIT 1"
        ).collect()[0][0]
    )


def _run(jars):
    from argparse import Namespace
    from datetime import datetime

    from pyspark.sql import SparkSession

    with tempfile.TemporaryDirectory() as work:
        os.environ["LB_ICEBERG_CATALOG"] = "lh"
        os.environ["LB_FINANCIAL_ACCOUNT_PATH"] = f"file://{work}/account.parquet"
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
        import score_financial as sf

        for ns in ("silver", "gold"):
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lh.{ns}")
        for fq, cols in _DDL.items():
            spark.sql(f"CREATE TABLE {fq} ({cols}) USING iceberg")
        t0 = datetime(2026, 6, 1)

        def txns(batch, uetrs):
            spark.createDataFrame(
                [(u, "S1", batch, t0) for u in uetrs], _DDL["lh.silver.transactions"]
            ).writeTo("lh.silver.transactions").append()

        def seal(batch):
            spark.sql(
                f"INSERT INTO lh.silver.silver_batch_versions VALUES ('S1', {batch}, current_timestamp())"
            )

        # Participants: datagen id n holds IBAN In; silver key 100+n.
        spark.createDataFrame(
            [(f"I{n}", n) for n in range(1, 10)], "iban STRING, holder_entity_id BIGINT"
        ).write.parquet(f"file://{work}/account.parquet")
        txns(0, ["u1", "u2", "u3", "u7"])
        seal(0)
        txns(1, ["u4"])
        seal(1)
        txns(2, ["u5"])  # sealed only after the versions snapshot (L2)
        spark.sql(
            "INSERT INTO lh.silver.accounts VALUES "
            + ", ".join(f"('I{n}', {100 + n})" for n in range(1, 10))
        )
        spark.sql(
            "INSERT INTO lh.silver.entities VALUES "
            + ", ".join(f"({100 + n}, true)" for n in range(1, 10))
        )
        status = [
            ("W2_structuring", "ran", None, "fan_out", 4, RUN),
            ("W4_risk_propagation", "ran", None, "stack", 0, RUN),
            ("W7_cross_border_high_risk", "skipped", "mode-excluded", "cross_border", None, RUN),
        ]
        spark.createDataFrame(status, _DDL["lh.gold.detection_status"]).writeTo(
            "lh.gold.detection_status"
        ).append()

        def alert(aid, uetrs):
            return (aid, "W2_structuring", 101, uetrs, t0, RUN)

        spark.createDataFrame(
            [alert("a1", ["u1"]), alert("a2", ["u5"]), alert("a3", ["u9"]), alert("a4", ["u7"])],
            _DDL["lh.gold.alerts"],
        ).writeTo("lh.gold.alerts").append()

        ids = {
            "txns": _snap(spark, "lh.silver.transactions"),
            "entities": _snap(spark, "lh.silver.entities"),
            "accounts": _snap(spark, "lh.silver.accounts"),
            "versions": _snap(spark, "lh.silver.silver_batch_versions"),
            "alerts": _snap(spark, "lh.gold.alerts"),
            "status": _snap(spark, "lh.gold.detection_status"),
        }
        # After the tick: batch 2 sealed, f3's account remapped, one more alert.
        seal(2)
        spark.sql("UPDATE lh.silver.accounts SET holder_entity_id = 999 WHERE iban = 'I3'")
        spark.createDataFrame([alert("a5", ["u2"])], _DDL["lh.gold.alerts"]).writeTo(
            "lh.gold.alerts"
        ).append()

        manifest = spark.createDataFrame(
            [
                ("f1", "fan_out", "W2", ["u1", "u2"], [1, 2], 1),
                ("f2", "fan_out", "W2", ["u3"], [2], 1),
                ("f3", "fan_out", "W2", ["u4"], [3], 1),
                ("f4", "fan_out", "W2", ["u5"], [4], 1),
                ("f5", "fan_out", "W2", ["u6"], [5], 1),
                ("s1", "stack", "W4", ["u6"], [6], 1),
                ("r1", "random", "W2", ["u7"], [7], 1),
                ("r2", "random", "W2", ["u8"], [8], 1),
            ],
            "typology_id STRING, typology_type STRING, expected_workload STRING, "
            "participant_uetrs ARRAY<STRING>, participant_entity_ids ARRAY<BIGINT>, seed BIGINT",
        )

        out_uri = f"file://{work}/out/recall.parquet"
        sf.run_covered(spark, Namespace(output=out_uri), manifest, ids)
        summary = json.loads(open(f"{work}/out/recall.json").read())
        parquet_columns = spark.read.parquet(out_uri).columns

        # Against the current tables instead of the pins.
        from aml_features import _account_id_map
        from common import sealed_txns_filter

        current = sf.covered_instances(
            manifest,
            sealed_txns_filter(
                spark,
                spark.table("lh.silver.transactions"),
                "lh",
                "silver.silver_batch_versions",
            ).select("uetr"),
            _account_id_map(
                spark.read.parquet(f"file://{work}/account.parquet"),
                sf._iban_to_key(spark.table("lh.silver.accounts")),
            ),
            spark.table("lh.silver.entities").selectExpr("entity_id AS key"),
        )
        cur = {r["typology_id"]: r["covered"] for r in current.collect()}

        bad = dict(ids, txns=123456789)
        sf.run_covered(spark, Namespace(output=f"file://{work}/x/recall.parquet"), manifest, bad)
        expired = json.loads(open(f"{work}/x/recall.json").read())
        args = Namespace(**{f"covered_{k}_snapshot": str(v) for k, v in ids.items()})
        args.covered_versions_snapshot = "unknown"
        try:
            sf.covered_snapshot_ids(args)
            unknown = {"reason": None}
        except sf.NotScored as e:
            unknown = {"reason": str(e)}

        print(
            json.dumps(
                {
                    "summary": summary,
                    "parquet_columns": parquet_columns,
                    "current_versions_would_cover_f4": bool(cur.get("f4")),
                    "current_accounts_would_drop_f3": not cur.get("f3"),
                    "expired": expired,
                    "unknown": unknown,
                }
            )
        )
        spark.stop()


if __name__ == "__main__":
    _run(sys.argv[1])
