"""Delta + Hive continuous table naming (lb16-cs), run in a fresh JVM.

Not collected by pytest (no ``test_`` prefix). ``test_c360_delta_continuous_spark``
runs it as a subprocess because the Delta jars must be on the driver
classpath at JVM launch.

The environment is the one job.py gives a hive-delta recipe: CATALOG_NAME is
the Trino catalog ("lakehouse"), which names no Spark catalog, and
LB_ICEBERG_CATALOG is spark_catalog. Before the fix the continuous reset
named bronze ``lakehouse.default.bronze_raw`` and failed with
REQUIRES_SINGLE_PART_NAMESPACE before any stream ran.

Usage: python c360_delta_continuous_scenarios.py <jar_dir> <work_dir>
Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from c360_stream_scenarios import bronze_df, run_stream, session, stage_files  # noqa: E402


def _files_under(path):
    return sum(len(f) for _, _, f in os.walk(path)) if os.path.isdir(path) else 0


def main():
    jar_dir, work = sys.argv[1], sys.argv[2]
    buckets = {k: f"{work}/{k}" for k in ("bronze", "silver", "gold")}
    os.environ.update(
        {
            "CATALOG_NAME": "lakehouse",
            "LB_ICEBERG_CATALOG": "spark_catalog",
            "LB_CATALOG_TYPE": "hive",
            "LB_BRONZE_URI": f"file://{buckets['bronze']}/",
            "LB_SILVER_URI": f"file://{buckets['silver']}/",
            "LB_GOLD_URI": f"file://{buckets['gold']}/",
            "LB_BRONZE_TABLE": "default.bronze_raw",
            "LB_SILVER_TABLE": "silver.customer_interactions_enriched",
            "LB_GOLD_TABLE": "gold.customer_executive_dashboard",
        }
    )
    spark = session(jar_dir, work)
    import bronze_ingest_delta
    from common import _describe_table, _norm_uri, table_exists, write_delta_table

    out = {}
    raw_dir = f"{buckets['bronze']}/customer/interactions"
    bronze_df(spark, 5).coalesce(1).write.mode("overwrite").parquet(f"file://{raw_dir}")

    # bronze-ingest as main() names it, through a real stream.
    name, loc = bronze_ingest_delta.bronze_target()
    out["bronze_name"] = name
    out["bronze_expected_location"] = _norm_uri(loc)
    bronze_uri = os.environ["LB_BRONZE_URI"]

    def write(df, bid):
        return bronze_ingest_delta.write_bronze_batch(df, bid, name, bronze_uri, loc)

    src = stage_files(spark, work, "bronze", 2, 10)
    run_stream(spark, src, f"{work}/ckpt-bronze", write)
    out["bronze_rows"] = spark.table(name).count()
    out["bronze_location"] = _norm_uri(_describe_table(spark, name)[0])

    # Silver and gold where the Delta stream jobs put them.
    for ns, key in (("silver", "LB_SILVER_TABLE"), ("gold", "LB_GOLD_TABLE")):
        spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS spark_catalog.{ns} "
            f"LOCATION 'file://{buckets[ns]}/warehouse/'"
        )
        write_delta_table(
            spark,
            bronze_df(spark, 4),
            f"spark_catalog.{os.environ[key]}",
            f"file://{buckets[ns]}/",
            mode="overwrite",
        )

    # The reset job, as the CLI submits it (LB_CONTINUOUS_RESET=1).
    os.environ["LB_CONTINUOUS_RESET"] = "1"
    import bronze_verify

    tables, _, _ = bronze_verify.continuous_reset_targets()
    out["targets"] = tables
    bronze_verify.main()  # stops the session
    spark = session(jar_dir, work)
    out["exists_after"] = {t: table_exists(spark, t) for t in tables}
    out["bronze_dir_files_after"] = _files_under(
        f"{buckets['bronze']}/warehouse/default.db/bronze_raw"
    )
    out["raw_files_after"] = len([f for f in os.listdir(raw_dir) if f.endswith(".parquet")])

    # A fresh stream after the reset recreates bronze at the same location.
    src = stage_files(spark, work, "bronze-again", 1, 10, start=100)
    run_stream(spark, src, f"{work}/ckpt-bronze-again", write)
    out["bronze_rows_after_restart"] = spark.table(name).count()
    spark.stop()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
