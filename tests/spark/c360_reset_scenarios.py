"""Continuous-reset scenarios for c360 (LB-142), run in a fresh JVM.

Not collected by pytest (no ``test_`` prefix). ``test_c360_reset_spark`` runs
it as a subprocess because the Iceberg and Delta jars must be on the driver
classpath at JVM launch.

The work directory stands in for the deployment's bucket: the Iceberg hadoop
warehouse, an external Delta table and the raw landing zone all live under
it, plus one table outside it that the reset must not delete files from.

Usage: python c360_reset_scenarios.py <jar_dir> <work_dir>
Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from c360_stream_scenarios import bronze_df, session  # noqa: E402


def _files_under(path):
    return sum(len(f) for _, _, f in os.walk(path)) if os.path.isdir(path) else 0


def main():
    jar_dir, work = sys.argv[1], sys.argv[2]
    spark = session(jar_dir, work)
    from common import (
        refuse_fresh_checkpoint_over_data,
        reset_stream_tables,
        table_exists,
    )

    wh = f"{work}/ice-wh"
    out = {}

    # Raw landing zone under the same root; the reset must never touch it.
    raw_dir = f"{wh}/customer/interactions"
    bronze_df(spark, 20).coalesce(1).write.mode("overwrite").parquet(f"file://{raw_dir}")

    # Tables as a batch run and a continuous run leave them.
    for ns in ("default", "silver", "gold"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS ice.{ns}")
    bronze_df(spark, 10).writeTo("ice.default.bronze_raw").create()
    bronze_df(spark, 10).writeTo("ice.silver.customer_interactions_enriched").create()
    bronze_df(spark, 5).writeTo("ice.gold.customer_executive_dashboard").create()
    # A file the Iceberg metadata does not know about; the reset removes it.
    orphan = f"{wh}/silver/customer_interactions_enriched/data/orphan.parquet"
    Path(orphan).parent.mkdir(parents=True, exist_ok=True)
    Path(orphan).write_bytes(b"x")

    # External Delta table under the owned root (plain DROP keeps its files).
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")
    delta_dir = f"{wh}/warehouse/silver.db/delta_t"
    bronze_df(spark, 10).write.format("delta").option("path", f"file://{delta_dir}").saveAsTable(
        "spark_catalog.silver.delta_t"
    )
    out["delta_files_before"] = _files_under(delta_dir)

    # A table outside the owned root: dropped, files kept.
    spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.other")
    foreign_dir = f"{work}/foreign/other.db/foreign_t"
    bronze_df(spark, 10).write.format("delta").option("path", f"file://{foreign_dir}").saveAsTable(
        "spark_catalog.other.foreign_t"
    )
    # A table whose directory contains a path the reset must keep.
    # Delta, so a plain DROP leaves the files and only the guard decides.
    wide_dir = f"{wh}/wide/wide_t"
    bronze_df(spark, 3).write.format("delta").option("path", f"file://{wide_dir}").saveAsTable(
        "spark_catalog.other.wide_t"
    )
    out["foreign_files_before"] = _files_under(foreign_dir)
    # A table whose location is a namespace root holding a sibling table's
    # files: the name guard keeps the directory.
    root_dir = f"{wh}/nsroot"
    bronze_df(spark, 3).write.format("delta").option("path", f"file://{root_dir}").saveAsTable(
        "spark_catalog.other.rooted_t"
    )
    Path(f"{root_dir}/sibling_t").mkdir(parents=True, exist_ok=True)
    Path(f"{root_dir}/sibling_t/part.parquet").write_bytes(b"x")

    # A fresh checkpoint over the full silver table: silver-stream refuses.
    ckpt = f"file://{work}/ckpt/silver-stream"
    try:
        refuse_fresh_checkpoint_over_data(spark, ckpt, "ice.silver.customer_interactions_enriched")
        out["refused_before"] = False
    except SystemExit:
        out["refused_before"] = True

    # Spelled file:///... on purpose; Hadoop reports some locations as
    # file:/... and others as file:///..., and the reset must treat both as one.
    owned = [f"file://{wh}/"]
    keep = [f"file://{raw_dir}/"]
    dropped = reset_stream_tables(
        spark,
        ["spark_catalog.silver.delta_t", "spark_catalog.other.foreign_t", "ice.nope.missing"],
        owned_uris=owned,
        keep_uris=keep,
    )
    # keep_uris inside the table's directory: dropped, directory kept.
    out["wide_files_before"] = _files_under(wide_dir)
    out["dropped_wide"] = reset_stream_tables(
        spark,
        ["spark_catalog.other.wide_t"],
        owned_uris=owned,
        keep_uris=[f"file://{wide_dir}/_delta_log/"],
    )
    out["wide_files_after"] = _files_under(wide_dir)
    out["dropped_rooted"] = reset_stream_tables(
        spark, ["spark_catalog.other.rooted_t"], owned_uris=owned, keep_uris=keep
    )
    out["sibling_survives"] = os.path.exists(f"{root_dir}/sibling_t/part.parquet")
    out["dropped_direct"] = dropped
    out["delta_exists_after"] = table_exists(spark, "spark_catalog.silver.delta_t")
    out["delta_files_after"] = _files_under(delta_dir)
    out["foreign_exists_after"] = table_exists(spark, "spark_catalog.other.foreign_t")
    out["foreign_files_after"] = _files_under(foreign_dir)

    # The job itself, as the CLI submits it.
    os.environ.update(
        {
            "LB_CONTINUOUS_RESET": "1",
            "CATALOG_NAME": "ice",
            "LB_ICEBERG_CATALOG": "ice",
            "LB_BRONZE_URI": f"file:{wh}/",
            "LB_SILVER_URI": f"file:{wh}/",
            "LB_GOLD_URI": f"file:{wh}/",
        }
    )
    import bronze_verify

    tables, _, _ = bronze_verify.continuous_reset_targets()
    out["targets"] = tables
    # main() stops the session; restart one to inspect the result.
    bronze_verify.main()
    spark = session(jar_dir, work)
    out["exists_after"] = {t: table_exists(spark, t) for t in tables}
    out["silver_dir_files_after"] = _files_under(f"{wh}/silver/customer_interactions_enriched")
    out["raw_files_after"] = len([f for f in os.listdir(raw_dir) if f.endswith(".parquet")])
    try:
        refuse_fresh_checkpoint_over_data(spark, ckpt, "ice.silver.customer_interactions_enriched")
        out["refused_after"] = False
    except SystemExit:
        out["refused_after"] = True

    # Idempotent: a second reset over nothing succeeds and drops nothing.
    out["second_dropped"] = reset_stream_tables(spark, tables, owned_uris=owned, keep_uris=keep)
    spark.stop()
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
