"""
Adaptive Gold Finalize (Delta Lake) - Scale-aware aggregation pipeline

Delta Lake variant of gold_finalize.py. Uses Delta write APIs instead of
Iceberg's DataFrameWriterV2 and DESCRIBE DETAIL instead of $files metadata.

Selects the aggregation strategy from the silver size:
- SIMPLE_AGG: < 500GB, standard single-pass aggregation
- TWO_PHASE_AGG: 500GB - 10TB, repartition by day then aggregate
- INCREMENTAL: cycles 2+ of a multi-cycle run only (LB_GOLD_INCREMENTAL);
  never chosen from table size or from a gold table that already has rows
"""

from __future__ import annotations

from common import (
    env,
    log,
    run_c360_gold_finalize,
    table_exists,
    write_delta_table,
)


def get_table_size_gb(spark, table_name: str) -> float:
    """Approximate size of a Delta table in GB from DESCRIBE DETAIL."""
    try:
        detail_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
        total_bytes = detail_df.select("sizeInBytes").collect()[0][0]
        return (total_bytes or 0) / (1024**3)
    except Exception as e:
        log(f"Warning: Could not get table size: {e}")
        return 0.0


def _delta_write_props() -> dict[str, str]:
    return {
        "delta.logRetentionDuration": "interval 30 days",
        "delta.deletedFileRetentionDuration": "interval 7 days",
    }


def write_gold(spark, df, gold_tbl, how) -> None:
    """Write *df* to the Delta gold table. ``how`` is ``replace`` (full
    rebuild), ``create`` (first incremental cycle) or ``overwrite`` (an
    existing table rewritten whole).

    Delta avoids REPLACE TABLE AS SELECT (Unity's UCSingleCatalog lacks it):
    an existing table is overwritten, a new one is created by appending.
    ``common.table_exists`` reports a genuine not-found as False and raises
    any other error, so a transient catalog failure cannot pick append and
    duplicate gold rows.
    """
    gold_bucket = env("LB_GOLD_URI", "s3a://lb-gold/")
    if how == "overwrite":
        write_delta_table(
            spark, df, gold_tbl, gold_bucket, mode="overwrite", options=_delta_write_props()
        )
        return
    opts = {"overwriteSchema": "true", "compression": "snappy"}
    opts.update(_delta_write_props())
    mode = "overwrite" if table_exists(spark, gold_tbl) else "append"
    write_delta_table(spark, df, gold_tbl, gold_bucket, mode=mode, options=opts)


def main() -> None:
    """Gold finalize. A function so a test can run it in-process; the Spark
    Operator runs this file as ``__main__``."""
    run_c360_gold_finalize(
        title="Customer 360 Gold Finalize (Delta)",
        app_name="lb-gold-finalize-delta",
        namespace=("Delta", "schema"),
        size_fn=get_table_size_gb,
        write=write_gold,
    )


if __name__ == "__main__":
    main()
