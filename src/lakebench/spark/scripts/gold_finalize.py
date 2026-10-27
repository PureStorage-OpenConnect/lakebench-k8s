"""
Adaptive Gold Finalize - Scale-aware aggregation pipeline

Selects the aggregation strategy from the silver size:
- SIMPLE_AGG: < 500GB, standard single-pass aggregation
- TWO_PHASE_AGG: 500GB - 10TB, repartition by day then aggregate
- INCREMENTAL: cycles 2+ of a multi-cycle run only (LB_GOLD_INCREMENTAL);
  never chosen from table size or from a gold table that already has rows
"""

from common import (
    METADATA_DELETE_AFTER_COMMIT,
    METADATA_PREVIOUS_VERSIONS_MAX,
    log,
    run_c360_gold_finalize,
)


def get_table_size_gb(spark, table_name: str) -> float:
    """Approximate size of an Iceberg table in GB from its ``files`` metadata."""
    from pyspark.sql.functions import sum as sum_

    try:
        files_df = spark.sql(f"SELECT file_size_in_bytes FROM {table_name}.files")
        total_bytes = files_df.agg(sum_("file_size_in_bytes")).collect()[0][0]
        return (total_bytes or 0) / (1024**3)
    except Exception as e:
        log(f"Warning: Could not get table size: {e}")
        return 0.0


def write_gold(spark, df, gold_tbl, how) -> None:
    """Write *df* to the Iceberg gold table. ``how`` is ``replace`` (full
    rebuild), ``create`` (first incremental cycle) or ``overwrite`` (an
    existing table rewritten whole)."""
    from pyspark.sql.functions import lit

    if how == "overwrite":
        df.writeTo(gold_tbl).overwrite(lit(True))
        return
    writer = (
        df.writeTo(gold_tbl)
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "snappy")
        .tableProperty(*METADATA_DELETE_AFTER_COMMIT)
        .tableProperty(*METADATA_PREVIOUS_VERSIONS_MAX)
        .tableProperty("write.target-file-size-bytes", "134217728")  # 128MB target
    )
    if how == "create":
        writer.create()
    else:
        writer.createOrReplace()


def main() -> None:
    """Gold finalize. A function so a test can run it in-process; the Spark
    Operator runs this file as ``__main__``."""
    run_c360_gold_finalize(
        title="Customer 360 Gold Finalize",
        app_name="lb-gold-finalize",
        namespace=("Iceberg", "namespace"),
        size_fn=get_table_size_gb,
        write=write_gold,
    )


if __name__ == "__main__":
    main()
