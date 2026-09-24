"""Bronze Verify - Validates data from the bronze bucket.

Fails the job (exit 1) when bronze could not produce a meaningful silver:
no rows, a missing or wrongly typed column that silver or gold computes on,
a key column that is entirely null, or no row that survives the silver
quality filter. These used to be warnings or not checked at all, so an empty
or schema-broken bronze passed and the run reported success on no data
(LB-044 class).
"""

from __future__ import annotations

import time

from common import env, log, path_size_gb, set_utc_session

_INT = ("tinyint", "smallint", "int", "bigint")
_NUM = _INT + ("float", "double", "decimal")
_TS = ("timestamp", "timestamp_ntz")
_STR = ("string",)
_BOOL = ("boolean",)

# Every column silver_build/gold_finalize read, plus the identifiers the
# benchmark relies on, with the Spark type families accepted for each.
REQUIRED_COLUMNS = {
    "id": _INT,
    "row_id": _INT,
    "event_timestamp": _TS,
    "event_id": _STR,
    "session_id": _STR,
    "customer_id": _INT,
    "email_raw": _STR,
    "phone_raw": _STR,
    "interaction_type": _STR,
    "product_id": _STR,
    "product_category": _STR,
    "transaction_amount": _NUM,
    "currency": _STR,
    "channel": _STR,
    "device_type": _STR,
    "browser": _STR,
    "ip_address": _STR,
    "city_raw": _STR,
    "state_raw": _STR,
    "zip_code": _STR,
    "page_views": _INT,
    "time_on_site_seconds": _INT,
    "support_ticket_id": _STR,
    "satisfaction_score": _INT,
    "utm_source": _STR,
    "utm_medium": _STR,
    "loyalty_member": _BOOL,
    "loyalty_tier": _STR,
    "points_earned": _INT,
    "points_redeemed": _INT,
    "data_quality_flag": _STR,
    "interaction_payload": _STR,
}

# Never null in datagen output. Entirely null means the corpus is broken;
# partly null is reported as a warning.
KEY_COLUMNS = ("event_timestamp", "customer_id", "interaction_type", "channel", "data_quality_flag")


def _family_ok(simple_type, allowed):
    return any(simple_type == a or simple_type.startswith(a + "(") for a in allowed)


def schema_problems(schema):
    """Missing or wrongly typed required columns, as messages."""
    types = {f.name: f.dataType.simpleString() for f in schema.fields}
    problems = []
    missing = [c for c in REQUIRED_COLUMNS if c not in types]
    if missing:
        problems.append(f"missing required columns: {missing}")
    for name, allowed in REQUIRED_COLUMNS.items():
        t = types.get(name)
        if t is not None and not _family_ok(t, allowed):
            problems.append(f"column {name} has type {t}, expected one of {list(allowed)}")
    return problems


def verify_bronze(df):
    """Check a bronze DataFrame. Returns (stats, problems, warnings).

    ``problems`` fail the job; ``warnings`` are logged. One aggregation pass
    computes the row count, per-key non-null counts, the rows silver keeps
    and the event-time range.
    """
    from pyspark.sql.functions import col, count, lit, when
    from pyspark.sql.functions import max as max_
    from pyspark.sql.functions import min as min_
    from pyspark.sql.functions import sum as sum_

    problems = schema_problems(df.schema)
    warnings = []
    present = set(df.columns)

    aggs = [count(lit(1)).alias("rows")]
    keys = [k for k in KEY_COLUMNS if k in present]
    aggs += [count(col(k)).alias(f"nn_{k}") for k in keys]
    if "data_quality_flag" in present:
        # Mirrors the silver filter (data_quality_flag != 'duplicate_suspected';
        # a NULL flag is dropped too).
        keep = col("data_quality_flag") != "duplicate_suspected"
        aggs.append(sum_(when(keep, 1).otherwise(0)).alias("silver_rows"))
    ts_ok = "event_timestamp" in present and not any("event_timestamp" in p for p in problems)
    if ts_ok:
        aggs += [min_("event_timestamp").alias("ts_min"), max_("event_timestamp").alias("ts_max")]
    r = df.agg(*aggs).collect()[0]

    rows = int(r["rows"])
    stats = {"rows": rows}
    if ts_ok:
        stats["ts_min"], stats["ts_max"] = r["ts_min"], r["ts_max"]
    if rows == 0:
        problems.append("bronze has 0 rows")
        return stats, problems, warnings

    for k in keys:
        nn = int(r[f"nn_{k}"])
        if nn == 0:
            problems.append(f"key column {k} is null in every row")
        elif nn < rows:
            warnings.append(f"key column {k} is null in {rows - nn:,} of {rows:,} rows")
    if "silver_rows" in r.asDict():
        stats["silver_rows"] = int(r["silver_rows"] or 0)
        if stats["silver_rows"] == 0:
            problems.append("no row survives the silver quality filter")
    return stats, problems, warnings


def main() -> None:
    from pyspark.sql import SparkSession

    bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")
    source = bronze_uri + "customer/interactions/"

    spark = SparkSession.builder.appName("lb-bronze-verify").getOrCreate()
    set_utc_session(spark)
    start_time = time.time()

    log("=" * 60)
    log("Bronze Data Verification")
    log("=" * 60)
    log(f"Reading from: {source}")

    try:
        df = spark.read.parquet(source)
    except Exception as e:
        log(f"ERROR: Cannot read Bronze data: {str(e)}")
        spark.stop()
        raise SystemExit(1)  # noqa: B904

    stats, problems, warnings = verify_bronze(df)
    row_count = stats["rows"]

    log("=" * 60)
    log("DATA SUMMARY")
    log("=" * 60)
    log(f"Rows: {row_count:,}")
    log(f"Columns: {len(df.columns)}")
    if "ts_min" in stats:
        log(f"Event time range (UTC): {stats['ts_min']} .. {stats['ts_max']}")
    if "silver_rows" in stats:
        log(f"Rows passing the silver quality filter: {stats['silver_rows']:,}")
    log("Schema:")
    for field in df.schema.fields:
        log(f"  - {field.name} ({field.dataType.simpleString()})")
    for w in warnings:
        log(f"WARNING: {w}")

    if problems:
        for p in problems:
            log(f"ERROR: {p}")
        log("Bronze Verification FAILED: bronze cannot produce a valid silver")
        spark.stop()
        raise SystemExit(1)
    log("All required columns present with expected types")

    log("=" * 60)
    log("SAMPLE DATA (5 rows)")
    log("=" * 60)
    df.select("event_id", "customer_id", "interaction_type", "channel", "city_raw").limit(5).show(
        truncate=False
    )

    log("=" * 60)
    log("VALUE DISTRIBUTIONS")
    log("=" * 60)
    log("Interaction types:")
    df.groupBy("interaction_type").count().orderBy("count", ascending=False).show()
    log("Channels:")
    df.groupBy("channel").count().orderBy("count", ascending=False).show()

    total_time = time.time() - start_time
    input_size_gb = path_size_gb(spark, source)

    log("=" * 60)
    log("Bronze Verification COMPLETED")
    log("=" * 60)
    log(f"Total rows verified: {row_count:,}")
    log(f"Total time: {total_time:.1f}s ({total_time / 60:.1f} min)")
    log("=== JOB METRICS: bronze-verify ===")
    log(f"input_size_gb: {input_size_gb:.3f}")
    log(f"estimated_rows: {row_count}")
    log(f"output_rows: {row_count}")
    log(f"elapsed_seconds: {total_time:.1f}")
    log("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
