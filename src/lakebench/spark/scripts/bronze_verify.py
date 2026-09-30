"""Bronze Verify - Validates data from the bronze bucket.

Fails the job (exit 1) when bronze could not produce a meaningful silver:
no rows, a missing or wrongly typed column that silver or gold computes on,
a key column that is entirely null, or no row that survives the silver
quality filter. These used to be warnings or not checked at all, so an empty
or schema-broken bronze passed and the run reported success on no data
(LB-044 class).

LB_CONTINUOUS_RESET=1 turns the job into the continuous preflight instead
(LB-142): it drops the tables the continuous jobs write (bronze_raw, silver,
gold) with their data, and verifies nothing. A batch run leaves silver and
gold full, and silver-stream rightly refuses to start a fresh checkpoint over
a full table. The CLI deletes the stream checkpoints before submitting this
job, so tables and checkpoints are cleared together. The raw landing zone is
never touched here; the CLI clears it only when the run generates new data.
"""

from __future__ import annotations

import time

from common import (
    _s3_table_path,
    c360_bronze_run_path,
    clear_unregistered_table_dirs,
    env,
    log,
    path_size_gb,
    pipeline_catalog,
    pipeline_table,
    reset_stream_tables,
    set_utc_session,
    write_bronze_data_clock,
)

_INT = ("tinyint", "smallint", "int", "bigint")
_NUM = _INT + ("float", "double", "decimal")
_TS = ("timestamp", "timestamp_ntz")
_STR = ("string",)
_BOOL = ("boolean",)

# Columns silver_build or gold_finalize compute on, with the Spark type
# families accepted for each. Missing or mistyped fails the job.
REQUIRED_COLUMNS = {
    "event_timestamp": _TS,
    "customer_id": _INT,
    "session_id": _STR,
    "email_raw": _STR,
    "phone_raw": _STR,
    "interaction_type": _STR,
    "transaction_amount": _NUM,
    "channel": _STR,
    "device_type": _STR,
    "browser": _STR,
    "city_raw": _STR,
    "state_raw": _STR,
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
}

# Columns silver only carries through. ``schema: custom`` also runs this job,
# so a missing or retyped one is a warning, not a failure.
PASSTHROUGH_COLUMNS = {
    "id": _INT,
    "row_id": _INT,
    "event_id": _STR,
    "product_id": _STR,
    "product_category": _STR,
    "currency": _STR,
    "ip_address": _STR,
    "zip_code": _STR,
    "interaction_payload": _STR,
}

# Never null in datagen output. Entirely null means the corpus is broken;
# partly null is reported as a warning.
KEY_COLUMNS = ("event_timestamp", "customer_id", "interaction_type", "channel", "data_quality_flag")


def _family_ok(simple_type, allowed):
    return any(simple_type == a or simple_type.startswith(a + "(") for a in allowed)


def _column_issues(types, columns, label):
    issues = []
    missing = [c for c in columns if c not in types]
    if missing:
        issues.append(f"missing {label} columns: {missing}")
    for name, allowed in columns.items():
        t = types.get(name)
        if t is not None and not _family_ok(t, allowed):
            issues.append(f"column {name} has type {t}, expected one of {list(allowed)}")
    return issues


def schema_problems(schema):
    """Missing or wrongly typed required columns, as messages."""
    types = {f.name: f.dataType.simpleString() for f in schema.fields}
    return _column_issues(types, REQUIRED_COLUMNS, "required")


def schema_warnings(schema):
    """Missing or retyped pass-through columns, as messages."""
    types = {f.name: f.dataType.simpleString() for f in schema.fields}
    return _column_issues(types, PASSTHROUGH_COLUMNS, "pass-through")


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
    warnings = schema_warnings(df.schema)
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


def continuous_reset_targets():
    """(tables, owned bucket URIs, raw landing zone) for the continuous reset.

    Tables are named in the catalog every continuous writer uses
    (common.pipeline_catalog): the recipe's catalog for Iceberg,
    spark_catalog for Delta + Hive.
    """
    bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")
    tables = [
        pipeline_table("LB_BRONZE_TABLE", "default.bronze_raw"),
        pipeline_table("LB_SILVER_TABLE", "silver.customer_interactions_enriched"),
        pipeline_table("LB_GOLD_TABLE", "gold.customer_executive_dashboard"),
    ]
    owned = [
        bronze_uri,
        env("LB_SILVER_URI", "s3a://lb-silver/"),
        env("LB_GOLD_URI", "s3a://lb-gold/"),
    ]
    return tables, owned, bronze_uri + "customer/interactions/"


def continuous_reset_explicit_locations():
    """(table, location) for continuous tables created at an explicit path,
    whose files a DROP leaves: the Delta + Hive bronze table
    (bronze_ingest_delta.bronze_target). spark_catalog as the pipeline
    catalog means Delta + Hive (job.py)."""
    if pipeline_catalog() != "spark_catalog":
        return []
    bronze_table = env("LB_BRONZE_TABLE", "default.bronze_raw")
    return [
        (
            pipeline_table("LB_BRONZE_TABLE", "default.bronze_raw"),
            _s3_table_path(env("LB_BRONZE_URI", "s3a://lb-bronze/"), bronze_table),
        )
    ]


def main() -> None:
    from pyspark.sql import SparkSession

    bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")
    # This run's cycles only in a multi-cycle run (the files silver holds).
    source = c360_bronze_run_path(bronze_uri)

    spark = SparkSession.builder.appName("lb-bronze-verify").getOrCreate()
    set_utc_session(spark)
    start_time = time.time()

    if env("LB_CONTINUOUS_RESET", "0") == "1":
        tables, owned, raw = continuous_reset_targets()
        log("=" * 60)
        log("Continuous reset (no verification)")
        log("=" * 60)
        dropped = reset_stream_tables(spark, tables, owned_uris=owned, keep_uris=[raw])
        clear_unregistered_table_dirs(
            spark, continuous_reset_explicit_locations(), owned_uris=owned, keep_uris=[raw]
        )
        log(f"Continuous reset complete: {len(dropped)} of {len(tables)} tables dropped")
        # No JOB METRICS block: this is not a bronze-verify stage.
        spark.stop()
        return

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
        # Read by lakebench.metrics.c360_correctness: bronze rows silver must hold.
        log(f"[c360-bronze] rows={row_count} silver_filter_rows={stats['silver_rows']}")
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
    df.select("customer_id", "interaction_type", "channel", "city_raw").limit(5).show(
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

    # C2 (silver-plan): record the bronze-side data clock so silver's env
    # builder (job.py._build_env_vars) can resolve LB_DATA_CLOCK to the
    # newest bronze event date rather than falling all the way to today.
    # ``ts_max`` is the max(event_timestamp) verify_bronze already computed
    # in the same aggregation pass; no extra Spark scan.
    write_bronze_data_clock(
        env("LAKEBENCH_NAMESPACE", ""),
        stats.get("ts_max") if isinstance(stats, dict) else None,
    )

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
