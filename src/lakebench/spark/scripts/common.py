"""Common utilities for Spark pipeline scripts.

Bridge module: provides lakebench helper functions (log, env) and
shared transformation logic used by both batch and streaming scripts.

Environment variables set by lakebench job.py:
  BRONZE_BUCKET, SILVER_BUCKET, GOLD_BUCKET, CATALOG_NAME,
  LB_BRONZE_URI, LB_SILVER_URI, LB_ICEBERG_CATALOG, LB_CATALOG_TYPE
"""

import os
from datetime import datetime


def log(msg):
    """Timestamped log line."""
    print(f"[lb] {datetime.utcnow().isoformat()} - {msg}", flush=True)


def env(name, default=None):
    """Read required env var."""
    v = os.getenv(name, default)
    if v is None:
        raise SystemExit(f"Missing env var: {name}")
    return v


def one_line(text, limit=200):
    """Collapse whitespace so a value stays on one log line.

    The driver-log parser reads per-rule status one line at a time; Spark
    exception messages are usually multi-line, and a rule whose error text
    spilled onto the next line vanished from rule_errors entirely instead of
    being reported as an error.
    """
    return " ".join(str(text).split())[:limit]


def table_exists(spark, table_name):
    """True if a catalog table exists, False only if it definitely does not.

    Use this, not DeltaTable.isDeltaTable, for catalog names: isDeltaTable's
    identifier is a FILE PATH, so "catalog.schema.table" is always False
    (Delta docs). Callers branch to create-with-overwrite on False, so any
    error other than a genuine not-found is re-raised: treating a transient
    catalog failure as "missing" would overwrite a live table.
    """
    try:
        spark.table(table_name).schema  # noqa: B018 -- forces resolution
        return True
    except Exception as e:  # noqa: BLE001
        text = str(e)
        if (
            "TABLE_OR_VIEW_NOT_FOUND" in text
            or "Table or view not found" in text
            or "NoSuchTableException" in type(e).__name__
            or "SCHEMA_NOT_FOUND" in text
        ):
            return False
        raise


def ensure_column(spark, fq_table, name, sql_type):
    """Add a nullable column to an existing table when it is missing.

    Reads the live schema first: Spark has no ``ADD COLUMN IF NOT EXISTS``
    for columns (that clause is for partitions), so the unconditional form
    failed with a parse error on every run and was silently swallowed. For a
    reused catalog whose table predates the column, that left the next write
    to fail on a schema mismatch. Returns True when the column was added.
    """
    if name in spark.table(fq_table).columns:
        return False
    spark.sql(f"ALTER TABLE {fq_table} ADD COLUMNS ({name} {sql_type})")
    log(f"[startup] added {name} to {fq_table} (reused-catalog upgrade)")
    return True


def path_size_gb(spark, uri):
    """Total bytes under a Hadoop-FS path, in GiB; 0.0 if it cannot be measured."""
    try:
        jvm = spark._jvm
        hconf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(uri), hconf)
        path = jvm.org.apache.hadoop.fs.Path(uri)
        if not fs.exists(path):
            return 0.0
        return fs.getContentSummary(path).getLength() / (1024**3)
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] size of {uri} unavailable: {one_line(e)}")
        return 0.0


def iceberg_table_stats(spark, fq_table):
    """(row_count, size_gb) of an Iceberg table from its ``data_files`` metadata.

    Reads manifest metadata only, so it costs no data scan. ``data_files``
    rather than ``files``: the latter includes delete files, which would
    overcount a merge-on-read table. Returns (0, 0.0)
    when the metadata table is unavailable (non-Iceberg table, catalog
    error); the collector treats zero input as unmeasured.
    """
    try:
        r = spark.sql(
            f"SELECT COALESCE(SUM(record_count), 0) AS n, "
            f"COALESCE(SUM(file_size_in_bytes), 0) AS b FROM {fq_table}.data_files"
        ).collect()[0]
        return int(r["n"]), float(r["b"]) / (1024**3)
    except Exception as e:  # noqa: BLE001
        log(f"[metrics] table stats for {fq_table} unavailable: {one_line(e)}")
        return 0, 0.0


def log_job_metrics(job, *, input_size_gb, input_rows, output_rows, elapsed_seconds):
    """Emit the ``=== JOB METRICS ===`` block the metrics collector parses."""
    log(f"=== JOB METRICS: {job} ===")
    log(f"input_size_gb: {input_size_gb:.3f}")
    log(f"input_rows: {int(input_rows)}")
    log(f"output_rows: {int(output_rows)}")
    log(f"elapsed_seconds: {elapsed_seconds:.1f}")
    log("=" * 60)


def stream_batch_lines(batch_id, rows, seconds, table):
    """Per-micro-batch lines in the format ``parse_streaming_logs`` reads.

    Empty batches produce nothing, matching c360's bronze_ingest: an idle
    trigger is not a processed batch.
    """
    if rows <= 0:
        return []
    return [
        f"Batch {batch_id}: writing {rows:,} rows to {table}",
        f"Batch {batch_id}: committed in {seconds:.1f}s",
    ]


def parse_size_gb(s):
    """Parse size string to GB."""
    return float(s)


# ============================================================
# Shared transformation logic (used by both batch and streaming)
# ============================================================


def apply_silver_transformations(df_bronze):
    """Apply Silver layer transformations - cleaning, standardization, enrichment.

    Pure column-level transforms: no joins, no shuffles. Each row is
    processed independently. Shared between silver_build.py (batch) and
    silver_stream.py (streaming via foreachBatch).
    """
    from pyspark.sql.functions import (
        col,
        concat_ws,
        current_timestamp,
        expr,
        lower,
        regexp_replace,
        to_date,
        trim,
        upper,
        when,
    )

    return (
        df_bronze
        # Light filtering - only remove truly bad data (~2% removed)
        .filter(col("data_quality_flag") != "duplicate_suspected")
        # === STANDARDIZED/CLEANED VERSIONS (keep originals) ===
        .withColumn(
            "email_clean", regexp_replace(lower(trim(col("email_raw"))), "\\.duplicate", "")
        )
        .withColumn(
            "phone_clean",
            regexp_replace(regexp_replace(col("phone_raw"), "[^0-9]", ""), "^1?(\\d{10})$", "+1$1"),
        )
        # Geographic standardization
        .withColumn(
            "state_standardized",
            when(upper(col("state_raw")).isin("CA", "CALIFORNIA"), "CA")
            .when(upper(col("state_raw")).isin("TX", "TEXAS"), "TX")
            .when(upper(col("state_raw")).isin("NY", "NEW YORK"), "NY")
            .when(upper(col("state_raw")) == "FL", "FL")
            .otherwise(upper(col("state_raw"))),
        )
        .withColumn(
            "city_standardized",
            when(upper(col("city_raw")).isin("NEW YORK", "NYC"), "New York")
            .when(upper(col("city_raw")) == "LA", "Los Angeles")
            .otherwise(col("city_raw")),
        )
        # === DERIVED TIME DIMENSIONS ===
        .withColumn("interaction_date", to_date(col("event_timestamp")))
        .withColumn("interaction_hour", expr("hour(event_timestamp)"))
        .withColumn("interaction_day_of_week", expr("dayofweek(event_timestamp)"))
        .withColumn("interaction_week_of_year", expr("weekofyear(event_timestamp)"))
        .withColumn("interaction_month", expr("month(event_timestamp)"))
        .withColumn("interaction_year", expr("year(event_timestamp)"))
        .withColumn("is_weekend", expr("dayofweek(event_timestamp) in (1, 7)"))
        .withColumn("is_business_hours", expr("hour(event_timestamp) between 9 and 17"))
        .withColumn(
            "is_peak_hours",
            expr("""
                hour(event_timestamp) between 12 and 14 or
                hour(event_timestamp) between 18 and 20
            """),
        )
        # === CUSTOMER VALUE SEGMENTATION ===
        .withColumn(
            "customer_value_tier",
            when(col("transaction_amount") > 500, "high_value")
            .when(col("transaction_amount") > 100, "medium_value")
            .when(col("transaction_amount") > 0, "low_value")
            .otherwise("browser_only"),
        )
        .withColumn(
            "transaction_size_category",
            when(col("transaction_amount") > 1000, "large")
            .when(col("transaction_amount") > 250, "medium")
            .when(col("transaction_amount") > 0, "small")
            .otherwise("none"),
        )
        # === BEHAVIORAL ANALYTICS ===
        .withColumn(
            "engagement_score",
            expr("""
                case when page_views = 0 then 0
                     when page_views <= 2 then 1
                     when page_views <= 5 then 2
                     when page_views <= 10 then 3
                     else 4 end
            """),
        )
        .withColumn(
            "session_depth_category",
            when(col("page_views") > 10, "deep")
            .when(col("page_views") > 3, "medium")
            .when(col("page_views") > 0, "shallow")
            .otherwise("bounce"),
        )
        .withColumn(
            "time_spent_category",
            when(col("time_on_site_seconds") > 1800, "long")
            .when(col("time_on_site_seconds") > 300, "medium")
            .when(col("time_on_site_seconds") > 0, "short")
            .otherwise("none"),
        )
        .withColumn(
            "channel_preference",
            when(col("channel") == "mobile_app", "mobile_first")
            .when(col("channel") == "web", "web_first")
            .when(col("channel") == "store", "physical_first")
            .otherwise("omnichannel"),
        )
        # === ADVANCED ANALYTICS (ML Features) ===
        .withColumn(
            "lifetime_value_estimate",
            expr("round(transaction_amount * (1 + points_earned/1000.0), 2)"),
        )
        .withColumn(
            "customer_recency_score",
            expr("30 - datediff(current_date(), to_date(event_timestamp))"),
        )
        .withColumn(
            "engagement_velocity",
            expr("round(page_views / greatest(time_on_site_seconds/60.0, 1.0), 4)"),
        )
        .withColumn(
            "churn_risk_indicator",
            when(col("satisfaction_score") <= 2, "high_risk")
            .when(col("satisfaction_score") <= 3, "medium_risk")
            .when(col("satisfaction_score").isNull(), "unknown_risk")
            .otherwise("low_risk"),
        )
        # === MARKETING ATTRIBUTION ===
        .withColumn(
            "attribution_channel",
            when(col("utm_source").isNotNull(), col("utm_source")).otherwise("direct"),
        )
        .withColumn(
            "attribution_quality",
            when(col("utm_source").isNotNull() & col("utm_medium").isNotNull(), "high")
            .when(col("utm_source").isNotNull(), "medium")
            .otherwise("low"),
        )
        .withColumn(
            "customer_journey_stage",
            when(col("interaction_type") == "browse", "awareness")
            .when(col("interaction_type") == "abandoned_cart", "consideration")
            .when(col("interaction_type") == "purchase", "conversion")
            .when(col("interaction_type") == "support", "retention")
            .otherwise("other"),
        )
        # === DEVICE CONTEXT ===
        .withColumn(
            "device_category",
            when(col("device_type") == "mobile", "mobile")
            .when(col("device_type") == "tablet", "tablet")
            .otherwise("desktop"),
        )
        .withColumn(
            "browser_family",
            when(col("browser").isin("chrome", "edge"), "chromium")
            .when(col("browser") == "safari", "webkit")
            .when(col("browser") == "firefox", "gecko")
            .otherwise("other"),
        )
        # === COMPOSITE FEATURES ===
        .withColumn(
            "interaction_context",
            concat_ws("|", col("device_type"), col("browser"), col("channel")),
        )
        .withColumn(
            "customer_segment_key",
            concat_ws(
                ":", col("customer_value_tier"), col("channel_preference"), col("loyalty_tier")
            ),
        )
        # === DATA LINEAGE ===
        .withColumn("silver_processing_timestamp", current_timestamp())
        .withColumn(
            "data_quality_score",
            when(col("data_quality_flag") == "clean", 1.0)
            .when(col("data_quality_flag") == "format_inconsistent", 0.8)
            .when(col("data_quality_flag") == "incomplete_data", 0.6)
            .otherwise(0.5),
        )
    )


def set_utc_session(spark):
    """Pin the Spark session time zone to UTC.

    Datagen writes ``event_timestamp`` as a UTC-adjusted instant, and every
    derived calendar field (``interaction_date``, hour, weekday, week, month,
    year) is computed in the session time zone. Left at the JVM default, the
    same data lands on different dates on a pod whose TZ is not UTC.
    """
    spark.conf.set("spark.sql.session.timeZone", "UTC")


def data_clock_date(df, ts_col="event_timestamp"):
    """Latest event date in ``df`` (UTC), or None when it has no timestamps.

    A full scan of one column. The fallback data clock when LB_DATA_CLOCK is
    not set (see ``resolve_data_clock``). Call with the session already
    pinned to UTC (``set_utc_session``).
    """
    from pyspark.sql.functions import col, to_date
    from pyspark.sql.functions import max as max_

    return df.agg(max_(to_date(col(ts_col))).alias("d")).collect()[0]["d"]


def configured_data_clock(value=None):
    """Last day of the configured datagen window, or None when unset.

    LB_DATA_CLOCK is ``datagen.timestamp_end`` (job.py). Datagen treats the
    end as exclusive, so the newest possible event date is the day before.
    """
    from datetime import date, timedelta

    raw = os.getenv("LB_DATA_CLOCK", "") if value is None else value
    raw = raw.strip()
    if not raw:
        return None
    return date.fromisoformat(raw[:10]) - timedelta(days=1)


def resolve_data_clock(df_fallback=None):
    """The c360 data clock: the date recency is measured from.

    One clock for the whole run: from LB_DATA_CLOCK when set, so batch,
    every multi-cycle cycle and every micro-batch use the same anchor and a
    rerun reproduces the same scores (GOALS P4.2). Only when it is unset is
    it measured as max(event date) of ``df_fallback``. Logs which was used.
    """
    anchor = configured_data_clock()
    if anchor is not None:
        log(f"Data clock (recency anchor): {anchor} from LB_DATA_CLOCK")
        return anchor
    if df_fallback is None:
        log("Data clock: LB_DATA_CLOCK unset and no data to measure; recency is NULL")
        return None
    anchor = data_clock_date(df_fallback)
    log(f"Data clock (recency anchor): {anchor} from max(event_timestamp); LB_DATA_CLOCK unset")
    return anchor


def apply_silver_transformations_anchored(df_bronze, anchor_date):
    """``apply_silver_transformations`` with recency anchored to the data clock.

    ``customer_recency_score`` is ``30 - days between the event date and
    anchor_date``: 30 for an event on the newest day of the data, 0 for one
    30 days older, negative beyond. The shared transform measures from
    ``current_date()``, which made the score a function of the run date
    (CLAUDE.md gotcha 17). ``anchor_date`` is a ``datetime.date``; when it is
    None (no timestamps at all) the score is NULL rather than run-dated.
    """
    from pyspark.sql.functions import col, datediff, lit

    out = apply_silver_transformations(df_bronze)
    if anchor_date is None:
        score = lit(None).cast("int")
    else:
        score = lit(30) - datediff(lit(anchor_date), col("interaction_date"))
    return out.withColumn("customer_recency_score", score)


def streaming_query_id(spark):
    """Id of the streaming query running the current ``foreachBatch`` call.

    Stable across driver restarts from the same checkpoint and new for a
    fresh checkpoint, so it scopes a batch id to one logical stream. Spark
    sets it as a local property on the micro-batch thread. Raises when it is
    absent: an idempotency key without it would be unsound.
    """
    qid = spark.sparkContext.getLocalProperty("sql.streaming.queryId")
    if not qid:
        raise RuntimeError("sql.streaming.queryId is not set; call from inside foreachBatch")
    return qid


def delta_idempotent_options(spark, app, batch_id):
    """Delta writer options that make a ``foreachBatch`` write exactly-once.

    Delta records (txnAppId, txnVersion) in the commit and skips any later
    write whose txnVersion is not greater than the recorded one, so a
    micro-batch replayed after a driver restart commits nothing the second
    time. The app id includes the streaming query id: with a fixed app id a
    fresh checkpoint (batch ids restart at 0) would have every write skipped,
    a silent zero-row run.
    """
    return {
        "txnAppId": f"{app}-{streaming_query_id(spark)}",
        "txnVersion": str(int(batch_id)),
    }


def stream_run_id(spark):
    """Run id of the streaming query in the current ``foreachBatch`` call.

    A new run id every time a query starts, including a restart from the
    same checkpoint. Spark uses it as the job group of the micro-batch
    thread. None when it cannot be read.
    """
    return spark.sparkContext.getLocalProperty("spark.jobGroup.id") or None


_RUNS_STARTED = set()


def replay_possible(spark):
    """True for the first micro-batch of each query run, False after.

    A batch that fails stops its query and the driver exits (``await_stream``),
    so only the first batch a run executes can repeat a batch an earlier run
    committed. Writers do their replay check (a DELETE, a snapshot or version
    lookup) only then instead of on every batch. When the run id is not
    readable every batch is treated as a possible replay: slower, never wrong.
    """
    run = stream_run_id(spark)
    if run is None:
        return True
    if run in _RUNS_STARTED:
        return False
    _RUNS_STARTED.add(run)
    return True


def delta_table_version(spark, fq_table):
    """Latest commit version of a Delta table."""
    return int(spark.sql(f"DESCRIBE HISTORY {fq_table} LIMIT 1").collect()[0]["version"])


def checkpoint_is_fresh(spark, checkpoint_location):
    """True when a streaming checkpoint has no committed offsets yet."""
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    offsets = jvm.org.apache.hadoop.fs.Path(checkpoint_location.rstrip("/") + "/offsets")
    fs = offsets.getFileSystem(hconf)
    return not fs.exists(offsets) or len(fs.listStatus(offsets)) == 0


def refuse_fresh_checkpoint_over_data(spark, checkpoint_location, fq_table):
    """Exit when a fresh checkpoint would re-read the source into a full table.

    A new checkpoint starts the source from the beginning, so every row
    already in ``fq_table`` would be written a second time. That happens when
    checkpoints are deleted but tables are not. Clear both, or neither.
    """
    if not checkpoint_is_fresh(spark, checkpoint_location):
        return
    if not table_exists(spark, fq_table) or spark.table(fq_table).limit(1).count() == 0:
        return
    log(
        f"ERROR: checkpoint {checkpoint_location} is empty but {fq_table} already has "
        "rows; starting would re-read the whole source and duplicate them. Drop the "
        "table or restore the checkpoint."
    )
    raise SystemExit(1)


def await_stream(spark, query):
    """Block until ``query`` stops; re-raise its failure.

    SIGTERM and SIGINT only set a flag; the loop below stops the query, so
    the handler never calls into py4j while the main thread may be inside
    it. A query that died with an exception fails the driver, so the
    Kubernetes job reports the real outcome instead of a pass with no data
    (LB-044).
    """
    import signal
    import time

    stop = {"signal": None}

    def _shutdown_handler(signum, frame):  # noqa: ARG001
        stop["signal"] = signum

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _shutdown_handler)
        except ValueError:
            pass

    while query.isActive:
        if stop["signal"] is not None:
            log(f"Signal {stop['signal']} received; stopping stream cleanly")
            try:
                query.stop()
            except Exception as e:  # noqa: BLE001
                log(f"query.stop failed: {e}")
            break
        time.sleep(1)

    exc = query.exception()
    if exc is not None:
        log(f"Streaming query failed: {one_line(exc, 2000)}")
        spark.stop()
        raise exc
    log("Streaming query stopped")


def get_daily_kpi_aggregations():
    """Return the list of aggregation expressions for daily KPIs.

    Shared between gold_finalize.py (batch) and gold_refresh.py
    (streaming via foreachBatch). Produces 46 KPI columns.
    """
    from pyspark.sql.functions import (
        avg,
        col,
        countDistinct,
        when,
    )
    from pyspark.sql.functions import (
        max as max_,
    )
    from pyspark.sql.functions import (
        round as round_,
    )
    from pyspark.sql.functions import (
        sum as sum_,
    )

    return [
        # Customer metrics
        countDistinct("customer_id").alias("daily_active_customers"),
        countDistinct("email_clean").alias("unique_emails"),
        countDistinct("session_id").alias("total_sessions"),
        # Revenue metrics
        round_(sum_("transaction_amount"), 2).alias("total_daily_revenue"),
        round_(avg("transaction_amount"), 2).alias("avg_transaction_value"),
        round_(max_("transaction_amount"), 2).alias("largest_transaction"),
        sum_(when(col("transaction_amount") > 0, 1).otherwise(0)).alias("total_transactions"),
        # Channel revenue breakdown
        round_(
            sum_(when(col("channel") == "web", col("transaction_amount")).otherwise(0)), 2
        ).alias("web_revenue"),
        round_(
            sum_(when(col("channel") == "mobile_app", col("transaction_amount")).otherwise(0)), 2
        ).alias("mobile_revenue"),
        round_(
            sum_(when(col("channel") == "store", col("transaction_amount")).otherwise(0)), 2
        ).alias("store_revenue"),
        round_(
            sum_(when(col("channel") == "call_center", col("transaction_amount")).otherwise(0)), 2
        ).alias("call_center_revenue"),
        # Engagement metrics
        round_(avg("engagement_score"), 2).alias("avg_engagement_score"),
        round_(avg("time_on_site_seconds"), 0).alias("avg_time_on_site_seconds"),
        round_(avg("page_views"), 1).alias("avg_page_views"),
        # Conversion funnel
        sum_(when(col("customer_journey_stage") == "awareness", 1).otherwise(0)).alias(
            "awareness_interactions"
        ),
        sum_(when(col("customer_journey_stage") == "consideration", 1).otherwise(0)).alias(
            "consideration_interactions"
        ),
        sum_(when(col("customer_journey_stage") == "conversion", 1).otherwise(0)).alias(
            "conversions"
        ),
        sum_(when(col("customer_journey_stage") == "retention", 1).otherwise(0)).alias(
            "retention_interactions"
        ),
        # Loyalty metrics
        sum_(when(col("loyalty_member") == True, 1).otherwise(0)).alias(  # noqa: E712
            "loyalty_member_interactions"
        ),
        sum_("points_earned").alias("total_points_earned"),
        sum_("points_redeemed").alias("total_points_redeemed"),
        # Customer satisfaction
        countDistinct("support_ticket_id").alias("support_tickets_created"),
        round_(avg("satisfaction_score"), 2).alias("avg_satisfaction_score"),
        # Risk indicators
        sum_(when(col("churn_risk_indicator") == "high_risk", 1).otherwise(0)).alias(
            "high_churn_risk_count"
        ),
        sum_(when(col("churn_risk_indicator") == "medium_risk", 1).otherwise(0)).alias(
            "medium_churn_risk_count"
        ),
        # Value metrics
        round_(sum_("lifetime_value_estimate"), 2).alias("total_estimated_ltv"),
        round_(avg("lifetime_value_estimate"), 2).alias("avg_estimated_ltv"),
        # Channel distribution
        sum_(when(col("channel") == "web", 1).otherwise(0)).alias("web_interactions"),
        sum_(when(col("channel") == "mobile_app", 1).otherwise(0)).alias("mobile_interactions"),
        sum_(when(col("channel") == "store", 1).otherwise(0)).alias("store_interactions"),
    ]


# ---------------------------------------------------------------------------
# Delta table write helper -- handles managed vs EXTERNAL tables
# ---------------------------------------------------------------------------


def _is_unity_catalog():
    """Check if the current catalog is Unity (requires EXTERNAL table writes)."""
    return os.getenv("LB_CATALOG_TYPE", "hive") == "unity"


def _s3_table_path(bucket_uri, fq_table):
    """Build the S3 path for an EXTERNAL Delta table.

    Args:
        bucket_uri: S3A URI for the layer bucket (e.g. "s3a://lb-silver/")
        fq_table: Fully-qualified table name WITHOUT catalog prefix
                  (e.g. "silver.customer_interactions_enriched")

    Returns:
        S3 path like "s3a://lb-silver/warehouse/silver.db/customer_interactions_enriched"
    """
    parts = fq_table.split(".", 1)
    if len(parts) == 2:
        schema, table = parts
    else:
        schema, table = "default", fq_table
    return f"{bucket_uri.rstrip('/')}/warehouse/{schema}.db/{table}"


def write_delta_table(
    spark, df, fq_table, bucket_uri, mode="append", partition_cols=None, options=None
):
    """Write a DataFrame as a Delta table, handling managed vs EXTERNAL.

    When LB_CATALOG_TYPE is "unity", writes data directly to S3 via
    df.write.save(path) and registers the table with CREATE TABLE ...
    LOCATION. This bypasses Unity's STS credential vending which fails
    on non-AWS S3 (FlashBlade, MinIO).

    When LB_CATALOG_TYPE is "hive" (default), uses saveAsTable() which
    registers through the session catalog (DeltaCatalog over Hive).

    Args:
        spark: SparkSession
        df: DataFrame to write
        fq_table: Catalog-qualified table name (e.g. "lakehouse.silver.table")
        bucket_uri: S3A URI for the layer (e.g. "s3a://lb-silver/")
        mode: Write mode -- "append" or "overwrite"
        partition_cols: List of partition column names, or None
        options: Dict of writer options (e.g. Delta table properties)
    """
    options = options or {}
    writer = df.write.format("delta").mode(mode)
    for k, v in options.items():
        writer = writer.option(k, v)
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    if _is_unity_catalog():
        # EXTERNAL table path -- bypass credential vending
        # Strip catalog prefix to get schema.table for path construction
        parts = fq_table.split(".", 1)
        schema_table = parts[1] if len(parts) > 1 else fq_table
        table_path = _s3_table_path(bucket_uri, schema_table)

        log(f"Writing EXTERNAL Delta table to {table_path} (mode={mode})")
        writer.save(table_path)

        # Register table in Unity catalog (idempotent)
        partition_clause = ""
        if partition_cols:
            partition_clause = f" PARTITIONED BY ({', '.join(partition_cols)})"
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {fq_table} "
            f"USING DELTA{partition_clause} "
            f"LOCATION '{table_path}'"
        )
    else:
        # Managed table path -- saveAsTable registers via DeltaCatalog/Hive
        log(f"Writing managed Delta table {fq_table} (mode={mode})")
        writer.saveAsTable(fq_table)
