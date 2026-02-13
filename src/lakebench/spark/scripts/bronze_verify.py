"""Bronze Verify - Validates data from the bronze bucket."""

import time

from common import env, log
from pyspark.sql import SparkSession

bronze_uri = env("LB_BRONZE_URI", "s3a://lb-bronze/")

spark = SparkSession.builder.appName("lb-bronze-verify").getOrCreate()
start_time = time.time()

log("=" * 60)
log("Bronze Data Verification")
log("=" * 60)
log(f"Reading from: {bronze_uri}customer/interactions/")

try:
    df = spark.read.parquet(bronze_uri + "customer/interactions/")
except Exception as e:
    log(f"ERROR: Cannot read Bronze data: {str(e)}")
    spark.stop()
    raise SystemExit(1)  # noqa: B904

row_count = df.count()
col_count = len(df.columns)

log("=" * 60)
log("DATA SUMMARY")
log("=" * 60)
log(f"Rows: {row_count:,}")
log(f"Columns: {col_count}")
log("Schema:")
for field in df.schema.fields:
    log(f"  - {field.name} ({field.dataType.simpleString()})")

# Verify expected columns exist
expected_cols = [
    "id",
    "row_id",
    "event_timestamp",
    "event_id",
    "session_id",
    "customer_id",
    "email_raw",
    "phone_raw",
    "interaction_type",
    "product_id",
    "product_category",
    "transaction_amount",
    "currency",
    "channel",
    "device_type",
    "browser",
    "ip_address",
    "city_raw",
    "state_raw",
    "zip_code",
    "interaction_payload",
]

missing_cols = [c for c in expected_cols if c not in df.columns]
if missing_cols:
    log(f"WARNING: Missing expected columns: {missing_cols}")
else:
    log("All expected columns present")

# Sample data quality check
log("=" * 60)
log("SAMPLE DATA (5 rows)")
log("=" * 60)
sample_df = df.select("event_id", "customer_id", "interaction_type", "channel", "city_raw").limit(5)
sample_df.show(truncate=False)

# Value distribution check
log("=" * 60)
log("VALUE DISTRIBUTIONS")
log("=" * 60)
log("Interaction types:")
df.groupBy("interaction_type").count().orderBy("count", ascending=False).show()

log("Channels:")
df.groupBy("channel").count().orderBy("count", ascending=False).show()

total_time = time.time() - start_time

# Measure input size via Hadoop filesystem API
try:
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    uri = sc._jvm.java.net.URI(bronze_uri + "customer/interactions/")
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
    hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(bronze_uri + "customer/interactions/")
    input_size_gb = (
        fs.getContentSummary(hadoop_path).getLength() / (1024**3) if fs.exists(hadoop_path) else 0.0
    )
except Exception:
    input_size_gb = 0.0

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
