# UAT results 1.6.0

Live-cluster runs behind the 1.6.0 release: OpenShift 4.x on bare metal,
FlashBlade S3, integrate tree e6927ce, datagen built from the datagen_rs
source released as `lb-datagen:1.6.0` (validated as tag `034f998`; the
source is unchanged). Every row passed its gates with non-degenerate output:
bronze, silver and gold rows above zero, the expected rules ran, and every
benchmark query returned rows. Each result is n=1. The S3 endpoint is
redacted in the checked-in `metrics.json` files.

| Recipe | Workload | Mode | Scale | Result | Run id |
|---|---|---|---|---|---|
| hive-iceberg-spark-trino | AML (financial) | continuous | 1 | PASS | 20260929-205000-ebb26f |
| hive-iceberg-spark-trino | Customer 360 | continuous | 1 | PASS | 20260929-204941-1d17f4 |
| hive-iceberg-spark-trino | Customer 360 | batch | 10 | PASS | 20260929-212900-5105a0 |
| hive-iceberg-spark-trino | AML (financial) | batch | 10 | PASS | 20260929-214442-825153 |
| polaris-iceberg-spark-trino | AML (financial) | batch | 1 | PASS | 20260929-221146-9d5345 |

Notes:

- AML batch scale 10: gold-finalize took 4,100 s of its 5,400 s auto
  timeout on 4 executors (CHANGELOG, LB-201). W1 connected components was
  skipped with the recorded reason "giant-component".
- Customer 360 continuous: Trino OPTIMIZE on
  `silver.customer_interactions_enriched` hit the 100 open-writer limit
  (LB-210); the run still passed its gates.
- Not run live for 1.6.0: Delta pipelines, the spark-thrift and duckdb query
  engines, and scale 100. These are covered at unit and logic tier only.
