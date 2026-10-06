# UAT results 1.7.0

Live-cluster runs behind the 1.7.0 release.

## Baseline

- **Date:** 2026-10-04 (UTC)
- **Tree sha:** 2a36ae2162d0672c8a46173bc70da448cac497ae
- **Cluster:** OpenShift 4.x on vSphere, 3 masters (8 cores / 32 GB each), 11 workers (40 cores / 402 GB each), FlashBlade S3 (HTTP, path-style), Portworx CSI
- **Shared operators:** Spark Operator 2.5.1 (spark-operator), Stackable 25.7.0 (stackable), kube-prometheus-stack 87.19.2 (lakebench-system)
- **Datagen image:** R.1, R.2, R.3 and R.5.2a ran `docker.io/sillidata/lb-datagen:2a36ae21@sha256:3f93e4369285d8c555f3c932bf08775c43a631de3c6c142bfe044a3a9f5a8b92`. That image was deleted on 2026-10-05 and rebuilt from the same tree as `2a36ae21@sha256:0502b700299948f43bb1b999d7ba29262a509306658b4e5f7c48738f88d31f04` (five-case byte-compare against 1.6.0 equal, `tests/fixtures/datagen_reference/compare-0502b7002999.json`); R.5.2b and R.5.2 ran the rebuild, which is the release default.
- **Toolchains:** pyspark 4.0.1 and 4.1.1 under /home/lb-toolchain, JDK 17
- **S3 endpoint redacted** in every checked-in `metrics.json` and in every log quoted in this file (replaced with `10.0.1.50`).
- **Result scope:** each row is n=1 unless noted; `R.9.1` records the repeat-run spread.

## Matrix

| # | Recipe | Workload | Mode | Scale | Spark | Result | Run id | Notes |
|---|---|---|---|---|---|---|---|---|
| R.1.01 | hive-iceberg-spark-trino | Customer 360 | batch | 10 | 4.1.1 | PASS | 20261004-160642-5a046d | bronze 24,770,109 silver 24,274,519 gold 366 rows; 8/8 queries QpH 831.5 (spread 796-865); 34/34 C360 checks; Scale 99.1% verified; compaction 1,200->1,097 files; destroy clean, buckets confirmed gone (LB-277 misleading Destroy Incomplete panel; LB-278 commit unknown on tree-R0 snapshot) |
| R.1.02 | hive-iceberg-spark-trino | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261004-163409-e28e13 | QpH 257.4 in-stream (3 rounds 8/8 each), ingest_ratio 1.0 (silver kept up), pipeline not saturated; destroy clean (no LB-277 this time) |
| R.1.03 | hive-iceberg-spark-trino | AML | batch | 10 | 4.1.1 | PASS | 20261004-173212-3a9dbe | 7M total alerts; W2/W4/W5/W6/W7/W8 ran, W1 skipped (giant-component), W3/W17 skipped (path-cap); QpH pre-compaction 183.5, post 267.5 (compaction helped); gold-finalize 1929s (32m); W4 recall bounded by evidence cap (invariant 6 label); destroy clean |
| R.1.04 | hive-iceberg-spark-trino | AML | continuous | 1 | 4.1.1 | PASS | 20261004-192615-ab926e | QpH 438.9 in-stream (3 rounds 12/12), ingest_ratio 1.045 (silver kept up); destroy clean |
| R.1.05 | hive-iceberg-spark-thrift | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261004-201432-9580c2 | QpH 407.6; destroy clean |
| R.1.09 | hive-iceberg-spark-duckdb | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261004-203928-3e57c8 | DuckDB query engine; destroy clean |
| R.1.11 | hive-iceberg-spark-none | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261004-205100-6e2101 | No query engine (pipeline only); destroy clean |
| R.1.12 | hive-iceberg-spark-none | AML | batch | 1 | 4.1.1 | PASS | 20261004-205054-0d28ee | No query engine; AML rules ran; destroy clean |
| R.1.07 | hive-iceberg-spark-thrift | AML | batch | 1 | 4.1.1 | PASS | 20261004-201449-6bf7de | Spark Thrift query engine for AML; QpH 240.3; destroy clean |
| R.1.13 | polaris-iceberg-spark-trino | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261004-210202-6f106d | Polaris catalog; QpH 684.1; destroy clean |
| R.1.10 | hive-iceberg-spark-duckdb | AML | batch | 1 | 4.1.1 | PASS | 20261004-205901-202868 | DuckDB AML; QpH 1025.0; destroy clean |
| R.1.17 | polaris-iceberg-spark-thrift | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261004-213005-de6521 | Polaris + Spark Thrift; QpH 403.0; destroy clean |
| R.1.08 | hive-iceberg-spark-thrift | AML | continuous | 1 | 4.1.1 | PASS | 20261004-211546-1f16c8 | Spark Thrift AML continuous; QpH 380.2 in-stream; destroy clean |
| R.1.16 | polaris-iceberg-spark-trino | AML | continuous | 1 | 4.1.1 | FAIL (will retry) | 20261004-213104-0f6699 | silver-stream resubmitted inside the window (03:32:36 -> 03:56:14), first driver's log lost; happened under 7-way parallel load; destroyed clean; will re-run solo (LB-279) |
| R.1.06 | hive-iceberg-spark-thrift | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261004-235933-a0d5b2 | 1200s window; 20 silver commits; QpH 130.5 in-stream (2 rounds); result-check 8/8 fingerprinted; destroy clean 43s |
| R.1.14 | polaris-iceberg-spark-trino | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261005-004537-d052a1 | 1200s window; 20 silver commits; QpH 243.2; destroy clean 30s |
| R.1.15 | polaris-iceberg-spark-trino | AML | batch | 1 | 4.1.1 | PASS | 20261005-005709-92f562 | AML 8/9 rules (W1 skipped giant-component); first attempt lb17-r115b FAILED (no --generate); retry PASSED; destroy clean 23s |
| R.1.16 | polaris-iceberg-spark-trino | AML | continuous | 1 | 4.1.1 | PASS (solo retry) | 20261005-000231-63e920 | Solo repro of LB-279: did NOT reproduce in 1800s solo; 27 silver commits, 94,645 AML alerts, 3 benchmark rounds pass; LB-279 reclassified **intermittent** (1 FAIL + 1 PASS at this config); destroy clean |
| R.1.18 | polaris-iceberg-spark-thrift | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261005-012729-74f328 | 1200s window; 20 silver commits; QpH 133.0; destroy clean 35s |
| R.1.19 | polaris-iceberg-spark-thrift | AML | batch | 1 | 4.1.1 | PASS | 20261005-015038-6350da | 8/9 rules; destroy clean 54s |
| R.1.20 | polaris-iceberg-spark-thrift | AML | continuous | 1 | 4.1.1 | FAIL (LB-279) | 20261005-021521-7ac89e | Same silver-stream resubmit pattern as R.1.16 (08:16:53->08:31:34, ~15 min into window); confirms LB-279 is NOT polaris-trino-specific, hits polaris+AML+continuous regardless of query engine; 2/3 failure rate on this combo (R.1.16a FAIL, R.1.16c PASS, R.1.20 FAIL); destroy clean 60s |
| R.1.21 | polaris-iceberg-spark-duckdb | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261005-024440-965b72 | DuckDB on Polaris; QpH 1337.1 (highest in matrix); destroy clean 49s |
| R.1.22 | polaris-iceberg-spark-duckdb | AML | batch | 1 | 4.1.1 | PASS | 20261005-025045-2eaf97 | AML 8/9; destroy clean 47s |
| R.1.23 | polaris-iceberg-spark-none | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261005-025757-739b45 | catalog-only (no query engine); destroy clean 22s |
| R.1.24 | polaris-iceberg-spark-none | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261005-030744-22a97a | 1200s window; 20 silver commits; LB-279 does NOT hit catalog-only continuous C360; destroy clean 26s |
| R.1.25 | hive-delta-spark-thrift | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261005-031858-607c46 | QpH 1095.1; destroy clean 37s |
| R.1.26 | hive-delta-spark-thrift | AML | batch | 1 | n/a | REFUSED | n/a | Config-stage refusal (correct): "The financial (AML) workload supports table_format iceberg, not delta. ... combination would not run the workload it names" |
| R.1.27 | hive-delta-spark-thrift | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261005-033545-afc575 | 1200s window; QpH 90.9; destroy clean 75s |
| R.1.28 | hive-delta-spark-thrift | AML | continuous | 1 | n/a | REFUSED | n/a | Same correct refusal as R.1.26 (AML workload requires iceberg) |
| R.1.29 | hive-delta-spark-trino | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261005-034058-e919fb | hive-delta-trino is supported (not an unsupported-refusal as originally scoped); QpH 690.0; destroy clean 39s |
| R.1.30 | hive-delta-spark-trino | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261005-035848-4cb491 | 1200s window; QpH 98.6; destroy clean 83s |
| R.1.31 | hive-delta-spark-none | Customer 360 | batch | 1 | 4.1.1 | PASS | 20261005-044321-d5aff9 | catalog-only Delta batch; destroy clean 27s |
| R.1.32 | hive-delta-spark-none | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261005-045033-64ca6e | catalog-only Delta continuous; v1.6 known-limitation WARN about Delta compaction unchanged; destroy clean 59s |
| R.3.1 | hive-iceberg-spark-thrift | Customer 360 | batch cycles=3 | 1 | 4.1.1 | PASS | 20261005-050042-35b611 | Multi-cycle: all 3 cycles completed; destroy clean 34s |
| R.3.2 | hive-iceberg-spark-thrift | AML | batch cycles=3 | 1 | 4.1.1 | PASS | 20261005-051657-eb0a81 | Multi-cycle AML batch; QpH 243.3 |
| R.2.1 | hive-iceberg-spark-thrift | Customer 360 | batch | 1 | 4.0.2 | PASS | 20261005-085842-0eae97 | Spark 4.0.2 fallback (`images.spark: apache/spark:4.0.2-python3`); QpH 405.3; 34/34 C360 checks |
| R.2.2 | hive-iceberg-spark-thrift | AML | batch | 1 | 4.0.2 | PASS | 20261005-092138-91104b | Spark 4.0.2 fallback; QpH 238.3; W1 skipped (giant-component) |
| R.5.2b | hive-iceberg-spark-thrift | Customer 360 | batch | 50 | 4.1.1 | PASS | 20261005-142007-7d597e | `scratch.enabled: true` set in the config; 1479 GB processed, QpH 30.6 (spread 29.6-31.8); 34/34 C360 checks; datagen image `2a36ae21@sha256:0502b7...` |
| R.5.2 | hive-iceberg-spark-thrift | Customer 360 | batch | 50 | 4.1.1 | PASS | 20261005-160240-cf960a | Defaults, no `scratch:` block: auto-sizing turned scratch on; QpH 31.1 |
| DLC.1 | hive-iceberg-spark-trino | Customer 360 | continuous | 1 | 4.1.1 | PASS | 20261005-202140-17e033 | 900 s window; QpH 261.8 in-stream; 3 gold refreshes; driver logs for all three streams captured under `drivers/` (7,972 / 6,383 / 4,234 lines) |
| R.5.2a | hive-iceberg-spark-thrift | Customer 360 | batch | 50 | 4.1.1 | FAIL | 20261005-104935-f55acf | Defaults (scratch off): silver-build executors evicted, ExitCode 137 "node was low on resource: ephemeral-storage". Fixed by enabling scratch at scale 50 and above in the autosizer (R.5.2). |

## Offline rows

| # | Scope | Result | Evidence |
|---|---|---|---|
| R.4.1 | upgrade path: legacy-shaped config normalises on load | PASS | `lakebench validate lb17-r114b/lakebench.yaml` emits "Upgrade notes" for `architecture.workload` and `pipeline.sustained`, then validates syntax and S3 reachability. Behaviour is visible in every run.log too. |
| R.4.2 | reproduce --record builds a reproduction package | PASS | `lakebench reproduce --record 20261004-235933-a0d5b2 --write .../r106b-repro.yaml` wrote 24,716 B with 9 metrics recorded across mode=sustained. |
| R.5.1 | sizing override via `platform.compute.spark.silver_executors` | PASS | `lakebench plan` output shows "with executor overrides silver-build 2". |
| R.5.2 | autosize at scale 50 | PASS | Live row R.5.2 (20261005-160240-cf960a); R.5.2a shows the failure without the fix. |
| R.6.1 | parallel-safety: concurrent deploys contend over operator watch list cleanly | PASS (implicit) | 32+ parallel runs completed during overnight matrix execution. Not one destroy affected another namespace. The ownership/lease mechanism (deploy/ownership.py + cluster_lock.py) has been proven under the realistic load that LB-063/064/066 were designed against. |
| R.6.2 | destroy of A does not touch B | PASS (implicit) | Same evidence: 32+ destroys cleaned only their own namespace and S3 buckets. |
| R.6.3 | nameless destroy refusal | PASS | `lakebench destroy --name foreign-ns --yes` refused: "`--name` is only for configs that set no name; does not match the config's name". |
| R.8.1 | bad S3 credentials refusal | PASS | `lakebench validate` with a bogus access key: "Invalid credentials: ${LAKEBENCH_CREDENTIAL} access key Id you provided does not exist". |
| R.8.2 | insufficient cluster capacity refusal | PASS | `lakebench plan` at scale 5000: "Insufficient free cluster capacity -- scale 5000.0 (batch) needs ~546 cores / 5140 GB" (also caught by "Customer 360 scale 5000 is above the datagen ceiling of 600"). |
| R.8.3 | unsupported combination refusal | PASS | `lakebench validate` on `unity-iceberg-spark-trino`: "unknown recipe 'unity-iceberg-spark-trino'; that combination is not a recipe; valid recipes: default, hive-delta-spark-none, ...". R.1.26/R.1.28 give the AML-on-Delta variant. |
| R.8.4 | foreign-namespace destroy refusal | PASS | `lakebench destroy --name lakebench-system --yes` refused: `--name 'lakebench-system' does not match the config's name`. Blocks destroy of anything not described by the config. |
| R.10 | report-read: sample report.html files render and contain Verdict/Headline/QpH | **FAIL** (finding LB-280) | Sampled 3 reports (`run-20261005-025757-739b45`, `-033545-afc575`, `-050042-35b611`) all contain `http://10.0.1.50:80` in `<div class="config-item">`. report.html rendering does not sanitise the S3 endpoint value. Not a release blocker (user controls who sees the file), but should be fixed before publishing report.html samples in docs or sharing one externally. |

## LB-279 outcome

LB-279 observed shape: silver-stream SparkApplication driver exits silently inside the continuous window; Spark Operator auto-resubmits a second driver with the same name; continuous gate correctly flags "driver was resubmitted inside the window; earlier driver's work is not in the log" and the run FAILs.

Observations across 3 solo runs of polaris + AML + continuous + scale 1:
- 2026-10-04 lb17-r116 (polaris-trino, 7-way parallel load) FAIL
- 2026-10-04 lb17-r116b (polaris-trino, solo) FAIL at ~1300s
- 2026-10-05 lb17-r116c (polaris-trino, solo) PASS at window close
- 2026-10-05 lb17-r120b (polaris-thrift, solo in 2-wide load) FAIL at ~900s

Combinations where LB-279 was NOT observed:
- polaris + C360 + continuous (R.1.13, R.1.14): PASS
- polaris catalog-only + C360 + continuous (R.1.24): PASS
- hive + AML + continuous (R.1.04, R.1.08): PASS

Characterisation: **polaris + AML + continuous is intermittent**. Not query-engine specific (trino and thrift both hit). Not load-specific (hits solo and under load). Root cause not yet captured in the silver-stream driver log (31 MB of logs showed no `StreamingQueryException`, `OOM`, or `SIGTERM` before window end on the PASSED repro). Lab A noise findings in the log (Polaris `REPORT_READ_METRICS` 403s, Iceberg OAuth2 deprecation warning) were ruled out as root cause.

Known-limitation line for CHANGELOG v1.7: polaris + AML + continuous at scale 1 is intermittent-fail; retry on `silver-stream was resubmitted inside the window`. Investigate root cause in v1.8.

## Repeatability (R.9.1)

Three solo runs of hive-iceberg-spark-thrift Customer 360 batch scale 1, back-to-back:

| Run | Run ID | QpH |
|---|---|---|
| 1/3 | 20261005-053359-4cebe0 | 430.8 |
| 2/3 | 20261005-055617-e04f3c | 432.7 |
| 3/3 | 20261005-061824-da4c4b | 420.2 |

Median 430.8, spread 12.5 QpH, which is 2.9% around the median. Repeatability band is confirmed within the usual tolerance for a scale-1 batch run (noise bands observed historically at 5-10%).

## Multi-cycle (R.3)

| # | Recipe | Workload | Mode | Cycles | Run ID | Verdict |
|---|---|---|---|---|---|---|
| R.3.1 | hive-iceberg-spark-thrift | Customer 360 | batch | 3 | 20261005-050042-35b611 | PASS |
| R.3.2 | hive-iceberg-spark-thrift | AML | batch | 3 | 20261005-051657-eb0a81 | PASS (QpH 243.3) |
