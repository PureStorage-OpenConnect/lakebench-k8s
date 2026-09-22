# Financial Benchmark Baselines

Published measurements for the Financial (FinServ-Crime, AML) workload
at each scale point. Referenced by ENG-2C.4.8 verification (W8 replay
timeout budget), by release regression detection, and by external
narratives that need quotable numbers.

**Values below are TBD (empty scaffold in v1).** Populated after each
release's UAT run per the update procedure at the bottom of this file.

## How to read this table

- **Scale.** Datagen `scale` factor. Scale 1 emits ~10 GB pacs.008
  baseline (500K accounts * 4 txns/month * 12 months, per REQ-S-01 /
  REQ-S-02). Linearly scales up: scale 100 ~= 1 TB, scale 10000 ~=
  100 TB (tier-1 universal bank AML retention target).
- **Wall-clock p50 / p95.** Median and 95th-percentile wall-clock
  seconds across N independent runs at the same scale on the same
  reference cluster. p95 catches skew / warm-up effects.
- **Alert count.** Total detection alerts written to `gold.alerts` at
  end of pipeline. Includes typology and typology-adjacent hits.
- **Recall.** Fraction of scheduled typology instances the workload
  detected. Computed by `lakebench financial score`.
- **Cores used.** Sum of executor cores requested across the pipeline
  (bronze_verify + silver_build + gold_finalize).
- **Storage read.** Aggregate S3 GET GB across the pipeline. Reference
  the storage-backend note when reading against slower object stores.

## Reference cluster

- **Node count:** 10
- **Cores per node:** 32
- **Memory per node:** 256 GB
- **Network:** 100 Gbps
- **Storage:** S3-compatible with ~10 GB/s aggregate read throughput

Numbers scale with cluster shape; comparing across clusters requires
adjustment. Publish only reference-cluster numbers here; extrapolations
go elsewhere.

## Batch pipeline (bronze_verify -> silver_build -> gold_finalize)

Wall-clock covers the batch pipeline only. As of PR-G (LB-112) detection
rules run inline as the final step of gold_finalize, so `alert count`
below reflects what one `lakebench run` writes to `gold.alerts` per
cycle (no separate `lakebench financial replay` needed). Cores used is
the peak Spark-executor request across the three batch jobs
(silver_build is the peak in every measured run).

| Scale | Wall-clock p50 (s) | Wall-clock p95 (s) | Alert count | Recall | Cores used | Storage read (GB) |
|------:|-------------------:|-------------------:|------------:|-------:|-----------:|------------------:|
|     1 |            930.5[1]|          1065.6[2] |         [3] |   [3]  |         32 |             14.4  |
|    10 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
|   100 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
|  1000 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
| 10000 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |

Footnotes on scale 1 (three runs, first three-iter clean baseline on
the reference cluster after PR-G + PR-H, 2026-09-22):

[1] Median of three consecutive `lakebench run` cycles at scale 1 on
2026-09-22. Wall-clock per iter: 1065.6s, 930.5s, 930.1s. Per-stage
figures (min/p50/max): bronze_verify 360.3 / 360.3 / 495.4s;
silver_build 255.2 / 255.2 / 255.2s; gold_finalize 300.2 / 300.2 /
300.3s. Silver_build and gold_finalize are within-0.1s across runs
-- the wall-clock spread lives entirely in bronze_verify's Ivy jar
resolution, which took 135s longer on iter 1 than on iter 2/3 (cold
mirror fetch first time through -- see PR-H Maven mirror fallback).
Gold_finalize is 300s (up from 75s in the earlier LB-092 run) because
the six W-rules now run inline as part of gold_finalize -- W1 through
W4, W7, and W8 all executed; the alert-writing DELETE-then-INSERT is
per-rule idempotent, so a re-run against the same silver yields the
same alert set.

[2] With n=3, p95 is effectively the maximum. The 1065.6s upper bound
was driven by the first run's cold Maven mirror fetch; a warmer cache
would bring this closer to p50. Repopulate once n >= 10 for a real p95.

[3] Alert count and per-rule recall are not yet reported in this row.
The three runs each wrote 1.068 GB to `gold.alerts` (identical to
three significant figures across runs -- consistent evidence detection
is deterministic), but the driver's per-rule alerts=N log lines were
not extracted into `metrics.json` at run time and the buckets were
destroyed between iterations. LB-116 tracks the missing metric
extraction; the next baseline pass (once the per-rule counts are
recorded to `metrics.json`) will populate both. LB-094 (silver-build
role-prefix bug that splits every entity into two silver rows) also
depresses recall structurally and is not addressed in PR-G; W1-W4
recall will step up once LB-101 lands.

Not populated in this row: QpH. Post-compaction QpH across the three
runs was 6.6 / 0.0 / 8.2. The zero was a spark-thrift pod crash
mid-benchmark on iter 2, not a pipeline failure. The other two runs
had four of eight FAML queries timing out at 300 s (FQ1 full silver
scan, FQ2 top corridors, FQ4 running balance, FQ5 alert triage, FQ8
alert-to-entity join). Pre-compaction QpH was 38.8 / 39.1 / 39.0
(4/8 queries succeeded, cadre stable). LB-113 fixed the 4 Gi OOM;
LB-117 tracks the remaining query-timeout tune for FAML analytical
queries. QpH will populate here after LB-117 lands.

## Sustained pipeline (bronze_ingest -> silver_stream -> gold_refresh)

| Scale | Ingest rate (rows/s) | Silver merge p50 (s) | Gold refresh p50 (s) | Cores used |
|------:|---------------------:|---------------------:|---------------------:|-----------:|
|     1 |                  TBD |                  TBD |                  TBD |        TBD |
|    10 |                  TBD |                  TBD |                  TBD |        TBD |
|   100 |                  TBD |                  TBD |                  TBD |        TBD |

## Replay (W8, `lakebench financial replay`)

Runs a detection rule against a historical Iceberg snapshot. Timeout
budget below is what the ENG-2C.4.8 verification asserts against.

| Scale | Rule           | Depth (months) | Wall-clock p50 (s) | Alert delta vs current | Cores used |
|------:|:---------------|---------------:|-------------------:|-----------------------:|-----------:|
|   100 | W2_structuring |             60 |                TBD |                    TBD |        TBD |
|   100 | W3_round_trip  |             60 |                TBD |                    TBD |        TBD |

## Reproduce (W10, `lakebench financial reproduce`)

Reproducing a specific past alert via `FOR TIMESTAMP AS OF`. Small,
bounded work; used as a supervisory-reproducibility smoke check rather
than a scaling metric.

| Scale | Wall-clock p50 (s) | Reproduction match rate |
|------:|-------------------:|------------------------:|
|   100 |                TBD |                     TBD |

## Update procedure

Follow this after each release UAT:

1. On the reference cluster, deploy at scale 10 and scale 100 and run
   the full Financial pipeline. Capture wall-clock from
   `lakebench-output/runs/<run-id>/metrics.json`.
2. Run `lakebench financial score --manifest s3://.../manifest.parquet
   --output s3://.../recall.parquet` and record the mean recall.
3. For W8 replay, run `lakebench financial replay --rule W2_structuring
   --depth-months 60` and compare alert count against the current-corpus
   W2 run.
4. Edit this file: replace TBD rows for the scales you measured. Keep
   older release numbers in a "History" section so regressions are
   traceable.
5. Note deviations from the previous release (>10% wall-clock change or
   >5-point recall change) in the release's lessons-learned document.

## History

**2026-09-21 (v1.5.0.dev0, run-20260921-220243-fea608)** -- first live end-to-end FAML pipeline on the reference cluster. Scale-1 row populated in the batch table. Not a release measurement -- the run surfaced five real defects (LB-088 FlashBlade tagging, LB-089 datagen v2 layout drift, LB-090 sustained-mode env-var drift, LB-091 sustained CLI missing bronze_verify, LB-112 gold_finalize does not invoke detection rules, LB-113 spark-thrift undersized) and the fixes are still in flight. Scale-10 and above will not be measured until the batch pipeline runs three times cleanly.

**2026-09-22 (v1.5.0.dev0, three consecutive S1 runs)** -- first clean three-iter S1 baseline on the reference cluster after PR-G (LB-112/113/114/115) and PR-H (Google Maven mirror fallback for repo1.maven.org rate-limits) landed. All three pipelines completed rc=0, gold_finalize wrote an identical 1.068 GB to `gold.alerts` in every run, and per-stage timings were within 0.1s except for iter-1's Ivy jar-resolution cold start. Scale-1 row repopulated with p50 = 930.5s and n=3 max = 1065.6s. LB-116 and LB-117 track two remaining follow-ups (missing per-rule alert counts in metrics.json, FAML analytical query timeouts); LB-094 (bipartite silver-build entity split) still depresses recall structurally.
