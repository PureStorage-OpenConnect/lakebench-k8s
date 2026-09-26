# Financial Benchmark Baselines

Published measurements for the Financial (FinServ-Crime, AML) workload
at each scale point. Referenced by ENG-2C.4.8 verification (W8 replay
timeout budget), by release regression detection, and by external
narratives that need quotable numbers.

**Numbers are pending the v1.6 frozen-generator runs.** Earlier
measurements (scale 1 on 2026-09-22, scale 10 on 2026-09-23) were taken on
a generator and rule set that have since changed and are void; see
History. The tables below are filled from the post-freeze runs, each with
its run id.

## How to read this table

- **Scale.** Datagen `scale` factor. Scale 1 emits 8.4 GB of pacs.008
  (111,111 entities * 4 txns/month * 60 months = 26.7M transactions,
  measured 2026-09-24 with the default 64 MB files). It scales linearly:
  scale 100 is about 840 GB by this estimate, and measured runs land
  11-13% above it (about 950 GB of bronze at scale 100 in
  run-20260925-104703-c02890). AML has been run end to end up to scale
  100; scale 500 (about 4.2 TB by the estimate) and above are untested.
- **Wall-clock p50 / p95.** Median and 95th-percentile wall-clock
  seconds across N independent runs at the same scale on the same
  cluster. p95 catches skew / warm-up effects.
- **Alert count.** Total detection alerts written to `gold.alerts` at
  end of pipeline. Includes typology and typology-adjacent hits.
- **Recall.** Fraction of scheduled typology instances the workload
  detected. Computed by `lakebench financial score`. Not scored in
  continuous mode in v1.6 (LB-168).
- **Cores used.** Peak executor cores requested by a single batch job
  (the batch jobs run sequentially), or the sum across the three
  concurrent streaming jobs in continuous mode.
- **Storage read.** Aggregate S3 GET GB across the pipeline. Reference
  the storage-backend note when reading against slower object stores.

## Test cluster

- **Platform:** OpenShift 4.x on bare metal
- **Allocatable CPU:** 434 cores
- **Storage:** Pure Storage FlashBlade (S3, path-style, HTTP)

Numbers scale with cluster shape; comparing across clusters requires
adjustment. Each row cites its run id, whose `metrics.json` records the
configuration it ran with.

## Batch pipeline (bronze_verify -> silver_build -> gold_finalize)

Wall-clock covers the batch pipeline only. Detection rules run inline as
the final step of gold_finalize, so `alert count` reflects what one
`lakebench run` writes to `gold.alerts` per cycle.

| Scale | Wall-clock p50 (s) | Wall-clock p95 (s) | Alert count | Recall | Cores used | Storage read (GB) | Run ids |
|------:|-------------------:|-------------------:|------------:|-------:|-----------:|------------------:|:--------|
|     1 | pending | pending | pending | pending | pending | pending | pending |
|    10 | pending | pending | pending | pending | pending | pending | pending |
|   100 | pending | pending | pending | pending | pending | pending | pending |

## Continuous pipeline (bronze_ingest -> silver_stream -> gold_refresh)

| Scale | Ingest rate (rows/s) | Time to detect p50 (s) | Time to detect p95 (s) | Data freshness (s) | Cores used | Run ids |
|------:|---------------------:|-----------------------:|-----------------------:|-------------------:|-----------:|:--------|
|    10 | pending | pending | pending | pending | pending | pending |
|   100 | pending | pending | pending | pending | pending | pending |

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

1. On the test cluster, deploy at scale 10 and scale 100 and run
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

The entries below are kept for traceability only. Every number in them is
void for comparison: they predate the v1.6 frozen generator, the AML rule
target changes, the 3-sample QpH median, and working table maintenance
(LB-172 to LB-174). Do not quote them.

**2026-09-21 (v1.5.0.dev0, run-20260921-220243-fea608)** -- first live end-to-end AML pipeline on the test cluster. Scale-1 row populated in the batch table. Not a release measurement -- the run surfaced five real defects (LB-164 FlashBlade tagging, LB-165 datagen v2 layout drift, LB-166 sustained-mode env-var drift, LB-167 sustained CLI missing bronze_verify, LB-112 gold_finalize does not invoke detection rules, LB-113 spark-thrift undersized) and the fixes are still in flight. Scale-10 and above will not be measured until the batch pipeline runs three times cleanly.

**2026-09-22 (v1.5.0.dev0, three consecutive S1 runs)** -- first clean three-iter S1 baseline on the test cluster after PR-G (LB-112/113/114/115) and PR-H (Google Maven mirror fallback for repo1.maven.org rate-limits) landed. All three pipelines completed rc=0, gold_finalize wrote an identical 1.068 GB to `gold.alerts` in every run, and per-stage timings were within 0.1s except for iter-1's Ivy jar-resolution cold start. Scale-1 row repopulated with p50 = 930.5s and n=3 max = 1065.6s. LB-116 and LB-117 track two remaining follow-ups (missing per-rule alert counts in metrics.json, AML analytical query timeouts); LB-094 (bipartite silver-build entity split) still depresses recall structurally.

**2026-09-22 (v1.5.0.dev0, scale-10 attempt)** -- first live scale-10 AML batch attempt failed at bronze-verify with `java.io.IOException: No space left on device` after 78 minutes (three attempts, same failure). Per-executor scratch PVC (50 Gi) is undersized for the bronze_verify_financial CTAS + DISTINCT ORDER BY on 100 GB of pacs.008 raw. Filed as LB-118; scale 10 row stays TBD pending the fix. All lower-scale rows (scale 1) unaffected -- 50 Gi is comfortable at that volume. This is per-job local disk, unrelated to the LB-113 spark-thrift heap bump.

**2026-09-23 (v1.5.0.dev0, scale-10 batch, run-20260923-120258-b71af2)** -- first clean scale-10 AML batch run end to end after LB-118. Pipeline completed rc=0 in 7971s (bronze_verify 4278s, silver_build 1216s, gold_finalize 2477s), zero OOMKilled, no disk-full. Financial scoring ran inline inside `lakebench run` (Phase 1a fold -- no separate `financial score` invocation) and the scorecard rendered all 15 typologies as scored with a real recall spread (mean 0.322, fan_out 0.911 down to corridor_high_risk 0.043). The unflattering honest numbers it surfaced: 1,665,017 alerts at 98.8% false-positive rate, driven by W4 (1.05M) and W8 (614K) over-firing -- filed LB-130 as the top credibility blocker. W7 fired 0 alerts (high-risk entity plumbing gap) and scale_ratio reported 0.0 (LB-131, cosmetic). Scale-10 batch row populated (n=1); p95 pending n>=3.
