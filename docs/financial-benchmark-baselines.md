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

| Scale | Wall-clock p50 (s) | Wall-clock p95 (s) | Alert count | Recall | Cores used | Storage read (GB) |
|------:|-------------------:|-------------------:|------------:|-------:|-----------:|------------------:|
|     1 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
|    10 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
|   100 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
|  1000 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
| 10000 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |

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

Empty until v1 release measurements land.
