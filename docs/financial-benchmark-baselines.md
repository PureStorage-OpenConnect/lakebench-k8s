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

Wall-clock covers the batch pipeline only. Detection rules run separately via `lakebench financial replay` (LB-092 tracks the gap); recall in this table is derived from that manual detection loop plus `lakebench financial score`. Alert count is the sum across every W-rule that completed. Cores used is the peak Spark-executor request across the three batch jobs (silver_build is the peak in every measured run).

| Scale | Wall-clock p50 (s) | Wall-clock p95 (s) | Alert count | Recall | Cores used | Storage read (GB) |
|------:|-------------------:|-------------------:|------------:|-------:|-----------:|------------------:|
|     1 |            707.5[1]|                TBD |    163,526[2]|   0.331[3]|         32 |             14.4  |
|    10 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
|   100 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
|  1000 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |
| 10000 |                TBD |                TBD |         TBD |    TBD |        TBD |               TBD |

Footnotes on scale 1 (run-20260921-220243-fea608, first live end-to-end FAML pipeline on the reference cluster, 2026-09-21):

[1] Single run; treat as p50 not p95. bronze_verify 360s, silver_build 255s, gold_finalize 75s. Datagen wrote 10 GB pacs.008 (26.66M silver rows).

[2] Sum across the four W-rules that completed via `lakebench financial replay --depth-months 0`: W2_structuring 2,438; W3_round_tripping 180; W4_risk_propagation 98,786; W8_dormant_reactivation 62,122. W1_connected_components and W7_cross_border_high_risk crashed on rule-code defects (`Column dst#63L are ambiguous` self-join in W1) and are not counted; file follow-ups against `detection_rules.py` before quoting this number in an external context.

[3] Weighted mean over the four typologies each completed rule targets per `RULE_TARGETS` in `benchmark/faml_queries.py`: micro_structuring 0.482 (222 instances), rapid_layering 0.305 (889), stack 0.401 (444), dormant_reactivation 0.233 (356). UETR-level FP rate across the full alert set is 0.985 -- roughly 98% of alerts do not touch any manifest-tagged typology row, which is expected on a synthetic baseline and NOT a claim about ops-queue FP rate (see `docs/faml-scoring.md`). Per-typology recall for all 15 planted typologies is in the `recall.parquet` artifact under the gold bucket.

Not populated in this row: QpH (LB-093: spark-thrift default 4Gi limit OOMs on scale-1 silver aggregation, so the 8-query FAML benchmark returns 0/8 until sizing is bumped) and Wall-clock p95 (single run).

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

**2026-09-21 (v1.5.0.dev0, run-20260921-220243-fea608)** -- first live end-to-end FAML pipeline on the reference cluster. Scale-1 row populated in the batch table. Not a release measurement -- the run surfaced five real defects (LB-088 FlashBlade tagging, LB-089 datagen v2 layout drift, LB-090 sustained-mode env-var drift, LB-091 sustained CLI missing bronze_verify, LB-092 gold_finalize does not invoke detection rules, LB-093 spark-thrift undersized) and the fixes are still in flight. Scale-10 and above will not be measured until LB-092 and LB-093 are closed.
