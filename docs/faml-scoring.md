# FAML: Financial-crime / AML Workload

Lakebench's FAML workload measures a lakehouse stack against a
Financial-crime / Anti-Money-Laundering (AML) detection pipeline: read
pacs.008 wire messages from bronze, build silver entity / account /
counterparty tables, aggregate risk in gold, then score six W-rule
detectors against synthetic planted typologies. This document explains
what the scorecard actually measures, what it does not measure, and how
to run one end-to-end.

## What you get on the scorecard

The two headline numbers are per-rule **recall** and per-rule
**precision** against the planted typology set. A third per-rule
quantity, **pattern-span**, is published alongside them but is a
description of the injected data, not a platform measurement -- see the
metric-trust caveats below before quoting it. All are computed by the
SQL templates registered in
[`src/lakebench/benchmark/faml_queries.py`](../src/lakebench/benchmark/faml_queries.py)
and joined against the datagen manifest at `bronze.manifest`. Two
aggregates round out the primary view:

- **`aggregate_typology_coverage`** -- for each planted typology, does at
  least one W-rule fire against it? Shows which typologies the rule set
  can address at all, distinct from how well.
- **`aggregate_reference_vs_rule`** -- rule recall vs a scikit-learn
  Gradient Boosted reference detector's recall, per typology. Answers
  "does the rule outscore what a canonical detector would learn from the
  same data?" A rule that scores well above the reference is either a
  smart heuristic or benefiting from a label leak; cross-reference with
  the leakage report to tell which.

The rule set:

| Rule | Targets typology | What it detects |
|---|---|---|
| W1_connected_components | gather_scatter | multi-entity graph clusters |
| W2_structuring | micro_structuring | high-frequency structuring under CTR |
| W3_round_tripping | rapid_layering | round-trip sequences |
| W4_risk_propagation | stack | high-velocity chains |
| W7_cross_border_high_risk | corridor_high_risk | corridors to FATF grey / high-risk jurisdictions |
| W8_dormant_reactivation | dormant_reactivation | inactive account, sudden large flow |
| W5_sanctions_match, W6_pep_counterparty | -- | detect-only; no planted typology today (party-flag targets are datagen follow-up) |

Every planted typology that no shipped rule targets is documented in
`UNMAPPED_TYPOLOGIES` in `faml_queries.py` with a one-line reason. That
list is the difference between "we plant this but no detector scores
it" and "we forgot to hook this up." Recall for an unmapped typology is
untestable rather than zero.

## Why benchmark precision is not the FP rate ops teams care about

The precision numbers in this benchmark answer a narrow question:
against a synthetic bronze layer where every non-typology row is a
log-normal baseline draw, what fraction of a rule's alerts land on
manifest-tagged typology rows? A typical result at scale 10 reads
around 0.80 -- 0.99 for the rules that target a planted typology, which
looks great next to the 90 %+ false-positive rate that Tier-1 AML
literature reports from production alert queues.

The two numbers measure different things.

- **The benchmark baseline is drawn from a single log-normal amount
  distribution with weekend / hour multipliers.** Real customer
  behaviour multi-modes across account type, industry, seasonality,
  known-good vendor relationships, and a long tail of small edge cases
  the datagen does not simulate. Ops-queue false positives largely come
  from the tail of "unusual but legitimate" activity that this datagen
  does not generate. A rule that scores 0.95 precision here is
  reporting how cleanly it separates from the log-normal baseline, not
  how many legitimate customers it would alert on in production.
- **The datagen has no ground-truth "legitimate but suspicious"
  category.** Every non-typology row is labelled clean. In production,
  a large fraction of alerts hit customers that a human reviewer will
  clear but that lack a manifest tag. Precision here has no counterpart
  for that bucket, so it cannot bound it.
- **The rule catalogue was written against this datagen.** The
  thresholds in `detection_rules.py` were tuned for the planted
  typologies; they are not a claim about what tuning is right for a
  particular bank's transaction pool.

Use the benchmark precision to compare stacks and to detect regressions
between releases, not to argue about production FP rates. When
comparing two configurations, precision moving is a signal that the
detector's separability against the same synthetic baseline changed.
When arguing about production, treat the number as a lower bound on
the modelling problem's difficulty and go get transaction-level
labelled data from the bank.

## The leakage gate and the reference detector

FAML shipped with a standing risk: a rule tuned tightly against the
same distribution the datagen plants will read high recall even when it
has learned nothing generalisable. The audit that motivated
[PR-A](../CHANGELOG.md) called this "distribution checks do not prove
semantics -- must run a reference detector and a leakage check." Both
now ship.

### Leakage gate

`score_financial_reference.py` emits `leakage_report.parquet` before it
trains anything. For each currency's structuring band -- the amount
range immediately below the reporting threshold that
`micro_structuring` plants into -- it counts baseline transactions and
typology transactions inside that band and reports the ratio
`baseline / typology`.

| Verdict | Meaning |
|---|---|
| `pass` | baseline density is at least 10 % of typology density in this band -- the band is not a label proxy. |
| `leaking` | ratio below 10 % -- the band effectively IS the label. Any rule that just filters on it will read 100 % recall. |
| `no_typology` | no typology rows in this band -- carries no leakage claim on its own. |

`overall_pass` is true only when at least one band scored and no band
came back `leaking`. An all-`no_typology` report is a datagen
regression, not a clean pass, and the gate refuses to treat it as one.

The threshold ships at 10 % (`DEFAULT_LEAKAGE_RATIO`), tunable via the
Python API's `threshold_ratio` argument. A `leaking` verdict must be
resolved by broadening the baseline distribution or narrowing the
typology's band, not by lowering the threshold; the threshold's job is
to make ratio drift visible, not to hide it.

The historical example: the datagen originally emitted every USD
`micro_structuring` transaction in the `[9500, 9999]` range while the
baseline log-normal produced essentially none. A rule reading "amount
between 9000 and 10000" reported 100 % recall and 99 % precision. The
gate now catches that shape and fails the run.

### Reference detector

`train_reference_gbt` in
[`src/lakebench/faml/reference_score.py`](../src/lakebench/faml/reference_score.py)
trains a scikit-learn Gradient Boosted Classifier per typology on a
deliberately narrow feature set:

- `log_amount_mean`, `log_amount_std`, `amount_pct_of_ceiling` --
  amount shape.
- `mean_hour`, `std_hour` -- when in the day activity happens.

The training API refuses at call time on any of five columns that
directly encode the label: `amount_in_structuring_band`,
`is_structuring`, `typology_type`, `typology_id`, `expected_workload`.
Passing one raises `ValueError` naming the column. The refusal is a
correctness feature -- exactly the mistake the standing rule warns
against -- so it lives in the library rather than in the driver.

Three additional features were removed after adversarial review before
PR-A landed and their absence is a deliberate trade-off in the current
datagen:

- **`high_risk_country_ratio`** was a byte-for-byte proxy for the
  `corridor_high_risk` typology (same HIGH_RISK_CC set). Removed.
- **`txn_count`** and **`unique_counterparties`** were cardinality
  fingerprints of the graph typologies. Removed.

The consequence: the reference detector cannot see the corridor or
graph-cardinality typologies in the current datagen and will read
`recall = 0` against them. That reads on the scorecard as "the rule
has no reference floor to beat," not as "the detector is bad." The
fix belongs in the datagen (probabilistic country overlays that mix
baseline and typology country distributions, cardinality noise on the
baseline entities), not in the reference model. When the datagen ships
those overlays, the removed features become measurement-safe again.

The verdicts:

| Verdict | Meaning |
|---|---|
| `ok` | trained and scored; `overall_f1` and per-typology recall/precision/f1 are meaningful. |
| `insufficient_labels` | one or more typology classes had fewer than `min_positive_per_class` rows in the sampled frame. Per-typology rows still populate but treat them as directional. |
| `no_sklearn` | the driver image did not ship scikit-learn. The FAML pipeline still produces rule scores; the reference detector row is empty and the `aggregate_reference_vs_rule` query labels it `not_run`. |

## Metric-trust caveats

Marcus's roleplay pass flagged three places where the scorecard names
suggest more than they measure. They apply to the whole pipeline
benchmark, not FAML specifically. `compute_efficiency_gb_per_core_hour`
shows up in every FAML run; the other two only appear in sustained
mode, and the shipped FAML example is batch mode.

- **`compute_efficiency_gb_per_core_hour`** is `GB / core_hours
  REQUESTED`, not `/ core_hours utilised`. A pod that requests 8 cores
  and uses 2 shows up as 4 x less efficient than one that requests 2
  cores and uses 2, even if they did identical work. Use it for
  release-to-release regression detection on the same config; do not
  compare against numbers from a stack sized differently.
- **`ingest_ratio`** (sustained mode only) divides bronze row count by
  a scale-derived estimate of what datagen would have produced, not a
  measurement of what it did produce. The estimate is a
  Customer 360 constant, so in a sustained FAML run the denominator is
  off by roughly the ratio of FAML's per-scale-unit row count to
  Customer 360's; treat the number as "did the pipeline keep pace at
  all" rather than as a precise fraction.
- **`qph_degradation_pct`** (sustained mode only) wants at least four
  rounds to read as a trend. Typical sustained runs produce five.
  Interpret values from a five-round run as a signal, not a conclusion.
- **`pattern_span_s`** (per rule, was labelled "time-to-detect") is NOT
  detection latency. It is the span from a planted typology's injection
  start to the event time of the last transaction a rule cites for it,
  because `alert_ts` carries the last contributing transaction's event
  time, not the wall-clock at which the alert was produced. The value is
  therefore a property of the datagen's typology window arithmetic in
  `datagen_rs/src/typology.rs` (roughly 3 days for `micro_structuring`,
  one civil day for `rapid_layering`), invariant to how fast or slow the
  stack under test runs -- median pattern-span at scale 10000 on a fast
  cluster equals median pattern-span at scale 1 on a slow one. A true
  time-to-detect needs a data-arrival clock in the same frame as the
  alert, which only exists in multi-cycle (per-cycle ingest timestamp)
  or sustained mode; it is tracked for a later release (LB-121). Use
  pattern-span to sanity-check that a rule fires inside its typology
  window, never as a speed comparison between stacks.

Precision and recall for FAML themselves are honest measurements of
what they say -- rule alerts joined against manifest rows -- with the
label-proxy risks the leakage gate now catches.

## Running a FAML pipeline

The full loop is `deploy -> generate -> run -> financial score`. Every
FAML config points to a `workload.schema=financial` config; the
example that ships is
[`examples/polaris-iceberg-spark-financial.yaml`](../examples/polaris-iceberg-spark-financial.yaml)
(and `polaris-iceberg-spark-financial-local.yaml` for developer sanity
at scale 1). Any of the Iceberg recipes works -- the schema flag routes
datagen and the pipeline scripts, not the catalog choice.

```bash
lakebench deploy   examples/polaris-iceberg-spark-financial.yaml
lakebench generate examples/polaris-iceberg-spark-financial.yaml --wait
lakebench run      examples/polaris-iceberg-spark-financial.yaml
lakebench financial score \
    examples/polaris-iceberg-spark-financial.yaml \
    --manifest s3://<bronze-bucket>/manifest/manifest.parquet \
    --output   s3://<bronze-bucket>/scores/recall.parquet
```

Two operator-facing subcommands cover the retention-workload scenarios:

- **`lakebench financial replay CONFIG --rule W2_structuring --depth-months 60`**
  reruns one rule against an Iceberg snapshot from N months ago and
  appends alerts to `gold.alerts` under a distinct `rule_id` scope. The
  W8 verification scenario in the working spec asserts against a 60-month
  replay's wall-clock budget.
- **`lakebench financial reproduce CONFIG --alert-id <id>`** takes a single alert
  from `gold.alerts` and reproduces it against the historical snapshot the
  original rule ran on. Small, bounded work; used as a
  supervisory-reproducibility smoke check, not as a scaling metric.

`CONFIG` in both cases is the same YAML you passed to `deploy`. Both
verbs load it, assert `workload.schema=financial`, and dispatch a
SparkApplication.

The scale factor sets bronze volume the same way it does for Customer
360: scale 1 is ~10 GB of pacs.008 messages, scale 100 is ~1 TB, scale
10000 is a tier-1 universal bank's AML retention target at ~100 TB.
The Pydantic schema accepts up to scale 10000, but scale 500 (~50 TB)
is the tested ceiling; runs above it have not been verified end-to-end
and are on the user.

## What the FAML workload deliberately does not measure

- **Real production alert queues.** The datagen has one baseline
  distribution and roughly a dozen planted typology shapes. A bank's
  transaction mix has orders of magnitude more variety. Do not use
  precision from this benchmark to argue an ops-queue FP rate.
- **Rule tuning quality.** The thresholds shipped in
  `detection_rules.py` were chosen to detect the planted typologies at
  reasonable recall on this datagen. Tuning them against a different
  transaction pool is a separate exercise.
- **The correctness of specific rule logic against real-world
  typologies.** These rules are illustrative implementations of common
  W-rule patterns for benchmarking pipeline throughput. Treat them as
  representative workload, not as production-ready detectors.
- **Any typology that does not appear in `RULE_TARGETS` or
  `UNMAPPED_TYPOLOGIES`.** If the manifest starts emitting a typology
  string that appears in neither, the test suite fails at build time so
  the gap is visible before a run.

## Where to look next

- [`src/lakebench/spark/scripts/detection_rules.py`](../src/lakebench/spark/scripts/detection_rules.py) -- the six W-rule implementations.
- [`src/lakebench/benchmark/faml_queries.py`](../src/lakebench/benchmark/faml_queries.py) -- rule -> typology mapping and query catalogue.
- [`src/lakebench/faml/reference_score.py`](../src/lakebench/faml/reference_score.py) -- leakage gate + reference detector library.
- [`src/lakebench/spark/scripts/score_financial_reference.py`](../src/lakebench/spark/scripts/score_financial_reference.py) -- Spark driver for both, called by `lakebench financial score`.
- [`docs/financial-benchmark-baselines.md`](financial-benchmark-baselines.md) -- reference-cluster wall-clock and recall numbers, populated per release.
