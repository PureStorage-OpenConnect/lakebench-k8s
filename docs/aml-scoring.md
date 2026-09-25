# AML: Financial-crime / AML Workload

Lakebench's AML workload measures a lakehouse stack against a
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
[`src/lakebench/benchmark/aml_queries.py`](../src/lakebench/benchmark/aml_queries.py)
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
| W2_structuring | micro_structuring | 3+ structuring-band transactions in 24 h, two kinds: per originator (`structuring`, tumbling day) and per beneficiary from 2+ senders (`structuring_beneficiary`, sliding 24 h) |
| W3_round_tripping | cycle | funds returning to the originator through 2-5 transfers within 30 days |
| W4_risk_propagation | rapid_layering | pass-through of 80%+ within 6 h |
| W17_layering_chain | stack | open chains of 3+ transfers, each forwarding 80-100% of the previous within 7 days |
| W7_cross_border_high_risk | corridor_high_risk | corridors to FATF grey / high-risk jurisdictions |
| W8_dormant_reactivation | dormant_reactivation | inactive account, sudden large flow |
| W5_sanctions_match, W6_pep_counterparty | -- | detect-only; no planted typology today (party-flag targets are datagen follow-up) |

W9-W16 are workload ids the spec reserves (writeback, reproduce, ingest, the
ML workloads), which is why the layering-chain rule is W17. W3 and W17 skip
with "not run" (`path-cap`) when their path search would not fit the job's
scratch; the budget is read from `LB_PATH_SEARCH_MAX_ROWS` or derived from
the executor count and scratch size.

Every planted typology that no shipped rule targets is documented in
`UNMAPPED_TYPOLOGIES` in `aml_queries.py` with a one-line reason. That
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

AML shipped with a standing risk: a rule tuned tightly against the
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
[`src/lakebench/aml/reference_score.py`](../src/lakebench/aml/reference_score.py)
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
| `no_sklearn` | the driver image did not ship scikit-learn. The AML pipeline still produces rule scores; the reference detector row is empty and the `aggregate_reference_vs_rule` query labels it `not_run`. |

## Metric-trust caveats

Marcus's roleplay pass flagged three places where the scorecard names
suggest more than they measure. They apply to the whole pipeline
benchmark, not AML specifically. `compute_efficiency_gb_per_core_hour`
shows up in every AML run; the other two only appear in sustained
mode, and the shipped AML example is batch mode.

- **`compute_efficiency_gb_per_core_hour`** is `GB / core_hours
  REQUESTED`, not `/ core_hours utilised`. A pod that requests 8 cores
  and uses 2 shows up as 4 x less efficient than one that requests 2
  cores and uses 2, even if they did identical work. Use it for
  release-to-release regression detection on the same config; do not
  compare against numbers from a stack sized differently.
- **`ingest_ratio`** (sustained mode only) divides bronze row count by
  a scale-derived estimate of what datagen would have produced, not a
  measurement of what it did produce. The estimate is a
  Customer 360 constant, so in a sustained AML run the denominator is
  off by roughly the ratio of AML's per-scale-unit row count to
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

Precision and recall for AML themselves are honest measurements of
what they say -- rule alerts joined against manifest rows -- with the
label-proxy risks the leakage gate now catches.

## Running an AML pipeline

The full loop is `deploy -> generate -> run -> financial score`. Every
AML config points to a `workload.schema=financial` config; the
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

The scale factor sets bronze volume linearly: scale 1 is 8.4 GB of
pacs.008 messages (26.7M transactions over 60 months), scale 100 is about
840 GB, and scale 10000 is a tier-1 universal bank's AML retention target
at about 84 TB.
The Pydantic schema accepts up to scale 10000, but scale 500 (~50 TB)
is the tested ceiling; runs above it have not been verified end-to-end
and are on the user.

## The transaction-monitoring operations layer

After detection, the gold stage runs the work a bank's TM operation does
with the alerts (`spark/scripts/tm_operations.py`, GOALS P10). One reporting
institution monitors its own customers (`silver.entities.is_customer`);
everyone else is a counterparty. The unit of work is the business day: an
alert is generated the day after its last payment, and the queue is replayed
day by day up to the cycle's as-of date (the day after the newest payment).
Anything that would be decided later is the open backlog.

| Table | Stage | What it holds |
|---|---|---|
| `gold.tm_reconciliation` | 1 | One set of rows per cycle (batch) or operations pass (continuous), appended: customers; source, bronze and silver payments; monitored vs excluded by reason (`no_customer_party`, `dq_unconvertible_currency`, and `in_flight` in continuous); DQ rule failures; the funnel alerts -> escalated -> cases -> SARs |
| `gold.scenario_coverage` | 1 | Scenario-to-typology matrix with this cycle's rule status, alert volume and planted instances; planted typologies no scenario targets appear as `gap` |
| `gold.alert_dispositions` | 6 | Triage priority (scenario weight x CRR tier: low, medium, high, critical) and a disposition that is never NULL: `escalated`, `closed_nfa`, `attached` to the customer's open case, `pending_l1`, `out_of_scope` (non-customer), `over_capacity` (past the per-customer cap); aging against the SLA; QA re-review; the alert's first-seen cycle |
| `gold.cases` | 7, 8 | Customer-keyed cases, at most one open per customer, with the alerts they pulled in and the payments of the lookback window; determination, `sar_filed` / `no_sar`, the filing limit that applied, the continuing-activity review and what happened to it |

**Dispositions are simulated, not made by people.** An alert is truly
suspicious when it touches a planted (non-control) typology payment. The L1
analyst decides correctly with probability `analyst_accuracy`, the L2
investigator and the QA reviewer with `investigator_accuracy`. Every draw is
a hash of the seed and a stable key (the alert's identity, the case id), so
a rerun reproduces the same decisions. Every operations number in the report
is conditional on these values, and the report says so.

**Regulatory clocks.** 31 CFR 1020.320: file within 30 days of the
determination, 60 when no suspect is identified (`no_suspect_rate` of new
cases). FinCEN continuing activity: review 90 days after each SAR and file
the continuing SAR within 120 days of the prior one. A review that falls due
while the customer's case is still under investigation is folded into it; a
review that falls due while that case is already determined and only waiting
to file is never credited to its investigation; the SAR that case then files
covers the activity and is recorded as the continuing-activity filing
(`superseded_by_sar`). Late filings are
drawn at `late_filing_rate` and flagged `filed_late`.

**Alert identity across cycles.** Detection re-runs over the whole corpus
each cycle, so an alert's window can grow as payments arrive. The layer
matches each alert to the previous cycle's: same content first, else the
same rule on the same customer with its last payment moved forward by at
most 31 days (the earliest such alert, so grown windows pair in order). A
matched alert keeps its key, generated date, truth and
priority as first seen; an alert detection stops emitting is kept
(`in_current_detection` false); a new alert whose payments predate the
previous cycle is dated on this cycle, the first day it could have been
raised. The day-by-day replay is causal and its inputs for seen alerts are
frozen, so recomputing it reproduces every earlier decision; the
`history_stable` invariant checks that against the previous cycle's table.
Case activity counts are recomputed each cycle from silver.

```yaml
architecture:
  workload:
    tm_operations:
      enabled: true
      seed: 20260924
      analyst_accuracy: 0.90
      investigator_accuracy: 0.95
      qa_sample_rate: 0.05
      alert_sla_days: 60          # policy SLA, alert to final decision
      case_lookback_months: 12    # 6-12
      late_filing_rate: 0.03
      no_suspect_rate: 0.05
      max_alerts_per_customer: 50000
      continuous_interval_seconds: 1800
      counterparty_scenarios: [W1_connected_components, W3_round_tripping,
                               W4_risk_propagation, W17_layering_chain]
```

**Workflow invariants.** Checked on the tables as written, every cycle: the
monitored population is not empty; monitored + excluded = source; every alert
has a disposition row and none is NULL; alerts on non-customers come only
from scenarios declared customer-and-counterparty; escalated <= alerts;
alert-driven cases <= escalated; SARs <= cases; at most one open case per
customer; one disposition row per alert identity; the funnel is monotone; every SAR past 90 days has its review (or
is waiting on a case pending filing); no review is credited to an
already-determined case; decided history is unchanged from the previous
cycle.

**The P10 verdict is separate from detection.** `fail` (the layer ran and an
invariant is violated) fails the run. `not_run` (no manifest, a layer error,
a continuous window that ended before the manifest was ready) says why and
leaves detection scoring alone. `unknown` means no driver log was captured
for some cycle, or an invariant could not be checked (the raw source files
failed to list).
`disabled` skips the gate. The verdict is kept in `metrics.json` as
`tm_operations` and heads the report section.

**Continuous.** gold-refresh runs the layer every
`continuous_interval_seconds`, after the tick's freshness is logged: one pass
over the full corpus takes minutes at scale 10. Silver is read at one pinned
snapshot taken before the source files are counted, so payments datagen
wrote after the snapshot are `in_flight`, never a negative. The continuous
reset drops all four tables.

**Investigator queries.** The AML benchmark set includes four timed
investigator queries (class `investigator`): customer 360 for the top open
case, the 12-month activity review of the newest case, the counterparty and
two-hop view, and open cases older than 60 days. They are left out when the
layer is disabled. QpH is recorded with its query-set id; `compare` and
`reproduce` refuse to compare QpH across different query sets, so an 8-query
AML run is never set against a 12-query one.

## What the AML workload deliberately does not measure

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
- [`src/lakebench/benchmark/aml_queries.py`](../src/lakebench/benchmark/aml_queries.py) -- rule -> typology mapping and query catalogue.
- [`src/lakebench/aml/reference_score.py`](../src/lakebench/aml/reference_score.py) -- leakage gate + reference detector library.
- [`src/lakebench/spark/scripts/score_financial_reference.py`](../src/lakebench/spark/scripts/score_financial_reference.py) -- Spark driver for both, called by `lakebench financial score`.
- [`docs/financial-benchmark-baselines.md`](financial-benchmark-baselines.md) -- reference-cluster wall-clock and recall numbers, populated per release.
