# Reading AML results

How to read AML results: what recall and precision measure, what they do not,
and how to run the workload. The full specification is
[benchmarks/AML.md](benchmarks/AML.md).

The workload reads pacs.008 wire messages into bronze, builds silver entity,
account and counterparty tables, and runs nine detection rules (W1 to W8 and
W17) in gold. It then scores the rules' alerts against the typologies the
generator planted.

## What you get on the scorecard

**Recall and precision are uncalibrated and in-sample.** No held-out
[Level-2](glossary.md#level-2) result is published. The default seed, 43, is the calibration corpus: the
generator, the rule thresholds and the reference features were developed on
it. A seed-43 figure says what the pipeline does on that corpus, not how the
rules generalise.

The scorer is the Spark job
[`score_financial.py`](../src/lakebench/spark/scripts/score_financial.py).
A batch `lakebench run` submits it after gold-finalize. It joins `gold.alerts`
to the generator's manifest on payment UETRs, with no time window.

| Scorecard figure | `recall.json` key | What it is |
|---|---|---|
| Recall | `typologies[].recall` | Share of a typology's planted instances that an alert from one of its designated rules touches |
| Incidental recall | `incidental_recall` | The same, with alerts from any rule. The `random` control typology's incidental recall is the chance floor |
| Off-target | `fp_rate_by_rule` | Share of a rule's alerts that touch no payment of its target typology, counted by alert |
| Txn precision | `txn_precision_by_rule` | On-target share weighted by payments |
| Overall off-target rate (footer) | `fp_rate` | One rate over all alerts, shown with the total alert count |

None of these is a production false-positive rate (next section). Each key
is defined in [AML spec 8.4](benchmarks/aml/scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails).

Two things that look like results are not:

- **Seven-day-window recall.** `aml_queries.py` holds Trino SQL templates that
  use a 7-day window. Only unit tests run them. A recall figure with a 7-day
  window did not come from Lakebench's scorer.
- **Pattern span** (`pattern_span_s`). It describes the planted data, not
  the stack. It is not time to detect and is no speed figure. Real
  detection latency is `time_to_detect_seconds` (continuous only). See
  [AML spec 6](benchmarks/aml/queries.md#6-query-set).

The rules, their target typologies and what each detects are in
[AML spec 4.2](benchmarks/aml/rules.md#42-detection-rules). The planted
typologies, including the sanctions and PEP screening track, are in
[sections 3.6 and 3.8](benchmarks/aml/typologies.md#36-typologies). A typology no rule
targets is listed in `UNMAPPED_TYPOLOGIES` with a reason. Its recall is
untestable, not zero.

## Why benchmark precision is not the FP rate ops teams care about

Precision here answers one narrow question. On a synthetic bronze layer, what
share of a rule's alerts land on payments the manifest tags as planted?
However high it reads, it does not compare with the 90%+ false-positive rate
that Tier-1 AML literature reports from production alert queues.

- **One baseline distribution.** Non-planted payments come from one
  log-normal amount distribution with weekend and hour multipliers. Real
  customers vary by account type, industry, season and known-good vendors,
  with a long tail of unusual but legitimate activity. That tail drives
  ops-queue false positives, and the generator does not make it. A rule at
  0.95 precision here separates cleanly from the baseline. It says nothing
  about how many legitimate customers it would alert on in production.
- **No "legitimate but suspicious" label.** Every non-planted row is clean.
  In production many alerts hit customers a reviewer clears. Benchmark
  precision has no counterpart for that bucket, so it cannot bound it.
- **The rules were written against this generator.** The thresholds in
  `detection_rules.py` were tuned for the planted typologies. They make no
  claim about the right tuning for a bank.

Use precision to compare stacks and to catch regressions between releases. A
precision change between two configurations means the detector's separation
from the same synthetic baseline changed. For production questions, treat the
number as a lower bound on how hard the problem is, and get labelled
transaction data from the bank.

What the workload does not measure:

- **Real alert queues.** One baseline distribution and about a dozen planted
  shapes, against a bank's far wider transaction mix.
- **Rule tuning quality.** Thresholds were chosen for reasonable recall on
  this generator. Tuning for another transaction pool is separate work.
- **Rule logic against real typologies.** The rules are illustrative W-rule
  patterns for benchmarking pipeline throughput, not production detectors.
- **Typologies missing from `RULE_TARGETS` and `UNMAPPED_TYPOLOGIES`.** No
  test checks that every typology the manifest emits is in one of them.

## Held-out evaluation

No Level-2 result is published. The registered held-out evaluation and
robustness [looks](glossary.md#look) are deferred.

**Seed 43 is the calibration corpus.** An unset `workload.datagen.seed` on a
`financial` config resolves to 43. Figures from a seed-43 run are in-sample.
When you publish numbers for comparison, cite the seed, and when it is 43,
say so.

What protects the held-out corpora:

- The repository stores only salted hashes of the evaluation and robustness
  seeds (`src/lakebench/spark/data/aml/heldout_hashes.json`), next to
  `aml_preregistration.json`. The pre-registration fixes the gate constants
  and when each look may be taken.
- Config load refuses a held-out seed unless `workload.datagen.corpus_role`
  declares the matching role.
- With the role declared, `run`, `benchmark`, `query`, `reproduce` and the
  `financial` subcommands still refuse the corpus with exit 2 before any
  cluster call. Only `lakebench generate --registered-corpus --yes` writes
  it, and only `scripts/aml_gate.py --registered` scores it.
- The in-run scorer, bronze-verify and the reference job read every manifest
  row and refuse a corpus from a held-out or spent seed, whatever seed the
  config claims.

The held-out protocol itself is maintained internally. What users can rely on
is stated here and in
[AML spec 3.3](benchmarks/aml/seed-policy.md#33-seed-policy), which has every rule,
refusal and exit code.

## The leakage gate and the reference detector

A rule tuned tightly to the generator's own distribution can read high recall
while learning nothing that generalises. Two checks look for that. Neither
runs inside `lakebench run`.

- **Band leakage report** (diagnostic, never fails a job). Per currency, it
  compares baseline and planted payment density just under the reporting
  threshold. It catches the historical case: every USD `micro_structuring`
  payment sat in `[9500, 9999]`, so a rule reading "amount between 9000 and
  10000" scored 100% recall and 99% precision.
- **Reference detector** (the pre-registered fidelity gate). A gradient-boosted
  model on pre-registered per-customer features. Per typology it reports
  out-of-fold average precision. It checks single- and pair-feature shortcuts
  against pre-registered leakage caps. It measures the generator's realism,
  not an architecture.

Run it with `lakebench financial reference-score`. Install
`lakebench-k8s[aml]` to run the detector or the local gate yourself. Verdicts,
outputs and constants are in
[AML spec 8.8](benchmarks/aml/scoring.md#88-band-leakage-report-and-reference-detector).

## Reason codes

Every alert carries `reason_codes`: its rule's base code first, then each
conditional code whose condition holds. Codes reuse cut points the rules
already have. No code changes which alerts a rule raises. Batch scoring
splits each rule's recall and false-positive rate by code
(`financial_scoring.recall_by_code`, `fp_by_code`, `alerts_by_code`). The
code list is in [AML spec 4.2](benchmarks/aml/rules.md#42-detection-rules).
The per-code figures are defined in
[section 8.4](benchmarks/aml/scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails).

## Per-alert evidence caps

W1, W2 (beneficiary kind), W4 and the W5 rescreen cut an alert's
related-payment list so one row cannot grow with the corpus. These are
Lakebench caps. A cut alert can miss planted payments past the cut.
The scoring summary then names the rule in `evidence_capped_alerts_by_rule`
and the bounded typologies in `recall_bounded_by_evidence_cap`. That recall
is bounded by a Lakebench cap, not by the detector alone. Cap sizes are in
[AML spec 7.4](benchmarks/aml/execution-rules.md#74-lakebench-imposed-caps).

## Running an AML pipeline

The loop is `deploy -> generate -> run`. A batch `run` scores recall and
precision after gold-finalize. `lakebench financial score` re-scores on
demand, for example after a replay or a rule change.

An AML config sets `workload.schema: financial`. The shipped example is
[`examples/polaris-iceberg-spark-financial.yaml`](../examples/polaris-iceberg-spark-financial.yaml).
`polaris-iceberg-spark-financial-local.yaml` is a developer sanity check at
scale 1. Any Iceberg recipe works: the schema routes datagen and the pipeline
scripts, not the catalog.

```bash
lakebench deploy   examples/polaris-iceberg-spark-financial.yaml
lakebench generate examples/polaris-iceberg-spark-financial.yaml
lakebench run      examples/polaris-iceberg-spark-financial.yaml

# Optional re-score of a batch run (the bronze prefix is fixed; a continuous
# corpus has one manifest-eNNNN.parquet per epoch)
lakebench financial score \
    examples/polaris-iceberg-spark-financial.yaml \
    --manifest s3a://<bronze-bucket>/pacs008/manifest/manifest.parquet \
    --output   s3a://<gold-bucket>/scoring/rescore/recall.parquet
```

- The inline score writes `s3a://<gold-bucket>/scoring/<run_id>/recall.parquet`
  and a `recall.json` summary.
- The `--manifest` above labels cycle 0 only. With
  `architecture.pipeline.cycles > 1`, later cycles write
  `pacs008/manifest/manifest-cNNN.parquet`, and `financial score` scores only
  the manifest you pass.
- `lakebench financial replay CONFIG --rule W2_structuring --depth-months 60`
  reruns one rule against an Iceberg snapshot from N months ago. It writes
  to the gold alerts table with an `_replay` suffix and never touches
  `gold.alerts`.
- `lakebench financial reproduce CONFIG --alert-id <id>` reruns one batch
  alert's rule on exactly what that run's gold-finalize read. Exit 0 means
  reproduced.

`CONFIG` is the YAML you passed to `deploy`. Both verbs check
`workload.schema=financial` and submit a SparkApplication. Options and exit
codes are in [AML spec 4.4](benchmarks/aml/pipeline.md#44-out-of-run-aml-commands).

Scale sets bronze volume linearly: about 26.7M payments per scale unit, and
9.36 GB of pacs.008 per unit as measured at scale 10. AML datagen is supported to scale 300,
unverified to 800 and refused above. Sizes and measurements are in
[AML spec 3.2](benchmarks/aml/generation.md#32-scale-factor).

### Where gold-finalize spends its time

Per-rule seconds, stage profiles, attribution and `limits.headroom_pct`:
[AML spec 8.6](benchmarks/aml/gold-timing.md#86-gold-finalize-timing).

### The alert set: are two runs' alerts the same?

Two batch runs whose `experiment.results.alert_set` differs are not
comparable: [AML spec 8.7](benchmarks/aml/scoring.md#87-alert-set).

## Continuous recall over covered instances

A continuous run ends by [draining](glossary.md#drain) gold-refresh: the
driver finishes the [tick](glossary.md#tick) it is in, then stops. The scorer then reads the exact silver and gold
snapshots that tick read. It scores `recall_covered` per typology: the
designated-rule hit rate over the instances that tick could have detected
(every participant payment and entity was in sealed silver). It is not the
batch `recall` and is never written under that name
(`financial_scoring.covered`, `mode: "covered"`).

The run reads `not_scored`, with a reason, when the drain did not complete,
a gate failed, or a recorded snapshot was gone. An earlier tick is never
scored instead. The scorecard says whether recall was scored and why not, and shows
`recall_covered` with its coverage, labelled "Recall over covered instances
(uncalibrated, in-sample)" (or the registered look's role). The drain, the tick records and
the time-travel check are in
[AML spec 4.5](benchmarks/aml/pipeline.md#45-continuous-drain-and-tick-records) and [8.4](benchmarks/aml/scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails).

## The transaction-monitoring operations layer

After detection, gold replays a bank's TM work on the alerts with simulated
analyst and investigator decisions. Only a `fail` verdict fails the run:
[AML spec 8.5](benchmarks/aml/tm-operations.md#85-tm-operations). Config
keys: `workload.tm_operations.*` in
[configuration.md](configuration.md#workload----customer-360-and-aml)
(`case_lookback_months` takes 6 to 12).

## Where to look next

- [`detection_rules.py`](../src/lakebench/spark/scripts/detection_rules.py): the nine rules.
- [`aml_queries.py`](../src/lakebench/benchmark/aml_queries.py): the rule-to-typology map and query catalogue.
- [`score_financial.py`](../src/lakebench/spark/scripts/score_financial.py): recall and precision, run by `lakebench run` and `lakebench financial score`.
- [`reference_score.py`](../src/lakebench/aml/reference_score.py): the band leakage gate library.
- [`fidelity_gate.py`](../src/lakebench/aml/fidelity_gate.py): the pre-registered fidelity gate and reference model.
- [`score_financial_reference.py`](../src/lakebench/spark/scripts/score_financial_reference.py): the Spark driver for both gates, called by `lakebench financial reference-score`.
