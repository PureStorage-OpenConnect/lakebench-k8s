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
- With the role declared, `run`, `benchmark`, `query` and the
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

## See also

- [AML spec 8.8](benchmarks/aml/scoring.md#88-band-leakage-report-and-reference-detector): the leakage gate and reference detector (`lakebench financial reference-score`).
- [AML spec 4.2](benchmarks/aml/rules.md#42-detection-rules): reason codes and the rule list.
- [AML spec 7.4](benchmarks/aml/execution-rules.md#74-lakebench-imposed-caps): per-alert evidence caps.
- [AML spec 4.4](benchmarks/aml/pipeline.md#44-out-of-run-aml-commands): `replay`, `reproduce`, `score` and `reference-score` commands.
- [AML spec 8.4](benchmarks/aml/scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails): continuous recall over covered instances.
- [AML spec 8.5](benchmarks/aml/tm-operations.md#85-tm-operations): the TM operations layer.
- [AML spec 8.6](benchmarks/aml/gold-timing.md#86-gold-finalize-timing): gold-finalize timing.
- [AML spec 8.7](benchmarks/aml/scoring.md#87-alert-set): the alert set and comparability.
- [AML spec 3.2](benchmarks/aml/generation.md#32-scale-factor): scale factor and sizes.
