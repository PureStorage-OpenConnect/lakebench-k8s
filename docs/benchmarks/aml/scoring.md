# AML benchmark: scoring, alert set and reference detector

## 8.4 AML scoring (reported; a batch run without a result fails)

Recall and false-positive figures never decide the verdict. A batch run fails
when its scorer counts zero alerts ([5](correctness.md#5-correctness-contract))
or scoring produced no result, since its answers went unchecked.

- The scorer is `spark/scripts/score_financial.py` (not the frozen reference
  scorer); its `recall.json` is copied into `financial_scoring`.
- It joins `gold.alerts` to the manifest on payment UETRs and writes
  `recall.parquet` and `recall.json` under `s3a://<gold>/scoring/<run_id>/`.
- The manifest URI is a command-line argument. `lakebench run` derives it
  from `s3a://<bronze>/pacs008/manifest/manifest*.parquet`, whose glob covers
  a continuous corpus's one manifest per 24-month epoch
  (`manifest-eNNNN.parquet`).
- The scorecard shows `fp_rate_by_rule` as **Off-target** (by alert id, not
  payment), `txn_precision_by_rule` as **Txn precision**, and `fp_rate` in its
  footer as **Overall off-target rate** beside the total alert count. None is
  a production ops-queue false-positive rate
  ([aml-scoring.md](../../aml-scoring.md)).

**Batch.** Scored over the whole run after gold-finalize:

| Key | Definition |
|---|---|
| `recall` (per typology) | share of planted instances any of whose participant UETRs is in `related_txn_ids` of an alert from a designated rule that ran. No time window. NULL unless the typology's status is `scored` or `partial` (others: `no_rule`, `rule_skipped`, `rule_error`) |
| `incidental_recall` | the same with any rule |
| `fp_rate` | alerts touching no manifest UETR / total alerts; null when the run raised no alerts |
| `fp_rate_by_rule` | 1 - the share of a rule's alerts with a related payment that touch a payment of the rule's target typology |
| `txn_precision_by_rule` | mean over (alert, UETR) pairs of on-target |
| `chance_by_rule`, `random_control_floor` | share of `random` control instances each rule's alerts touch; incidental recall of `random`. Recall at or below these is indistinguishable from chance |
| `recall_by_code` | per designated rule that ran, `{rule: {code: value}}`; 0.0 when no alert carries the code; null for every code when the typology has no instances |
| `recall_by_code`, value | share of the target typology's instances (counted as typology recall counts them) with a planted payment in an alert of that rule carrying the code |
| `fp_by_code` | 1 minus the share of the rule's alerts carrying the code that touch a payment of that typology, over alerts with a related payment as `fp_rate_by_rule` counts them; an alert counts once per code. Null when no such alert carries the code |
| `alerts_by_code` | alerts per code (0 when none carries it) |
| `nonplanted_alerts_by_rule` | per rule with a target typology, alerts touching no payment of it, counted on `gold.alerts` before TM, so no TM cap truncates it. An evidence cap can still make an alert whose planted payments were cut read non-planted (see `evidence_capped_alerts_by_rule`). Diagnostic |
| `customer_count` | customers in `silver.entities`, the denominator for a per-customer non-planted alert rate; null when unreadable. Diagnostic |
| `evidence_capped_alerts_by_rule`, `recall_bounded_by_evidence_cap`, per-typology `bounded_by_evidence_cap` | which rules had an alert cut by a per-alert evidence cap, and which typologies' recall that cap bounds ([7.4](execution-rules.md#74-lakebench-imposed-caps)) |
| Bookkeeping | `typology_counts` (typologies per status); `rules` (status, reason, target, alert count); `subject_customer_check` (planted subjects silver does not hold as customers, whose recall is understated); per typology `workload_category`, `designated_rules`, `instance_count`, `subjects_not_customer` |

- Every alert carries its base code, so the base code's figures are the
  rule's own. Only rules that ran are split by code. When no alert carries a
  code, or the alerts predate the column, the per-code blocks are empty and
  `by_code_status` says why. `reason_code_vocabulary` hashes the codes and the
  cut points they read, naming which codes, meaning what, the figures use. A
  rule's evidence-cap label applies to each of its codes.
- `nonplanted_alerts_by_rule` and `customer_count` feed the published check
  that W5 and W6 non-planted alerts per customer stay flat across scale
  (below).
- When any alert of a rule was cut by an evidence cap, that typology's recall
  is bounded by a Lakebench cap, not the detector alone. Then:
  - the rule is in `evidence_capped_alerts_by_rule`;
  - its typologies are in `recall_bounded_by_evidence_cap`;
  - each such typology's entry in `typologies` names the rule in
    `bounded_by_evidence_cap`.

Recorded: in both 1.6 batch records 8 of 17 typologies were `scored`, 8
`no_rule` and 1 `rule_skipped` (W1, giant component), n=1 each.

**Screening rates across scale.** `scripts/aml_screen_rates.py` reads stored
AML batch records (never a bucket or cluster) and writes
`docs/benchmarks/data/aml_screening_rates.json`.

- Inputs: seed 43's runs at scale 1 and 10, plus the calibration seed's at
  both when the pre-registration's calibration seed is not 43. Each needs an
  observed generator digest, scale and seed (generate in the same namespace
  before the run).
- Output: one n=1 row per seed role (`seed-43` or `calibration`), scale and
  rule, with run id and generator digest, and the ratio scale 10 over scale 1
  from the raw counts.
- Refused:
  - a protected-corpus record;
  - a verdict other than PASSED;
  - a record lacking the counts or in which W5 or W6 did not run;
  - a scale pair from different generators or workload versions;
  - fewer than 50 non-planted W5 plus W6 alerts at scale 1.
- A row with `evidence_capped_alerts` above 0 is an upper bound; a ratio with
  either side cut is marked `bounded_by_evidence_cap`.

**Continuous (covered mode).** After the [drain](../../glossary.md#drain)
([4.5](pipeline.md#45-continuous-drain-and-tick-records)) the scorer runs over
the snapshots the last completed tick read; `financial_scoring.mode` is
`covered`.

- `financial_scoring.covered.typologies[].recall_covered` is the
  designated-hit rate over covered instances only: every participant payment
  was in the sealed transactions at the tick's snapshot, and every
  participant mapped, through `silver.accounts` at that snapshot, to an
  entity in `silver.entities` at that snapshot. `covered_instances`,
  `corpus_instances`, `coverage` and `no_participant_txns` say how much of
  the manifest that was. No key named `recall` is written.
- The scored tick can begin after the window closed (the drain waits for the
  tick in progress, and the window's bucket listing runs first);
  `tick.pinned_after_window_end_s` says by how much.
- False positives and precision count every planted payment in the full
  manifest, so an alert on a planted payment the tick had not yet covered is
  not a false positive. The chance floor uses the covered `random` instances.
- Typologies whose rules are all excluded in continuous mode are in
  `covered.excluded_typologies`. The per-code keys,
  `nonplanted_alerts_by_rule` and `customer_count` are batch only.
- `financial_scoring.status` is `not_scored`, with a `reason`, when:
  - the drain did not complete, or the run failed a gate;
  - the last tick's record is missing or names a snapshot as `unknown` or
    `none`;
  - the tick still had a pending rule;
  - a recorded snapshot expired before scoring;
  - the score job failed or was interrupted. An earlier tick is never scored instead, and the
  current tables never stand in for a recorded snapshot.
- The scored tick's alert-set fingerprint is recorded in
  `experiment.results.alert_set_continuous` as a diagnostic, never a result:
  continuous alerts depend on when ticks ran.
- The scorecard says whether recall was scored and why not, and shows
  `recall_covered` with its coverage, labelled "Recall over covered instances
  (uncalibrated, in-sample)" (or the [registered look](../../glossary.md#look)'s role).

## 8.7 Alert set

Two AML batch runs are compared on results before speed, and the benchmark
queries alone do not show that both raised the same alerts. After its last
write to `gold.alerts`, gold-finalize fingerprints the run's alerts in Spark
and prints one `LB_ALERT_SET` line, kept as `experiment.results.alert_set`:

- An alert is `(rule_id, entity_id, alert_ts)`: rule, subject and the event
  time the rule derived from the data. `alert_id`, `run_id`, `detected_ts`
  and every other column are left out, so two runs raising the same alerts
  read equal.
- `by_rule` holds each rule's alert count (`rows`) and an order-independent
  hash (`h`, an exact sum of one xxhash64 per alert); top-level `rows` and `h`
  are their totals. `spec` (`as1`) and `cols_sha` name the definition and
  column types. The value is the same on the Spark 4.0 and 4.1 lines.

A different alert set is a different result: two runs where any rule's count
or hash differs are not comparable.

- Consider an AML batch record written by 1.7 (exp2, or exp1 with
  `v2_unavailable`) with no alert set: the fingerprint failed (reason in
  `results.alert_set_unavailable`) or gold-finalize did not run. It cannot
  show its results match another run's; do not compare it on query results
  alone.
  `reproduce` refuses such a run; it does not yet compare alert sets with its
  package.
- Records from 1.6 have no alert set.
- A rule that ran and raised no alert is absent from `by_rule`, like a rule
  that did not run; `experiment.rules` records which rules ran.
- Continuous runs never fingerprint inside a tick (a full scan inside time to
  detect). Their alert set is taken once after the drain
  (`results.alert_set_continuous`) and is diagnostic only.

## 8.8 Band-leakage report and reference detector

A rule tuned tightly to the distribution the generator plants can read high
recall while learning nothing that generalises. Two checks address this: a
legacy band-density report and the pre-registered fidelity gate (the
reference detector). Both run in `score_financial_reference.py`, submitted by
`lakebench financial reference-score`
([4.4](pipeline.md#44-out-of-run-aml-commands)); `lakebench run` does not run
it. `src/lakebench/aml/reference_score.py` supplies the band leakage gate.

**Band leakage report** (legacy; never fails the job). It runs before the
fidelity gate and writes `leakage_report.parquet`: per currency, baseline and
typology payment counts inside `[95%, 99.99%]` of the reporting threshold
(`structuring_band` in `datagen_rs/src/amounts.rs`), reported as
`baseline / typology`. Structured amounts are now drawn across `[60%, 100%)`
(`STRUCTURED_MAX_DEPTH = 0.4`, `structuring_amount`), so most planted rows fall
outside the window and the ratios are loose.

| Verdict | Meaning |
|---|---|
| `pass` | baseline density is at least 10% of typology density in this band |
| `leaking` | ratio below 10%; a rule filtering only on this band would read high recall from a label indicator |
| `no_typology` | no typology rows in this band; no leakage claim on its own |

- The threshold ships at 10% (`DEFAULT_LEAKAGE_RATIO`), tunable through the
  Python API's `threshold_ratio` and the CLI's `--leakage-threshold`.
- The report's `overall_pass` goes into `aml_gate_report.json` as
  `band_leakage_overall_pass`. Neither it nor the parquet rows fail the job:
  only the fidelity gate's own verdict (`error` or `empty_frame`, or a
  `counts_only` mismatch, checked in `score_financial_reference.py:main`)
  raises `SystemExit`. The pre-registered leakage caps that gate a corpus
  live in the fidelity gate.
- It catches a rule that filters on the band alone: when every planted
  payment sat in `[9500, 9999]` USD, "amount between 9000 and 10000" read
  100% recall and 99% precision.

**Reference detector.** After the band report, the script builds the
pre-registered per-customer features from silver
(`spark/scripts/aml_features.py`):

- the 21 `FEATURE_COLUMNS`: activity counts and gaps, amount level and
  spread, counterparty count, cross-border, round-amount, structuring-band
  and high-risk-corridor fractions, the 24 h burst, overnight, weekend and
  hour-of-day entropy, `home_country_high_risk`, `customer_type`, `crr_tier`;
- plus the four `HISTORY_FEATURE_COLUMNS` of the monthly unit.

It labels each customer by manifest participation and runs `evaluate_gate`
in `src/lakebench/aml/fidelity_gate.py`. Only customers are scored.

- The model is scikit-learn's `HistGradientBoostingClassifier`. Its
  hyperparameters, the feature list, the unit of scoring (customer by UTC
  calendar month), the cross-validation folds and every gate constant come
  from `aml_preregistration.json`.
- Per in-scope typology the gate reports out-of-fold average precision with a
  bootstrap confidence interval and the positive count. It checks single- and
  pair-feature shortcuts, a nuisance-only model and a nuisance ablation
  against the pre-registered leakage caps.
- It writes `aml_gate_report.json` (the full report) and
  `reference_metrics.parquet` (one aggregate row, one row per typology) under
  the output prefix.
- scikit-learn is not on the Spark image; the job installs it per run at the
  versions pinned in `REFERENCE_PY_DEPS`. To run the detector or the local
  gate yourself, `pip install "lakebench-k8s[aml]"`, which pins the same
  versions.

| Verdict | Meaning |
|---|---|
| `ok` | the gate ran; per-typology results and passes are in `aml_gate_report.json` |
| `counts_only` | the script's `--counts-only` option: per typology only counts and prevalence, no model |
| `no_sklearn` | scikit-learn not importable on the driver; the band report still ran and the job does not fail |
| `empty_frame` / `error` | no customer could be scored, or the gate failed; outputs are written and the job fails |
