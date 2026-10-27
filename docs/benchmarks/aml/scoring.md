# AML benchmark: scoring, alert set and reference detector

## 8.4 AML scoring (reported; a batch run without a result fails)

Recall and false-positive figures never decide the verdict. A batch run fails
when its scorer counts zero alerts ([5](correctness.md#5-correctness-contract))
or scoring produced no result.

- The scorer runs as SQL through the Trino query engine; its `recall.json`
  is assembled into `financial_scoring`.
- It joins `gold.alerts` to the `bronze.manifest` table on payment UETRs and
  writes `recall.json` under `s3a://<gold>/scoring/<run_id>/`. The counts come
  from integer-returning statements; the ratios and null rules are computed on
  the client, so a figure never depends on a floating-point last bit.
- `bronze.manifest` is the registered Iceberg copy of the generator's manifest
  (bronze-verify registers it, gold-refresh re-registers it as the corpus
  grows). Scoring reads it at its current snapshot and fails closed when it is
  missing. `bronze.accounts` holds the generator's account-to-holder map
  (`holder_entity_id`, `iban`), through which a planted participant's id
  resolves to its silver entity key.
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
  code, or the alerts have no `reason_codes` column, the per-code blocks are empty and
  `by_code_status` says why. `reason_code_vocabulary` hashes the codes and the
  cut points they read. A rule's evidence-cap label applies to each of its
  codes.
- `nonplanted_alerts_by_rule` and `customer_count` feed the published check
  that W5 and W6 non-planted alerts per customer stay flat across scale
  (below).
- When an evidence cap cut any alert of a rule, that typology's recall is
  bounded by a Lakebench cap, not the detector alone. Then:
  - the rule is in `evidence_capped_alerts_by_rule`;
  - its typologies are in `recall_bounded_by_evidence_cap`;
  - each such typology's entry in `typologies` names the rule in
    `bounded_by_evidence_cap`.

**Screening rates across scale.** `scripts/aml_screen_rates.py` reads stored
AML batch records, never a bucket or cluster, and writes
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

**Continuous (covered mode).** After the window closes and the streams
stop, the scorer runs over the snapshots the last tick that completed and
committed inside the window read ([4.5](pipeline.md#45-continuous-tick-records));
`financial_scoring.mode` is `covered`. Each table is read at the snapshot
the tick pinned (`VERSION AS OF`), so a later tick rewriting `gold.alerts`
does not reach the score.

- `financial_scoring.covered.typologies[].recall_covered` is the
  designated-hit rate over covered instances only: every participant payment
  was in the sealed transactions at the tick's snapshot, and every
  participant mapped, through `silver.accounts` at that snapshot, to an
  entity in `silver.entities` at that snapshot. `covered_instances`,
  `corpus_instances`, `coverage` and `no_participant_txns` say how much of
  the manifest that was. No key named `recall` is written.
- False positives and precision count every planted payment in the full
  manifest, so an alert on a planted payment the tick had not yet covered is
  not a false positive. The chance floor uses the covered `random` instances.
- Typologies whose rules are all excluded in continuous mode are in
  `covered.excluded_typologies`. The per-code keys,
  `nonplanted_alerts_by_rule` and `customer_count` are batch only.
- `financial_scoring.status` is `not_scored`, with a `reason`, when:
  - the run failed a gate;
  - no tick completed and committed inside the window, or the last one
    names a snapshot as `unknown` or `none`;
  - the tick still had a pending rule;
  - a recorded snapshot expired before scoring;
  - the score job failed or was interrupted.

  An earlier tick is never scored instead, and the current tables never stand
  in for a recorded snapshot.
- The scored tick's alert-set fingerprint is recorded in
  `experiment.results.alert_set_continuous` as a diagnostic, never a result:
  continuous alerts depend on when ticks ran.
- The scorecard shows whether recall was scored (and why not) and
  `recall_covered` with its coverage, labelled "Recall over covered instances
  (uncalibrated, in-sample)" or the [registered look](../../glossary.md#look)'s
  role.

## 8.7 Alert set

Two AML batch runs are compared on results before speed, and the benchmark
queries alone do not show that both raised the same alerts. After its last
write to `gold.alerts`, gold-finalize fingerprints the run's alerts in Spark
and prints one `LB_ALERT_SET` line, kept as `experiment.results.alert_set`.

- An alert is `(rule_id, entity_id, alert_ts)`: rule, subject and the event time
  the rule derived from the data. `alert_id`, `run_id`, `detected_ts` and every
  other column are left out, so two runs raising the same alerts read equal.
- `by_rule` holds each rule's alert count (`rows`) and an order-independent hash
  (`h`, an exact sum of one xxhash64 per alert); top-level `rows` and `h` are
  their totals. `spec` (`as1`) and `cols_sha` name the definition and column
  types. The value is the same on the Spark 4.0 and 4.1 lines.

A different alert set is a different result: two runs where any rule's count
or hash differs are not comparable.

- An AML batch record with no alert set means the fingerprint failed (reason
  in `results.alert_set_unavailable`) or gold-finalize did not run. Do not compare it with another run on query
  results alone.
- A rule that ran and raised no alert is absent from `by_rule`, like a rule
  that did not run; `experiment.rules` records which rules ran.
- Continuous runs never fingerprint inside a tick, since a full scan would fall
  inside time to detect. The alert set is taken once after the window closes
  (`results.alert_set_continuous`) and is diagnostic only.

## 8.8 Band-leakage report and reference detector

A rule tuned to the distribution the generator plants can read high recall
while learning nothing that generalises. `lakebench financial reference-score`
([4.4](pipeline.md#44-out-of-run-aml-commands)) runs two checks against this,
a band leakage report and the pre-registered fidelity gate (the reference
detector). `lakebench run` runs neither.

**Band leakage report** (never fails the job). `leakage_report.parquet` gives,
per currency, baseline over typology payment density in `[95%, 99.99%]` of the
reporting threshold, `leaking` below `--leakage-threshold` (default 10%); its
`overall_pass` is copied to `aml_gate_report.json` as
`band_leakage_overall_pass`.

**Reference detector.** The job builds the pre-registered per-customer
features from silver:

- 21 features: activity counts and gaps, amount level and spread,
  counterparty count, cross-border, round-amount, structuring-band and
  high-risk-corridor fractions, the 24 h burst, overnight, weekend and
  hour-of-day entropy, `home_country_high_risk`, `customer_type`, `crr_tier`;
- plus four history features of the monthly unit.

It labels each customer by manifest participation and runs the fidelity gate.
Only customers are scored.

- The model is scikit-learn's `HistGradientBoostingClassifier`. Its
  hyperparameters, the feature list, the unit of scoring (customer by UTC
  calendar month), the cross-validation folds and every gate constant come
  from the pre-registration file, `aml_preregistration.json`.
- Per in-scope typology the gate reports out-of-fold average precision with a
  bootstrap confidence interval and the positive count. It checks single- and
  pair-feature shortcuts, a nuisance-only model and a nuisance ablation
  against the pre-registered leakage caps.
- It writes `aml_gate_report.json` (the full report) and
  `reference_metrics.parquet` (one aggregate row, one row per typology) under
  the output prefix.
- scikit-learn is not on the Spark image; the job installs it per run at
  pinned versions. To run the detector or the local gate yourself,
  `pip install "lakebench-k8s[aml]"`, which pins the same versions.

| Verdict | Meaning |
|---|---|
| `ok` | the gate ran; per-typology results and passes are in `aml_gate_report.json` |
| `counts_only` | the script's `--counts-only` option: per typology only counts and prevalence, no model |
| `no_sklearn` | scikit-learn not importable on the driver; the band report still ran and the job does not fail |
| `empty_frame` / `error` | no customer could be scored, or the gate failed; outputs are written and the job fails |
