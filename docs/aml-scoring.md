# AML: Financial-crime / AML Workload

Lakebench's AML workload measures a lakehouse stack against a
Financial-crime / Anti-Money-Laundering (AML) detection pipeline: read
pacs.008 wire messages from bronze, build silver entity / account /
counterparty tables, aggregate risk in gold, then score nine W-rule
detectors (W1-W8 and W17) against synthetic planted typologies. This document explains
what the scorecard actually measures, what it does not measure, and how
to run one end-to-end.

## What you get on the scorecard

**Recall and precision are uncalibrated.** No held-out Level-2 result is
published. On the default seed (43, the calibration corpus the generator,
rule thresholds and reference features were developed on) they are
in-sample: they say what the pipeline does on that corpus, not how the
rules generalise. The registered held-out evaluation and robustness looks
remain deferred.

The two headline numbers are per-typology **recall** and per-rule
**precision** against the planted typology set. The authoritative
runtime path that produces both is the Spark job
[`src/lakebench/spark/scripts/score_financial.py`](../src/lakebench/spark/scripts/score_financial.py),
which a batch `lakebench run` submits after gold-finalize
(`lakebench financial score` reruns it on demand). It joins
`gold.alerts` against the datagen manifest on transaction UETRs, with
no time window, and writes `recall.parquet` and a `recall.json`
summary under `s3a://<gold>/scoring/<run_id>/`. The manifest URI is
passed in on the command line; the default path template is
`s3a://<bronze>/pacs008/manifest/manifest*.parquet`, and `lakebench
run` derives the URI from that template. Recall counts only alerts
from a typology's designated rules; alerts from other rules are
reported separately as `incidental_recall`, and the `random` control
typology's incidental recall is the chance floor. Precision is
reported per rule as the share of its alerts touching no payment of
its target typology (`fp_rate_by_rule` in `recall.json`, shown as the
scorecard's **Off-target** column, alerts by alert id, not by payment)
and as transaction-level precision (`txn_precision_by_rule`, shown as
**Txn precision**, weighted by payments). A single overall rate is
shown in the scorecard footer as **Overall off-target rate** with the
total alert count beside it. None of these are a production ops-queue
false-positive rate; see the caveat section below.

[`src/lakebench/benchmark/aml_queries.py`](../src/lakebench/benchmark/aml_queries.py)
holds the rule-to-typology mapping (`RULE_TARGETS`, `UNMAPPED_TYPOLOGIES`)
and a separate catalogue of Trino SQL templates for per-rule detect,
precision, recall and **pattern-span** queries plus four aggregates,
among them `aggregate_typology_coverage` (does at least one W-rule
fire on each planted typology) and `aggregate_reference_vs_rule`
(rule recall beside the reference model's per-typology row in
`reference_metrics.parquet`). Those SQL templates use a 7-day window
and are called only from the unit tests
(`load_aml_queries`); no CLI command executes them, and they do NOT
produce the published recall or precision. If a doc, script or memo
cites a 7-day-window recall figure, it is wrong: the shipped numbers
come from `score_financial.py` and carry no window at all. Pattern-span
is a description of the injected data, not a platform measurement,
and the runtime scorer does not compute it -- see the metric-trust
caveats below before quoting it.

The rule set:

| Rule | Targets typology | What it detects |
|---|---|---|
| W1_connected_components | gather_scatter | multi-entity graph clusters |
| W2_structuring | micro_structuring | 3+ structuring-band transactions in 24 h, two kinds: per originator (`structuring`, tumbling day) and per beneficiary from 2+ senders (`structuring_beneficiary`, sliding 24 h) |
| W3_round_tripping | cycle | funds returning to the originator through 2-5 transfers within 30 days |
| W4_risk_propagation | rapid_layering | pass-through of 80%+ within 6 h |
| W17_layering_chain | stack | open chains of 3+ transfers, each forwarding 80-100% of the previous within 7 days |
| W7_cross_border_high_risk | corridor_high_risk | cross-border payments into a FATF black / grey list country or one of the generator's synthetic high-risk corridor countries |
| W8_dormant_reactivation | dormant_reactivation | inactive account, sudden large flow |
| W5_sanctions_match | sanctions_match | fuzzy screen of payment beneficiaries against the corpus's dated sanctions list, at transaction time and as a rescreen when a list version is published |
| W6_pep_counterparty | pep_match | the same fuzzy screen against the corpus's PEP list; MED priority at $10,000 or more, LOW below |

### Sanctions and PEP screening track

The generator publishes a synthetic, dated watchlist with every corpus
(`bronze/watchlist.parquet`): a sanctions list in two versions (version 1 at
the corpus start, version 2 adding about a quarter more entries three
quarters of the way through) and a PEP list. Entries carry a name, aliases,
country and town; nothing marks which entries were paid. Listed parties are
external counterparties, never the bank's customers and never in the party
master.

About 70% of listed parties are paid by one or two customers, one to three
payments each. Each payer relationship pays one external account whose
creditor name is fixed: the list spelling, an alias, a token-order swap, a
one-letter typo, a different romanisation or (companies) the legal suffix
dropped, so an exact join on the list name misses most of them. About a
fifth of those accounts are in another country than the list entry, so a
screen that requires the country to match pays for it in recall.
Version-2 parties are paid only before their listing, so only the rescreen
finds them. Namesake decoys (another middle name or line of business, a
one-letter-different name in the same country, or the same name in another
country) are paid too and are not in the manifest, so a loose screen pays
for it in precision. A background of ordinary external payees, twenty per
list entry, with the same name shapes, one to three payments each and no
party or account master record, keeps the planted payments from standing
out as "rare external payees": on a seed-43 calibration corpus that
heuristic reaches 3% precision.

Ground truth is the manifest: one `sanctions_match` or `pep_match` instance
per (customer, listed party), its UETRs the customer's payments to that
party, with `list_id`, `list_version`, `detectable_by` (`transaction_screen`
or `rescreen`), `name_variant` and `account_country` (`listed` or `other`)
in `injection_parameters`. Recall and precision are computed exactly as for
the behavioural rules. Transaction-level precision is weighted by payments,
so one heavily paid world entity whose name happens to sit near a list
entry (world persons carry a middle initial, list entries a full middle
name) can dominate it; read it beside the alert count.

The screen is bounded on purpose. Soundex codes of token pairs are used only
to block candidate pairs; the match itself is Levenshtein similarity of at
least 0.85 over normalized, token-sorted names (`SCREEN_SIMILARITY_MIN`,
self-chosen), with the entry's country as the only secondary identifier.
There is no phonetic similarity scoring, no date of birth and no identifier
matching; this is a benchmark workload, not a screening product. W6 screens
every payment and uses $10,000 only to order the queue, so its recall
measures the screen, not the amount distribution. In continuous mode only
W2, W3, W4 and W17 run; W1, W5, W6, W7 and W8 are recorded as skipped
("not run").

The high-risk corridor list W7 uses alongside the FATF list
(`synthetic_corridors.json`) is synthetic and is the same country pool the
generator draws `corridor_high_risk` participants from: no generator home
country is on the June 2026 FATF lists. W7's recall on that typology is
therefore partly by construction (the rule uses the generator's corridor
list, as a bank uses its own corridor list), and neither its recall nor its
alert volume is comparable with 1.5.

W9-W16 are workload ids the spec reserves (writeback, reproduce, ingest, the
ML workloads), which is why the layering-chain rule is W17. W3 and W17 skip
with "not run" (`path-cap`) when their path search would not fit the job's
scratch; the budget is read from `LB_PATH_SEARCH_MAX_ROWS` or derived from
the executor count and scratch size. A `path-cap` skip, like W1's
`vertex-cap`, is a Lakebench cap: the run can still pass, and the skip is
labelled in `limits.bound` and in the verdict's `rule_caps` qualifier (see
"What a PASSED verdict asserts" in [benchmarking.md](benchmarking.md)).

Every planted typology that no shipped rule targets is documented in
`UNMAPPED_TYPOLOGIES` in `aml_queries.py` with a one-line reason. That
list is the difference between "we plant this but no detector scores
it" and "we forgot to hook this up." Recall for an unmapped typology is
untestable rather than zero.

### How three behavioural typologies are planted (v1.5 realism rework)

The reference-model calibration check found `micro_structuring` and
`dormant_reactivation` too easy for the reference model and
`corridor_high_risk` just under the band floor. Each was planted in a
shape real cases do not have. The planting changed; the features did
not. Every parameter below is self-chosen unless a source is named, and
none is set from a detection rule's threshold. Rule recall
moves as a consequence and is reported, not targeted.

| Typology | Before | Now | Why |
|---|---|---|---|
| `micro_structuring` | 8 distinct senders each paid the collector once within 3 days, every amount in `[0.95, 0.9999]` of the reporting threshold | A crew of 3 to 8 depositors (some deposit more than once) pays the collector 8 times over 3 to 21 days. 3 to 8 of the payments are structured; the rest are the depositors' own ordinary amounts. Structured amounts sit `threshold x 0.4 x (1 - sqrt(u))` below the threshold: a triangular density highest at the threshold and finite there, always under it, with no step at W2's 90% band floor, and about 44% inside W2's band | Structurers reuse a few people ("smurfing") and run campaigns over weeks so no single day shows the pattern; the FFIEC BSA/AML manual describes both "just under" amounts and varied amounts meant to avoid an obvious pattern. The old shape made the band fraction plus the counterparty count a two-feature label (the calibration pair reached average precision 0.641 against the reference model, which is exactly the shortcut the leakage caps forbid) |
| `dormant_reactivation` | Dormancy log-uniform 60 to 365 days, then 4 sends within 2 days | Dormancy log-uniform 45 to 365 days; two thirds of the reactivations are sudden (2 days), one third is the account coming back into use over 4 to 10 days (kept short so few reactivations straddle a month boundary, where the monthly unit would see the burst without its gap). Amounts stay the account's own draws | A 60-day floor put every episode beyond almost any natural quiet spell; an account sending about once a month goes 45 days without a send about one time in five, so the short end now overlaps normal gaps. Not every reactivation is a single burst |
| `corridor_high_risk` | One payment between two residents of the higher-risk pool | A run of 2 to 4 payments from the subject to one counterparty in the pool over 2 to 5 weeks, amounts from the sender's own distribution | W7's "corridors to high-risk jurisdictions" is about where an account's money goes; one payment among dozens cannot show that. The typology spends the same total rows as before, so density (D11) is unchanged. A run over weeks often crosses a month boundary (about 38% of runs split their payments across two months); the monthly unit labels the last month and excludes the earlier one, so a split run shows fewer corridor payments in its labelled month. That is the unit seeing a real flow, not a planting choice |

The three typologies draw their amounts from an instance-keyed stream,
and the shared amount stream replays the draws their old rows took
(`amounts::own_amount_stream`), so the other typologies keep their
amounts, times and participants. The moved dormancy windows are the one
coupling left: they change which of a dormant account's own base and
other-typology sends are suppressed, and the resulting one-row changes
in the planted total shift base rows' calendar positions very slightly.
Measured against the old generator at seed 7777, scale 0.05: 301 of 302
other-typology instances identical (the other gained a row a dormancy
window used to drop) and 99.94% of baseline rows identical.

### Baseline timing: scheduled and bursty senders

Every baseline send used to be an independent activity-weighted draw of its
originator, so each account sent as a memoryless process on the calendar and
its gap coefficient of variation sat near 1 (in the calibration cohort,
0.004% of accounts below 0.5, 58% above 1.0; the pre-registered target is
at least 15% on each side). Real payment behaviour is a mixture: some
accounts pay mostly on a steady cadence (standing orders, bills, payroll
and supplier runs) and others are bursty.

`datagen_rs/src/regular.rs` makes 30% of accounts "scheduled": 75 to 95% of
the account's expected sends follow a steady cadence (one payment every 1/K
of calendar mass, rolled to the next business day), and the rest stay random
draws. Spacing the cadence in calendar mass rather than wall-clock time
keeps scheduled rows on the corpus's day-of-week, salary-day and
quarter-end shape, which typology rows share; evenly spaced wall-clock
times put fewer rows on salary days and more on Mondays, and planted rows
would then stand out by date.
Only timing changes. The account's expected total sends, its amounts
(persona draws) and its counterparty draws (the same ring and extended-band
draw as a random row) are unchanged, as are every typology's rows and the
corpus row count. A dormant account's cadence stops during its dormancy.
Scheduled events are computed per file from (seed, uid), so memory stays
O(accounts) at any scale and files still depend only on (seed, file).
Parameters are self-chosen; the primary source for the mixture split is
still pending a citation.

## Why benchmark precision is not the FP rate ops teams care about

The precision numbers in this benchmark answer a narrow question:
against a synthetic bronze layer where every non-typology row is a
log-normal baseline draw, what fraction of a rule's alerts land on
manifest-tagged typology rows? However high that reads, it is not
comparable with the 90 %+ false-positive rate that Tier-1 AML
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
has learned nothing generalisable. Two independent checks address this
risk: a legacy band-density report and the pre-registered fidelity
gate (the reference detector).

### Band leakage report (legacy, non-failing)

`score_financial_reference.py` emits `leakage_report.parquet` before the
fidelity gate runs. For each currency it counts baseline transactions
and typology transactions inside a narrow `[95%, 99.99%]` window of
the reporting threshold (`structuring_band` in
`datagen_rs/src/amounts.rs`) and reports the ratio
`baseline / typology`. That narrow window is the shape the original
`micro_structuring` planting emitted every payment into; the current
generator draws structured amounts across a wider `[60%, 100%)` range
of the threshold (`STRUCTURED_MAX_DEPTH = 0.4`,
`structuring_amount`), so most planted rows now fall outside the
window and the report's ratios are looser than they were.

| Verdict | Meaning |
|---|---|
| `pass` | baseline density is at least 10 % of typology density in this narrow band. |
| `leaking` | ratio below 10 % in this narrow band. Any rule that filters just on it would read high recall from a label indicator. |
| `no_typology` | no typology rows in this band -- carries no leakage claim on its own. |

The report's `overall_pass` flag is written into
`aml_gate_report.json` as `band_leakage_overall_pass` and its rows land
in `leakage_report.parquet`. Neither value fails the reference-score
job: only the fidelity gate's own verdict (`error` or `empty_frame`, or
a `counts_only` mismatch, at `score_financial_reference.py:685-691`)
raises `SystemExit`. The band report is a diagnostic on the historical
shape, kept for backward compatibility; the pre-registered leakage
caps that actually gate a corpus live in the fidelity gate below.

The threshold ships at 10 % (`DEFAULT_LEAKAGE_RATIO`), tunable via the
Python API's `threshold_ratio` argument.

The historical example this report catches: the datagen originally
emitted every USD `micro_structuring` transaction in the `[9500, 9999]`
range while the baseline log-normal produced essentially none. A rule
reading "amount between 9000 and 10000" reported 100 % recall and 99 %
precision. The current wider planting range dilutes the ratio but the
report still shows it.

### Reference detector (the pre-registered fidelity gate)

`lakebench financial reference-score CONFIG --manifest ... --output-prefix ...`
submits
[`score_financial_reference.py`](../src/lakebench/spark/scripts/score_financial_reference.py);
`lakebench run` does not run it. After the band leakage gate, the script
builds the pre-registered per-customer features from silver
([`spark/scripts/aml_features.py`](../src/lakebench/spark/scripts/aml_features.py)):
the 21 `FEATURE_COLUMNS` (activity counts and gaps, amount level and
spread, counterparty count, cross-border, round-amount, structuring-band
and high-risk-corridor fractions, the 24 h burst, overnight, weekend and
hour-of-day entropy, `home_country_high_risk`, `customer_type`, `crr_tier`)
plus the four `HISTORY_FEATURE_COLUMNS` of the monthly unit. It labels each
customer by manifest participation and runs `evaluate_gate` in
[`src/lakebench/aml/fidelity_gate.py`](../src/lakebench/aml/fidelity_gate.py).
Only customers are scored.

The reference model is scikit-learn's `HistGradientBoostingClassifier`. Its
hyperparameters, the feature list, the unit of scoring (customer by UTC
calendar month), the cross-validation folds and every gate constant come
from `aml_preregistration.json`. Per in-scope typology the gate reports
out-of-fold average precision with a bootstrap confidence interval and the
positive count, and checks single- and pair-feature shortcuts, a
nuisance-only model and a nuisance ablation against the pre-registered
leakage caps. It writes `aml_gate_report.json` (the full report) and
`reference_metrics.parquet` (one aggregate row and one row per typology)
under the output prefix. scikit-learn is not on the Spark image; the job
installs it per run, at the versions pinned in `REFERENCE_PY_DEPS`. To run
the detector or the local gate yourself, install
`pip install "lakebench-k8s[aml]"`, which pins the same versions.

The older `train_reference_gbt` in `src/lakebench/aml/reference_score.py`
(a `GradientBoostingClassifier` on five amount and hour features) is no
longer called by any driver; that module now supplies the band leakage
gate.

The report verdicts:

| Verdict | Meaning |
|---|---|
| `ok` | the gate ran; per-typology results and passes are in `aml_gate_report.json`. |
| `counts_only` | the script's `--counts-only` option: per typology only counts and prevalence, no model. |
| `no_sklearn` | scikit-learn was not importable on the driver. The band leakage gate still ran; the job does not fail. |
| `empty_frame` / `error` | no customer could be scored, or the gate failed. Outputs are written and the job fails. |

## Held-out evaluation

v1.6 publishes no Level-2 result: the calibration corpora, Level-2 scoring
and the registered held-out looks are deferred to v1.7. When a Level-2
result is published, it will be measured on held-out corpora that are never
used during development. The pre-registered gate constants and the rules for
when the one-shot evaluation and robustness runs may be taken are fixed in
`src/lakebench/spark/data/aml/aml_preregistration.json`; the evaluation and
robustness seeds are recorded only as salted hashes in
`src/lakebench/spark/data/aml/heldout_hashes.json`. A registered look's
seed is given to its config as `workload.datagen.seed` with the matching
`corpus_role`; on the cluster it travels only through a Secret in the
deployment's namespace (it is in no Job argument or Spark spec), and
`scripts/aml_gate.py` reads it from `--seed-file`. Registered looks need the
datagen image built for v1.7; an older image refuses a registered corpus. Maintainers:
the full protocol is `docs/internal/aml-protocol.md` in the source repository.

**Seed 43 is the calibration corpus.** If you leave
`workload.datagen.seed` unset for a `financial` config, or you set it
to 43 explicitly, you are running against the seed the generator, the
rule thresholds and the reference features were tuned against. Recall
and precision numbers from a seed-43 run are in-sample: they say what
the pipeline does on the corpus it was developed on, not what it does
on a corpus it has never seen. The held-out evaluation and robustness
seeds are registered as salted hashes in `heldout_hashes.json`, next to
`aml_preregistration.json`, and are refused at config load unless
`workload.datagen.corpus_role` declares the matching role. Even then,
every command that reads or scores data (`run`, `benchmark`, `query`,
`reproduce` and the `financial` subcommands) refuses the
protected corpus with exit 2 before any cluster call: its corpus is
generated only with `lakebench generate --registered-corpus --yes`, which
records the attempt in `~/.lakebench/aml_corpora.jsonl` first, and scored
only by `scripts/aml_gate.py --registered`, which records the look. A
financial config whose bronze prefix this host generated a registered
corpus into is refused the same way. So accidentally scoring against them
is not possible from Lakebench (a hand-made datagen Job is caught by the
scorers' manifest checks below). The in-run scorer
(`score-financial`) reads every manifest row too and refuses a corpus any
of whose rows come from a held-out or spent seed. The reference job
also recovers the corpus seed from every manifest row's instance seed
and refuses a corpus whose manifest comes, wholly or partly, from a spent
or held-out seed it was not declared for, whatever seed the deployment
claims. The check reads the manifest only: it does not tie the
transactions under the bronze prefix to it, so transaction files from
another generator run left in the same prefix are not detected. Generate
each corpus into its own prefix.

The salt in `heldout_hashes.json` is public, so a hash hides a seed only
when the seed is drawn uniformly from 63 bits. The current 8-digit seeds
are recovered from their hashes in seconds (they are already public, so
the hash records their role rather than hiding them). A held-out seed
added later must be a uniform 63-bit draw for its hash to hide it.
Numbers you publish for comparison with other stacks should cite the
seed the run used and, when it is 43, say so.

## Reason codes

Every alert in `gold.alerts` carries `reason_codes` (the last column): its
rule's base code first, then each code below whose condition holds on the
alert. A code never changes which alerts a rule raises. The conditional codes
reuse cut points the rules already have (the HIGH priority threshold, the
screen's exact/fuzzy split, the rescreen pass, the corridor list's risk
tier); none is a threshold of its own.

| Rule | Base code | Conditional codes |
|---|---|---|
| W1_connected_components | `W1_COMPONENT` | `W1_LARGE_COMPONENT` (component of 8 or more entities, HIGH priority) |
| W2_structuring | `W2_SUB_THRESHOLD_BURST` | `W2_BENEFICIARY_FAN_IN` (beneficiary kind), `W2_HIGH_COUNT` (6 or more in-band payments, HIGH) |
| W3_round_tripping | `W3_CYCLE` | `W3_LONG_CYCLE` (4 or more hops, HIGH) |
| W4_risk_propagation | `W4_FAST_PASS_THROUGH` | `W4_MULTI_CHAIN` (3 or more chains, HIGH) |
| W5_sanctions_match | `W5_SANCTIONS_HIT` | `W5_EXACT`, `W5_FUZZY` (name match), `W5_RESCREEN` (raised by a list version) |
| W6_pep_counterparty | `W6_PEP_HIT` | `W6_EXACT`, `W6_FUZZY` |
| W7_cross_border_high_risk | `W7_HIGH_RISK_CORRIDOR` | `W7_FATF_BLACK`, `W7_FATF_GREY`, `W7_SYNTHETIC_CORRIDOR` |
| W8_dormant_reactivation | `W8_DORMANCY_GAP` | none |
| W17_layering_chain | `W17_CHAIN` | `W17_LONG_CHAIN` (5 or more hops, HIGH) |

The generator's home countries include none of the FATF-listed
jurisdictions, so on generated corpora W7 alerts carry
`W7_SYNTHETIC_CORRIDOR` and the two FATF codes are listed with no alerts.

Batch scoring splits each designated rule's recall and false-positive rate by
code (`financial_scoring.recall_by_code`, `fp_by_code`, `alerts_by_code`,
each `{rule: {code: value}}`): a code's recall is the share of the rule's
target typology's instances (counted as typology recall counts them) with a
planted payment in an alert of that rule carrying the code, 0.0 when no alert
carries it (`alerts_by_code` then reads 0), and its false-positive rate is 1
minus the share of the rule's alerts carrying the code that touch a payment
of that typology, over the alerts with a related payment as the rule's
false-positive rate counts them (an alert counts once per code it carries;
null when no such alert carries the code). When the target typology has no
instances in the corpus, every code's recall is null, as the typology's is.
Because every alert carries its base code, the base code's figures are the
rule's own. Only rules that ran are split. When an alert carries no code, or
the alerts predate the column, the blocks are empty and
`financial_scoring.by_code_status` says why. `reason_code_vocabulary` is a
digest of the code list the run used. A rule's evidence-cap label (below)
applies to each of its codes. Continuous runs are not split by code in v1.7.

Two diagnostic counts sit beside the scores and are not results:
`financial_scoring.nonplanted_alerts_by_rule` (per rule with a target
typology, the alerts that touch none of its planted payments, counted on
`gold.alerts` before the TM layer, so no Lakebench cap truncates the count;
an evidence cap can still make an alert whose planted payments were cut read
non-planted, which `evidence_capped_alerts_by_rule` shows) and
`financial_scoring.customer_count` (customers in `silver.entities`). They
feed the published limitation on how W5 and W6 non-planted alerts per
customer grow with scale: `scripts/aml_screen_rates.py` reads stored AML
batch records (never a bucket or a cluster) and writes
`docs/benchmarks/data/aml_screening_rates.json` from seed 43's runs at
scale 1 and 10, plus the calibration seed's at both when the
pre-registration's calibration seed is not 43, each with an observed
generator digest, scale and seed (generate in the same namespace before the
run).
It gives one n=1 row per seed role (`seed-43` or `calibration`), scale and
rule, with the run id and generator digest, and the ratio scale 10 over
scale 1 from the raw counts. It refuses a protected-corpus record, a verdict
other than PASSED, a record that lacks the counts or in which W5 or W6 did
not run, a scale pair from different generators or workload versions, and
fewer than 50 non-planted W5 plus W6 alerts at scale 1. A row whose
`evidence_capped_alerts` is above 0 is an upper bound, and a ratio with
either side cut is marked `bounded_by_evidence_cap`.

## Per-alert evidence caps

Some rules cut an alert's related-transaction list so one alert row cannot
grow with the corpus. These are Lakebench-imposed caps, set by Lakebench and
not tuned to any result:

| Rule | List | Cap | Kept |
|---|---|---|---|
| W1_connected_components | `related_txn_ids` | 250,000 | earliest by time |
| W2_structuring, beneficiary kind | `related_txn_ids`, `related_entity_ids` | 1,000 each | first by uetr, first by entity id |
| W4_risk_propagation | `related_txn_ids`, `related_entity_ids` | 1,000 | first in sorted order |
| W5_sanctions_match, rescreen | `related_txn_ids` | 200 | first by payment time |

The W2 originator kind and the other rules are not cut. Each capped alert's
`evidence` map carries the full count (`txn_total`, and `entity_total` for
W4) and whether the cap cut the list (`txns_truncated`, and
`entities_truncated` for W4). W2's sender list has no such flag, and its
narrative's sender count is the capped count. Scoring matches planted payments against
`related_txn_ids`, so a cut alert can miss planted payments past the cut.
When any alert of a rule was cut, the scoring summary counts them in
`evidence_capped_alerts_by_rule`, lists the typologies the rule detects in
`recall_bounded_by_evidence_cap`, and each such typology's entry in
`typologies` names the rule in `bounded_by_evidence_cap`: that recall is
bounded by a Lakebench-imposed cap, not a property of the detector alone.

## Metric-trust caveats

Three places on the scorecard where the metric name suggests more
than the number measures. They apply to the whole pipeline benchmark,
not AML specifically. `compute_efficiency_gb_per_core_hour` shows up
in every AML run; the other two only appear in continuous mode, and
the shipped AML example is batch mode.

- **`compute_efficiency_gb_per_core_hour`** is `GB / core_hours
  REQUESTED`, not `/ core_hours utilised`. A pod that requests 8 cores
  and uses 2 shows up as 4 x less efficient than one that requests 2
  cores and uses 2, even if they did identical work. Use it for
  release-to-release regression detection on the same config; do not
  compare against numbers from a stack sized differently.
- **`ingest_ratio`** (continuous mode only) is bronze rows ingested by
  the window's end divided by `released_rows`, the rows the trickle had
  made available to bronze by then (`max_files_per_trigger` files per
  bronze trigger since bronze's first write, at the corpus's mean rows
  per file, capped at the corpus). 1.0 means bronze kept up with what
  arrived; it is not the share of the corpus taken, which is
  `corpus_ingest_ratio` (bronze rows over datagen rows produced) and
  sits below 1 on a default run, whose trickle is sized to outlast the
  window. The trickle rate is a Lakebench-imposed cap, so a run the trickle
  held (`experiment.limits.trickle_bound`, see
  [Scoring and Benchmarking](benchmarking.md#continuous-mode)) measured the
  configured offered load, not the pipeline's capacity, whatever its
  `intake_limit` reads.
- **`qph_degradation_pct`** (continuous mode only) wants at least four
  rounds to read as a trend. Typical continuous runs produce five.
  Interpret values from a five-round run as a signal, not a conclusion.
  With the TM operations layer the early rounds run 8 queries and the later
  ones 12, so the halves time different work: the figure is withheld and
  `scores.qph_degradation_withheld` says why.
- **`pattern_span_s`** (per rule, was labelled "time-to-detect") is NOT
  detection latency. It is the span from a planted typology's injection
  start to the event time of the last transaction a rule cites for it,
  because `alert_ts` carries the last contributing transaction's event
  time, not the wall-clock at which the alert was produced. The value is
  therefore a property of the datagen's typology window arithmetic in
  `datagen_rs/src/typology.rs` (3 to 21 days for `micro_structuring`,
  one civil day for `rapid_layering`), invariant to how fast or slow the
  stack under test runs. Use pattern-span to sanity-check that a rule
  fires inside its typology window, never as a speed comparison between
  stacks.
- **`time_to_detect_seconds`** (AML continuous mode only) is the real
  detection latency. Each gold-refresh tick finds the alerts it newly
  raised: an alert is new when its (rule, entity, sorted related
  transaction ids) was not in `gold.alerts` at the snapshot before the
  tick. For each new alert, time to detect is the moment its rule's
  INSERT into `gold.alerts` committed minus the newest bronze `ingest_ts`
  among its related transactions, so it runs from the arrival of the
  last piece of evidence to the alert being visible in gold
  (`spark/scripts/gold_refresh_financial.py`). Each tick logs a
  histogram in 10 s bins, and the collector merges every tick into
  `time_to_detect_seconds` (median), `time_to_detect_p95_seconds` (upper
  edge of the bin that reaches the 95th percentile, capped at the
  maximum), `time_to_detect_max_seconds` and `time_to_detect_alerts`
  (`metrics/collector.py`). `time_to_detect_late_alerts` counts alerts
  whose evidence was already in silver before the previous pass read it
  (a re-raise after a rule error, or evidence outside
  `related_txn_ids`); they stay in the percentiles, so they can only
  lengthen them. `time_to_detect_unmeasured_cycles` counts ticks that
  logged no measurement; their alerts are measured one tick late. The
  ticks also log a pass-end histogram (every new alert measured at the
  end of the detection pass, the definition used before per-rule commit
  times) and one histogram per rule, kept with the gold-refresh stage
  metrics so a rule that got slower is not hidden in the merged figure.
  Batch runs report no time to detect. AML continuous runs always carry
  the keys, set to null when nothing was measured.

Precision and recall for AML themselves are honest measurements of
what they say -- rule alerts joined against manifest rows -- with the
label-proxy risks the leakage gate now catches.

## Running an AML pipeline

The full loop is `deploy -> generate -> run`. A batch `run` scores recall
and precision inline after gold-finalize; `lakebench financial score` is an
optional re-score (for example after a replay or a rule change). Every
AML config points to a `workload.schema=financial` config; the
example that ships is
[`examples/polaris-iceberg-spark-financial.yaml`](../examples/polaris-iceberg-spark-financial.yaml)
(and `polaris-iceberg-spark-financial-local.yaml` for developer sanity
at scale 1). Any of the Iceberg recipes works -- the schema flag routes
datagen and the pipeline scripts, not the catalog choice.

```bash
lakebench deploy   examples/polaris-iceberg-spark-financial.yaml
lakebench generate examples/polaris-iceberg-spark-financial.yaml
lakebench run      examples/polaris-iceberg-spark-financial.yaml

# Optional re-score (the manifest path assumes the default path template)
lakebench financial score \
    examples/polaris-iceberg-spark-financial.yaml \
    --manifest s3a://<bronze-bucket>/pacs008/manifest/manifest.parquet \
    --output   s3a://<gold-bucket>/scoring/rescore/recall.parquet
```

The inline score writes to `s3a://<gold-bucket>/scoring/<run_id>/recall.parquet`.

Two operator-facing subcommands cover the retention-workload scenarios:

- **`lakebench financial replay CONFIG --rule W2_structuring --depth-months 60`**
  reruns one rule against an Iceberg snapshot from N months ago. By
  default it writes to the config's gold alerts table with an `_replay`
  suffix, first deleting that rule's rows there, so the batch run's
  `gold.alerts` is never touched; `--output-alerts` names another table. The
  historical-replay workload's verification scenario asserts against a
  60-month replay's wall-clock budget. In the internal workload
  catalogue that scenario is workload id W8, which is unrelated to
  detection rule W8_dormant_reactivation and is never a rule id passed
  to `--rule`.
- **`lakebench financial reproduce CONFIG --alert-id <id>`** (`--run RUN_ID`)
  reruns one alert's rule on exactly what the run's gold-finalize read. A
  batch gold-finalize logs the snapshot of `silver.transactions`,
  `silver.entities` and `silver.silver_batch_versions` it reads, and the
  batch scorer, before maintenance, fingerprints every column of each
  (`financial_scoring.read_snapshots` in the run record: table, snapshot,
  `total_records`, `rows`, `fp`, `cols_sha`). The command reads that record
  (`--run`, or the deployment's latest AML batch run on this host; exit 2
  when there is none here) and refuses before any cluster call when that
  run is from a protected corpus (exit 2) or recorded no read snapshots
  (exit 4: it predates 1.7, or it was not scored, like a `run --stage`
  subset). The job reads each table at its recorded snapshot, or, when
  that expired, the current table if its fingerprint is the same (content
  and batch stamping equal: `basis: equivalent`); filters the transactions
  to the batches the versions table had sealed when gold read it; runs the
  rule with gold's parameters; and matches the alert on (rule, entity,
  `alert_ts`) and the set of related transactions. It writes
  `scoring/reproduce/<alert_id>/result.json` and the command exits 0 when
  the alert is reproduced, 1 when it is not (no match, several, a different
  set, or the rule declined to run) or the alert is not in `gold.alerts` for
  that run (a rule version other than the running code's included), and 4
  when a snapshot is gone and the content changed. The basis covers the
  three silver tables: W5 and W6 read the bronze watchlist as it is now and
  W1 its vertex cap from the current config, which the result lists as
  `not_pinned`. W3 and W17 path budgets depend on the job's executor count
  and scratch size, so reproduce a W2 or W4 alert for a clean check. A reproduction runs the
  rule over the whole silver snapshot, and the batch scorer's fingerprints
  read all three tables once per run.

`CONFIG` in both cases is the same YAML you passed to `deploy`. Both
verbs load it, assert `workload.schema=financial`, and dispatch a
SparkApplication.

The scale factor sets bronze volume linearly. Lakebench's estimate
(`src/lakebench/config/scale.py`) is 111,111 entities x 4 transactions a
month x 60 months per scale unit (about 26.7M transactions). Its size is
measured to scale 10: 8.47 GB of pacs.008 at scale 1 and 93.6 GB at scale 10
(bytes per row grow between the two; [data-generation.md](data-generation.md)
has the runs, and two scale-100 runs on other setups read within 2% of the
scale-10 size per unit). At 9.36 GB per unit, scale 10000, a tier-1
universal bank's AML retention target, is about 94 TB. The Pydantic schema accepts up to scale
10000, but AML datagen is banded: supported up to scale 300, unverified up
to 800, and refused above 800, where a datagen pod would exceed the 16 GiB per-pod memory cap (a
Lakebench-imposed cap). The pipeline has been run end to end only up to
scale 100, on the pre-freeze generator.

### Where gold-finalize spends its time

The gold-finalize job's entry in `metrics.json` (`jobs[]`, job type
`gold-finalize`) records, besides `alerts_by_rule`:

- `rule_elapsed_s`: wall seconds per detection rule, for every rule that
  started (ran, failed or skipped for a structural reason such as W1's
  vertex cap), from the rule's start to its alerts' commit.
- `stage_profile`: per rule, its three heaviest Spark stages by summed
  executor run time (`exec_s`), with the stage's status, task count, wall
  time, longest task (`max_task_s`), shuffle read in MB and the number of
  stages the rule ran. Each rule runs in its own Spark job group,
  `lb-rule-<rule>-<id>`, which is how its stages are told apart; the
  stages are read from the driver's status store after the rule's commit,
  outside `rule_elapsed_s`. Three flags say how far the numbers can be
  trusted: `complete: false` when the driver's status listener had not
  caught up within 5 seconds, `truncated: true` when the store had already
  dropped some of the rule's jobs or stages (for AML gold-finalize it keeps
  the last 1,000 of each, enough for a whole rule; 100 for every other
  job), and `lossy: true` when the listener dropped events during the
  rule, so task totals are low. When the starting point could not be read,
  `truncated` and `lossy` are both true. The 5 second wait covers every
  listener queue, so with Spark's event log turned on `complete` can read
  false while the status store had caught up. An empty list means the rule
  ran no stage.
  When there is no usable list (the store could not be read, or it held no
  stage of the rule while a flag is set), the rule is listed in
  `stage_profile_unavailable` with the reason instead. Detection is never
  affected. The wait for the listener adds at most 5 seconds per rule to
  the gold-finalize job, and nothing when the listener keeps up;
  `stage_profile_cost_s` records the seconds each rule's read took, which
  is Lakebench overhead inside the job's time and never part of
  `rule_elapsed_s`. The continuous gold tick does not profile, so its
  timings are unchanged.
- `tm_ops.phases`: wall seconds per stage of the TM operations pass, in
  pass order `pin`, `reconcile`, `prior_state`, `plan` (building the
  alert-input, replay and disposition plans), `write_ledger`, `inputs`
  (the alert-input build), `simulate` (the per-customer replay),
  `write_dispositions`, `write_cases`, `coverage`, `read_back`,
  `recon_write`, `invariants`; together they make up
  `tm_ops.elapsed_seconds`. The JSON keys are sorted, not in pass order.
  Spark evaluates lazily, so a phase holds the work its own reads and
  writes trigger; the alert inputs and the replay are materialised in
  their own phases, after the cycle is recorded as started, so a failure
  there fails the pass as before.
- `alert_set_seconds`: the seconds the alert-set fingerprint took (next
  section). It runs last in the gold-finalize pod, after the TM pass. It is
  Lakebench's work, not the pipeline's, so the CLI takes it off the stage's
  `elapsed_seconds` and end time, and so off CPU-seconds, as it does for the
  Customer 360 check; the report prints it beside the stage time ("excludes
  1.2s of Lakebench's alert-set fingerprint"). Time to value loses it in a
  single-cycle run; with `cycles` above 1 the earlier cycles' fingerprints
  stay inside time to value, as the Customer 360 check's do.

The run's record derives two diagnostic blocks from the fields above (neither
enters identity, a verdict or a comparison):

- `experiment.attribution` (AML batch): the gold-finalize job's slowest
  rule (`dominant_rule`, its `rule_elapsed_s` and `share_of_job`, the rule's
  time over the job's), that rule's heaviest stage (`dominant_stage`: stage
  id, name, tasks, executor seconds, wall seconds, longest task, its share
  of the executor time of the rule's logged stages (the three heaviest),
  and the profile's flags), and the TM pass's
  time and share (`tm_elapsed_s`, `tm_share`). `profile` is `read`, or says
  why the stage is missing (`unavailable: <reason>`, `no_stage`,
  `missing`). When the status store could not be read, the same profile can
  be built from a Spark event log of a rerun of gold-finalize with
  `scripts/aml_stage_attribution.py EVENTLOG --record metrics.json`, which
  marks the block `profile_source: eventlog`.
- `limits.headroom_pct` (batch): per stage, `100 x (1 - elapsed / per-job
  timeout)` against the per-job timeout the run gave every stage (recorded
  as `job_timeout_seconds`); a stage that ran more than once reports its
  slowest run, and a failed stage reads null. The benchmark phase has no
  per-job timeout: its queries are bounded one by one, so
  `benchmark_query` is `100 x (1 - slowest timed query sample / per-query timeout)`
  (recorded as `benchmark_query_timeout_seconds`, 900 s for AML), null
  when a query failed or the benchmark was replaced afterwards by
  `lakebench benchmark`. 25 or more means at most 75% of the budget used.

### The alert set: are two runs' alerts the same?

Two AML batch runs are compared on their results before their speed, and
the benchmark queries alone do not show that both runs raised the same
alerts. After its last write to `gold.alerts`, gold-finalize fingerprints
the run's alerts in Spark and prints one `LB_ALERT_SET` line; the record
keeps it as `experiment.results.alert_set`:

- an alert is `(rule_id, entity_id, alert_ts)`: the rule, the subject and
  the event time the rule derived from the data. `alert_id`, `run_id`,
  `detected_ts` and every other column are left out, so two runs that
  raised the same alerts read equal;
- `by_rule` holds each rule's alert count (`rows`) and an order-independent
  hash (`h`, an exact sum of one xxhash64 per alert); `rows` and `h` are
  their totals. `spec` (`as1`) and `cols_sha` name the definition and the
  column types. The value is the same on the Spark 4.0 and 4.1 lines.

A different alert set is a different result, like a different query
result: two runs where any rule's count or hash differs are not
comparable. An AML batch record written by 1.7 (exp2, or exp1 with
`v2_unavailable`) that has no alert set, because the fingerprint failed
(the reason is in `results.alert_set_unavailable`) or gold-finalize did not
run, cannot show its results match another run's; do not compare it on its
query results alone. The perf gate and `reproduce` refuse such a run; they
do not yet compare alert sets with their baseline or package. Records from
1.6 have no alert set. A rule that ran
and raised no alert is absent from `by_rule`, like a rule that did not run;
which rules ran is recorded separately (`experiment.rules`).

Continuous runs never fingerprint inside a tick (that would be a full scan
inside time to detect). Their alert set is taken once after the drain
(`results.alert_set_continuous`, below) and is diagnostic only.

## Known limitations in v1.6

- **No counter-leakage hard negatives yet.** The generator does not yet
  plant legitimate accounts built to mimic a typology's shortcut signature
  (for example naturally long-quiet seasonal or travel accounts that break
  the dormancy gap signature). The leakage gate still runs, but a
  detector can still score on such a shortcut where no hard negative
  exists to punish it. Planned for v1.7.
- **AML continuous per-rule recall is not scored.** Stopping the
  streams can interrupt a gold-refresh tick and leave rule statuses
  `pending`; post-run scoring refuses them, and the report says "Recall is
  not scored in continuous mode". Batch recall is unaffected. From v1.7 a
  continuous run drains the last tick and records `recall_covered`
  instead; see [Continuous recall over covered instances](#continuous-recall-over-covered-instances).
- **Stream restarts longer than 1 h are not safe.** Continuous
  Iceberg snapshot expiry is floored at 1 h while streams are live. A
  bronze-ingest driver down for longer can replay a batch and append
  duplicates, and a silver stream down for longer may resume from an
  expired snapshot and fail. Short restarts are fine. Target v1.7.
- **Delta continuous runs get no effective maintenance.** While streams
  are live, Delta VACUUM keeps Delta's 7-day default retention, so a
  continuous Delta run shorter than 7 days removes nothing and its object
  count grows for the whole run. Delta OPTIMIZE never runs either, so Delta
  tables are not compacted.
- **The concurrency degradation ratio is measured in reduced form.**
  v1.6 measures investigator-query latency on Trino at scale 10, idle
  against loaded, over 30 executions each. The full measurement (Spark and
  Trino, scale 10 and 100, idle, beside one other workload and beside
  everything, at least 100 executions each) moves to v1.7.
- **AML datagen throughput is reported, not gated.** AML datagen reports
  per-pod write throughput and CPU-hours per TB, and no release gate fails
  on either figure. The throughput figures published earlier were measured
  before the generator freeze and are superseded. v1.6 has no measurements
  on the frozen generator; they are deferred to v1.7.
- **Recall is uncalibrated.** v1.6 publishes no held-out Level-2 result;
  recall and precision are in-sample on the calibration corpus. The
  registered held-out looks are deferred to v1.7.

## Continuous recall over covered instances

A continuous run ends its window by draining gold-refresh instead of
deleting it mid-tick. The CLI writes a marker object,
`<checkpoint_base>/gold-refresh/_lb_stop` in the gold bucket, whose body is
the run id. The driver finishes the tick it is in, logs
`Drain complete: last completed cycle N`, frees its executors and waits
until the streams are stopped. The CLI waits up to 1800 s for that line.
The run fails when the drain times out (`gold drain timed out; last tick
interrupted`), when gold-refresh is deleted before it drains, or when the
driver had restarted and found the marker before its first tick, because in
each case `gold.alerts` may be half rewritten. A marker that cannot be
written does not fail the run; its recall reads `not_scored`.
`lakebench stop` drains the same way, with a 300 s budget, and stops the
jobs whether or not the drain is confirmed, Ctrl-C included.

Every tick logs the snapshots it read and wrote: `silver.transactions`,
`silver.entities`, `silver.accounts` and `silver_batch_versions` when it
pinned silver, and `gold.alerts` and `gold.detection_status` once
detection committed. Detection filters the pinned transactions through the
versions table at the logged snapshot, so the scorer can see exactly the
sealed batches detection saw. The record keeps them as
`continuous.ticks[]`, with `continuous.drain` and
`continuous.ticks_unpinned` (ticks whose transactions or versions snapshot
was not pinned, which detection then read through the current versions
table). They come from the current gold-refresh driver pod's log, so a
driver that restarted leaves its earlier pod's ticks out
(`continuous.drain.ticks_scope`), and
`continuous.drain.log_from_driver_start` is false when log rotation trimmed
the log's first ticks.

Each tick also records the `silver.transactions` snapshot it read for the
time-travel read after the window, from the snapshot's metadata only (no
scan, so the tick's timings do not move): `continuous.time_travel.ticks[]`
holds the driver start, the cycle, whether the tick completed, the snapshot
id, its `committed_at` (UTC), and from the Iceberg snapshot summary its
`total_records` (the record count of its live data files) and
`pos_deletes` and `eq_deletes` (deleted-row totals, 0 on the copy-on-write
tables Lakebench creates, when `total_records` is the live row count), with
`count_source: "summary"`. When the summary has no record count,
`total_records` is null and `count_source` is `"unavailable"`; the current
table's count is never recorded in its place. A tick with no transactions
snapshot records none.

After the score job of a run that passed its gates, `time-travel-financial`
reads those snapshots back (`spark/scripts/time_travel_financial.py`),
newest first. A hash pass fingerprints each recorded snapshot still in the
table over its business columns (every column less the batch-version
sentinels `_batch_id`, `_stream_id`, `ingest_ts`, `committed_at`, the
definition `scripts/release/silver_parity.py` uses), and writes
`scoring/<run_id>/tt_hashes.json` with any snapshot it could not read; the
read pass reads that file back from storage, times a full scan `VERSION AS
OF` each snapshot with the same fingerprint, and compares it with the
tick's `total_records` (when it is a live-row count) and with the hash
pass. `pass` needs at least one `verified` snapshot, one compared with the
count its tick recorded. Each
entry of `continuous.time_travel.ticks[]` gains `state`, `read_s`, `rows`,
`fp_match` and `count_match`; an expired snapshot gains `expired_by` from
`continuous.retention.rounds` (when each maintenance round ended, the
expiry it applied, its engine, the tables whose `expire_snapshots` ran;
Trino's cutoff is compared on the cluster clock, Spark Thrift's on this
host's), or reads
`missing_unexplained`. `continuous.time_travel.verdict` is `pass`, `fail`,
`incomplete` or `not_run`, and the line beside the run verdict says why;
it never fails the run. The job's budget (`continuous.time_travel.budget`)
is the per-job timeout less 120 s, a Lakebench-imposed bound passed as a
deadline on the cluster clock: after the first scan of each pass the job
starts no scan unless the time left exceeds 1.5 times its longest scan, the
hash pass gets half of it, and the result is then `incomplete`.
A wait that ends before the job does deletes the job.

After the streams stop and every gate has decided, the score job reads
those six snapshots of the drained tick and scores **`recall_covered`** per
typology: the designated
rule hit rate over the instances the tick could have detected. An instance
is covered when every one of its participant transactions is in the sealed
transactions at the tick's snapshot and every participant maps, through
`silver.accounts` at that snapshot, to an entity in `silver.entities` at
that snapshot. Each typology also reports `covered_instances`,
`corpus_instances`, `coverage` and `no_participant_txns`; a typology whose
designated rules are all excluded from continuous mode is listed under
`excluded_typologies`. False positives and transaction precision count the
whole manifest, so an alert on a planted payment the tick had not yet
covered is not a false positive. The per-rule chance floor uses the covered
random-control instances, as recall does.

`recall_covered` is not the batch `recall` and is never written under that
name: it lands in `financial_scoring.covered` with `mode: "covered"`.
The run is `not_scored`, with the reason, when the drain did not complete,
the run failed a gate, the last tick's record is missing or names a
snapshot as `unknown` or `none`, a recorded snapshot was expired before
scoring, or the run was interrupted before scoring finished. An earlier
tick is never scored instead, and the current tables are never read in
place of a recorded snapshot. The scored tick can begin after the window
closed (the drain waits for the tick in progress, and the window's bucket
listing runs first); `financial_scoring.tick.pinned_after_window_end_s`
says by how much. The same job fingerprints `gold.alerts` at the scored
tick's commit (`experiment.results.alert_set_continuous`), which is
diagnostic only: continuous alerts depend on when ticks ran. The scorecard
says whether recall was scored and why not, but does not render
`recall_covered` yet.

## The transaction-monitoring operations layer

After detection, the gold stage runs the work a bank's TM operation does
with the alerts (`spark/scripts/tm_operations.py`). One reporting
institution monitors its own customers (`silver.entities.is_customer`);
everyone else is a counterparty. The unit of work is the business day: an
alert is generated the day after its last payment, and the queue is replayed
day by day up to the cycle's as-of date (the day after the newest payment).
Anything that would be decided later is the open backlog.

| Table | Stage | What it holds |
|---|---|---|
| `gold.tm_reconciliation` | 1 | One set of rows per cycle (batch) or operations pass (continuous), appended: customers; source, bronze and silver payments; monitored vs excluded by reason (`no_customer_party`, `dq_unconvertible_currency`, and `in_flight` in continuous); DQ rule failures; the funnel alerts -> escalated -> cases -> SARs |
| `gold.scenario_coverage` | 1 | Scenario-to-typology matrix with this cycle's rule status, alert volume and planted instances; planted typologies no scenario targets appear as `gap` |
| `gold.alert_dispositions` | 6 | Triage priority (scenario weight x CRR tier: low, medium, high, critical) and a disposition that is never NULL: `escalated`, `closed_nfa`, `attached` to the customer's open case, `pending_l1`, `out_of_scope` (non-customer), `over_capacity` (past the per-customer cap, applied after identity matching so an alert already in
the workflow is never capped); aging against the SLA; QA re-review; the alert's first-seen cycle |
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
matches each alert to the last completed cycle's (read at the table
snapshots that cycle recorded in the ledger): same content first, else the
same rule on the same customer, its last payment moved forward by at most
31 days, and holding at least a quarter of the payments the prior was first
raised on (a frozen 32-hash sketch of them, checked against all of the new
alert's payments) and never fewer than two, largest overlap first. A new
alert sharing a single payment with a prior never takes its identity. Rules cap the related-payment
list by payment id, so an alert that grows past about three times that cap
between cycles can lose its identity: it is then carried as withdrawn and
raised again as new, which inflates alert counts but never moves a decision.
A matched alert keeps its key, generated date, truth and
priority as first seen; an alert detection stops emitting is kept
(`in_current_detection` false); a new alert whose payments predate the
previous cycle is dated on this cycle, the first day it could have been
raised. The day-by-day replay is causal and its inputs for seen alerts are
frozen, so recomputing it reproduces every earlier decision; the
`history_stable` invariant checks that against the previous cycle's table.
Case activity counts are recomputed each cycle from silver.

```yaml
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

**Who a scenario alerts on.** The bank monitors its own customers. W2, W5,
W6, W7 and W8 raise an alert only when its subject (`entity_id`) is a customer
in `silver.entities`; the counterparty stays in `related_entity_ids`. The
graph scenarios (W1, W3, W4, W17) run across the whole payment network and can
alert on an account at another bank, which is why they are the declared
`counterparty_scenarios`. The scope is fixed in `detection_rules.py`
(`CUSTOMER_SCOPED_RULES`); the config list only tells the invariant which
non-customer alerts to expect. A customer-scoped rule on a silver with no
customer flag, or no customer, reports `skipped`, not zero alerts.

**Workflow invariants.** Checked on the tables as written, every cycle: the
monitored population is not empty; monitored + excluded = source; every alert
has a disposition row and none is NULL; alerts on non-customers come only
from scenarios declared customer-and-counterparty; escalated <= alerts;
alert-driven cases <= escalated; SARs <= cases; at most one open case per
customer; one disposition row per alert identity; the funnel is monotone; every SAR past 90 days has its review (or
is waiting on a case pending filing); no review is credited to an
already-determined case; decided history is unchanged from the previous
cycle.

**The operations verdict is separate from detection.** `fail` (the layer
ran and an invariant is violated) fails the run. `not_run` (no manifest,
a layer error, a continuous window that ended before the manifest was
ready) says why and leaves detection scoring alone. `unknown` means no
driver log was captured for some cycle, or an invariant could not be
checked (the raw source files failed to list).
`disabled` skips the gate. The verdict is kept in `metrics.json` as
`tm_operations` and heads the report section.

**Continuous.** gold-refresh runs the layer every
`continuous_interval_seconds` (at least 60), counted from the end of the
last pass so detection ticks always run between passes, plus one final pass
timed to finish before the window closes. One pass over the full corpus
takes minutes at scale 10, and gold is not refreshed meanwhile, so a
freshness sample is logged after each pass and the pass time counts in the
freshness score (sampled after a pass while data is moving). The final pass
is timed from the first gold-refresh driver's start, persisted in its
checkpoint, plus the window length. The continuous jobs carry the CLI run
id, so a restarted driver appends to the same ledger. Each pass takes its cycle number in the
ledger before it writes; a pass that fails after writing is reported as a
failure and never carried from.

Silver, then bronze, are pinned before the raw files are counted.
In-flight payments are bounded, not inferred: bronze must hold exactly the
rows of the raw files its stream's checkpoint log says it took up to the
pinned snapshot's batch (`spark.sql.streaming.epochId`), so the rest are
files not yet taken; and rows silver has not taken must be newer than
silver's ingest watermark. Anything else is `unaccounted` and fails
reconciliation. Freshness samples taken after a pass follow the tick's own
rule (only while data is moving), so a drained corpus's idle time never
becomes the score. The continuous reset drops all four tables.

**Investigator queries.** The AML benchmark set includes four timed
investigator queries (class `investigator`): customer 360 for the top open
case, the 12-month activity review of the newest case, the counterparty and
two-hop view, and open cases older than 60 days. They read only this run's
rows, and are left out unless this run's TM verdict is pass or fail (the
standalone `benchmark` command includes them only when the deployment's
newest run had a pass or fail verdict, since each run overwrites the tables) (so a
disabled or not-run layer never times empty or stale tables). A continuous
run's in-window rounds include them once the run has a case: each round first
probes `gold.cases` for the run's `base_run_id` (untimed) and runs the 12-query
set when a row comes back, or the 8-query set before the first TM pass, labelled
`investigator_queries: absent_no_cases` (`probe_failed` when the probe errors).
Such a run's rounds usually span both sets, so its in-stream composite QpH reads
`blended`, with the median per set in `scores.composite_qph_by_set`. QpH is recorded with its query-set id; `reproduce`
refuses to compare QpH across different query sets, so an 8-query
AML run is never set against a 12-query one. A run recorded before the id
existed gets a pinned historical id when its query names are the c360 set or
the 8-query AML set and was recorded after that set's last SQL change (so
it stays comparable with runs over the same SQL); older records and any other
legacy set are `unknown`. A run of an older branch recorded after that date
is the one case this cannot tell apart.

**Queries that read the mode's layout.** Continuous AML writes
`silver.counterparty_edges` as one row per (source, target) pair per
micro-batch, where batch writes one row per pair, and it stores
`silver.account_statements` running balances (`bal_after`) in arrival order
(labelled `arrival_order_running_balance` when a statement arrives late).
The benchmark queries do not depend on either layout. FQ3 and both of IQ3's
hops sum the edge rows per pair. FQ4 recomputes each entry's running
balance in ledger order (book time, then transaction id, debit before
credit, as batch silver orders it) from the account's opening balance, its
last stored `bal_after` less the sum of its entries, instead of returning
the stored `bal_after`. On batch silver both queries return what they did
before, row for row; on continuous silver over the same corpus, once it has
settled, they return the batch answer. The change moved the AML query-set
id (12 queries and the 8 before the first TM pass), so no record from
before it compares with one after; it is part of workload version `aml-2`.
A batch record is still never compared with a continuous one: the mode is a
workload identity key, so the two are not comparable, and the perf gate
and `reproduce` refuse the pair.

**Investigators under load (`architecture.benchmark.investigator_sessions`).**
Set to N (1 to 32) on an AML config with TM operations on Trino or Spark
Thrift (refused at load otherwise; `run` refuses it in batch, so it needs a
continuous run), it adds one round right after the first in-stream round
that included the investigator queries (that round is the baseline). N
sessions run concurrently, each working one case of this run picked in IQ1's
queue order (open cases first, by priority, oldest first; closed cases fill
in when fewer are open): IQ1, IQ2 and IQ3 bound to that case and IQ4
unchanged, once each. The round is not a benchmark round, so in-stream QpH
and the round count do not move; it takes window time, so the round count
can be lower than without it. It runs only when at least twice the baseline
round's time is left; the case pick's timeout is at most 120 s and a fifth of
the time left, and each session query's timeout, taken from what is left after
the pick, is at most 300 s and a quarter of it less a 10 s cleanup margin, so
the round ends inside the window (`status: no_time` when that is under 30 s). `continuous.investigators` records
`sessions_requested`, `sessions_run` (the sessions started: fewer when the
run has fewer cases, with `lowered_reason`; a session whose queries failed
still counts, and shows in `failed` and `status`), the `case_ids`,
`rows_per_session`, `seconds_per_session`, the nearest-rank `latency` p50 and
p95 per query over the sessions whose query succeeded (with `n` and the
`failed` count), the `baseline` round's time per query, the session `window`
(on the Lakebench host's clock, while `continuous.window` is on the cluster's),
`query_timeout_s`, `session_sql` (a hash of each bound query's SQL), any
`failed` or `empty` queries and a `status`: `pass`; `fail` when a session
query failed or a session's IQ1 or IQ3 returned no rows (it fails the
investigators check, not the run); `no_cases`; `no_rounds` (no in-stream
round ran, for example under `--skip-benchmark`); `no_time`; or
`case_query_failed`. It is labelled `n=1 per arm` and `shared S3
contention`, plus `BOUNDED BY Lakebench per-query timeout (Ns)` when a
session query timed out and the engine's Lakebench-set memory bound when one
failed on memory. After the window, each detection tick that ended inside
the continuous window is placed against the session window (its end from the
log, shifted to the host clock, minus its `total`): `tick_delta` gives the
count and median tick time of the ticks with at least half their time inside
the sessions' window and of those entirely outside, and `load_label`
("investigator load START-END: k of m ticks overlap") states it. The verdict
carries the check and the label as its `investigators` qualifier, shown
beside the verdict: time to detect and continuous throughput keep their
values and include those ticks. `experiment.investigators` holds
`{requested, run}`; the identity key `investigator sessions` is the number
that ran, an outcome condition: two runs that ran different numbers compare
as not like-for-like, and the perf gate does not refuse on the number (8
against 3), but it refuses load against no load: sessions that ran against a
baseline with none configured or none run, and the other way round.

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

- [`src/lakebench/spark/scripts/detection_rules.py`](../src/lakebench/spark/scripts/detection_rules.py) -- the nine W-rule implementations.
- [`src/lakebench/benchmark/aml_queries.py`](../src/lakebench/benchmark/aml_queries.py) -- rule -> typology mapping and query catalogue.
- [`src/lakebench/spark/scripts/score_financial.py`](../src/lakebench/spark/scripts/score_financial.py) -- recall and precision, run inline by `lakebench run` and by `lakebench financial score`.
- [`src/lakebench/aml/reference_score.py`](../src/lakebench/aml/reference_score.py) -- band leakage gate library.
- [`src/lakebench/aml/fidelity_gate.py`](../src/lakebench/aml/fidelity_gate.py) -- pre-registered fidelity gate and reference model.
- [`src/lakebench/spark/scripts/score_financial_reference.py`](../src/lakebench/spark/scripts/score_financial_reference.py) -- Spark driver for both gates, called by `lakebench financial reference-score`.
- [`docs/financial-benchmark-baselines.md`](financial-benchmark-baselines.md) -- test-cluster wall-clock, recall and time-to-detect numbers, populated per release (no v1.6 measurements; deferred to v1.7).
