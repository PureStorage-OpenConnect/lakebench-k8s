# AML (financial) Pipeline Benchmark Specification

Workload version `aml-2` (as stamped in `experiment.workload.version`),
generator model `datagen-v2-rs-0.3`, query sets `qs12-910d16a91962` (batch
with TM operations) and `qs8-ffe2bc1a012e` (FQ1 to FQ8), maintenance policy
`m2-2026-09-26`.

Audience: engineers who run, reproduce or compare this benchmark, and
contributors who implement it on a new component. Every statement describes
what the code of this release does, and names the module that does it.
Python paths are relative to `src/lakebench/`; generator paths to
`datagen_rs/`. Figures labelled "recorded" carry the id of the run that
recorded them; the run records are not in the repository. The three AML
runs cited (`run-20260929-205000-ebb26f`, `run-20260929-214442-825153`,
`run-20260929-221146-9d5345`) were written by Lakebench 1.6: they carry
workload version `aml-1` and experiment schema `exp1`, predate the continuous
drain and covered scoring, the reason codes and the record gates, and are
not comparable with a run of this release (section 10). Each is one run
(n=1) at its own development seed.

## 1. Purpose and scope

The workload models a bank's transaction-monitoring (TM) back office over a
five-year archive of ISO 20022 pacs.008 credit transfers. A synthetic
generator plants known money-laundering typologies into an otherwise clean
payment stream. The pipeline lands the messages (bronze), normalises them
into a payments, parties, accounts, statements, counterparty-graph and
profile model (silver), runs nine detection rules and a simulated
alert-triage, case and SAR workflow (gold), and a fixed query set is timed
against the result.

What it measures about an architecture (catalog x table format x pipeline
engine x query engine, on a known system):

- Batch mode: how long the architecture takes to turn a fixed raw corpus
  into queryable gold (`time_to_value_seconds`), the data volume and the
  requested compute it used on the way, and how fast its query engine
  answers the query set (`composite_qph`).
- Continuous mode: how stale gold is while a finite corpus is trickled in at
  a fixed offered load (`data_freshness_seconds`), how long a planted
  pattern takes to raise an alert after its payments land
  (`time_to_detect_seconds`), the ingest rate bronze holds, and the query
  engine's QpH while the tables are being written.

Those figures are valid only when the run's verdict passes (section 5).
A batch comparison also needs both runs to have returned the same query
results (section 10). An AML continuous run records no result fingerprints,
because its alerts depend on when detection and TM passes ran relative to
arrival, so a continuous comparison can never establish its results.

What it does not claim:

- It is not an AML product. The rules are illustrative detectors with fixed,
  self-chosen parameters, and the TM workflow is a seeded simulation with
  default analyst (0.90) and investigator (0.95) accuracies. Both are
  configurable and hashed into the workload parameters, so a change makes
  two runs not comparable (section 7.3).
- Detection quality (recall, false-positive rate) is a property of the
  corpus and the rules, not of the architecture. It is reported beside a run
  and never decides its verdict. A batch verdict fails an AML run on rule
  errors, skips that are not allowed, expected rules that did not run, or
  zero alerts; a continuous verdict on a mode-excluded rule that ran or zero
  alerts (section 5).
- Figures bounded by a Lakebench-imposed cap (executor ceilings, rule caps,
  the continuous trickle, the TM per-customer alert cap) measure that cap,
  not the infrastructure (section 7.4).
- The reference-model fidelity gate and the registered evaluation looks
  measure the generator's realism, not an architecture. Neither runs inside
  `lakebench run`. On a development seed the gate runs on the cluster with
  `lakebench financial reference-score`; a registered evaluation or
  robustness look is refused there and runs only through
  `scripts/aml_gate.py --registered`, which records the look (section 3.3).

## 2. Data model

All tables are Iceberg; the AML workload refuses Delta at config load. Every
table is written as format-version 2, snappy, metadata delete-after-commit,
50 previous metadata versions (`spark/scripts/common.py`). The DDL that
executes is inline in each stage script; `deploy/financial_ddl.py` holds a
reference copy that no runtime code imports, and unit tests keep it in step
with the silver and TM column lists. No table declares a primary key; the
keys below are logical.

Recorded row counts below come from two published batch records: scale 1
from `run-20260929-221146-9d5345` (polaris-iceberg-spark-trino) and scale 10
from `run-20260929-214442-825153` (hive-iceberg-spark-trino), both verdict
PASSED on the v1.6 generator image. Each figure is n=1 at that run's seed;
bronze and everything downstream move with the seed through the screening
track (section 3.2). Both records hold the corpus as declared by the config,
not observed from the datagen pods (`experiment.corpus.observed: false`).

### 2.1 Raw corpus (S3, written by the generator)

| Object | Grain | Key | Layout | Scale 1 | Scale 10 |
|---|---|---|---|---|---|
| `bronze/pacs008/part-NNNNNN.parquet` | one payment (pacs.008 credit transfer) | `txn_id`, `uetr` | flat files, no partition directories; 137 files at scale 1 and 1,371 at scale 10 (section 3.2) | 26,671,867 rows (recorded) | 266,718,594 rows (recorded) |
| `bronze/party.parquet` | one party of the modelled world | `entity_id` | single object | 111,111 (formula) | 1,111,110 (formula) |
| `bronze/account.parquet` | one account | `account_id` / `iban` | single object | not recorded | not recorded |
| `bronze/watchlist.parquet` | one sanctions (v1, v2) or PEP list entry | `list_id` | single object | not recorded | not recorded |
| `manifest/manifest.parquet` | one planted instance (the answer key) | `typology_id` | separate `manifest/` prefix | 7,285 instances (recorded) | 73,083 instances (recorded) |

The pacs.008 file has 41 top-level columns (`datagen_rs/src/schema.rs`)
covering the group header (`msg_id`, `cre_dt_tm`, `nb_of_txs`, `ctrl_sum`,
settlement date and method), payment type information, the transaction
(`txn_id`, `end_to_end_id`, `uetr`, settlement and instructed amounts as
DECIMAL(18,5) with currencies, exchange rate, charge bearer), the
instructing, instructed and initiating parties, up to three intermediary and
previous instructing agents, the full debtor and creditor party, account and
agent structs, ultimate parties, purpose, regulatory reporting and
remittance information. Bronze carries no label column; the answer keys are
only in the manifest, which lists per instance its typology, participant
entity ids, participant UETRs, injection window, parameters, expected
workload, severity, seed and `model_version`. The party file carries
identity, address, contact, LEI/BIC and KYC columns (`is_customer`,
`home_fi`, `customer_since`, `customer_type`, `expected_monthly_volume_usd`,
`crr_score`, `crr_tier`, `crr_factors`) and `model_version`; the account
file's `current_balance` is always NULL.

The pacs.008 row count exceeds the base-payment formula (26,666,640 at
scale 1) because sanctions and PEP screening rows are added on top of the
base budget; their number depends on the seed.

### 2.2 Bronze tables

| Table | Grain | Partitioning | Scale 1 | Scale 10 |
|---|---|---|---|---|
| `architecture.tables.bronze` (default `default.bronze_raw`) | one payment, schema inferred from the Parquet | unpartitioned (zero-copy `add_files`, or a CTAS fallback) | 26,671,867 | 266,718,594 |
| `bronze.manifest` | one planted instance, CTAS of the manifest Parquet | unpartitioned | 7,285 | 73,083 |

In continuous mode the bronze table gains `ingest_ts` and is filled by the
bronze-ingest stream.

### 2.3 Silver tables

| Table | Grain | Logical key | Partitioning | Scale 1 | Scale 10 |
|---|---|---|---|---|---|
| `silver.transactions` | one payment | `txn_id` | `months(txn_timestamp)` | 26,671,867 | 266,718,594 |
| `silver.entities` | one distinct party seen in payments, joined to KYC | `entity_id` | none | 113,721 | 1,137,292 |
| `silver.accounts` | one IBAN | `account_id` | none | 113,721 | 1,137,292 |
| `silver.account_statements` | one booked entry (camt.053 shape): one DBIT and one CRDT per payment | (`account_id`, `entry_seq`) | `months(book_ts)` | 53,343,734 | 533,437,188 |
| `silver.counterparty_edges` | one (originator, beneficiary) pair (batch); one pair per micro-batch (continuous) | (`source_entity_id`, `target_entity_id`) | `bucket(64, source_entity_id)` | 4,894,537 | 49,005,465 |
| `silver.entity_profiles` | one entity's behavioural baseline | `entity_id` | `bucket(64, entity_id)` | 113,721 | 1,137,292 |
| `silver.silver_batch_versions` | one sealed commit marker | (`stream_id`, `batch_id`) | none | one per batch cycle | one per batch cycle |

In both published records entities = accounts = profiles, statements = 2 x
transactions, and edges < transactions. Lakebench records these counts
(`jobs[].silver_tables`) but does not check the relations (section 12).
`entity_id` is `xxhash64` of the LEI, falling back to a hash of upper(name),
country and upper(city); `silver.entities` is larger than the party file
because it includes counterparties outside the modelled world. Silver rows
carry the per-run sentinels `_batch_id`, `_stream_id` and `ingest_ts`, which
are excluded from content parity. `silver.entity_profiles.profile_updated_ts`
is stamped from the silver data clock (section 3.5).

### 2.4 Gold tables

| Table | Grain | Logical key | Partitioning | Scale 1 | Scale 10 |
|---|---|---|---|---|---|
| `gold.alerts` | one alert from one rule in this run | `alert_id` (per-run UUID) | `months(alert_ts)` | 712,775 | 7,419,581 |
| `gold.detection_status` | one rule's outcome for the run | (`rule_id`, `run_id`) | none | 9 | 9 |
| `gold.daily_dashboards` | one (day, rule_id, disposition); includes one `baseline` row per settlement day | (`day`, `rule_id`, `disposition`) | none | not recorded | not recorded |
| `gold.risk_scores` | one entity scored by W4 | (`entity_id`, `model_id`) | none | not recorded | not recorded |
| `gold.entity_clusters` | one W1 cluster member | cluster id | none | not recorded (W1 skipped) | not recorded (W1 skipped) |
| `gold.tm_reconciliation` | one monitoring-completeness ledger row per cycle | (`run_id`, cycle) | none | not recorded | not recorded |
| `gold.scenario_coverage` | one scenario's coverage row | scenario | none | not recorded | not recorded |
| `gold.alert_dispositions` | one alert's L1 disposition | alert identity | none | 712,775 | 7,419,581 |
| `gold.cases` | one L2 case (at most one open per customer) | `case_id` | none | 66,177 | 687,407 |

`gold.alerts` columns: `alert_id`, `rule_id`, `rule_version`, `model_id`,
`model_version`, `entity_id`, `related_txn_ids`, `related_entity_ids`,
`alert_ts`, `alert_score`, `priority`, `status`, `disposition`,
`alert_type`, `run_id`, `narrative`, `evidence` (map), `detected_ts`,
`reason_codes` (array, section 4.2; new in this release). An
optional `gold.alerts_replay` is written only by `lakebench financial
replay` (section 4.4).

## 3. Data generation

### 3.1 Generator identity

The corpus is produced by the Rust generator in `datagen_rs/`, entered via
`datagen_rs/entrypoint.py` with `--schema financial` (the default schema).
Its identity has two parts:

- `MODEL_VERSION = "datagen-v2-rs-0.3"` (`datagen_rs/src/model.rs`), the AML
  generator model version. It is stamped as a `model_version` column in the party,
  account, manifest and watchlist files, never in pacs.008 rows. A generator
  built from this release's `datagen_rs/` source also writes it into each
  node's completion marker (`_corpus/c<cycle>-node-<node>.json`) and prints
  it with `--version`. Lakebench stamps
  `experiment.workload.generator_model_version` from a table kept equal to
  `model.rs` by a test (`metrics/experiment.py`), not from the corpus.
- The image. `images.datagen` defaults to
  `docker.io/sillidata/lb-datagen:2a36ae21@sha256:0502b700299948f43bb1b999d7ba29262a509306658b4e5f7c48738f88d31f04`,
  built from this release's `datagen_rs/` source (commit `2a36ae210`); the
  runtime pulls the digest. A five-case byte-compare against the v1.6
  release image `lb-datagen:1.6.0` (financial seed 43 with and without the
  robustness perturbation, Customer 360 seed 42, and both at two cycles;
  markers excluded) is equal
  (`tests/fixtures/datagen_reference/compare-0502b7002999.json`). The
  generator lineage that enters the
  corpus identity is the observed image digest, mapped through
  `config/datagen_lineage.yaml`, so an output-neutral re-pin keeps the same
  lineage (section 7.3). A registered look does not use a tag:
  `scripts/aml_gate.py --registered` refuses to start without a
  digest-pinned `--generator-image`, and refuses one that differs from the
  image the committed per-typology predictions were made with.

A generator built from this release's source writes one completion marker
per node and cycle under `_corpus/`, holding the resolved generator
arguments and their sha256; `--print-resolved-args` prints the same object
without writing anything. Lakebench folds the markers into corpus id v2
(`metrics/corpus_identity.py`). It also refuses, with exit 2, any flag it
does not know, an abbreviated flag, and any stray argument, so a typo never
runs as a silent default.

The default image has those features: it writes the corpus markers, runs
the generator-start held-out check and reads `LB_DATAGEN_SEED`. A run on it
records corpus id v2, and its experiment block is identity v2 (exp2) when
the other identity inputs also exist (the run-start identity version and a
system identity with at least one observed part); otherwise it is exp1 and
names what was missing in `v2_unavailable` (`metrics/experiment.py`). An
image built from older source, `1.6.0` included, writes no markers, so its
runs stay exp1, whose corpus id hashes config fields the AML generator never
reads (section 12), and it cannot generate a registered corpus, whose seed
reaches the pod only as `LB_DATAGEN_SEED`.

### 3.2 Scale factor

Scale maps to domain dimensions in `config/scale.py` and independently in
the generator (`datagen_rs/src/world.rs`):

| Dimension | Formula | Scale 1 | Scale 10 |
|---|---|---|---|
| Parties (population) | round(111,111 x scale), at least 100 in the generator (Lakebench's own estimate floors at 1) | 111,111 | 1,111,110 |
| Base payments | population x 4 per month x 60 months | 26,666,640 | 266,666,400 |
| Screening rows | added on top of base payments; the count depends on the seed | 5,227 (recorded, by difference) | 52,194 (recorded, by difference) |
| Expected pacs.008 size (the `scale_ratio` denominator) | scale x GB per scale unit, measured: 8.47 at scale 1 and 9.36 at scale 10, interpolated in log10(scale) between them and held outside (`config/scale.py`) | 8.47 GB | 93.6 GB |
| Files at 64 MiB, snappy | clamp(rows x 345 B / file size, 64, rows / 1000) | 137 | 1,371 |

The entity mix is 55% person, 40% company and 5% financial institution;
about 89% of homes are US; each entity holds 1 to 4 accounts; half the
population are customers of the reporting institution. Unset, datagen pod
parallelism follows the scale (2 pods up to scale 5, 4 above 5 up to
scale 10) and, above scale 50, is raised to what the cluster's CPU allows
(`config/autosizer.py`), and financial above scale 100 is raised to at least
8 pods. A value you set is used exactly, with a warning when the cluster
cannot fit it (a continuous run is then refused at preflight) or it is
under that floor. Pod memory comes
from an autosizer model. Neither pod count nor memory changes row content,
but the pod count is passed to the generator as `--total-nodes`, which is
part of the resolved arguments corpus id v2 hashes. Above scale 50, two
clusters of different size therefore produce different corpus ids unless
`datagen.parallelism` is set to a value both clusters fit (section 7.3).

### 3.3 Seed policy

A seed is mandatory for the financial schema: the generator exits 2 when it
receives no seed (and, in the default image or any image built from this
release's source, when it receives both `--seed` and `LB_DATAGEN_SEED`).
Resolution and refusals run at config load (`config/datagen_seed.py`,
called from the workload validator in `config/schema.py`); a refused config
exits 2 before anything is deployed.

| Seed class | Rule |
|---|---|
| Unset | resolves to the calibration seed, 43. With `corpus_role: evaluation` or `robustness` an unset seed is refused: a registered corpus names its seed, which is checked by hash. With `corpus_role: calibration` an unset seed uses 43 |
| Development | any seed that is neither spent nor held out is accepted. The AML protocol allows generator tuning only on the calibration seed 43 and its four pre-registered replicates; Lakebench does not enforce that. Declaring `corpus_role: calibration` requires seed 43 |
| Spent | refused for every use, including a declared role: the pre-registration's spent list (42 and 50000042), every seed with a recorded or burned look in `aml_registered_looks.json`, and the spent list in `heldout_hashes.json`. The default generator refuses again at start the spent seeds it knows (its compiled list and the hash file's spent list), not seeds known only from a recorded look |
| Held out (evaluation, robustness) | the repository stores no held-out seed, only a salted SHA-256 hash per role in `spark/data/aml/heldout_hashes.json` (append-only), with the current hashes also compiled into the guard. A config that declares the matching `corpus_role` and names a seed whose hash matches that role is accepted while the pre-registration has registered looks open; without the role, or with a seed that hashes to another role, it is refused |

`datagen.robustness_perturbation` (financial only): at config load it is
required with `corpus_role: robustness` and refused with `calibration` or
`evaluation`; without a role it is allowed on any seed the guard accepts. At
start the generator refuses the robustness seed without it, and the
evaluation seed or a calibration replicate with it. The reference scorer
reads the perturbation from the manifest's stamp, not from the config.

The held-out check runs at five points, each hashing the seed it sees and
comparing it with the per-role hashes: config load and the look guard
after it (below); generator start (`datagen_rs/src/heldout.rs`, in the default image; an image
built from older source, `1.6.0` included, has no such check), which refuses to generate a financial
corpus without the hash file Lakebench mounts on the datagen pod;
bronze-verify (`spark/scripts/bronze_verify_financial.py`) and the recall
scorer (`spark/scripts/score_financial.py`), which recover the corpus seed
from every manifest row and refuse a held-out or spent corpus; and the
cluster reference scorer (`spark/scripts/score_financial_reference.py`),
which does the same and refuses a corpus from a spent seed, or from a
held-out seed outside its registered run, whatever the config claims. No refusal message prints a held-out seed.

**Registered corpora.** When a financial config declares `corpus_role:
evaluation` or `robustness`, or names a seed whose hash matches a held-out
role, `lakebench generate` writes the seed into a Kubernetes Secret in the
deployment's namespace, named after the seed's salted hash
(`config/seed_secret.py`). The datagen Job and the reference scorer read it
as `LB_DATAGEN_SEED` from that Secret, so it appears in no Job argument, pod
spec or SparkApplication spec; `lakebench destroy` deletes it. Every other
seed, 43 included, is passed as a plain argument. Set `datagen.corpus_role`
only on the one deployment that generates a registered corpus, and score
that corpus only with `scripts/aml_gate.py --registered`. That script takes
the held-out seed only from `--seed-file`, refuses it on `--seed`, records
the seed as spent before any model is fitted and the report's sha256 before
it prints a verdict. No `lakebench` command records a look.

**The look guard** (`aml/look_guard.py`). `run`, `benchmark`, `query`,
`reproduce` and the `financial` commands refuse a protected corpus right
after the config loads, before any cluster call, with exit 2
(`run.protected_corpus`): a config that declares `corpus_role: evaluation`
or `robustness`, names a seed whose hash matches a held-out role, or (AML)
points its bronze datagen prefix at one where this host generated a
registered corpus; `financial reproduce` refuses a stored run record from
such a corpus. `generate` writes
a protected corpus only with `--registered-corpus --yes` and a digest-pinned
`images.datagen`; it records the attempt in the host's corpus ledger
(`~/.lakebench/aml_corpora.jsonl`, `LB_AML_CORPORA_LEDGER`) before its first
cluster call, then `generated` (with the corpus fingerprint and the pod
image ids) or `failed`, and refuses a seed whose look was already taken.
For the financial workload bronze-verify also reads every row of the corpus
manifest before anything else and stops (exit 2) on a corpus from a
held-out or spent seed, on a manifest no corpus seed can be recovered from,
and on a batch corpus with no manifest; a `run --stage` subset runs that
check alone first, and a check that could not run (a storage error) exits 1.
`scripts/aml_gate.py --registered` scores a look only when the ledger holds
a matching `generated` entry for the corpus it reads.
`scripts/aml_heldout_audit.py` lists this host's run records, journals,
ledgers and ledger buckets that touch a protected corpus; an AML record is
known by its workload name too, so one missing its corpus block is listed
as unidentified. A run record, its datagen fleet record and its
HTML report never show a protected seed (held out, spent or with a recorded
look): it is recorded as its salted reference (`{"seed_ref": ..., "role":
...}`, the role set for a held-out seed), and as withheld (`{"seed_ref":
null}`) when the held-out record cannot be read. Any other seed stays in
plaintext, so a record names its corpus; the corpus id is unchanged. The release evidence also rests on the
expected-results corpus id and bronze-verify's in-run refusal. `destroy` and the
read-only commands skip the load-time seed check, so a deployment that
generated a registered corpus can still be torn down.

The AML protocol grants each held-out seed exactly one registered look. The
look, its calibration and its predictions use one datagen image, named by
digest; a change to generator output needs a new image and a
`MODEL_VERSION` bump, and a look on that image needs its calibration re-run.
Datagen is never tuned to a rule's recall or false-positive rate on any
seed.

### 3.4 Delivery modes and identity

`datagen.mode` selects the S3 delivery pattern for pacs.008 files only:
`batch` is one PUT per file, `continuous` streams each file through an S3
multipart upload as row groups close, and `auto` resolves to `continuous` at
every scale. Party and account are always multipart, and manifest and
watchlist always a single PUT, whatever the mode. Row content is
independent of delivery mode, thread count and node count: generator tests
check row-multiset identity across delivery modes and across thread,
file-size and node layouts (`datagen_rs/tests/cycles.rs`). `datagen.file_size`
is fixed at `64mb` for every workload; any other value is refused at config
load.

### 3.5 Content, time range and dirty data

The corpus window is fixed: 2021-01-01 00:00 to 2026-01-01 00:00 (60
months, 1,826 days); Lakebench does not pass `--corpus-months`, so every
Lakebench corpus spans those 60 months. Timestamps are zone-less
(interpreted as UTC by the Spark stages and the query engines) and
whole-second. Volume follows a weekday, salary-day, quarter-end and intraday
calendar with per-country holiday roll-forward (`datagen_rs/src/timing.rs`).

The generator ignores `datagen.timestamp_start`, `timestamp_end` and
`dirty_data_ratio`; there is no dirty-data injection. `timestamp_end` is not
inert downstream: it is the first rung of the silver data clock and sets
`silver.entity_profiles.profile_updated_ts`. With the default config
`timestamp_end` is unset, so the clock falls through to the clock
bronze-verify recorded, then `timestamp_start`, then the run date. Both
published batch records show `data_clock_source: fallback_default`, so their
`profile_updated_ts` is the date the run executed. No query or rule reads
that column, but it is a business column of a frozen silver table: two runs
of the same seed on different dates differ in it unless `timestamp_end` is
set, which does not change the generated corpus.

The only deliberate noise is synthetic-identity PII overrides in the party
file and, in the screening track, creditor names written as an alias, a
typo, a token-order swap, a different romanisation or a dropped suffix, plus
namesake decoys. Party and company names come from fixed per-country pools
(`datagen_rs/src/realism.rs`).

### 3.6 Typologies

Fifteen typologies are planted (`datagen_rs/src/typology.rs`): `bipartite`,
`cycle`, `fan_in`, `fan_out`, `gather_scatter`, `random`, `scatter_gather`,
`stack`, `synthetic_identity`, `corridor_high_risk`, `cross_border_cycle`,
`dormant_reactivation`, `micro_structuring`, `rapid_layering` and
`tbml_repeated_invoice`, plus `sanctions_match` and `pep_match` from the
screening track (`datagen_rs/src/screening.rs`), for 17 manifest types. Each
typology receives an equal share of a row budget of 0.1% of base payments,
so instance counts differ by typology with instance size. Every instance's
subject is a customer.

Nine typologies have a designated rule (section 4.2). The other eight
(`bipartite`, `cross_border_cycle`, `fan_in`, `fan_out`, `random`,
`scatter_gather`, `synthetic_identity`, `tbml_repeated_invoice`) have none;
`benchmark/aml_queries.py` lists each with the reason (`UNMAPPED_TYPOLOGIES`).
`random` is a control: its hit rate is the chance floor (section 8.4). The
pre-registered behavioural subset is `gather_scatter`, `rapid_layering`,
`stack`, `dormant_reactivation`, `micro_structuring` and
`corridor_high_risk`.

### 3.7 Multi-cycle

With `--cycles N`, cycle n emits the rows whose calendar mass falls in
[n/N, (n+1)/N); the union of all cycles equals the one-shot corpus. Cycle 0
keeps the one-shot names (`part-NNNNNN.parquet`, `manifest/manifest.parquet`);
cycle n > 0 writes `part-cNNN-NNNNNN.parquet` and
`manifest/manifest-cNNN.parquet`, so bronze accumulates. Party, account and
watchlist are written in cycle 0 only. The deployer's per-cycle timestamp
windows do not apply to the financial schema.

## 4. Pipeline

### 4.1 Batch mode

`lakebench run` executes, in order (`cli/_run.py`):

| # | Stage | Input | Transformation | Output |
|---|---|---|---|---|
| 1 | datagen, only with `--generate` | seed, scale | section 3 | raw corpus |
| 2 | bronze-verify | `pacs008/` Parquet, manifest | schema and null checks; registers the files as an Iceberg table zero-copy (`add_files`), falling back to CTAS when the source exceeds 1.5 TiB or 800,000 files or when `add_files` fails (CTAS doubles bronze storage); CTAS of the manifest as `bronze.manifest` (a missing manifest is a warning); writes the bronze data clock (max settlement date) | bronze table, `bronze.manifest` |
| 3 | silver-build | bronze, party, account | full rebuild each cycle: payments to USD at fixed FX rates, deterministic entity ids (a hash of name, country, city and LEI; no fuzzy matching), one account per IBAN, double-entry statements with running balances, counterparty pair aggregates, per-entity behavioural profiles; then a sealed marker | six silver tables plus `silver_batch_versions` |
| 4 | gold-finalize | sealed silver | baseline dashboard; nine detection rules; the W1 and W4 projections (`gold.entity_clusters`, `gold.risk_scores`, best effort); TM operations | gold tables (2.4) |
| 5 | score-financial | `gold.alerts`, `gold.detection_status`, manifest | recall and false-positive scoring (section 8.4) | `recall.parquet`, `recall.json` |
| 6 | AML batch gate, TM verdict | driver logs, `recall.json` | section 5 | pass or fail |
| 7 | pre-compaction benchmark (scale below 50, maintenance on) | silver, gold | warm-up pass, then FQ1 to FQ8, not fingerprinted | `pre_compaction_qph` |
| 8 | table maintenance | maintained tables | expire snapshots and orphan removal, compaction on silver and gold only (never bronze), under policy `m2-2026-09-26` and one shared 1,800 s budget; DuckDB recipes run none | `maintenance_outcomes` |
| 9 | settle wait (skipped when no maintenance statement ran), then the scored benchmark | silver, gold | 12 queries when TM operations produced a `pass` or `fail` verdict, otherwise FQ1 to FQ8; power mode, hot, `benchmark.iterations` samples per query | QpH, fingerprints |

Batch profiles are computed directly over the whole corpus, and store the
Welford M2 term so the continuous stream can merge later batches with the
parallel Welford recurrence. Business rules applied in silver that affect
every downstream figure: `txn_type` is always `wire`; `cross_border` is NULL
when either country is unknown; `regulatory_reported` is true when the
message carries regulatory reporting; statement opening balances are seeded
deterministically from the account id; `current_balance` is the last
statement entry's balance, NULL for an account with no statements.

Without `--generate` the run measures whatever corpus is already in bronze.
Its corpus parameters come from the datagen fleet record an earlier
`lakebench generate` left for the namespace; only when there is none are
seed and image recorded as declared by the config (`experiment.corpus.observed:
false`, with `observed_note`). Every run also reads the generator's
per-node markers under the bronze prefix at run end and records corpus id v2
and the generator lineage from them (`metrics/corpus_identity.py`). A
publishable run should generate its corpus in the same run.

A non-empty datagen prefix with `--generate` and without `--regenerate` is
refused with exit 3 before datagen starts (exit 4 when the bucket cannot be
listed). `--regenerate` clears only the datagen prefix of the bronze bucket,
aborting incomplete multipart uploads, and only when this deployment created
the bucket. On any other bucket the run refuses unless
`--allow-stale-bronze` is given; datagen then writes over the existing
objects and the record carries `datagen_stale_bronze` and a verdict warning.
Before generating, the run deletes any earlier datagen Job and refuses with
exit 3 if its pods are still running five minutes later. A datagen that
exceeds its wait budget (the per-job timeout) stops the datagen Job and
fails the run with exit 1; `verdict.reasons` says "datagen timed out".

Stages 5 and 6 run only on a full run (no `--stage`) whose three pipeline
stages succeeded. The scorer refuses a `gold.detection_status` written by
another run, so a run with stale gold carries no recall.

Silver-build invariants: every pre-staged frame has at least one row before
any write; statements and accounts are non-empty after writing (otherwise
`silver.transactions` is truncated and the stage fails); account rows equal
distinct IBANs; silver output rows > 0. It refuses to run while a continuous
stream's `_STARTED` marker exists, unless `--force-rebuild` (batch runs
only). Bronze-verify fails on a missing required column (driver exit 2) or
any NULL settlement date (driver exit 3), and either fails the stage; it
only warns on NULL `uetr`, `txn_id` or `msg_id`, and has no zero-row or
duplicate check of its own (the record gates in section 5 cover zero rows).

Multi-cycle batch (`cycles > 1`): the run generates every cycle itself and
refuses `--generate` and `--repeat` with exit 2; before cycle 0 it clears an
owned datagen prefix as `--regenerate` would. Silver rebuilds from
cumulative bronze, and gold re-detects over the whole corpus with run id
`<run_id>-cN` (a single-cycle run also uses `<run_id>-c1`), deleting other
cycles' alerts. The last cycle's gold is the result; the TM verdict covers
every cycle.

### 4.2 Detection rules

Gold-finalize runs the rules in the order W5, W6, W2, W3, W17, W4, W7, W8,
then W1 last (`spark/scripts/gold_finalize_financial.py`). Each rule is
first marked `pending` in `gold.detection_status`, alerts from other runs
are deleted, then each rule's rows are replaced. A rule that raises
`RuleSkipped` is recorded `skipped` with a reason. Any other exception is
isolated: the rule is recorded `error`, its alerts are removed, and the
other rules still run.

| Rule id | Target typology | Base reason code | Conditional reason codes | HIGH priority cut | Modes |
|---|---|---|---|---|---|
| `W1_connected_components` | `gather_scatter` | `W1_COMPONENT` | `W1_LARGE_COMPONENT` | component size 8 | batch |
| `W2_structuring` | `micro_structuring` | `W2_SUB_THRESHOLD_BURST` | `W2_BENEFICIARY_FAN_IN`, `W2_HIGH_COUNT` | 6 in-band payments | batch, continuous |
| `W3_round_tripping` | `cycle` | `W3_CYCLE` | `W3_LONG_CYCLE` | 4 hops | batch, continuous |
| `W4_risk_propagation` | `rapid_layering` | `W4_FAST_PASS_THROUGH` | `W4_MULTI_CHAIN` | 3 chains | batch, continuous |
| `W5_sanctions_match` | `sanctions_match` | `W5_SANCTIONS_HIT` | `W5_EXACT`, `W5_FUZZY`, `W5_RESCREEN` | -- | batch |
| `W6_pep_counterparty` | `pep_match` | `W6_PEP_HIT` | `W6_EXACT`, `W6_FUZZY` | -- | batch |
| `W7_cross_border_high_risk` | `corridor_high_risk` | `W7_HIGH_RISK_CORRIDOR` | `W7_FATF_BLACK`, `W7_FATF_GREY`, `W7_SYNTHETIC_CORRIDOR` | -- | batch |
| `W8_dormant_reactivation` | `dormant_reactivation` | `W8_DORMANCY_GAP` | none | -- | batch |
| `W17_layering_chain` | `stack` | `W17_CHAIN` | `W17_LONG_CHAIN` | 5 hops | batch, continuous |

The rule-to-typology map is `RULE_TARGETS` in `benchmark/aml_queries.py`;
scoring and the verdict read it, never the manifest's coarser `workload`
category. The cuts are `HIGH_PRIORITY_CUTOFFS` in
`spark/scripts/detection_rules.py`.

**Reason codes** (`spark/scripts/aml_reason_codes.py`). Every alert carries
`reason_codes`: its rule's base code, then each conditional code whose
condition holds on the alert. The base code makes a rule's codes cover all
of its alerts. Conditional codes read only columns the rule's own projection
holds and only cut points the rule already has (its HIGH priority cut, the
screen's exact or fuzzy split at similarity 1.0, the rescreen pass, the
corridor list's risk tier), so no code adds a threshold, and no code changes
which alerts a rule raises. `W5_RESCREEN` marks an alert raised when a list
version listed a prior counterparty. A code that reads the HIGH cut
(`*_LARGE_COMPONENT`, `*_HIGH_COUNT`, `*_LONG_CYCLE`, `*_MULTI_CHAIN`,
`*_LONG_CHAIN`) always marks a HIGH-priority alert.

### 4.3 Continuous mode

`lakebench run` in continuous mode (`pipeline.mode: continuous`;
`cli/_sustained.py`):

1. Resolves the trickle (`max_files_per_trigger`). Unset, the run derives it
   so arrival lasts about 1.2 x `run_duration`: int(files x trigger seconds
   / (1.2 x `run_duration`)), clamped to 1 to 50 (a Lakebench-imposed cap),
   where files is the nominal corpus size divided by the 64 MB file size. At
   scale 1 with the default 1,800 s window this is 1 file per trigger
   (recorded: `run-20260929-205000-ebb26f`, `continuous.trickle`). An
   explicit value that would offer the whole corpus before the window ends
   is refused at run start with exit 2, as is a window longer than the corpus
   can fill at one file per trigger, a window shorter than three gold
   refresh intervals (900 s at defaults), and an explicit
   `retention_interval` or `compaction_interval` that cannot fire inside the
   window unless maintenance is disabled. The resolved value is recorded in
   `continuous.trickle` and `experiment.limits.max_files_per_trigger`.
2. Stops leftover streams, deletes the stream checkpoints, and clears the
   raw datagen prefix unless `--skip-generate`.
3. Starts datagen, waits up to 300 s for the first Parquet file, and runs a
   bronze-verify preflight in `schema` mode. It drops bronze (without PURGE,
   so registered datagen files survive) and recreates it empty with the
   inferred schema plus `ingest_ts`; drops all seven silver tables; drops
   and re-registers `bronze.manifest`; and drops the TM operations tables and
   the five gold tables `alerts`, `risk_scores`, `entity_clusters`,
   `daily_dashboards` and `detection_status`.
4. Runs three concurrent jobs for the window (`run_duration`, default
   1,800 s):
   - **bronze-ingest**: Structured Streaming over the Parquet prefix, the
     resolved `max_files_per_trigger` every `bronze_trigger_interval`
     (default 30 s); appends with `ingest_ts`.
   - **silver-stream**: Iceberg streaming read of bronze, `foreachBatch`
     every `silver_trigger_interval` (default 60 s). Per micro-batch:
     transactions (idempotent delete-then-append on replay), edges
     (per-batch aggregates; readers must SUM), dimension MERGE with KYC,
     statements and balances, profile MERGE with the Welford recurrence,
     then the sealed marker last. Every MERGE reads a source materialised
     from a local checkpoint (`spark/scripts/common.py`,
     `materialised_source`), because on Spark 4.1 with Iceberg a MERGE whose
     source is a temp view over an Iceberg table fails inside Spark.
   - **gold-refresh**: a timer loop every `gold_refresh_interval` (default
     5 min). Each tick pins one sealed silver snapshot and runs W4, W2, W17
     and W3 over the full pinned corpus. W5, W6, W1, W7 and W8 are written
     to `gold.detection_status` as `skipped` with reason `mode-excluded`;
     they do not appear in `experiment.rules.skipped`, and
     `experiment.support.mode_note` names them. The tick then refreshes the
     baseline dashboard rows, logs freshness and time to detect, and runs a
     TM pass when one is due (every `continuous_interval_seconds`, default
     1,800 s), with a final pass before the window closes. After five
     consecutive failed ticks (or five consecutive failed baseline
     refreshes) the driver exits 1, and the operator restarts it under the
     streams' restart policy.
5. Runs in-stream benchmark rounds: power mode, hot, one sample per query.
   With TM operations enabled each round first probes `gold.cases` for a
   case of this run (an untimed query, 60 s timeout). Once a case exists the
   round runs all twelve queries; before that it runs FQ1 to FQ8 and is
   labelled (`investigator_queries`: `included`, `absent_no_cases` or
   `probe_failed`). Rounds are not fingerprinted. With
   `architecture.benchmark.investigator_sessions` set (1 to 32; financial,
   TM operations on, Trino or Spark Thrift; refused otherwise), one extra
   round right after the first round that ran the investigator queries runs
   that many concurrent sessions, each working one case of this run: IQ1 to
   IQ3 bound to that case and IQ4 unchanged
   (`benchmark/investigator_sessions.py`). It is not a benchmark
   round (QpH and the round count leave it out) and is recorded in
   `continuous.investigators` with per-query latency, the detection ticks
   it overlapped (`tick_delta`, `load_label`) and its Lakebench-set query
   timeout; the number of sessions that ran is an outcome condition, so two
   runs that ran different numbers compare as not like-for-like
   (`docs/aml-scoring.md`).
6. Maintains tables inside the window: expire snapshots and orphan removal
   every `retention_interval` (default `run_duration / 3`, within 300 to
   7,200 s), and compaction every `compaction_interval` (default twice
   that). While streams run, AML compacts silver only, Iceberg expiry is
   floored at 1 h, and orphan removal never uses less than 24 h 10 min. The
   applied retention is recorded in `continuous.retention`.
7. **Drains gold-refresh and scores.** When the window closes the CLI
   writes a drain marker under the gold-refresh checkpoint
   (`cli/_aml_post.py`). The driver finishes its current tick, logs the last
   completed cycle and idles; the CLI waits up to 1,800 s for that line (a
   Lakebench-imposed budget). A drain that times out, a gold-refresh deleted
   before it drained, or a driver that restarted and drained before any
   tick fails the run. If the run passed every gate, `score-financial` then
   runs in covered mode over exactly the snapshots the last completed tick
   read (section 8.4). `lakebench stop` drains the same way with a 300 s
   budget.
   Then, on the same condition, `time-travel-financial` re-reads every
   `silver.transactions` snapshot the ticks recorded (section 8.2,
   time-travel reads). It runs with the streams stopped, outside every
   measured interval, and its check never fails the run.
8. Applies the continuous window gate, the drain check, the AML continuous
   gate, the TM verdict and the record gates (section 5).

The end-of-run result check does not run for AML continuous: the record
says why in `experiment.results.not_checked`. No published record carries
a covered score yet; the published continuous record predates the drain.

### 4.4 Out-of-run AML commands

None of these is part of `lakebench run` or its verdict (`cli/_financial.py`):

- `lakebench financial score --manifest <uri> --output <uri>` runs the
  recall scorer outside a run; with no run id it scores the run
  `gold.detection_status` names.
- `lakebench financial replay --rule <id>` re-runs one rule against the
  `silver.transactions` snapshot committed at least `--depth-months`
  (default 60) before now. The depth is measured on snapshot commit time,
  not settlement dates, so a deployment younger than the depth has no
  snapshot and the job exits non-zero. It writes to the gold alerts table
  name with an `_replay` suffix (`--output-alerts` to change it), replacing
  only that rule's rows.
- `lakebench financial reproduce CONFIG --alert-id <id>` (`--run RUN_ID`)
  reruns one batch alert's rule on exactly what that run's gold-finalize
  read (`cli/_financial.py`, `spark/scripts/reproduce_financial.py`).
  Gold-finalize logs the snapshot of `silver.transactions`,
  `silver.entities` and `silver.silver_batch_versions` it reads, and the
  batch scorer fingerprints each (`financial_scoring.read_snapshots`). The
  command reads that run record (the deployment's latest AML batch record
  on this host by default; exit 2 when there is none), refuses one from a
  protected corpus (exit 2) or with no read snapshots (exit 4) before any
  cluster call, reads each table at its recorded snapshot (or, once that
  expired, the current table when its fingerprint is unchanged), filters
  the transactions to the batches sealed when gold read them, runs the rule
  with gold's parameters and matches the alert on rule, entity, `alert_ts`
  and its related transactions. It exits 0 when the alert is reproduced, 1
  when it is not or the alert is not in `gold.alerts` for that run, and 4
  when a snapshot is gone and the content changed; the result is written
  to `scoring/reproduce/<alert_id>/result.json`. Continuous alerts are not
  reproduced. This is a different command from `lakebench reproduce`.
- `lakebench financial reference-score` runs the reference detector, the
  band-leakage report and the fidelity gate, installing hash-checked
  scikit-learn wheels from the deployment's dependency set in an init
  container; `--leakage-threshold` defaults to 0.10. On a deployment that
  declares `corpus_role: evaluation` or `robustness` its driver refuses
  after submission, so the command exits 1. Its
  features are built from silver read without the sealed-batch filter
  (section 12).

## 5. Correctness contract

A PASSED batch run asserts the gated rows of the table below, including the
record gates the verdict applies before the record is saved
(`metrics/verdict.py`). The row relations between layers (bronze = generated
rows, `silver.transactions` = bronze, the relations of section 2.3) are
expected at a fixed seed but are not checked; check them from `metrics.json`
(`jobs[].output_rows`, `jobs[].silver_tables`) before publishing.

| Check | Where | Gates the run? |
|---|---|---|
| Non-empty datagen prefix at `--generate` without `--regenerate` | `lakebench run` | yes (exit 3 before datagen; exit 4 when bronze cannot be listed) |
| Datagen wait budget exceeded | `lakebench run` | yes (exit 1; "datagen timed out") |
| Missing required bronze column; NULL settlement date | bronze-verify | yes (stage fails) |
| NULL `uetr`/`txn_id`/`msg_id` | bronze-verify | no (warning) |
| Silver frames non-empty, accounts = distinct IBANs, output > 0 | silver-build | yes (stage fails) |
| `detection_status` written | gold-finalize | yes (stage fails) |
| Any stage failure | `lakebench run` | yes (exit 1) |
| Any rule `error`; zero alerts (from `recall.json`, else per-rule log counts) | AML batch gate (`cli/_run.py`) | yes |
| Per-rule counts and scoring both missing | AML batch gate | the CLI warns; the `layer_rows` record gate fails a gold-finalize whose log was not read |
| A skipped rule whose target typology is in the pre-registered behavioural subset | AML batch gate | no (warning, every run) |
| Every layer has rows (a continuous layer with no row figure passes on its measured size) | record gate `layer_rows` | yes |
| Batch: the expected rules ran, none errored, detection alerted, and every skip is an allowed one (W1 `giant-component` or `vertex-cap`; W3 and W17 `path-cap`). A W3 or W17 `edge-cap` skip is not allowed | record gate `aml_rules` | yes; an allowed cap skip passes and is labelled in `verdict.qualifiers.rule_caps` |
| Continuous: no mode-excluded rule ran, and gold-refresh counted alerts. The rules are taken from the time-to-detect lines, which exist only for rules that alerted, so rule errors, skips and rules that raised no alert are not judged | record gate `aml_rules` | yes |
| Batch bronze reached 95% of the scale's expected volume | record gate `scale_ratio` | yes |
| No benchmark query outside `allow_empty` returned zero rows | record gate `query_answers` | yes |
| TM invariants: monitored population, reconciliation (monitored + excluded = source), every alert dispositioned, one row per alert identity, no NULL disposition, non-customer alerts declared, escalated <= alerts, cases <= escalated, SARs <= cases, one open case per customer, funnel monotone, continuing reviews fire, reviews not folded into determined cases, history stable, workflow completed after the TM tables were written | TM verdict (`metrics/tm_ops.py`) | yes, only when the verdict is `fail`; `unknown` (an unchecked invariant, an interrupted pass, an unparsed cycle log), `not_run` and `disabled` only warn |
| Any failed query; a non-`allow_empty` query with 0 rows | benchmark gate | yes. Batch: the scored round. Continuous: a failed query in any round fails the run; the empty-result check applies to the last round only |
| Recall, precision, false-positive rate | score-financial | no; scoring is best effort |
| Subject-customer check (every planted subject is a customer) | score-financial | no (warning) |
| Reference detector, band-leakage report, fidelity gate | `financial reference-score`, `scripts/aml_gate.py` | not part of `run` |
| Continuous: data arrived during the window, silver committed and gold refreshed on new data at least twice inside it, gold freshness measured | continuous window gate | yes |
| Continuous: gold-refresh drained (section 4.3, step 7) | drain check | yes |
| Continuous: gold-refresh logs present, a cumulative-alerts line, peak alerts > 0 | AML continuous gate | yes |
| Continuous: ingest ratio at least 0.95, unless the trickle bounded intake and the pipeline kept pace (then a warning); gold freshness at most half the run's duration | record gates | yes |
| Continuous: end-of-run result check | not performed; recorded in `experiment.results.not_checked` | n/a |

There are no fixed expected results for any AML query. Query correctness
across runs is established only by result fingerprints compared between two
batch runs (section 10); an answer wrong in the same way on both sides is
not detected. The detection gate reads the last gold-finalize job; the TM
verdict reads every cycle and is `unknown` when any cycle's log was not
parsed.

## 6. Query set

The AML set is `get_benchmark_queries("financial")` (`benchmark/queries.py`),
twelve queries in execution order: `FQ1_txn_full_scan`,
`FQ2_top_corridors_window`, `FQ6_structuring_scan`, `FQ3_entity_edge_risk`,
`FQ7_cross_border_concentration`, `FQ4_running_balance_window`,
`FQ5_alert_triage`, `FQ8_alert_to_entity_join`, then the investigator class
`IQ1_customer_360`, `IQ2_case_activity_12m`, `IQ3_counterparty_two_hop`,
`IQ4_open_cases_over_60_days`. The investigator class runs only when TM
operations is enabled and a TM run id is supplied: batch `run` supplies it
for its scored round when the TM verdict is `pass` or `fail`, and continuous
rounds add it once the run has a case (section 4.3). The pre-compaction
round and runs with TM disabled execute FQ1 to FQ8 only, so the pre- and
post-maintenance rounds of an AML batch run time different query sets.

`query_set_id` is `qs<N>-<first 12 hex of sha256>` over the sorted query
names and SQL of the queries actually recorded, failed ones included. In
this release FQ1 to FQ8 plus IQ1 to IQ4 is `qs12-910d16a91962`, FQ1 to FQ8 is
`qs8-ffe2bc1a012e`, and IQ1 to IQ4 alone is `qs4-bc3b5e556bf7`. A `--class`
subset produces its own id. Each continuous in-stream round also records the
query set it executed; rounds that executed different sets are not combined
into one assessed QpH (section 8.2).

| ID | Business question | Reads | Ordering and checks |
|---|---|---|---|
| `FQ1_txn_full_scan` | Payment count, distinct originators and beneficiaries, total and average USD | silver.transactions | one row; the fifth column (`avg_txn_usd`) compared at a quantum of 0.01 |
| `FQ2_top_corridors_window` | Top 100 bank-to-bank corridors by USD in the last 30 days of data | silver.transactions | window anchored on MAX(txn_timestamp), not the wall clock; total order by volume, then BICs and currency |
| `FQ6_structuring_scan` | Originators with 3 or more payments just under a currency's reporting threshold (the W2 shape) | silver.transactions | top 500, total order by count, originator, currency |
| `FQ3_entity_edge_risk` | Per-entity out-degree and outbound USD | counterparty_edges, entities | top 200 by outbound USD, entity id |
| `FQ7_cross_border_concentration` | Cross-border share of USD per corridor | silver.transactions | top 100, total order; the fifth column (`xborder_share`) at a quantum of 0.001 |
| `FQ4_running_balance_window` | Ordered statement entries for the 50 most active accounts, with the running balance recomputed in ledger order | account_statements | balance = the account's last stored `bal_after` less the sum of its entries, plus the running sum over (book_ts, txn_id, debit first), so batch and settled continuous silver give one answer; ROW_NUMBER over (book_ts, txn_id, debit first, entry_seq); outer order account, entry. Returns every entry of those accounts, so its result grows with scale (1,491,153 rows recorded at scale 10, `run-20260929-214442-825153`) |
| `FQ5_alert_triage` | Alerts by rule, priority and status with entity count and mean score | gold.alerts | ORDER BY alerts DESC only, no LIMIT; the fingerprint is order-independent; the sixth column (`avg_score`) at a quantum of 0.001 |
| `FQ8_alert_to_entity_join` | 100 most recent alerts with entity and payment count | gold.alerts, entities | total order by alert_ts, entity, rule, alert_id; `alert_id` volatile (NULL-ness only). `txns_in_alert` is `cardinality(related_txn_ids)`, which the per-alert evidence caps bound (section 7.4), so it is a Lakebench-capped count |
| `IQ1_customer_360` | 360 view of the top open case | cases, alert_dispositions, accounts, entities | case picked by status, priority, opened date, case id; TM reads scoped to the TM run id |
| `IQ2_case_activity_12m` | Monthly activity in the 365 days before the newest escalation case | cases, silver.transactions | `allow_empty` |
| `IQ3_counterparty_two_hop` | Counterparties and two-hop network of the oldest open case | cases, edges, entities, alert_dispositions | both hops sum edge rows per pair (continuous writes one row per pair per micro-batch); top 50 hop-1, top 500 overall, total order |
| `IQ4_open_cases_over_60_days` | Open cases older than 60 days | cases, entities | ordered by age, case id; `allow_empty` |

Rules that apply to every query:

- Every ORDER BY feeding a LIMIT or ROW_NUMBER ends in keys that make the
  order total.
- Engine sessions are pinned to UTC (Trino `--timezone UTC`, DuckDB
  `SET TimeZone='UTC'`, Spark Thrift session time zone UTC); the
  fingerprint reads zone-less timestamps as UTC.
- Queries are written in Trino dialect. Engine adapters change dialect
  (`date_add`, `DATE_DIFF`, and on DuckDB `cardinality` to `len`), with at
  most one `DATE_DIFF` per investigator query. The DuckDB adapter also
  replaces each catalog table name with a direct `iceberg_scan` of the
  table's storage path, so DuckDB reads table metadata from object storage
  rather than through the catalog; the run records this as
  `query_access_path: direct_storage`.
- Result check (batch): after the timed samples of the scored round, one
  untimed execution per successful query records an `rf2` fingerprint: an
  order-independent sum of per-row hashes over exact Decimal-normalised
  cells; approximate columns are summed per column, once plainly and once
  weighted by a per-row factor, and match when both sums are within
  tolerance; volatile columns count as NULL-ness. A row-count mismatch
  between the timed and fingerprint executions marks the fingerprint
  unusable. The pre-compaction round and continuous rounds are never
  fingerprinted.

**Scoring queries (not timed).** `benchmark/aml_queries.py` also defines 40
scoring queries, four per rule plus four aggregates. `lakebench run` does
not execute or time them; only its rule-to-typology map is used, by scoring
and the verdict. They are not part of the AML query set or its QpH.

| Rule | Detect | Precision | Recall | Pattern span |
|---|---|---|---|---|
| W1 | `W1_connected_components_detect` | `W1_connected_components_precision` | `W1_connected_components_recall` | `W1_connected_components_pattern_span` |
| W2 | `W2_structuring_detect` | `W2_structuring_precision` | `W2_structuring_recall` | `W2_structuring_pattern_span` |
| W3 | `W3_round_tripping_detect` | `W3_round_tripping_precision` | `W3_round_tripping_recall` | `W3_round_tripping_pattern_span` |
| W4 | `W4_risk_propagation_detect` | `W4_risk_propagation_precision` | `W4_risk_propagation_recall` | `W4_risk_propagation_pattern_span` |
| W5 | `W5_sanctions_match_detect` | `W5_sanctions_match_precision` | `W5_sanctions_match_recall` | `W5_sanctions_match_pattern_span` |
| W6 | `W6_pep_counterparty_detect` | `W6_pep_counterparty_precision` | `W6_pep_counterparty_recall` | `W6_pep_counterparty_pattern_span` |
| W7 | `W7_cross_border_high_risk_detect` | `W7_cross_border_high_risk_precision` | `W7_cross_border_high_risk_recall` | `W7_cross_border_high_risk_pattern_span` |
| W8 | `W8_dormant_reactivation_detect` | `W8_dormant_reactivation_precision` | `W8_dormant_reactivation_recall` | `W8_dormant_reactivation_pattern_span` |
| W17 | `W17_layering_chain_detect` | `W17_layering_chain_precision` | `W17_layering_chain_recall` | `W17_layering_chain_pattern_span` |

Aggregates: `aggregate_alert_volume`, `aggregate_top_entities`,
`aggregate_typology_coverage`, `aggregate_reference_vs_rule`.

## 7. Execution rules

### 7.1 Required

| Item | Rule | Enforced by |
|---|---|---|
| Composition | financial requires Iceberg and one of the 8 Iceberg 4-tuples (section 11); `custom` is refused | config load |
| Scale | above 800 refused (a datagen pod would exceed 16 GiB); above 300 up to 800 runs as unverified, with a note | config load, run start (`config/support.py`) |
| Support state | an `unsupported` combination is refused at run start (exit 2); `--local` is refused for AML | `lakebench run` |
| Stages | batch: bronze-verify, silver-build, gold-finalize, then scoring and the AML gates; continuous: bronze-ingest, silver-stream, gold-refresh, then the drain and covered scoring | `run`; scoring and gates run only on a full run (no `--stage`) whose stages succeeded |
| Corpus | batch datagen only with `--generate` (a multi-cycle run generates in its cycles and refuses the flag); continuous runs always generate unless `--skip-generate` | `run` |
| Seed | per 3.3; leave `datagen.corpus_role` unset on a `run` config | config load (seed and role checks); the role on a `run` config is not refused (section 12) |
| Benchmark iterations | `benchmark.iterations`, 1 to 100, default 3 (batch); continuous rounds take 1 sample | config load |
| Batch benchmark mode | one hot-cache power pass with one stream. A `run` config that sets `benchmark.mode` to `throughput` or `composite`, `benchmark.cache: cold`, or `benchmark.streams` above 1 is refused (exit 2); `standard` and `extended` are power runs | config load |
| Maintenance | policy `m2-2026-09-26`; `--skip-maintenance` stamps `m2-2026-09-26+skipped`; the effective outcome per operation is recorded in `maintenance_outcomes` and `experiment.effective_maintenance` | `run`, record |
| Job timeout | when `--timeout` is not given: max(3600, 120 x scale) + 900 s for financial, never below the AML bronze-verify budget max(5400, 120 x scale); an explicit `--timeout` is used as given | job submission |
| Query timeout | 900 s per query for financial in `run`, in the warm-up, pre-compaction and scored rounds alike, recorded as `benchmark_query_timeout_seconds` | runner |
| Maintenance budget | 1,800 s per statement and 1,800 s shared across expire, orphan removal and compaction; the first timeout stops the rest | `run` |
| Settle wait | probes `FQ1_txn_full_scan` until two consecutive probes agree within `maintenance_settle.tolerance_pct` (default 10%), up to `maintenance_settle.max_seconds` (default 2,700 s); skipped when no maintenance statement ran | `run` |

A publishable AML result is a full batch `lakebench run` without `--stage`,
with the benchmark and a query engine, on an unverified or supported
composition, with verdict PASSED. An AML continuous run records no result
fingerprints, so it cannot show it returned the same answers as another run.

### 7.2 Permitted tuning (still publishable)

None of these changes the Workload or Corpus keys of the experiment
identity, so runs that differ only here stay comparable. What the difference
means depends on the setting (`metrics/comparability.py`):

- **Architecture keys.** Catalog (Hive or Polaris) and query engine within
  the Iceberg recipes; component versions; the query access path (catalog
  or direct storage); the observed image digests and the dependency
  pinset; user-set executor overrides, driver overrides and Spark conf. Two
  runs that differ only here, on the same system and with matching results,
  differ in architecture alone. If the system also differs, no difference
  can be put down to either. A pair whose only Architecture difference is
  the dependency pinset is not like-for-like. Query engine `none` runs no
  benchmark, so it records no results and cannot show matching answers.
- **Not in the identity.** Executor counts and sizes that Lakebench derives
  from scale, Trino sizing, scratch storage class and size, and datagen pod
  CPU and memory. An executor override below what the profile asks at that
  scale is recorded as a Lakebench limit that bound the run (a condition).
- **Conditions** (not like-for-like when they differ): effective
  maintenance, the compaction operation that ran (Trino `optimize` or Spark
  `rewrite_data_files`), maintenance settings, benchmark iterations and
  mode, the Lakebench limits that bound, and (continuous) the in-stream
  round count. A difference in round count is an outcome of the run, so
  `reproduce` does not refuse on it.

Datagen parallelism is not permitted tuning: it is a corpus input (section
3.2). `datagen.file_size` is fixed at `64mb`.

### 7.3 Prohibited changes (invalidate a result or are refused)

These change the identity, so the runs are not comparable:
the workload version (`aml-2` in this release; records at `aml-1` are not
comparable with it); generator `MODEL_VERSION`; workload parameters
(`parameters_id` hashes every TM operations setting, `w1_max_vertices`,
`retention_workload` and `retention_months`); mode; a different query set or
any differing result fingerprint; the corpus group (corpus id, seed, corpus
role, scale, cycle count above 1, and the generator digest when both runs
observed one); records of different identity versions (exp1 against exp2);
a side with no PASSED member (a member that failed or errored is excluded
from its side); and a `lakebench
benchmark` record (`record_kind: benchmark`), which is never a comparable
run.

On an exp2 record the corpus id is v2: the hash of the generator's resolved
arguments as each datagen node wrote them in its marker (schema, a salted
seed reference, cycles, node count, file size, delivery mode,
`MODEL_VERSION`, Parquet writer settings, scale, corpus months, mode,
robustness flag, bytes per row), the generator lineage, and the cycle count
when above 1. A config edited after generation does not change it. On an
exp1 record it is the config hash of schema, generator image, seed, corpus
role, perturbation, scale, the declared timestamps, `dirty_data_ratio` and
the Customer 360 `unique_customers`, which the AML generator never reads
(`metrics/experiment.py`). A record is exp2 only when the generator wrote
its per-node markers, the run started under identity version 2, and part of
the system identity was observed; otherwise it is exp1 and lists what was
missing in `experiment.v2_unavailable`. A Customer 360 setting in an AML
config is not read by the AML generator, and config load prints a note
naming it.

Also prohibited, not enforced: modifying stage scripts, detection rules,
queries or the TM simulation without a new workload version (a change to a
benchmark query's SQL does change `query_set_id`, so such runs are not
comparable); an executor override above 28 (warned, not refused); publishing
numbers from `lakebench benchmark` (300 s query timeout, not the 900 s
`run` uses) or from `--stage` runs; using the calibration or held-out seeds
outside the protocol in 3.3.

### 7.4 Lakebench-imposed caps

| Cap | Value | Effect | How a bound cap is reported |
|---|---|---|---|
| Executor ceiling | 28 (`_MAX_EXECUTORS_SAFE`); per AML job: bronze-verify 28, silver-build 28, gold-finalize 28, bronze-ingest 20, silver-stream 28, gold-refresh 28 | per-job executor count is the profile's base at scale <= 10, else min(base + int((scale - 10) x per100 // 100), cap) | `experiment.limits.executors[].cap`, `cap_hit`, `override`, `override_bound`; `limits.bound`, `limits.bound_kinds` |
| Continuous concurrent budget | 90% of the cluster CPU left after co-resident services and, while it runs, datagen | fewer executors per stream | `limits.executors[].budget_cap` |
| Auto-sizing cuts to fit the cluster | cluster-derived | smaller resources than requested | `limits.autosize_cuts`, `limits.bound` |
| `w1_max_vertices` | 8,000,000 (`financial.w1_max_vertices`) | W1 skipped `vertex-cap` | `rules.skipped`; `limits.bound`; verdict `rule_caps` |
| W1 giant-component share | 0.5 | W1 skipped `giant-component` | `rules.skipped` only; not in `limits.bound`. Recorded at scale 1 and 10 (`run-20260929-221146-9d5345`, `run-20260929-214442-825153`) |
| W3 and W17 path budget | executor count x scratch size x 0.7 / 260 bytes per row, or `LB_PATH_SEARCH_MAX_ROWS` when set | rule skipped `path-cap`, an allowed skip | `rules.skipped`, `limits.bound`; verdict `rule_caps` |
| W3 and W17 edge cap | 3,000,000,000 flow edges | rule skipped `edge-cap`; not an allowed skip, so the batch run fails | `rules.skipped`, `limits.bound`, `verdict.reasons` |
| Per-alert evidence caps | W1 250,000 related payments; W2 beneficiary kind 1,000 (the originator kind is not cut); W4 1,000 related payments and 1,000 related entities; W5 rescreen 200 | truncates `related_txn_ids` (and W4's `related_entity_ids`); W1, W2, W4 and the W5 rescreen record the true count (`txn_total`) and `txns_truncated` in the alert evidence | `financial_scoring.evidence_capped_alerts_by_rule`, `recall_bounded_by_evidence_cap`, per-typology `bounded_by_evidence_cap` |
| TM `max_alerts_per_customer` | 50,000 | excess alerts dispositioned `over_capacity` | `limits.tm_alerts_over_capacity`, `limits.bound` |
| Pre-benchmark maintenance budget | 1,800 s | remaining maintenance stopped | `limits.maintenance_stopped`, `limits.bound` |
| Continuous trickle `max_files_per_trigger` | derived per run (1 to 50) unless set in config | sets the offered load; `sustained_throughput_rps` and `corpus_ingest_ratio` then measure a Lakebench-set arrival rate | `continuous.trickle`, `limits.max_files_per_trigger`, `limits.trickle_bound` and a `trickle:` line in `limits.bound` (never in `bound_kinds`, since every continuous run sets one) |
| Continuous drain budget | 1,800 s (300 s under `lakebench stop`) | a tick that takes longer fails the run | the drain problem in `verdict.reasons`; `financial_scoring.status: not_scored` |
| Query timeout | 900 s | query fails, run fails | query error |

`intake_limit` reads `trickle_rate` only when `ingest_ratio` is below 0.95,
so a pipeline that keeps up reports `none`; `limits.trickle_bound`
(`value`, `source`, `kept_pace`) is the label that says whether the trickle
held intake. When it did, throughput figures measure the offered load set
by the trickle, not infrastructure capacity (`metrics/bounds.py`).

## 8. Metrics

Units, directions and bands are the metric registry's
(`metrics/metric_registry.py`), which `reproduce` and the
report read; `pipeline_benchmark.score_descriptions` gives a
one-line description of each score a run recorded. A direction of `none`
means a delta in the metric has no better side. Pipeline scores are in
`metrics.json` under `pipeline_benchmark.scores`; AML scoring is the
top-level `financial_scoring` block, TM operations the top-level
`tm_operations` block, and the storage multiple the top-level
`storage_multiple` block. `tests/test_benchmark_specs.py` fails when the
registry gains a financial metric this spec does not name.

### 8.1 Batch

Primary: `time_to_value_seconds`, lower is better.

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `time_to_value_seconds` | s | lower | wall-clock seconds from the start of the first pipeline job (bronze-verify) to the end of the last (gold-finalize); a job that never recorded an end counts at its start. Datagen, maintenance, settle and the benchmark are outside it. Recorded: 874.51 s at scale 1 (`run-20260929-221146-9d5345`) and 5,086.03 s at scale 10 (`run-20260929-214442-825153`), n=1 each |
| `time_to_value_datagen_excluded_seconds` | s | none | diagnostic, multi-cycle Customer 360 batch only: the cycles' datagen seconds left out of `time_to_value_seconds`. An AML record never carries it (collector.py sets it for customer360 only) |
| `total_elapsed_seconds` | s | lower | sum of the elapsed seconds of every stage, datagen and the query benchmark included |
| `total_data_processed_gb` | GB | none | sum of every stage's input GB, including the query benchmark's read of gold |
| `pipeline_throughput_gb_per_second` | GB/s | higher | `total_data_processed_gb / time_to_value_seconds` |
| `total_core_hours` | core-h | lower | requested executors x cores x elapsed / 3600 over the bronze, silver and gold jobs (drivers excluded) |
| `compute_efficiency_gb_per_core_hour` | GB/core-h | higher | `total_data_processed_gb / total_core_hours` |
| `scale_ratio` | ratio | target 1.0 | bronze input GB of the last bronze-verify job / the expected bronze GB for the scale; a batch run below 0.95 fails |
| `composite_qph` | QpH | higher | power QpH of the scored round after maintenance: successful queries / sum of their per-query median seconds x 3600 |
| `pre_compaction_qph`, `post_compaction_qph` | QpH | higher | the pre- and post-maintenance rounds. The pre round is 8 queries and the post round 12 when TM produced a verdict, so the two are not a before-and-after pair |
| `maintenance_value_pct`, `maintenance_value_reason`, `maintenance_paired_queries` | pct, text, count | none | change from the pre to the post round over the queries that succeeded in both (`maintenance_paired_queries`); null within noise, with the reason. This is the before-and-after figure |
| `benchmark_samples_per_query`, `qph_spread` | count, struct | none | samples behind the medians; QpH of the slowest and fastest combination of samples |
| `maintenance_stopped`, `maintenance_stop_reason` | bool, text | none | pre-benchmark maintenance stopped on a statement timeout or its budget, and why |
| `maintenance_live_streams`, `maintenance_live_streams_reason` | bool, text | none | stream apps were present, or could not be read, during pre-benchmark maintenance |
| `maintenance_settle_seconds`, `maintenance_settled`, `maintenance_settle_capped`, `maintenance_settle_verified` | s, bool | none | the settle wait (7.1); not in time to value. `maintenance_settle_verified` is false at scale 50 and above, where no pre round ran |
| `cycle_progression` | struct | none | per-cycle elapsed, QpH and table health (multi-cycle) |

### 8.2 Continuous

Primary: `data_freshness_seconds`, lower is better.

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `data_freshness_seconds` | s | lower | worst-case gold staleness during the window: the maximum per-stage freshness, using active freshness once the corpus drained; null when unmeasured. Recorded: 202 s (`run-20260929-205000-ebb26f`, scale 1, n=1) |
| `time_to_detect_seconds`, `time_to_detect_p95_seconds`, `time_to_detect_max_seconds` | s | lower | from the newest bronze ingest of an alert's related payments to the commit of the rule's alerts on the tick that first raised it: median, 95th percentile and maximum. The median and the 95th percentile are both read from a histogram of 10 s bins, at the upper edge of the bin that reaches the quantile, capped at the measured maximum. AML only. Recorded median: 220 s (`run-20260929-205000-ebb26f`, n=1) |
| `time_to_detect_alerts`, `time_to_detect_late_alerts`, `time_to_detect_unmeasured_cycles` | count | none | alerts measured; alerts whose payments were all in silver before the previous pass (included in the percentiles); gold cycles that logged no time-to-detect line |
| `sustained_throughput_rps` | rows/s | higher | bronze rows ingested inside the window / `arrival_seconds`. When intake was trickle-bound it is the offered load set by the trickle (a Lakebench cap), not a capacity |
| `window_seconds` | s | none | the measured window length; follows `run_duration` |
| `arrival_seconds` | s | none | seconds of the window data was still arriving at bronze |
| `window_arrival_fraction` | ratio | none | `arrival_seconds / window_seconds` |
| `pre_window_rows` | rows | none | bronze rows ingested before the window opened; in no window score |
| `released_rows` | rows | none | rows the trickle had made available to bronze by the window's end |
| `ingest_ratio` | ratio | target, guard | bronze rows by the window's end / `released_rows` (falls back to `corpus_ingest_ratio`); should sit inside 0.95 to 1.05 |
| `corpus_ingest_ratio` | ratio | none | bronze rows / datagen rows. Recorded: 0.4374 in the default 1,800 s window at a trickle of 1 file per trigger (`run-20260929-205000-ebb26f`, scale 1, n=1) |
| `pipeline_saturated` | bool | none | `ingest_ratio < 0.95`; false when the trickle, not the pipeline, bounded intake |
| `intake_limit` | text | none | `none`, `trickle_rate`, `bronze_capacity`, `below_bronze_capacity` (7.4) |
| `bronze_busy_fraction` | ratio | none | share of the window bronze spent inside micro-batches |
| `corpus_drain_seconds` | s | none | seconds the trickle needs to ingest the whole corpus at the rate it held |
| `corpus_drained` | bool | none | every datagen row reached bronze and silver committed it before the window ended |
| `stage_latency_profile` | struct | none | mean micro-batch time per stream (`bronze_ms`, `silver_ms`, `gold_ms`) |
| `total_rows_processed` | rows | none | rows taken in inside the window across streams (gold re-reads of silver included) |
| `composite_qph`, `in_stream_composite_qph` | QpH | higher | median QpH of the in-stream rounds that measured one, or the single post-stream benchmark when none did |
| `composite_qph_rounds`, `benchmark_rounds_count` | count | none | rounds behind the median (0 when `composite_qph` is the post-stream benchmark), and rounds executed |
| `composite_qph_basis`, `composite_qph_by_set` | struct | none | whether the median blends rounds that executed different query sets, the rounds per executed set, and the median per set |
| `qph_degradation_pct` | pct | lower | QpH change from the first to the second half of the rounds (positive is slower), with 4 or more rounds |
| `qph_degradation_withheld` | text | none | why `qph_degradation_pct` is absent with 4 or more rounds: the rounds ran different query sets |
| `query_time_event_age_seconds` | s | none | median age of gold's newest event date at query time; tracks the corpus's position in time, not freshness |
| `total_elapsed_seconds` | s | none | the run's wall-clock seconds |
| `pipeline_throughput_gb_per_second`, `compute_efficiency_gb_per_core_hour` | GB/s, GB/core-h | higher | data processed over the window; the streams' input over their core-hours |
| `total_core_hours` | core-h | none | core-hours of the three streams, which run for the whole window, so it follows the window length |

**Time-travel reads** (`continuous.time_travel`; reported, never gating).
Each tick records the `silver.transactions` snapshot it read and the
snapshot summary's record and delete counts, from metadata only. After the
window a Spark job fingerprints each recorded snapshot still in the table,
newest first, over its business columns (`common.frame_fingerprint`; every
column less the batch-version sentinels `_batch_id`, `_stream_id`,
`ingest_ts` and `committed_at`, recorded in `hashed_columns`), writes the
hashes to the gold `scoring/<run_id>/` prefix, reads them back, and then
times a full scan `VERSION AS OF` each snapshot with the same fingerprint.
Only ticks in the current gold-refresh driver pod's log are read (a driver
restart leaves its earlier ticks out). `verified` shows that the snapshot
id the tick read still holds the row count its summary gave when the tick
read it, and reads identically twice; snapshots are immutable, so the hash
comparison shows read determinism and an unaltered hashes file, not the
content the tick saw:

| Field | Unit | Definition |
|---|---|---|
| `ticks[].state` | text | `verified` (scan rows equal the recorded count and the fingerprint equals the hash), `verified_hash_only` (the recorded count is not a live-row count: no summary count, or delete files), `mismatch`, `error` (a listed snapshot could not be read), `not_read` (the job's budget ran out), `expired` with `expired_by` (the earliest maintenance round that ran `expire_snapshots` on the table and could have expired the snapshot: committed before the round's latest possible cutoff minus its applied retention, where the cutoff is the round's end moved to the cluster clock for Trino and the round's start on this host's clock for Spark Thrift; round, end time on this host's clock, configured and applied retention, reason), or `missing_unexplained` (expired, and no Lakebench round could have) |
| `ticks[].read_s` | s | the timed read-pass scan plus fingerprint of that snapshot (the second read of it in the job, after the hash pass); a snapshot several ticks read is read once per pass |
| `current_read_s` | s | the same scan of the current snapshot, for comparison only |
| `policy` | struct | the configured retention and the expiry applied while the streams ran (floored at 1 h), always stated |
| `budget` | struct | `budget_s`, the Lakebench-imposed time-travel budget (the per-job timeout less 120 s; after the first scan of each pass the job starts no scan unless the time left exceeds 1.5 times its longest scan, and the hash pass gets half), with its label. The first scan of a pass always starts, so a single scan longer than the budget ends the wait and reads `not_run` |
| `verdict` | text | `fail` on any `mismatch`, `missing_unexplained`, `error` or `not_supported`, zero recorded snapshots, or no `verified` record (`verified_hash_only` alone compares nothing the tick recorded); `incomplete` when the budget ran out; `not_run` when the job could not run or the run failed its gates; else `pass` |

At the default 30 min retention, older tick snapshots are expected to
expire inside a 2 h window and the last ones to stay. Nothing is pinned:
Lakebench creates no tag or branch, so expiry and destroy are unchanged.
The check is shown beside the run verdict and never fails the run. No
published record carries a time-travel result yet.

On AML with TM enabled a round adds IQ1 to IQ4 once the run has a case, so
rounds can execute different query sets. `composite_qph_basis.blended` then
says so, `composite_qph_by_set` gives the median per set,
`qph_degradation_pct` is withheld, and the round median is not comparable
with another run's. A published continuous `composite_qph` must carry
`composite_qph_basis`.

### 8.3 Both modes

| Metric | Unit | Direction | Definition |
|---|---|---|---|
| `snapshots_expired`, `orphan_files_removed`, `storage_reclaimed_mb` | count, count, MB | none | what table maintenance removed |
| `maintenance_elapsed_seconds` | s | lower | maintenance and compaction time (batch pre-benchmark, or continuous in-window) |
| `maintenance_pct_of_pipeline` | pct | none | maintenance time as a share of `total_elapsed_seconds` |
| `pre_compaction_file_count`, `post_compaction_file_count` | count | none | data files before and after compaction |
| `compaction_ratio` | ratio | higher | pre over post file count; diagnostic |
| `total_s3_objects` | count | none | objects across the three buckets at the end of the run (continuous: at the window's end); unbounded growth means maintenance is not keeping up |
| `datagen_aggregate_mbps`, `datagen_mbps_per_pod` | MB/s | higher | datagen fleet and per-pod write throughput |
| `datagen_cpu_hr_per_tb` | cpu-h/TB | lower | datagen CPU-hours per TB written |
| `storage_multiple_total` | ratio | lower, diagnostic (never a directional delta) | physical over logical table bytes at run end, measured once after maintenance and only when the run passed (`storage_multiple.total.multiple`); a condition of the maintenance policy, not a system score |

The three datagen figures are derived by `reproduce` from
the datagen record, and `storage_multiple_total` from the record's
`storage_multiple` block; none is emitted under `scores`. Per-stage and
per-query figures (`<stage>_seconds`, `query_qph_<query>`) are recorded
beside these and follow the stage and query definitions above.

### 8.4 AML scoring (reported, never gating)

Recall and false-positive figures never decide the verdict; a batch run
whose scorer counts zero alerts fails (section 5), and a scoring job that
fails leaves the record without recall but does not fail the run. The
scorer is `spark/scripts/score_financial.py` (not the frozen reference
scorer); its `recall.json` is copied into `financial_scoring`.

**Batch.** Scored over the whole run after gold-finalize:

- `recall` per typology: the share of planted instances any of whose
  participant UETRs appears in `related_txn_ids` of an alert from a
  designated rule that ran. No time window. NULL unless the typology's
  status is `scored` or `partial` (other statuses: `no_rule`,
  `rule_skipped`, `rule_error`).
- `incidental_recall`: the same with any rule.
- `fp_rate`: alerts touching no manifest UETR / total alerts; null when the
  run raised no alerts.
- `fp_rate_by_rule`: 1 - the share of a rule's alerts with at least one
  related payment that touch a payment of the rule's target typology.
- `txn_precision_by_rule`: mean over (alert, UETR) pairs of on-target.
- `chance_by_rule`: the share of `random` control instances each rule's
  alerts touch; `random_control_floor`: the incidental recall of the
  `random` typology. Recall at or below these is indistinguishable from
  chance.
- Per reason code, for each designated rule that ran: `recall_by_code` (the
  share of the target typology's instances hit by that rule's alerts
  carrying the code), `fp_by_code` (as `fp_rate_by_rule`, over that code's
  alerts) and `alerts_by_code`, with `by_code_status` and
  `reason_code_vocabulary` (a hash of the codes and the cut points they
  read, so a reader knows which codes, meaning what, the figures are in).
- `nonplanted_alerts_by_rule`: per rule with a target typology, the alerts
  touching no payment of that typology, counted on `gold.alerts` before TM,
  so no TM cap truncates it. `customer_count`: the customers in
  `silver.entities`, the denominator for a per-customer non-planted alert
  rate; null when unreadable. Both are diagnostics.
- `evidence_capped_alerts_by_rule` and `recall_bounded_by_evidence_cap` (and
  each typology's `bounded_by_evidence_cap`): which rules had an alert cut
  by a per-alert evidence cap, and which typologies' recall that
  Lakebench-imposed cap bounds (7.4).
- Bookkeeping: `typology_counts` (typologies per status), `rules` (every
  rule's status, reason, target and alert count), `subject_customer_check`
  (planted subjects silver does not hold as customers, whose recall is
  understated), and per typology `workload_category`, `designated_rules`,
  `instance_count` and `subjects_not_customer`.

Recorded: in both published batch records 8 of 17 typologies were `scored`,
8 `no_rule` and 1 `rule_skipped` (W1, giant component), n=1 each.

**Continuous (covered mode).** After the drain (section 4.3, step 7) the
scorer runs over the snapshots the last completed tick read.
`financial_scoring.mode` is `covered`, and
`financial_scoring.covered.typologies[].recall_covered` is the
designated-hit rate over covered instances only: those whose every
participant payment was sealed in silver, and every participant entity was
in silver, at that tick. `covered_instances`, `corpus_instances` and
`coverage` say how much of the manifest that was, and `tick` carries
`pinned_after_window_end_s`. False positives and precision count every
planted payment in the full manifest, so an alert on a planted payment the
tick had not yet covered is not a false positive; the chance floor uses the
covered `random` instances. No key named `recall` is written. Typologies
whose rules are all excluded in continuous mode are listed in
`covered.excluded_typologies`. The per-code keys,
`nonplanted_alerts_by_rule` and `customer_count` are batch only. When the
drain, the tick record or the score job fails, the tick still had a pending
rule, or the run failed its gates, `financial_scoring.status` is
`not_scored` with a `reason`. The alert-set fingerprint of the scored tick
is recorded in `experiment.results.alert_set_continuous` as a diagnostic,
never a result.

<!-- PENDING screening-rates: scripts/aml_screen_rates.py writes
docs/benchmarks/data/aml_screening_rates.json from stored records (seed 43
and a calibration replicate, scale 1 and 10). When it lands, add here: the
W5 and W6 non-planted alerts per customer at each scale, the ratio s10/s1,
each with its run id and labelled n=1, cited from that file, never
recomputed; and the matching limitation in section 12. -->

### 8.5 TM operations

`tm_operations.status` is `pass`, `fail`, `not_run`, `disabled` or
`unknown`; only `fail` (a violated workflow invariant) fails the run. In
continuous mode the operations pass runs every
`continuous_interval_seconds` (default 1,800 s), so a window shorter than
that may record `not_run`. `tm_operations.ops` reports the alert funnel
(payments, monitored, alerts, escalated, cases, SARs), dispositions,
priorities, alert aging and SLA breaches, case status, filing-days median
and p95, late filings and continuing reviews. Every one depends on
simulated parameters (`spark/scripts/tm_operations.py`, recorded in
`tm_operations.ops.simulation`): analyst accuracy 0.90, investigator
accuracy 0.95, QA sample 0.05, late filing 0.03, no-suspect 0.05, simulation
seed 20260924, SLA 60 days, lookback 12 months, the counterparty scenario
list, and the Lakebench-imposed per-customer alert cap
(`max_alerts_per_customer`, 50,000; alerts past it are dispositioned
`over_capacity` and the cap is listed in `experiment.limits.bound`). They
measure the simulation, not the architecture.

## 9. Disclosure requirements

`metrics.json` carries an `experiment` block: schema `exp2` (identity
version 2) when corpus id v2, the identity version and an observed system
identity exist, otherwise `exp1`, with `v2_unavailable` naming what was
missing (`metrics/experiment.py`). A published AML result must show:

| Field | Source in metrics.json |
|---|---|
| Workload `financial` and version `aml-2`, `parameters_id` and parameters | `experiment.workload` (`parameters` holds the TM operations settings, `w1_max_vertices`, `retention_workload`, `retention_months`) |
| Generator model version | `experiment.workload.generator_model_version` |
| Configured image, observed pod image and digest, seed, corpus role, perturbation, corpus id, corpus id v2 or the reason there is none, and whether the corpus was observed or only declared | `experiment.corpus`, `experiment.corpus.datagen`, `experiment.corpus.id_v2` (or `id_v2_unavailable`), `experiment.corpus.observed` |
| Recipe, catalog, format, engine and query-engine versions, query access path | `experiment.architecture`, `experiment.architecture.access_paths` |
| Support state and basis | `experiment.support` |
| System | `experiment.system` (`cluster` or `local`) and `experiment.system_identity` (the observed cluster and object-store fingerprint) |
| Scale, mode | `experiment.corpus.scale`, `experiment.mode` |
| Maintenance policy id, settings and effective outcome per operation | `maintenance_policy_id`, `experiment.maintenance_settings`, `experiment.effective_maintenance` |
| Stages executed and skipped; rules executed, skipped (with reason) and errored | `experiment.stages`, `experiment.rules`. Continuous: the five mode-excluded rules are not listed in `experiment.rules.skipped`; disclose them from `experiment.support.mode_note` |
| Caps configured and caps that bound | `experiment.limits` (`bound`, `bound_kinds`; continuous `trickle_bound`) |
| Repetitions | `experiment.repetitions` (each record is one run, `runs: 1`; `lakebench run` with `--repeat` records a series whose members are listed in its manifest; label a figure from one run n=1) |
| Query set id and per-query fingerprints, or `not_checked` with the reason | `experiment.results.query_set_id`, `.fingerprints`, `.not_checked` |
| AML scoring and its mode | `financial_scoring` (`mode`, `status`, `reason`) |
| Code provenance | `provenance` and `experiment.lakebench` (`lakebench_version`, `git_sha`, `git_dirty`, `install`, `tree_sha256`, the dependency set and observed image digests) |

For continuous runs also disclose the window, the resolved trickle
(`continuous.trickle`), `limits.trickle_bound`, `intake_limit` and
`corpus_ingest_ratio`: a window that did not drain the corpus measures a
subset of it, and the arrival rate is Lakebench-set. Disclose every skipped
rule and any skipped-rule warning the batch gate printed (not enforced).

## 10. Comparability

Lakebench does not compare runs for you; a reader compares two reports (see
[Comparing Runs](../benchmarking.md#comparing-runs)). For AML, two runs are
comparable only when all of these hold (`metrics/comparability.py`):

- both passed, and both carry an experiment block of the same identity
  version;
- every Workload and Corpus identity key (7.3) is equal, and neither run has
  a corpus problem (config and datagen pods disagree);
- they ran the same query set and every result fingerprint matches, and for
  batch the alert set matches rule by rule (a different alert set is a
  different result).

Results are not checked for AML continuous runs
(`experiment.results.not_checked`) or for a `*-none` recipe, so such runs
cannot show they returned the same answers. Execution conditions (7.2), or
only the dependency pinset, differing make the pair comparable but not
like-for-like: a difference in the numbers may come from them. When the
architecture and the system both differ, no difference can be put down to
either.

Batch and continuous AML runs are never comparable (mode is a Workload key).
A batch run whose TM layer did not run (8 queries) is not comparable with
one where it ran (12), because the query sets differ. A different
maintenance policy id is refused by `reproduce`. Records
written by Lakebench 1.6 (exp1, workload `aml-1`), including the three
cited AML runs, are not comparable with this release's records: they
are reference figures, not baselines.

## 11. Supported compositions

AML runs on Iceberg only. Of the 11 structurally valid tuples, 8 are
workload-compatible, in both batch and continuous mode
(`config/schema.py`, `WORKLOAD_TABLE_FORMATS` and `WORKLOAD_MODES`). The
release validation record (`config/validated_combinations.yaml`) lists
nothing yet, so every valid AML composition is stamped **unverified**; none
is **supported**. A supported cell names the Spark minor and table format
version its validation runs used; any other version, and any tuple outside
the release matrix, stays unverified.

| Recipe | Query access path | State (batch / continuous) | Spark image | Notes |
|---|---|---|---|---|
| hive-iceberg-spark-trino (`default`) | catalog | unverified / unverified | 4.1.1 | recorded: continuous scale 1 PASSED (`run-20260929-205000-ebb26f`) and batch scale 10 PASSED (`run-20260929-214442-825153`), both n=1 on Spark 4.0.2 |
| hive-iceberg-spark-thrift | catalog | unverified / unverified | 4.1.1 | query and pipeline share Spark |
| hive-iceberg-spark-duckdb | direct storage | unverified / unverified | 4.1.1 | DuckDB runs no Iceberg maintenance |
| hive-iceberg-spark-none | none (no query engine) | unverified / unverified | 4.1.1 | no query benchmark, no QpH, no result fingerprints |
| polaris-iceberg-spark-trino | catalog | unverified / unverified | 4.0.2 | recorded: batch scale 1 PASSED (`run-20260929-221146-9d5345`, n=1, Spark 4.0.2) |
| polaris-iceberg-spark-thrift | catalog | unverified / unverified | 4.0.2 | |
| polaris-iceberg-spark-duckdb | direct storage | unverified / unverified | 4.0.2 | as DuckDB above |
| polaris-iceberg-spark-none | none (no query engine) | unverified / unverified | 4.0.2 | as none above |
| hive-delta-spark-{trino,thrift,none} | -- | unsupported | -- | refused at load: the AML stage scripts and DDL are Iceberg-only |

The pipeline engine is Spark in every recipe. Each recipe names its Spark
image (`config/recipes.py`): `apache/spark:4.1.1-python3` on the Hive
recipes and `apache/spark:4.0.2-python3` on the Polaris recipes; a config
that sets `images.spark` runs that image. The release matrix's AML rows are
batch hive-iceberg-spark-trino at scale 1 and 10, batch
polaris-iceberg-spark-trino, continuous hive-iceberg-spark-trino and
continuous polaris-iceberg-spark-trino (`config/support.py`).
Continuous mode runs the reduced rule set W2, W3, W4 and W17 on every
recipe. `--local` is refused for AML: local mode runs Customer 360 batch
only.

## 12. Known limitations

- **Proven on one system type.** Every published AML record ran on
  OpenShift with Portworx scratch and FlashBlade S3; the minimum cluster for
  a config is what `lakebench plan` reports.
- **Supported tuples outside the release matrix are unverified.** No AML
  composition is supported in this release. Spark 3.5 is accepted but is in
  no release-matrix row, so its runs are unverified. AML is unverified above
  scale 300 and refused above 800.
- **Detection covers 8 of 17 planted types in batch.** Eight typologies
  have no designated rule, and W1 was skipped as `giant-component` at scale
  1 and 10 in the published records (at scale 100 its skip reason is
  `vertex-cap`, a Lakebench cap). With W1 skipped, the pre-registered
  behavioural typology `gather_scatter` has no detector, and every such run
  prints a warning saying so. Recall figures describe this rule set, not
  the architecture.
- **AML entity names come from fixed pools.** Party and company names are
  drawn from fixed per-country pools, so name-based screening (W5, W6) sees
  a bounded vocabulary while the watchlist grows with the population.
  <!-- PENDING screening-rates: add the measured growth of W5 and W6 non-planted alerts
  per customer from docs/benchmarks/data/aml_screening_rates.json (s10/s1 on
  seed 43 and on a calibration seed, n=1 each). -->
- **Continuous runs a different workload slice.** Continuous AML runs W2,
  W3, W4 and W17 every tick; W1 and W5 to W8 are not run in this mode, and
  their typologies read `mode-excluded`. Each tick re-runs detection over
  the whole pinned silver corpus, so tick cost grows with the corpus.
  Continuous results are not comparable with batch.
- **Continuous AML recall is `recall_covered`** over what the last drained
  tick had read, not the batch `recall`. No published record carries one
  yet.
- **Continuous results are never checked.** AML continuous runs record no
  result fingerprints, so any comparison that includes one is NOT
  ESTABLISHED at best. In the published continuous record
  (`run-20260929-205000-ebb26f`, scale 1, n=1) bronze took 43.7% of the
  corpus in the default 1,800 s window.
- **Continuous throughput is an offered load bounded by the trickle.** At
  scale 1 the derived trickle is one 64 MB file per trigger, so
  `sustained_throughput_rps` and `corpus_ingest_ratio` measure a
  Lakebench-set arrival rate, not what the architecture can take in. A
  continuous record names the trickle in `limits.bound` and
  `limits.trickle_bound`; label continuous throughput with the resolved
  trickle whenever it is published. The published continuous record
  predates that label and shows `bound: []`.
- **Lakebench names no winner.** Comparing two runs is left to the reader
  (section 10).
- **No expected results.** Lakebench ships no expected AML query results for
  a user run. Correctness across architectures rests on fingerprint
  equality between batch runs; an answer wrong in the same way on both sides
  is not detected.
- **Row relations between layers are not enforced.** No code compares
  bronze rows with generated rows, `silver.transactions` with bronze, or any
  relation of section 2.3. The record gates check that every layer has rows
  and that batch bronze holds at least 95% of the scale's expected volume;
  a PASSED verdict asserts no relation. Check them from `metrics.json`
  before publishing.
- **Repeatability.** Each run record is one run (`runs: 1`). `run --repeat`
  (up to 20 runs) runs a series on one corpus; Lakebench does not summarise
  the series, so read its members' records for the spread.
  Every published AML record is n=1, and any investigator or time-travel
  figure from this release is one run per arm on one system (n=1).
- **The corpus is fixed in time and zone-less.** 2021-01-01 to 2026-01-01.
  The generator ignores the config's timestamp and dirty-data fields, but
  they are recorded and hashed into corpus id v1, `corpus.timestamp_start`
  and `timestamp_end` are the config's values rather than the corpus's
  range, and `timestamp_end` sets silver's `profile_updated_ts`, which
  otherwise is the run date.
- **Corpus id v1 hashes inert fields.** On exp1 records an edit to a field
  the AML generator never receives (the timestamps, `dirty_data_ratio`, the
  Customer 360 `unique_customers`) makes identical corpora not comparable.
  Corpus id v2 hashes what the generator applied, but only an image that
  writes corpus markers produces it. The default image does; records from an
  image without markers, `1.6.0` included, stay exp1 (section 3.1).
- **Records from Lakebench 1.6 are not comparable** with this release's
  records (workload `aml-1`, identity v1), so the published AML records are
  reference figures only.
- **Bronze tables are unpartitioned** (zero-copy registration), so bronze
  scans are full scans on every engine.
- **TM operations figures are simulations.** Dispositions come from
  simulated analysts and investigators with seeded accuracies (8.5). Recall
  and false-positive rate are measured from alerts against the manifest.
- **`lakebench financial reproduce` pins the three silver tables only.**
  W5 and W6 read the bronze watchlist as it is now and W1 its vertex cap
  from the current config (the result lists them as `not_pinned`), and the
  W3 and W17 path budgets depend on the job's executor count and scratch
  size, so a W2 or W4 alert is the clean check. Continuous alerts are not
  reproduced.
- **The reference-model score reads unsealed silver.**
  `lakebench financial reference-score` builds its features from silver
  tables read without the sealed-batch filter the detection rules and the
  covered scorer use (`spark/scripts/aml_features.py`), so silver rows from
  a batch whose seal marker never landed (a crash between the transaction
  commit and the marker) are counted there and nowhere else.
- **The look guard is per host.** The corpus ledger that records a
  registered corpus and the bronze prefix it was written to is a local file
  (section 3.3), so a development config on another host pointed at that
  prefix is refused only by bronze-verify's manifest check, after the
  deploy. Take every look from one host.
- **`--generator-image` is a string, not a check of the corpus.** A
  registered look or calibration shard run through `scripts/aml_gate.py`
  needs `--generator-image` as a digest-pinned reference
  (`...@sha256:<digest>`), and a registered look refuses one that is not
  the same string the per-typology predictions were made with: the default's
  `repo:tag@sha256:<digest>` spelling and `repo@sha256:<digest>` for the
  same image are refused against each other. `aml_gate.py` does not check
  that the scored corpus was written by that image; the corpus markers'
  `build_commit` is checked against `config/datagen_lineage.yaml` by hand.
- **A materialised MERGE source can fail a micro-batch.** An executor lost
  between materialising a silver-stream MERGE source and the MERGE fails
  that micro-batch, and the stream restarts through its replay path. No
  published record yet shows AML continuous on Spark 4.1; the published
  continuous record ran Spark 4.0.2.
