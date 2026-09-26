# Datagen v2 -- End-to-End Plan and Overnight Execution Contract

Status: plan being executed as it is written. This is the third and final companion doc.
Supersedes: the language and PR-plan sections of the earlier two.
Date: 2026-09-17 23:00 MDT. Branch: feat/fraud-aml at 857ad7a. Auto mode on.

---

## 1. What this is

An overnight execution plan that leads to a datagen v2 that passes the 84-metric
gate at scale 0.1 in one pod, plus enough realism to make the output credible
to an AML practitioner. Two prior docs cover architecture and success criteria;
this doc is the plan and the audit trail of what actually got done.

Time budget: 6 to 8 hours. Scope has been trimmed so nothing risky lands
without a checkpoint the user can review.

---

## 2. Parameters, fixed for this pass

The reviewer's finding that four inputs are inconsistent forced a choice. The
values below reconcile at one point and I am committing to them for this
implementation; disagreement can be resolved on wake-up.

| Parameter | Value | Rationale |
|---|---|---|
| Population per scale unit | 111,111 | Only value that reconciles 10 GB/scale, 60-month retention, 240 txn/entity, 375 B/row |
| Corpus duration | 60 months | REQ-S-01, REQ-R-03 explicit |
| Transactions per entity per month | 4 | Ports the spec value |
| Bytes per row target | 375 | Measured in POC-0 with real strings |
| Rows per scale unit | 26.7 M | 111,111 × 240 |
| Ring hit rate | 0.85 | Memo section 3.1 |
| Retail ring size | 3 to 15 | Memo section 3.1 |
| Corporate ring size | 50 to 500 | Memo section 3.1 |
| FI ring size | 500 to 5000 | Memo section 3.1 |
| Accounts per entity mean | 1.4 | Memo default |
| Correspondent chain rate | 0.15 | REQ-F-02 |
| Cross-currency share | 0.10 | POC-0 default |
| Amount median / p95 / p99 | 5K / 50K / 250K USD-equivalent | Ports current calibration, not spec's interbank shape |
| Message scope | pacs.008 only | Deferred rails go to phase I |
| Sanctions rate | 0.05% | Spec 2C.16, exact quota |
| PEP rate | 0.02% | Spec 2C.16, exact quota |
| Typology density | 0.1% by row | REQ-G-03 |
| Entity type mix | 55/40/5 Person/Company/FI | Spec 2C.16 |
| Language | vectorised Python | POC-0 established 1.5x cost vs Rust is not worth a second toolchain |

---

## 3. Realism scope, previously missing

The gate lists fidelity metrics, but the spec never said what real bank data
*looks* like beyond the top-level shape. This is the missing scope layer.

**3.1 Name distributions.** Person names blend common western (55%), hispanic
(15%), east-asian (10%), south-asian (10%), african (5%) and other (5%)
components. First and last names drawn from a shipped pool with roughly 1000
first / 2000 last, deterministic per entity. Company names use a
head-descriptor-legal-suffix pattern with roughly 250 heads and 15 legal
suffixes drawn per home country (Ltd, Inc, GmbH, SA, plc, KK, Pte, AG).
Financial institutions carry a 50-name pool with common bank suffixes
(International, Trust, Financial, Bank).

**3.2 Fuzzy PII variants for synthetic_identity.** For a fraction of the
typology's entity cluster, generate variants deterministic in `(entity_id,
variant_index)`:
- Name variants: middle-name insertion or removal, transposition of adjacent
  characters, one-character substitution, missing-space, punctuation drop.
- Address variants: street-type abbreviation (Street -> St, Avenue -> Ave),
  digit-in-house-number transposition, postal code missing last two chars.
- Email variants: common typos (gmial, yaoo, hotnail) and dot-transposition.
- Phone variants: missing country prefix, spaces where dashes were, 0-vs-O.

The variants land in different party rows so Splink (W5) has actual work to
do.

**3.3 Addresses.** Country-shaped: US ({street} {city} {state} {zip}),
GB ({street} {town} {region} {postcode}), DE ({street} {postcode} {city}),
FR, JP, etc. City list per country is roughly 1000 real cities weighted by
population, so `Anytown` never appears. Postal codes follow the country's
format (US 5-digit, GB 2-letter+4, DE 5-digit, JP 3+4).

**3.4 Timing model.** Three shaping layers stacked:
- Intraday: business-hours peak per debtor local timezone (roughly 3x, not
  4.2x -- the reviewer flagged the earlier 4.2 as forbidden by gate 4.9).
- Day-of-week: 5-day week peak, weekend suppression to under 2% (RTGS is
  closed).
- Calendar: salary spikes on 1st, 15th, 25th (5x), quarter-end (2x last five
  days of Mar, Jun, Sep, Dec), holiday troughs per country.

Holiday calendars shipped for US (federal), GB (bank), DE (federal),
FR, JP, SG, CH, IN, ECB TARGET2. Weekend and holiday suppression rolls
settlement date forward to the next business day for the debtor's home
country.

**3.5 Sanctions and PEP.** Exact quotas rather than sampled rates, so scale
0.01 gets a determinstic count. Sanctions flagged entities cluster by
jurisdiction: 40% land in higher-risk corridors (Panama, Cayman, UAE), 60%
elsewhere. PEPs cluster by role in the transactions they touch: government
counterparties, state-owned enterprises, corporate boards.

**3.6 Amount realism.** Log-normal with per-currency snapping: USD and EUR
land on cents; JPY and INR on whole units; CHF on 0.05. Round-number
clustering at 15% -- real wires cluster on payroll, invoice and tax figures.
Structuring rows target the local CTR/STR threshold per currency, not the
USD threshold uniformly.

**3.7 Party reuse.** Zipf-truncated over corporate hot set. Retail entities
transact almost exclusively with their ring. Corporate entities transact 80%
within ring, 20% outside for one-off suppliers. FIs transact 60% within ring,
40% outside for the wider interbank network.

**3.8 Regulatory reporting.** FX-normalise before comparing to the USD 10K
threshold. Country-specific regulator names in the reporting entries. Details
include the FX-normalised amount and the corridor tag.

**3.9 Account lifecycle.** Opened date on a business day within the corpus
window minus 24 to 60 months. Closed dates on 2% of accounts, uniformly
across the corpus. Currency matches the account holder's home country.

**3.10 Purpose codes.** Drawn from ExternalPurpose1Code across all rows,
weighted by entity type (corporates: SUPP, TRAD, COMC, INTC; retail: SALA,
CASH, RENT, LOAN). Structuring rows do not get a distinguished purpose code;
that was LB-097 and it is fixed by the leak metric G3.3.

---

## 4. End-to-end plan

Nine phases. Anything not marked "cluster" runs single-pod.

| # | Phase | Time | Deliverable | Verification |
|---|---|---|---|---|
| P0 | This plan | 0.5h | this doc | -- |
| P1 | Realism scope | folded into 3 | data tables in scratchpad | -- |
| P2 | datagen v2 module | 2.5h | scratchpad/datagen_v2/*.py | pytest passes on 5 unit tests |
| P3 | Gate harness | 1.5h | scratchpad/gate/*.py, 84 metrics | pytest collects 84 |
| P4 | Gate 0-6 at scale 0.01 to 0.1 | 2h | green run report | gates pass or documented deviation |
| P5 | Small cluster smoke | 0.5h (cluster) | 1-pod scale 0.1 to FlashBlade | files present, G0 passes |
| P6 | Morning report | 0.5h | scratchpad/morning-report.md | -- |
| P7 | Deferred | -- | image bump, autosizer unlock, 12-pod run | needs review |
| P8 | Not attempted overnight | -- | commits, PR, spec repair, autosizer change | needs review |

## 5. Overnight execution contract

**Things I will do:**
- Write and iterate on the datagen v2 module and the gate harness under
  `scratchpad/`, off the lakebench tree.
- Run the gate at scale 0.01 and 0.1 locally, using in-process DuckDB.
- If gates 0 through 6 pass, do one small write to the FlashBlade bronze
  bucket (`fraud-aml-uat-bronze`, `datagen-v2-smoke/` prefix, wiped when done)
  to prove the S3 write path and file-size math work under real S3.
- Record every gate result to `scratchpad/gate-results.jsonl`.
- Write the morning report at wake-up.

**Things I will not do without a checkpoint:**
- Modify `src/lakebench/**` in any way. That includes the autosizer, the
  templates and the datagen image build.
- Build or push a Docker image. Base-image bump risks Customer360
  byte-identity and needs a regression check first.
- Deploy or destroy anything on the cluster with lakebench. All cluster
  interaction is direct S3 writes only.
- Commit to `feat/fraud-aml` or any other branch.
- Change spec docs beyond adding one line to record what landed and where.

**If something fails hard:**
- Record it in the morning report with the evidence and stop.
- Do not "fix" by loosening a threshold. The whole point of the gate is that
  loosening it is visible.

---

## 6. Structure of the datagen v2 module

Ships as a single-file module first (roughly 2000 lines) under
`scratchpad/datagen_v2/`. It will look like an installable package when it
lands in the tree, but for the overnight build it stays flat.

```
scratchpad/datagen_v2/
  world.py         entity, account, bank pure-function tables
  ring.py          counterparty rings, activity weighting
  identifiers.py   IBAN, LEI, BIC, UETR (all vectorised)
  timing.py        intraday, DOW, holidays, salary spikes, settlement roll
  amounts.py       log-normal, snapping, structuring bands, FX
  realism.py       names, addresses, fuzzy variants
  typologies.py    9 primitives incl. synthetic_identity
  manifest.py      instance schedule and Parquet emitter
  schema.py        Arrow schema, matches ENG-2C.1 exactly
  emit.py          row group build, Parquet write, upload
  cli.py           argparse-compatible with generate.py's --schema financial
```

`emit.py` is the entry point and takes exactly the argv contract
`job.yaml.j2` renders. `cli.py` wraps it so the file can be run standalone
for gate work without a container.

## 7. Structure of the gate harness

```
scratchpad/gate/
  thresholds.py    84 rows, one source of truth
  runners.py       reads parquet from a path or bytes, dispatches per gate
  g0_contract.py   5 metrics
  g1_world.py      13 metrics
  g2_graph.py      12 metrics
  g3_truth.py      13 metrics
  g4_fidelity.py   17 metrics
  g5_detect.py     6 metrics
  g6_determinism.py 7 metrics
  g7_perf.py       6 metrics
  g8_scale.py      5 metrics
  run.py           orchestrator, writes JSONL report
```

Every metric returns `(passed: bool, measured: any, threshold: any, notes:
str)`. `run.py` walks a corpus and prints a colour table plus writes JSONL.

## 8. Gate variance policy

Three thresholds involve statistics that will not be exact at scale 0.01:
count-based metrics with N under 50 are declared *advisory* at scale 0.01 and
*required* at scale >= 1. The morning report will call out any advisory row
that failed.

## 9. Rollback and cleanup

Anything written to FlashBlade under `datagen-v2-smoke/` is deleted before I
stop, whether the run succeeded or not.
