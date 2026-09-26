# Datagen v2 -- Architecture, Execution Plan, Success Measures

Status: proposal, awaiting decisions in section 7.
Date: 2026-09-17. Branch: feat/fraud-aml at 857ad7a.
Supersedes: ENG-2C.2 PR-C and the datagen half of `lakebench.next-spec-eng-2c3-memo.md`.
Companion: `scratchpad/aml-datagen-completeness-review.md` (the defect audit this plan answers).

---

## 1. Conclusion

Rewrite the financial generator as a Rust binary inside the existing single datagen
image, built on a new generative model, and gate it single-pod before it ever runs
at parallelism greater than one. The current generator is distributionally
plausible and semantically broken: five defects each independently reduce the
detection workloads W1 through W4 to empty or wrong results, and none of the five
is visible in a distribution metric.

Three things change at once, in this order, and the order is the point.

1. **The model.** Per-row independent sampling is replaced by a two-phase model: a
   deterministic static world (entities, accounts, banks, relationships) and a
   transaction stream that draws from it. This is what fixes the graph, the entity
   attributes, the population scaling, and the ground-truth leak. It is not a
   refactor of the existing code; the existing code has no world.
2. **The gate.** Correctness becomes 47 numbered metrics with numeric thresholds
   across seven gates, of which six run single-pod with no cluster, in CI. Nothing
   proceeds to perf work until gates 1 through 6 are green.
3. **The language.** Rust, because 500 MB/s per pod is 30 to 80 times the current
   per-core row rate and Python cannot close that gap. Written once, in Rust, not
   prototyped in Python first.

Estimated 4,200 LOC across nine phases. Phases A through F need no cluster.

---

## 2. Why the sequencing is what it is

The scale sweep we ran on 2026-09-16 and 17 proved the parallelism mechanics
(scale invariance within 0.2%, no file-id collisions, linear wall-clock to scale
10) on a workload that does not detect. That work is not wasted, but none of its
numbers should ship, because the corrected model changes row count per scale point
by roughly 2.2x and bytes per row by roughly 2.5x. Re-measuring after the model
lands is cheaper than measuring twice.

The structural expression of "right in one pod first" is that gates 1 through 6
are single-pod, in-process, and CI-runnable. Gate 7 is single-pod on one node.
Only gate 8 needs the cluster. A model defect cannot reach the cluster.

---

## 3. Architecture

### 3.1 Generative model: static world plus transaction stream

**Phase A, the world.** Entities, accounts, banks and counterparty relationships
are pure functions of `(seed, id)`. Nothing is materialised as shared state and
nothing is shipped between pods. `entity(seed, id) -> Entity` is computed on
demand by whichever pod needs it, from `xxh3(seed, id, field_salt)`. The same
holds for `account(seed, id)` and `counterparties(seed, id)`.

This single property delivers four things the current generator cannot:

- Entity attributes exist at all (fixes P1-1, P1-2), because they are derivable
  rather than needing a table.
- Attributes are stable across every file and every pod, with no coordination.
- The party zone Parquet is written by partitioning the entity id space
  (`id % total_nodes == node_id`), which is embarrassingly parallel.
- Population becomes a first-class input, so `financial_dimensions(scale)` drives
  it (fixes P0-2).

**Phase B, the stream.** Transactions are generated per file window as today. Each
transaction picks an originator from the population (Zipf-shaped for corporates,
uniform retail), then picks the beneficiary from that originator's counterparty
ring 85% of the time and from the population at large 15% of the time.

**The counterparty ring is the fix for P0-1 and the thing that makes the graph
real.** Each entity has a deterministic ring of size `k`, where `k` is drawn from
the entity's own type (retail 3 to 15, corporate 50 to 500, FI 500 to 5000):

```
ring(e) = [ xxh3(seed, e, "cp", j) % population  for j in 0..k(e) ]
```

Consequences worth stating explicitly. Every entity both sends and receives, so
the graph is not bipartite and cycles can close. Repeat edges dominate volume,
matching the 70 to 90% of real bank volume that runs on existing relationships.
In-degree is the number of rings containing you, which gives a realistic
long-tailed distribution for free. And all of it costs one hash per lookup with no
state.

**Entity naming.** The name is derived from `entity_id` alone, never from the
transaction role. This is the one-line change that fixes P0-1, and the world model
is what makes it possible to do properly rather than by stripping a prefix.

### 3.2 Typology set: nine primitives, not eight

The eight AMLworld primitives stay. A ninth is added, and it closes a gap the
audit surfaced late: **W1 detects shared personally-identifying attributes via
connected components, and no current typology produces shared attributes.** The
manifest maps `bipartite` to `W1_synthetic_id`, but bipartite is a
transaction-graph shape, not a PII-sharing shape. W1 has no ground truth today.

`synthetic_identity` emits a cluster of entities that deliberately share PII, in
two flavours within each instance:

- Exact-shared attributes (same address, same email, same phone) across a subset,
  which is what W1 connected components finds.
- Fuzzy variants (transpositions, abbreviations, missing middle names, street
  abbreviations) across another subset, which is what W5 Splink finds.

Without the fuzzy half, W5 has nothing to resolve. Without the exact half, W1 has
no edges. REQ-G-04 says "at least the eight" and requires extensibility, so a
ninth is in-spec.

Typologies become data-driven rather than a compiled registry: each is a shape
description in a TOML file the binary reads, so adding one does not require
touching generator code. That satisfies the plugin requirement in ENG-2C.2 better
than the current Python function registry does.

### 3.3 Parallel decomposition and the determinism contract

Unchanged from today, because it works: `file_id -> time window`, pod owns
`file_id % total_nodes == node_id`, typology instance owned by the single file
containing its `injection_ts_start`.

The contract that must hold, and that gate 6 tests:

| Property | Statement |
|---|---|
| Seed determinism | File content depends only on `(seed, file_id)`. |
| Parallelism invariance | File content does not depend on `total_nodes` or `node_id`. |
| Thread invariance | File content does not depend on rayon thread count or scheduling. |

The third is new and is the one Rust puts at risk. Intra-file parallelism is
allowed only across row groups, each seeded from `(seed, file_id, row_group)`.
Nothing may read a shared mutable RNG.

### 3.4 Rust workspace layout

```
datagen/
  Cargo.toml                     workspace
  crates/
    lb-world/      ~600 LOC      entity, account, bank, counterparty ring,
                                 hash derivation, party-zone schema and emit
    lb-typology/   ~500 LOC      9 primitives, TOML shapes, scheduler, manifest
    lb-pacs008/    ~800 LOC      Arrow schema, columnar builders, row generation,
                                 FX, identifiers, timing model
    lb-camt053/                  phase I, deferred
    lb-emit/       ~400 LOC      Parquet writer, S3 multipart upload, retry
    lb-datagen/    ~300 LOC      binary: argv contract, file windowing,
                                 rayon orchestration, DATAGEN_FILE_METRICS
```

`lb-world` is the dependency root and the most heavily gated crate. `lb-emit` is
schema-agnostic and is where the throughput work concentrates. Typology is kept
separate from pacs008 so camt.053 reuses it unchanged in phase I.

Crates: `arrow` and `parquet` (arrow-rs), `object_store` for concurrent S3
multipart, `rayon`, `xxhash-rust` (xxh3) pinned by version. The hash choice is
never exposed to the gate, which reads emitted Parquet and checks internal
consistency rather than recomputing hashes, so the pin is an implementation
detail.

**Columnar generation, not row dicts.** The current path builds a dict per row and
transposes it, which the profile attributes 22% of wall-clock to on its own, with
most of the remaining 76% also being per-row Python object churn. Rust appends
directly into per-column Arrow builders, with nested structs through
`StructBuilder` and all string formatting written into reused buffers. This is
simultaneously the largest perf lever and the cleaner design.

### 3.5 One image, Python entrypoint, exec dispatch

Per your call: one image, one tag, one push. Multi-stage Dockerfile with a cargo
build stage; the final `python:3.13-slim` stage copies the binary in beside
`generate.py`. `generate.py` stays the entrypoint, parses argv as it does now,
and on `--schema financial` replaces itself:

```python
os.execv(FINANCIAL_BIN, [FINANCIAL_BIN, *sys.argv[1:]])
```

Process replacement means no Python is resident while the generator runs, so the
hot path is pure Rust, and there is exactly one dispatch point and one argv
contract -- the same one `templates/datagen/job.yaml.j2` already renders.
Customer360 continues down the existing Python path untouched, and its
byte-identical guarantee is unaffected because all its randomness goes through
numpy's PCG64 and blake2b, with no stdlib `random` and no `hash()` on strings
anywhere in the datagen tree.

Python 3.11 to 3.13 on the base image. All four runtime dependencies (pyarrow,
numpy, boto3, tqdm) publish 3.13 wheels. Not 3.14: numpy and boto3 support there
is newer and free-threading is still not default.

### 3.6 Performance architecture and the 500 MB/s derivation

The target needs stating precisely, because it is compression-dependent and the
dependency runs counter-intuitively. 500 MB/s of **compressed Parquet bytes landed
in S3** is the meaningful reading, since that is what the storage sees and what the
bronze bucket grows by. Fewer bytes per row therefore makes the target *harder*,
not easier, because more rows are needed per megabyte.

| bytes/row (snappy) | rows/s per pod for 500 MB/s | per core @16 | per core @32 |
|---|---|---|---|
| 202 (today, degenerate strings) | 2.48M | 155K | 78K |
| 350 | 1.43M | 89K | 45K |
| 500 (projected, full schema) | 1.00M | 63K | 31K |
| 650 | 0.77M | 48K | 24K |

Today's 202 bytes/row is measured against degenerate strings: `D-42`,
`IBAN0000000000000042`, `{n} Main St`, town equal to a two-letter country code.
Real names, structured addresses, conformant IBANs, remittance information and a
500-BIC pool will push bytes/row substantially up, plausibly to 400 to 650. **The
richer the data gets, the easier the MB/s target becomes.** So the row-rate target
is a derived quantity and phase C measures the input to it. The plan commits to
500 MB/s per pod and treats rows/s as the dependent variable.

Plausible Rust rates for a 41-column nested schema, to be confirmed by
`cargo bench` in phase F: 200 to 500K rows/s per core generate-only, 150 to 300K
rows/s per core generate plus Parquet encode. Against the table that makes
500 MB/s per pod comfortable at 16 cores once the schema is complete, and tight
but plausible even at today's 202 bytes/row.

Three consequences for pod sizing and prerequisites, all of which want checking
before phase G:

- **Cores.** 16 to 32 cores per pod, against today's `datagen.cpu: "2"`. Worker
  nodes allocate 39.5 cores, so a 32-core pod is one pod per node and 12 usable
  workers gives roughly 6 GB/s aggregate.
- **Network.** 500 MB/s per pod is 4 Gbps sustained. Node NICs need to be 25GbE
  or better for 32-core pods. Add a prerequisite check.
- **Storage ceiling.** 6 GB/s aggregate writes needs confirming against the
  FlashBlade before we design a run around it.

Per-pod memory should *fall*, not rise: columnar builders with bounded row groups
and streaming multipart upload need well under the current 4 Gi.

### 3.7 What silver must change

The generator cannot fix the workloads alone. `silver_build_financial.py` currently
hardcodes three of the audit's findings and must change in step:

| Line | Today | Must become |
|---|---|---|
| :210-211 | `sanctions_status='clear'`, `pep_status=false` | read from the party zone |
| :196-212 | entity_type, address, email, phone, lei, bic all NULL | read from the party zone |
| :161 | `txn_amount_usd = amt * xchg_rate` | normalise from the shipped FX table |
| :162 | `txn_type = 'wire'` | from the message type, once camt.053 lands |
| :143 | `entity_id = xxhash64(name|country)` | keep, but it now resolves 1:1 |

Also `array_remove(x, NULL)` at :165-172 is a known Spark failure and is fixed in
the same pass. And `financial_dimensions()` in `config/scale.py:196` needs
recalibrating once phase C publishes measured bytes/row, per REQ-S-03.

The corrected model reconciles that function with the spec for the first time. Population `scale * 500K` at 48 transactions per account per year
gives `scale * 24M` rows, which at a projected 500 bytes/row is `scale * 12 GB`
against the promised `scale * 10 GB`. Today we only reach 10 GB per scale unit by
inflating transactions per account 20-fold. Row count at scale 10 falls from 523M
to 240M; total bytes stays in the same band.

---

## 4. Success measures

47 metrics, eight gates. Every threshold is numeric and every row names where the
number comes from. Gates 1 through 6 are `pytest tests/test_financial_datagen_gate.py`
reading emitted Parquet through DuckDB, no cluster, in CI. Gate 7 is `cargo bench`
plus one single-pod run. Gate 8 is the only cluster gate.

A gate is green only when every row passes. No partial credit, no "mostly".

### Gate 1 -- World model (party zone)

| # | Metric | Threshold | Source |
|---|---|---|---|
| 1.1 | Entity count vs `financial_dimensions(scale).customers` | within 1% | party zone row count |
| 1.2 | Entity type mix Person / Company / FI | 55 / 40 / 5, each within 2pp | party zone |
| 1.3 | Attribute completeness: name, legal_name, address, email, phone, country | 100% non-null | party zone |
| 1.4 | LEI present on Company and FI only | 100% | party zone |
| 1.5 | LEI ISO 17442 mod-97 check digit valid | 100% | gate validator |
| 1.6 | IBAN mod-97 check digit valid | 100% | gate validator |
| 1.7 | IBAN country format matches account country | 100% | gate validator |
| 1.8 | BIC format valid, 8 or 11 char | 100% | gate validator |
| 1.9 | Distinct BIC pool size | >= 500 | party zone |
| 1.10 | Sanctions flagged share | 0.05% within 0.01pp | party zone |
| 1.11 | PEP flagged share | 0.02% within 0.01pp | party zone |
| 1.12 | Accounts per entity, mean | 1.4 within 0.3 | account zone |
| 1.13 | Entity attribute stability across files | 100% single-valued per entity_id | join across all files |

### Gate 2 -- Transaction and graph semantics

| # | Metric | Threshold | Source |
|---|---|---|---|
| 2.1 | Entities appearing as both originator and beneficiary | >= 85% | silver edges, fixes P0-1 |
| 2.2 | Datagen entity to silver entity_id mapping | 1:1, 100% | join party zone to silver |
| 2.3 | `cycle` instances forming a closed path in edges | 100% | manifest join |
| 2.4 | `stack` instances forming a connected path | 100% | manifest join |
| 2.5 | `gather_scatter` / `scatter_gather` hub reachable both sides | 100% | manifest join |
| 2.6 | Transactions per entity per year | 48 within 10 | row count / population |
| 2.7 | Repeat-relationship volume share (edges with txn_count > 1) | 0.70 to 0.90 | silver edges |
| 2.8 | Out-degree p50 | 3 to 15 | silver edges |
| 2.9 | Out-degree p99 | > 100 | silver edges |
| 2.10 | Single-entity share of all edges (supernode guard) | < 2% | silver edges |
| 2.11 | UETR uniqueness across corpus | 100% | count vs count distinct |

### Gate 3 -- Ground truth integrity

| # | Metric | Threshold | Source |
|---|---|---|---|
| 3.1 | Typology density | 0.1% within 0.02pp | manifest rows / total rows |
| 3.2 | Density stability across scale 0.01, 0.1, 1 | within 0.02pp of each other | three runs |
| 3.3 | Max categorical enrichment: for any column value, `P(manifest\|value)/P(manifest)` | < 10x | leak test, fixes P0-4 |
| 3.4 | Amount KS statistic, typology vs baseline, same currency | < 0.5 | leak test |
| 3.5 | Manifest path outside every workload read path | true | path assertion, REQ-G-02 |
| 3.6 | Bronze row count equals sum of per-file rows | exact | manifest-in-read-path check |
| 3.7 | Manifest carries transaction_ids and injection_parameters | present | schema assertion, REQ-G-01 |
| 3.8 | All 9 typology types present in manifest | 100% | manifest |
| 3.9 | `synthetic_identity` instances sharing exact PII | >= 1 shared attribute each | manifest join to party zone |
| 3.10 | `synthetic_identity` instances carrying fuzzy PII variants | >= 1 variant pair each | manifest join |

### Gate 4 -- ISO 20022 and practitioner fidelity

| # | Metric | Threshold | Source |
|---|---|---|---|
| 4.1 | `txn_amount_usd` vs FX-table normalisation | within 0.5% on 100% of rows | fixes P0-5 |
| 4.2 | Cross-currency share (instd_ccy != sttlm_ccy) | 0.05 to 0.15 | bronze |
| 4.3 | `xchg_rate` not 1.0 on cross-currency rows | 100% | bronze |
| 4.4 | Multi-hop correspondent chain share | 0.15 within 0.03 | fixes P1-8, REQ-F-02 |
| 4.5 | `prvs_instg_agt_1` populated share | >= 5% | REQ-F-02 |
| 4.6 | Bank per account stable across transactions | 100% | fixes P1-4 |
| 4.7 | `instg_agt` differs from `dbtr_agt` on multi-hop rows | >= 50% | bronze |
| 4.8 | Salary-day volume ratio (1st, 15th, 25th vs baseline) | 5.0 within 1.0 | REQ-F-05 |
| 4.9 | Business-hours ratio in debtor local timezone | 3.0 within 0.5 | REQ-F-05 |
| 4.10 | Weekend wire share | < 2% | REQ-F-05 |
| 4.11 | Settlement date on a business day for the corridor | 100% | holiday calendar |
| 4.12 | Holiday trough present on each shipped calendar | >= 50% drop | REQ-F-05 |
| 4.13 | Purpose code distribution across ExternalPurpose1Code | >= 12 distinct, top < 40% | fixes P0-4, spec 2C.16 |
| 4.14 | Columns with top-value share > 0.95 | zero outside documented allowlist | fixes P1-9 |
| 4.15 | `nb_of_txs` distribution | > 1 on >= 20% of messages | fixes P1-9 |
| 4.16 | Distinct town names per country | >= 100 | spec 2C.16 |
| 4.17 | Realism disclaimer printed at start | present | spec 2C.16 |

### Gate 5 -- Detection efficacy

Reference detectors implemented in the gate over DuckDB, not the Spark workloads.
Recall is per typology instance per D-04: detected when at least one alert cites
any participant transaction or entity.

| # | Metric | Threshold | Source |
|---|---|---|---|
| 5.1 | W2 structuring reference detector recall | >= 0.90 | gate detector |
| 5.2 | W3 round-tripping reference detector recall | >= 0.90 | gate detector |
| 5.3 | W1 synthetic-identity reference detector recall | >= 0.90 | gate detector |
| 5.4 | Reference detector lift over base rate | > 50x each | gate detector |
| 5.5 | W7 two-hop to sanctioned or PEP returns rows at scale 1 | >= 1 row | fixes P1-1 |
| 5.6 | Precision per detector | reported, no threshold | gate detector |

Gate 5b, on-cluster, deferred to phase G: the same three recalls re-measured
through the Spark workloads against silver, agreeing with 5.1 to 5.3 within 0.05.

### Gate 6 -- Determinism and parallel invariance

| # | Metric | Threshold | Source |
|---|---|---|---|
| 6.1 | Two runs, same seed, per-file sha256 | identical, 100% | REQ-R-04 |
| 6.2 | parallelism 1 vs 4 vs 16, per-file sha256 for common file_ids | identical, 100% | section 3.3 |
| 6.3 | rayon threads 1 vs 8, per-file sha256 | identical, 100% | section 3.3 |
| 6.4 | file_id coverage across pods | no gap, no duplicate | S3 inventory |

### Gate 7 -- Single-pod performance

| # | Metric | Threshold | Source |
|---|---|---|---|
| 7.1 | Generate-only rows/s per core, single thread | >= 150K | cargo bench |
| 7.2 | Generate plus encode rows/s per core | >= 100K | cargo bench |
| 7.3 | Compressed MB/s landed in S3, one pod | >= 500 | single-pod run |
| 7.4 | MB/s per core | >= 15 | derived |
| 7.5 | Per-file bytes vs `file_size` target | within 5% | S3 inventory |
| 7.6 | Peak RSS per pod | < 8 GB | pod metrics |
| 7.7 | Measured bytes/row, published | recorded | REQ-S-03 |

### Gate 8 -- Multi-pod scale-out

| # | Metric | Threshold | Source |
|---|---|---|---|
| 8.1 | Aggregate MB/s at 12 pods vs 12x single-pod | within 10% | cluster run |
| 8.2 | All gate 2, 3, 4 metrics at parallelism 12 | unchanged from single-pod | cluster run |
| 8.3 | Storage footprint linearity, scale 1 to 10 to 100 | within 10% of linear | REQ-S-02 |

---

## 5. Execution plan

Nine phases. Phases A through F need no cluster. Dependencies are strict.

| Phase | Work | LOC | Needs cluster | Gates closed |
|---|---|---|---|---|
| A | Design pin-down. Distribution validation script (throwaway Python, ~200 LOC). Decide section 7. Write the world-model parameter spec: exact hash derivations, exact distributions, exact constants. | 200 | no | none, sign-off gate |
| B | Rust workspace, `lb-world`. Entity, account, bank, counterparty ring. Party zone Parquet emission. | 600 | no | Gate 1 |
| C | `lb-pacs008` and `lb-typology`. Corrected transactions, 9 typologies incl. `synthetic_identity`, FX, identifiers, timing, purpose codes. Measure bytes/row. | 1300 | no | Gates 2, 3, 4 |
| D | `lb-emit` and the binary. Parquet writer, S3 multipart, argv contract, file windowing, metrics emission. First end-to-end single-pod run. | 700 | no | Gate 6 |
| E | Reference detectors in the gate (DuckDB), recall and lift measurement. | 300 | no | Gate 5 |
| F | Performance. Profile, eliminate allocation, rayon tuning, row-group pipelining. | 300 | no | Gate 7 |
| G | Image and integration. Multi-stage Dockerfile, Python 3.13, execv dispatch, per-schema pod sizing, prerequisite checks for NIC and storage ceiling. Silver updates per 3.7. Cluster run. | 500 | yes | Gates 5b, 8 |
| H | `financial_dimensions()` recalibration from measured bytes/row. Spec amendments for the section 7 decisions. Delete the Python financial path. | 100 | no | REQ-S-03 |
| I | camt.053 multi-rail: cash deposits, ACH, card as statement entries. Deferred until gates 1 to 8 are green. | 900 | yes | new gates |

Gate tests are written alongside the phase that closes them, not after. The gate
test module is Python in the lakebench repo, reading Parquet, so it is indifferent
to which language wrote the files.

**Why Rust-first rather than a Python prototype.** The gate reads Parquet and is
language-agnostic, and the statistical tuning loop (generate a million rows, check
distributions) is faster in Rust than in Python. The only real cost of Rust-first
is iteration speed on model *design*, which phase A absorbs on paper and in a
200-line throwaway script that validates distributions only, with no schema and no
Parquet. Writing 2,500 lines twice and then reconciling two models is the worse
trade.

---

## 6. What we delete, what stays

Delete once phase G is green: `datagen/financial.py`, `datagen/typologies.py`,
`datagen/realism.py`, `datagen/manifest.py`, the financial branch of
`write_file_to_s3`, and `datagen/verify_run.py` (the gate replaces it, and
verify_run is CPU-bound to the point of unusability at scale 10 anyway).

Stays untouched: `datagen/generate.py` Customer360 path and its byte-identical
guarantee, `templates/datagen/job.yaml.j2` argv contract, the K8s Indexed Job
decomposition, `DATAGEN_FILE_METRICS` as the observability channel, and the
`file_id % total_nodes` partitioning.

Keeps its value from the old work: the amount, corridor and chain calibrations in
`realism.py` are ported as parameters, not rewritten. The nine bugs fixed across
the three prior review cycles are all either preserved in the new model or made
moot by it.

---

## 7. Decisions needed before phase B starts

Four. The first three amend the spec; the fourth sizes phase C.

1. **Bank model and currency mix.** Code is 85% US home country, giving 91% USD.
   Spec 2C.16 says USD 40 / EUR 30 / GBP 10 / JPY 5 / CHF 5 / other 10.
   Recommend: keep the single-bank model with home market configurable, and amend
   the spec. The spec's mix describes the SWIFT network, not one bank's estate,
   and REQ-S-01 positions this as one tier-1 universal bank.
2. **Amount tail.** Code gives p50 5K, p95 50K, p99 127K. Spec says p95 500K,
   p99 5M. Recommend: keep the code's calibration and amend the spec. A 100x
   median-to-p95 ratio is interbank-shaped, not customer-wire-shaped.
3. **Correspondent chain target.** REQ-F-02 says 15% multi-hop. Confirm 15%
   against an 18% cross-border mix, which implies a ~55% chain rate on
   cross-border wires and is high. Alternative: 10%.
4. **Cash and ACH in phase C or phase I.** Real AML structuring is cash deposits
   and ACH, not wires just under a threshold. Our `fan_in` typology emits wires,
   which is the wrong rail for the canonical typology. Recommend phase I, because
   the entity model is the prerequisite for both and phase C is already 1,300 LOC,
   but flagging that until phase I ships, structuring fidelity is compromised in a
   way an AML practitioner will notice.

---

## 8. Risks

Ordered by cost if they land.

**500 MB/s may not be reachable at an allocatable pod size.** At today's 202
bytes/row it needs 155K rows/s per core on 16 cores, at the top of the plausible
Rust range. Mitigation: bytes/row rises with the richer schema, which moves the
target the right way, and phase C measures it before phase F commits. Fallback: 32
cores per pod, one pod per node.

**Node NIC and FlashBlade write ceiling are unverified.** 4 Gbps per pod and
roughly 6 GB/s aggregate. Both need a check in phase G before a run is designed
around them, and either could cap aggregate throughput below 12x single-pod
regardless of generator quality.

**Determinism under rayon.** The thread-invariance property in 3.3 is easy to
break and the break is silent. Mitigation: gate 6.3 runs on every commit, and
row-group seeding is the only sanctioned intra-file parallelism.

**arrow-rs nested StructBuilder performance is unproven for us.** 41 columns with
five levels of nesting. Mitigation: a spike in phase B on the pacs.008 schema
shape before phase C commits to it.

**Rust becomes a maintenance surface.** One more toolchain in the image build and
one more language for whoever inherits this. Mitigation: the generator is a leaf
binary behind a stable argv contract, with no Rust anywhere in the lakebench
package, and typologies are TOML rather than code.

**Silver and generator must land together.** Gate 5b and gate 8 are meaningless if
silver still hardcodes `sanctions_status='clear'`. Phase G carries both, and
neither ships alone.
