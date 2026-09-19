# Datagen v2 -- POC, Implementation, Characterization

Status: proposal. Companion to `lakebench.next-spec-datagen-v2-memo.md`, which
holds the architecture and the 47-metric gate. That doc is the design; this one is
how it gets built and how it gets proven.
Date: 2026-09-17. Branch: feat/fraud-aml.

---

## 1. Shape of the work

Three stages, and the boundaries matter.

**POC** answers the five questions whose answers would change the design. Each
has a pass threshold and a kill criterion. Roughly 800 LOC, throwaway except for
one harness that becomes `cargo bench`. One of the five needs the cluster.

**Implementation** is twelve mergeable PRs, roughly 5,300 LOC. Ten of the twelve
verify with no cluster at all, because Garage covers the S3 path locally. The
gate harness lands second, before any domain code, so every later PR flips
skipped metrics to passing and unmeasured progress is impossible.

**Characterization** is five designed experiments that replace the ad-hoc sweep
we ran on 2026-09-16 and 17. It is the deliverable a customer reads.

The POC exists because three of the memo's architectural commitments rest on
unverified assumptions: that arrow-rs is fast enough on a deeply nested schema,
that one pod can push 500 MB/s at the object store, and that the counterparty
ring model produces a graph where planted patterns are distinguishable from
background noise. If any of those is wrong, it is much cheaper to know before
5,300 lines exist.

---

## 2. POC

### POC-1 -- arrow-rs throughput on the pacs.008 nested schema

The load-bearing assumption of the whole plan. 41 columns, struct nesting three
deep, and one `List<Struct<..., List<String>>>` for regulatory reporting.

Build the schema in arrow-rs, fill it through per-column builders with
realistic-length strings (36-char UETR, 22-char IBAN, 20-char LEI, 11-char BIC,
30-char names, structured addresses), and measure single-threaded row build,
Parquet plus snappy encode, and bytes per row.

| | Threshold |
|---|---|
| Pass | >= 200K rows/s build, >= 150K rows/s build plus encode, single core |
| Kill | < 100K rows/s build plus encode |

A kill means 500 MB/s needs more than 32 cores per pod at projected bytes/row,
and the response is to reconsider the schema shape (flatten the nesting, or
pre-render the high-cardinality string columns) before reconsidering the
language. ~250 LOC. No cluster. The harness is kept and becomes the `cargo bench`
target in I-9.

### POC-2 -- object-store ceiling from one pod, and in aggregate

Independent of generation. Feed `object_store` pre-generated 64 MB buffers and
sweep upload concurrency (4, 8, 16, 32) against part size (8, 16, 32 MB). Then
run the same binary on 2, 4, 8 and 12 pods concurrently for the aggregate
ceiling.

| | Threshold |
|---|---|
| Pass | >= 500 MB/s single pod, >= 4 GB/s at 8 pods |
| Kill | < 250 MB/s single pod |

This is the only POC that needs the cluster, and it is the one that retires two
prerequisites the memo currently lists as unverified: 4 Gbps sustained per pod
against the node NIC, and the FlashBlade's aggregate write ceiling. A kill here
means the 500 MB/s target is storage-bound rather than generator-bound, which
changes the goal rather than the implementation. ~150 LOC.

### POC-3 -- ring-model graph properties and the background motif rate

The model POC, and the one most likely to change the design. Pure Python, no
schema, no Parquet, because it is combinatorics.

Generate the ring model at population 500K, sample 24M transactions, build the
edge list, and measure the graph properties the gate will later assert:
bidirectional entity share, out-degree p50 and p99, repeat-edge volume share,
supernode share.

Then the question the memo does not yet answer: **what is the natural background
rate of the patterns we plant?** Count naturally occurring 4-cycles and 4-to-1
fan-ins in the baseline with no typologies injected at all.

This matters because planted density is 0.1%. If the ring model throws 4-cycles
at a comparable rate by accident, W3's precision sits at the floor and a planted
cycle is indistinguishable from noise, so recall becomes meaningless even when
the detector works. The response would be to tighten the planted signature
(amount similarity, shorter time window, consistent currency) or sparsen the
rings, and that is a model decision best made now.

| | Threshold |
|---|---|
| Pass | bidirectional >= 85%, repeat-edge share 0.70 to 0.90, supernode < 2%, background motif rate quantified with a recommended planted-signature tightness |

~250 LOC. No cluster.

### POC-4 -- determinism under rayon

Folded into POC-1's harness. Seed per row group from `(seed, file_id, row_group)`,
generate at 1 and 8 threads, compare sha256 of the written file.

Pass: identical. Fail means intra-file parallelism is restricted to whole files
and the perf budget loses a degree of freedom. ~50 LOC. No cluster.

### POC-5 -- realised bytes per row

Assemble the synthetic data tables first (name lists, city lists per country,
postcode formats, 500-BIC pool), then re-run POC-1 with real string
distributions rather than fixed-width filler.

No threshold. This measurement *is* the answer, because it sets the rows/s target
that the perf work is held to. Today's 202 bytes/row comes from degenerate
strings; the memo projects 400 to 650. ~100 LOC, mostly data assembly. No
cluster.

### POC exit

| POC | If it fails |
|---|---|
| 1 | Reconsider schema shape, then language |
| 2 | Target is storage-bound; revise the goal, not the build |
| 3 | Redesign planted signature or ring density |
| 4 | Lose intra-file parallelism from the perf budget |
| 5 | No failure mode; it sets the rows/s target |

---

## 3. Implementation

Twelve PRs. Each keeps Customer360 untouched and `pytest tests/` green. The
"Verify" column is what makes the PR mergeable without cluster access unless
stated.

| # | PR | LOC | Depends | Verify | Gate rows |
|---|---|---|---|---|---|
| I-1 | Build and dispatch plumbing. Cargo workspace, multi-stage Dockerfile with cargo stage, `python:3.13-slim` final, `generate.py` execv dispatch on `--schema financial`. Binary is a hello-world that parses the full argv contract and echoes it as JSON. | 150 | -- | `podman build`; financial dispatch echoes parsed argv; customer360 still runs Python; pytest green | none |
| I-2 | Gate harness scaffold. `tests/test_financial_datagen_gate.py` plus `tests/gate/`, threshold table as data in one place, DuckDB readers for bronze, party and manifest zones, all 47 metrics stubbed as skips. | 300 | I-1 | pytest collects 47, all skip; threshold table matches the memo | scaffold |
| I-3 | `lb-world`. Entity, account, bank, counterparty ring as pure functions of `(seed, id)`. Synthetic data tables. Party and account zone Parquet, partitioned over the id space. | 700 | I-2 | Gate 1 at scale 0.01 and 1; cargo tests for hash stability and ring determinism | 1.1-1.13 |
| I-4 | `lb-pacs008` schema and baseline generation. Arrow schema, columnar builders, baseline rows drawing on `lb-world`, ported amount and corridor parameters. | 600 | I-3 | Gate 2 identity and volume rows; bytes/row recorded | 2.1, 2.2, 2.6-2.11 |
| I-5 | Fidelity layer. IBAN and LEI check digits, BIC format, cross-currency wires with real `xchg_rate`, timing model (salary days, holidays, local timezone, business-day settlement), purpose-code distribution, correspondent chain plus `prvs_instg_agt`, variable `nb_of_txs`, remittance and ultimate parties. | 500 | I-4 | Gate 4 | 4.1-4.17 |
| I-6 | `lb-typology`. Nine primitives including `synthetic_identity`, TOML shape definitions, density-calibrated scheduler, manifest with `transaction_ids` and `injection_parameters`, written outside the workload read path. | 600 | I-5 | Gate 3 and Gate 2 path rows | 2.3-2.5, 3.1-3.10 |
| I-7 | `lb-emit` and orchestration. Parquet writer with row-group control, `object_store` multipart with retry, file windowing, rayon over row groups with per-row-group seeding, `DATAGEN_FILE_METRICS`. | 700 | I-6 | Gate 6 against local Garage; end-to-end scale 0.01 | 6.1-6.4 |
| I-8 | Reference detectors. W1, W2, W3 in DuckDB over emitted Parquet; recall, lift and precision against the manifest. | 350 | I-7 | Gate 5 | 5.1-5.6 |
| I-9 | Performance. Allocation elimination, reused string buffers, rayon tuning, row-group pipelining, upload concurrency. `cargo bench` from POC-1's harness. | 300 | I-8 | Gate 7; one single-pod run on one worker node | 7.1-7.7 |
| I-10 | Silver and lakebench integration. `silver_build_financial` consumes the party and account zones, FX-table normalisation, `array_remove` fix, `txn_type` from message type. Per-schema datagen pod sizing. Prerequisite checks for NIC bandwidth and object-store write ceiling. | 500 | I-9 | pytest for Spark query builders; Gate 5b and Gate 8 on cluster | 5b, 8.1-8.3 |
| I-11 | Characterization harness. Experiment runner, `characterization.parquet` emitter, scorecard block. | 400 | I-10 | pytest for the runner; C-1 dry run | -- |
| I-12 | Cleanup. Delete `financial.py`, `typologies.py`, `realism.py`, `manifest.py`, `verify_run.py` and the Python financial branch. `financial_dimensions()` recalibration from measured bytes/row. Spec amendments for the four memo decisions. Docs. | 100 net | I-11 | pytest green, gate green, ruff clean | REQ-S-03 |

Two sequencing choices are deliberate and worth defending.

**Plumbing first (I-1).** A hello-world binary through the real multi-stage image
and the real execv dispatch means we never debug domain logic and build
infrastructure at the same time. It is the cheapest PR and it de-risks every one
after it.

**Gate second (I-2).** The gate is the progress bar. Landing 47 skipped metrics
before any generator code exists means each subsequent PR has an objective,
pre-agreed definition of done, and a PR that claims to implement the timing model
without turning rows 4.8 to 4.12 green is visibly incomplete.

---

## 4. Characterization

### 4.1 Why this is what the perf work is for

The spec's top scale point is currently unmeasurable. At the 9.4 MB/s aggregate
we measured at 8 pods, against a target of roughly 6 GB/s at 12 pods:

| Scale | Approx corpus | At measured 9.4 MB/s | At target 6 GB/s |
|---|---|---|---|
| 100 | 1.2 TB | 35 hours | 3 minutes |
| 1000 | 12 TB | 15 days | 33 minutes |
| 10000 | 120 TB | 148 days | 5.6 hours |

REQ-S-01 promises scale 10000 as the tier-1 credibility floor. It is a 148-day
run today, which is to say it does not exist. The case for 500 MB/s per pod is
not that faster is nicer; it is that the benchmark cannot currently characterize
its own headline scale point. Scale 1000 and 10000 cells are gated on confirming
FlashBlade capacity for a 120 TB corpus.

### 4.2 The five experiments

**C-1, per-pod perf envelope.** Cells: cores in {1, 2, 4, 8, 16, 32} at fixed
scale. Metrics: rows/s, MB/s, MB/s per core, peak RSS, CPU utilisation. Informs
the per-schema `datagen.cpu` default and locates the per-core efficiency knee.

**C-2, scale-out linearity.** Cells: pods in {1, 2, 4, 8, 12} at C-1's optimum
core count. Metrics: aggregate MB/s, per-pod MB/s, departure from linear, S3
request rate, object-store latency p50 and p99. Informs maximum useful
parallelism, and attributes the ceiling to generator, network or storage.

**C-3, scale-factor curves.** Cells: scale in {0.01, 0.1, 1, 10, 100, 1000}.
Metrics: wall clock, rows, bytes, bytes/row, object count, plus every Gate 2, 3
and 4 metric re-measured. Informs the REQ-S-02 linearity claim, REQ-S-03
calibration, and the published dimensions table. This is also the scale-invariance
proof, which the earlier sweep did provide and which must be re-established
against the new model.

**C-4, file-size sensitivity.** Cells: `file_size` in {16, 64, 256, 512} MB at
fixed scale. Metrics: datagen MB/s, object count, and downstream silver-build
wall-clock and throughput over the same corpus. Informs the `file_size` default.
This quantifies the small-file penalty end to end, which nothing in the project
has measured.

**C-5, pipeline balance.** At scale 1, 10 and 100: datagen MB/s against
bronze-verify, silver-build and gold-finalize throughput. Informs whether datagen
is still the constraint. If datagen reaches 6 GB/s while silver-build sustains
100 MB/s, the bottleneck has moved and the next optimisation target is silver, not
the generator. This is the honest counterweight to the 500 MB/s goal and it should
be published alongside it.

### 4.3 Methodology

Specified up front, because otherwise cells are not comparable and the results
are not defensible.

- Three repeats per cell. Report median with min and max spread.
- Bronze wiped between cells. Cold start every time.
- One image digest per experiment, recorded with the results.
- Results to `characterization.parquet` keyed on `(experiment, cell, metric,
  value, unit, image_digest, run_id, captured_at)`, satisfying REQ-M-03
  provenance.
- A cell whose spread exceeds 10% is re-run and flagged, never averaged.
- No cell is recorded as passing on exit code alone. Every cell carries its
  measured values or it is a failure. This is the LB-044 lesson: sustained mode
  was green for two UAT rounds while producing no data, because the runner
  checked only exit codes.

### 4.4 Dependencies

C-1 and C-2 need I-9. C-3 needs I-9 plus confirmed storage capacity per scale
point. C-4 and C-5 need I-10, since both measure downstream Spark stages.
POC-2 is independent of everything and can run first, in parallel with the POC-1
spike.
