# AML (financial) Pipeline Benchmark Specification

Reference: the AML benchmark specification: purpose, supported compositions, and links to the numbered pages.

- Workload version `aml-3` (`experiment.workload.version`).
- Generator model `datagen-v2-rs-0.4`.
- Query sets `qs12-910d16a91962` (batch with TM operations) and `qs8-ffe2bc1a012e` (FQ1 to FQ8).
- Maintenance policy `m2-2026-09-26`.

For engineers who run, reproduce or compare this benchmark, and contributors
who implement it on a new component. Every statement describes the code of
this release and names the module. Python paths are relative to
`src/lakebench/`; generator paths to `datagen_rs/`. A reader's guide to the
scores is [aml-scoring.md](../aml-scoring.md).

Figures labelled "recorded" come from three 1.6 run records
([recorded 1.6 figures](aml/limitations.md#recorded-16-figures)).

## Pages

1. [Purpose and scope](#1-purpose-and-scope)
2. [Data model](aml/data-model.md)
3. Data generation: [generator, scale, delivery, content](aml/generation.md) (3.1, 3.2, 3.4, 3.5, 3.7); [seed policy](aml/seed-policy.md) (3.3); [typologies and planting](aml/typologies.md) (3.6, 3.8, 3.9)
4. Pipeline: [batch, continuous, drain, out-of-run commands](aml/pipeline.md) (4.1, 4.3, 4.4, 4.5); [detection rules](aml/rules.md) (4.2)
5. [Correctness contract](aml/correctness.md)
6. [Query set](aml/queries.md)
7. [Execution rules](aml/execution-rules.md)
8. Metrics: [batch and both modes](aml/metrics.md) (8.1, 8.3); [continuous](aml/continuous-metrics.md) (8.2); [scoring, alert set, reference detector](aml/scoring.md) (8.4, 8.7, 8.8); [TM operations](aml/tm-operations.md) (8.5); [gold-finalize timing](aml/gold-timing.md) (8.6)
9. [Disclosure](aml/comparability.md#9-disclosure-requirements) and 10. [Comparability](aml/comparability.md#10-comparability)
11. [Supported compositions](#11-supported-compositions)
12. [Known limitations](aml/limitations.md)

## 1. Purpose and scope

The workload models a bank's transaction-monitoring (TM) back office. It
runs over a five-year archive of ISO 20022 pacs.008 credit transfers. A
synthetic generator plants known money-laundering typologies into a clean
payment stream. The pipeline:

- lands the messages (bronze);
- normalises them into payments, parties, accounts, statements, a
  counterparty graph and profiles (silver);
- runs nine detection rules and a simulated alert-triage, case and SAR
  workflow (gold).

A fixed query set is then timed against the result.

What it measures about an architecture (catalog x table format x pipeline
engine x query engine, on a known system):

| Mode | Measures |
|---|---|
| Batch | time to turn a fixed raw corpus into queryable gold (`time_to_value_seconds`); data volume and requested compute used; query-set speed (`composite_qph`) |
| Continuous | gold staleness while datagen writes at the scale's offered load (`data_freshness_seconds`); time from a planted pattern's payments landing to its alert (`time_to_detect_seconds`); the ingest rate bronze holds; QpH while tables are written |

- These figures are valid only when the verdict passes
  ([5](aml/correctness.md#5-correctness-contract)).
- A batch comparison also needs both runs to return the same query results
  ([10](aml/comparability.md#10-comparability)).
- An AML continuous run records no result fingerprints. Its alerts depend on
  when detection and TM passes ran relative to arrival. A continuous
  comparison can never establish its results.

What it does not claim:

- **It is not an AML product.** The rules are illustrative detectors with
  fixed, self-chosen parameters. The TM workflow is a seeded simulation with
  default analyst (0.90) and investigator (0.95) accuracies. Both are
  configurable and hashed into the workload parameters, so a change makes two
  runs not comparable ([7.3](aml/execution-rules.md#73-prohibited-changes-invalidate-a-result-or-are-refused)).
- **Detection quality is not an architecture property.** Recall and
  false-positive rate belong to the corpus and the rules. They are reported
  beside a run and never decide its verdict. What does fail a run
  ([5](aml/correctness.md#5-correctness-contract)):
  - batch: rule errors, disallowed skips, expected rules that did not run,
    or zero alerts;
  - continuous: a mode-excluded rule that ran, an expected rule that
    skipped, errored or did not run on the scored [tick](../glossary.md#tick), or zero alerts.
- **Capped figures measure the cap.** Lakebench imposes executor ceilings,
  rule caps, the continuous [trickle](../glossary.md#trickle) and the TM per-customer alert
  cap ([7.4](aml/execution-rules.md#74-lakebench-imposed-caps)).
- **The fidelity gate and [registered looks](../glossary.md#look) measure generator realism**, not
  an architecture. Neither runs inside `lakebench run`.
  - On a development seed the gate runs on the cluster with
    `lakebench financial reference-score`.
  - A registered evaluation or robustness look is refused there. It runs
    only through `scripts/aml_gate.py --registered`, which records the look
    ([3.3](aml/seed-policy.md#33-seed-policy)).

## 11. Supported compositions

AML runs on Iceberg only. Of the 11 structurally valid tuples, 8 are
workload-compatible, in batch and continuous (`config/schema.py`,
`WORKLOAD_TABLE_FORMATS` and `WORKLOAD_MODES`).

Support states per recipe and mode are in the generated
[support table](../compatibility-matrix.md#support-states). AML is supported
only on the cells it lists; every other valid combination is unverified.

| Recipe | Query access path | Spark image | Notes |
|---|---|---|---|
| hive-iceberg-spark-trino (`default`) | catalog | 4.1.1 | |
| hive-iceberg-spark-thrift | catalog | 4.1.1 | query and pipeline share Spark |
| hive-iceberg-spark-duckdb | direct storage | 4.1.1 | DuckDB runs no Iceberg maintenance |
| hive-iceberg-spark-none | none | 4.1.1 | no query benchmark, QpH or result fingerprints |
| polaris-iceberg-spark-trino | catalog | 4.0.2 | |
| polaris-iceberg-spark-thrift | catalog | 4.0.2 | |
| polaris-iceberg-spark-duckdb | direct storage | 4.0.2 | as DuckDB above |
| polaris-iceberg-spark-none | none | 4.0.2 | as none above |
| hive-delta-spark-{trino,thrift,none} | -- | -- | unsupported, refused at load: the AML stage scripts and DDL are Iceberg-only |

- The pipeline engine is Spark in every recipe. Each recipe names its Spark
  image (`config/recipes.py`): `apache/spark:4.1.1-python3` on Hive,
  `apache/spark:4.0.2-python3` on Polaris. A config that sets `images.spark`
  runs that image.
- The release matrix's AML rows (`config/support.py`): batch
  hive-iceberg-spark-trino at scale 1 and 10, batch
  polaris-iceberg-spark-trino, continuous hive-iceberg-spark-trino and
  continuous polaris-iceberg-spark-trino.
- Continuous mode runs W2, W3, W4, W5 (transaction screen only, no rescreen),
  W6 and W17 on every recipe.
- `--local` is refused for AML; local mode runs Customer 360 batch only.
