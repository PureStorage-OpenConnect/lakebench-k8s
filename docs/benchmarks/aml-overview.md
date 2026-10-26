# AML workload: overview

For the person choosing which Lakebench workload to run against an
architecture. It explains what the Financial Crime / AML workload is, what
each stage is designed to stress, and what a comparison across architectures
therefore learns. For the exact rules, metrics, scoring and compatibility,
follow the links into [the specification](AML.md).

## What it models

A bank's transaction-monitoring back office reading a five-year archive of
ISO 20022 pacs.008 credit transfers. A generator plants known
money-laundering typologies (structuring, layering, round-tripping,
sanctions and PEP hits) into an otherwise clean payment stream. The pipeline
lands the messages, normalises them into payments, parties, accounts,
double-entry statements, a counterparty graph and behavioural profiles, runs
detection rules that raise alerts, and answers investigator and reporting
queries.

The workload exercises how an architecture executes this pipeline. It is not
a measure of how good a production AML system would be at catching crime; the
typologies are planted and known, so detection quality is a correctness
check on the pipeline, not a claim about real-world efficacy.

## Why run it

AML is the heavy, stateful workload. Pick it when the architectural question
you want measured is any of these:

- How a table format and catalog handle registering a large body of existing
  files, then maintaining update-heavy tables under streaming writes.
- How the stack sustains incremental detection over an accumulated history
  that grows for the whole run.
- How graph, window and ledger queries behave over tens of millions of
  payments.

If you want the light, scan-and-aggregate end of the spectrum instead, run
[Customer 360](c360-overview.md). Choosing between the two workloads is
choosing which architectural stress you want to measure.

AML runs on Iceberg recipes with a Trino query engine: recall and precision
are scored with SQL on Trino, so `none`, `duckdb` and `spark-thrift` are
refused at load. It is not supported on Delta.

## What each stage is designed to stress

Each stage targets a distinct architectural capability.

### Bronze: file registration, not ingest compute

Bronze-verify validates the landing Parquet and registers it into Iceberg
with a zero-copy `add_files`, so no payment data is rewritten. The stage
therefore measures catalog and table-format behaviour under mass file
registration, not scan or compute throughput. Above 1.5 TiB or 800,000
files, or if `add_files` fails, it falls back to a CTAS that copies the data
and doubles bronze storage.

### Silver: streaming upsert and commit cost

Silver is where update-heavy maintenance is stressed. It builds payments,
parties, one account per IBAN, double-entry statements with running
balances, a counterparty-pair graph, and per-entity behavioural profiles. In
continuous mode each micro-batch runs a dimension MERGE and a profile MERGE
with a running statistical recurrence, then commits. So silver measures
streaming upsert cost and the per-commit overhead a catalog and table format
carry when many small transactions land back to back.

Silver maintains accumulated state, and the cost of a batch tracks the size of
that state, not only the rows the batch brought in. The double-entry ledger
and the dimensions are rewritten or merged against the whole table, so
per-batch cost rises as the run accumulates history.

### Gold detection: incremental computation over growing state

Gold runs the detection rules. In continuous mode a tick re-detects only what
the silver rows new since the last tick can change, reading each rule's
event-time spans, not all of silver. This is the incremental-detection
stress: how the architecture sustains repeated graph and window detection
against a table that keeps growing while it is being written. As with silver,
per-tick cost is sensitive to accumulated history, so the comparison learns
how each architecture holds up as the corpus grows, not just at one size.

### TM operations: operational writes and lookups

Alert triage simulates an operational workload: alerts become dispositions
and cases. This is the write-and-point-lookup axis, distinct from the
analytical queries, and it exercises how the stack handles a stream of small
operational writes alongside the detection passes.

## What the queries probe

The query set spans a ladder of access patterns, so one run reads an engine's
behaviour across several shapes: raw scan throughput, predicate pruning and
windowing, two-hop graph and edge aggregation over the counterparty graph, a
ledger-ordered running-balance window whose result grows with scale, and
operational alert and investigator lookups. The exact queries and what each
one reads are in [the query set](aml/queries.md).

## Batch and continuous are different questions

The two modes do not measure the same thing at different speeds.

- **Batch** rebuilds silver and runs detection once over a fixed corpus, then
  scores recall and runs the query benchmark. It answers: how fast does this
  architecture turn a body of data into answers, and how good are those
  answers.
- **Continuous** runs ingestion, silver and detection as live streams and
  measures sustained freshness and throughput under constant writes. It
  answers a different question: how well does the architecture keep an
  always-on pipeline current as data arrives. Some batch measures, such as the
  matching-results comparison, do not apply in continuous mode.

Run continuous when the thing you care about is steady-state behaviour under
load; run batch when you want a clean, scored, repeatable pass over a fixed
corpus.

## How to read the results, and where the detail is

Results compare only across runs made under matching conditions, and a cap
measures the cap, not the architecture's ceiling. The disclosure and
comparability rules are in [comparability](aml/comparability.md); what is
fixed, tunable and prohibited is in [execution rules](aml/execution-rules.md).

For the detail behind everything above:

- [Data model](aml/data-model.md): the bronze, silver and gold tables.
- [Data generation](aml/generation.md) and [typologies](aml/typologies.md):
  the corpus, scale and planted patterns.
- [Pipeline](aml/pipeline.md): the batch and continuous stages in order.
- [Detection rules](aml/rules.md): each rule and what it detects.
- [Query set](aml/queries.md): the queries and what they read.
- [Scoring](aml/scoring.md) and [reading the scores](../aml-scoring.md):
  recall, precision and the alert set.
- [Metrics](aml/metrics.md) and [continuous metrics](aml/continuous-metrics.md).
- [Known limitations](aml/limitations.md).

The full specification index is [AML.md](AML.md).
