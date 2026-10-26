# Customer 360 workload: overview

For choosing which Lakebench workload to run against an architecture: what
the Customer 360 workload is, what each stage stresses, and what a comparison
across architectures learns. Exact columns, metrics and compatibility are in
[the specification](C360.md).

## What it models

A retail analytics pipeline. Customer interaction events (web, app, store,
support and more) are generated, validated, enriched into a wide per-event
record, aggregated into daily key performance indicators, and queried by
analysts and dashboards.

The workload exercises how an architecture executes this pipeline, not the
business value of any particular metric.

## Why run it

Customer 360 is the light, scan-and-aggregate workload. Its silver stage is
a single pass with no joins, and its gold is a small daily rollup. Pick it
when the architectural question you want measured is any of these:

- Single-pass transform and scan-and-write throughput, free of join cost.
- Grouped aggregation and single-file write behaviour.
- How an engine behaves across a ladder of query shapes, from raw scan to
  windowed analytics, over a clean wide table.

[AML](aml-overview.md) is the heavy, stateful workload. Choosing between the
two workloads is choosing which architectural stress you want to measure.

Customer 360 runs on both Iceberg and Delta recipes, so it also serves as the
cross-table-format comparison.

## What each stage is designed to stress

### Bronze: schema validation and scan throughput

In batch mode no bronze table is written. Bronze-verify is a validation pass
over the landing Parquet: required columns and their types, row counts, and
key columns that must not be entirely NULL. Bronze measures schema
validation and file-scan throughput. AML's bronze registers files in place.

### Silver: single-pass transform, no joins

Silver drops duplicate-flagged rows and derives a wide per-event record in a
single pass with no joins. In batch mode one shuffle clusters the rows by day
before the write. It measures transform throughput: how fast the architecture
can scan, compute per-row columns, and write. With no cross-row state, silver
cost tracks the data volume of the run, not the length of a history. AML's
silver maintains state that grows.

### Gold: grouped aggregation and single-file write

Gold groups the whole silver table by day into a set of KPIs and writes the
result as a single coalesced file. The output is around a year of daily rows,
so the stage measures the aggregation path, not output volume. In batch mode a
non-degeneracy gate then reads gold and silver to confirm one gold row per
silver day, a full-table verification read. In continuous mode gold
recomputes only the dates that new rows touch.

## What the queries probe

The query set spans a ladder of access patterns, so one run reads an engine's
behaviour across several shapes: raw scan, predicate pushdown and partition
pruning, hash aggregation, window frames, a recency and frequency calculation
over dates, and a read of the pre-aggregated gold table. What each query
stresses is in [the query set](c360/queries.md).

## Batch and continuous are different questions

The two modes do not measure the same thing at different speeds.

- **Batch** runs the pipeline once over a fixed corpus and then the query
  benchmark. It answers: how fast does this architecture turn a body of data
  into answers.
- **Continuous** runs the pipeline as a live stream and measures sustained
  freshness and throughput under constant writes, with gold recomputing only
  the dates that change. It answers: how well does the architecture keep an
  always-on rollup current as events arrive.

## How to read the results, and where the detail is

Results compare only across runs made under matching conditions, and a cap
measures the cap, not the architecture's ceiling. The disclosure and
comparability rules are in [comparability](c360/comparability.md); what is
fixed, tunable and prohibited is in [execution rules](c360/execution-rules.md).

For the detail behind everything above:

- [Data model](c360/data-model.md): the silver columns and gold KPIs.
- [Data generation](c360/generation.md): the corpus and scale.
- [Pipeline](c360/pipeline.md): the batch and continuous stages in order.
- [Query set](c360/queries.md): the queries and what they read.
- [Metrics](c360/metrics.md).
- [Known limitations](c360/limitations.md).

The full specification index is [C360.md](C360.md).
