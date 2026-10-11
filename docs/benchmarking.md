# Scoring and Benchmarking

Reference: what Lakebench measures and where each score, verdict rule and record field is defined.

| Measurement | What it answers |
|---|---|
| Pipeline scorecard | End to end over datagen, bronze, silver and gold. Batch: how fast does raw data become queryable gold? Continuous: how fresh is gold, and can the pipeline keep up? |
| Query engine benchmark | SQL over silver and gold on the recipe's engine (Trino, Spark Thrift Server or DuckDB). Customer 360 runs 8 queries; AML runs 12 (FQ1-FQ8 and investigator queries IQ1-IQ4). The score is QpH (queries per hour). |

- The scorecard carries QpH as `composite_qph`, but the two are separate operations.
- `lakebench run` produces both.
- `lakebench benchmark` runs only the query benchmark and saves its own record ([Benchmark records](benchmarking/records.md#benchmark-records)).

## Pages

- [Batch scorecard](benchmarking/scorecard.md): batch scores, stage timing, resources, Customer 360 checks
- [Continuous scores](benchmarking/continuous.md): window, scores, regimes, intake limits, [trickle](glossary.md#trickle)
- [Query benchmark](benchmarking/query-benchmark.md): QpH modes, samples, in-stream rounds
- [Maintenance and Lakebench caps](benchmarking/maintenance.md): maintenance scoring, settle wait, compaction, caps
- [Verdict](benchmarking/verdict.md): PASSED, continuous gate, result check, requested vs effective
- [Tuning a continuous pipeline](benchmarking/continuous-tuning.md): levers and checklist
- [Run records](benchmarking/records.md): `metrics.json`, provenance, benchmark records
- [HTML report layout](benchmarking/html-report.md): header, cards, sections, AML results
- [Comparing runs](benchmarking/comparing.md)
- [Glossary](glossary.md): terms used in reports and records
