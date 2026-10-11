# Customer 360 benchmark: disclosure and comparability

## 9. Disclosure requirements

`metrics.json` carries an `experiment` block, schema `exp2` or `exp1`
([7.4](execution-rules.md#74-prohibited-changes-invalidate-a-result-or-are-refused);
`v2_unavailable` names what was missing, `metrics/experiment.py`). A
published result carries all of it:

| Area | Must show |
|---|---|
| Workload | name `customer360`, version `c360-2.dev1`; parameters id and the resolved `unique_customers` and event window |
| Corpus | id, generator image as configured and as run, digest, seed, scale, timestamps, `dirty_data_ratio`, whether each was observed from the datagen pods or only declared; on exp2, corpus id v2 and its lineage; problems when the pods disagree |
| Architecture | recipe; catalog, table format and query engine types and versions (Hive as the derived Stackable image); Spark image; query access path (`catalog`, `direct_storage`, or null without a query engine) |
| System and support | `cluster` or `local`; support state frozen at run start (`supported`, `unverified`, `unsupported`) and its basis |
| Mode and maintenance | mode; maintenance policy id, configured settings and effective maintenance per operation (compaction operation included), with known limitations |
| Stages | executed and skipped (a skipped benchmark or maintenance is listed) |
| Limits | benchmark iterations and mode, continuous rounds and [trickle](../../glossary.md#trickle), trigger intervals set in the config (`limits.trigger_bound`), executor caps with `cap_hit` and budget caps, sizing cuts, maintenance stop, the bound kinds |
| Results | `query_set_id` and one fingerprint per query, or `not_checked` with the reason; for continuous `composite_qph`, its `composite_qph_basis` |
| Correctness | the `c360_correctness` record: its 16 gating checks drive the verdict and exit code; other failures are reported only ([5.2](correctness.md#52-expected-result-checks-batch-only)) |
| Repetitions | runs = 1 and samples per query; a repeatability claim needs repeated runs or an `n=1` label |
| Provenance | Lakebench commit, dirty tree flag |

## 10. Comparability

Lakebench does not compare runs; a reader compares two reports
([Comparing runs](../../benchmarking/comparing.md)). Two C360 runs are
comparable only when (`metrics/comparability.py`):

- both passed (FAILED, INTERRUPTED and VOID runs have no performance to
  compare; a failed gating `c360_correctness` check fails the verdict) and
  carry experiment blocks of the same identity version;
- every Workload and Corpus identity key
  ([7.4](execution-rules.md#74-prohibited-changes-invalidate-a-result-or-are-refused))
  is equal and neither run has a corpus problem;
- they ran the same query set with every result fingerprint matching.

- A run with no checked results (`--skip-benchmark`, a `*-none` recipe, a
  continuous run that did not settle or skipped the result check) cannot
  show matching answers.
- Execution conditions: effective maintenance, compaction operation,
  maintenance settings, benchmark iterations and mode, the Lakebench caps
  that bound, and (continuous) the in-stream round count. When they differ,
  or only the dependency pinset does, a difference in the numbers may come
  from them. A Trino-vs-Thrift Iceberg pair that both compacted differs in
  conditions ([7.2](execution-rules.md#72-maintenance-policy-handling)).
- Query access path and system are Architecture and System keys, not
  conditions; when architecture and system both differ, no difference can be
  put down to either.
- Compare QpH only between runs with the same non-unknown `query_set_id`; a
  continuous median over rounds of different sets reads `blended`
  ([8.2](metrics.md#82-continuous)).
- Continuous rows per second from a run whose corpus drained with less than
  90% arrival is over a short arrival; do not compare it.
