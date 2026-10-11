# AML benchmark: execution rules

## 7.1 Required

| Item | Rule | Enforced by |
|---|---|---|
| Composition | Iceberg and one of the 8 Iceberg 4-tuples ([11](../AML.md#11-supported-compositions)); `custom` refused | config load |
| Scale | above 800 refused; above 300 up to 800 runs as unverified, with a note ([3.2](generation.md#32-scale-factor)) | config load, run start (`config/support.py`) |
| Support state | `unsupported` refused at run start (exit 2); `--local` refused for AML | `lakebench run` |
| Stages | batch: bronze-verify, silver-build, gold-finalize, then scoring and the AML gates; continuous: bronze-ingest, silver-stream, gold-refresh, then the [drain](../../glossary.md#drain) and covered scoring | `run`; scoring and gates only on a full run (no `--stage`) whose stages succeeded |
| Corpus | batch datagen only with `--generate` (a multi-cycle run generates in its cycles and refuses the flag); continuous always generates unless `--skip-generate` | `run` |
| Seed | per [3.3](seed-policy.md#33-seed-policy); leave `datagen.corpus_role` unset on a `run` config | config load (seed and role checks); the role on a `run` config is not refused ([12](limitations.md#12-known-limitations)) |
| Benchmark iterations | `benchmark.iterations`, 1 to 100, default 3 (batch); continuous rounds take 1 sample | config load |
| Batch benchmark mode | one hot-cache power pass, one stream. `benchmark.mode` `throughput` or `composite`, `benchmark.cache: cold`, or `benchmark.streams` above 1 on a `run` config is refused (exit 2); `standard` and `extended` are power runs | config load |
| Maintenance | policy `m2-2026-09-26`; `--skip-maintenance` stamps `m2-2026-09-26+skipped`; the effective outcome per operation is in `maintenance_outcomes` and `experiment.effective_maintenance` | `run`, record |
| Job timeout | without `--timeout`: max(3600, 120 x scale) + 900 s, never below the AML bronze-verify budget max(5400, 120 x scale); an explicit `--timeout` is used as given | job submission |
| Query timeout | 900 s per query in `run` (warm-up, pre-compaction and scored rounds), recorded as `benchmark_query_timeout_seconds` | runner |
| Maintenance budget | 1,800 s per statement and 1,800 s shared across expire, orphan removal and compaction; the first timeout stops the rest | `run` |
| Settle wait | probes `FQ1_txn_full_scan` until two consecutive probes agree within `maintenance_settle.tolerance_pct` (default 10%), up to `maintenance_settle.max_seconds` (default 2,700 s); skipped when no maintenance statement ran | `run` |

A publishable AML result is a full batch `lakebench run` without `--stage`,
with the benchmark and a query engine, on an unverified or supported
composition, with verdict PASSED. An AML continuous run records no result
fingerprints, so it cannot show it returned another run's answers.

## 7.2 Permitted tuning (still publishable)

None of these changes the Workload or Corpus identity keys, so runs differing
only here stay comparable. What a difference means (`metrics/comparability.py`):

- **Architecture keys**: catalog (Hive or Polaris) and query engine within
  the Iceberg recipes; component versions; query access path (catalog or
  direct storage); observed image digests and the dependency pinset;
  user-set executor overrides, driver overrides and Spark conf.
  - Two runs differing only here, on the same system with matching results,
    differ in architecture alone. If the system also differs, no difference
    can be put down to either.
  - A pair differing only in the dependency pinset is not like-for-like.
  - Query engine `none` runs no benchmark, records no results and cannot
    show matching answers.
- **Not in the identity**: executor counts and sizes Lakebench derives from
  scale, Trino sizing, scratch storage class and size, datagen pod CPU and
  memory. An executor override below the profile's ask at that scale is
  recorded as a Lakebench cap that bound the run (a condition).
- **Conditions** (not like-for-like when they differ): effective maintenance,
  the compaction operation that ran (Trino `optimize` or Spark
  `rewrite_data_files`), maintenance settings, benchmark iterations and mode,
  the Lakebench caps that bound, and (continuous) the in-stream round count.

Datagen parallelism is not permitted tuning: it is a corpus input
([3.2](generation.md#32-scale-factor)). `datagen.file_size` is fixed at
`64mb`.

## 7.3 Prohibited changes (invalidate a result or are refused)

These change the identity, so the runs are not comparable:

- the workload version (`aml-3`; records at `aml-1` or `aml-2` are not
  comparable with it);
- generator `MODEL_VERSION`;
- workload parameters (`parameters_id` hashes every TM operations setting,
  `w1_max_vertices`, `retention_workload` and `retention_months`);
- mode;
- a different query set or any differing result fingerprint;
- the corpus group: corpus id, seed, corpus role, scale, cycle count above 1,
  and the generator digest when both runs observed one;
- different identity versions (exp1 against exp2);
- a side with no PASSED member (a failed or errored member is excluded from
  its side);
- a `lakebench benchmark` record (`record_kind: benchmark`), never a
  comparable run.

**Corpus id** (`metrics/experiment.py`):

- On an exp2 record it is v2, a hash of:
  - the generator's resolved arguments as each datagen node wrote them in its
    marker (schema, a salted seed reference, cycles, node count, file size,
    delivery mode, `MODEL_VERSION`, Parquet writer settings, scale, corpus
    months, mode, robustness flag, bytes per row);
  - the generator lineage;
  - the cycle count when above 1.
- A config edited after generation does not change it.
- On an exp1 record it is the config hash of schema, generator image, seed,
  corpus role, perturbation, scale, the declared timestamps,
  `dirty_data_ratio` and the Customer 360 `unique_customers`. The AML
  generator never reads `unique_customers`.
- A record is exp2 only when the generator wrote its per-node markers, the
  run started under identity version 2, and part of the system identity was
  observed. Otherwise it is exp1 and lists what was missing in
  `experiment.v2_unavailable`.
- Config load prints a note naming any Customer 360 setting in an AML
  config, which the AML generator does not read.

Also prohibited, not enforced:

- modifying stage scripts, detection rules, queries or the TM simulation
  without a new workload version (a change to a benchmark query's SQL does
  change `query_set_id`, so such runs are not comparable);
- an executor override above 28 (warned, not refused);
- publishing numbers from `lakebench benchmark` (300 s query timeout, not the
  900 s `run` uses) or from `--stage` runs;
- using the calibration or held-out seeds outside the protocol in
  [3.3](seed-policy.md#33-seed-policy).

## 7.4 Lakebench-imposed caps

| Cap | Value | Effect | How a bound cap is reported |
|---|---|---|---|
| Executor ceiling | 28 (`_MAX_EXECUTORS_SAFE`); per AML job: bronze-verify 28, silver-build 28, gold-finalize 28, bronze-ingest 20, silver-stream 28, gold-refresh 28 | per-job executors are the profile's base at scale <= 10, else min(base + int((scale - 10) x per100 // 100), cap) | `experiment.limits.executors[].cap`, `cap_hit`, `override`, `override_bound`; `limits.bound`, `limits.bound_kinds` |
| Continuous concurrent budget | 90% of the cluster CPU (and memory) left after co-resident services, datagen while it runs, and the stream drivers; each executor goes to the stream with the smallest share of its need | fewer executors per stream; a stream below its need cannot balance and the run fails | `limits.executors[].budget_cap` |
| Streaming trigger intervals | back to back by default | freshness is measured at each write, so an interval is not in it | `limits.trigger_bound` (not a bound kind) |
| Sizing cuts to fit the cluster | cluster-derived | smaller resources than requested | `limits.autosize_cuts`, `limits.bound` |
| `w1_max_vertices` | 8,000,000 (`financial.w1_max_vertices`) | W1 skipped `vertex-cap` | `rules.skipped`; `limits.bound`; verdict `rule_caps` |
| W1 giant-component share | 0.5 | W1 skipped `giant-component` | `rules.skipped` only, not `limits.bound`. Recorded at scale 1 and 10 (1.6 batch records, n=1 each) |
| W3 and W17 path budget | executor count x scratch size x 0.7 / 260 bytes per row, or `LB_PATH_SEARCH_MAX_ROWS` when set. Without scratch PVCs each executor counts 20 GiB, too small at scale 10 (the search holds about 267M rows against a 231M budget), so run scale 10 with `platform.storage.scratch.enabled: true` | rule skipped `path-cap`, an allowed skip | `rules.skipped`, `limits.bound`; verdict `rule_caps` |
| W3 and W17 edge cap | 3,000,000,000 flow edges | rule skipped `edge-cap`; not allowed, so the batch run fails | `rules.skipped`, `limits.bound`, `verdict.reasons` |
| Per-alert evidence caps | table below | truncate `related_txn_ids` (and W4's `related_entity_ids`) | `financial_scoring.evidence_capped_alerts_by_rule`, `recall_bounded_by_evidence_cap`, per-typology `bounded_by_evidence_cap` |
| TM `max_alerts_per_customer` | 50,000 | excess alerts dispositioned `over_capacity` | `limits.tm_alerts_over_capacity`, `limits.bound` |
| Pre-benchmark maintenance budget | 1,800 s | remaining maintenance stopped | `limits.maintenance_stopped`, `limits.bound` |
| Continuous [trickle](../../glossary.md#trickle) `max_files_per_trigger` | unset (no limit) with the run's own datagen; derived per run (1 to 50) under `--skip-generate` ([4.3](pipeline.md#43-continuous-mode)); or set in config | when set, it sets the offered load; `sustained_throughput_rps` and `corpus_ingest_ratio` then measure a Lakebench-set arrival rate | `continuous.trickle`, `limits.max_files_per_trigger`, `limits.trickle_bound` and a `trickle:` line in `limits.bound` (never in `bound_kinds`) |
| Continuous drain budget | 1,800 s (300 s under `lakebench stop`) | a [tick](../../glossary.md#tick) that takes longer fails the run | drain problem in `verdict.reasons`; `financial_scoring.status: not_scored` |
| Query timeout | 900 s | query fails, run fails | query error |

`intake_limit` reads `trickle_rate` only when `ingest_ratio` is below 0.95, so
a pipeline that keeps up reports `none`. `limits.trickle_bound` (`value`,
`source`, `kept_pace`) says whether the trickle held intake; when it did,
throughput figures measure the offered load the trickle set, not
infrastructure capacity (`metrics/bounds.py`).

**Per-alert evidence caps** keep one alert row from growing with the corpus.
Lakebench sets them; they are not tuned to any result.

| Rule | List | Cap | Kept |
|---|---|---|---|
| W1_connected_components | `related_txn_ids` | 250,000 | earliest by time |
| W2_structuring, beneficiary kind | `related_txn_ids`, `related_entity_ids` | 1,000 each | first by uetr, first by entity id |
| W4_risk_propagation | `related_txn_ids`, `related_entity_ids` | 1,000 each | first in sorted order |
| W5_sanctions_match, rescreen | `related_txn_ids` | 200 | first by payment time |

- The W2 originator kind and the other rules are not cut.
- W1, W2, W4 and the W5 rescreen record in the alert's `evidence` map the
  full count (`txn_total`, plus `entity_total` for W4) and whether the cap cut
  the list (`txns_truncated`, plus `entities_truncated` for W4). W2's sender
  list has no such flag, and its narrative's sender count is the capped
  count.
- Scoring matches planted payments against `related_txn_ids`, so a cut alert
  can miss planted payments past the cut.
  [8.4](scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails)
  says how a bounded recall is labelled.
