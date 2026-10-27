# Customer 360 benchmark: known limitations

## 12. Known limitations

- **Proven on one system type**: OpenShift with Portworx scratch and
  FlashBlade S3. `lakebench plan` reports the minimum cluster.
- **Supported tuples outside the release matrix are unverified.** Customer
  360 is supported only on the cells the
  [support table](../../compatibility-matrix.md#support-states) lists; every
  other combination is unverified ([11](../C360.md#11-supported-compositions)).
  Spark 3.5 is accepted but in no release-matrix row, so its runs are
  unverified.
- **Settings outside the experiment identity** (query-engine sizing, storage
  classes, datagen execution, continuous trigger intervals) leave identities
  equal; a `gold_refresh_interval` that raises the warmup and interval floor
  enough to change the in-stream round count is a condition
  ([7.3](execution-rules.md#73-permitted-tuning-still-publishable)). State
  these settings beside any published comparison.
- **Only 16 of the 34 expected-result checks gate the run**
  ([5.2](correctness.md#52-expected-result-checks-batch-only)). A gated check
  that fails or does not run fails the run with a non-zero exit; the other 18
  (statistical and row-count checks) are recorded only.
  - The one gated statistical check, `avg_transaction_value_overall`, can
    fail a correct run by chance, with probability well under 1e-6.
  - Continuous runs have no expected-result checks. Their correctness rests
    on the window gate and the last in-stream round's row counts.
- **Continuous QpH can be over a smaller query set**: a failed Q9 is tolerated
  in rounds, and rounds over different sets give `query_set_id: blended`, are
  not assessed, and `composite_qph_basis` records the rounds per set.
- **Multi-cycle generates every cycle** unless `--skip-generate` reuses a
  finished corpus of this config (checked against its series marker);
  `--generate` is refused. Before cycle 0 a non-empty datagen prefix is
  refused unless `--regenerate` clears it (a bronze bucket this deployment
  owns) or `--allow-stale-bronze` writes over it (any other bucket). With
  `--allow-stale-bronze`, cycle 0 reads the old objects as this run's data
  and can over-count rows.
- **Lakebench names no winner**: [10](comparability.md#10-comparability)
  states when two runs compare; the comparison is the reader's.
- **Delta continuous has no effective maintenance**: small files accumulate
  and in-window QpH can decline, so its composite QpH is not a steady-state
  figure. Delta batch never runs OPTIMIZE, and on Spark Thrift no VACUUM.
- **DuckDB runs no maintenance** and reads storage directly. Against Trino
  or Thrift it differs in access path (Architecture) and, unless both runs
  used `--skip-maintenance`, in effective maintenance (a condition).
- **Continuous throughput follows the offered load** (10 MB/s per scale unit
  by default). When the pipeline keeps up, rows per second is that load. It
  is a capacity only when datagen stays ahead with bronze busy
  (`datagen_ahead`, `intake_limit: bronze_capacity`). Under `--skip-generate`
  or an explicit `max_files_per_trigger` the [trickle](../../glossary.md#trickle) bounds it
  (`intake_limit: trickle_rate`).
- **Executor ceiling**: counts do not grow up to scale 10 and stop at the
  per-job cap above it ([7.5](execution-rules.md#75-lakebench-imposed-caps-and-how-a-bound-cap-is-reported)).
  Time to value there is bounded by the profile; the record marks the job
  `cap_hit` under `limits.executors`.
- **Pre-maintenance round only below scale 50**, so `maintenance_value_pct` is
  absent at scale 50 and above, and the settle wait is unverified there.
- **Silver cleaning is partial.** State standardisation maps only CA, TX, NY
  and FL spellings; city standardisation only New York, NYC and LA. The
  Illinois, Arizona and Pennsylvania variants and the injected misspelled
  cities pass through. Email cleaning only lowercases, trims and removes the
  `.duplicate` marker. No query reads these columns; gold `unique_emails`
  depends on `email_clean`.
- **`silver_processing_timestamp` and, in continuous runs,
  `customer_recency_score` depend on when the run happened.** No query
  reads either column.
  - A continuous run without `timestamp_end` resolves its data clock in this
    order: a bronze ConfigMap value left by an earlier batch run (when no
    generate gate or destroy cleared it), `timestamp_start` if set, today.
    The continuous reset writes no data clock.
  - Batch anchors it to the corpus's last day.
- **Multi-cycle time to value spans every cycle**, each cycle's datagen
  subtracted ([8.1](metrics.md#81-batch)); `cycles` above 1 is a Corpus
  identity key.
- **Stage timing** comes from pod and API-server clocks mapped to the host
  clock; stages that fall back to the 5 s status poll read
  `timing_source: poll`.
- **Repeatability.** A single run is `n=1`; a repeatability claim needs `run
  --repeat` or repeated runs.
