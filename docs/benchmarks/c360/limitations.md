# Customer 360 benchmark: known limitations

## 12. Known limitations

- **Proven on one system type.** Live runs so far are on OpenShift with
  Portworx scratch and FlashBlade S3; the minimum cluster is what `lakebench
  plan` reports.
- **Supported tuples outside the release matrix are unverified.** Customer
  360 is supported only on the cells the
  [support table](../../compatibility-matrix.md#support-states) lists; every
  other combination is unverified ([11](../C360.md#11-supported-compositions)).
  Spark 3.5 is accepted but in no release-matrix row, so its runs are
  unverified.
- **Settings outside the experiment identity** (query-engine sizing, storage
  classes, datagen execution, continuous trigger intervals) leave identities
  equal unless they move the in-stream round count
  ([7.3](execution-rules.md#73-permitted-tuning-still-publishable)). State
  them beside any published comparison.
- **Only 16 of the 34 expected-result checks gate the run**
  ([5.2](correctness.md#52-expected-result-checks-batch-only)). The 16 fail the
  run and its exit code when they fail or do not run; the other 18
  statistical and row-count checks are recorded only.
  - A statistical check at 6 standard errors can fail a correct run by
    chance (the module puts that probability well under 1e-6 per check).
  - Continuous runs have no expected-result checks. Their correctness rests
    on the window gate and fingerprint equality with a reference run.
- **Continuous QpH can be over a smaller query set**: a failed Q9 is tolerated
  in rounds, and rounds over different sets give `query_set_id: blended`, are
  not assessed, and `composite_qph_basis` records the rounds per set.
- **Multi-cycle regenerates every cycle**, even with `--skip-generate`, so it
  cannot reuse an existing corpus; `--generate` is refused. Before cycle 0 the
  run clears the datagen prefix in a bronze bucket this deployment can prove
  it owns. In any other bucket existing objects are refused unless
  `--allow-stale-bronze`; cycle 0 then reads them as this run's data and can
  over-count rows.
- **Lakebench names no winner**; comparing runs is left to the reader
  ([10](comparability.md#10-comparability)).
- **Delta continuous has no effective maintenance**: small files accumulate
  and in-window QpH can decline, so its composite QpH is not a steady-state
  figure. Delta batch never runs OPTIMIZE, and on Spark Thrift no VACUUM.
- **DuckDB runs no maintenance** and reads storage directly: it differs from
  Trino or Thrift in access path (Architecture) and effective maintenance (a
  condition). Two runs that both used `--skip-maintenance` have the same
  effective maintenance.
- **Continuous throughput follows the offered load** (10 MB/s per scale unit
  by default). When the pipeline keeps up, rows per second is that load. It
  is a capacity only when datagen stays ahead with bronze busy
  (`datagen_ahead`, `intake_limit: bronze_capacity`). Under `--skip-generate`
  or an explicit `max_files_per_trigger` the [trickle](../../glossary.md#trickle) bounds it
  (`intake_limit: trickle_rate`).
- **Large continuous corpora may not settle**: when the estimate exceeds
  1,800 s ([4.2](pipeline.md#42-continuous-mode-pipelinemode-continuous-or-run---continuous))
  the result check is skipped without waiting, the run records
  `results.not_checked`, and it cannot show matching answers.
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
  clock; stages that fall back to the 5 s status poll are labelled
  `timing_source: poll`.
- **Repeatability.** A single run is `n=1`; a repeatability claim needs `run
  --repeat` or repeated runs.
