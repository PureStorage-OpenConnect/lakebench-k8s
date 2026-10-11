# Changelog

All notable changes to Lakebench are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [1.7.1] - 2026-10-10

Results from 1.7.1 are not comparable with 1.7.0 for the workloads, modes and metrics listed under Breaking. Fixed entries correct numbers or verdicts that 1.7.0 got wrong.

### Breaking

- **AML corpora change.** `datagen-v2-rs-0.4` name pools grow with population, so W5/W6 false matches per watchlist entry stay flat (seed 43, scale 1/10/100: 0.39, 0.35, 0.34; were 0.14, 1.86, 17.2). Every AML corpus changes; registered v1.7 looks keep `datagen-v2-rs-0.3`. [Detail](docs/benchmarks/AML.md)
- **W3 and W17 treat an account as a hub only in weeks** it sends over 200 transfers, not for the whole corpus. Alerts, recall and false positives change in both modes. Workload `aml-3`, rule 1.1.0; `financial reproduce` refuses 1.7.0 alerts.
- **AML continuous runs W5 and W6 every tick**, screening each payment once as it arrives. W5's rescreen stays batch-only; covered scoring leaves the sanctions instances only a rescreen finds out of recall and counts them.
- **Continuous W4 raises one alert per entity per week**, not one per entity over its history. Its counts, recall and false positives differ from batch W4 (unchanged) and from 1.7.0 continuous runs.
- **Continuous stages run back to back by default.** `bronze_trigger_interval`, `silver_trigger_interval` and `gold_refresh_interval` default to `0 seconds` (were 30 s, 60 s, 5 minutes). The `run_duration` floor of 3 x `gold_refresh_interval` applies only with gold on an interval.
- **Continuous datagen runs for the whole window** instead of writing a fixed corpus, at an offered load set by scale: AML 4 MB/s and Customer 360 10 MB/s per scale unit. See [Running Pipelines](docs/running-pipelines.md).
- **Freshness and time to detect run from file landing** in both workloads. AML bronze stamps `ingest_ts` with the file's landing time on the cluster clock, so time to detect includes the wait before bronze takes a file.
- **A file written before bronze started** (`--skip-generate`) counts from when bronze took it. Bronze fails when its object-store clock probe fails three times.
- **Metrics redefined.** Batch `total_elapsed_seconds` is the wall clock (was summed stage seconds). `maintenance_pct_of_pipeline` is maintenance over time to value plus maintenance (it could exceed 100%).
- **Stage GiB come from what each stage read or wrote**, not bucket listings. Datagen falls back to a bronze listing when no pod reported.
- **`total_data_processed_gb`, `pipeline_throughput_gb_per_second` and `compute_efficiency_gb_per_core_hour` change.** They had counted retained snapshots, the bronze table and earlier runs' files (about twice the data in a continuous run).
- **AML continuous `composite_qph`, `in_stream_composite_qph` and `qph_degradation_pct`** use only the rounds that ran the fixed 12-query set. The earlier 8-query rounds stay in `composite_qph_by_set`.
- **Continuous pace** counts a silver batch straddling the window edge by its share inside. Whole-batch counting moved pace up to 25% (AML scale 10, three runs: 18.3, 21.6, 23.4 s per million rows before; 16.7, 16.3, 16.0 after).
- **`query_time_event_age_seconds` left the scorecard.** It is kept under `pipeline_benchmark.diagnostics`.
- **Spark manifests differ from 1.7.0.** Every job sets the Kubernetes client to list executor pods from the API server cache, poll less often above 20 executors, and request pods in bounded waves, so manifest fingerprints change.
- **New default datagen image** `lb-datagen:5d7ce61a`, pinned by digest (`sha256:ed4057e097f09fdd3e37631bc37eb88e5fce561cb8ebe06cd6fa2fd7d23e4bfc`). `lb-datagen:2a36ae21` and `1.6.0` are gone from the registry. Its corpora get a corpus id of their own.
- **A `datagen.parallelism` set in the config is used exactly**, with a warning where the autosizer would have changed it. A continuous run whose datagen pods do not all fit is refused at preflight, naming `datagen.parallelism`.
- **`lakebench compare` is gone.** It is now an unknown command. Compare runs from their reports.
- **`lakebench reproduce` is gone.** It is now an unknown command (exit 2), with no replacement. Exit code 14 is reserved: no command produces it, and it is never reused. `lakebench financial reproduce` is unchanged. `datagen_aggregate_mbps`, `datagen_mbps_per_pod` and `datagen_cpu_hr_per_tb`, which only `reproduce` derived and no record carries, leave the metric registry.
- **`deploy --require-new` refusals name the exit path `deploy.existing_namespace`** (was `reproduce.existing_namespace`), still exit 3.
- **A bare `"0"` trigger interval is refused at load.** The three continuous intervals need a whole number and a unit (`"0 seconds"`, `"5 minutes"`). Before, `"0"` went to Spark unchanged for Customer 360 and became 10 s for AML.
- **Result fingerprints and the continuous result check are gone.** Batch runs no longer execute each benchmark query a second time, untimed, after the timed samples; per-query times and QpH are unchanged, and batch `total_elapsed_seconds` loses that pass. Continuous runs no longer wait for silver and gold to settle after the window: the streams stop at window end (AML gold-refresh still finishes its tick first). Continuous `total_elapsed_seconds` loses the settle phase, and the storage multiple is measured before silver and gold catch up with bronze, so neither compares with 1.7.0. `experiment.results.fingerprints`, `continuous.result_check` and `continuous.settle` are no longer written, and the `result_check` verdict gate is gone; older records keep the fields and load. Lakebench does not compare query answers: compare the query set id, the AML alert set and the reports' row counts. See [Comparing runs](docs/benchmarking/comparing.md).
- **A continuous Customer 360 run with a benchmark fails when no in-stream round ran, or Q9 failed in every round** (`c360 continuous gate: ...`): the rounds are its only query checks (failed queries other than a tolerated Q9, and empty answers in the last round). Continuous `experiment.results.query_set_id` names every query the rounds ran (AML: the 12-query set once a round ran the investigator queries). AML continuous records from 1.7.0 stored none, so they do not compare with 1.7.1 ones.
- **`lakebench.modules` no longer exports `CatalogModule`, `QueryEngineModule`, `PipelineEngineModule`, `TableFormatModule` or `ModuleRegistry`.** `modules/base.py` and `modules/registry.py` are removed; nothing in Lakebench used them.

### New

- **Continuous balance check.** Each handoff's lag (datagen to bronze, bronze to silver, silver to gold) is reported every two minutes. An unbalanced run fails with a bottleneck line naming the stage and the setting to change (`continuous.balance.lever`). See [Running Pipelines](docs/running-pipelines.md).
- **Capacity and steady-state runs.** A continuous record says which it was: `datagen_ahead`, `backlog_rows`, `pace_seconds_per_million_rows` and `bronze_pace_seconds_per_million_rows`. A backlog with bronze at capacity no longer fails the verdict; one with bronze idle still does.
- **`stage_capacity`** in a continuous record: each stage's busy share and MB/s per core at full busy, and datagen's MB/s per core. `offered_load` gives the declared and sized MB/s; the Balance card says when datagen fell short.
- **Gold freshness p50, p95 and max** (`continuous.freshness`) in the record and report. Peak gold freshness is no longer a gate: it was divided by wall clock, so any fixed share of the window failed a slow gold cadence.
- **Wider streaming executors.** A stream that needs more than its profile's `max_executors` grows executors to 8, then 16 cores. `platform.compute.spark.{bronze_ingest,silver_stream,gold_refresh}_executor_cores` set the cores exactly. The record keeps each shape (`config_snapshot.spark.streaming_shape`).
- **Storage growth.** The report gives a continuous run's raw datagen growth per hour and the space a 24-hour run needs. The raw files are kept.
- **Trigger-bound label.** A continuous stream on a trigger interval set in the config is labelled beside freshness (`experiment.limits.trigger_bound`). A trickle's 30 s bronze cadence is printed when it replaces back to back.

### Changed

- **Support label:** a run of one of the 15 combinations this release was validated on (C360 and AML, batch and continuous) is now stamped "supported" and names the validating runs. The validation record no longer takes a commit hash.
- **Continuous minimum cluster** (`config show`, `recommend`, preflight, README) is the smallest cluster that gives every stream all its executors. The old figure was the request alone (AML scale 10: 168 cores, silver got 12 of 19 executors).
- **When the cluster cannot hold every stream**, each executor goes to the stream with the smallest share of its need. The old split by profile size gave AML gold-refresh 22 executors and silver 2 (scale 100, 300 cores).
- **`run --continuous` sizes and records as continuous.** A batch config run this way sizes for continuous (at scale 50+ it no longer turns scratch on) and records the continuous settings, naming the command line as the mode's source.
- **`run --continuous` starts datagen once the streams run**, opens the window at its first data file and stops it with the `_corpus/stop` marker. AML bronze is created from the generator's schema; bronze-ingest exits 2 on a mismatch.
- **Continuous bronze has no per-trigger limit** when the run starts its own datagen. `max_files_per_trigger` is unset by default and labelled as a Lakebench cap when set. Trickle rules apply only with `--skip-generate`.
- **`silver_bronze_wait_seconds` auto** is `run_duration` / 4, at least 600 s (was 10 s). The wait now runs before the window opens.
- **Batch executor scratch PVCs scale with the data.** Each executor gets its share of the stage's per-scale need, between 50Gi and the old size (300Gi silver and gold, 500Gi AML bronze). Scale 1 batch: 400 Gi, was 2,400 Gi. [Spark](docs/component-spark.md#batch-jobs)
- **Datagen pods run one generator thread per started core** (`CPU_LIMIT`), so a fractional pod (1300m) is no longer held to one thread.
- **AML continuous gold re-detects only around new rows.** Each rule recomputes the days its new rows touch, widened by its windows; the result equals a full recompute. Cost follows the arrival rate, not run length. Passes log `[incremental]`.
- **AML continuous `detected_ts`** is the first pass that wrote an alert's content, not the last rewrite.
- **AML continuous silver** keeps `distinct_counterparties_out/in` from a new table, `silver.counterparty_pairs` (`architecture.tables.silver_counterparty_pairs`), so a batch's cost no longer grows with run length. Counts stay exact; batch-stream parity holds.
- **Customer 360 continuous gold streams silver** commits since its checkpoint, at one pinned snapshot (Delta: version), and recomputes only the dates touched. A restart resumes from the checkpoint. Bronze keeps each row's arrival as `ingest_ts`.

### Fixed

- **Stage times no longer include Lakebench's own bookkeeping.** AML gold ran a count per rule inside each rule's time and its continuous time to detect; C360 gold recomputed the gold aggregate to count it; AML bronze verify read the data seven times and C360 bronze verify twice more for output nobody read. Each is gone or one pass.
- **Digest-pinned Spark images** (`apache/spark:4.1.1-python3@sha256:...`) are accepted; the version came from the digest and the config was refused.
- **A finished command no longer hangs at exit.** After a continuous run wrote its record, Python could wait forever on a logging lock, so a script's next step (`destroy`) never ran.
- **Observability numbers** ([Observability](docs/component-observability.md)). Trino counts the run's queries. S3 requests and latency, platform CPU and memory, bucket sizes and the continuous S3 object count read `not collected` (`null`), not 0. An interrupted run with observability on says metrics were skipped.
- **Observability report and dashboards.** Platform Metrics always appears, with the reason when empty. No Spark engine metrics are claimed (the Spark UI is off). Grafana Trino panels use lowercase metric names; CPU counts containers once. JMX exporter pinned by digest.
- **`deploy` no longer fails at Hive** with a 409 AlreadyExists on `lakebench-hive-metastore` when the Stackable operator creates that Service first; it updates the Service instead.
- **Unknown GiB read "not measured"**, not 0.00 GiB, for a continuous run without a datagen fleet record. An older record re-renders the AML composite it recorded.
- **In-stream query rounds** aggregate every round with a QpH, not the first round's samples.
- **The query stage names the benchmark's engine** (it read "trino" on DuckDB and Spark Thrift runs).
- **Binding caps** name a continuous run whose own datagen set its intake, and an AML typology whose recall the evidence cap bounds. Both read "none" before.
- **Metric descriptions** say freshness is sampled at each gold refresh, `total_rows_processed` counts gold's re-reads, `ingest_ratio` is an estimate, and core-hours count requested executor cores only.
- **Delta + Hive continuous after a failed batch build** no longer fails on a schema mismatch. The reset now clears unregistered silver and gold table directories in the deployment's own buckets.
- **Long continuous runs keep their driver logs.** Logs are read from a copy kept since pod start, so kubelet rotation no longer cuts ticks gates and scoring read. A gold log missing its first tick fails as "driver log incomplete".
- **AML continuous scoring is no longer skipped** with "snapshot unknown at the last completed tick" when the gold log rotated during the drain tick: the drained driver repeats the tick's records with each drain line.
- **Iceberg Customer 360 bronze logs rows after the commit**, so the rows of a batch stopped mid-write leave bronze's window rows and rows/s.
- **A batch AML run whose score job failed** or refused the corpus now fails with the reason and records `status: not_scored`. It passed with no recall.
- **AML continuous gold's busy share and cadence** count the whole tick, including time to detect and the TM pass. The Balance card understated it.
- **`run --continuous --duration N`** sizes the auto silver wait from N, not the config's `run_duration`.
- **Continuous verdicts name each failed gate.** A gate failure no longer marks every stream failed or adds "Pipeline crashed or was interrupted". A recipe without a query engine says so instead of blaming `--skip-benchmark`.
- **`run` with no corpus** stops before any stage with exit 4 (`run.no_corpus`) and names `--generate`. It failed later in bronze-verify as "crashed".
- **AML batch maintenance** skips `silver.counterparty_pairs`, which only continuous silver writes. Its statements failed with "Table does not exist".
- **The AML continuous rules gate** fails a run when a rule skipped or failed on the scored tick. It and the record's rule list read the scorer's detection status, or without scoring the drain tick's `continuous.ticks[].rule_status`.
- **A JVM log line appended to a rule's `[detection]` line** no longer makes the AML rules gate read that rule as not run (the parser required `elapsed=Ns` at the line's end).
- **The AML continuous gate reads the drain tick's log** for alert counts and TM invariants. It had read the log captured before the drain, printing an earlier tick's count and missing the drain cycle's TM checks.
- **AML continuous late alerts** (`time_to_detect_late_alerts`): an alert is late only when every related transaction was in a silver batch the previous pass read.
- **The continuous capacity preflight** no longer refuses a cluster and admits a smaller one. Stream floors come from the shared budget, which also respects memory. At scale 100 the verdict flipped over a hundred times between 304 and 697 cores.
- **AML continuous datagen** builds the bank once per pod (every file stays byte-identical) and never starts a period after the stop marker. Needs the new default image.
- **An AML continuous window ending in the first 24-month period** records datagen totals when the process wrote all of that period (the ingest ratio read unmeasurable). A restarted pod records none, so totals are never undercounted. Needs the new image.
- **The continuous window opens on datagen's first data file**, not the AML bank's manifest, party and account files. It warns when none arrives within 300 s.
- **A Customer 360 continuous refusal** for a time slice of 30 minutes or less names the smallest pod count that runs.
- **A batch run after AML continuous** can build silver again: `run --continuous` clears the stream-active marker after a clean stop, once the driver pod is gone. A run whose streams had a window problem keeps it.
- **DuckDB on AML gets its 16g auto-size.** The DuckDB recipes set `memory: 4g`, which blocked it and got the pod OOMKilled in AML continuous benchmark rounds. They now leave DuckDB resources to the defaults and the autosizer.
- **A failed batch job prints the driver's exception line** (`Cause: ...`) before the log tail.
- **The gold event-age probe** says the table is empty for a NULL result, instead of saying it could not read the output.
- **AML scoring of a continuous run** reads the `-eNNNN` epoch ids datagen writes after the first 24-month period, instead of refusing the corpus. A held-out seed behind any epoch is still found.
- **A steady-state continuous run's ingest ratio** no longer fails on bronze's trigger cadence: `released_rows` counts what datagen had written one bronze cadence before the window's end (a Customer 360 scale-1 run that kept up read 0.94).
- **AML continuous compaction** skips the tables silver-stream MERGEs into every batch (entities, accounts, entity_profiles, silver_batch_versions). A rewrite under the MERGE ended the stream ("Missing required files to delete").
- **An ingest-ratio failure with bronze idle** says "bronze took in less than arrived without being at capacity", not "pipeline saturated".
- **Run records no longer store an object-store address** quoted in an error: an IP-literal host is replaced by the snapshot's endpoint hash.
- **A continuous capacity run** (`datagen_ahead`) warns, not fails, on gold staleness above half the window.
- **A capacity run with `intake_limit: bronze_capacity`** no longer fails its ingest ratio: the stored record had dropped `datagen_ahead`, `backlog_rows` and both paces, so the recomputed verdict never saw them.
- **The 1.7.0 notes named `scripts/aml_stage_attribution.py`**, an event-log fallback for the gold-finalize stage profile. It never shipped: the profile comes only from the driver's status store.

## [1.7.0] - 2026-10-06

### Breaking changes
One line per breaking change, from docs/upgrading/breaking-1.7.yaml; UPGRADING-1.7.md says what to do about each. The entries after them give the detail.

- Twenty config keys nothing read are removed: at its 1.6 default each loads with a note; another value is refused by the commands that change data.
- `platform.compute.spark.driver`, `.executor` and `platform.storage.scratch.size` sized nothing: set to other than their 1.6 default, they are refused.
- A removed config key is refused by the commands that change data; read and teardown commands drop it with a note.
- `lakebench results` is an alias of `report --format table` that prints one line on stderr; it is removed in v1.8.
- `admin install-spark-operator` and `admin install-scratch-storage-class` are aliases of `admin install --component`, removed in v1.8.
- `init --interactive`, `-i` and `--advanced` print one line and write the default config: the wizard is removed.
- `run --sustained` is a hidden, deprecated alias of `--continuous` and prints a warning.
- `recommend --extended` / `-e` is a deprecated alias of `--slow-datagen`, which is now ignored.
- `lakebench config upgrade` exits 2 before opening any file: it rewrote configs lossily and wrote secrets in plaintext.
- `clean bronze` and `clean data` are refused (exit 2): a run regenerates its own corpus.
- `clean metrics`, `clean journal` and `clean --metrics-dir` are refused (exit 2): records and journals are evidence.
- `lakebench compare` exits 2 with any arguments: comparing runs is left to the reader, and each report states the corpus, components, result fingerprints and caps needed to judge a comparison.
- `init --access-key` and `--secret-key` exit 2 without echoing the value; init writes `${VAR}` references.
- `generate --wait` / `-w`, `admin release-lock --expired-only` and `deploy --include-observability` are unknown options (exit 2).
- The run-from-a-checkout wrapper `lbrun.py` is removed.
- `status` exits 1 on drift or a missing namespace, `stop` and `logs` exit 1 on a failure, and API errors exit 4 (all were 0).
- A config that does not load, an unsupported combination, a bad argument or a nameless config exits 2 (was 1 or 0).
- Ownership, redeploy, non-empty bronze and held-lease refusals exit 3 (was 1 or 2).
- A failed preflight check and an unreachable Kubernetes API or S3 bucket exit 4 (was 1 or 2).
- A declined or unanswerable confirmation exits 5 (was 1 or 3).
- `destroy` exits 6 (was 4) when its steps finished but the namespace is still terminating.
- `reproduce` exits 14 (was 2) for metric drift or commit drift without `--allow-commit-drift`.
- A `run` whose datagen did not finish in time exits 1 (was 5); the record says "datagen timed out".
- Customer 360 gold is never silently incremental and a multi-cycle run takes one data clock; records carry workload version `c360-2.dev1` and do not compare with `c360-1`.
- AML alert evidence is capped at 1,000 ids per W4 alert and flagged; records carry workload version `aml-2` and do not compare with `aml-1`.
- Experiment identity v2: the system and the query access path are architecture and system groups, no longer conditions that make a pair not like-for-like.
- A continuous record without a stored round count reads it from its rounds; a stored C360 Trino vs Thrift pair is now not like-for-like.
- The perf gate (`scripts/perf_gate.py`, `benchmarks/perf/`, the baseline store) is removed.
- A new deployment generates its own Polaris client secret and database passwords; 1.6 used fixed values for every install.
- Jobs take every jar and wheel from the deployment's dependency server; `run` on a deployment made by 1.6 exits 4.
- `stop` on an AML deployment waits up to 300 s for gold-refresh to finish its detection tick before it deletes the jobs; a continuous AML run ends with the same drain (up to 1800 s) and a score job, and fails when the drain times out.
- A continuous AML run that passed its gates ends with one more Spark job after the score job: it re-reads every transactions snapshot the detection ticks recorded (two full scans of each that is still live, and one of the current snapshot), bounded by the per-job timeout; its check is reported beside the verdict and never fails the run.
- `financial reproduce` reproduces the alert from the snapshots its run's gold read, which runs record from 1.7 on: exit 0 when reproduced, 1 when not reproduced or not found, 2 when this host has no record of the run, 4 when those snapshots are gone or the run predates 1.7; 1.6 exited 1 after every reproduction it waited for (it could not reproduce), and 0 after a submit with `--no-wait`, which now refuses first when the record cannot drive a reproduction.
- An AML run over a corpus with no manifest (batch, continuous with `--skip-generate`, or a `run --stage` subset), or over a bucket that holds a corpus from a held-out or spent seed (such as 42), stops at bronze-verify with exit 2; 1.6 only warned about a missing manifest and refused a spent corpus only at reference scoring.
- `run`, `benchmark`, `query`, `reproduce` and the `financial` commands refuse an evaluation or robustness AML corpus, by role or by seed, with exit 2, before any cluster call.
- Executor overrides take 1 to 28 (`driver_cores` 1 to 16), count in the capacity check, and keep a run out of release evidence.
- `benchmark` saves a record of its own (`record_kind: benchmark`) instead of rewriting the run's; `query` writes no record.
- `run` exits 2 before any cluster call on a flag its mode does not use (the list is under `run` in docs/cli-reference.md).
- `reproduce` refuses (exit 3) an existing namespace or bucket instead of destroying it, and destroys only what it created.
- `init` writes a 12-line config: a new name per `init`, recipe `polaris-iceberg-spark-trino` (was Hive), scale 1 (was 10), `${VAR}` credentials.
- A catalog, format or engine that contradicts `recipe:` is refused at load by the commands that change data; 1.6 let it win silently.
- `${VAR}` is substituted per value, not in the file text: an environment value is no longer parsed as YAML.
- A config with no `recipe:`, or `recipe: default`, loads with a note; v1.8 requires `recipe:`.
- Flat top-level keys (`endpoint:`, `scale:` and the rest) load with a note naming the nested key.
- `deploy` only checks the scratch StorageClass, Spark Operator, Stackable and observability stack; `operator.install: true` is refused.
- `admin install` installs only what is missing and refuses a version change (exit 2, or 3 with `--allow-version-change`).
- `deploy`, `run` and `destroy` edit the Spark Operator watch list on the installed chart, or refuse when it cannot be read.
- `admin doctor` runs the prerequisite checks and exits 1 when one fails or cannot run.
- A config with no `name:` is refused by the commands that change data, and reads or tears down only a deployment it can prove is its own.
- A zero, negative or out-of-range count (Trino workers, generators, ports, cores), which 1.6 accepted, is refused.
- `spark.conf` merges over seven job defaults, and keys Lakebench sets (including `userClassPathFirst` and `spark.kubernetes.*`) are refused.
- `run` refuses a config whose benchmark sets `mode: throughput|composite`, `cache: cold` or `streams` above 1.
- A `driver_memory` Spark cannot read (`16Gi`, `1.5g`) and a `spark.lb.gold.strategy` other than `auto`, `simple_agg` or `two_phase_agg` are refused by the commands that change data.
- Deploy stamps owned buckets with the cluster; a bucket 1.6 adopted is used but no longer emptied or deleted by `destroy`.
- `run` compares its request with free capacity, not allocatable, and refuses (exit 4) when nodes or pods cannot be read.
- A Customer 360 batch verdict fails on sixteen exact checks only, including when they cannot be evaluated; others are listed, not gating.
- A new shared observability install gets a generated Grafana password; an existing install keeps `admin`/`lakebench`.
- `metrics.json` `config_snapshot` drops `spark.driver` and `spark.executor` and replaces `scratch.size` with `scratch.size_per_job`.
- The Hive recipes now default to Spark 4.1.1. A config that does not set `images.spark` runs Spark 4.1.1 (and Delta 4.1.0) where v1.6 ran 4.0.2. Its jars, dependency set and perf fingerprint change, and a deployment made from it must be redeployed before run.
- A PASSED verdict also needs rows in every layer, the expected AML rules (W1 giant-component or vertex-cap and W3 or W17 path-cap allowed), a batch scale ratio of at least 0.95 and no empty answer; `run` exits 1 when its record does not read PASSED.
- `report` reads the stricter of a record's stored verdict and the one recomputed from it: three stored AML batch records without a watchlist now read FAILED, and their Hive-versus-Polaris pair is not comparable.
- The 1.7 datagen image (pinned before the release) exits 2 on an unknown, repeated, valueless or unparseable flag, a stray argument, a non-finite float or a Customer 360 `--cycle` without `--cycles`; 1.6 dropped them or used a default.
- Building the datagen image needs `--build-arg LB_BUILD_COMMIT=<commit>`; a plain `podman build` of `datagen_rs/` now fails.
- Datagen pods on the 1.7 image honour `platform.storage.s3.path_style`, `verify_ssl` and `ca_cert`, which 1.6 ignored (path-style, plain HTTP and the system CAs always); a value they cannot read exits 2, and with `ca_cert` set datagen trusts only the CAs in that file.
- A run that reuses bronze exits 3 when its corpus series marker is unfinished or made for another cycle count, window or generation, or is missing on a multi-cycle config or over later cycles' files (4 when bronze cannot be read); a multi-cycle run over a non-empty datagen prefix exits 3 without `--regenerate`; `generate` or `run --generate-only` on a multi-cycle config and `run --skip-generate` on a multi-cycle AML config exit 2.
- The default datagen image is `lb-datagen:2a36ae21`, pinned by digest: the v1.7 look image. A config that does not set `images.datagen` generates with it where v1.6 used `lb-datagen:1.6.0`; its output on the five byte-compare cases is byte-identical to 1.6.0, and the lineage table maps it to the 1.6.0 root.

- **The Hive recipes default to Spark 4.1.1.** Hive-based recipes take `apache/spark:4.1.1-python3`; Polaris recipes stay on 4.0.2. Pin `images.spark: apache/spark:4.0.2-python3` to keep the old image. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **The verdict is decided from the record.** A PASSED verdict now requires rows in every layer, the expected AML rules, a batch scale ratio of at least 0.95, and no empty query answer. `run` exits 1 when the verdict is not PASSED; `report` takes the stricter of stored and recomputed verdicts. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **Multi-cycle runs and reused corpora check a corpus series marker.** Every generate writes `_corpus/series.json`; a run reusing bronze checks the marker and exits 3 on a mismatch or incomplete generate. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **Executor overrides are bounded, counted and kept out of evidence.** Executors take 1 to 28 and `driver_cores` 1 to 16; larger values are refused. Overrides enter the capacity check and the experiment identity. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`run` needs a 1.7 deploy.** Jobs take jars from the deployment's dependency server; a 1.6 deployment exits 4. Redeploy once after upgrading. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`clean bronze`, `clean data`, `clean metrics` and `clean journal` are refused** (exit 2). Use `lakebench run CONFIG --generate --regenerate` for bronze and `clean silver`/`clean gold` for data. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **Renamed commands print one line and are removed in v1.8.** `results`, `admin install-spark-operator` and `admin install-scratch-storage-class` are hidden aliases of their replacements. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`benchmark` and `query` no longer write into a run's record.** `benchmark` saves a record of its own; `query` prints its result and no longer appends it. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`run` refuses arguments it used to ignore, before any cluster call.** Conflicting or mode-inappropriate flags exit 2 before contacting the cluster. The full list is under `run` in docs/cli-reference.md. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`reproduce` no longer destroys before its run.** It refuses an existing namespace or bucket (exit 3). Run `lakebench destroy CONFIG` first. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **Continuous runs recorded before the round count was stored compare on their rounds.** A record with no stored `limits.benchmark_rounds` reads it from its rounds, so a pair with different round counts now reads not like-for-like.
- **`init` writes a first-day config, and the wizard is removed.** A 12-line config with a unique name, `recipe:`, scale 1, and `${VAR}` credentials. The interactive wizard and `--access-key`/`--secret-key` are gone. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **A component that contradicts its recipe is refused at load.** A catalog, format or engine that differs from the recipe fails commands that change data. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`${VAR}` is substituted per value, not in the file text.** Environment values are no longer parsed as YAML, so secrets with special characters work. Two edge cases fail: unclosed defaults and references inside flow syntax. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **A config with no `recipe:`, or `recipe: default`, is deprecated.** It still resolves as before and loads with a note. v1.8 requires `recipe:`.
- **Deploy never installs a shared component; `lakebench admin install --component` does.** Shared components (scratch StorageClass, Spark Operator, Stackable, observability) are installed once by a cluster admin. `deploy` only checks them. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`admin install` never changes an installed component.** It installs what is missing with `helm install` (never an upgrade). A version change is refused unless `--allow-version-change` is given. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`admin install-spark-operator` and `admin install-scratch-storage-class` are aliases** of `admin install --component`. `install-spark-operator --version` no longer upgrades an installed operator.
- **The watch-list edits never fall back to the config's operator version.** `deploy`, `run` and `destroy` pin the installed chart, read inside the cluster lease.
- **`admin doctor` runs the prerequisite checks** of `docs/prerequisites.md` for the shared components and exits 1 when one fails.
- **A config needs a `name:` to change data.** Commands that change data refuse a nameless config. A v1.6 directory needs `name:` added to the config that deployed it. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **Removed config keys are refused by the commands that change data,** with what to do instead. Read-only commands list dropped keys in an "Upgrade notes" block.
- **Counts are bounded at load.** Zero, negative or out-of-range counts for replicas, cores and generators are refused.
- **Settings that were recorded but not honoured are refused.** `spark.driver`, `spark.executor` and `scratch.size` sized nothing; they are removed. Use `<job>_executors`, `driver_memory` or `driver_cores` instead. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **Config fields nothing read are removed.** Twenty config keys (images, observability flags, properties, and others) that had no consumer are removed; a key at its v1.6 default loads with a note. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`spark.conf` merges over the job defaults; keys Lakebench sets are refused.** `spark.conf` now holds your own keys only; each job starts from proven defaults. Keys Lakebench or its scripts set are refused. See [UPGRADING-1.7.md](UPGRADING-1.7.md) for migration steps.
- **`operator.install: true` is refused.** Shared operators are installed with `admin install`; `deploy` only checks them.
- **`lakebench run` refuses benchmark settings it does not run.** A config with `mode: throughput|composite`, `cache: cold` or `streams` above 1 is refused by `run`; use `lakebench benchmark` with those flags.
- **Flat top-level config keys are deprecated.** They still load with a note naming the nested key to write.

### Added
- **AML benchmark specification.** `docs/benchmarks/AML.md` publishes the AML workload's data model, seed policy, pipeline, detection rules, correctness contract, query set, metrics and scoring rules.
- **`scripts/gen_docs.py`** regenerates every generated docs block in one command; `--check` exits 1 when any is stale.
- **`LB_EXIT_PATH_FILE`.** When set, `lakebench` appends `<code> <path>...` to that file as it exits, so scripts can tell refusals apart without reading message text.
- **AML batch runs record their alert set.** Gold-finalize fingerprints alerts over `(rule_id, entity_id, alert_ts)`. Two runs whose alert sets differ are not comparable. See [aml-scoring.md](docs/aml-scoring.md#the-alert-set-are-two-runs-alerts-the-same).
- **W5/W6 non-planted alerts per customer, by scale.** AML scoring records diagnostic counts per rule and scale. `scripts/aml_screen_rates.py` computes screening rates from stored records.
- **Requested and effective values.** Each run records what it asked for against what it did (gold strategy, pipeline mode, executors, trickle). A mismatch is labelled in the verdict but never fails the run.
- **`architecture.benchmark.investigator_sessions` (AML continuous).** Optional key (1 to 32) for concurrent investigator sessions on an AML continuous run. The round never counts as a benchmark round and never fails the run. See [aml-scoring.md](docs/aml-scoring.md).
- **AML continuous runs drain the last detection tick and score `recall_covered`.** At window end gold-refresh finishes its tick, then recall is scored over covered instances. See [aml-scoring.md](docs/aml-scoring.md#continuous-recall-over-covered-instances).
- **AML continuous runs re-read the snapshots their ticks read.** A post-score time-travel job verifies snapshot integrity and records the result beside the run verdict. The check never fails the run.
- **Each AML continuous tick records its transactions snapshot for time travel.** Metadata only; tick timings, time to detect and freshness are unchanged.
- **`init --from OLD -o NEW` converts a 1.6 config.** Keeps the deployment's name, moves keys to current locations, replaces plaintext credentials with `${VAR}` references, and lists what `run` still refuses.
- **Every AML alert carries reason codes.** `gold.alerts` gains `reason_codes`; batch scoring adds per-code recall, false positives and alert counts. See [aml-scoring.md](docs/aml-scoring.md#reason-codes).
- **AML batch records attribute gold-finalize time and show stage headroom.** Diagnostics: slowest rule, heaviest Spark stage, and headroom against timeouts. See [aml-scoring.md](docs/aml-scoring.md#where-gold-finalize-spends-its-time).
- **AML gold-finalize records where its time goes.** Per-rule elapsed seconds, stage profile and TM operations phases. No alert or score changes. See [aml-scoring.md](docs/aml-scoring.md#where-gold-finalize-spends-its-time).
- **Storage multiple.** Every run measures physical bytes over the current snapshot per table, per layer and in total, under the maintenance policy that ran. Recorded as `storage_multiple` and shown on the HTML report.
- **Customer 360 results on the HTML report.** The report shows the 34 C360 expected-results checks: passed, failed and gating status.
- **`--json` on the read verbs.** `plan`, `status`, `report`, `config recipes` and `query` write a structured `lb-cli/1` JSON document to stdout.
- **`docs/cli-reference.md` is generated from the CLI.** A unit test fails when the reference drifts from the code.
- **`report` absorbs `results`.** `report [RUN|CONFIG]` takes a run id or a config; `results` is an alias of `report --format table`.
- **`financial reproduce` reproduces a real batch alert.** Reruns the alert's rule on the snapshots its run read: exit 0 reproduced, 1 not reproduced or not found, 4 when snapshots are gone.
- **A protected AML corpus is never read or scored outside its registered look.** Commands refuse a config whose seed hashes to a held-out seed (exit 2 before any cluster call). Teardown and read commands skip that check.
- **`lakebench generate --registered-corpus`** generates the registered evaluation or robustness corpus. It is the only command that takes a protected config. The attempt is ledgered before any cluster call.
- **bronze-verify refuses a corpus from a held-out or spent AML seed** before it reads or writes anything, so a development config pointed at a registered corpus never reaches silver.
- **A registered look scores only the corpus `generate --registered-corpus` wrote.** `scripts/aml_gate.py --registered` verifies the corpus fingerprint against the ledger.
- **`scripts/aml_heldout_audit.py`** (maintainers) lists every protected-role scored run on this host. Read-only; prints no seed or key.
- **Each deployment gets a dependency server.** `deploy` runs a `deps` step: a `lb-deps` Deployment that resolves jars once and serves them read-only. A cold resolve adds one to a few minutes to the first deploy.
- New optional config block `platform.deps` for Maven, PyPI and DuckDB mirrors on clusters without public egress.
- Two new entries on `docs/prerequisites.md`: `deps-storage-class` and `egress-hosts`.
- **BOUNDED BY trickle.** A continuous run whose trickle held intake is labelled; the report shows rows/s and GB/s as the offered load, not capacity.
- **Compaction by engine, and blended in-stream QpH.** Each run records its compaction operation and parameters; runs with different engines are not like-for-like. See `docs/benchmarking.md`.
- **`lakebench plan CONFIG...`**, read-only: shows the components, recipe, support state, minimum cluster, prerequisites and free capacity. `--offline` makes no cluster call.
- **Run provenance is complete.** `metrics.json` records how lakebench was installed, the config's sha256, image digests, dependency set and scratch PVCs. See `docs/benchmarking.md`.
- **`run --repeat N`: one series of batch runs over one corpus.** Repetitions 2 to N rebuild silver and gold from the same bronze. Records carry `series {id, index, size}`.
- `[aml]` install extra (`pip install "lakebench-k8s[aml]"`) for the AML reference detector and local gate.
- **A nameless config tears down or reads only a deployment it can prove is its own.** `destroy`, `stop`, `status` and `logs` take `--name` and verify a nonce before acting.
- **Corpus id v2.** Every run records its corpus observation and gains `experiment.corpus.id_v2`, hashed from the generator's resolved arguments and image lineage.
- **`docs/prerequisites.md` is generated** from the prerequisite checks, so the page and the checks cannot drift.
- **Each datagen pod writes a corpus marker when it finishes.** The marker records files, rows, bytes, build commit and `corpus_args` with their sha256.

### Changed
- The package version is 1.7.0.dev0 until the release commit sets 1.7.0, so development records stamp lakebench 1.7.0.dev0 instead of 1.6.0.
- User documentation review: dropped the fabricated README scorecard sample
  (described the outcome instead), rewrote the "v1.7 is coming" framing in
  `aml-scoring.md`, `getting-started.md`, `configuration.md` and
  `financial-benchmark-baselines.md`, renamed `pipeline.sustained.run_duration`
  to `pipeline.continuous.run_duration` in `data-generation.md`, fixed the
  stale 200Gi scratch comment in `examples/polaris-iceberg-spark-financial.yaml`
  (silver-build is 300Gi; the financial profile bumps bronze-verify to 500Gi),
  dropped `unity` from the `architecture.catalog.type` enum comment in
  `component-hive.md` (schema still accepts it; no recipe uses it), rebuilt
  the `docs/README.md` index to cover `DESIGN.md`, `compatibility-matrix.md`,
  `storage-backends.md`, `financial-benchmark-baselines.md`,
  `perf-regression-gate.md`, `deep-dive/`, `design/`, `reproductions/` and
  `upgrading/`, moved `operators-and-catalogs.md` to component reference,
  and tightened the legacy-package section in `docs/reproductions/README.md`
  plus minor weasel-word nits.
- **FQ4 and IQ3 give one answer per corpus in batch and continuous.**
  Continuous AML stores edge rows per pair per micro-batch and statement
  running balances in arrival order, so FQ4 (which returned the stored
  `bal_after`) and IQ3's second hop (which returned raw edge rows) answered
  differently from batch, and between continuous runs, on one corpus. FQ4
  now recomputes the running balance in ledger order (book time, transaction
  id, debit first) from each account's opening balance, and IQ3 sums its
  second hop per pair as it already did the first. Batch answers are
  unchanged row for row; continuous answers over a settled corpus equal
  them. FQ4 still reads the statements once (the opening balance is two
  window aggregates over the same rows). The SQL change moves the AML
  query-set ids: the 12-query set is now `qs12-910d16a91962` (was
  `qs12-4bd2d9416abb`), FQ1 to FQ8 `qs8-ffe2bc1a012e` (was
  `qs8-32f521a57551`) and IQ1 to IQ4 `qs4-bc3b5e556bf7`, so QpH from before
  the change is not compared with QpH after it; workload version `aml-2`
  covers it. Batch and continuous records are still never
  compared with each other (the mode is a workload identity key).
- **`RELEASING.md` and `make release-check`.** One release process: the
  scripted steps run in order with `make release-check VERSION=X.Y.Z`
  (`DRY=1` for the dry run, `make rc-<step>` for one step), and the
  owner-only steps are a checklist. It replaces `docs/releasing.md`.
- **AML `scale_ratio` divides by the measured pacs.008 size.** The expected
  bronze per scale unit was a flat 8.4 GB (scale 1), so complete scale-10 and
  scale-100 AML batch runs read 1.114 and 1.118 and the perf gate (1.10)
  refused them as "more data than the scale". The size is now the bytes
  bronze-verify read at scales 1 and 10 (8.47 and 93.6 GB; bytes per row
  grow between them), interpolated between them and held at the scale-10
  value above; the scale-1 and scale-10 runs read 1.000 to 1.001, and the
  two scale-100 runs on record 1.004 and 1.018. The run record keeps the
  expected size to two decimals (`config_snapshot.approx_bronze_gb`).
  Stored records keep the ratio they were recorded with; they are `aml-1`
  records, which do not compare with `aml-2` ones anyway. The same size,
  about 11% larger from scale 10, sets a continuous AML run's automatic
  trickle (`max_files_per_trigger`: at the default 1800 s window, scale 5
  goes from 9 to 10 files per trigger and scale 10 from 18 to 20), the
  raw-corpus replace limit and `generate --timeout auto`. The datagen Job's
  arguments do not change: its `--target-tb` keeps the old 8.4 GB per unit
  (the AML generator sizes from `--scale` and ignores it).
- **Datagen pods also get `SSL_CERT_FILE` when `platform.storage.s3.ca_cert`
  is set**, pointing at the same mounted CA as `S3_CA_CERT`, so an image
  that predates `S3_CA_CERT` (1.6.0) trusts the configured CA through
  rustls-native-certs. It replaces the pod's system CA store on every image,
  the default one included: with `ca_cert` set, datagen trusts that CA alone
  (before, the 1.7 image trusted it on top of the system CAs), so a `ca_cert`
  for a proxy in front of a public-CA endpoint must also carry the public
  CA.
- **The two manual datagen Job manifests (`job-scale1.yaml` and
  `job-scale1-8core.yaml` under `datagen_rs`) are removed**: they named the deleted
  `lb-datagen-rs:latest` image. A unit test now fails on any tracked
  reference to a datagen image that is not the default, 1.6.0 or an
  allowlisted history entry. `docs/data-generation.md` drops the unsupported
  batch-versus-continuous speed figures.
- **The default datagen image is `lb-datagen:2a36ae21`, pinned by digest**
  (`ImagesConfig.datagen` is
  `docker.io/sillidata/lb-datagen:2a36ae21@sha256:0502b700299948f43bb1b999d7ba29262a509306658b4e5f7c48738f88d31f04`;
  was `lb-datagen:1.6.0`). It is the one image built after the held-out
  hash, strict-argument, corpus-marker, S3-transport and seed-Secret source
  changes, from integrate `2a36ae210`, and the image the registered AML looks
  pin (`docs/internal/aml-protocol.md`). The five-case byte-compare against
  1.6.0 (financial seed 43 with and without the robustness perturbation,
  Customer 360 seed 42, and both at two cycles; per-node markers excluded) is
  equal, recorded in `tests/fixtures/datagen_reference/compare-0502b7002999.json`,
  and `src/lakebench/config/datagen_lineage.yaml` maps the new digest to the
  1.6.0 root, so the two images give one corpus lineage. Every perf-gate
  config under `benchmarks/perf/` takes the same pin (they follow the
  default; none has a baseline recorded on the 1.7 tree yet).
- **Support is keyed by Spark minor and table format version, and the record
  is generated from run records.** A `validated_combinations.yaml` entry now
  names `spark` (the Spark minor of the image tag) and
  `table_format_version` beside workload, recipe and mode, and its `tree` is
  the 40-hex freeze commit; a row without the versions, a version pair the
  job builder cannot run, or a short tree is refused. A run is stamped
  `supported` only on the listed versions: a Spark 4.1 entry leaves a Spark
  4.0 run of the same recipe `unverified`, with both pairs in the basis. The
  Spark minor is read only from an `apache/spark` image with a release tag,
  so a run on a custom or forked Spark image is never `supported`. An entry
  that is not a release-matrix row at the matrix's versions is refused at
  load.
  The support table in the README and docs names each supported cell's
  version pairs and gives each unverified cell's reason as a note.
- **The release gate and CI check what the package ships.**
  `scripts/package_guard.py` reads the built wheel and sdist, and the
  script ConfigMaps rendered from the wheel, and fails on a
  `docs/internal/` or other maintainer-only member, a binary or link
  member, an access key, a private key or a gitleaks finding; once the held-out hash file exists it
  also runs the held-out absence check (a hit is `PENDING-OA5` until the
  file says `enforce`). CI's package build runs it, the release gate gains
  a `package-guard` check, and `release.yml` runs both on the files it
  publishes.
- **The release gate's `em-dashes` check is now `prose`.** It runs
  `scripts/prose_guard.py` over every tracked file instead of the docs,
  workflows, examples and CLI sources, and fails on emoji and AI
  attribution lines as well as em dashes (an HTML em dash entity counts).
  The unit tests run the same guard (`tests/test_prose_style.py`). A hit
  that has to stay is listed in `scripts/prose_allowlist.txt` with a
  reason. `--only em-dashes` is now an unknown check; use `--only prose`.
- **AML gold-finalize keeps 1,000 jobs and 1,000 stages in the Spark
  driver's status store** (`spark.ui.retainedJobs`, `spark.ui.retainedStages`;
  100 before, and still 100 for every other job and workload), so the
  per-rule stage profile holds whole rules; the path-search rules (W3,
  W17) can run more jobs than the store kept before. These keys are Lakebench's, so they
  enter the perf-gate fingerprint of AML runs, and AML perf baselines taken
  before this change do not compare like for like with runs after it.
- **AML results move to workload version `aml-2`.** W4's
  `related_txn_ids` and `related_entity_ids` are sorted and cut to 1,000
  per alert (an evidence cap, as W2 already had), and the evidence maps of
  W2, W4 and the W5 rescreen gain the full count and a truncation flag
  (`txn_total`, `txns_truncated`; W4 also `entity_total`,
  `entities_truncated`). Scoring records the cut alerts per rule and labels
  the recall of the typologies those rules detect as bounded by the cap.
  Every rule now builds its alert columns through one helper, and W5 and
  W6 share one persisted screening input when both run; both are
  results-neutral. In continuous mode a W4 hub alert over the cap is
  raised again only when a new payment's uetr sorts into the kept 1,000,
  and payments past the cut get no time to detect; across TM cycles a
  capped hub that grows several times over can lose its alert identity and
  open as a new alert. Records stamped `aml-1` do not compare with `aml-2`
  runs. See
  [aml-scoring.md](docs/aml-scoring.md#per-alert-evidence-caps).
- **The AML results block shows the funnel and labels capped totals.** The
  report opens the AML results with a funnel from rule alerts to SARs filed,
  each count with its record path and nested counts shown as "of which",
  and checks the identities the transaction-monitoring step holds, sizing
  any difference. Total alerts, the overall off-target rate and the funnel
  say they cover only the rules that ran and carry a BOUNDED BY label when
  a rule was skipped on a Lakebench cap. A continuous run's recall is shown
  over covered instances with coverage beside it, per-reason-code recall
  and FP are shown when recorded (or the scorer's status), and leakage
  reads "not measured in this run".
- **Reports open with the front matter.** The HTML report, `lakebench
  report` and the end of `run` show, before any metric: the verdict (the
  strictest of the stored and the recomputed one, with "stored X;
  recomputed Y" when they differ) and its headline, the evidence class,
  the corpus, the support state and what it means, the binding caps, n,
  the provenance and the identity digest, then the verdict's qualifiers
  and what limits interpretation. The evidence class is read from the
  registered-look record, never the config: every run reads "development"
  until a completed look names it. The fixed "internal benchmark,
  single-owner recorded" stamp is gone. A batch scale ratio above 1.05 is
  now a badge warning (amber badge and the headline of a passed run),
  never a failure; the stored verdict does not change. Mode labels read
  batch or continuous ("Continuous Throughput", "Continuous jobs"), never
  "Sustained" or "streaming".
- **Every derived number on the HTML report is checked against the record.**
  Each percentage, total and count the report computes is wrapped in a
  `data-lb-derived` span that names the `metrics.json` paths it came from,
  and a test recomputes each one from the stored record. The rendered text
  is unchanged with two exceptions: the bottleneck bar's tooltip no longer
  repeats the CPU share the legend shows, and a bottleneck share whose
  denominator is zero (no stage recorded any time or compute) reads "-"
  instead of "0.0%"; counts of 1,000 or more now carry a thousands
  separator.
- **Held-out AML seeds are checked as salted hashes.** The evaluation and
  robustness seeds are matched against `spark/data/aml/heldout_hashes.json`
  (which may only be appended to) and a compiled copy of the current
  hashes; the Python guards no longer read the plaintext seeds. A registered
  `evaluation` or `robustness` look must set `workload.datagen.seed`;
  `corpus_role` alone no longer fills it in. The guard's refusal messages
  name the role, never the seed, and `scripts/aml_gate.py` records an unspent held-out
  seed in its report by its salted hash. A missing or malformed hash file
  refuses every Spark scripts deploy, Customer 360 included. A held-out
  seed can be retired without a look: it is recorded as `burned` in
  `aml_registered_looks.json` and is then spent, and its replacement's hash
  is appended to the role.
- **The AML reference job checks every manifest row.** The corpus seed is
  recovered from each row's instance seed (it used to compare a 200-row
  sample), so a held-out seed behind any one manifest file is found, and a
  manifest the seed cannot be recovered from is refused rather than
  scored. The report's `corpus_seed_verified` pass uses the same all-rows
  check, and a report without it reads as not verified. `scripts/aml_gate.py`
  does the same.
- **The Spark scripts ConfigMaps are scanned for held-out seeds when they
  are built**, before any apply and in the continuous runner's pre-check, so
  a refusal comes before the continuous reset drops any state. Every integer
  token, every 6 to 19 digit window of a longer digit run and every comma-
  or space-grouped number is hashed and compared with the held-out hashes.
  A hit refuses the run (exit 1) before any job is submitted or any state
  is reset (`absence_check: enforce`), naming the map and key, never the
  value.
- **A registered (held-out) AML corpus's seed travels through a Kubernetes
  Secret.** For a `financial` config whose `corpus_role` is `evaluation` or
  `robustness` (or whose seed hashes to a held-out seed), `generate
  --registered-corpus` (the only command that takes such a config) writes
  the seed into an immutable Secret in the namespace,
  `lakebench-datagen-seed-<salted-hash prefix>`, and the datagen Job and the
  reference scorer read it as `LB_DATAGEN_SEED` from that Secret instead of
  from `--seed` or a plaintext env value; no Spark job of that deployment
  gets `LB_SEED`. A later registered generate for another seed deletes the
  older seed Secret; otherwise it stays until `destroy` removes it.
  `scripts/aml_gate.py` takes a held-out seed only from `--seed-file`
  (owner-only file) and refuses one on `--seed` in every mode. Development
  configs, seed 43 included, pass their seed as before (every financial
  datagen Job also mounts the held-out hash file, see below). Needs the next
  datagen image (the generator and its entrypoint read `LB_DATAGEN_SEED`); an
  older image refuses a registered corpus with exit 2.
- **The datagen generator parses its arguments strictly.** An unknown flag,
  a flag given twice, a flag without its value, a stray argument, or a value
  that does not parse now exits 2 in the entrypoint and the Rust binary
  instead of being dropped or falling back to a default (`--node-id abc` used
  to run as node 0). `--payload-kb` is still accepted by the entrypoint. The
  Rust `--scale` default is 1.0, as the entrypoint's (Lakebench always passes
  it for the financial schema).
- **The datagen generator checks held-out seeds by hash.** It no longer
  compiles the evaluation and robustness seeds in: it reads
  `heldout_hashes.json` from `LB_HELDOUT_HASHES` (a financial generate
  applies the `lakebench-heldout-hashes` ConfigMap and mounts it; `destroy`
  removes it) plus a compiled floor of today's hashes, and refuses the
  financial schema with exit 2 when the file is missing or malformed.
  Robustness refusals name the role, never the seed. The file's `spent` list
  is unioned with the compiled one. `datagen_rs/Cargo.toml` gains
  `serde_json` and `ring` (both already in the locked dependency tree) and
  `license = "Apache-2.0"`. Generator output is unchanged (the in-tree pins
  pass); needs the next datagen image.
- **`lakebench generate` fails at once when a datagen pod's Secret or
  ConfigMap does not exist** (`CreateContainerConfigError` with "not
  found") instead of waiting for the timeout. `run --generate` still waits.
- **The pre-registration and the protocol no longer hold the held-out seeds
  in plaintext.** The pre-registration drops `corpora.evaluation_seed` and
  `corpora.robustness_seed` and names those seeds by role in its notes, and
  `heldout_hashes.json` moves its absence check from `report` to `enforce`.
- **The configuration reference is generated from the schema.** The field
  tables and the removed-keys table in `docs/configuration.md` are written by
  `scripts/gen_config_reference.py` from `LakebenchConfig`: every key with
  its type, default, tier (`first day` for the keys `lakebench init`
  writes) and description, and every removed key with what to do instead.
  The descriptions live in `config/schema.py` (a field's `description` or
  the string after it); `tests/test_config_reference_drift.py` fails on a
  hand edit or a schema change without a regenerate. Keys the old tables
  left out (several images, the Unity and Pushgateway fields, the AML
  workload keys) are now listed.
- **Nothing resolves from Maven or PyPI at run time.** Spark jobs name the
  set's jars by URL (`spark.jars`, in the order `--packages` used to load
  them; `spark.submit.pyFiles` for the Delta jar) and set no
  `spark.jars.packages`, repositories or Ivy cache; the Spark Operator
  controller and the drivers resolve nothing. The AML reference job installs
  its wheels from the set with `--no-index --require-hashes` into the same
  `/opt/lb-pydeps`. Spark Thrift copies the set from the server and puts it
  on its driver classpath after the image's jars, in the jobs' jar order
  (before, the jars were copied into `/opt/spark/jars` in directory order).
  DuckDB installs its wheel and extensions from the set and runs with
  extension autoinstall off. Spark Thrift and DuckDB use the Recreate
  strategy, and deploy waits until their pod runs the deployment's set.
  Every job, Thrift and DuckDB pod carries `lakebench.io/deps-set`.
- A run records `provenance.deps` (the set's pinset, request, repositories,
  files and Python versions) and, at the end, checks the pinset the Spark
  Thrift or DuckDB pods run (`pods_checked`, `pod_mismatches`; the jobs are
  all built from the run's one set). A mismatch, or pods that could not be
  read, fails the run (exit 1, "pods ran different dependency sets").
  `benchmark` and `query` say before they start, and do not add results to
  the latest run, when the query engine now runs another set than that run
  recorded or its set cannot be read.
- Every Spark driver waits (up to 2 minutes, plus one 5 s probe) in an `lb-deps-ready` init
  container until the dependency server serves its set, so a server restart
  delays a job instead of failing it, and a registered look is not lost to
  one. The Python path of a job changes: with `--packages` every resolved jar
  was on `sys.path` and the executors' Python path; now only the Delta jar is
  (through `spark.submit.pyFiles`), the only one that ships Python, so imports
  are the same and Python workers start with a shorter path.
- A Spark stage that fails fetching a jar from the dependency server says so
  (not served, server error, unreachable), and a failed driver init
  container is named in the stage failure.
- `destroy` removes the `lakebench.deployment/deps-set` annotation right
  after its ownership check, before any teardown.
- **`reproduce` checks continuous runs the way they vary.** `ingest_ratio`
  is a range guard: the rerun's value must lie in [0.95, 1.05] (an honest
  continuous rerun of one corpus measured up to 1.034 and failed before).
  Values that follow the config, such as a continuous stream stage's
  seconds (the window length), are no longer packaged, and an older
  package's are shown as ignored. A package records its corpus role; one
  from a registered evaluation or robustness look is never rerun:
  `reproduce PACKAGE --report PATH` compares the report's sha256 with the
  look record (0 on a match, 14 on a mismatch or when the record holds no
  report sha256, 2 without `--report`). A held-out package whose look has
  not run and any financial package while the look record cannot be read
  are refused (3); a config that names a protected AML corpus is refused
  (2, `run.protected_corpus`).
  A package whose `pipeline_mode` is unknown or disagrees with its
  experiment identity is refused (2).
- **Customer 360 batch runs are gated on sixteen expected-result checks.**
  The fifteen invariant and reconcile checks (bronze rows, silver
  invariants, bronze to silver to gold reconciliation, gold KPI identities)
  and the overall average transaction value now fail the run and its
  verdict when they fail or cannot be evaluated, or when gold-finalize
  logged no facts or the check itself raised. Until now the verdict failed
  on any failed check, statistical ones included, while a run whose exact
  checks could not be evaluated, or that logged no facts, read PASSED. A
  failed check outside the sixteen no longer fails the verdict; it is
  printed, and listed in `metrics.json` under
  `verdict.qualifiers.c360_failed_not_gating`. `--local` and `--stage` runs
  make no Customer 360 check, as before.
- **AML continuous rounds run the investigator queries (workload version
  `aml-2`).** With the TM operations layer on, each in-stream round first
  probes `gold.cases` for the run's cases (untimed) and runs IQ1-IQ4 with the
  eight analytical queries once a case exists; a round before the first TM
  pass runs the eight and records `investigator_queries: absent_no_cases`
  (`probe_failed` when the probe errors), and its console line says so. The
  continuous query set therefore changes, AML records carry `aml-2` and do
  not compare with `aml-1` records, and an AML continuous run's in-stream
  composite QpH usually reads `blended` (median per set in
  `scores.composite_qph_by_set`). `qph_degradation_pct`, which compares the
  first and second halves of the rounds, is withheld for any continuous
  run whose rounds ran different query sets, and
  `scores.qph_degradation_withheld` says why.
- **Customer 360 gold is never silently incremental (workload version
  `c360-2`).** gold-finalize used to switch to its incremental strategy
  whenever gold already had rows and silver was over 1,000 GB, so a repeat
  run from about scale 100 aggregated only the last gold day and left the
  rest stale on a changed corpus, under the same identity. It now picks
  `simple_agg` below 500 GB of silver and `two_phase_agg` above, both full
  rebuilds; incremental gold runs only for multi-cycle cycles 2 and later.
  In a Customer 360 config, `spark.lb.gold.strategy=incremental`, or a
  value that names no strategy, is refused at load by the commands that
  change data (exit 2), and by the script before any write; the
  `LB_GOLD_STRATEGY` environment fallback, which nothing set, is gone. Each
  gold-finalize job records `gold_strategy` and `gold_strategy_source` in
  `jobs[].extra_metrics`. Customer 360 records now carry workload version
  `c360-2`, so they do not compare with `c360-1` records.
- **Multi-cycle time to value leaves the cycles' datagen out.** A
  multi-cycle run generates cycles 2 and later between one cycle's gold and
  the next bronze, inside the span `time_to_value_seconds` measures, so the
  score counted datagen as pipeline time. Each cycle now records
  `datagen_start` and `datagen_end`, and the overlap of those intervals with
  the span is subtracted and reported as
  `pipeline_benchmark.scores.time_to_value_datagen_excluded_seconds`.
  Customer 360 only (an AML multi-cycle time to value is unchanged);
  single-cycle time to value is unchanged; a multi-cycle record from before
  this does not compare with one after it (workload version `c360-2.dev1`).
- **A multi-cycle Customer 360 run takes one data clock (workload version
  `c360-2.dev1`).** Each cycle's silver job anchored `customer_recency_score`
  to that cycle's bronze-verify clock, so one run's rows were scored against
  different days. Every cycle now takes the exclusive end of the event-time
  range the run's cycles cover (`data_clock_source` `cycle_series_end` in the
  silver driver log). Single-cycle Customer 360 and AML at any cycle count
  are unchanged. Customer 360 records carry `c360-2.dev1`, which does not
  compare with `c360-2` records.
- **`admin repair-operator` reads and repairs under the lease.** It now
  takes the cluster lease first (waiting up to 37.5 min, three watch-list
  holds) and reads the release state, the Helm values, the `--namespaces`
  of the controller and webhook Deployments, and the Active namespaces
  inside it, so a namespace a deploy re-created after an earlier read is
  kept. It sets the watch list with one `helm upgrade` to the namespaces
  any of the three lists that are still Active; `default` is added only
  when nothing else is left (before, it was always added, entries were
  dropped one upgrade at a time, and only the Helm values were read). When
  some of them watch every namespace and others list namespaces it changes
  nothing and exits 3, before any rollback. A release left
  `pending-upgrade` or `pending-rollback` whose pending revision started at
  least 10 minutes ago, by the API server's clock (the revision Secret's
  creation time against the server's Date), is rolled back to the newest
  deployed revision that names no deleted or Terminating namespace (and,
  when the operator watches every namespace, only to one that does too),
  and the list read before the rollback is then set, so a namespace an
  interrupted add wrote is kept. Otherwise, and for `pending-install`, it exits 3 with the
  reason; when every earlier revision names a deleted namespace (and the
  operator does not watch every namespace) the message gives the manual
  recovery. With no release it exits 4; an unreadable
  Deployment or namespace list exits 1. `--dry-run` reads without the lease,
  prints the rollback verdict and changes nothing.
- **Deploy and run stop when the Spark Operator's watch list cannot be
  read.** An operator that is ready but whose watch list could not be read
  used to pass as watching; SparkApplications in a namespace it does not
  watch are never reconciled. `validate` warns instead of passing. An
  operator whose CRD or Deployments could not be read (API down, a refused
  read) is reported as "could not check", not "not installed", so nobody is
  told to install over a running operator. A Helm `spark.jobNamespaces`
  list containing an empty entry is read as "every namespace", as the chart
  renders it.
- **Watch-list waits run on the lease's 750 s hold budget.** The helm
  upgrade, the OpenShift patch rollout and the operator restart each have a
  180 s phase, bounded by what the hold has left; the two Deployments'
  rollout waits share one phase instead of 120 s each, and a step with too
  little left fails without starting (destroy then keeps the namespace).
  A helm attempt under the lease is at most 120 s; outside the lease helm
  gets no subprocess timeout, so it is never killed mid-upgrade there. A
  deploy, run or destroy waiting for the lease to change the watch list now
  waits up to 37.5 min (was 10 min), three holds at that budget; a deploy
  waits no longer than its `--timeout` allows.
- **The run capacity preflight counts free capacity and fails closed.** It
  compares the request with what the schedulable nodes can still take
  (allocatable minus what pods in other namespaces request), not with total
  allocatable, so a busy cluster that fits only on paper is refused. A
  node or pod list that cannot be read, a pod on a node the list does not
  show, or no schedulable node now refuses the run (exit 4, "capacity could
  not be read") where it used to pass. An untainted control-plane node
  counts, so a single-node cluster is checked rather than skipped. With
  scratch enabled the scratch request is compared with the StorageClass's
  `CSIStorageCapacity`; none published is a warning, recorded as
  `provenance.preflight.scratch: not_measurable`. A batch or continuous
  cluster run records `provenance.preflight` (provenance, not identity;
  `--local` runs have no preflight), and a run with
  `--skip-preflight` records `capacity: skipped` with the verdict qualifier
  "capacity not checked". `deploy`'s capacity check reads free capacity
  too, and sizes against the worker nodes' allocatable as deploy does.
  When only the pod side cannot be read (a pod list it may not read) it
  checks the workers' allocatable instead; when it cannot read capacity
  otherwise (an unreachable cluster, a node list it may not read, no worker
  or no schedulable node) it skips with a warning (deploy has no
  `--skip-preflight`), and `run`'s preflight refuses until it can.
- **`recommend` exits 3 on a context conflict** while reading capacity,
  instead of falling back to the reference table and exiting 0.
- **Deploy records its nonce beside the config.** Every `deploy` writes
  the nonce it stamps on the namespace to `.lakebench/<name>.json` first
  (last five kept, under a host-local lock), and the namespace gets
  `lakebench.deployment/state-schema: lb-state/1`. `deploy --dry-run`
  writes no state; a state that cannot be written or read stops the deploy
  with exit 4 before any cluster change, and a state copied from another
  directory or host stops it with exit 3. A destroy that finds the
  namespace redeployed since its check exits 3 with nothing deleted.
  `python -m lakebench.config.deploy_state relocate CONFIG NEWDIR [--name
  NAME]` moves a config with its state; only the directory that wrote the
  state can move it.
- **One cluster context per process.** A `lakebench` command
  resolves its cluster context once, at its first cluster call, from
  `platform.kubernetes.context` or, when that is empty, from the
  kubeconfig's current context by name. Every API client and every
  `kubectl`, `helm` and `oc` call in that process then uses it, so
  switching the current context during a long run no longer moves the rest
  of the run to another cluster. With several files in `$KUBECONFIG` the
  current context is the one `kubectl config current-context` prints (the
  first file that sets it; the Python client used to take the last), so a
  deployment made with a multi-file `$KUBECONFIG` and no configured context
  may now resolve another context: set `platform.kubernetes.context` for
  those. A second context in one process is refused (exit 3,
  `context.changed`),
  and so is a context whose API server or CA changes in the kubeconfig
  while the command runs; the check runs at each `kubectl`, `helm` or `oc`
  call and at each client load, and the ownership fingerprint a deploy
  stamps or a destroy compares is the CA read when the context was pinned.
  `config recommend CONFIG` sizes against the config's context (a config
  that does not load is refused). `admin`
  commands without a config, `status --namespace` and `recommend` print
  the context they resolved. A
  context name that is not in the kubeconfig is refused (`admin` commands
  used to fall back to in-cluster credentials), and in-cluster credentials
  are used only when no kubeconfig file exists (before, a command with no
  configured context tried them first).
- **One source of metric metadata.** Every score's unit, direction and band
  now come from `metrics/metric_registry.py`, which `reproduce`,
  the perf gate, the HTML report and `score_descriptions` read, and some
  directions change:
  `qph_degradation_pct` is lower is better (a run that slowed down was shown
  as the faster side); `qph_spread`, `maintenance_value_pct`,
  `compaction_ratio`, `window_seconds`, `benchmark_rounds_count`,
  `total_rows_processed`, `bronze_busy_fraction`, `ingest_ratio`,
  `corpus_ingest_ratio`, `query_time_event_age_seconds`, the time-to-detect
  alert counts, the maintenance file and snapshot counts and the other
  diagnostic scores are no longer coloured; in a continuous run
  core-hours (they scale with the window) and total elapsed seconds are not
  coloured, and the report's continuous CPU-hours card drops its "lower is
  better" hint. A score with no registry entry is shown uncoloured (it used
  to read as lower is better). Score values, their
  descriptions, and what `reproduce` and the perf gate check are unchanged.
- **One sizing source.** `config show`, `info`, `recommend`,
  `config recommend`, the `run` capacity preflight and the sizing tables in
  `README.md` and `docs/getting-started.md` now all come from
  `lakebench.config.sizing`. The minimum is what must fit at once: the
  Spark peak plus the query engine, Hive Metastore and Postgres; in
  continuous mode the streams plus those pods and datagen while its Job
  runs. Batch datagen is shown beside it and is elastic: pods the cluster
  cannot place wait their turn, and the preflight warns when that happens.
  The tables are generated by `scripts/gen_sizing_tables.py` and a drift
  test holds them to the code.
- **Published minimums moved.** They now include the always-on pods,
  including the catalog and Postgres memory requests and the deployment's
  dependency server (`lb-deps`, 1 core and 2 GiB): Customer 360 or AML
  batch at scale 1 is 41 cores / 544 GB (the Spark peak alone, 36 cores /
  512 GB, was quoted before), AML continuous at scale 1 is 139 cores /
  1,023 GB (was 118 / 980). No per-executor sizing changed; the
  driver-overhead entry below explains the Spark peak's move to 525 and
  990 GB.
- **The capacity preflight checks what the run deploys.** It sizes the
  config against the same cluster capacity `run` auto-sized it with (and
  offline when `run` could not read the cluster), so Trino and datagen are
  checked at the sizes deployed. Its largest-pod check now covers the
  query-engine pods and, when the run creates datagen pods, the 8-core
  datagen pod. A batch `run` creates datagen pods only with `--generate`
  (without `--skip-generate`) or in a multi-cycle run, and counts datagen
  only then. The Spark Thrift pod is counted at its pod request (heap plus
  overhead) rather than its heap.
- **`recommend` and `config recommend` use the preflight's model.** The
  separate model (Customer 360 dimensions for every workload, a guessed
  4-core infrastructure line and 15% headroom) is gone. With a cluster,
  "largest scale that fits" is the largest scale at which every scale up to
  it passes the preflight's check, bounded by the workload's datagen
  ceiling; a scale above the largest measured one (300) is labelled
  unverified. In batch that is the check for `run --generate`. In
  continuous `recommend` prints two answers, a plain `run` (datagen counted
  beside the streams) and a corpus generated first (`generate`, then
  `run --skip-generate`). On `recommend`, `--slow-datagen` is ignored and
  `--scale` must be 1 or more.
  `config recommend` sizes the config itself (its query engine and datagen
  settings) and fails on a config that does not load, where it used to fall
  back to Customer 360 batch.
- typer is capped below 0.28 (`typer>=0.12.0,<0.28`), so a new typer minor
  cannot change the CLI without a tested raise of the cap.
- The `[dev]` extra includes `[aml]`, so a development install now gets the
  pinned AML libraries (scikit-learn 1.7.2, numpy 2.2.6, pandas 2.3.3, scipy
  1.15.3) instead of the newest releases. These pins support Python 3.10 to
  3.13.
- `metrics.json` `config_snapshot`: `spark.driver` and `spark.executor` are
  gone, `scratch.size` is replaced by `scratch.size_per_job` (from the job
  profiles), and the HTML report shows each job's requested executors
  instead of the unused executor block.
- A config validation error no longer echoes the input it failed on: a
  model-level error used to print the whole block, which could carry a
  datagen seed or a key.
- **A refused OpenShift SCC grant fails the deploy.** The `anyuid`
  grant for `lakebench-spark-runner` and `lakebench-postgres` is now made
  through the Kubernetes API: a LocalSubjectAccessReview first (an existing
  grant is left alone), else the RoleBinding `system:openshift:scc:anyuid` in
  the deployment's namespace, then a second review that the grant took
  effect. The Spark Operator's service accounts get theirs the same way.
  Lakebench no longer runs `oc` anywhere. A grant that cannot be made fails
  the RBAC or PostgreSQL step (or the operator install) with the `oc adm
  policy` command for a cluster admin; 1.6 logged a warning and the pods were
  rejected later. OpenShift is detected from the `security.openshift.io` API
  group, and a failed detection fails the RBAC step instead of skipping the
  grant. OpenShift before 4.10 is no longer supported.
- **`lakebench run`'s preflight uses the shared prerequisite checks.** It now
  also checks the scratch StorageClass, requires a ready Spark Operator
  controller in `platform.compute.spark.operator.namespace` (not only the
  CRD). A check that cannot run (an API error, or no right to list
  cluster-wide) fails the preflight with "could not check";
  `--skip-preflight` bypasses it.
- The capacity check before `run` and the auto-sizer count the dependency
  server among the always-on pods, at its pod's reservation of 1 CPU and
  2 GiB (the resolve init container's request stays reserved for the pod's
  life). The reported co-resident request therefore rises by 1 core and
  2 GB. Where the CPU budget is binding, the auto-sized datagen parallelism
  and Spark executor instances can come out one step (2) lower, which
  happens at scale 100 and above on clusters of a few hundred cores. The
  concurrent budget of continuous-mode streams does not count the server
  yet, so the AML continuous executor split is unchanged.
- Config errors name the nearest key: an unknown key gets "did you mean"
  from its own section, then from the whole schema (for a key written in
  the wrong section), and an unknown recipe names the nearest recipe.
- A setting of the other workload (`customer360.*` or `dirty_data_ratio`
  under `schema: financial`; `tm_operations` or
  `w1_max_vertices` under `schema: customer360`) loads with a note saying
  the workload does not read it.
- Read-only commands create no files: `validate` no longer opens a journal,
  and `report` and `results` no longer create `lakebench-output/runs/`.
- `platform.storage.s3.secret_ref` is refused by the commands that change
  data: nothing reads an existing Secret, so a secret_ref-only config
  deployed empty S3 credentials, and the key is removed.
  `destroy`, `status` and `clean` still load it, so an old deployment stays
  destroyable. `config validate` and the deploy preflight ask for the
  inline keys only.
- Run provenance and the Hive deploy result record Hive 3.1.3, the version
  the Stackable HiveCluster template renders, instead of the tag of
  `images.hive`. An `images.hive` naming another version is refused by the
  commands that change data, since the key is removed.
- The generated config template no longer carries
  `architecture.catalog.hive.thrift.*`, `architecture.catalog.polaris.version`,
  `observability.storage_class` or `secret_ref`, which nothing reads (removed;
  see Breaking changes).
- Deploy step labels say "Verifying scratch StorageClass" and "Checking
  Spark Operator and watch list", and the deploy summary lists the operator
  step whether or not `operator.install` is set. The HTML report's
  continuous section is headed "Continuous Pipeline".
- `docs/reproductions/c360-scale-0-1.yaml` is marked as a legacy package
  that `reproduce` refuses.
- **`install.sh` detects a corrupted or incomplete download.** It downloads
  the binary to a temporary directory, checks it against the `SHA256SUMS`
  file that each release now publishes, and only then moves it into
  `INSTALL_DIR`, so a failed, truncated or corrupted download leaves nothing
  there. The checksum file comes from the same release, so it does not
  prove who built the binary. The script refuses an `INSTALL_DIR` it cannot
  write before downloading, installs the binary with mode 755, runs
  `version` on the downloaded binary before it replaces anything (not on
  the first `lakebench` on `PATH`), so a binary that does not run on the
  machine leaves the installed one in place, and refuses Linux arm64 (never published) with the list of
  available binaries. Releases before 1.7.0 have no `SHA256SUMS`, so
  `VERSION=1.6.0` (or `latest` while 1.6 is the newest release) installs
  unverified with a warning on stderr. A release at 1.7.0 or later without
  `SHA256SUMS` is refused, and so is a checksum mismatch on any release.
- The package ships a `py.typed` marker, so type checkers read its
  annotations.
- **`logs`, `stop` and `status` cover what Lakebench started and exit
  non-zero when something is wrong.** All three use the Kubernetes
  API in the config's context; `logs` no longer runs `kubectl`.
  - `logs` takes `CONFIG COMPONENT` (the 1.6 order `COMPONENT CONFIG` still
    works and prints one warning) and reads `datagen`, each pipeline stage's
    Spark driver (`silver-build`, `gold-refresh`, `score-financial` and the
    rest), `trino-worker`, `thrift` and `duckdb` as well as the five 1.6
    components. `--previous` reads a restarted container. With several
    matching pods it prints each one under a header on stderr; `--follow`
    follows the newest. It exits 1 when no pod matches or none has a log to
    read yet (was 0) and 4 on an API error (was 0).
  - `stop` deletes every `lakebench-*` SparkApplication in the namespace
    that has not finished, batch stages included, and the datagen Job while
    it runs (1.6 deleted only the three continuous streams). Finished ones
    are left in place, so a failed stage's logs survive. `--dry-run` lists
    without deleting. A deletion that fails is reported and the rest still
    run; the exit is then 1 (1.6 reported every error as "not running" and
    exited 0).
  - `status` exits 1 when the namespace does not exist or a component is not
    ready, scaled to zero or missing, and 4 when the cluster cannot be read
    (all were 0).
- **Exit codes follow one table.** `lakebench` has a single
  exit-code enum, `lakebench.exit_codes.ExitCode`, importable without loading
  the CLI, and the table in `docs/exit-codes.md` is generated from it. Every
  command now exits with a code from that table: 1 a failed run or step, 2 a
  usage or config error before anything ran, 3 a refusal by the safety model,
  4 a missing prerequisite before anything ran, 5 not confirmed, 6 incomplete
  and safe to re-run, 14 a requirement unmet. An error Lakebench does not
  classify prints one `ERROR` line, not a traceback, and exits 1
  (`LAKEBENCH_DEBUG=1` prints the traceback). Scripts that test exit codes
  need these changes:
  - a declined confirmation prompt exits 5 (was 3), and so does a prompt with
    no answer (`deploy` and `generate` off a terminal without `--yes`, end of
    input), a cancelled `init` wizard (was 0), `destroy` without `--force`
    off a terminal, and `run` without `--yes` when the namespace does not
    exist (all were 1);
  - `destroy` with the namespace still terminating exits 6 (was 4);
  - a datagen timeout in `run` exits 1 (was 5); the run record keeps the
    distinction: its `verdict.reasons` contains "datagen timed out";
  - a config that fails to load or validate, an unsupported workload, recipe
    and mode combination, and an argument a command checks itself (an
    unknown recipe, component, stage or example, a missing file, conflicting
    options) exit 2 (were 1); `run --stage` with an unknown name is now
    refused before anything runs;
  - a non-empty bronze prefix without `--regenerate` exits 3 (was 2), and a
    bronze bucket that cannot be read to check it exits 4 (was 2);
  - refusals by the safety model exit 3 (were 1): a namespace or bucket owned
    by another deployment or without lakebench ownership proof (`deploy`,
    `destroy`, `clean`), "Destroy NOT completed" because the namespace is a
    newer deployment, a cluster lease another process holds (`destroy`,
    `admin`), a continuous run that would reset data without
    `--force-reset`, a non-empty bucket `admin reclaim-bucket` will not
    retag. A command whose failed steps include any other failure still
    exits 1;
  - a Kubernetes config that does not load or an API that cannot be
    reached, a bronze bucket or namespace that cannot be read for an
    ownership or emptiness check, a failed `run` prerequisite and a Spark
    Operator that is not ready exit 4 (were 1 or 2);
  - `reproduce PACKAGE` exits 14 for performance or correctness drift (were
    1 and 2), for commit drift without `--allow-commit-drift` and for a run
    that does not follow the package (were 2), and 1 when its pipeline could
    not run (was 2).
- **Errors are one line and markup-safe; machine output is plain.**
  `ERROR`, `WARN`, `OK` and progress lines now go to stderr, and their text
  is printed verbatim: a value such as `s3a://b/[x]/y` or `[/tmp]` no longer
  vanishes or crashes the command with a Rich `MarkupError`, and a long
  message is not wrapped. `query --format json|csv` and `results --format
  json|csv` write to plain stdout, with notices such as
  "N rows in Xs" on stderr, so the output pipes into a parser. urllib3
  retry lines and warnings are silenced. A config whose top level is not a
  YAML mapping is refused with one line naming the problem instead of an
  `AttributeError`.
- `pydantic-settings` is no longer a dependency: nothing imported it, so
  every install pulled it in for nothing and the binary bundled it.
- `botocore`, `pydantic-core` and `urllib3` are declared dependencies:
  Lakebench imports them directly, and they came only through boto3,
  pydantic and kubernetes. Their floors are ones the existing floors already
  imply, so they add no constraint; a fresh install resolves the same
  versions as before.
- **Experiment identity v2 and identity groups.** A run is stamped
  `experiment.schema: exp2` with `identity_version: 2` only when it has a
  corpus id v2, the run-start identity version and an observed system
  identity; otherwise it is `exp1` and `experiment.v2_unavailable` names
  what was missing (no v1.7 run is exp2 until the datagen image writes
  the corpus markers). An exp2 identity drops the generator image tag and
  the `cluster`/`local` string and adds corpus id v2, the query set id,
  the system fingerprint, `architecture.access_paths` and the dependency
  pinset, so exp2 runs of an unchanged config get new identity digests;
  stored records keep theirs. The system and the query access path are no
  longer execution conditions: a pair that differs in them is no longer
  "not like-for-like". The compaction operation now is
  one: Trino `optimize` at 128MB and Spark Thrift Iceberg
  `rewrite_data_files` read as different conditions, so the stored AML
  batch pair polaris-Thrift against hive-Trino (runs 103055-de1772 and
  130953-f8a2cf) is now comparable, not like-for-like, where 1.6 called it
  like-for-like. A perf-gate baseline or reproduction package recorded
  under the other identity version is refused with one message naming
  both versions. A pair whose architecture and system both differ is
  confounded and is no longer called like-for-like, nor is a pair that
  differs only in its dependency set; a record with a required workload or
  corpus key missing, or a withheld seed, is not comparable.
- **System and load at run start and end.** `run` records
  `experiment.system_identity` (the system fingerprint, sampled at run start)
  and `experiment.observed`: allocatable CPU and memory of the schedulable
  workers and the CPU and memory requested by other namespaces' scheduled
  pods, at run start and end. The load is evidence only (n=1 per sample);
  nothing compares on it in 1.7. Co-tenant requests include platform
  pods (DaemonSets, shared operators), so an idle cluster reads above
  zero; pods not yet scheduled are recorded apart. Sampling reads two node
  lists, a paged cluster-wide pod list, the API server version and
  ClusterVersion and sends one HEAD on the bronze bucket at run start, and
  a node list and the pod list when the record is saved; each sample
  returns within 120 s and 60 s, and a refused or unfinished read is
  recorded as `not_observed`. A `--local` run records a local system
  identity with no part observed, and no load.
- **A stored experiment block is never rebuilt.** Loading and saving a
  record keeps its block as written; 1.6 rebuilt it with the current code,
  which moved the identity digest of seven stored records.
  `lakebench benchmark` updates only the benchmark half of the stored
  block (batch results, benchmark iterations and mode, samples per query,
  the "benchmark (not run)" stage entry) and notes it in
  `experiment.benchmark_source`; that moves the record's identity digest,
  since it now describes another benchmark.
- **`deploy --timeout` now bounds every wait.** It used to be
  checked only between steps, so a step waiting on Spark Thrift (300 s),
  DuckDB (900 s), Polaris (600 s plus 600 s) or an operator rollout could
  run past it. Every wait is now clamped to the time left, including the
  wait for the cluster lease, and a wait the deadline cuts short fails the
  step with "deploy timeout (N s) reached after M s while waiting for
  <component>: <resource> (<last state>)". No shared change (a helm
  upgrade of the Spark Operator watch list or of the operator itself, a
  Stackable or observability install) starts after the deadline; one that has started is completed,
  with its rollout and verify, before the step fails, so the shared
  operator is never left mid-restart. That completion can take several
  minutes past the timeout (restart, rollout and verify are bounded by
  their own timeouts, about 9 minutes in the worst case, while holding the
  cluster lease). Helm calls already running finish first, and other
  polling waits can overrun by one poll interval (10 s at most).
- The deploy failure panel no longer claims that successful steps are
  skipped on retry: re-running deploy re-applies every step and keeps the
  existing resources.
- **Per-deployment secrets.** A new deployment generates its own Hive
  metastore DB password (Secret `lakebench-postgres-secret`), Polaris DB
  password (`lakebench-polaris-db`) and Polaris client secret
  (`lakebench-polaris-client`), once, in its namespace. 1.6 used the fixed
  values `lakebench-hive-2024` and `lakebench-polaris-2024` for every
  install. An existing deployment keeps its own: a stored Secret wins, and a
  deployment made by 1.6 (Postgres PVC or `polaris` role present) gets the
  1.6 password stored. Every deploy then sets the `hive` and `polaris` roles
  to their Secret's password (as a SCRAM verifier, so no plaintext crosses the
  exec request or the logs), so a lost Secret cannot lock the metastore out.
  `destroy` keeps these Secrets while the Postgres PVC survives.
- **`architecture.catalog.polaris.client_secret` is optional.** Unset,
  `deploy` generates one for a fresh Polaris and `run`, `benchmark`, Trino
  and Spark Thrift read it from the namespace. Set before the first deploy,
  it is used and stored. A bootstrapped Polaris keeps its secret: a differing
  config value, or none in the config and none stored, stops `deploy` with
  the fix. The examples, the AML perf config and the root `lakebench.yaml` no
  longer set it.
- **No credential literals in the Spark Thrift and Trino specs.** Thrift reads
  the S3 keys and the Polaris client secret from Secret-backed env vars, and
  Trino reads the client secret through `${ENV:POLARIS_CLIENT_SECRET}`. The
  Spark job `sparkConf` S3 keys stay literal until v1.8; `spark.redaction.regex`
  now also hides `credential` keys in the Spark UI and event log (a user's
  own `spark.redaction.regex` is kept, with Lakebench's terms added in front). A Secret
  holding an empty password or client secret stops `deploy` instead of being
  used.
- **Grafana has no fixed password.** A new shared observability install gets
  a generated password in the Secret `lakebench-observability-grafana`, and
  `deploy` prints the command that reads it. An existing install keeps
  `admin`/`lakebench`.
- **Stale bronze on buckets this deployment did not create is refused.**
  `generate`, `run --generate` and a multi-cycle run's first cycle
  go through one gate. `--regenerate` now clears only the datagen prefix
  (aborting its incomplete multipart uploads) instead of the whole bronze
  bucket, and only on a bucket this deployment owns; on any other bucket it
  exits 3 (refused), where 1.6 emptied the bucket whoever owned it. Datagen over a
  non-empty prefix of such a bucket needs the new `--allow-stale-bronze`
  flag on `generate` and `run`; the run records `datagen.stale_bronze` in
  `metrics.json` and the report warns "bronze held N objects before
  generate; rows may be over-counted". The deployer no longer skips such a
  bucket silently before cycle 0. A user with a pre-provisioned bronze
  bucket who relied on `--regenerate` clears the prefix, or claims the bucket
  once with `lakebench admin reclaim-bucket` and then uses `--regenerate`
  (`--allow-stale-bronze` would over-count). A multi-cycle run takes the
  same gate before cycle 0, and a `run` that reuses a corpus generated with
  `--allow-stale-bronze` still records the note (from the corpus series
  marker, or the note `generate` left on the host). `run` refuses
  `--allow-stale-bronze` (exit 2, before any cluster call) where no generate
  reads it: without `--generate`, `--generate-only` or a multi-cycle batch
  run that generates, and with `--local`, `--deploy-only` or a continuous run other than
  `--generate-only`. A `run --repeat` series passes it to repetition 1
  only, and its manifest carries repetition 1's note (`corpus.stale_bronze`).
- **`destroy` clears the kept silver-state's data clock when it empties
  bronze.** With `create_namespace: false`, `lakebench-silver-state`
  survives destroy for its rebuild counters; its `bronze_data_clock` now
  goes when destroy empties the bronze bucket, and when a generate replaces
  bronze, so silver stages no longer read the old data's clock.
- **Bucket ownership names the cluster.** Deploy stamps each bucket
  it owns with `lakebench.cluster=<API-server fingerprint>` and refuses when it
  cannot compute the fingerprint. Deploy refuses, and destroy, `clean` and
  the continuous reset keep, a bucket another cluster stamped, so the same
  deployment name on two clusters sharing an object store can no longer
  empty each other's data. A 1.6 bucket without the stamp is stamped on the
  next deploy when this namespace's record shows it created or adopted it;
  one it does not record (adopted by 1.6) is used but no longer emptied or
  deleted, and `lakebench admin reclaim-bucket` can claim it. With no
  fingerprint, destroy keeps every stamped bucket: "Destroy NOT completed:
  this cluster has no fingerprint". On a backend without tagging
  (FlashBlade), deploy adopts a pre-existing empty bucket, or a
  pre-provisioned one with `create_buckets: false`, only with
  `--force-legacy`; without it the bucket is used but destroy leaves its data.
  A bucket 1.6 recorded as adopted while empty is no longer emptied on that
  record (1.6 wrote it for another cluster's bucket too); claim it with
  `admin reclaim-bucket`. Destroy stamps a recorded 1.6 bucket before it
  empties it, and keeps the stamp on every bucket it keeps (`--keep-buckets`
  included), so the bucket stays this deployment's.
  There the stamp is an owner marker object, `.lakebench/owner.json`, written
  with a conditional PUT where the backend enforces it. `.lakebench/` keys
  are never counted as data, and `clean` and `--regenerate` keep them. The
  `boto3` floor rises to 1.35.2, the first release whose botocore accepts
  `IfNoneMatch` on PutObject.
- **`destroy` removes what it used to leave in a surviving namespace.** With
  `create_namespace: false`, destroy left the PostgreSQL ServiceAccount, the
  `lakebench-ca-certificate` Secret (with `s3.ca_cert`) and, with
  observability on, the Pushgateway Deployment, Service and PVC, the
  Prometheus ConfigMap and five PodMonitors. A new step, after the component
  steps and before the namespace step, deletes them by name from the
  Category-1 registry (`deploy/category1.py`). The registry lists every
  object deploy and run create in the namespace and the step that deletes
  it; a unit test runs deploy and the run-time creators against it, and
  checks every template. The `lakebench-silver-state` ConfigMap is kept on
  purpose (its rebuild counters must not reset while table data can outlive
  destroy), and so are the deployment's identity annotations.
- **`destroy` keeps a namespace an operator pod still watches.** After it
  removes the namespace from the Spark Operator watch list and the operator
  restarts, destroy waits inside the cluster lease (up to 120 s) until no
  operator pod that is still running, and no operator Deployment template,
  lists the namespace in `--namespaces=`. A stale pod nobody is replacing
  gets one more restart of the shared operator's controller and webhook (as
  the watch-list change itself does). If something still lists it, destroy keeps
  the namespace and exits 1 with "operator pods [...] still watch it",
  because the operator crash-loops on a watched namespace that no longer
  exists.
- **The legacy SecretClass cleanup in `destroy` runs under the cluster
  lease.** The cluster-wide count of other lakebench namespaces and the
  deletes of `lakebench-s3-credentials-class` and
  `lakebench-s3-ca-cert-class` used to run without it. The lease is taken
  only when one of them exists; if it stays held for 600 s the cleanup is
  skipped and they are kept.
- **The cluster lease holder names the process.** The `holder`
  field is now `<host>@<user>@<sha>#<pid>-<8 hex>`, unique to each acquire,
  and release matches the lease's write nonce, so a process never deletes a
  lease another run from the same host wrote in the same second. An acquire
  that fails or is interrupted after its write landed (a lost reply, a 504,
  a Ctrl-C, a SIGTERM) releases that lease instead of leaving it to the
  3600 s TTL, and an acquire whose own write comes back as a conflict adopts
  it instead of waiting on itself.
- **`destroy` deletes the PostgreSQL data PVC when the namespace
  survives.** With `create_namespace: false`, `data-lakebench-postgres-<n>`
  and the catalog metadata on it used to survive destroy, because the cleanup
  selected on a label the claim never carried; the next deploy then started
  on the old metastore. Destroy now deletes the claims by name. It no longer
  selects on `app.kubernetes.io/component=postgres`, which could only ever
  match another application's claim in a shared namespace.
- **Spark scripts ship in one ConfigMap per role.** The single
  `lakebench-spark-scripts` ConfigMap, about 45 KB from the 1 MiB limit with
  every AML addition, is replaced by six maps (`lakebench-scripts-common`,
  `-c360`, `-aml-rules`, `-aml-jobs`, `-aml-gate`, `-aml-data`), projected
  together at `/opt/spark/scripts`, so script paths and imports are unchanged.
  Each map is refused above 80% of 1 MiB, measured on the bytes applied. A
  script listed for shipping but missing from the installed package now stops
  `run` before any job is submitted, where 1.6 skipped it and the driver
  failed later with an ImportError. `run` will not change a scripts map that a
  running SparkApplication mounts, and a job is not submitted if its maps
  changed since its run applied them. The first 1.7 `run` deletes the 1.6 map
  unless a running SparkApplication still mounts it; `destroy` deletes all
  scripts maps, also when `create_namespace: false`.
- **Interrupts wait for the cluster lease to be released.** A Ctrl-C,
  SIGTERM or SIGHUP while a command holds the `lakebench-cluster-lock`
  lease no longer stops it between a `helm upgrade` of the shared Spark
  Operator and the operator restart. The command prints the hold budget
  left (750 s, 1800 s for `admin` commands), finishes the shared change,
  releases the lease, then stops; a third interrupt aborts at once and
  still releases the lease. `kubectl`, `helm` and `oc` run under the lease
  in their own session with a timeout from that budget, and are stopped
  with SIGTERM rather than killed, so a terminal Ctrl-C or a timeout no
  longer leaves the release `pending-upgrade`. A leased command that runs
  out of time fails deploy or destroy closed (destroy keeps the namespace);
  for helm the error names `helm rollback` and
  `lakebench admin repair-operator`.

### Removed
- **`compare`.** Comparing two runs is left to the reader: read the two
  runs' reports side by side. Each report's Experiment section states the
  corpus, datagen image, components, maintenance, stages and rules that
  ran, and a result fingerprint per benchmark query; see Comparing Runs in
  `docs/benchmarking.md`. `lakebench compare` exits 2 with any arguments
  and names the replacement, and exit codes 10 to 13 are gone. `run
  --repeat` still records a series, but nothing summarises it: read its
  members' records for the spread.
- **`config upgrade` refuses.** It rewrote configs lossily, in place
  by default, and wrote the S3 secret key into the result in plaintext. It
  now exits 2 before opening any file and names the replacement,
  `lakebench init --from OLD.yaml -o NEW.yaml`.
- **Dead flags.** `generate --wait` / `-w` (generate always waited;
  there was no `--no-wait`), `admin release-lock --expired-only` (always on;
  `release-lock` releases only an expired lease unless `--force` is given)
  and `deploy --include-observability` (set `observability.enabled: true`
  in the config instead). Each is now an unknown option and exits 2.
- `lbrun.py`, the run-from-a-checkout wrapper. Use
  `PYTHONPATH=src python -m lakebench` instead.

### Fixed
- A batch run at scale 50 or above with default settings no longer loses its silver-build executors to "node was low on resource: ephemeral-storage": when `platform.storage.scratch.enabled` is unset, auto-sizing turns scratch PVCs on for batch at scale 50 and above, and `plan`, `validate`, deploy's preflight and `admin install --component all` check or install the scratch StorageClass for it. Set `enabled: false` to keep emptyDir shuffle. Continuous mode is unchanged.
- A Customer 360 Iceberg batch full rebuild (`--force-rebuild`) of a silver table written by 1.6 or by a continuous run no longer fails on a Hive recipe, where the metastore refused a replace that moved columns and each refused attempt left its data files in the bucket. Silver keeps 1.6's column order (`_batch_id` last), so a 1.6 table is replaced in place as before; a table with other columns (a continuous run's, with `_stream_id`) is dropped from the catalog first (on Hive and Polaris its files stay in the bucket; a write that then fails leaves no table, which the next run builds).
- A continuous run keeps its completed datagen pods for its window (`--duration` or the config) plus an hour, so the fleet record is still readable at window end; they were deleted an hour after the Job finished.
- A continuous run whose window gate fails (no data arriving, too few silver commits or gold refreshes, a stream that died or restarted) names that problem in its verdict instead of "Pipeline crashed or was interrupted". Other gates still fall back to it, for example the drain, result check, AML detection, TM verdict, C360 zero-row, in-stream benchmark round and deps-pods gates.
- `run --yes`, `run --generate-only` and `reproduce` no longer print deploy's "Next:" steps when they deploy and carry on; a plain `deploy`'s next steps name its config; `run` says the infrastructure was not checked when a flag skipped the check; a non-interactive `destroy` asks for `--yes` (it said `--force`, an alias); destroy progress shows no internal component names or negative times, and its re-deploy hint names the config.
- Run records, the datagen fleet record and report.html never show a protected AML seed: it is recorded as its salted reference and role, and withheld when the held-out record cannot be read.
- **A redeploy refreshes the namespace's committed-sha stamp.** A
  namespace already stamped with this deployment's identity was left as
  it was, so `lakebench.deployment/committed-sha` kept naming the code of
  the first deploy. A redeploy from other code now updates it (or removes
  it when that code cannot name its commit). The identity annotations and
  `stamped-at` are unchanged, a foreign or other-cluster stamp is never
  refreshed, and a failed refresh never refuses the deploy.
- **Owner markers record the Lakebench version.** `.lakebench/owner.json`
  read the version of a distribution named `lakebench`, but the package is
  `lakebench-k8s`, so every marker recorded `lakebench_version: "unknown"`.
  It now records the package version.
- **A nameless config reached through a symbolic link reads the v1.6 name
  where v1.6 did.** The name in `.lakebench/state.json` is read beside the
  path given, not beside the file the link points to, so `destroy`, `stop`,
  or `status` through a link no longer use, or check `--name`
  against, another directory's v1.6 name. When the two directories record
  different names, or only the target's records one, every command that
  may look at a deployment refuses a nameless load without `--name`;
  `info`, `config show`, `config storage` and `config recommend` load it
  under the link directory's name (or a suggested one) with a note. `init
  --overwrite` through a link refuses when either directory records a v1.6
  name, and so does `relocate`; `init --from` through a link refuses a name
  recorded only beside the link's target.
- **A fresh generate waits for an earlier datagen Job's pods to stop.**
  The previous Job is deleted in the background, so its pods kept running
  for their grace period and could land a `part-*` file in the datagen
  prefix after it was cleared; silver then counted the old file as this
  run's rows and nothing refused. `generate`, `run --generate` and a
  multi-cycle run now delete the earlier Job and wait until no pod labelled
  `app=lakebench-datagen` is still running before the bronze gate lists or
  clears the prefix, and a continuous run (C360 and AML) does so before its
  reset clears the raw prefix. The wait is bounded at five minutes; a pod
  still running then refuses with exit 3 (`datagen.pods_live`) and names
  it; pods that cannot be listed exit 4, as an unreachable cluster does
  (`k8s.unreachable`). The datagen
  deployer now takes the gate's decision rather than the
  `--allow-stale-bronze` flag, so objects that appear after the gate saw
  an empty prefix are refused instead of written over with no
  `datagen.stale_bronze` record.
- **A datagen refusal exits 3.** A stale-bronze refusal raised by the
  datagen deployer (a continuous run, or a batch run whose prefix filled
  after the CLI gate) exited 1; it now exits 3 (`run.bronze_nonempty`), as
  the exit-code table says.
- **A continuous run's stale-bronze refusal names a remedy that applies.**
  When datagen found objects in a bronze prefix this deployment cannot prove
  it may empty, the message told the operator to pass `--allow-stale-bronze`,
  which `run` refuses on a continuous run (exit 2). The continuous reset has
  already cleared that prefix by then, so another writer put the objects
  there since. The message now says to re-run once `kubectl get pods -l
  app=lakebench-datagen` lists none and nothing else writes there (the reset
  clears the prefix again), and that `--force-reset` does not change this
  check. Batch messages, and `run --continuous --generate-only`, which does
  take the flag, are unchanged.
- **The namespace's committed-sha stamp no longer names the shell's
  repository.** `lakebench.deployment/committed-sha` was read from the
  working directory's repository, so a deploy run from one checkout with
  the shell in another repository stamped the other repository's commit. It
  now comes from the lakebench package's own checkout (the commit the run
  record's `provenance.git_sha` names), with `-dirty` for uncommitted
  changes. A wheel install stamps the commit its build info names, marked
  `-buildinfo` (and `-dirty` for a build from a modified tree); the stamp is
  left off when no commit can be read. A redeploy refreshes it (see the
  entry above).
- **Report numbers that misled.** Platform CPU and memory no longer count
  containers more than once: the Prometheus queries exclude cAdvisor's
  pod-level and pause-container series and take each (pod, container) once
  when the kubelet is scraped twice; `platform_metrics.query_version` is 2,
  and a report of an older record says its figures are overstated. The
  stage table labels its max columns "sum of per-pod peaks". Table Maintenance shows each QpH with its query
  count and the change as "QpH change, paired over N queries" (a pre round
  of 8 queries and a post round of 12 are no longer set beside each other
  as the effect). QpH tags read `n=1 run, 3 samples/query` or `n=1 run, 4
  rounds` instead of counting samples or rounds as runs. The benchmark
  section is titled with the recorded query engine, not always "Trino". A
  batch scale ratio above 1.05 shows amber ("above the scale") instead of
  green "Complete". The continuous stability section shows the recorded
  `qph_degradation_pct` instead of a trend the page computed itself.
- **Report: failed runs, resources, labels and provenance.** A run whose
  verdict is not PASSED shows no headline number: the page leads with the
  first verdict reason a reader can act on and the failed jobs' errors,
  every score card reads "-", and the pipeline summary line withholds its
  figures; an INTERRUPTED run reads INTERRUPTED in the front panel. "Resources as run" replaces the configuration's executor rows
  (which showed the snapshot's unused `spark.executor` defaults): executors
  per job as run, cores and memory from the job profile, and the scratch PVC
  as the cluster held it. Continuous runs label the ingest ratio "bronze
  rows / rows the trickle released" and show corpus coverage, the window and
  the offered load; AML rules continuous mode does not run read "excluded in
  continuous mode", not "no data". The front panel names the provenance
  (version, commit, dirty or clean) and what limits interpretation (n=1,
  skipped rules, in-sample AML recall, a dirty tree); AML recall is
  labelled "uncalibrated, in-sample" unless a completed registered look
  names the run, and the planted-subject customer check is shown. A
  malformed AML record shows "AML results could not be rendered: <error>"
  instead of an empty section. Throughput and efficiency say they are over
  stage inputs (bronze + silver + gold + query reads) and show the corpus
  size beside them (for a continuous run, the bronze bucket at run end,
  named as landing files plus the bronze table). The bottleneck caption names requested core-seconds; a Spark
  Thrift or DuckDB query stage is no longer charged Trino's cores, and the
  continuous query stage's seconds are no longer shown as milliseconds and
  summed into the latency share.
  changes, and is left off when no checkout commit can be read (a wheel
  install). It is still written only when the namespace is first stamped.
- **Multi-cycle scale ratio.** A multi-cycle batch run's `scale_ratio` now
  reads the last bronze-verify, which reads every cycle; it read the first,
  so the ratio was about one over the cycle count and the run read failed.
- **Destroy stops at a failed Spark Operator restart.** After removing the
  namespace from the watch list, a failed operator restart used to be
  ignored, leaving destroy's pod poll (one more restart, then keep the
  namespace if a pod still listed it) as the only check. Destroy now keeps
  the namespace and exits 1 as soon as the restart fails; the namespace is
  already off the list, so re-run destroy once the operator pods are Ready.
  On OpenShift the patch's rollout is awaited before the restart.
- **A run that generates its corpus records the generator.** `lakebench run
  --generate` (batch) and a continuous run that starts its own datagen now
  read the fleet from their own datagen pods, record it as `datagen_fleet`
  and write the namespace's sidecar, so `experiment.corpus.datagen` carries
  the generator image digest instead of "no datagen fleet record for this
  run". Before, only `lakebench generate` wrote the sidecar, and a batch
  `run --generate` attached whatever an older generate had left, which could
  describe a corpus the run had replaced. `lakebench generate` and a run
  that generates now remove that sidecar before they replace the corpus
  (with `--regenerate`, before the bronze check that empties it), so a
  generate that fails leaves none; a batch run that does not generate still
  takes it. A batch run with `pipeline.cycles` above 1 records no fleet,
  with or without `--generate` (its cycle pods are not read, and cycle 0
  replaces the corpus), and removes the sidecar; `run --local` is
  unchanged. A continuous run's fleet is read at window end, so the perf
  gate's `data_quality` refusal now applies to continuous records too.
- **Delta continuous Customer 360 works again.** Its silver stream failed
  on the first micro-batch on every `hive-delta-*` recipe: it passed the
  three-part `spark_catalog.silver.customer_interactions_enriched` to
  Delta's table builder, which Delta 4.0 and 4.1 reject. It now passes the
  two-part name. Two writers in one Spark application racing to create
  the table no longer fail: the loser, whose create Delta refuses, waits
  up to 30 s for the winner's table and appends to it (a create failure
  that leaves no table is raised after that wait). An append that loses a
  concurrent metadata or protocol change is retried up to five times with
  the same transaction id, so it still commits once and is counted once.
  Both waits count in that micro-batch's time. The stream runs one query
  per driver, so this covers writers in one Spark application; two driver
  pods writing one table rely on the S3 log store, which serialises
  commits only within one JVM.
- `lakebench financial replay` runs on Spark 4.1 with Iceberg 1.11. It read
  silver at the replay snapshot with the `snapshot-id` read option, which
  Iceberg 1.11 removed, so it failed before running any rule; it now reads
  with `VERSION AS OF`.
- **A run that fails while saving its record no longer leaves its signal
  handlers installed.** When the metrics save, the report or the journal
  raised at the end of a batch or continuous run, the run's SIGINT/SIGTERM
  handler stayed in the process, and a later cluster-lock acquire in the
  same process (which guards only an unhandled SIGTERM) ran unguarded. The
  handlers are now put back however the run ends.
- **The capacity check counts the Spark driver's memory overhead.**
  The driver pod requests its heap plus the overhead Spark on Kubernetes
  adds to a Python driver, 40% of the heap (12.8 GiB for the 32 GiB
  silver-build driver); `plan`, the preflight and the continuous budget
  counted the heap only. Per-job memory now rounds up to a whole GB. The
  scale-1 batch peak is now 36 cores / 525 GB (was 512 GB), Customer360
  continuous at scale 1-10 38 cores / 282 GB (was 272 GB) and AML
  continuous 118 cores / 990 GB (was 980 GB); the docs tables follow.
  What the pods request is unchanged.
- **`deploy` runs the cluster capacity check before it creates anything,
  and `run --skip-deploy` no longer skips the prerequisites.** A config the
  cluster cannot hold was deployed and only failed at `run` (or never, with
  `--skip-deploy`). Deploy now refuses it with exit 4 and creates nothing
  (the check counts the pipeline and always-on pods, not datagen, which
  deploy does not run; `--dry-run` shows the result).
  `--skip-deploy` is no longer an alias of `--skip-preflight`: it skips the
  deploy and the infrastructure readiness check, and the read-only
  prerequisite checks still run; `--skip-preflight` skips both as before.
  When the peak calculation itself fails, the message names the exception.
- **The capacity check reads every Kubernetes quantity, and fails rather
  than skips when it cannot.** One parser (`lakebench.quantity`) now
  serves the preflight, the continuous stream budget, the autosizer, node
  allocatable and the system fingerprint. `1Ti`, `2000000Ki`, `4G` and
  `1e3` read correctly (`16G` is 16e9 bytes, 14.9 GiB, where the old
  parser read 16 GiB); `16g` for a pod memory is not a Kubernetes size and
  is named, and `admin --controller-tmp-size` no longer takes `1K` or
  `1 Gi`, which Kubernetes rejects too. A config value the check cannot read now fails it (`run` exits
  4) where it used to pass as "Capacity check skipped"; an unreachable
  cluster skips only `deploy`'s check (the `run` preflight fails closed,
  above). DuckDB's Spark-style memory (`4g`) is counted
  as the pod deploy renders (`4Gi`).
- **The capacity check, `config show` and `info` count driver overrides.**
  They read the job profiles only, so `platform.compute.spark.driver_memory`
  and `driver_cores` and the 24g Spark 3 silver and gold drivers were never
  counted (a 64g driver under-counted silver-build by 45 GB). They now
  count the driver each manifest requests. A `driver_memory` Spark cannot
  read (`16Gi`, `1.5g`) is refused by the commands that change data, since
  the job would fail at submit; `16gb` is accepted. The capped continuous request
  counts the always-on pods as the capacity plan does (lb-deps once, the
  catalog and Postgres memory), so AML continuous at scale 10 runs
  degraded from 82 cores, not 81.
- `lakebench clean silver` followed by `run` works on Delta recipes. The
  clean empties the silver bucket and keeps the catalog entry, and the next
  silver build failed on it (DELTA_TABLE_NOT_FOUND), with or without
  `--force-rebuild`. The build now drops an entry with nothing left at its
  location and builds the table afresh. When the Delta log is gone but data
  files remain, it refuses and leaves the files.
- An AML continuous run on a reused catalog no longer scores, or shows,
  the previous run's gold. The reset before the run now also drops
  gold.alerts, risk_scores, entity_clusters, daily_dashboards and
  detection_status (gold-refresh recreates them), so a score or a query
  before this run's first gold tick no longer reads the previous run's
  alerts. The financial score inside `lakebench run` now also refuses a
  detection status that another run wrote: it took the run id from that
  table without comparing it to its own.
- An AML continuous run on a reused catalog no longer starts from an
  earlier run's account statements and entity profiles. The reset before
  the run dropped transactions, edges, entities and accounts only, so the
  stream appended statements to and folded profiles into the old ones;
  it now drops every silver table the stream writes, including the
  batch-versions sidecar. The stream's refusal to start a fresh checkpoint
  over populated silver checks all of them too, not only transactions and
  edges.
- `lakebench clean` followed by `run` works on recipes with a Trino or
  Spark Thrift query engine. `clean` emptied buckets and kept the catalog,
  so the next run met tables whose files were gone: Delta gold after `clean
  gold` or `clean data`, the continuous Delta jobs, and Iceberg on a Hive
  catalog after any clean failed on them. `clean` now unregisters a layer's
  tables before emptying its bucket, through that engine's pod, and keeps the
  bucket (exit 1) when a table there cannot be unregistered, so a re-run can
  finish. On recipes with DuckDB or no query engine it still warns and leaves
  the entries.
- A multi-cycle Customer 360 batch run no longer loses silver rows when a
  later cycle finds no silver table and rebuilds it. On Iceberg the rebuild
  tagged every row with that cycle, so an operator retry of the cycle, which
  deletes the cycle's rows before appending them again, deleted the whole
  rebuild and kept only that cycle, and the run exited 0. Rebuilt rows now
  take the cycle in their bronze file's name. The rebuild, on Iceberg and
  Delta, also reads only this run's bronze files (cycle 0 up to the current
  cycle, the files bronze-verify counts), not the later cycles' files an
  earlier run with more cycles left under the prefix. On Iceberg, a rebuild
  whose own cycle's files are not named as datagen names them is refused.
- Silver-build no longer rebuilds a populated table without
  `--force-rebuild` when its check for existing rows fails. A failed read
  counted as an empty table; it is now a refusal that names the error. On
  Iceberg the check that the table exists also no longer counts any error
  as "no table"; only a table the catalog does not have is missing.
- A Delta Customer 360 multi-cycle batch run no longer loses cycles when the
  deployment's rebuild epoch reads lower than one the silver table already
  used: the `lakebench-silver-state` ConfigMap lost or recreated while the
  table survived, or the epoch read at job submission falling back to 0.
  Delta skipped the new run's cycles 1..N as already committed under the
  old (txnAppId, txnVersion) keys, so silver held only the new cycle 0 and
  the run exited 0. Silver-build now takes the epoch from the table's Delta
  log: a full build writes under an epoch above every one in the log, and
  each append continues the newest. An operator retry of a committed cycle
  is still skipped, and now also when that cycle found no table and built
  it from every cycle's files (the retry appended the cycle a second time).
  The build refuses when it cannot read the log's transaction ids, and when
  a later cycle of its epoch is already committed (a manual re-run of an
  earlier cycle, which was a silent no-op). When the metastore is lost and
  the table files are kept, cycle 0 on the Hive catalog already refused to
  adopt the old Delta log; that is unchanged.
- **Ctrl-C or SIGTERM during `run` seals the record INTERRUPTED and stops
  this run's jobs.** A batch run interrupted while a stage ran used to save
  `success: true` and a PASSED verdict, and left the SparkApplication and
  any datagen Job running; a continuous run read FAILED and left its datagen
  Job. Now the run deletes every SparkApplication and datagen Job it created
  and has not seen finish, each with the uid of the object it created as a
  precondition, so an object of the same name created since by another
  invocation is never deleted (it is listed as left). The cleanup takes at
  most about 60 s. metrics.json gains `interrupted` (signal, stage, time,
  `prior_failure`, and the objects stopped, left and skipped) and the
  verdict gate `interrupt`; the verdict is INTERRUPTED, or FAILED when
  something had already failed, never PASSED. The run then exits 130. A
  signal while the results are gathered no longer loses the record. A
  second Ctrl-C cuts the cleanup short and still writes the record; a third
  stops at once. After an interrupt the run does not measure bucket sizes or
  read Prometheus; it still lists the datagen prefix once for the corpus
  observation. `report --list` shows such a run as Interrupted. Inside
  the cluster lease the signal still waits for the shared change to finish
  first. SIGHUP is not handled.
- **A continuous run notices that its namespace is gone.** It used to keep
  looping to the end of its window and its settle wait after `destroy`,
  then stopped streams by name, which after a redeploy were the new
  deployment's. It now reads the namespace every 30 s in the window and
  the settle wait, around each benchmark round, before each maintenance and
  compaction round and before stopping its streams; when the namespace was
  deleted, is being deleted or was deleted and deployed again (or three
  reads in a row fail), it stops at that read, exits 1 and saves the record
  with `abort_reason`.
- **`run --generate` on a multi-cycle run is refused (exit 2).** It generated
  the whole corpus before the cycle loop, then cycle 0 again under the same
  file names. On a bucket the deployment owns, cycle 0 cleared the whole
  corpus (a wasted generate); on one it did not create, `--allow-stale-bronze`
  left most of it beside cycle 0's slice and cycle 0's silver read both,
  about (2 - 1/cycles) times the rows with exit 0; without the flag, cycle 0
  refused the files the run had just written (exit 1). A multi-cycle `run`
  generates one slice per cycle without `--generate`.
- **A multi-cycle `run` no longer fails at cycle 1.** Its datagen wait used
  a name only the single-shot generate defined, so every multi-cycle run
  without `--generate` stopped with "cannot access local variable" (exit 1).
- **`run --generate` no longer runs its stages over a datagen Job that
  failed.** Its progress poll stopped when no datagen pod was active, which
  a Job whose pods failed after their retries also is, printed "Datagen
  completed" and ran the pipeline over a partial corpus. It now exits 1
  ("Datagen did not complete: N/M pods succeeded").
- `run --continuous --skip-generate` no longer journals a "Datagen started"
  event for a datagen it did not start.
- Trino compaction of the Customer 360 silver table no longer fails with
  "Exceeded limit of 100 open writers for partitions" when it rewrites files
  in more than 100 `interaction_date` partitions, as the silver of the one
  recorded continuous Customer 360 run did (n=1). A silver table with more than 90 partitions is now
  compacted in chunks of at most 90, after a read of its partition values.
  This also applies to batch runs, whose single statement happened to
  succeed (batch silver holds a few large files per partition): the
  pre-benchmark maintenance of a batch Customer 360 run on Trino now runs
  one partition read and several `optimize` statements where it ran one,
  which can change the recorded maintenance time. Compaction outcomes count
  tables, not statements, and `experiment.effective_maintenance` names each
  table whose compaction failed in `reasons` and
  `detail.compaction_failures`. The maintenance policy id and the effective
  maintenance `id` are unchanged.
- Trino compaction of AML `silver.transactions` and `silver.account_statements` runs one month to merge per statement, avoiding "Query exceeded per-node memory limit"; maintenance time can change.
- Continuous AML on Spark 4.1 with Iceberg no longer fails its silver
  stream with an internal error ("No plan for TableReference") on the
  entity and account MERGEs. Every MERGE in the AML silver stream whose
  source reads a table now reads a materialised copy of it: the same rows,
  computed once.
- Continuous AML `silver.entity_profiles` now leaves `total_sent_usd` NULL
  for an entity that never sent and `total_received_usd` NULL for one that
  never received, as batch does; it wrote 0.00. A continuous deployment
  that started before the fix keeps 0.00 on the rows it already wrote. No detection
  rule, score or query reads these two columns, and `passthrough_ratio`
  was already equal.
- **Spark Thrift on Spark 4.1 with Iceberg 1.11 loaded the 4.0 runtime.**
  Thrift picked the Iceberg runtime from the Spark version alone, so it loaded
  `iceberg-spark-runtime-4.0` while the pipeline jobs loaded the native
  `iceberg-spark-runtime-4.1`. Thrift now makes the same choice as the jobs.
  Thrift deployments on Spark 4.1 with Iceberg 1.11 change runtime jar on
  their next deploy; other combinations are unchanged.
- **A failed datagen upload completion is retried.** A continuous-delivery
  file whose multipart upload or completion fails is rebuilt from the same
  rows and uploaded again on the same key after 2, 4 and 8 s before the pod
  fails (it used to fail at once and restart from scratch); the S3 client's
  own per-request retries go from 1 to 3 (every request, parts and single
  PUTs included). Output bytes are unchanged. Each pod logs
  `delivery_mode=<batch|continuous>` and reports it in its metrics line,
  and the run record's `datagen_fleet.delivery_mode` carries it.
- **The datagen pods honour `platform.storage.s3.path_style`, `verify_ssl`
  and `ca_cert`.** The Rust S3 client used path-style addressing, plain HTTP
  and the system CAs whatever the config said; it now reads `S3_PATH_STYLE`,
  `S3_VERIFY_SSL` and `S3_CA_CERT` (already rendered into the Job), allows
  plain HTTP only for an `http://` endpoint, and exits 2 on a value it cannot
  read or a CA file it cannot load. With `path_style: false` the bucket goes
  into the endpoint's host (virtual-hosted requests); a FlashBlade or MinIO
  config must keep `path_style: true`, which the datagen pods now honour
  like Spark does.

### Known limitations
- **Continuous throughput is the configured offered load, not a measured
  system capacity.** `sustained_throughput_rps` in the report reads the rate
  the generator fed the pipeline (scale divided by `run_duration`); it is not
  how fast the system could have gone unbounded. `intake_limit` and
  `pipeline_saturated` tell the reader when the system was below the
  configured rate. Batch throughput is a real measured number. Batch and
  continuous numbers are not comparable with each other; see
  `docs/data-generation.md`.
- **AML silver differs batch vs continuous on columns whose value depends on
  arrival order** (`accounts.currency`/`current_balance`,
  `account_statements.bal_before`/`bal_after`,
  `entity_profiles.first_seen_ts`/`active_span_days`). Row counts and
  transactions match; detection rules read none of these columns, so alerts
  are unaffected. Not a release blocker because batch and continuous are
  different workload shapes and are not compared; a run under one mode is
  reproducible under the same mode. Fixed in 1.7.1.
- **The capacity preflight sums free capacity across nodes.** Ten nodes
  with 12 cores free each read as 120 free cores, though each holds one
  8-core pod; only the largest pod is checked against a single node. The
  scratch check sums the StorageClass's `CSIStorageCapacity` and ignores
  `maximumVolumeSize`, and with scratch disabled the executors' node disk
  is not checked.
- **Continuous above scale 50 should generate first.** The autosizer sizes
  the datagen Job to about 90% of the CPU left after the always-on pods,
  and the capacity preflight counts it beside the streams, so a continuous
  `run` that generates its own corpus above scale 50 is refused, or
  admitted only with its streams capped hard, depending on the cluster.
  Run `lakebench generate`, then `lakebench run --skip-generate` within an
  hour of generation finishing (the finished datagen Job is deleted after
  3,600 s, and an absent Job is counted as still running).
- **Per-job executor overrides and the driver overrides are not counted**
  in the sizing figures yet; `config show` and `info` say so when a config
  sets one.

- **polaris + AML + continuous is intermittent at scale 1.** The
  silver-stream Spark driver exits silently inside the 30-minute window and
  the Spark Operator auto-resubmits a second driver; the continuous gate
  correctly refuses the run on "silver-stream was resubmitted inside the
  window". Observed on polaris-iceberg-spark-trino and
  polaris-iceberg-spark-thrift. Not observed on hive-based catalogs, on
  polaris-iceberg-spark-none (catalog-only), or on polaris + C360 +
  continuous. Under heavy parallel load, the failure was seen in 3 of 4
  attempts; the solo reproduction rate is not characterised precisely.
  No `StreamingQueryException`, `OOM` or `SIGTERM` found in the preserved
  first-driver logs. Scoped to all three streaming jobs (bronze-ingest,
  silver-stream, gold-refresh) because the continuous gate refuses a run on
  any one of them rotating inside the window, so none can usefully
  auto-rerun there. Workaround: retry the run. Root cause and fix tracked
  for v1.8.
- **`report.html` embeds the configured S3 endpoint value verbatim.**
  The field `S3 Endpoint` inside the `config-item` div is rendered
  as the live config value, so a shared `report.html` leaks the operator's
  endpoint. Access and secret keys were already excluded from
  `config_snapshot` and never rendered. Fix tracked for v1.7.x: remove the
  `S3 Endpoint` field from the report config section and redact the raw
  endpoint value in the `config_snapshot` and the system-identity
  fingerprint.

- **AML silver tables differ between batch and continuous modes on the same
  corpus.** Row counts match, and `transactions`, `entities` and
  `counterparty_edges` are byte-identical; `accounts.currency` and
  `accounts.current_balance`, `account_statements.bal_before` and
  `.bal_after`, and `entity_profiles.first_seen_ts` and `.active_span_days`
  differ because the continuous silver writer picks arrival-order values
  where the batch writer picks a stable key. Each mode is internally
  consistent, and each mode's queries return the expected results against
  the mode's own silver. A reader must not compare batch AML numbers
  against continuous AML numbers on these tables; invariant 2 covers that
  case. Fixed in 1.7.1.

- **FQ4 answers differ between batch and continuous on the same corpus.**
  FQ4 reads raw rows from `counterparty_edges`; batch silver dedups edges,
  continuous silver appends one edge per micro-batch, so counts differ by
  the number of micro-batches. Each mode is internally consistent and
  passes its own expected-results check; cross-mode comparison of FQ4 is
  the uncovered case. Fixed in 1.7.1.

- **W5/W6 AML screening alerts grow roughly quadratically with scale.** At
  scale 1 W5 produces 126 alerts; at scale 100, 7.6M (false-positive rate
  rises from 0.61 to 0.999 while recall stays near 0.8). The cause is a
  fixed name pool in the datagen generator (79 first names, 77 last, 50
  company heads, 18 descriptors) against a watchlist and background that
  grow linearly with scale, so namesake collisions grow with the product.
  Each individual scale's run passes; cross-scale totals, the TM funnel and
  gold time are distorted. A reader must not compare W5/W6 alert totals
  across scales. Fixed in 1.7.1 (name pools grow with population).

## [1.6.0] - 2026-09-30

Lakebench 1.6 makes the workload a first-class part of an experiment and
runs two workloads, Customer 360 and AML, through the same composable
architectures. Every run now records what produced it, `compare` refuses to
compare runs whose workload results differ, and several published metrics
changed meaning. Most numbers recorded by 1.5 and earlier are not comparable
with 1.6; the first section lists why.

### Known limitations
- **AML recall is uncalibrated.** v1.6 publishes no held-out Level-2
  result. Recall and precision are in-sample on the calibration corpus, and
  the report labels the column "Recall (uncalibrated)". The registered
  held-out looks are deferred to v1.7.
- **No v1.6 performance baselines.** The performance re-baseline and the
  AML frozen-generator performance and size measurements are deferred to
  v1.7. No pinned config is required by the release gate's
  `perf-baselines` check.
- **Trino OPTIMIZE hits the open-writer limit.** On the Customer
  360 continuous silver table `silver.customer_interactions_enriched`,
  Trino OPTIMIZE can fail with "Exceeded limit of 100 open writers for
  partitions".
- **AML continuous recall is not scored.** Stopping the streams
  can interrupt a gold-refresh tick; scoring refuses the partial pass.
  Batch recall is unaffected.
- **AML gold-finalize is slow.** At scale 10 it took 4,100 s of its
  5,400 s auto timeout on 4 executors (n=1); at
  scale 100 a skewed detection stage takes minutes per task. Under heavy
  parallel load, raise `--timeout` for AML runs at scale 10 and above.
- **AML expected-size estimate is off.** The financial bronze size
  estimate in `config/scale.py` does not match the generator, so the AML
  `scale_ratio` score is not exact.

### Read this first: comparability with earlier releases
- **Datagen file size is fixed at 64mb and datagen scale is banded.**
  `workload.datagen.file_size` accepts only `64mb` (any case); another size
  is refused by `deploy`, `generate` and `run` (destroy and clean still load
  such a config, with a warning). Batch runs published at another size are
  not like-for-like at the bronze stage. Datagen scale has per-workload
  limits: AML supported to 300, unverified to 800, refused above; Customer
  360 supported to 300, unverified to 600, refused above.
- **Datagen batch delivery now actually runs batch.** Since the Rust
  default flipped to continuous, `datagen.mode: batch` silently ran
  continuous. Datagen timings recorded as batch before this fix were
  continuous.
- **Before 1.6.0 no Iceberg `expire_snapshots` or `remove_orphan_files` and
  no Delta `VACUUM` ever ran, so no earlier
  continuous number is comparable with 1.6.** Trino refused every Iceberg
  expiry and orphan removal below its 7-day system minimum, the Spark Thrift
  form failed a parameter-binding error, Delta VACUUM sent its retention
  override in a separate session so it never applied, and `exec_sql`
  discarded the exit code, so every failure was reported as a success. Batch
  pre-benchmark maintenance was therefore compaction only. Continuous
  freshness, rows/s, in-stream QpH, object counts and maintenance-value
  numbers from 1.5 or earlier are not comparable with 1.6.
- **`maintenance_policy_id` is stamped into every metrics.json**
  (`m2-2026-09-26` for 1.6; a record without it reads as `m1-legacy`, and a
  `--skip-maintenance` or `--local` run as `<id>+skipped`). The perf gate and
  `reproduce` refuse to compare, record or verify across policies; `compare`
  warns. report.html shows the policy.
- **Delta continuous ships with no effective table maintenance in 1.6.**
  Delta VACUUM keeps Delta's 7-day retention while streams are live, so a
  continuous run shorter than 7 days removes nothing (Trino runs VACUUM and
  it is recorded as `ran_no_effect`; Spark Thrift does not run it and records
  `not_supported`), and no bounded OPTIMIZE runs in continuous mode. The run
  header, the evidence (`effective_maintenance.known_limitations`) and the
  report state this, and the report shows the in-window QpH trend
  (`qph_trend`: first and last round, silver file count at each end) so a
  median over a falling series is not read as steady state.
- **Iceberg metadata cleanup after commit.** Every Iceberg table Lakebench
  creates sets `write.metadata.delete-after-commit.enabled=true` and
  `write.metadata.previous-versions-max=50`; before, old `metadata.json`
  files grew by one per commit for the whole of a continuous run. Tables in a
  reused catalog keep their old properties until they are recreated.
- **Query sets changed, so QpH from 1.5 does not compare.** Total-order
  tiebreakers (FQ2, FQ3, FQ4, FQ6, FQ7, FQ8, IQ3, Q4), Q1 averaging
  purchases only, the AML set growing to 12 benchmark queries, and QpH as
  the median of 3 samples all move the `query_set_id` or the estimator;
  `compare`, the perf gate and `reproduce` refuse QpH across them.
- **Perf-gate baselines and reproduce packages recorded before 1.6 refuse
  every run** until re-recorded: they carry no experiment identity, no
  maintenance policy and poll-quantized stage times.
- **Continuous metrics changed meaning** (details under Changed): scores are
  taken only inside the measurement window, `ingest_ratio` is measured
  against the rows the trickle released, the headline freshness is the
  scored `data_freshness_seconds`, and the default trickle and maintenance
  interval are derived from the window.
- **Batch stage times come from the Spark driver's end** (`finishedAt`), not
  the 15 s monitor poll, so every stage reads up to 15 s shorter than in 1.5.
  The perf gate refuses to compare runs with different timing sources.
- **Run, stage and round timestamps are UTC** with the zone in metrics.json
  (they were naive host-local time). Run ids keep their host-local form.
- **AML results are not comparable with 1.5.** The generator is frozen at
  `datagen-v2-rs-0.3`, answer keys are out of bronze, W5/W6 are now scored
  screens, W7 uses the June 2026 FATF lists plus synthetic corridors, and
  several rules changed their targets (see Breaking changes).

### Behaviour changes you must know
- **Every result is an identified experiment.** Every metrics.json carries
  an `experiment` block: workload and version, generator model version,
  corpus id, seed and scale as read from the datagen pods, the datagen image
  and its pod digest, recipe and component versions, query access path
  (`catalog` or `direct_storage`), mode, requested and effective
  maintenance, stages and detection rules executed or skipped with the
  reason, Lakebench-imposed limits and which of them bound, the support
  state, repetitions, and a result fingerprint per benchmark query. It also
  records `provenance` (Lakebench version, git commit, dirty flag).
  report.html shows the block.
- **Result fingerprints.** After the timed samples each successful query is
  run once more, untimed, and its rows hashed after canonicalising cells
  across engines (fingerprint spec `rf2`: exact digits, approximate sums for
  declared DOUBLE columns with a row-keyed weighted sum, volatile columns
  fingerprinted by NULL-ness only). Two runs with equal fingerprints
  returned the same rows.
- **`lakebench compare` verdicts.** Three verdicts: **comparable** (the
  workload results are equivalent), **NOT COMPARABLE** (different
  experiments, different results, a failed run, or a record without the
  experiment block; deltas and winner withheld, exit 1) and
  **comparability not established** (a side has no checked results:
  `--skip-benchmark`, a recipe without a query engine, or a continuous run
  without a settled result check; raw numbers shown, no deltas, exit 0). A
  comparable pair whose execution conditions differ (effective maintenance,
  access path, system, benchmark iterations, in-stream round count, bound
  limits) is labelled not like-for-like. `comparison.json` gains `verdict`,
  `comparable`, `like_for_like`, `condition_differences`, `support` and
  `refusals`; CSV output gains `comparable` and `like_for_like`.
- **Support states: supported, unverified, unsupported.** The state is
  computed per workload x recipe x mode (`lakebench.config.support`). A
  combination the architecture, workload or mode checks refuse is
  **unsupported** and refused at config load, or by `run` for the mode
  `--continuous` selects, before anything is deployed. A valid combination
  is **unverified** unless the release validation record
  (`config/validated_combinations.yaml`) lists it with the live runs that
  validated it, in which case it is **supported**. The record ships empty in
  this tree; it is filled from release validation runs. The state is frozen
  at run start and stamped in the evidence; a checkout with local changes,
  or one whose git status is unknown, is never stamped supported, and local
  runs are never supported. `config recipes` lists the state per workload x
  mode, `config show` prints the config's state, and `compare` shows both
  sides'. The recipe and support tables in the docs are generated from this
  code.
- **Lakebench-imposed caps are labelled.** The experiment block lists the
  limits that applied (executor ceiling and per-job `max_executors`, the
  continuous concurrent executor budget as requested vs granted, the
  pre-benchmark maintenance budget, the continuous trickle, in-stream
  benchmark rounds and iterations, TM alert capacity, `w1_max_vertices`) and
  which of them bound, so a bound figure is not read as infrastructure
  performance.
- **AML generator image.** `images.datagen` defaults to
  `docker.io/sillidata/lb-datagen:1.6.0` (digest
  `sha256:5fda9025fb9b455b390e1138d82e9f6ef16d214dfa9419815be0111d2f6fce0a`),
  generator `MODEL_VERSION` `datagen-v2-rs-0.3`, the release build of the
  same datagen_rs source as the validated `034f998` image. Its output is
  byte-identical to the frozen generator built from 9382420 source (seed 43,
  141/141 objects, across thread and pod counts). It adds the per-pod memory
  model with a 16Gi cap, fixed 64 MB files and delivery-mode forwarding
. It is the functional default, not the registered-look
  image: registered-look, D8, A6 and calibration corpora pass an explicit
  frozen digest via `--generator-image` (see `docs/internal/aml-protocol.md`).
  The pinned image is the reproducibility unit; bit-exact output holds within
  one build environment. A corpus from an image before the freeze is
  pre-freeze. Prior tags `e14d0fd`, `30603b1`, `9382420` (digest
  `sha256:2faad1cc0252a165a56361a06f159a62ba7c4387c83adfb7c46fe260af23b8f2`,
  live-metrics Pushgateway push), `b6f2905`, `25f1aa8`, `7c24641` and
  `0a83acd` are recorded in
  `src/lakebench/config/schema.py::ImagesConfig.datagen` (`7c24641` digest
  `sha256:c5a6bc80d89341b0753dccd39abb5cbe835ed31a9d14b33e0989863cca774f3b`);
  all but `e14d0fd`
  were deleted from docker.io and must be rebuilt from source.
- **Config contract (v1.6).** `workload` is a top-level key; the old
  `architecture.workload` block still loads with a deprecation warning, and
  setting both with different values is an error. `continuous` is the
  canonical pipeline mode (`mode: continuous`, `pipeline.continuous`,
  `run --continuous`); `sustained` is accepted as a deprecated alias, and
  metrics files keep recording `pipeline_mode="sustained"`. Refused at load:
  the financial (AML) workload on Delta (its scripts write Iceberg only),
  workload schema `custom` (it silently ran the Customer 360 queries), and
  Iceberg 1.11+ on a Java 11 Spark image. `images.prometheus`,
  `images.grafana`, `observability.reports`, `table_format.iceberg.file_format`
  and the Iceberg and Delta `properties` never did anything; a non-default
  value now warns, and they are removed in v1.7. `pipeline.pattern` is
  deprecated.
- **`--local` refuses AML and continuous.** Local mode ran the Customer 360
  batch job map whatever the config named; it now refuses an AML config and
  continuous mode instead of running Customer 360 under the AML label.
- **Default bucket names are `<name>-bronze`, `<name>-silver`, `<name>-gold`.**
  They were the fixed `lakebench-bronze/-silver/-gold`, which collide on
  stores where bucket names are global (FlashBlade across accounts, AWS) and
  were shared by every deployment on one store. A deployment that relied on
  the old defaults must set `platform.storage.s3.buckets` to the old names
  to keep using (and to destroy) its existing buckets.
- **A benchmark that raises fails the run.** It used to print a warning and
  leave the run successful. Now `run` exits non-zero, no QpH is recorded,
  the journal records the benchmark as failed, and metrics.json and the
  report carry `benchmark_error`.
- **A benchmark query that returns no rows fails the run** unless the query
  is declared allowed-empty (IQ2 and IQ4). In continuous mode only the last
  in-stream round is held to this, and an empty Q9 there fails the gate.
- **Recipes without a query engine skip the benchmark** and `run` exits 0;
  no QpH is recorded. `benchmark_type` names the engine that actually ran
  (it read `trino_query` on DuckDB and Spark Thrift).
- **The observability stack is shared.** kube-prometheus-stack installs
  cluster-wide objects, so `deploy` installs one release into the
  `lakebench-observability` namespace only when none exists, never upgrades
  an existing one, and `destroy` never uninstalls it (a release an older
  lakebench put in the deployment's own namespace is still removed).
- **Cross-engine row counts were wrong on Spark Thrift and DuckDB.** Thrift
  reported `n + 3 x ceil(n/100)` rows (beeline options after `-e` were
  dropped, so it printed its table format), and DuckDB reported 2 rows for
  any query slower than 2 s (its progress bar broke the JSON payload and the
  executor counted lines). Both are fixed; a DuckDB payload that cannot be
  read is now an error. Every engine session is pinned to UTC.
- **AML sanctions and PEP screening is scored (generator 0.3).** Every AML
  corpus now carries a synthetic, dated watchlist
  (`bronze/watchlist.parquet`: a sanctions list in two versions and a PEP
  list) and planted payments to listed parties under name variants
  (aliases, token-order swaps, one-letter typos, other romanisations,
  dropped legal suffixes), with namesake decoys. W5 is a fuzzy screen
  against that list at transaction time plus a rescreen on each list
  version; W6 uses the same screen for PEP payments (MED priority at
  $10,000 or more, LOW below). Both are scored for recall and precision
  against planted `sanctions_match` / `pep_match` instances. The packaged
  `sanctions_list.json` and `pep_list.json` are gone; a corpus without a
  watchlist reports W5/W6 as not run, and continuous mode skips them.
  This is a benchmark screening workload, not a production sanctions list.
- **No answer keys in the AML party zone.** `party.parquet` no longer has
  `sanctions_status`, `pep_status` or `initial_risk_score`, the customer
  risk rating no longer uses PEP status, and `silver.entities` leaves those
  three columns NULL.
- **FATF list refreshed to June 2026.** `high_risk_jurisdictions.json` now
  holds the FATF lists published on 19 June 2026, dated and sourced. No
  generator home country is on them, so W7 also alerts on the generator's
  synthetic high-risk corridor countries (`synthetic_corridors.json`,
  labelled synthetic; the same pool the generator plants
  `corridor_high_risk` from). W7 recall and alert volume are not comparable
  with 1.5.
- **AML scoring counts are truthful.** `recall.json` gains
  `typology_counts` (scored, partial, no rule, rule skipped, rule error) and
  a `rules` list with each rule's status, reason, target and alert count;
  the CLI prints "N of M typologies scored" from them (it claimed 15 when 6
  were scored). The generator's AML category is reported as
  `workload_category`, beside `designated_rules`.
- **Maintenance statements report their real outcome.** Trino sends
  `SET SESSION <catalog>.<procedure>_min_retention` in the same submission
  as the procedure, Spark uses a `TIMESTAMP` literal, Delta VACUUM and its
  retention setting run in one submission, and a failed or timed-out
  statement is reported as one. The effective maintenance in the evidence
  comes from what each call did, per operation: `ran`, `ran_no_effect`,
  `not_supported`, `skipped_by_user`, `failed` or `not_run`, with the
  applied retention of each operation. Batch Customer 360 maintenance no
  longer targets the continuous-only `bronze_raw` table.
- **Retention floors.** Orphan-file removal never runs below 24 h 10 min on
  any engine or path. Iceberg snapshot expiry is floored at 1 h while
  streams are live. Delta VACUUM keeps Delta's 7-day default while streams
  are live.
- **`retention_threshold` is strict.** It must be a whole number and one
  unit (`s`, `m`, `h` or `d`, for example `30m` or `7d`); anything else,
  such as `1.5h` or `30min`, is rejected when the config loads. A
  continuous Iceberg config that sets it below the 1 h live-stream floor
  warns once; the applied value is recorded in `continuous.retention`.
- **Pre-benchmark maintenance has a 30-minute budget.** Expire, orphan
  removal and compaction share it; the first statement timeout or the
  deadline stops the rest and the benchmark runs anyway. When maintenance
  stopped early or ran beside live stream apps, the perf gate excludes
  post-maintenance QpH, and if no QpH metric is left to gate the verdict is
  `NOT_COMPARABLE` (exit 2, never a pass, never a baseline). Continuous
  maintenance and compaction statements get `min(600 s, interval / 2)` each,
  a round is capped at half the interval and at the time left in the run,
  and the next round resumes at the table after the one that timed out.
- **Destroy never deletes files outside proven ownership.** Trino
  `DROP TABLE` deleted every file an Iceberg (Hive) table referenced,
  including `add_files`-registered datagen files, and a managed Delta
  table's directory, before the bucket step's ownership checks ran; a
  bucket destroy refuses (another deployment's shared bronze, an adopted
  bucket holding data, an untagged pre-provisioned bucket) still lost the
  files its tables pointed at. Destroy now runs Trino
  `system.unregister_table` (catalog entry only). Spark Thrift keeps
  `DROP TABLE` for Iceberg (no PURGE) and drops a Delta table only when
  `DESCRIBE DETAIL` puts its location in a bucket destroy will empty. Tables
  left registered, and the refused buckets whose files remain, are printed
  and journaled. On Polaris, when destroy deletes the namespace the drops are
  skipped (the catalog's only state is in the deployment's PostgreSQL PVC),
  so a polaris+trino destroy no longer exits 1 on refused purge-drops.
  `write_delta_table` refuses to create a managed table over an existing
  `_delta_log` that is not in the catalog.
- **Destroy on stores without bucket tagging (FlashBlade) empties only
  buckets it can prove it owns.** It used to empty any config-named bucket
  whose name matched the deployment. It now also needs the namespace's
  created-buckets record, or the record of a bucket deploy adopted while
  empty; an unrecorded match is left in place and reported FAILED, and
  `--force-legacy` empties it but never deletes it. `clean` and the
  continuous reset follow the same rule.
- **Destroy semantics.** Destroy runs no table maintenance (no Iceberg
  expire or orphan removal, no Delta VACUUM). It empties the buckets it owns
  and then deletes only the ones this deployment created (the
  created-buckets record on the namespace plus the ownership checks);
  pre-provisioned, adopted and `--keep-buckets` buckets are emptied and
  kept. If a recorded bucket cannot be deleted, the namespace is kept as
  the ownership record. Destroy waits until the namespace is NotFound
  before reporting it deleted (`--namespace-timeout`, default 600 s; exit 3
  when it is still terminating). The scratch StorageClass is never deleted.
- **Spark Operator watch-list edits keep the installed chart and hold the
  lease.** A watch-list add or remove could move the shared operator to the
  repository's latest chart or to a tenant's pin; it now pins the chart of
  the installed release. `validate` treats an unwatched namespace as
  advisory before the first deploy, no longer suggests a raw
  `helm upgrade --reuse-values` (which bypassed the lease), and fails when
  the credentials cannot upgrade the operator release.

### Breaking changes (read before upgrading)
- **Unknown config keys are rejected.** Every config model forbids extra
  keys, and the error names the full path (for example
  `architecture.workload.datagen.scael: Extra inputs are not permitted`).
  Keys that were silently ignored now stop every command, including
  `destroy`. Keys removed in earlier releases warn and are ignored:
  `images.pull_secrets`, `table_format.hudi`, `medallion.silver.strategy`,
  `customer360.channels` / `event_types` / `quality_distribution`,
  `scratch.create_storage_class`. If an old deployment's config has a
  mistyped key, delete it before running `destroy`: correcting it can
  retarget the namespace or buckets.
- **Double spellings are errors.** Setting both `processing` and
  `pipeline`, both `continuous` and `sustained`, or both `schema` and
  `schema_type` used to drop one silently.
- **`-f` means `--file`.** `destroy`/`clean`: use `--force`, `--yes` or
  `-y`. `init`: `--force`. `results`: `--format` / `-o`. `logs`:
  `--follow` / `-F`. Admin commands gain `-f/--file`.
- **`-f` on `destroy` and `clean` exits 2 in every context** and names
  `--force` / `-y`; `LAKEBENCH_LEGACY_SHORT_F=1` restores the old meaning
  with a warning. `results` rejects a `--format` that conflicts with `-f`.
- **`results -o json|csv`** prints plain stdout.
- **A continuous run whose explicit settings cannot produce continuous
  evidence is refused at start:** a `max_files_per_trigger` that would offer
  the whole corpus before the window ends, a `retention_interval` or
  `compaction_interval` whose first round cannot run inside the window
  (unless maintenance or compaction is turned off), and a window shorter
  than three gold refreshes (`run_duration` below `3 x
  gold_refresh_interval`, 900 s at the defaults). Each refusal names the
  setting and the value to use.
- **A c360 continuous run over existing state refuses without
  `--force-reset`.** It lists the non-empty tables, checkpoints and raw
  prefixes it would delete.
- **AML rule targets changed.** W3 now searches 2-5 hop cycles and is
  scored against `cycle`; W4 is scored against `rapid_layering`; a new
  chain rule `W17_layering_chain` is scored against `stack`; W2 adds a
  per-beneficiary alert kind; W5 and W6 are scored (see above). Per-rule
  recall and FP are not comparable with earlier runs.
- **AML customer-scoped rules alert on customers only.** W2, W5, W6, W7 and
  W8 drop alerts whose entity is not a customer in `silver.entities`; the
  graph rules (W1, W3, W4, W17) stay unscoped. Alert counts and false
  positives drop.
- **QpH is the median of 3 samples per query.**
  `architecture.benchmark.iterations` defaults to 3 and `lakebench run`
  passes it to both benchmark rounds (it was ignored before, and the
  `benchmark --iterations` default of 1 overrode the config). Throughput
  QpH counts executions. The perf gate and `reproduce` refuse a QpH taken
  with a different sample count.
- **The AML benchmark set grew from 8 to 12 queries** with the investigator
  class (IQ1-IQ4). `metrics.json` records a `query_set_id`; `compare` and
  `reproduce` refuse QpH across different or unrecorded sets.
- **Metric meanings changed:** `maintenance_value_pct` is null when not
  measured (was 0.0); c360 `customer_recency_score` and Q6 are anchored to
  the data clock, not the run date (Q6 and c360 QpH not comparable);
  silver-build `output_rows` in incremental mode is per cycle; a scale ratio
  of 0 means not measured and no longer shows as Complete.
- **Customer 360 gold KPIs were wrong and are corrected.**
  `avg_transaction_value` averaged every interaction (82% have amount 0.0)
  and read about 5.5x low; it is now per transaction, as is
  `avg_estimated_ltv`. `avg_page_views` and `avg_time_on_site_seconds` were
  about 1.9x low and are now per visit. `support_tickets_created` merged
  colliding ticket ids and is now one per support interaction. Multi-cycle
  batch counted every earlier cycle again; each appending cycle now reads
  only its own bronze files. Gold KPIs from 1.5 are not comparable.
- **Release process (maintainers):** one `release.yml`; PyPI uploads after
  the GitHub Release; tags must be the normalised version and on `main`;
  `uat/results-<version>.md` with a `# UAT results <version>` heading and a
  results table citing at least one run id, each resolving to a
  metrics.json, is required; pre-release and dev versions are refused. See
  `docs/releasing.md` for the repository settings the gates depend on.

### Changed: continuous mode
- **Scores come from inside the measurement window** (from every stream
  running to `run_duration` later), from the timestamped stream log lines.
  Rows taken in before the window are recorded as `pre_window_rows` and
  kept out of every score. Totals are cut at the window's end.
- **The continuous gate needs genuinely continuous output:** bronze took
  rows in at least two batches inside the window (the last past its
  halfway point), silver committed at least two micro-batches and gold
  refreshed on new silver data at least twice after bronze's first write,
  gold freshness was measured, and every stream was still RUNNING when the
  window closed. A corpus drained before the window opened, or a stream that
  restarted inside it, fails the run.
- **Stream submission failures are visible.** Each stream's
  SUBMISSION_FAILED retries (for example a truncated Maven download, named
  by artifact and byte count) are printed, journaled and recorded per
  stream with the seconds they cost (`submission_retry_seconds`). An error
  or Ctrl-C after submission fails the run and stops the streams.
- **Result check after settle.** After a run that passed, the streams keep
  running (up to 1,800 s, not scored) until the whole corpus has reached
  gold, then stop, and the query set is fingerprinted over the settled
  tables. Those fingerprints are the continuous run's results; without them
  (corpus did not settle, `--skip-benchmark`, gates failed, AML continuous)
  results are not established and the perf gate and `reproduce` refuse the
  run.
- **`ingest_ratio` is bronze rows over the rows the trickle had released**
  by the window's end, so `pipeline_saturated` means bronze fell behind what
  arrived. The whole-corpus share is kept as `corpus_ingest_ratio`; records
  without a window or file count fall back to it. `corpus_drained` means
  every datagen row reached bronze and was committed by silver.
- **The trickle is derived from the window.** `max_files_per_trigger`
  is unset (auto) by default: the most files per trigger, up to 50, whose arrival
  lasts 1.2 x `run_duration`, from the nominal corpus size (at the defaults,
  2 files for c360 scale 1, 22 for scale 10, the 50 cap from scale 100). The
  run prints the trickle and records it in the evidence as a
  Lakebench-imposed limit.
- **The default maintenance interval fires inside the window.** An unset
  `retention_interval` resolves at run start to `run_duration / 3` within
  300-7,200 s (600 s at the default window), and automatic compaction to
  twice that. It used to default to 1,800 s, equal to the default window, so
  a defaults-only continuous run never ran maintenance. The resolved values
  are recorded.
- **Headline freshness is the scored `data_freshness_seconds`.** The
  in-stream probe (query time minus the newest gold event date) measures
  the corpus's event-time position, not pipeline freshness; it is recorded
  as `gold_event_age_seconds` per round and
  `query_time_event_age_seconds` in the scores, and no longer headlines the
  score line. Old files with `gold_freshness_seconds` load into the new
  field. On Spark Thrift the probe uses Spark SQL.
- **QpH round counts are recorded.** Scores carry `composite_qph_rounds`
  and the experiment block `limits.benchmark_rounds`; a pair with different
  in-stream round counts is comparable but not like-for-like, and a run
  whose every in-stream round failed is refused against an in-stream
  baseline. A round that cannot fit the time left is skipped once and
  journaled.
- **Stage fields are measured or absent.** Unmeasured freshness, gold
  unique rows and failed table-health probes are absent instead of 0.0 or
  -1; `output_rows` comes from the logs.
- **Delta continuous works.** hive-delta recipes failed at reset
  (`REQUIRES_SINGLE_PART_NAMESPACE`); every table is now named in the
  pipeline catalog and Delta bronze is created at
  `s3a://<bronze>/warehouse/default.db/bronze_raw`.

### Changed: batch mode and measurement
- **Stage times come from the Spark application.** A stage ends at the
  driver container's `terminated.finishedAt` (else the SparkApplication's
  `terminationTime`), mapped to this host's clock by the API server's clock
  offset; elapsed runs from the SparkApplication's creation. An implausible
  end falls back to the poll. Each job and stage records `timing_source`
  and `timing_resolution_seconds`; the stage poll is 5 s. Time to value runs
  to the gold application's end and includes Lakebench's work between
  stages.
- **Batch SUBMISSION_FAILED retries are printed, journaled and recorded**
  (`submission_failures`, `submission_retry_seconds`); the stage line says
  how long it waited on failed operator submissions.
- **Storage settle wait before the post-maintenance round.** In
  batch mode a probe query runs every 60 s after maintenance until two
  consecutive probes agree within 10% and neither is slower than the
  pre-maintenance median by more than the tolerance (widened by twice the
  median absolute deviation of the pre-maintenance samples, capped at 20%),
  capped at 45 minutes. It is skipped when no maintenance statement ran.
  `maintenance_value_pct` is null when the wait is skipped, capped or
  fails. The wait is not a stage and is not counted in time to value.
  Configured under `architecture.benchmark.maintenance_settle`.
- **Customer 360 expected-result checks.** Bronze-verify and gold-finalize
  log facts that `metrics/c360_correctness.py` checks against the
  generator's semantics (bronze rows equal to the rows datagen was sized to
  write, bronze to silver rows, KPI identities, benchmark row counts); the
  verdict is recorded in metrics.json as `c360_correctness`. A missing or
  failed fact collection is unknown, never a pass.
- **Delta silver is clustered by `interaction_date` before the write.** It
  wrote about one file per task per day (tasks x 366); Q3/Q6 on
  hive-delta-spark-thrift exceeded the 300 s timeout. Set
  `spark.lb.silver.distribution_mode=none` for the old layout.
- **Delta table-health file counts** come from `DESCRIBE DETAIL numFiles`
  on Spark Thrift; on Trino they are reported as unavailable. A Delta file
  count never measures a compaction.
- `config show` and `info` report the peak requested resources from
  `compute_peak_requirements()` plus co-resident services (at scale 1 the
  pipeline alone peaks at 36 cores / 512 GB); `info` names the workload by
  pipeline mode and says what the datagen mode means for a continuous run.
  `recommend` never sizes Spark below the peak request.

### Added
- **`lakebench admin` subcommand tree.** Cluster admins run one-time setup
  (`install-spark-operator`, `install-scratch-storage-class`) before
  developers can `deploy`. Also `status`, `doctor`, `release-lock`,
  `migrate-deployment` (for legacy pre-ownership namespaces),
  `repair-operator` (reconciles the Spark Operator watch list and the
  controller `/tmp` size), and `reclaim-bucket`. Every mutating admin
  command acquires the cluster-wide `lakebench-cluster-lock` lease.
- **Spark Operator controller `/tmp` sizing.** spark-submit runs in the
  operator controller and resolves `spark.jars.packages` into `/tmp/.ivy2`;
  the chart's 1Gi `/tmp` emptyDir is smaller than one Spark line's jars, so
  the kubelet evicted the controller repeatedly under load and every
  tenant's submissions failed and retried. `admin install-spark-operator`
  and `admin repair-operator` set the controller `/tmp` to 8Gi
  (`--controller-tmp-size`, floor 4Gi) under the cluster lease and verify
  it; `--dry-run` shows the plan. `admin doctor` and `admin status` report
  the size and current storage evictions. Installing over an existing
  release keeps the tenants' watch lists and the installed chart version.
- **Shared-cluster ownership discipline.** Every deployment carries an
  identity: a `lakebench.deployment/name` annotation on its namespace, a
  per-deploy nonce, and a matching `lakebench.deployment` tag on each of its
  S3 buckets where the store supports tagging. Destroy and clean verify
  identity before mutating; a foreign stamp is a hard refusal. Cluster-scoped
  resources (Stackable `SecretClass`) are named per deployment. See
  `docs/design/namespace-isolation.md`.
- **`--force-legacy` on `deploy`, `clean` and `destroy`;
  `--allow-unverified-cluster` on `destroy`.** Explicit escape hatches for
  pre-ownership deployments. `destroy` refuses annotation-less namespaces
  and untagged buckets by default; run `lakebench admin migrate-deployment
  <namespace>` first, or pass `--force-legacy` once you have confirmed the
  resource is yours. Foreign stamps are always refused.
- **`lakebench reproduce`.** Records a reproduction package from a run and
  verifies later runs against it with per-metric direction tables and
  tolerance bands. Exit codes 0/1/2 distinguish pass / performance drift /
  correctness drift. Packages carry the experiment identity and maintenance
  policy.
- **Performance regression gate.** `benchmarks/perf/` holds pinned configs
  (c360 batch s10, c360 continuous s10, AML batch s1) and
  `baselines.yaml`; `scripts/perf_gate.py` compares a run with its baseline
  and refuses runs that are not like-for-like (experiment identity,
  maintenance policy, stage timing basis, result fingerprints). The release
  gate gains a local `perf-baselines` check. The checked-in baselines are
  legacy and refuse until re-recorded.
- **AML workload (financial crime / transaction monitoring).** Rust
  generator schema `financial` with a monitored population of one reporting
  bank, minimal KYC and customer risk rating, planted typologies with a
  ground-truth manifest, and the sanctions/PEP screening track; detection
  rules scored for recall and precision against planted instances; a
  tracked fidelity gate (one feature definition in
  `spark/scripts/aml_features.py` feeding a pre-registered reference model,
  scored per customer and UTC month, with a leakage check); per-rule alert
  counts and errors in metrics.json (`alerts_by_rule`, `rule_errors`); and
  `lakebench financial replay` / `reproduce`. See `docs/aml-scoring.md`.
- **TM operations layer for AML.** After detection, `tm_operations.py`
  writes `tm_reconciliation`, `scenario_coverage`, `alert_dispositions` and
  `cases` to gold, with dispositions simulated from the datagen ground truth
  at a configured analyst and investigator accuracy (truth is rule-aware:
  a sanctions or PEP hit is true only for W5/W6 alerts). A violated workflow
  invariant fails the run; a layer that could not run is reported as
  `not_run`. Configured under `workload.tm_operations`.
- **AML continuous: time to detect.** `time_to_detect_seconds` (median),
  `_p95_seconds`, `_max_seconds`, `_alerts`, `_late_alerts` and
  `_unmeasured_cycles` on AML continuous scorecards, from the newest bronze
  ingest of an alert's transactions to the end of the detection pass that
  first raised it. `intake_limit` and `bronze_busy_fraction` say whether
  bronze's own processing bounded intake.
- **DuckDB runs all AML analytical and investigator queries.**
- **Per-file coverage floors** on the scoring, metrics and detection code
  (`scripts/check_coverage.py`), run in CI, plus a gitleaks tree scan.
- **Root `--version` / `-V` flag.**
- **`--regenerate` on `run --generate` and `generate` (A4).** A non-empty
  bronze prefix is now refused by the CLI unless `--regenerate` is passed
  (exit 2 with the prefix, object count and size named); with the flag, the
  whole bronze bucket is emptied via `S3Client.empty_bucket()` (which also
  aborts dangling multipart uploads on FlashBlade) before datagen submits.
  Before, `lakebench generate` and `run --generate` deployed datagen
  straight onto whatever was in bronze and the deployer's clear
  covered only buckets this deployment recorded creating.

### Changed
- Datagen image `lb-datagen:1.6.0`: AML datagen pod memory at scale
  100 falls from 18.18 GiB to 5.70 GiB (mimalloc allocator, typology rows kept
  only for each pod's own files, world columns recomputed on demand). Output is
  byte-identical to the v1.6 AML generator freeze (seed-43 byte-compare on the
  pushed image; pinned in `datagen_rs/tests/cycles.rs`). The autosizer memory
  model is re-fit to cluster measurements and a datagen pod never requests more
  than 16Gi; AML above scale 100 runs at least 8 datagen pods.
- **Datagen: the Python image is retired; the Rust image serves both
  schemas** (`datagen_rs/`, `--schema customer360` and `--schema
  financial`). Per-pod throughput on customer360 measured at 590 MB/s
  (snappy, 8 CPU / 8 Gi, n=1), against 6-8 MB/s for the Python path. The
  default codec is `snappy` (was `zstd1`); override with
  `DG_COMPRESSION=zstd`, `lz4` or `none`. An unset `datagen.cpu` is 8 in
  both modes, and an unset `datagen.memory` is derived from a measured
  peak-RSS model for the schema, scale, thread count and file size, with a
  4 Gi floor (continuous pods were fixed at 24 Gi).
- **Higher cluster minimum for AML continuous.** Under `schema: financial`
  bronze-ingest runs 5 executors x 4 cores, silver-stream 10 x 4 and
  gold-refresh 12 x 4, sized from scale-10 runs where the smaller streams
  fell behind. The full request is 118 cores / 980 GB / 2,300 Gi scratch at
  scale 1-10 and 222 cores / 1,948 GB / 4,660 Gi at scale 100. In continuous
  mode a smaller cluster runs degraded with a WARNING naming the capped
  stages when the capped request fits (AML scale 1-10: 57 cores), and fails
  preflight only when it does not. Stream stages report the executor
  count actually granted, so core-hours are right on capped clusters.
- **AML bronze-verify sizing.** Under `schema: financial`
  bronze-verify gets 500Gi scratch per executor, `executors_per_100_scale: 8`
  and `max_executors: 28`; the capacity preflight uses the AML profile.
- **Continuous runs held to the trickle are not saturation.** A run
  whose bronze ran a micro-batch on at least 90% of the window's triggers,
  inside each trigger, with silver keeping up, reports
  `intake_limit: trickle_rate`, `pipeline_saturated: false` and
  `corpus_drain_seconds` (the window that would drain the corpus at the rate
  held). Stream stages carry `batch_span_seconds`.
- **`ScratchStorageConfig.create_storage_class` removed.** The StorageClass
  is shared infrastructure; `deploy` verifies it exists and points at
  `lakebench admin install-scratch-storage-class`. YAML that still carries
  the key loads with a warning.
- **Spark Operator watch-list mutation is lease-gated in strict mode.** If
  removing the namespace from the watch list fails, destroy raises
  `WatchListMutationError` and does not delete the namespace (a deleted
  watched namespace crash-loops the operator for every tenant); run
  `lakebench admin repair-operator`.
- **`LB_FINANCIAL_BRONZE_PREFIX` is the datagen root on every AML reader
.** `bronze_ingest_financial` read it as the inner path; a caller
  that set `bronze/pacs008/` there now reads zero rows. Set it to the
  datagen root, or set `LB_FINANCIAL_PACS_PATH`.
- **DuckDB probes** get realistic timeouts (startup 15 s, readiness and
  liveness 10 s); the 1 s default restarted the container at random.
- **SparkApplication status reads** time out after 30 s and retry on
  timeouts, resets, 429 and 5xx; the monitor logs slow reads and stalls.
- Dependency floors for click and jinja2 raised past known CVEs.

### Removed
- `platform.storage.scratch.create_storage_class` (see Changed).
- Checkpoint resume for data generation. The Rust generator does not
  implement it. The `--resume` CLI flag and the `workload.datagen.checkpoint.*`
  config block are removed. Old configs that carry `datagen.checkpoint:`
  load with a `DeprecationWarning` and the block is dropped from the
  loaded config. An interrupted `lakebench generate` re-runs from the
  start and needs `--regenerate` to empty the partial bronze data first.
- `workload.datagen.uploaders`. Never forwarded to the Rust generator;
  uploader concurrency is fixed inside the S3 sink. Old configs that
  carry the field load with a `DeprecationWarning` and the field is
  dropped from the loaded config.
- The Python datagen (`datagen/`) and its image.
- Packaged `sanctions_list.json` and `pep_list.json` (replaced by the
  per-corpus watchlist).

### Fixed
- **Datagen timeout on `run --generate` no longer prints "Datagen
  completed" (A4).** The wait loop's timeout fell through to a success line
  even when the datagen Job was still running; the follow-up pipeline
  stages then built on a partial bronze (invariant 3). The run now exits
  with a distinct code (`4`), stops the datagen Job so it stops writing,
  and deletes any leftover streaming SparkApplication
  (`bronze-ingest`, `silver-stream`, `gold-refresh`) that was consuming
  the trickle so the timed-out generate does not leave orphan compute
  behind.
- **Spark Thrift `s3://` table locations** map to S3A, so Polaris orphan
  removal can open them, and the Thrift server sets `fs.s3a.endpoint.region`
  like the Spark jobs. A failed orphan removal is no longer stamped
  as maintenance that ran.
- **`validate` failed on every example config before deploy** ("Spark
  Operator does not watch namespace"); the check is advisory before the
  first deploy.
- **The continuous monitoring loop busy-spun** (0 s sleeps) when an
  in-stream benchmark round could not fit the time left.
- **The destroy table step says what it did** when no engine can drop
  tables, and a re-run after the buckets were emptied no longer fails on a
  Delta table whose directory is gone.
- **The continuous reset** deletes the owned Delta bronze path when the table
  is not in the catalog, so an interrupted reset no longer wedges the
  deployment.
- **`list_runs` ordered runs by timestamp text**, so with mixed local and UTC
  timestamps a UTC+X host picked an older run as the latest; it now orders
  by instant.
- **Trino coordinator OOM-killed under load.** `-Xmx` equalled the
  container memory limit, so heap plus native memory exceeded the cgroup
  limit (exit 137). The coordinator and worker heaps are now 80% of the pod
  memory limit; pod limits are unchanged.
- **hive-delta-spark-thrift failed 5 of 8 c360 queries.** The
  delta-spark `ClassCastException` on MIN/MAX of the date column (Q2 and Q6) is worked around with
  `spark.databricks.delta.optimizeMetadataQuery.enabled=false` for Delta +
  Hive, in the Thrift server and the Spark jobs; Q2 on Delta + Thrift is no
  longer tolerated as a known failure. The Thrift pod limit is now heap +
  max(10% of heap, 1 GiB). Delta + Hive + Thrift defaults to 8 cores / 16g
  heap when unset (fitted down on small nodes); Iceberg keeps 2 / 4g.
- **W3/W17 path search failed at AML scale 10** after executor
  loss (`CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND`). Path levels are written as
  Parquet under the gold bucket instead of local checkpoints, the step
  frames persist to disk only, and the join is partitioned by edge count.
  Alerts are identical to the previous code on the test graphs.
- **continuous freshness grew with wall clock** once a finite
  corpus had drained. The trailing idle gold cycles of a drained run are
  left out of freshness; any other idle cycle still counts as a stall.
- **silver overestimated c360 customers** (14.7M at scale 10 where
  there are 1M). It now uses a Chao1 estimate over the sample.
- **maintenance value reported when compaction changed nothing.**
  It is reported only when compaction changed the file count, and no
  compaction ratio is recorded when the post file count is unknown.
- **destroy kept the namespace** when FlashBlade listed a finished
  multipart upload and the abort raised `NoSuchUpload`; that is now
  treated as done.
- **c360 continuous writers are exactly-once** across a driver restart,
  bronze-verify fails on columns silver or gold compute on, and Q6 recency
  uses the data clock.
- **AML analytical query QpH was unstable** (an iteration read 0.0
  when Spark Thrift ran out of memory). The AML Spark Thrift memory target is
  24g, and the benchmark `query_timeout` for `schema: financial` is 900 s
  both before and after compaction (300 s before it for other schemas). On clusters with under 36 GiB
  allocatable the target is `max(4, min(20, allocatable - 8))g`.
- **AML bronze-verify ran out of scratch disk at scale 5 and above**
  on its CTAS fallback. See the AML bronze-verify sizing under Changed.
- **batch AML never ran the detection rules** and wrote an empty
  `gold.alerts`. gold-finalize now runs every scheduled rule after the
  baseline dashboards, cheapest first, with per-rule error isolation and a
  delete-then-insert per rule so a re-run gives reproducible counts. The
  per-job timeout gains 900 s under `schema: financial`.
- **Spark Thrift's 4g default ran out of memory on every AML query
  at scale 1.** The autosizer raises it for `schema: financial` when the
  field is at its default, capped to what the largest node can hold. An
  already-deployed Thrift pod needs destroy and deploy to pick it up.
- **W1 connected components failed with an ambiguous-column error**
  on the second label-propagation iteration. The evidence now carries
  `converged=true|false`.
- **W7 crashed in the driver on every run** (no `silver.entities`
  passed, and the AML reference data could not be found inside the Spark
  image). The reference data ships in the scripts ConfigMap, W7 loads
  `silver.entities` itself, and its country choice per entity is
  deterministic.
- **the AML pipeline could not read datagen_rs output.** The bronze
  readers read a flat `pacs008/` layout; datagen_rs writes
  `bronze/pacs008/`, `bronze/party.parquet`, `bronze/account.parquet` and
  `manifest/manifest.parquet`. Both readers now derive the pacs.008 path
  from the datagen root (`LB_FINANCIAL_PACS_PATH` overrides it), and
  bronze-verify registers `bronze.manifest`, which the ad-hoc AML scoring
  query templates read (no QpH query reads bronze).
- **FlashBlade does not implement bucket tagging.** Deploy and
  destroy fall back to name ownership with longest-prefix-wins against the
  other lakebench deployments on the cluster, refuse when those cannot be
  listed, and now also require the namespace's bucket record (see Behaviour
  changes). `lakebench config storage` reports bucket tagging as an advisory
  check.


For releases before 1.6.0, see [CHANGELOG-archive.md](CHANGELOG-archive.md).
