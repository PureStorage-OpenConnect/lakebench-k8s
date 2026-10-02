# CLI Reference

Lakebench provides a single `lakebench` command with subcommands for every
stage of the deployment and benchmarking lifecycle. The CLI is built with
Typer and Rich.

```
lakebench [COMMAND] [OPTIONS] [CONFIG_FILE]
```

Most commands accept an optional config file argument. If omitted, the CLI
looks for `./lakebench.yaml` in the current directory.

Most commands also take the config as `--file` / `-f` (on `destroy`,
`clean` and `logs` only the long form `--file`; see [clean](#clean)).

Several commands prompt for confirmation before running. Use `--yes` / `-y`
to skip the prompt; `destroy` and `clean` also accept `--force` as the same
flag.

Exit codes are listed in [Exit Codes](exit-codes.md).

## Commands

### init

Write a starter configuration file.

```
lakebench init [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--output` | `-o` | `lakebench.yaml` | Output file path |
| `--name` | `-n` | `lb-<user>-<4 hex>` | Deployment name; the default is new on every `init` |
| `--scale` | `-s` | `1` (`0.1` with `--local`) | Scale factor (1 = ~10 GB, 100 = ~1 TB) |
| `--endpoint` | | `""` | S3 endpoint URL |
| `--credentials-env` | | `LAKEBENCH_S3` | Prefix of the two credential variables: the file references `${PREFIX_ACCESS_KEY}` and `${PREFIX_SECRET_KEY}` |
| `--namespace` | | `""` | Kubernetes namespace (default: the name) |
| `--recipe` | `-r` | `polaris-iceberg-spark-trino` | Architecture recipe (`lakebench config recipes`) |
| `--workload` | `-w` | `customer360` | Workload schema: `customer360` or `financial` |
| `--overwrite` | | `false` | Overwrite an existing file; keeps its name, and refuses (exit 3) a change of namespace, buckets, endpoint or recipe under that name |
| `--force` | | `false` | Old spelling of `--overwrite` (`-f` is deprecated here) |
| `--local` | | `false` | Generate a config for local mode (podman/docker, no Kubernetes) |

The file has 12 lines of settings: the name, the recipe once, the workload
and scale, the endpoint and the two S3 credentials as `${VAR}` references.
The components the recipe sets are written as a commented block; written
out, they must agree with the recipe or the config is refused at load. No
plaintext secret is written, and no Polaris client secret: deploy creates
one for a new Polaris. `init` prints what it chose on stderr, with or
without a terminal, and refuses (exit 2, nothing written) a combination
that would not load, such as `--workload financial` with a Delta recipe.

The wizard is removed. `--interactive`, `-i` and `--advanced` print one line
saying so and write the default config. `--access-key` and `--secret-key`
are refused with exit 2; export the variables instead.

```bash
# Default: Polaris, customer360, scale 1
lakebench init
export LAKEBENCH_S3_ACCESS_KEY=... LAKEBENCH_S3_SECRET_KEY=...

# Another recipe and endpoint, credentials from MY_LAB_ACCESS_KEY / MY_LAB_SECRET_KEY
lakebench init -r hive-iceberg-spark-trino --endpoint http://my-s3:80 --credentials-env MY_LAB
```

### compare

Compare two sides of stored run records. `compare` is read-only: it reads
`metrics.json` files and series manifests, and deploys, runs, generates and
destroys nothing.

```
lakebench compare SIDE_A SIDE_B [--runs-dir DIR]... [--format table|json|csv] [-o PATH]
```

A side is one or more comma-separated refs. Each ref is tried in this order:

1. `series:<id>`: the members of a `run --repeat` series, read from its
   manifest `<output dir>/series/<id>.json`.
2. A run id (`20260929-212900-5105a0`, with or without `run-`), looked up in
   every `--runs-dir` (default `lakebench-output/runs`). Two directories
   holding different files for one run id are refused.
3. A run directory holding `metrics.json`, or a path to a `metrics.json`.
4. A config (`.yaml`/`.yml`): the latest run record of its deployment name,
   by `start_time`. When that record belongs to a series, every member the
   series manifest names is taken; a series whose manifest is missing is
   refused (pass the run ids instead). A record in the runs directories
   that cannot be read is skipped with a warning naming it.

Side A is the baseline and B the candidate; deltas are B relative to A.

| Flag | Short | Default | Description |
|---|---|---|---|
| `--runs-dir` | | `lakebench-output/runs` | Directory of `run-<id>/` records; repeat it to search several. Series manifests are read from each directory's sibling `series/` |
| `--format` | | `table` | Output format: table, json, csv |
| `--output` | `-o` | (none) | Write the comparison to this file (JSON, or CSV with `--format csv`). Nothing is written without it. A path that is one of the inputs, is named `metrics.json`, or lies inside a runs or series directory is refused |

```bash
lakebench compare 20260929-212900-5105a0 20260929-214442-825153
lakebench compare a.yaml b.yaml --format json -o comparison.json
lakebench compare series:s-20261101-120000-a1b2c3 b.yaml
lakebench compare r1,r2,r3 r4,r5,r6 --runs-dir lakebench-output/runs
```

The flags of the earlier `compare`, which ran both configs (`--keep`,
`--scale`, `--skip-benchmark`, `--timeout`, `--local`, `--generate`,
`--yes`), are refused with exit 2 and the replacement: run each side first
with `lakebench run`, then compare.

**Resolution.** Before anything else `compare` prints, on stderr, how each
side resolved: its refs, deployment, series and every member with its
verdict, for example `A: a.yaml -> deployment lb-a1, series s-..., 3 runs:
r1 passed, r2 passed, r3 FAILED: ... (excluded)`. A member whose verdict did
not pass is excluded and listed; n counts the passed members. A record
written before the experiment block is not excluded: the pair is refused
on it. Two sides that resolve to the same runs, or share a run, are
refused, as are two configs with one deployment name and different
contents ("a name is one deployment").

**Verdicts.** The pair is decided by the comparability ladder over the
passed members, and the exit code is the verdict:

| Verdict | Meaning | Exit |
|---|---|---|
| LIKE-FOR-LIKE | Same experiment, equal results, same execution conditions. The attribution says what differs: the architecture, the system, or nothing (a repeat) | 0 |
| NOT COMPARABLE | Different experiments (workload, version, mode, corpus, seed, scale, generator), different results, a run that did not pass, a record without the experiment block, or a side whose runs are not one experiment | 10 |
| NOT ESTABLISHED | Nothing contradicts the pair, but a side has no checked results (no benchmark, a recipe without a query engine, a continuous run without an end-of-run result check) | 11 |
| NOT LIKE-FOR-LIKE | Comparable, but an execution condition differs: effective maintenance or its compaction operation, maintenance settings, benchmark iterations or mode, in-stream rounds, the Lakebench limits that bound; or the only architecture difference is the dependency set | 12 |
| CONFOUNDED | Comparable, but the architecture and the system both differ, so no difference can be put down to either | 13 |

Usage errors (a ref that resolves to nothing, the same runs on both sides,
an unreadable record or manifest, a removed flag, an unsupported format)
exit 2.

**The missing condition.** Every verdict comes with the one condition the
pair lacks, for the first ladder step that failed, and, where one exists,
the command that supplies it, for example "corpus scale differs (1 vs 10).
Missing: the same corpus. Set `architecture.workload.datagen.scale: 1` in
b.yaml, then `lakebench run b.yaml --generate --regenerate`" (a continuous
side regenerates with `lakebench run <config> --continuous`). Some
conditions have no command: two table formats that run different
maintenance operations, compaction by different engines, a Lakebench
bound, a differing round count. Commands name the side's config from its
record (`provenance.config_path`), or "the config of deployment <name>"
for a record that does not carry it. A continuous side cannot be repeated
with `--repeat`, so its hint says to run it again with `--continuous` and
pass the run ids. Two continuous runs whose in-stream rounds differ are not
one experiment when they are on one side: rounds are an outcome of speed,
so compare single runs.

**Metrics.** Each score is shown with the median, range and n of each side
and the delta of the medians. No winner is named and no colour marks a
better side: the winner rule is not in this release. Each row's
`assessment` says what may be read from it:

| Assessment | When |
|---|---|
| `withheld` | The pair is NOT COMPARABLE or NOT ESTABLISHED; the delta is not computed |
| `not_directional` | The score has no better side (correctness, guard and diagnostic scores, scores that follow the config, a score the registry does not know, or a mode-dependent score on a record without a mode) |
| `confounded` | The pair is confounded |
| `not_assessed` | A median over in-stream rounds (`composite_qph`, `in_stream_composite_qph`, `qph_degradation_pct`) on a side whose rounds ran different query sets or all missed the same query, on any pair that is not withheld (the hint names the side); every other directional row: on a NOT LIKE-FOR-LIKE pair because the pair is not like-for-like, otherwise because the winner rule is not in this release |
| `capped` | On a like-for-like pair, a Lakebench limit bound the row on a passed member of either side (a bound kind the row depends on, or the trickle of a continuous run): the figure measures that limit, not the system |

Whatever its assessment, a row that a Lakebench limit bound on either side
lists the limit in `capped_by` (`bound_by` in the CSV), and the table says
BOUNDED BY it. Directions come from the metric registry
(`metrics/metric_registry.py`).

**Output.** `--format json` (and `-o`) writes the `cmp2` document:
`verdict`, `exit_code`, `step`, `attribution`, `missing` (`condition`,
`command`, `hint`), `cause`, `reasons`, `notes`, `sides` (per side: `refs`,
`deployment`, `series`, `members` with `run_id`, `verdict`, `digest`,
`excluded` and `reason`, `n_attempted`, `n_passed`, the first passed
member's `experiment` block, `support`, `bound`), `groups` (the differing
keys per identity group), `warnings` and `metrics` (per row: `metric`,
`unit`, `direction`, `a` and `b` with `median`, `min`, `max`, `values`, `n`,
`delta_pct`, `assessment`, `winner` (always null), `missing`, `hint`,
`capped_by`, and `rounds`, which says why a round median is not assessed
for its rounds, else null). `--format csv` writes the header fields as `# key: value`
lines, then one row per metric with `metric`, the medians and ranges,
`delta_pct`, `verdict`, `attribution`, `n_a`, `n_b`, `assessment`,
`bound_by` and `rounds`; the table prints the `rounds` reason after the
assessment. A protected or spent AML seed is never printed; it reads
`<protected seed>`.

### config

Configuration management subcommands.

```
lakebench config show CONFIG_FILE       # show resolved config with source annotations
lakebench config validate CONFIG_FILE   # validate config + test connectivity
lakebench config storage CONFIG_FILE    # check the S3 backend supports what lakebench needs
lakebench config recommend CONFIG_FILE  # largest scale the cluster holds, sized from this config
lakebench config recipes [NAME]         # list recipes, what each one trades off, and support states
```

`config validate` and `validate` load the config as `deploy` does, so they
fail on a config with no `name:` or with a removed key.

`config show` prints the config's support state and its peak requested
resources. `config recipes` lists, for every recipe, its support state per
workload and mode (Customer 360 and AML, batch and continuous: supported,
unverified or unsupported) and whether it runs in local mode; the detail
view (`config recipes NAME`) gives the basis for each state. See
[Compatibility Matrix](compatibility-matrix.md#support-states).

`config validate` and `config recipes` both take `--local`: for `validate`,
it validates for local (podman/docker) mode instead of Kubernetes; for
`recipes`, it filters the list down to recipes that can run in local mode.

#### config storage

Runs graded conformance checks against the configured S3 endpoint and reports
what the backend does. Diagnostic only: it never gates `deploy` or `run`, so a
store lakebench has not seen before is checked rather than refused.

```
lakebench config storage [CONFIG_FILE] [OPTIONS]
```

| Option | Default | Description |
|---|---|---|
| `--full` / `--no-full` | `--full` | Create a temporary bucket for write and multipart checks. Use `--no-full` when the account cannot create buckets; write checks are then reported as skipped, not failed. |

Exit code 0 means no required check failed. Exit code 1 means a required check
failed; 2 means the config could not be loaded or names no S3 endpoint.

Checks are graded. **Required** failures (connectivity, bucket enumeration,
object operations, multipart abort) mean lakebench cannot run against the store.
**Advisory** results (sigv4 region strictness) are recorded because they change
how lakebench configures Spark, not because they are defects.

See [Storage Backends](storage-backends.md) for validated backends, what each
check covers, and what it deliberately does not.

### validate

Equivalent to `lakebench config validate`. Validate configuration and test connectivity to S3 and Kubernetes.

```
lakebench validate [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--verbose` | `-v` | `false` | Show detailed validation output |

Checks performed: YAML syntax, required fields, S3 endpoint reachability,
S3 credential validity, Kubernetes context accessibility, namespace status,
platform security (SCC on OpenShift), storage classes, Spark Operator status,
and the configured scale. Executor sizing is not graded: it is the job
profiles'. Before the first deploy, a
namespace the Spark Operator does not watch yet is reported as advisory:
`deploy` adds it to the watch list under the cluster lock.

### deploy

Deploy lakehouse infrastructure to Kubernetes.

```
lakebench deploy [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--dry-run` | | `false` | Show what would be deployed without making changes |
| `--yes` | `-y` | `false` | Skip confirmation prompt |
| `--timeout` | `-t` | `3600` | Global deployment timeout in seconds (`0` = no timeout); bounds the waits inside every step, see [deployment](deployment.md) |
| `--local` | | `false` | Deploy locally with podman/docker instead of Kubernetes |
| `--workdir` | | `~/.lakebench/local/<name>` | Host directory for local mode state (only used with `--local`) |
| `--force-legacy` | | `false` | Claim ownership without tag proof: a pre-1.5 annotation-less namespace or untagged bucket, or a bucket on a backend without tagging that does not match the deployment-name prefix. Use only when you have confirmed the resources are yours |

Before it creates anything, deploy runs `run`'s cluster capacity check
(read-only, without datagen: deploy does not generate) and refuses with exit
4 when the cluster's allocatable capacity or its largest node cannot hold
the config's pipeline and always-on pods; a cluster it cannot reach does not
block it. `--dry-run` prints the result without refusing.

Deploys components in order: namespace, secrets, S3 buckets, scratch
StorageClass check (it must already exist; `lakebench admin
install-scratch-storage-class` installs it), PostgreSQL, catalog (Hive or
Polaris), Spark RBAC, Unity Catalog (only when `catalog.type` is `unity`; no
recipe uses it), Spark Operator check and watch-list entry for the
namespace (under the cluster lock), the dependency server (`lb-deps`:
resolves the jars and wheels onto its own PVC and serves them in the
namespace), then the query engine (Trino, Spark Thrift or DuckDB), and
optionally the shared observability stack. The Spark
Operator step always runs: it checks the operator is ready and adds the
namespace to its watch list. It never installs the shared operator (a cluster
admin runs `lakebench admin install-spark-operator` once), and a config with
`operator.install: true` is refused.

With `--local`, lakebench runs the same pipeline against podman/docker
containers on the local machine instead of a Kubernetes cluster, useful for
testing a recipe without a cluster available.

### generate

Generate synthetic data to the bronze S3 bucket.

```
lakebench generate [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--timeout` | `-t` | `0` | Timeout in seconds when waiting; `0` computes it from scale, parallelism and a conservative per-pod throughput |
| `--yes` | `-y` | `false` | Skip confirmation prompt |
| `--regenerate` | | `false` | Clear the datagen prefix (not the whole bucket) before generating, when this deployment owns the bronze bucket (its stamp, or a bucket it created). Without this flag, a non-empty bronze prefix is refused (exit 3) so existing datagen output is never overwritten silently. Refused (exit 3) on a bucket this deployment cannot prove it owns (`lakebench admin reclaim-bucket` claims one). |
| `--allow-stale-bronze` | | `false` | Generate over objects already in the datagen prefix of a bronze bucket this deployment cannot prove it owns. Rows may be over-counted; `run` records it in `metrics.json` (`datagen.stale_bronze`) and the report shows "bronze held N objects before generate". |

Runs parallel Kubernetes Jobs to produce Parquet files. At scale 100 this
generates approximately 1 TB of data. Use `--timeout` for large scales that
may take hours. Without `--yes`, the command prompts for confirmation before
submitting jobs. The Rust generator has no checkpoint-resume; an interrupted
run is re-run from the start.

**Exit codes**: `0` on success, `1` on generic failure and when datagen
exceeds its wait budget (`--timeout`), `3` (refused) when the bronze datagen
prefix is non-empty and neither `--regenerate` (on a bucket this deployment
owns) nor `--allow-stale-bronze` (on one it does not) applies, or
`--regenerate` was passed for a bucket it does not own or with an empty
datagen prefix, and `4` when bronze or its ownership cannot be checked;
when datagen exceeds its wait budget the datagen Job and any leftover
streaming SparkApplication consuming the trickle are stopped before exit.
Only the initial-pass datagen is guarded by this exit code; per-cycle
datagen inside a multi-cycle run reports its own timeout independently.

### run

Execute the data pipeline (batch or continuous).

```
lakebench run [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--stage` | `-s` | all | Run a specific stage only (`bronze-verify`, `silver-build`, `gold-finalize`) |
| `--timeout` | `-t` | auto | Timeout per job in seconds. When omitted: `max(3600, scale * 120)`; the AML workload adds 900 s and never goes below its bronze-verify budget |
| `--skip-benchmark` | | `false` | Skip the query benchmark after pipeline |
| `--skip-preflight` | | `false` | Skip prerequisite checks and infrastructure validation, the cluster capacity check included |
| `--skip-deploy` | | `false` | Skip the deploy and the infrastructure readiness check (namespace and components); the read-only prerequisite checks, cluster capacity included, still run and fail the run with exit 4 |
| `--skip-generate` | | `false` | Skip datagen (refused with `--generate`) |
| `--regenerate` | | `false` | With `--generate`: clear the datagen prefix before generating, when this deployment owns the bronze bucket. Without this flag, a non-empty bronze prefix is refused (exit 3) so existing datagen output is never overwritten silently. Never clears a bucket this deployment does not own. A multi-cycle run clears an owned prefix before cycle 0 without it. Refused without `--generate` or `--generate-only`, and in a local or continuous run. |
| `--allow-stale-bronze` | | `false` | With `--generate` (or a multi-cycle run): generate over objects already in the datagen prefix of a bronze bucket this deployment did not create. Rows may be over-counted; `metrics.json` records it (`datagen.stale_bronze`). |
| `--skip-maintenance` | | `false` | Skip pre-benchmark maintenance (compaction, snapshot expiry) |
| `--force-rebuild` | | `false` | Silver batch only: opt in to a full rebuild that drops an existing populated silver table. Atomically bumps the deployment's silver rebuild epoch so downstream Delta idempotency keys move to a new namespace. On Delta the silver table's own log has the last word: the rebuild writes under an epoch above every one the table has used, even if the counter reads lower |
| `--force-reset` | | `false` | Continuous c360 only: allow the run to drop existing bronze_raw, silver and gold tables, stream checkpoints and raw data. Without it a continuous run over existing state refuses and lists what it would delete. Raw data alone from `lakebench generate` on a deployment with no tables or checkpoints is not refused: continuous runs generate their own data, so a separate `generate` before `run --continuous` is not needed |
| `--deploy-only` | | `false` | Deploy infrastructure and exit |
| `--generate-only` | | `false` | Deploy + generate data and exit |
| `--continuous` | | `false` | Run the continuous pipeline instead of batch. `--sustained` is a deprecated hidden alias. |
| `--duration` | | config value | Continuous run duration in seconds |
| `--generate` | | `false` | Run datagen before pipeline (batch mode only) |
| `--yes` | `-y` | `false` | Skip confirmation prompts |
| `--local` | | `false` | Run locally with podman/docker instead of Kubernetes |
| `--workdir` | | `~/.lakebench/local/<name>` | Host directory for local mode state (only used with `--local`) |
| `--repeat` | | off | Run the batch pipeline N times (1 to 20) as one series over one corpus; see below |

**Repeating a run.** `run --repeat N` runs the batch pipeline N times as one
series. Repetition 1 runs as the other options ask and may generate;
repetitions 2 to N never generate and rebuild silver and gold from the same
bronze (as `--force-rebuild`). The config is loaded once, so an edit during
the series changes nothing that runs. The datagen prefix in the bronze
bucket is listed before repetition 1 (when it does not generate), after it,
and before every later repetition, and each record's own pre-save listing
must match repetition 1's: a difference, even a same-size rewrite of one
object, stops the series with exit 3 and the changed repetition is not
counted. A later repetition inherits repetition 1's corpus identity only
under that check (`experiment.corpus.inherited_from`). A repetition that
fails its verdict does not stop the series; Ctrl-C does (exit 130). The
series stops after repetition 1 when its bronze-verify or silver-build did
not pass, or when its datagen Job is still running or cannot be read. Each
record carries `series {id, index, size}`, and
`lakebench-output/series/<id>.json` lists the repetitions, which passed,
and the corpus; only repetitions that passed and share the corpus count.
Exit: 0 when every repetition passed, 1 when any did not, 3 when bronze
changed, 130 on an interrupt.

**Refused arguments.** `run` checks every option before it makes any
cluster call, and exits 2 (usage) naming the first refused one:

- `--stage` that is not `bronze-verify`, `silver-build` or `gold-finalize`;
- `--stage` with a continuous run (flag or config);
- `--deploy-only` with `--generate-only`;
- `--deploy-only` with `--stage`, `--generate` or `--skip-generate`;
- `--skip-deploy` with `--deploy-only` or `--generate-only`;
- `--generate-only` with `--skip-generate`;
- `--local` with `--deploy-only`, `--generate-only`, `--force-rebuild` or `--skip-maintenance`;
- `--regenerate` without `--generate` or `--generate-only`;
- `--regenerate` with `--local`, or with a continuous run other than `--generate-only`;
- `--skip-generate` with `--generate`;
- `--force-reset` on a batch run;
- `--force-rebuild` on a continuous run;
- `--duration` on a batch run;
- `--duration` below 60;
- `--timeout` below 1;
- `--repeat` below 1 or above 20;
- `--repeat` with a continuous run;
- `--repeat` with `cycles` above 1;
- `--repeat` with `--stage`, `--local`, `--deploy-only` or `--generate-only`.

`--local` with a continuous run, and any workload, recipe and mode `run`
does not support, are refused just after these, also before any cluster
call. Benchmark settings `run` does not honour are refused when the config
loads.

The run command executes 7 phases:

1. **Prerequisites** -- check kubectl, helm, K8s cluster, S3, Spark Operator
2. **Infrastructure** -- verify deployed components are ready
3. **Generate** -- optional datagen (with `--generate`)
4. **Pipeline** -- bronze-verify, silver-build, gold-finalize
5. **Maintenance** -- pre-benchmark compaction + snapshot expiry (measures cost)
6. **Benchmark** -- query benchmark with pre/post compaction QpH comparison
7. **Results** -- scorecard with maintenance value metrics

In continuous mode (`--continuous`), `run` launches the three stream jobs
(bronze-ingest, silver-stream, gold-refresh) together, runs in-stream
benchmark rounds and maintenance during the measurement window, gates on
continuous output inside the window, then lets the corpus settle and
fingerprints the query set over the settled tables. See
[Running Pipelines](running-pipelines.md#continuous-mode). The run reads
its namespace every 30 s during the window and the settle wait, before and
after each benchmark round, before each maintenance and compaction round,
and before it stops its streams. When the namespace is gone (deleted, being
deleted, or deleted and deployed again), or three reads in a row fail, the
run stops at that read, exits 1 and saves its record with `abort_reason`
(the reason and the run second). A maintenance or compaction statement in
flight finishes first. The run does not try to stop streams that went with
the namespace, or that belong to a deployment that replaced it.

**Interrupting a run.** Ctrl-C (SIGINT) or SIGTERM stops `run` and exits
130. The run first deletes the SparkApplications and the datagen Job it
created and has not seen finish: a stage, or a datagen Job, that completed
is kept, so its logs stay readable. Each delete carries the uid of the
object this run created, so an object of the same name that another
invocation created since is left alone. The cleanup takes at most about
60 s. The record is then saved: its verdict is INTERRUPTED (FAILED when
something had already failed before the interrupt, never PASSED), and its
`interrupted` block names the signal, the stage, and the objects stopped,
left and skipped. After an interrupt the run does not measure bucket sizes
or read Prometheus; it still lists the datagen prefix once to record which
corpus it read (a corpus cut short is recorded as incomplete). A signal that arrives while the results are being
gathered at the end of a run does not stop the record being written; the
record is sealed the same way, at stage `results` (a run that had already
failed keeps its own exit code). One that arrives after the record is
saved changes nothing.

Press Ctrl-C a second time to cut the cleanup short: what it had not reached
is recorded as skipped (a quick double press can skip all of it). A third
press stops at once and may lose the record. For anything left or skipped
the run prints the `kubectl delete` that stops it. While the run holds the
cluster lease (the Spark Operator watch-list heal at the start), the
interrupt waits for that shared change to finish, as for any command (see
[Troubleshooting](troubleshooting.md)). SIGHUP (a closed terminal or a
dropped SSH session) is not handled and ends the run without a record or a
cleanup: run long jobs under `tmux` or `nohup`. Trino queries of an
interrupted benchmark or maintenance step are not cancelled. `report
--list` shows such a run as Interrupted; the HTML report shows it as
failed, with the interrupt as its reason.

A recipe without a query engine (`*-none`) skips the benchmark and exits 0
with no QpH. `run` refuses an unsupported workload x recipe x mode
combination before anything is deployed; `--local` refuses AML configs and
continuous mode.

### stop

Stop running continuous-mode jobs.

```
lakebench stop [CONFIG_FILE]
```

Deletes the continuous-mode SparkApplications (`bronze-ingest`, `silver-stream`,
`gold-refresh`) from the cluster. `--file` / `-f` points at the config the
same way it does on the other commands; `--name` names the deployment of a
nameless config, as for `destroy`.

### benchmark

Run the query engine benchmark independently.

```
lakebench benchmark [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--mode` | `-m` | `power` | Benchmark mode: `power`, `throughput`, or `composite` |
| `--streams` | `-s` | `4` | Concurrent query streams (throughput/composite modes) |
| `--cold` | | `false` | Flush Iceberg metadata cache before each query |
| `--iterations` | `-n` | config (`3`) | Timed runs per query, scored by the median. Overrides `architecture.benchmark.iterations` |
| `--class` | `-c` | all | Run only one query class: `scan`, `filter_prune`, `aggregation`, `analytics`, `operational`, and for AML also `investigator`. A name that matches no query runs nothing |

Executes the workload's query set (8 queries for Customer 360, 12 for AML)
against the silver and gold layers and reports Queries per Hour (QpH), the
median of `--iterations` timed samples per query. Each successful query is
then run once more, untimed, to record its result fingerprint. Power mode runs queries sequentially. Throughput mode runs
N concurrent streams. Composite mode runs both and reports the geometric mean.

### query

Execute SQL queries against the configured query engine.

```
lakebench query [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--sql` | `-q` | | SQL query string to execute |
| `--example` | `-e` | | Built-in query name (`count`, `revenue`, `channels`, `engagement`, `funnel`, `clv`) |
| `--sql-file` | | | Read SQL from file (use `-` for stdin) |
| `--interactive` | `-i` | `false` | Start interactive SQL shell (REPL) |
| `--format` | `-o` | `table` | Output format: `table`, `json`, `csv` |
| `--show-query` | | `false` | Print the SQL before executing |
| `--timeout` | `-t` | `120` | Query timeout in seconds |

Specify exactly one of `--sql`, `--example`, `--sql-file`, or `--interactive`.
(`--file` / `-f` is the config file, as on other commands.)

```bash
lakebench query --example count
lakebench query --sql "SELECT count(*) FROM lakehouse.gold.customer_executive_dashboard"
lakebench query --interactive
```

### status

Show deployment status of Lakebench components in the cluster.

```
lakebench status [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--namespace` | `-n` | from config | Kubernetes namespace to check |
| `--local` | | `false` | Show local mode status instead of Kubernetes |
| `--workdir` | | `~/.lakebench/local/<name>` | Host directory for local mode state (only used with `--local`) |
| `--name` | | | For a config with no name: the deployment name (several nameless configs in the directory, or a v1.6 directory). See [Deploy state and nameless teardown](configuration.md#deploy-state-and-nameless-teardown) |

Displays a table of the deployment's components (PostgreSQL, the configured
catalog and query engine) with their readiness and replica counts, and the
datagen job's progress while it runs. With only `--namespace` and no config,
it lists every component lakebench can deploy, plus the shared Prometheus and
Grafana.

### info

Deprecated and hidden; use `lakebench config show`, which carries the same
peak-request figure. `info` still works.

Show configuration summary with scale dimensions and peak requested resources.

```
lakebench info [CONFIG_FILE]
```

Displays deployment name, namespace, recipe, schema, scale factor, derived
dimensions (customers, rows, data size), per-job executor counts (auto vs
override), catalog type, table format, query engine, S3 endpoint, and bucket
names, and the minimum CPU / memory / scratch the config requests: the
Spark peak (`compute_peak_requirements()`) plus the query engine, catalog
and Postgres, with batch datagen at its default parallelism shown beside it
(`lakebench.config.sizing.plan_requirements`, the source `config show`,
`config recommend` and the `run` capacity preflight also use). With a
cluster it also prints the preflight's decision and any auto-sizing cuts.
No additional flags.

### recommend

Deprecated and hidden; `lakebench config recommend [CONFIG_FILE]` runs the same
logic on your config. It sizes the default recipe (`hive-iceberg-spark-trino`)
of the workload and mode with `lakebench.config.sizing`: `--scale` and the
reference table print the minimum without a cluster (datagen at its default
parallelism), and with a cluster each scale is decided by the same check the
`run` capacity preflight makes (batch: a `run --generate`). Continuous mode
prints two answers: a plain `run`, whose datagen Job is counted beside the
streams, and a corpus generated before the streams start (`generate`, then
`run --skip-generate` within an hour). A scale above the workload's largest
measured scale (300) is labelled unverified.

Show cluster sizing guidance for lakebench workloads.

```
lakebench recommend [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--cores` | `-c` | auto-detect | Total cluster CPU cores |
| `--memory` | `-m` | auto-detect | Total cluster memory in GB |
| `--scale` | `-s` | auto | Target scale factor to check requirements for |
| `--slow-datagen` | | `false` | Ignored: datagen pods that do not fit queue, so datagen never limits the scale. `--extended` / `-e` is a deprecated alias |
| `--mode` | | `batch` | Pipeline mode: `batch` or `continuous` |
| `--schema` | | `customer360` | Workload schema: `customer360` or `financial` |

Without arguments, auto-detects the connected cluster capacity and shows the
largest scale at which every scale up to it fits, bounded by the workload's
datagen ceiling (600 for Customer 360, 800 for AML). With `--cores` and
`--memory` the node sizes are unknown, so the largest-pod check is skipped.
Use `--scale` (1 or more) to see what one scale requests and what the figure
is built from.

```bash
lakebench recommend                        # auto-detect cluster
lakebench recommend --cores 64 --memory 256
lakebench recommend --scale 100            # requirements for ~1 TB
```

### clean

Delete data without destroying infrastructure.

```
lakebench clean TARGET [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--force` / `--yes` | `-y` | `false` | Skip confirmation prompt |
| `--metrics-dir` | `-m` | `./lakebench-output/runs` | Metrics directory (for `metrics` target) |
| `--force-legacy` | | `false` | Clean a bucket that has no lakebench ownership tag. Foreign-tagged buckets are always refused |
| `--allow-unverified-cluster` | | `false` | Proceed when the kubeconfig cannot prove which cluster it points at |
| `--file` | | | Config file (alternative to the positional argument; no short form) |

On a backend without bucket tagging, `clean` (like `destroy` and the
continuous reset) empties a bucket only when the namespace records creating
it or adopting it empty.

Before emptying a layer's bucket, `clean` unregisters that layer's tables
through the deployment's Trino or Spark Thrift pod (see
[Deployment](deployment.md)). A table it cannot unregister makes it exit 1,
and that bucket is left as it is unless the table's files are already gone.

`-f` is not accepted on `destroy` or `clean`: it exits 2 and names `--force` / `-y`,
because `-f` means `--file` everywhere else. `LAKEBENCH_LEGACY_SHORT_F=1` restores the old
meaning (force) with a warning for this release only.

Valid targets:

| Target | Action |
|---|---|
| `bronze` | Empty the bronze S3 bucket |
| `silver` | Empty the silver S3 bucket |
| `gold` | Empty the gold S3 bucket |
| `data` | Empty all three buckets |
| `metrics` | Delete local metrics/runs directory |
| `journal` | Delete all journal session files |

### destroy

Tear down the resources this deployment owns.

```
lakebench destroy [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--force` / `--yes` | `-y` | `false` | Skip confirmation prompt |
| `--local` | | `false` | Tear down the local stack instead of Kubernetes |
| `--workdir` | | `~/.lakebench/local/<name>` | Host directory for local mode state (only used with `--local`) |
| `--remove-data` | | `false` | Local mode only: also delete generated data and the Ivy cache |
| `--namespace-timeout` | | `600` | Seconds to wait for the namespace to finish terminating after the delete; `0` skips the wait, so destroy exits 6 unless the namespace is already gone |
| `--keep-buckets` | | `false` | Empty the S3 buckets but do not delete them |
| `--force-legacy` | | `false` | Proceed on a namespace or bucket with no lakebench ownership annotation or tag. Foreign-owned namespaces and buckets are refused regardless |
| `--allow-unverified-cluster` | | `false` | Bypass the API-server fingerprint match when it cannot be computed |
| `--file` | | | Config file (alternative to the positional argument; no short form) |
| `--name` | | | For a config with no name: the deployment name (several nameless configs in the directory, or a v1.6 directory). See [Deploy state and nameless teardown](configuration.md#deploy-state-and-nameless-teardown) |

Removes everything in this order: ownership check, Spark jobs, orphaned
pods, datagen jobs, table removal from the catalog, S3 bucket contents
(then the buckets themselves, see below), observability, the query engine,
the catalog, PostgreSQL, RBAC and secrets, and the namespace. Destroy runs no
table maintenance: no Iceberg `expire_snapshots` or `remove_orphan_files` and
no Delta `VACUUM` before the drops.

Table removal never deletes files. On Trino it runs
`CALL <catalog>.system.unregister_table(...)`, because Trino's `DROP TABLE`
deletes every file an Iceberg table references (including datagen files
registered with `add_files`) and a managed Delta table's directory. On Spark
Thrift an Iceberg `DROP TABLE` (no `PURGE`) removes only the catalog entry. A
Spark Thrift `DROP TABLE` of a Delta table deletes its directory, so it runs
only when destroy is emptying every bucket of the deployment; otherwise the
tables are left registered and reported. Files are removed only by the bucket
step, from buckets destroy proves it owns. A later run refuses to create a
Delta table over a `_delta_log` that is not in the catalog (for example after
`--keep-buckets` with the namespace deleted): delete that table directory, or
use other buckets. The scratch StorageClass is shared
cluster-scoped infrastructure and is never deleted.

S3 buckets are emptied, then deleted only if lakebench created them: deploy
records each bucket it creates (a `lakebench.created` tag where the backend
supports tagging, and the `lakebench.deployment/created-buckets` namespace
annotation), and destroy deletes a bucket only when it is in that record and
its ownership checks out. Buckets deploy adopted, pre-provisioned buckets
(`create_buckets: false`), and buckets emptied under `--force-legacy` are
emptied but kept. If a recorded bucket cannot be emptied or deleted, the
namespace is kept as the ownership record so a re-run can finish.

Destroy waits for the namespace to be gone before reporting it deleted. If a
concurrent destroy of the same deployment finished first and a redeploy has
re-created the name, destroy stops and leaves the new deployment alone.

Exit codes: `0` everything removed; `1` a step failed (see the summary);
`2` the config did not load, including a nameless config with no `--name`
in a directory that cannot name its deployment; `3` a nameless config could
not prove the deployment is its own, or the namespace was redeployed since
that check (nothing deleted either way), or a redeploy was found partway
through (destroy stops there, and the steps before it may have removed
components); `5` the confirmation prompt was
declined (no side effects); `6` everything else succeeded but the namespace
was still terminating at `--namespace-timeout` (usually a PVC or pod
finalizer; check with `kubectl get ns <namespace>` before re-deploying
under the same name). The full table is in [Exit Codes](exit-codes.md).

### report

Read a saved benchmark run. The default action prints the summary and
points at the delivered `run-<id>/report.html` without modifying it.
Use `--render` to regenerate a fresh HTML report at
`lakebench-output/reports/report-<run_id>-<ts>.html` without touching the
delivered file.

```
lakebench report [CONFIG_FILE] [OPTIONS]
```

The optional `CONFIG_FILE` scopes the default-summary lookup to that
deployment. On a shared `lakebench-output` tree it prevents `report
other.yaml` from picking up another deployment's latest run.

| Flag | Short | Default | Description |
|---|---|---|---|
| `--metrics` | `-m` | `./lakebench-output/runs` | Directory containing run subdirectories |
| `--run` | `-r` | latest | Specific run ID to report on |
| `--list` | `-l` | `false` | List available runs instead of reporting on one |
| `--render` |  | `false` | Regenerate HTML to `lakebench-output/reports/report-<run_id>-<ts>.html`. Never rewrites the delivered `report.html` in the run directory. |
| `--output` |  | (unset) | Explicit output path for `--render`. Refuses to overwrite an existing file unless `--force` is also given. |
| `--force` |  | `false` | Allow `--render` to overwrite an existing file at `--output`. Requires both `--render` and `--output`. |
| `--summary` | `-s` | `false` | With `--render`, also print the summary. Without `--render`, the summary is already printed. |

### results

Display pipeline benchmark results in the terminal.

```
lakebench results [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--metrics` | `-m` | `./lakebench-output/runs` | Directory containing run subdirectories |
| `--run` | `-r` | latest | Specific run ID |
| `--format` | `-o` | `table` | Output format: `table`, `json`, `csv` (`-f` is deprecated here) |

Shows a stage-matrix view of pipeline performance with throughput, data
volumes, executor counts, and timing for each stage.

### logs

Stream logs from a deployed component.

```
lakebench logs COMPONENT [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--follow` | `-F` | `false` | Follow log output (like `tail -f`; `-f` is deprecated here) |
| `--lines` | `-n` | `100` | Number of lines to show |
| `--name` | | | For a config with no name: the deployment name (several nameless configs in the directory, or a v1.6 directory). See [Deploy state and nameless teardown](configuration.md#deploy-state-and-nameless-teardown) |

Valid components: `postgres`, `hive`, `polaris`, `trino`, `spark-driver`.

### journal

View command and execution provenance journal.

```
lakebench journal [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--session` | `-s` | | Show events for a specific session |
| `--last` | `-n` | `10` | Show last N sessions |
| `--dir` | | `./lakebench-output/journal` | Journal directory |

Displays the history of all lakebench operations including deploys, data
generation, pipeline runs, and teardowns.

### reproduce

Record a reproduction package from a saved run, or verify a later run
against one. See [Reproduce](deep-dive/reproduce.md).

```
lakebench reproduce --record RUN_ID --write PACKAGE.yaml [--config-reference PATH]
lakebench reproduce PACKAGE.yaml [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--record` | | | Record mode: build a package from this saved run id |
| `--write` | | | Record mode: write the package to this path |
| `--config-reference` | | | Record mode: store this relative config path in the package |
| `--config` | `-c` | package's config | Verify mode: config to run instead of the package's `config_reference` |
| `--timeout` | `-t` | auto | Verify mode: per-job timeout in seconds |
| `--keep` | | `false` | Verify mode: keep the deployment after the run. reproduce never destroys before the run: it refuses an existing namespace or bucket |
| `--allow-commit-drift` | | `false` | Verify mode: run even when HEAD differs from the recorded commit (refused with exit 14 otherwise) |
| `--dry-run` | | `false` | Verify mode: parse the package and exit |
| `--report` | | | Verify mode, registered looks only: the look's report; its sha256 is checked against the look record and nothing is run |

Exit codes: `0` pass; `14` (requirement unmet) for performance or
correctness drift, commit drift without `--allow-commit-drift`, or a run that
did not follow the package (different samples, maintenance policy, experiment
or benchmark results); `2` for a package or config refused before running
(including `create_namespace: false` or `create_buckets: false`); `3` when
the namespace or a bucket already exists, or when another deploy replaced the
deployment reproduce created (then nothing is destroyed); `4` when the
namespace or buckets cannot be read; `1` when the pipeline could not run.
A package from a registered evaluation or robustness look is never rerun:
`--report` matching the look record exits `0`, a mismatch `14`, no
`--report` `2`; a held-out package whose look has not run is refused with
`3`. 1.6 used `1` for performance drift and
`2` for correctness drift.

### financial

AML (financial workload) operator actions. Each submits a Spark job against
the deployment in the config; `--wait/--no-wait` (default wait) controls
whether the command waits for it.

| Subcommand | Required arguments | Purpose |
|---|---|---|
| `financial score CONFIG` | `--manifest S3_URI --output S3_URI` | Compute rule recall from the datagen manifest and `gold.alerts` |
| `financial reference-score CONFIG` | `--manifest S3_URI --output-prefix S3_URI` | Run the reference detector and leakage gate over silver and the manifest (`--leakage-threshold`, default 0.1) |
| `financial replay CONFIG` | `--rule RULE_ID` | Rerun one detection rule against a historical Iceberg snapshot (`--depth-months`, default 60; `--threshold`; `--output-alerts`, default the gold alerts table with an `_replay` suffix) |
| `financial reproduce CONFIG` | `--alert-id ID` | Reproduce one past alert via Iceberg time travel |

See [AML Scoring](aml-scoring.md).

### admin

Cluster-admin operations on shared infrastructure. Mutating subcommands take
the cluster-wide `lakebench-cluster-lock` lease.

| Subcommand | Options | Purpose |
|---|---|---|
| `admin status` | `--operator-namespace` (default `spark-operator`) | Installed operators, lease state, lakebench namespaces, controller `/tmp` size and storage evictions |
| `admin doctor [CONFIG]` | `-f/--file` | Read-only preflight of shared cluster state (StorageClass, operators, watch list, controller `/tmp`) |
| `admin install-scratch-storage-class [CONFIG]` | `-f/--file` | Install the scratch StorageClass named by the config |
| `admin install-spark-operator [CONFIG]` | `--version`, `--operator-namespace`, `--controller-tmp-size` (default 8Gi, floor 4Gi), `-f/--file` | Install or upgrade the shared Spark Operator. An upgrade keeps the tenants' watch lists, the installed chart unless `--version` or the config names one, and a larger `/tmp` already set |
| `admin repair-operator [CONFIG]` | `--dry-run`, `--controller-tmp-size` (default 8Gi), `-f/--file` | Remove stale watch-list entries and raise a controller `/tmp` smaller than the given size |
| `admin migrate-deployment NAMESPACE [CONFIG]` | `--api-server-fingerprint`, `-f/--file` | Stamp identity annotations on a legacy pre-ownership namespace |
| `admin reclaim-bucket BUCKET [CONFIG]` | `--force-nonempty`, `-f/--file` | Rewrite a bucket's ownership tag to this deployment and this cluster, or on a backend without tagging its owner marker (`.lakebench/owner.json`); refused (exit 3) when the bucket holds objects unless `--force-nonempty`, exit 4 when the cluster fingerprint cannot be computed |
| `admin release-lock` | `--force` | Release an expired cluster lease; `--force` releases a live one (last resort) |

The Spark Operator runs spark-submit in its controller pod, which caches
jars under `/tmp`; the chart default of 1Gi is too small and gets the
controller evicted. See [Troubleshooting](troubleshooting.md).

### version

Show version information.

```
lakebench version
```

No flags. Prints the installed lakebench version.

## Common Patterns

### Typical Workflow

```bash
lakebench init                        # create config
lakebench config validate             # check connectivity
lakebench deploy --yes                # deploy infrastructure
lakebench generate --timeout 14400    # generate data (large scales need hours)
lakebench run --timeout 7200          # run pipeline + benchmark, delivers report.html
lakebench report                      # print the summary of the delivered report
lakebench destroy --force             # tear down everything
```

### Re-running the Pipeline

```bash
lakebench clean data --force          # empty S3 buckets
lakebench generate --timeout 14400    # regenerate data
lakebench run                         # re-run pipeline
```

### Running a Single Stage

```bash
lakebench run --stage silver-build --timeout 3600
```

### Continuous Pipeline

```bash
lakebench run --continuous --duration 3600
lakebench stop                        # stop the continuous jobs manually
```
