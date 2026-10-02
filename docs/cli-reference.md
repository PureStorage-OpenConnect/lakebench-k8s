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

Generate a starter configuration file.

```
lakebench init [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--output` | `-o` | `lakebench.yaml` | Output file path |
| `--name` | `-n` | `my-lakehouse` | Deployment name |
| `--scale` | `-s` | `10` | Scale factor (1 = ~10 GB, 100 = ~1 TB) |
| `--endpoint` | | `""` | S3 endpoint URL |
| `--access-key` | | `""` | S3 access key |
| `--secret-key` | | `""` | S3 secret key |
| `--namespace` | | `""` | Kubernetes namespace |
| `--recipe` | `-r` | `""` | Architecture recipe |
| `--workload` | `-w` | `customer360` | Workload schema: `customer360` or `financial` |
| `--interactive/--no-interactive` | `-i` | `true` | Guided setup with prompts |
| `--advanced` | | `false` | Full 5-step wizard (recipe, mode, scale) |
| `--force` | | `false` | Overwrite existing file (`-f` is deprecated here) |
| `--local` | | `false` | Generate a config for local mode (podman/docker, no Kubernetes) |

Quick mode (default) asks 4 questions: endpoint, access key, secret key, scale.
Advanced mode (`--advanced`) runs the full 5-step wizard with recipe selection,
pipeline mode, and detailed review.

```bash
# Quick setup (4 questions)
lakebench init

# Full wizard with recipe selection
lakebench init --advanced

# Non-interactive with flags
lakebench init --no-interactive --endpoint http://my-s3:80 --access-key AAA --secret-key BBB
```

### compare

Compare two configurations side-by-side.

```
lakebench compare CONFIG_A CONFIG_B [OPTIONS]
```

On a cluster, `compare` does not deploy: run `lakebench deploy` on both
configs first. With `--local` it deploys each stack itself before the run.

| Flag | Short | Default | Description |
|---|---|---|---|
| `--keep` | | `false` | Keep deployments after (do not destroy) |
| `--scale` | | (from config) | Override scale for both configs |
| `--output` | `-o` | (none) | Write comparison report to file |
| `--format` | | `table` | Output format: table, json, csv, html |
| `--skip-benchmark` | | `false` | Skip benchmark phase |
| `--local` | | `false` | Run both configs on this host with podman/docker |
| `--generate` | | `false` | Generate data before each run. On a cluster, a side whose bronze prefix already holds data fails when its run reaches datagen (the inner `run` exits 3, refused, and `compare` reports that side as failed); empty it first with `lakebench clean bronze <config>` |
| `--timeout` | | `7200` | Per-run timeout in seconds |
| `--yes` | `-y` | `false` | Skip confirmation prompt |

```bash
lakebench compare hive-config.yaml polaris-config.yaml
lakebench compare a.yaml b.yaml --format json --output comparison.json
lakebench compare a.yaml b.yaml --local --generate
```

**Verdicts.** `compare` checks the two runs' workload results before it
shows any performance difference, and prints one of three verdicts:

| Verdict | Meaning | Exit |
|---|---|---|
| comparable | Both runs completed and their benchmark result fingerprints match | 0 |
| NOT COMPARABLE | Different experiments (workload, corpus, scale, mode), different results, a failed run, or a record without the experiment block. Deltas and winner colouring are withheld | 1 |
| comparability not established | A side has no checked results (`--skip-benchmark`, a recipe without a query engine, or a continuous run without a settled result check). Raw numbers are shown, no deltas or winner | 0 |

A comparable pair whose execution conditions differ (effective maintenance,
query access path, system, benchmark iterations or in-stream rounds, limits
that bound) is labelled **not like-for-like** and the differences are
listed. Each side's support state (supported, unverified, unsupported) is
shown. `comparison.json` records `verdict`, `comparable`, `like_for_like`,
`condition_differences`, `support` and `refusals`.

The two configs run one after the other, not side by side. Running them
concurrently on one host would measure the contention between them rather than
the configs themselves.

With `--local`, the two configs must have different `name:` values. The name
keys the workdir, the Garage container, and the bucket names, so identical
names would mean the second config ran against the first one's data.

**Reading the delta column** (comparable pairs only). Delta is B relative to A, and it is coloured by
whether the change is an improvement -- higher is better for QpH, throughput,
and efficiency; lower is better for times and sizes. Descriptive scores
(`scale_ratio`, `total_data_processed_gb`, `total_s3_objects`) are never
coloured, because a change in them is information rather than a win or a loss.

Differences under 2% are printed without colour. Repeated local benchmarks on
unchanged data measured 0.9% run-to-run spread (n=5, stdev 1.8 QpH on a mean of
472.9), so a smaller difference is not something a single pair of runs can
resolve. The figure is recorded as `noise_floor_pct` in the saved comparison.

One caveat on local timings: `bronze-verify` is the first stage to touch S3 and
runs about 9s slower on a freshly deployed stack than on a warm one (30.4s vs
21.4s measured). The heavier stages do not show this -- `silver-build` and
`gold-finalize` were stable within a second across runs. With `--local`,
`compare` deploys each config fresh, so both sides pay this cost equally.

### config

Configuration management subcommands.

```
lakebench config show CONFIG_FILE       # show resolved config with source annotations
lakebench config validate CONFIG_FILE   # validate config + test connectivity
lakebench config storage CONFIG_FILE    # check the S3 backend supports what lakebench needs
lakebench config recommend CONFIG_FILE  # show cluster sizing guidance for the config's mode
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
and compute adequacy for the configured scale. Before the first deploy, a
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
| `--timeout` | `-t` | `3600` | Global deployment timeout in seconds (`0` = no timeout) |
| `--local` | | `false` | Deploy locally with podman/docker instead of Kubernetes |
| `--workdir` | | `~/.lakebench/local/<name>` | Host directory for local mode state (only used with `--local`) |
| `--force-legacy` | | `false` | Claim ownership without tag proof: a pre-1.5 annotation-less namespace or untagged bucket, or a bucket on a backend without tagging that does not match the deployment-name prefix. Use only when you have confirmed the resources are yours |

Deploys components in order: namespace, secrets, S3 buckets, scratch
StorageClass check (it must already exist; `lakebench admin
install-scratch-storage-class` installs it), PostgreSQL, catalog (Hive or
Polaris), Spark RBAC, Unity Catalog (only when `catalog.type` is `unity`; no
recipe uses it), Spark Operator check and watch-list entry for the
namespace (under the cluster lock), then the query engine (Trino, Spark
Thrift or DuckDB), and optionally the shared observability stack. The Spark
Operator step always runs: it checks the operator is ready and adds the
namespace to its watch list whatever `operator.install` says;
`operator.install: true` also installs a missing operator.

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
| `--regenerate` | | `false` | Empty the bronze bucket before generating. Without this flag, a non-empty bronze prefix is refused (exit 3) so existing datagen output is never overwritten silently. |

Runs parallel Kubernetes Jobs to produce Parquet files. At scale 100 this
generates approximately 1 TB of data. Use `--timeout` for large scales that
may take hours. Without `--yes`, the command prompts for confirmation before
submitting jobs. The Rust generator has no checkpoint-resume; an interrupted
run is re-run from the start.

**Exit codes**: `0` on success, `1` on generic failure, `2` when bronze is
non-empty and `--regenerate` was not passed, `5` when datagen exceeds its
wait budget (`--timeout`); in that case the datagen Job and any leftover
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
| `--skip-preflight` (alias `--skip-deploy`) | | `false` | Skip prerequisite checks and infrastructure validation |
| `--skip-generate` | | `false` | Skip datagen even with `--generate` |
| `--regenerate` | | `false` | With `--generate`: empty the bronze bucket before generating. Without this flag, a non-empty bronze prefix is refused (exit 3) so existing datagen output is never overwritten silently. No effect without `--generate`. |
| `--skip-maintenance` | | `false` | Skip pre-benchmark maintenance (compaction, snapshot expiry) |
| `--force-rebuild` | | `false` | Silver batch only: opt in to a full rebuild that drops an existing populated silver table. Atomically bumps the deployment's silver rebuild epoch so downstream Delta idempotency keys move to a new namespace |
| `--force-reset` | | `false` | Continuous c360 only: allow the run to drop existing bronze_raw, silver and gold tables, stream checkpoints and raw data. Without it a continuous run over existing state refuses and lists what it would delete. Raw data alone from `lakebench generate` on a deployment with no tables or checkpoints is not refused: continuous runs generate their own data, so a separate `generate` before `run --continuous` is not needed |
| `--deploy-only` | | `false` | Deploy infrastructure and exit |
| `--generate-only` | | `false` | Deploy + generate data and exit |
| `--continuous` | | `false` | Run the continuous pipeline instead of batch. `--sustained` is a deprecated hidden alias. |
| `--duration` | | config value | Continuous run duration in seconds |
| `--generate` | | `false` | Run datagen before pipeline (batch mode only) |
| `--yes` | `-y` | `false` | Skip confirmation prompts |
| `--local` | | `false` | Run locally with podman/docker instead of Kubernetes |
| `--workdir` | | `~/.lakebench/local/<name>` | Host directory for local mode state (only used with `--local`) |

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
[Running Pipelines](running-pipelines.md#continuous-mode).

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
names, and the peak CPU / memory / scratch the pipeline requests
(`compute_peak_requirements()` plus the query engine and catalog). No
additional flags.

### recommend

Deprecated and hidden; `lakebench config recommend [CONFIG_FILE]` runs the same
logic for the config's pipeline mode. The Spark figure is never below
`compute_peak_requirements()`, the request the `run` capacity preflight checks.

Show cluster sizing guidance for lakebench workloads.

```
lakebench recommend [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--cores` | `-c` | auto-detect | Total cluster CPU cores |
| `--memory` | `-m` | auto-detect | Total cluster memory in GB |
| `--scale` | `-s` | auto | Target scale factor to check requirements for |
| `--slow-datagen` | | `false` | Reduce datagen parallelism to fit smaller clusters (slower generation, same Spark resources). `--extended` / `-e` is a deprecated alias |
| `--mode` | | `batch` | Pipeline mode: `batch` or `continuous` |
| `--schema` | | `customer360` | Workload schema: `customer360` or `financial` |

Without arguments, auto-detects the connected cluster capacity and shows the
maximum feasible scale. Use `--scale` to check what a specific scale requires.

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
that check (nothing deleted either way); `5` the confirmation prompt was
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

Exit codes: `0` pass; `14` (requirement unmet) for performance or
correctness drift, commit drift without `--allow-commit-drift`, or a run that
did not follow the package (different samples, maintenance policy, experiment
or benchmark results); `2` for a package or config refused before running
(including `create_namespace: false` or `create_buckets: false`); `3` when
the namespace or a bucket already exists, or when another deploy replaced the
deployment reproduce created (then nothing is destroyed); `4` when the
namespace or buckets cannot be read; `1` when the pipeline could not run. 1.6 used `1` for performance drift and
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
| `admin reclaim-bucket BUCKET [CONFIG]` | `--force-nonempty`, `-f/--file` | Rewrite a bucket's ownership tag to this deployment (refused when the bucket holds objects unless `--force-nonempty`) |
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
lakebench init --interactive          # create config
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
