# CLI Reference

Lakebench provides a single `lakebench` command with subcommands for every
stage of the deployment and benchmarking lifecycle. The CLI is built with
Typer and Rich.

```
lakebench [COMMAND] [OPTIONS] [CONFIG_FILE]
```

Most commands accept an optional config file argument. If omitted, the CLI
looks for `./lakebench.yaml` in the current directory.

Several commands prompt for confirmation before running. Use `--yes` (deploy,
generate) or `--force` (destroy, clean) to skip the prompt. The `--force`
naming is used for destructive operations to make scripted teardowns more
deliberate.

## Commands

### init

Generate a starter configuration file with sensible defaults.

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
| `--interactive` | `-i` | `false` | Guided setup with prompts |
| `--force` | `-f` | `false` | Overwrite existing file |

```bash
# Quick start with flags
lakebench init --endpoint http://my-s3:80 --access-key AAA --secret-key BBB

# Interactive guided setup
lakebench init --interactive
```

### validate

Validate configuration and test connectivity to S3 and Kubernetes.

```
lakebench validate [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--verbose` | `-v` | `false` | Show detailed validation output |

Checks performed: YAML syntax, required fields, S3 endpoint reachability,
S3 credential validity, Kubernetes context accessibility, namespace status,
platform security (SCC on OpenShift), storage classes, Spark Operator status,
and compute adequacy for the configured scale.

### deploy

Deploy lakehouse infrastructure to Kubernetes.

```
lakebench deploy [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--dry-run` | | `false` | Show what would be deployed without making changes |
| `--include-observability` | | `false` | Deploy Prometheus and Grafana |
| `--yes` | `-y` | `false` | Skip confirmation prompt |

Deploys components in order: namespace, secrets, scratch StorageClass,
PostgreSQL, catalog (Hive or Polaris), Trino, Spark RBAC, and optionally
Prometheus and Grafana.

### generate

Generate synthetic data to the bronze S3 bucket.

```
lakebench generate [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--wait` | `-w` | `true` | Wait for data generation to complete |
| `--timeout` | `-t` | `7200` | Timeout in seconds when waiting |
| `--yes` | `-y` | `false` | Skip confirmation prompt |
| `--resume` | | `false` | Resume from checkpoint if previous run was interrupted |

Runs parallel Kubernetes Jobs to produce Parquet files. At scale 100 this
generates approximately 1 TB of data. Use `--timeout` for large scales that
may take hours. Without `--yes`, the command prompts for confirmation before
submitting jobs.

### run

Execute the data pipeline (batch or continuous).

```
lakebench run [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--stage` | `-s` | all | Run a specific stage only (`bronze-verify`, `silver-build`, `gold-finalize`) |
| `--timeout` | `-t` | auto | Timeout per job in seconds (`max(3600, scale * 60)` when omitted) |
| `--skip-benchmark` | | `false` | Skip the Trino query benchmark after pipeline |
| `--continuous` | | `false` | Run streaming pipeline instead of batch |
| `--duration` | | config value | Streaming run duration in seconds |
| `--generate` | | `false` | Run datagen before pipeline (batch mode only; continuous always runs datagen) |

In batch mode, runs `bronze-verify`, `silver-build`, and `gold-finalize`
sequentially as Spark jobs, then executes the query benchmark.

In continuous mode (`--continuous`), launches `bronze-ingest`, `silver-stream`,
and `gold-refresh` as concurrent Spark Structured Streaming jobs, monitors
for the configured duration, then stops streaming and runs the benchmark.

### stop

Stop running streaming jobs.

```
lakebench stop [CONFIG_FILE]
```

Deletes all streaming SparkApplications (`bronze-ingest`, `silver-stream`,
`gold-refresh`) from the cluster. No flags required.

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
| `--iterations` | `-n` | `1` | Iterations per query (>1 uses median) |
| `--class` | `-c` | all | Run only a specific query class (`scan`, `analytics`, `gold`) |

Executes 8 analytical queries against the gold layer and reports Queries
per Hour (QpH). Power mode runs queries sequentially. Throughput mode runs
N concurrent streams. Composite mode runs both and reports the geometric mean.

### query

Execute SQL queries against Trino.

```
lakebench query [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--sql` | `-q` | | SQL query string to execute |
| `--example` | `-e` | | Built-in query name (`count`, `revenue`, `channels`, `engagement`, `funnel`, `clv`) |
| `--file` | | | Read SQL from file (use `-` for stdin) |
| `--interactive` | `-i` | `false` | Start interactive SQL shell (REPL) |
| `--format` | `-o` | `table` | Output format: `table`, `json`, `csv` |
| `--show-query` | | `false` | Print the SQL before executing |
| `--timeout` | `-t` | `120` | Query timeout in seconds |

Specify exactly one of `--sql`, `--example`, `--file`, or `--interactive`.

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

Displays a table of components (PostgreSQL, Hive, Trino, Prometheus, Grafana)
with their readiness status and replica counts.

### info

Show configuration summary with scale dimensions and compute guidance.

```
lakebench info [CONFIG_FILE]
```

Displays deployment name, namespace, recipe, schema, scale factor, derived
dimensions (customers, rows, data size), per-job executor counts (auto vs
override), catalog type, table format, query engine, S3 endpoint, and bucket
names. No additional flags.

### recommend

Show cluster sizing guidance for lakebench workloads.

```
lakebench recommend [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--cores` | `-c` | auto-detect | Total cluster CPU cores |
| `--memory` | `-m` | auto-detect | Total cluster memory in GB |
| `--scale` | `-s` | auto | Target scale factor to check requirements for |
| `--extended` | `-e` | `false` | Show extended scale with reduced datagen parallelism |

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
| `--force` | `-f` | `false` | Skip confirmation prompt |
| `--metrics-dir` | `-m` | `./lakebench-output/runs` | Metrics directory (for `metrics` target) |

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

Tear down all Lakebench resources from the cluster.

```
lakebench destroy [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--force` | `-f` | `false` | Skip confirmation prompt |

Removes everything in the correct order: Spark jobs, orphaned pods, datagen
jobs, Iceberg table maintenance, DROP TABLEs, S3 bucket contents, Grafana,
Prometheus, Trino, Hive/Polaris, PostgreSQL, RBAC, scratch StorageClass,
and the namespace.

### report

Generate an HTML benchmark report from collected metrics.

```
lakebench report [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--metrics` | `-m` | `./lakebench-output/runs` | Directory containing run subdirectories |
| `--run` | `-r` | latest | Specific run ID to report on |
| `--summary` | `-s` | `false` | Print key scores to the terminal without opening the HTML report |
| `--list` | `-l` | `false` | List available runs instead of generating report |

### results

Display pipeline benchmark results in the terminal.

```
lakebench results [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--metrics` | `-m` | `./lakebench-output/runs` | Directory containing run subdirectories |
| `--run` | `-r` | latest | Specific run ID |
| `--format` | `-f` | `table` | Output format: `table`, `json`, `csv` |

Shows a stage-matrix view of pipeline performance with throughput, data
volumes, executor counts, and timing for each stage.

### logs

Stream logs from a deployed component.

```
lakebench logs COMPONENT [CONFIG_FILE] [OPTIONS]
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--follow` | `-f` | `false` | Follow log output (like `tail -f`) |
| `--lines` | `-n` | `100` | Number of lines to show |

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
lakebench validate                    # check connectivity
lakebench deploy --yes                # deploy infrastructure
lakebench generate --timeout 14400    # generate data (large scales need hours)
lakebench run --timeout 7200          # run pipeline + benchmark
lakebench report                      # generate HTML report
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

### Continuous Streaming Pipeline

```bash
lakebench run --continuous --duration 3600
lakebench stop                        # stop streaming jobs manually
```
