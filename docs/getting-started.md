# Getting Started

Guide: from an empty cluster to a first report and back: install, deploy, run
the pipeline at scale 1, read the report, destroy.

The stack is
Polaris, Iceberg, Spark and Trino, from one YAML file.

## Prerequisites

### Kubernetes cluster

- Kubernetes 1.26+. Tested on OpenShift 4.x (bare metal and vSphere) and
  vanilla Kubernetes (kubeadm, EKS, GKE, AKS).
- Permission to create a namespace. `deploy` creates its own and refuses one
  it did not create (see [Operations](operations.md)).
- On OpenShift, Lakebench grants the `anyuid` Security Context Constraint to
  its Spark and PostgreSQL service accounts, and stops the deploy if that
  grant is refused.
- Room for the run. Scale 1 batch needs about 41 cores and 544 GB for
  `hive-iceberg-spark-trino`. [Sizing](sizing.md) has every workload, mode
  and scale. `lakebench plan` checks your cluster.

The checks a cluster must pass, and the fix for each, are on the generated
[Prerequisites](prerequisites.md) page.

### CLI tools on PATH

| Tool | Used for |
|------|----------|
| `kubectl` | All Kubernetes operations |
| `helm` | Spark Operator install, namespace detection, and Stackable operators (Hive catalog) |

Lakebench does not need `oc`, even on OpenShift.

### Default StorageClass

You need a default StorageClass, or set `postgres.storage_class` and
`deps.storage_class` in your config. Check with `kubectl get sc`.

### S3-compatible object storage

You need an endpoint URL, an access key and a secret key, for the bronze,
silver and gold buckets.

- Validated: Pure Storage FlashBlade (HTTP, path-style) and Garage.
- Expected to work, not validated: AWS S3 (virtual-hosted or path-style),
  MinIO, and other S3-compatible stores (Ceph RGW, Dell ECS).

`lakebench config storage` checks yours. See
[Storage Backends](storage-backends.md).

### Shared cluster components

```bash
lakebench admin install --component all lakebench.yaml --dry-run   # what it would install
lakebench admin install --component all lakebench.yaml             # once per cluster
lakebench admin doctor lakebench.yaml                              # confirm everything is in place
```

What each piece is and when it is needed:
[Operations](operations.md#installing-the-shared-pieces). For Polaris, see
the [Polaris Quick Start](component-polaris.md).

## Install

```bash
pip install lakebench-k8s
lakebench version
```

Binaries that need no Python (Linux amd64, macOS amd64 and arm64), the
install script and source installs are in
[Deployment](deployment.md#installing-the-cli).

## First run

The whole path, on a fresh cluster:

```bash
lakebench init --endpoint http://your-s3-endpoint:80   # writes lakebench.yaml
export LAKEBENCH_S3_ACCESS_KEY=... LAKEBENCH_S3_SECRET_KEY=...
lakebench admin install --component all lakebench.yaml # cluster admin, once per cluster
lakebench run lakebench.yaml --generate --yes          # deploy, generate, pipeline, benchmark
lakebench report lakebench.yaml                        # print the scorecard
lakebench destroy lakebench.yaml --yes                 # remove what this deployment created
```

Both flags on `run` matter:

- `--yes` lets `run` deploy the namespace and components. Without it `run`
  refuses and asks you to run `lakebench deploy` first.
- `--generate` fills the bronze bucket first. Without it `run` refuses with
  exit 4 and names `--generate`.

The steps below walk the same path with the checks in between.

### 1. Generate a configuration file

```bash
lakebench init --name my-first-lakehouse --endpoint http://your-s3-endpoint:80
export LAKEBENCH_S3_ACCESS_KEY=YOUR_ACCESS_KEY
export LAKEBENCH_S3_SECRET_KEY=YOUR_SECRET_KEY
```

`init` writes `lakebench.yaml` in the current directory and prints what it
chose on stderr.

- Without `--name`, the name is `lb-<user>-<4 hex>`, new on every `init`.
- Without `--recipe`, the recipe is `polaris-iceberg-spark-trino`.
- Credentials are `${VAR}` references, never plaintext. `--credentials-env
  PREFIX` picks other variable names. `--access-key` and `--secret-key` are
  refused.

The file holds only what a first run needs:

```yaml
name: my-first-lakehouse
recipe: polaris-iceberg-spark-trino
workload:
  schema: customer360
  datagen:
    scale: 1  # 1 is about 10 GB of bronze
platform:
  storage:
    s3:
      endpoint: "http://your-s3-endpoint:80"
      access_key: "${LAKEBENCH_S3_ACCESS_KEY}"
      secret_key: "${LAKEBENCH_S3_SECRET_KEY}"
# Set by the recipe. Written out, each must agree with it:
# architecture:
#   catalog: {type: polaris}
#   table_format: {type: iceberg}
#   pipeline_engine: spark
#   query_engine: {type: trino}
```

- The recipe sets the catalog, table format, pipeline engine and query
  engine. A component with a different value (say `catalog.type: hive`
  under this recipe) is refused at load, naming both keys. Pick another
  recipe with `lakebench init --recipe` instead.
- For virtual-hosted bucket addressing (like AWS S3), set
  `path_style: false`. For MinIO and FlashBlade leave it `true`, the
  default.

### 2. Plan and validate

```bash
lakebench plan lakebench.yaml
lakebench config validate lakebench.yaml
```

- `plan` shows the minimum cluster, the cluster prerequisites, the Polaris
  client secret's source and the hosts the deploy contacts. A missing
  scratch StorageClass, Spark Operator or Stackable fails with the
  `admin install` command a cluster admin runs. `--offline` sizes without a
  cluster.
- `config validate` checks YAML syntax, the schema, Kubernetes connectivity
  and S3 reachability. Fix any errors before you continue.
- Before the first deploy, its Spark Operator section reports that the
  namespace is not watched yet. That is expected: `deploy` adds it.

### 3. Run

```bash
lakebench run lakebench.yaml --generate --yes
```

Scale 1 (about 10 GB) exercises every stage of the pipeline. This deploys (if needed), generates the data, runs the pipeline and the
benchmark. `--yes` skips confirmation prompts and enables auto-deploy.

Or run each step on its own:

```bash
lakebench deploy lakebench.yaml --yes     # deploy infrastructure
lakebench status lakebench.yaml           # verify deployment
lakebench generate lakebench.yaml         # generate test data
lakebench run lakebench.yaml              # run pipeline + benchmark
```

`run` executes three Spark jobs in sequence, then a query benchmark:

1. **bronze-verify** validates and deduplicates the raw Parquet data.
2. **silver-build** enriches, normalizes and writes an Iceberg table.
3. **gold-finalize** aggregates into an executive dashboard.
4. **benchmark** runs 8 analytical queries (12 for AML) against the silver
   and gold tables through the query engine (Trino by default).

Each job's progress, duration and throughput are recorded.

### 4. Read the report

```bash
lakebench report
```

- Every run writes `lakebench-output/runs/run-<id>/report.html`: job
  performance, query latencies, throughput and a configuration snapshot.
- `lakebench report` prints the latest run's summary and the path to that
  file. Open it in a browser.
- `lakebench report --render` writes a fresh copy under
  `lakebench-output/reports/` without touching the original.
- `lakebench report --list` lists all recorded runs.
- `lakebench report lakebench.yaml --format table` prints the stage matrix
  (`--format json` or `csv` for scripts).

### 5. Compare two configurations

```bash
lakebench init --recipe hive-iceberg-spark-trino --endpoint http://your-s3-endpoint:80 -o lakebench-hive.yaml
lakebench deploy lakebench-hive.yaml --yes
lakebench run lakebench-hive.yaml --generate --yes
lakebench report lakebench.yaml
lakebench report lakebench-hive.yaml
```

Give the two configs different names: a name is one deployment.

Read the two HTML reports side by side (`report` prints each path). The
Experiment section of each states the corpus, datagen image, components,
maintenance, the stages and rules that ran, the benchmark query set id and,
for AML batch runs, the alert set.

Compare performance only when the corpus, the query set id and the alert set
match. Lakebench does not compare query answers: check the per-query row
counts and numbers of both reports by eye. A number bounded by a Lakebench
cap is labelled as such.

### 6. Tear down

```bash
lakebench destroy lakebench.yaml
```

`destroy` asks first; `--yes` or `--force` skips the prompt. It removes only
what this deployment created:

- It stops the Spark jobs and datagen pods, and removes the tables from the
  catalog without deleting files.
- It empties the buckets and deletes the ones Lakebench created. Buckets that
  existed before deploy are kept.
- It tears down the components and deletes the namespace.

The full order and the bucket ownership rules are in
[Deployment](deployment.md#destroy-order).

## If something goes wrong

- **Deploy fails partway.** Re-run it. `deploy` is idempotent: components
  that already exist are skipped.
- **Generate fails or times out.** Raise the timeout with `--timeout 14400`
  (4 hours). The generator cannot resume, so a re-run starts from the
  beginning. The failed run left partial data in bronze, so re-run with
  `lakebench generate lakebench.yaml --regenerate`, which clears the datagen
  prefix first. Without it `generate` exits 3 (refused) and names the
  non-empty prefix.
- **A pipeline stage fails.** Re-run that stage:
  `lakebench run lakebench.yaml --stage silver-build`.
- **The benchmark fails but the pipeline succeeded.** Trino may not be
  healthy. Check with `lakebench status`, then re-run the benchmark alone:
  `lakebench benchmark lakebench.yaml`.
- **Start over.** `lakebench destroy lakebench.yaml --force`, then
  `lakebench deploy lakebench.yaml`.

More in [Troubleshooting](troubleshooting.md).

## What ran

The pipeline ran bronze-silver-gold stages and a query workload. See
[Running pipelines](running-pipelines.md) for details.

## Useful commands

```bash
lakebench status lakebench.yaml                # component readiness
lakebench config show lakebench.yaml           # resolved config, each value's source, peak request
lakebench logs lakebench.yaml trino --follow   # a component's logs, tailed

lakebench query lakebench.yaml --example revenue      # revenue with 7-day moving average
lakebench query lakebench.yaml --example channels     # channel revenue breakdown
lakebench query lakebench.yaml --example engagement   # engagement and churn risk
lakebench query lakebench.yaml --example clv          # customer lifetime value
lakebench query lakebench.yaml --sql "SELECT count(*) FROM lakehouse.gold.customer_executive_dashboard"
```

Every command and flag: [CLI Reference](cli-reference.md).

## Local mode (no cluster)

`lakebench init --local` writes a podman or docker config that runs against
a Garage container on `localhost`:

```bash
lakebench init --local --scale 1
lakebench deploy lakebench.yaml --local
lakebench run lakebench.yaml --local --generate --yes
lakebench destroy lakebench.yaml --local
```

- `--generate` fills bronze on the first local run. Later runs against the
  same `--workdir` reuse it. You need `--generate` again only after
  `destroy --remove-data` or a scale change.
- Without `--generate` the pipeline runs on whatever bronze the workdir
  holds. An empty workdir gives an empty pipeline.
- Local mode is Iceberg and Customer 360 batch only. AML configs and
  continuous mode are refused with an error naming the fix; see the
  [Compatibility Matrix](compatibility-matrix.md).

## Scaling up

Change `scale` in the config. See [Data generation](data-generation.md)
for scale factors and row counts.

- `lakebench config recommend lakebench.yaml` gives the largest scale your
  cluster holds.
- The `generate` and `run` timeouts grow with scale. Leave `--timeout`
  unset; raise it only after a job times out
  ([Operations](operations.md#several-deployments-on-one-cluster)).

## Other recipes and workloads

- **Recipes.** There are 11; `lakebench init` writes
  `polaris-iceberg-spark-trino`, and a config with no recipe falls back to
  Hive, Iceberg and Trino. See [Recipes](recipes.md) to choose.
- **Component versions** and how to override them:
  [Supported Components](compatibility-matrix.md#components).
- **AML (financial crime).** Any Iceberg recipe runs it. Set
  `workload.schema: financial`, or start from
  [`examples/polaris-iceberg-spark-financial.yaml`](../examples/polaris-iceberg-spark-financial.yaml).
  The loop is the same: `deploy`, `generate`, `run`. `run` scores the nine
  detection rules (W1-W8 and W17) inline after gold-finalize.

  Recall and precision from the default seed, 43, are in-sample and
  uncalibrated. What that means, how to cite them, and the reference
  detector: [AML Scoring](aml-scoring.md).

## Next steps

- [Running Pipelines](running-pipelines.md): batch and continuous modes
- [Configuration](configuration.md): every YAML field
- [Benchmarking](benchmarking.md): what the scores mean
- [Choosing a catalog](recipes.md#choosing-a-catalog): Hive or Polaris
