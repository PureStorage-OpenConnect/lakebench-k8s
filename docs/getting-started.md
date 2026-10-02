# Getting Started

This guide walks you through installing Lakebench, deploying your first
lakehouse stack on Kubernetes, running the medallion pipeline, and viewing
benchmark results. By the end you will have a working bronze-to-gold
pipeline with Spark, Iceberg, and Trino -- all from a single YAML file.

---

## Prerequisites

Before you begin, make sure your environment has the following.

If you are the **cluster admin** setting up Lakebench on a shared cluster for the first time, the fastest path is:

```bash
lakebench admin install-spark-operator            # once per cluster
lakebench admin install-scratch-storage-class     # once per cluster, if using scratch PVCs
lakebench admin doctor                            # confirm everything is in place
```

Developers then use ordinary `lakebench deploy` / `run` / `destroy` without cluster-admin privileges. Every `admin` mutation takes a cluster-wide lease so concurrent admins on different workstations do not race each other; see `lakebench admin --help` for the full subcommand tree.

### Kubernetes cluster

Any cluster running Kubernetes 1.26+ will work. Lakebench is tested on:

- **OpenShift 4.x** (bare metal and vSphere)
- **Vanilla Kubernetes** (kubeadm, EKS, GKE, AKS)

You need admin-level access to the target namespace (or permission to create
one). On OpenShift, Lakebench grants the `anyuid` Security Context
Constraint to its Spark and PostgreSQL service accounts, and stops the deploy
if that grant is refused. The checks a cluster must pass, and the fix for
each, are on the generated [Prerequisites](prerequisites.md) page.

Minimum cluster size depends on the workload, the pipeline mode and the
scale factor. The table below is the minimum cluster for the default recipe
(`hive-iceberg-spark-trino`), computed without a cluster by
`lakebench.config.sizing.plan_requirements`, the function behind
`lakebench config show`, `lakebench config recommend` and the `run`
capacity preflight. On a real cluster the preflight sizes datagen and Trino
against that cluster first, as `run` does, so its datagen figure can differ
from the default-parallelism column below:

<!-- BEGIN GENERATED: sizing-detail -->
<!-- Generated from the code by `python3.11 scripts/gen_sizing_tables.py`; do not edit by hand. -->

| Workload | Mode | Scale | Minimum CPU | Minimum RAM | Spark peak | Datagen (default parallelism) | Always on | Scratch PVC (if enabled) | Largest pod |
|:---|:---|---:|---:|---:|:---|:---|:---|---:|---:|
| Customer 360 | batch | 1 | 40 cores | 542 GB | 36 cores / 525 GB | 2 pods, 16 cores / 8 GB | 4 cores / 17 GB | 2,400 Gi | 8 cores / 60 GB |
| Customer 360 | batch | 10 | 47 cores | 570 GB | 36 cores / 525 GB | 4 pods, 32 cores / 16 GB | 11 cores / 45 GB | 2,400 Gi | 8 cores / 60 GB |
| Customer 360 | batch | 100 | 113 cores | 1,338 GB | 76 cores / 1,125 GB | 10 pods, 80 cores / 40 GB | 37 cores / 213 GB | 5,400 Gi | 8 cores / 60 GB |
| Customer 360 | continuous | 1 | 58 cores | 307 GB | 38 cores / 282 GB | in always on | 20 cores / 25 GB | 640 Gi | 8 cores / 40 GB |
| Customer 360 | continuous | 10 | 81 cores | 343 GB | 38 cores / 282 GB | in always on | 43 cores / 61 GB | 640 Gi | 8 cores / 40 GB |
| Customer 360 | continuous | 100 | 201 cores | 953 GB | 84 cores / 700 GB | in always on | 117 cores / 253 GB | 1,700 Gi | 8 cores / 48 GB |
| AML | batch | 1 | 40 cores | 542 GB | 36 cores / 525 GB | 2 pods, 16 cores / 14 GB | 4 cores / 17 GB | 2,400 Gi | 8 cores / 60 GB |
| AML | batch | 10 | 47 cores | 570 GB | 36 cores / 525 GB | 4 pods, 32 cores / 28 GB | 11 cores / 45 GB | 2,400 Gi | 8 cores / 60 GB |
| AML | batch | 100 | 113 cores | 1,338 GB | 76 cores / 1,125 GB | 10 pods, 80 cores / 80 GB | 37 cores / 213 GB | 5,500 Gi | 8 cores / 60 GB |
| AML | continuous | 1 | 138 cores | 1,021 GB | 118 cores / 990 GB | in always on | 20 cores / 31 GB | 2,300 Gi | 8 cores / 40 GB |
| AML | continuous | 10 | 161 cores | 1,063 GB | 118 cores / 990 GB | in always on | 43 cores / 73 GB | 2,300 Gi | 8 cores / 40 GB |
| AML | continuous | 100 | 339 cores | 2,251 GB | 222 cores / 1,958 GB | in always on | 117 cores / 293 GB | 4,660 Gi | 8 cores / 48 GB |

<!-- END GENERATED: sizing-detail -->

How to read it:

- **Minimum CPU and RAM** is what must fit at once. In batch mode datagen
  runs first and the Spark jobs after it. The datagen Job is elastic: pods
  the cluster cannot place wait and run as others finish, so only one of its
  pods has to fit and the minimum is the Spark peak plus the always-on pods.
  In continuous mode the three stream jobs run at once, so the minimum is
  their sum plus the always-on pods, and datagen is counted in the always-on
  line while its Job runs.
- **Spark peak** comes from the Spark job profiles
  (`compute_peak_requirements()`). Per-executor sizing is fixed; executor
  counts grow with scale above scale 10, up to 28 per job. In batch,
  `silver-build` sets the peak: 8 executors of 4 cores and 60 GB (48 GB heap
  plus 12 GB overhead) at scale 10 and below.
- **Datagen** is the default `workload.datagen.parallelism`, before cluster
  scaling. `run` caps it to fit the cluster it finds, and above scale 50
  raises it to use about 90% of the CPU left after the always-on pods: the
  AML scale-100 generate in run-20260925-104703-c02890 ran 44 pods at 8
  cores, about 350 cores at once (measured, n=1). When the whole Job does not
  fit at once the preflight passes with a warning that some pods queue.
- **Always on** is the query engine (here Trino, sized by scale tier), the
  Hive Metastore and Postgres (their memory requests, and one core between
  them as the autosizer budgets it). Other recipes change this line;
  `lakebench config show` prints it for your config.
- **Scratch PVC** is the Spark scratch request when
  `platform.storage.scratch` is enabled (AML batch at scale 100 sets it with
  `bronze-verify`, 11 executors of 500 Gi for its CTAS fallback). With
  scratch disabled, the default, no PVC is requested.
- **Largest pod** must fit on one node. A cluster with 512 GB spread across
  sixteen 32 GB nodes has enough memory on paper and still cannot schedule a
  60 GB `silver-build` executor. The 8 cores are a datagen pod.
- Per-job executor overrides (`silver_executors` and the like) are not
  counted in these figures yet; `config show` says so when a config sets one.

AML (`schema: financial`) continuous sizes its three stream jobs from a
measured scale-10 run: bronze-ingest 5 executors x 4 cores so the corpus
drains inside a 30-minute window, silver-stream 10 x 4 so a micro-batch
finishes inside its 60 s trigger, and gold-refresh 12 x 4 so a detection
tick over the whole scale-10 silver finishes inside the 5-minute refresh
interval. The AML scale-10 continuous run run-20260925-180003-bb3df4 ran at
exactly this split. gold-refresh grows with scale to the 28-executor cap
(reached near scale 23); past that a tick outgrows the interval and time to
detect grows with it.

On a smaller cluster a continuous run caps the stream jobs to what fits and
warns naming each capped job. Each AML stage keeps at least the cores the
Customer360 split would give it, and the room above that goes upstream first
(bronze-ingest, then silver-stream up to the count that keeps pace with
bronze, then gold-refresh, then the rest of silver-stream), since a stage
runs no faster than its input arrives. The capacity preflight passes such a
cluster with a WARNING naming the capped stages, as long as the capped
request plus Trino, Hive/Postgres and datagen fits; it fails only when even
that does not fit, or when a single pod fits no node. An explicit
`*_executors` count is not capped and is counted as set.

Lakebench checks this for you. The prerequisite phase of `lakebench run`
compares the minimum against what the cluster can still take: the
allocatable capacity of the schedulable nodes (Ready, not cordoned, no
`NoSchedule` or `NoExecute` taint; an untainted control-plane node counts)
minus what pods in other namespaces already request. It fails immediately
with the specific shortfall, naming the need, the free amount and the
allocatable one, rather than leaving pods `Pending` until the job times
out. It fails closed: when the node or pod list cannot be read (no
permission, an error, a pod on a node the list does not show) the run is
refused with exit 4, "capacity could not be read". With scratch enabled it
also compares the scratch request with the `CSIStorageCapacity` the
StorageClass publishes; when none is published the run goes ahead with a
warning, and the record's `provenance.preflight.scratch` says
`not_measurable`. Free capacity is summed across nodes, so a cluster whose
free cores are spread thin can pass and still leave executors `Pending`;
only the largest pod is checked against one node. `--skip-preflight` skips
the check; the record then says `capacity: skipped` and the verdict carries
"capacity not checked". A batch
`run` counts datagen only when it creates datagen pods: with `--generate`
(and not `--skip-generate`), or in a multi-cycle run. A plain batch `run`
over data from an earlier `lakebench generate` checks the Spark peak and the
always-on pods.

Above scale 50 a continuous run that generates its own corpus is refused,
or admitted only with its streams capped hard (one bronze-ingest executor,
for example), depending on the cluster: the autosizer sizes the datagen Job
to about 90% of the CPU left after the always-on pods, and the preflight
counts it beside the streams. To run the streams at their full size,
generate the corpus first (`lakebench generate`), then start the streams
with `lakebench run --skip-generate` within an hour of generation
finishing: a finished datagen Job is not counted, but Kubernetes deletes it
after 3,600 s and an absent Job is counted as still running.
`lakebench config recommend` prints the largest scale for both ways.

Run `lakebench config recommend lakebench.yaml` after install to find the
largest scale your cluster holds for that config, and
`lakebench config show lakebench.yaml` to see the request at the scale in
your config.

### CLI tools on PATH

| Tool | Minimum version | Used for |
|------|----------------|----------|
| `kubectl` | 1.26+ | All Kubernetes operations |
| `helm` | 3.12+ | Spark Operator install, namespace detection, and Stackable operators (Hive catalog) |

Lakebench runs `kubectl` and `helm`; it does not need `oc`, even on
OpenShift.

### Default StorageClass

Your cluster must have a **default StorageClass** (annotated with
`storageclass.kubernetes.io/is-default-class: "true"`) for PostgreSQL metadata
storage. Most managed Kubernetes distributions include one by default:

- **EKS** -- `gp2` or `gp3`
- **GKE** -- `standard` or `premium-rwo`
- **AKS** -- `managed-premium` or `managed-csi`
- **OpenShift** -- varies by platform (typically `thin-csi` or Portworx)

Self-managed clusters (kubeadm, bare metal) may need a StorageClass created
manually. Alternatively, set `platform.compute.postgres.storage_class`
explicitly in your YAML config to bypass the default.

Trino workers and Spark shuffle use ephemeral storage by default and do **not**
require a StorageClass. Set their `storage_class` fields if you want
PVC-backed storage instead.

Check your cluster's default StorageClass:

```bash
kubectl get storageclass -o wide
```

Look for `(default)` next to one of the class names.

**Scratch StorageClass**: if you enable `platform.storage.scratch` (Portworx-backed shuffle volumes on OpenShift, for example), the named `StorageClass` must exist before `deploy` runs. `deploy` will refuse with an actionable error rather than create it -- a `StorageClass` is shared infrastructure and a create-race between parallel deploys could strip it out from under an in-flight run. A cluster admin installs it once with:

```bash
lakebench admin install-scratch-storage-class
```

### S3-compatible object storage

Lakebench needs an S3-compatible endpoint for the bronze, silver, and gold
data layers. Any of the following will work:

- **Pure Storage FlashBlade** (HTTP, path-style access)
- **MinIO** (self-hosted or operator-managed)
- **AWS S3** (virtual-hosted or path-style)
- Any other S3-compatible store (Ceph RGW, Dell ECS, etc.)

You will need: an endpoint URL, an access key, and a secret key.

### Spark Operator

The **Kubeflow Spark Operator v2.x** (2.5.1 is the current default) must be installed cluster-wide before any `lakebench deploy` runs. Lakebench treats it as shared infrastructure: `deploy` never installs it and fails if it is missing (a cluster admin installs it once with `lakebench admin install-spark-operator`; `platform.compute.spark.operator.install: true` is refused). `deploy` always checks the operator and adds its own namespace to the operator's `spark.jobNamespaces` watch list, under the `lakebench-cluster-lock` lease, and `destroy` removes it again. Never edit that list by hand with `helm upgrade --reuse-values`: it skips the lease and can drop another deployment's entry.

The supported installation path is:

```bash
lakebench admin install-spark-operator
```

`admin install-spark-operator` takes the cluster-wide `lakebench-cluster-lock` lease before running `helm upgrade`, so concurrent `admin` invocations from different workstations cannot race each other. Confirm the install with `lakebench admin doctor`.

Falling back to raw Helm still works:

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install spark-operator spark-operator/spark-operator \
  --version 2.5.1 \
  --namespace spark-operator \
  --create-namespace \
  --set spark.jobNamespaces="" \
  --set webhook.enable=true
```

but no lock is taken, so two parallel Helm installs may still stomp each other. Prefer the `admin` command on any cluster used by more than one engineer.

### Catalog operator (depends on your recipe)

Which catalog operators you need depends on your recipe choice:

| Recipe prefix | Catalog | Operators required |
|---|---|---|
| `hive-*` | Hive Metastore | Stackable commons, secret, listener, and hive operators |
| `polaris-*` | Apache Polaris | **None** -- Lakebench deploys Polaris directly |

For **Hive** recipes (the default), the Stackable operators are required.
They are shared cluster infrastructure: a cluster admin installs them once,
and `lakebench deploy` never does
(`architecture.catalog.hive.operator.install: true` is refused):

```bash
helm install commons-operator oci://oci.stackable.tech/sdp-charts/commons-operator \
  --version 25.7.0 --namespace stackable --create-namespace
helm install secret-operator oci://oci.stackable.tech/sdp-charts/secret-operator \
  --version 25.7.0 --namespace stackable
helm install listener-operator oci://oci.stackable.tech/sdp-charts/listener-operator \
  --version 25.7.0 --namespace stackable
helm install hive-operator oci://oci.stackable.tech/sdp-charts/hive-operator \
  --version 25.7.0 --namespace stackable
```

For **Polaris** recipes, skip the Stackable install entirely. See the
[Polaris Quick Start](quickstart-polaris.md) for details.

---

## Installation

### pip (recommended)

```bash
pip install lakebench-k8s
```

Or use [pipx](https://pipx.pypa.io/) for an isolated install:

```bash
pipx install lakebench-k8s
```

### Pre-built binary

Single-file binaries are available for Linux (amd64) and macOS (amd64, arm64).
No Python required.

```bash
# Auto-detect OS and architecture
curl -fsSL https://raw.githubusercontent.com/PureStorage-OpenConnect/lakebench-k8s/main/install.sh | bash
```

The script checks the download against the release's `SHA256SUMS` file
and runs the downloaded binary's `version` before it installs it into
`INSTALL_DIR` (default `/usr/local/bin`; run it with `sudo bash` or set
`INSTALL_DIR` to a directory you can write). It installs nothing, and keeps
any lakebench already there, if the download fails, the checksum does not
match or the binary does not run. That catches a corrupted or incomplete
download; the checksum file comes from the same release, so it does not
prove who built the binary. `SHA256SUMS` is published from 1.7.0 on: an
older release, which is what `latest` resolves to until 1.7.0 is out,
installs unverified, and the script says so on stderr. A release at 1.7.0
or later without `SHA256SUMS` is refused.
There is no Linux arm64 binary; on that platform install from PyPI.

Set `INSTALL_DIR` or `VERSION` on the `bash` side of the pipe, and pass them
through `sudo` with `env`:

```bash
curl -fsSL https://raw.githubusercontent.com/PureStorage-OpenConnect/lakebench-k8s/main/install.sh | INSTALL_DIR="$HOME/.local/bin" bash
curl -fsSL https://raw.githubusercontent.com/PureStorage-OpenConnect/lakebench-k8s/main/install.sh | sudo env VERSION=1.7.0 bash
```

Or download manually from
[GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases):

```bash
# Linux
curl -LO https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases/latest/download/lakebench-linux-amd64

# macOS (Apple Silicon)
curl -LO https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases/latest/download/lakebench-macos-arm64

sudo install -m 755 lakebench-* /usr/local/bin/lakebench
```

### From source

```bash
git clone https://github.com/PureStorage-OpenConnect/lakebench-k8s.git
cd lakebench-k8s
pip install -e ".[dev]"
```

### Verify

```bash
lakebench version
```

---

## First Deployment Walkthrough

This section walks through a complete deploy-generate-run cycle at **scale 1**
(approximately 10 GB of generated data). Scale 1 is small enough to finish in
minutes on most clusters while still exercising every stage of the pipeline.

### First-day workflow at a glance

On a fresh cluster the shortest path is four commands. The two flags on
`run` are load-bearing: `--yes` lets `run` deploy the namespace and
components when they do not exist yet (without it `run` refuses and asks
you to run `lakebench deploy` first), and `--generate` populates the
bronze bucket before the pipeline (without it, `run` executes against an
empty bronze).

```bash
lakebench init                                  # writes lakebench.yaml
lakebench run lakebench.yaml --generate --yes   # deploy + generate + pipeline + benchmark
lakebench results lakebench.yaml                # print the scorecard
lakebench destroy lakebench.yaml --yes          # tear down what this deployment owns
```

The step-by-step below walks the same path with the intermediate checks
(`config validate`, `status`) for a first-time cluster.

### 1. Generate a configuration file

```bash
lakebench init --name my-first-lakehouse --scale 1
```

This creates `lakebench.yaml` in the current directory with sensible defaults.

### 2. Edit the configuration

Open `lakebench.yaml` and fill in your S3 connection details:

```yaml
name: my-first-lakehouse

# Optional: use a quick-recipe for one-line setup (sets catalog + format + engine)
# recipe: hive-iceberg-spark-trino

platform:
  storage:
    s3:
      endpoint: http://your-s3-endpoint:80   # your S3 endpoint URL
      access_key: YOUR_ACCESS_KEY            # your S3 access key
      secret_key: YOUR_SECRET_KEY            # your S3 secret key

workload:
  datagen:
    scale: 1                               # ~10 GB bronze data
```

If your storage uses virtual-hosted bucket addressing (like AWS S3), set
`path_style: false`. For MinIO and FlashBlade, leave it as `true` (the
default).

### 3. Validate the configuration

```bash
lakebench config validate lakebench.yaml
```

This checks YAML syntax, Pydantic schema validation, Kubernetes connectivity,
and S3 reachability. Fix any errors before continuing. Before the first
deploy, the Spark Operator section reports that the namespace is not yet
watched; that is expected, because `deploy` adds it.

### 4. Run everything (single command)

```bash
lakebench run lakebench.yaml --generate --yes
```

This deploys infrastructure (if not already deployed), generates test data,
runs the pipeline, and benchmarks -- all in one command. The `--yes` flag
skips confirmation prompts and enables auto-deploy.

Alternatively, run each step separately for more control:

```bash
lakebench deploy lakebench.yaml --yes     # deploy infrastructure
lakebench status lakebench.yaml           # verify deployment
lakebench generate lakebench.yaml  # generate test data (~5 min at scale 1)
lakebench run lakebench.yaml              # run pipeline + benchmark
```

### 5. What happens during `run`

```bash
lakebench run lakebench.yaml
```

This executes three Spark jobs in sequence, followed by a query benchmark:

1. **bronze-verify** -- validates and deduplicates raw Parquet data
2. **silver-build** -- enriches, normalizes, and writes an Iceberg table
3. **gold-finalize** -- aggregates into a business-ready executive dashboard
4. **benchmark** -- runs 8 analytical queries (12 for AML) against the silver and gold
   tables via the active query engine (Trino by default)

Each job's progress, duration, and throughput are recorded to metrics.

### 6. View the report

```bash
lakebench report
```

Every run writes an HTML report to `lakebench-output/runs/run-<id>/report.html`
containing job performance tables, query latencies, throughput metrics, and a
configuration snapshot. `lakebench report` prints the latest run's summary and
the path to that file; open it in your browser to review the results.
`lakebench report --render` writes a fresh copy under
`lakebench-output/reports/` without touching the original.

To list all recorded runs:

```bash
lakebench report --list
```

`lakebench results lakebench.yaml` prints the same scorecard in the terminal
(`--format json` or `csv` for scripts).

### 7. Compare two configurations

```bash
lakebench deploy lakebench-polaris.yaml --yes
lakebench generate lakebench-polaris.yaml
lakebench compare lakebench.yaml lakebench-polaris.yaml
```

`compare` runs each config through the pipeline and benchmark in turn and
prints the two results side by side. It destroys each deployment after its
run unless you pass `--keep`, and it does not deploy a missing stack, so run
`lakebench deploy` on both configs first. Both configs need bronze data:
`lakebench.yaml` already has it from step 4, so generate only for the second
one. Do not pass `--generate` here: generating into a bronze prefix that
already holds data is refused, and that side of the comparison fails. Give the two configs different
names and bucket names: the first deployment's destroy empties its buckets
before the second one runs.

### 8. Tear down

When you are done, destroy all resources:

```bash
lakebench destroy lakebench.yaml
```

You will be prompted to confirm. Add `--force` to skip the confirmation prompt.

The destroy sequence runs in this order: kill running Spark jobs, clean up
datagen pods, remove the tables from the catalog without deleting files (no
snapshot expiry, orphan removal, or VACUUM runs first), empty the S3 buckets and delete the ones lakebench created, tear down
infrastructure in reverse deploy order, and finally delete the namespace and
wait for it to be gone. If a bucket lakebench created cannot be deleted, the
namespace is kept so a re-run can finish. Buckets that existed before deploy
are emptied but never deleted. On a backend without bucket tagging
(FlashBlade) the name is the only other evidence of ownership, so destroy
empties a pre-existing bucket only if deploy found it empty and recorded
that; one that already held data is left alone and reported, and
`--force-legacy` empties it.

---

## If Something Goes Wrong

**Deploy fails partway through:** Safe to re-run. `lakebench deploy` is
idempotent -- components that already exist are skipped.

**Generate fails or times out:** Increase the timeout with `--timeout 14400`
(4 hours). The Rust generator has no checkpoint-resume, so a re-run starts
from the beginning, and because the failed run left partial data in bronze,
the re-run needs `--regenerate` (`lakebench generate lakebench.yaml
--regenerate`), which empties the bronze bucket first. Without it `generate`
exits 3 (refused) and names the non-empty prefix.

**A pipeline stage fails:** Re-run just that stage:

```bash
lakebench run lakebench.yaml --stage silver-build
```

**Benchmark fails but pipeline succeeded:** Trino might not be healthy. Check
with `lakebench status`, then re-run the benchmark alone:

```bash
lakebench benchmark lakebench.yaml
```

**Start over completely:** Destroy and redeploy:

```bash
lakebench destroy lakebench.yaml --force
lakebench deploy lakebench.yaml
```

For more detailed diagnosis, see the [Troubleshooting Guide](troubleshooting.md).

---

## What Just Happened

The workflow above deployed and ran a complete **medallion architecture**
pipeline:

```
Raw Parquet (S3)
  |
  v
Bronze: Validate, deduplicate, enforce schema
  |
  v
Silver: Normalize emails/phones, geo-enrich, segment customers, flag quality issues
  |                    (written as an Iceberg table)
  v
Gold: Aggregate into an executive dashboard with daily KPIs, channel performance,
      customer lifetime value
  |                    (written as an Iceberg table)
  v
Benchmark: 8 queries against the silver and gold tables (RFM segmentation,
           revenue moving averages, cohort retention, channel attribution, CLV, etc.)
```

The bronze layer lives as raw Parquet files in S3. Silver and gold are Apache
Iceberg tables registered in the catalog (Hive Metastore or Polaris). The
query engine (Trino, Spark Thrift, or DuckDB) queries the silver and gold
tables through its Iceberg connector.

All processing is done by Apache Spark running on Kubernetes via the Spark
Operator. Executor count scales automatically with the data volume (controlled
by the scale factor), while per-executor sizing stays fixed at proven resource
profiles.

---

## Useful Commands

Once infrastructure is deployed, these commands help you inspect and debug
the running environment.

### Check deployment status

```bash
lakebench status lakebench.yaml
```

Shows the state of every deployed component: namespace, PostgreSQL, Hive or
Polaris, Trino coordinator and workers, Spark service account, and S3 buckets.

### Review configuration

```bash
lakebench config show lakebench.yaml
```

Prints the resolved configuration with the source of each value (recipe,
config file or default), and the peak CPU, memory and scratch the pipeline
requests at the configured scale.

### Stream component logs

```bash
lakebench logs trino lakebench.yaml            # Trino coordinator
lakebench logs hive lakebench.yaml             # Hive Metastore
lakebench logs postgres lakebench.yaml         # PostgreSQL
lakebench logs spark-driver lakebench.yaml     # Latest Spark driver pod
lakebench logs trino lakebench.yaml --follow   # Tail in real time
```

### Run ad-hoc queries

After the pipeline finishes, you can query the gold table interactively:

```bash
# Built-in example queries
lakebench query lakebench.yaml --example revenue      # Revenue with 7-day moving average
lakebench query lakebench.yaml --example channels     # Channel revenue breakdown
lakebench query lakebench.yaml --example engagement   # Engagement and churn risk
lakebench query lakebench.yaml --example clv          # Customer lifetime value

# Custom SQL
lakebench query lakebench.yaml --sql "SELECT count(*) FROM lakehouse.gold.customer_executive_dashboard"
```

---

## Local Mode (no cluster)

For a laptop try-out without Kubernetes, `lakebench init --local` writes a
podman/docker config that runs against a Garage container on `localhost`:

```bash
lakebench init --local --scale 1
lakebench deploy lakebench.yaml --local
lakebench run lakebench.yaml --local --generate --yes
lakebench destroy lakebench.yaml --local
```

`--generate` populates bronze on the first local run. Subsequent runs
against the same `--workdir` reuse the existing bronze corpus, so
`--generate` is only needed again after `destroy --remove-data`, after
`clean bronze`, or when the scale factor changes. Without `--generate`
the pipeline runs against whatever bronze the workdir already holds; an
empty workdir gives an empty pipeline.

Local mode is Iceberg-only and Customer 360 batch only. AML configs and
continuous mode are refused with an actionable error; see
[Compatibility Matrix](compatibility-matrix.md).

## Scaling Up

To run at a larger scale, change the `scale` value in your config:

| Scale | Approximate bronze size | Customers | Rows |
|------:|:-----------------------:|----------:|-----:|
| 1 | 10 GB | 100K | 2.4M |
| 10 | 100 GB | 1M | 24M |
| 100 | 1 TB | 10M | 240M |

Datagen scale is banded per workload. Customer 360 is supported up to scale
300 and unverified up to 600; AML (financial) is supported up to 300 and
unverified up to 800. Above the ceiling `deploy` and `generate` refuse the
config, because a datagen pod would exceed the 16 GiB per-pod memory cap
(a Lakebench-imposed cap); in the unverified range they warn. The run's
support state records the band.

Use `lakebench config recommend lakebench.yaml` to get cluster-aware sizing
guidance before scaling up. At scale 100+ you will want to increase the `--timeout` on both generate
and run commands:

```bash
lakebench generate lakebench.yaml --timeout 14400
lakebench run lakebench.yaml --timeout 7200
```

---

## Component Versions

Lakebench ships with these default component versions. All are configurable
in the `images` section of your YAML.

| Component | Default version | Image |
|-----------|----------------|-------|
| Apache Spark | 3.5.x / 4.0.x / 4.1.x | `apache/spark:4.0.2-python3` (default), `4.1.1-python3`, or `3.5.4-python3` |
| Spark Operator | 2.5.1 | Kubeflow Helm chart |
| Apache Iceberg | 1.11.0 (1.10.1 on `3.5.4-python3`) | Spark runtime JAR |
| Hive Metastore | 3.1.3 | Stackable Hive Operator 25.7.0 |
| Apache Polaris | 1.6.0 | `apache/polaris:1.6.0` |
| Trino | 483 | `trinodb/trino:483` |
| PostgreSQL | 17 | `postgres:17` |

Iceberg 1.11.0 needs Java 17, and the `3.5.4-python3` image ships Java 11.
With that image and no explicit `table_format.iceberg.version`, config load
logs a warning and uses Iceberg 1.10.1. An explicit 1.11.0 on a Java 11 image
is refused; use a `java17` Spark 3.5 image tag to run 1.11.0 on Spark 3.5.

---

## Choosing a Recipe

The default deployment uses Hive + Iceberg + Trino. Lakebench supports 11
component combinations ("recipes"); the eight Iceberg ones are below. Use the `recipe:` field for
quick setup, or set architecture fields individually:

```yaml
recipe: polaris-iceberg-spark-trino   # one-line setup
```

| Recipe | `recipe:` value | Use case |
|--------|----------------|----------|
| Standard (default) | `hive-iceberg-spark-trino` | Ad-hoc SQL analytics via Trino |
| Spark SQL | `hive-iceberg-spark-thrift` | Spark-native analytics |
| DuckDB | `hive-iceberg-spark-duckdb` | Lightweight single-pod analytics |
| Headless | `hive-iceberg-spark-none` | ETL-only, no query engine |
| Polaris | `polaris-iceberg-spark-trino` | REST catalog API, OAuth2 access control |
| Polaris + Spark SQL | `polaris-iceberg-spark-thrift` | REST catalog with Spark-native analytics |
| Polaris + DuckDB | `polaris-iceberg-spark-duckdb` | REST catalog with lightweight engine |
| Polaris headless | `polaris-iceberg-spark-none` | REST catalog, ETL-only |

See the [Recipes Guide](recipes.md) for all 11 combinations, decision guidance,
and detailed YAML snippets.

Use `lakebench config recommend lakebench.yaml` to get cluster-aware sizing
guidance before choosing a scale factor. To see what a given scale requests,
set it in the config and run `lakebench config show lakebench.yaml`.

---

## Choosing a Workload

Lakebench ships two workload schemas. The recipe (catalog + format + engine)
is orthogonal to the workload -- switch schemas by changing one field.

### Customer 360 (default)

Retail customer-interactions from ~8 channels through a bronze / silver /
gold medallion into an executive dashboard, then the 8-query analytical
benchmark. This is the workload the [First Deployment Walkthrough](#first-deployment-walkthrough)
above runs.

```yaml
workload:
  schema: customer360   # default; can be omitted
```

### Financial crime (AML)

pacs.008 wire-message pipeline with nine detection rules (W1-W8 and
W17) scoring against planted AML typologies. `lakebench run` scores
per-rule recall and precision inline after gold-finalize by joining
`gold.alerts` against the datagen manifest on transaction UETRs
(`spark/scripts/score_financial.py`); it does not run the reference
detector, so the run itself does not cross-check recall against a
model. Pattern-span (a datagen window-width property, not detection
latency) is reported for context. A separate command,
`lakebench financial reference-score`, submits the pre-registered
fidelity gate over silver and writes a per-typology reference-model
report; that job is invoked on its own, and the gate report says
whether the corpus was on the calibration seed or a held-out one.

```yaml
workload:
  schema: financial
  retention_workload: true    # keeps snapshots for replay / reproduce
  retention_months: 60
```

A worked example ships at [`examples/polaris-iceberg-spark-financial.yaml`](../examples/polaris-iceberg-spark-financial.yaml).
The full loop is `deploy -> generate -> run`; `financial score` is an
optional re-score after a rule change or replay:

```bash
lakebench deploy   examples/polaris-iceberg-spark-financial.yaml
lakebench generate examples/polaris-iceberg-spark-financial.yaml
lakebench run      examples/polaris-iceberg-spark-financial.yaml

# Optional re-score. Path prefix is pacs008/ under the default template.
lakebench financial score \
    examples/polaris-iceberg-spark-financial.yaml \
    --manifest s3a://<bronze-bucket>/pacs008/manifest/manifest.parquet \
    --output   s3a://<gold-bucket>/scoring/rescore/recall.parquet
```

The manifest path only labels cycle 0. On a multi-cycle corpus
(`architecture.pipeline.cycles > 1`) later cycles land at
`pacs008/manifest/manifest-cNNN.parquet`, and this command scores
only the manifest URI you pass it.

Two additional operator subcommands cover the retention scenarios:

- `lakebench financial replay CONFIG --rule W2_structuring --depth-months 60`
  reruns one rule against a historical Iceberg snapshot.
- `lakebench financial reproduce CONFIG --alert-id <id>` reproduces a specific
  past alert via Iceberg time-travel.

`CONFIG` in both cases is the same YAML you passed to `deploy`.

The AML precision numbers on this benchmark are not a claim about a
production ops-queue false-positive rate -- the datagen has one baseline
distribution and roughly a dozen planted typology shapes, and the rules
were tuned against it. Use them for stack comparison and regression
detection.

`workload.datagen.seed = 43` is the calibration corpus the generator,
the rule thresholds and the reference features were tuned against, and
it is the default when `seed` is unset for `financial`. Recall and
precision from a seed-43 run are in-sample. v1.6 publishes no held-out
result, so AML recall in v1.6 is uncalibrated; the held-out looks are
deferred to v1.7. Numbers meant for comparison with other stacks should
cite the seed and, when it is 43, say so.

See [AML Scoring](aml-scoring.md) for the full explanation
of what the metrics measure, the band leakage report, the reference
detector, and the current untargeted typologies.

---

## Next Steps

- [Recipes Guide](recipes.md) -- all supported component combinations
- [Polaris Quick Start](quickstart-polaris.md) -- use Apache Polaris instead of Hive
- [AML Scoring](aml-scoring.md) -- financial-crime / AML workload: what precision and recall measure here, the leakage gate, and the reference detector
- [Configuration Reference](configuration.md) -- full YAML schema with all options
- [Operators and Catalogs](operators-and-catalogs.md) -- tested versions and troubleshooting
