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
one). On OpenShift, Lakebench automatically handles Security Context
Constraints for the Spark service account.

Minimum cluster size depends on the scale factor. These figures are the peak
resources Lakebench actually requests from Kubernetes, derived from the Spark
job profiles:

| Scale | Bronze data | Minimum CPU | Minimum RAM | Scratch PVC |
|------:|------------:|------------:|------------:|------------:|
| 1 | ~10 GB | 36 cores | 512 GB | 2,400 Gi |
| 10 | ~100 GB | 36 cores | 512 GB | 2,400 Gi |
| 50 | ~500 GB | 52 cores | 752 GB | 3,600 Gi |
| 100 | ~1 TB | 76 cores | 1,112 GB | 5,400 Gi |

Three things surprise people about this table:

- **Scale 1 and scale 10 request the same resources.** Executor counts are
  fixed for every scale at or below 10, so the smallest run is no cheaper
  than a 100 GB run. Below scale 10 you are choosing how much data to
  process, not how much cluster to use.
- **The `silver-build` job sets the peak.** It requests 8 executors at 4
  cores and 60 GB each (48 GB heap plus 12 GB overhead). The medallion jobs
  run sequentially, so the cluster only needs to satisfy the largest one,
  not the sum of all three.
- **Individual pods must fit on a single node.** A `silver-build` executor
  needs 60 GB on one node. A cluster with 512 GB spread across sixteen 32 GB
  nodes has enough total memory on paper and still cannot schedule the job.

The table above is the Customer360 workload. AML (`schema: financial`) batch
requests come from the same function, `compute_peak_requirements(scale,
"batch", "financial")`. They match Customer360 except for scratch at scale
100, where the AML bronze-verify job (11 executors with 500 Gi PVCs, for its
CTAS fallback) sets the scratch peak:

| Workload | Scale | Minimum CPU | Minimum RAM | Scratch PVC |
|:---------|------:|------------:|------------:|------------:|
| AML batch | 1-10 | 36 cores | 512 GB | 2,400 Gi |
| AML batch | 50 | 52 cores | 752 GB | 3,600 Gi |
| AML batch | 100 | 76 cores | 1,112 GB | 5,500 Gi |

These are the Spark pipeline's requests. Data generation runs before the
pipeline and can be the larger demand: the AML scale-100 generate in
run-20260925-104703-c02890 ran 44 datagen pods at 8 cores, about 350 cores
at once (measured, not derived from `compute_peak_requirements`). The pod
count is `architecture.workload.datagen.parallelism`; set it lower on a
smaller cluster and generation takes longer.

Continuous mode runs its three streaming jobs at the same time, so the
minimum is their sum, and it differs by workload. AML (`schema: financial`)
sizes all three from a measured scale-10 run: bronze-ingest 5 executors x 4
cores so the corpus drains inside a 30-minute window, silver-stream 10 x 4 so
a micro-batch finishes inside its 60 s trigger, and gold-refresh 12 x 4 so a
detection tick over the whole scale-10 silver finishes inside the 5-minute
refresh interval. gold-refresh grows with scale to the 28-executor cap
(reached near scale 23); past that a tick outgrows the interval and time to
detect grows with it:

| Workload | Scale | Minimum CPU | Minimum RAM | Scratch PVC |
|:---------|------:|------------:|------------:|------------:|
| Customer360 | 1-10 | 38 cores | 272 GB | 640 Gi |
| AML | 1-10 | 118 cores | 980 GB | 2,300 Gi |
| Customer360 | 50 | 56 cores | 438 GB | 1,060 Gi |
| AML | 50 | 198 cores | 1,756 GB | 4,220 Gi |
| Customer360 | 100 | 84 cores | 690 GB | 1,700 Gi |
| AML | 100 | 222 cores | 1,948 GB | 4,660 Gi |

The AML scale-10 continuous run run-20260925-180003-bb3df4 ran at exactly
this split (bronze-ingest 5, silver-stream 10, gold-refresh 12 executors at 4
cores each).

On a smaller cluster the run caps the streaming jobs to what fits and warns
naming each capped job. Each AML stage keeps at least the cores the
Customer360 split would give it, and the room above that goes upstream first
(bronze-ingest, then silver-stream up to the count that keeps pace with
bronze, then gold-refresh, then the rest of silver-stream), since a stage
runs no faster than its input arrives. The capacity preflight passes such a
cluster with a WARNING naming the capped stages, as long as the capped
request plus Trino, Hive/Postgres and datagen fits (AML scale 1-10: 57
cores); it fails only when even that does not fit, or when a single pod fits
no node. An explicit `*_executors` count is not capped and is counted as
set.

Lakebench checks this for you. The prerequisite phase of `lakebench run`
compares the peak request against your cluster's allocatable capacity and
fails immediately with the specific shortfall, rather than leaving pods
`Pending` until the job times out. Skipped when you pass `--skip-preflight`.

Run `lakebench recommend` after install to check your cluster's maximum
supported scale.

### CLI tools on PATH

| Tool | Minimum version | Used for |
|------|----------------|----------|
| `kubectl` | 1.26+ | All Kubernetes operations |
| `helm` | 3.12+ | Spark Operator install, namespace detection, and Stackable operators (Hive catalog) |

If you are on OpenShift, `oc` works as a drop-in replacement for `kubectl`.

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

The **Kubeflow Spark Operator v2.x** (2.5.1 is the current default) must be installed cluster-wide before any `lakebench deploy` runs. Lakebench treats it as shared infrastructure; a developer's `deploy` will not install or upgrade it.

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
You have two options:

**Option A: Auto-install** (add to your config YAML):

```yaml
architecture:
  catalog:
    hive:
      operator:
        install: true    # lakebench deploy will install Stackable operators
```

This installs all four Stackable operators (commons, listener, secret, hive)
via Helm during `lakebench deploy`. Requires cluster-admin.

**Option B: Manual install:**

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

architecture:
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
and S3 reachability. Fix any errors before continuing.

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
lakebench generate lakebench.yaml --wait  # generate test data (~5 min at scale 1)
lakebench run lakebench.yaml --skip-preflight  # run pipeline + benchmark
```

### What happens during `run`

```bash
lakebench run lakebench.yaml
```

This executes three Spark jobs in sequence, followed by a query benchmark:

1. **bronze-verify** -- validates and deduplicates raw Parquet data
2. **silver-build** -- enriches, normalizes, and writes an Iceberg table
3. **gold-finalize** -- aggregates into a business-ready executive dashboard
4. **benchmark** -- runs 8 analytical queries against the gold table via the
   active query engine (Trino by default)

Each job's progress, duration, and throughput are recorded to metrics.

### 7. Generate a report

```bash
lakebench report
```

This produces an HTML report in `lakebench-output/runs/<run-id>/report.html`
containing job performance tables, query latencies, throughput metrics, and a
configuration snapshot. Open it in your browser to review the results.

To list all recorded runs:

```bash
lakebench report --list
```

### 8. Tear down

When you are done, destroy all resources:

```bash
lakebench destroy lakebench.yaml
```

You will be prompted to confirm. Add `--force` to skip the confirmation prompt.

The destroy sequence runs in this order: kill running Spark jobs, clean up
datagen pods, drop tables (no snapshot expiry, orphan removal, or VACUUM runs
first), empty the S3 buckets and delete the ones lakebench created, tear down
infrastructure in reverse deploy order, and finally delete the namespace and
wait for it to be gone. If a bucket lakebench created cannot be deleted, the
namespace is kept so a re-run can finish.

---

## If Something Goes Wrong

**Deploy fails partway through:** Safe to re-run. `lakebench deploy` is
idempotent -- components that already exist are skipped.

**Generate fails or times out:** Increase the timeout with `--timeout 14400`
(4 hours). Datagen supports resume -- re-running picks up where it left off.

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
Benchmark: 8 queries against the gold table via query engine (RFM segmentation,
           revenue moving averages, cohort retention, channel attribution, CLV, etc.)
```

The bronze layer lives as raw Parquet files in S3. Silver and gold are Apache
Iceberg tables registered in the catalog (Hive Metastore or Polaris). The
query engine (Trino, Spark Thrift, or DuckDB) queries the gold table through
its Iceberg connector.

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
lakebench info lakebench.yaml
```

Prints the resolved configuration including computed values: which recipe
(catalog + table format + query engine) is active, how many executors each
Spark job will use at the current scale, and the component versions.

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

## Scaling Up

To run at a larger scale, change the `scale` value in your config:

| Scale | Approximate bronze size | Customers | Rows |
|------:|:-----------------------:|----------:|-----:|
| 1 | 10 GB | 100K | 2.4M |
| 10 | 100 GB | 1M | 24M |
| 100 | 1 TB | 10M | 240M |
| 1,000 | 10 TB | 100M | 2.4B |

Use `lakebench recommend` to get cluster-aware sizing guidance before scaling
up. At scale 100+ you will want to increase the `--timeout` on both generate
and run commands:

```bash
lakebench generate lakebench.yaml --wait --timeout 14400
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
| Apache Iceberg | 1.11.0 | Spark runtime JAR |
| Hive Metastore | 3.1.3 | Stackable Hive Operator 25.7.0 |
| Apache Polaris | 1.6.0 | `apache/polaris:1.6.0` |
| Trino | 483 | `trinodb/trino:483` |
| PostgreSQL | 17 | `postgres:17` |

---

## Choosing a Recipe

The default deployment uses Hive + Iceberg + Trino. Lakebench supports 8
validated component combinations ("recipes"). Use the `recipe:` field for
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

Use `lakebench recommend` to get cluster-aware sizing guidance before choosing
a scale factor:

```bash
lakebench recommend --scale 100
```

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
architecture:
  workload:
    schema: customer360   # default; can be omitted
```

### Financial crime (AML)

pacs.008 wire-message pipeline with six W-rule detectors scoring against
planted AML typologies. The scorecard reports per-rule recall, precision,
and pattern-span (a datagen window-width property, not detection latency),
joined against a scikit-learn reference detector so a
rule cannot read high recall from a label proxy without being caught.

```yaml
architecture:
  workload:
    schema: financial
    retention_workload: true    # keeps snapshots for replay / reproduce
    retention_months: 60
```

A worked example ships at [`examples/polaris-iceberg-spark-financial.yaml`](../examples/polaris-iceberg-spark-financial.yaml).
The full loop is `deploy -> generate -> run -> financial score`:

```bash
lakebench deploy   examples/polaris-iceberg-spark-financial.yaml
lakebench generate examples/polaris-iceberg-spark-financial.yaml --wait
lakebench run      examples/polaris-iceberg-spark-financial.yaml
lakebench financial score \
    examples/polaris-iceberg-spark-financial.yaml \
    --manifest s3://<bronze-bucket>/manifest/manifest.parquet \
    --output   s3://<bronze-bucket>/scores/recall.parquet
```

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
detection. See [AML Scoring](aml-scoring.md) for the full explanation
of what the metrics measure, the leakage gate, the reference detector,
and the current untargeted typologies.

---

## Next Steps

- [Recipes Guide](recipes.md) -- all supported component combinations
- [Polaris Quick Start](quickstart-polaris.md) -- use Apache Polaris instead of Hive
- [AML Scoring](aml-scoring.md) -- financial-crime / AML workload: what precision and recall measure here, the leakage gate, and the reference detector
- [Configuration Reference](configuration.md) -- full YAML schema with all options
- [Operators and Catalogs](operators-and-catalogs.md) -- tested versions and troubleshooting
