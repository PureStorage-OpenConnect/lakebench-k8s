# Getting Started

This guide walks you through installing Lakebench, deploying your first
lakehouse stack on Kubernetes, running the medallion pipeline, and viewing
benchmark results. By the end you will have a working bronze-to-gold
pipeline with Spark, Iceberg, and Trino -- all from a single YAML file.

---

## Prerequisites

Before you begin, make sure your environment has the following.

### Kubernetes cluster

Any cluster running Kubernetes 1.26+ will work. Lakebench is tested on:

- **OpenShift 4.x** (bare metal and vSphere)
- **Vanilla Kubernetes** (kubeadm, EKS, GKE, AKS)

You need admin-level access to the target namespace (or permission to create
one). On OpenShift, Lakebench automatically handles Security Context
Constraints for the Spark service account.

Minimum cluster size depends on the scale factor. As a rough guide:

| Scale | Minimum CPU | Minimum RAM | Bronze data |
|------:|------------:|------------:|------------:|
| 1 | 8 cores | 32 GB | ~10 GB |
| 10 | 16 cores | 64 GB | ~100 GB |
| 100 | 64 cores | 256 GB | ~1 TB |

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

### S3-compatible object storage

Lakebench needs an S3-compatible endpoint for the bronze, silver, and gold
data layers. Any of the following will work:

- **Pure Storage FlashBlade** (HTTP, path-style access)
- **MinIO** (self-hosted or operator-managed)
- **AWS S3** (virtual-hosted or path-style)
- Any other S3-compatible store (Ceph RGW, Dell ECS, etc.)

You will need: an endpoint URL, an access key, and a secret key.

### Spark Operator

The **Kubeflow Spark Operator 2.4.0+** must be installed cluster-wide.
Lakebench assumes the operator is pre-installed (the default is
`platform.compute.spark.operator.install: false`). Set it to `true` if you
want `lakebench deploy` to install it automatically via Helm.

Install manually with Helm if you prefer:

```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set spark.jobNamespaces="" \
  --set webhook.enable=true
```

### Catalog operator (depends on your recipe)

Which catalog operators you need depends on your recipe choice:

| Recipe prefix | Catalog | Operators required |
|---|---|---|
| `hive-*` | Hive Metastore | Stackable commons, secret, listener, and hive operators |
| `polaris-*` | Apache Polaris | **None** -- Lakebench deploys Polaris directly |

For **Hive** recipes (the default), install the Stackable operators:

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
lakebench validate lakebench.yaml
```

This checks YAML syntax, Pydantic schema validation, Kubernetes connectivity,
and S3 reachability. Fix any errors before continuing.

### 4. Deploy infrastructure

```bash
lakebench deploy lakebench.yaml
```

You will be asked to confirm before deploying. Add `--yes` to skip the prompt.

Lakebench creates (in order): the Kubernetes namespace, S3 secrets, scratch
StorageClass, PostgreSQL (metadata backend), Hive Metastore or Polaris
(depending on your catalog choice), the query engine, and the Spark RBAC
service account. Wait for the command to finish -- it reports the status of
each component as it deploys.

Check that everything is healthy:

```bash
lakebench status lakebench.yaml
```

### 5. Generate test data

```bash
lakebench generate lakebench.yaml --wait
```

You will be asked to confirm before generating. Add `--yes` to skip the prompt.

This launches Kubernetes Jobs that write synthetic Customer360 Parquet files
into the bronze S3 bucket. The `--wait` flag blocks until generation is
complete. At scale 1 this typically finishes in under 5 minutes.

### 6. Run the pipeline

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

The destroy sequence runs in the correct order: kill running Spark jobs, clean
up datagen pods, run Iceberg maintenance (expire snapshots, remove orphan
files), drop tables, empty S3 buckets, tear down infrastructure in reverse
deploy order, and finally delete the namespace.

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
| Apache Spark | 3.5.4 | `apache/spark:3.5.4-python3` |
| Spark Operator | 2.4.0 | Kubeflow Helm chart |
| Apache Iceberg | 1.10.1 | Spark runtime JAR |
| Hive Metastore | 3.1.3 | Stackable Hive Operator 25.7.0 |
| Apache Polaris | 1.3.0-incubating | `apache/polaris:1.3.0-incubating` |
| Trino | 479 | `trinodb/trino:479` |
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

See the [Recipes Guide](recipes.md) for all 8 combinations, decision guidance,
and detailed YAML snippets.

Use `lakebench recommend` to get cluster-aware sizing guidance before choosing
a scale factor:

```bash
lakebench recommend --scale 100
```

---

## Next Steps

- [Recipes Guide](recipes.md) -- all supported component combinations
- [Polaris Quick Start](quickstart-polaris.md) -- use Apache Polaris instead of Hive
- [Configuration Reference](configuration.md) -- full YAML schema with all options
- [Operators and Catalogs](operators-and-catalogs.md) -- tested versions and troubleshooting
