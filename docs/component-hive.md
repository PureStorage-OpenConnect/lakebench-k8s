# Component Reference: Hive Metastore

## Overview

Hive Metastore (HMS) provides the metadata catalog for Iceberg and Delta
tables in Lakebench. It tracks table schemas, partition layouts, and data file locations
so that Spark and Trino can discover and query lakehouse tables without
managing metadata themselves.

Lakebench deploys HMS through the **Stackable Hive Operator**, which manages a
`HiveCluster` custom resource. The operator handles image lifecycle, S3
credential injection, and PostgreSQL connectivity. HMS stores its schema catalog
in PostgreSQL and references data files in the S3-compatible object store
(FlashBlade, MinIO, or AWS S3).

HMS is the default catalog type. Set `architecture.catalog.type: hive` (or
leave it unset) to use it.

## Stackable Operator

The Stackable Hive Operator (plus commons, listener, and secret operators)
must be present on the cluster before deploying a Hive catalog.

A cluster admin installs all four once, under the cluster lease, with:

```bash
lakebench admin install --component stackable lakebench.yaml
```

It installs at `version` in `namespace`:

```yaml
architecture:
  catalog:
    hive:
      operator:
        namespace: "stackable"      # Where the operators run
        version: "25.7.0"           # SDP chart version a fresh install uses
```

An SDP that is already installed (in any namespace) is left as it is; a
partial install from an interrupted run is completed at the installed
version. `lakebench deploy` never installs the operators. It checks for the
`hiveclusters.hive.stackable.tech` and `secretclasses.secrets.stackable.tech`
CRDs, and that their operators are running; if either is missing, the Hive
step fails with the `admin install` command. The v1.6 key
`operator.install: true` is refused by the commands that change data
(`destroy`, `status` and `admin` still load it, as false).

### Managed Resources

When `lakebench deploy` runs, the Hive deployer creates three Kubernetes
resources (plus a CA-certificate SecretClass, `lakebench-s3-ca-cert-<namespace>`,
when `platform.storage.s3.ca_cert` is set):

| Resource | Kind | Purpose |
|---|---|---|
| `lakebench-s3-credentials-<namespace>` | SecretClass (cluster-scoped) | Stackable secret backend that locates S3 credentials via `k8sSearch` |
| `lakebench-hive` | HiveCluster | Stackable-managed metastore with S3 and PostgreSQL integration |
| `lakebench-hive-metastore` | Service (ClusterIP) | Stable DNS endpoint for thrift connections |

### Service DNS

All components (Spark jobs, Trino) connect to HMS through the ClusterIP
service at:

```
lakebench-hive-metastore.<namespace>.svc.cluster.local:9083
```

The thrift URI used in Spark and Trino configurations is:

```
thrift://lakebench-hive-metastore.<namespace>.svc.cluster.local:9083
```

## YAML Configuration

All Hive settings live under `architecture.catalog`. Below are the
configurable fields with their defaults.

### Hive Version

The metastore runs Stackable's own Hive image
(`oci.stackable.tech/sdp/hive:3.1.3-stackable<sdp-version>`), and the
HiveCluster template renders `productVersion: "3.1.3"`. Hive 3.1.3 is
deliberate: Stackable recommends it because Hive 4 breaks Iceberg
(`get_table` TApplicationException) and Trino ANALYZE. Run output records
3.1.3, the version the template renders. There is no config key for it:
v1.7 removed `images.hive`, which never selected it. A config that still
names 3.1.3 there loads with a note; one that names another version is
refused by the commands that change data.

### Catalog Selection and Tuning

```yaml
architecture:
  catalog:
    type: hive                       # hive | polaris  (unity and none have no supported recipe and are refused)
    hive:
      resources:
        cpu_min: "500m"              # CPU request
        cpu_max: "2"                 # CPU limit
        memory: "4Gi"               # Memory request and limit
```

The `resources` fields are applied to the HiveCluster. The HiveCluster
template sets `hive.metastore.server.min.threads=10`, `max.threads=50` and
`hive.metastore.client.socket.timeout=300s`, tuned for moderate concurrency
(up to 50 simultaneous catalog operations). v1.7 removed the `hive.thrift`
block, which never changed them: a config that carries it at those values
loads with a note, and any other value is refused by the commands that
change data.

### Operator-Injected Configuration

The HiveCluster template also applies proven `hive-site.xml` overrides
that are not exposed as YAML fields:

- **Connection pool**: maxPoolSize=20, maxActive=15, maxIdle=5, minIdle=2
- **Batch retrieval**: batch.retrieve.max=500, table.partition.max=1000
- **Reliability**: tcp.keepalive=true, failure.retries=3, connect.retry.delay=5s
- **Concurrency**: hive.support.concurrency=true, dynamic.partition.mode=nonstrict

These are hardcoded in the template because they are proven at 1TB+ scale and
should not need adjustment for typical workloads.

## PostgreSQL Backend

HMS stores its catalog schema (table definitions, partition metadata, column
statistics) in PostgreSQL. Lakebench deploys one PostgreSQL instance per
deployment; it backs whichever catalog the deployment uses (Hive, Polaris or
Unity).

PostgreSQL is configured under `platform.compute.postgres`:

```yaml
platform:
  compute:
    postgres:
      storage: "10Gi"               # PVC size for PostgreSQL data
      storage_class: ""              # StorageClass (empty = cluster default)
```

For production deployments, set `storage_class` to a replicated StorageClass
such as `px-csi-db` (repl=3) to survive node failures. The database name and
user are both `hive`, and the JDBC connection string is:

```
jdbc:postgresql://lakebench-postgres.<namespace>.svc.cluster.local:5432/hive
```

PostgreSQL must be healthy before the HiveCluster can initialize its schema.
The deployment engine enforces this ordering automatically.

## When to Use Hive vs Polaris

| Consideration | Hive | Polaris |
|---|---|---|
| Setup complexity | Low -- Stackable operators (commons, secret, listener, hive) | Moderate -- REST catalog + config |
| Protocol | Thrift (binary, port 9083) | REST/HTTP (JSON, port 8181) |
| Multi-engine sharing | Spark + Trino (same cluster) | Any engine with Iceberg REST support |
| Credential vending | No (credentials injected via SecretClass) | No: lakebench runs Polaris with `stsUnavailable: true`, and each client (Spark, Trino) keeps its own static S3 credentials |
| Multi-tenancy | Single catalog namespace | Namespace-level isolation |
| Table format support | Iceberg, Delta | Iceberg |

**Choose Hive** when you want the simplest deployment path and your workload
uses a single compute cluster, or when you need Delta. Hive is battle-tested
at 1TB+ scale and is the catalog of the default recipe.

**Choose Polaris** when you need REST API access to the catalog, plan to share
tables across multiple compute engines or clusters. Lakebench does not use
Polaris credential vending (S3 has no STS on FlashBlade), so vending is not
a reason to choose it here. See [quickstart-polaris.md](quickstart-polaris.md)
for migration steps.

**Recipes using Hive:** `hive-iceberg-spark-trino` (the default),
`hive-iceberg-spark-thrift`, `hive-iceberg-spark-duckdb`,
`hive-iceberg-spark-none`, `hive-delta-spark-trino`,
`hive-delta-spark-thrift`, `hive-delta-spark-none`.
See the [Recipes Guide](recipes.md) for all combinations.

## Cross-References

- [Recipes](recipes.md) -- all supported component combinations
- [quickstart-polaris.md](quickstart-polaris.md) -- Switching from Hive to Polaris
- [component-postgres.md](component-postgres.md) -- PostgreSQL backend details
- [architecture.md](architecture.md) -- Overall Lakebench architecture
- [deployment.md](deployment.md) -- Full deployment walkthrough
- [troubleshooting.md](troubleshooting.md) -- Common Hive/HMS issues
