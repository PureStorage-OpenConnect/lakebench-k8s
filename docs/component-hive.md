# Hive Metastore

Reference: configure the Hive Metastore catalog: Stackable operator, config keys and deploy behaviour.

## What it does

Hive Metastore (HMS) is the metadata catalog for Iceberg and Delta tables: schemas, partition layouts and data file locations.

- HMS stores its catalog in PostgreSQL and references data files in the S3-compatible store (FlashBlade, MinIO or AWS S3).
- Lakebench deploys it as a `HiveCluster` through the **Stackable Hive Operator**, which handles the image, S3 credential injection and PostgreSQL connectivity.
- Hive is the catalog when `architecture.catalog.type` is `hive` or unset. `lakebench init` writes a Polaris recipe instead. Hive or Polaris: [Choosing a catalog](recipes.md#choosing-a-catalog).
- Recipes: `hive-iceberg-spark-trino` (`recipe: default`), `hive-iceberg-spark-thrift`, `hive-iceberg-spark-duckdb`, `hive-iceberg-spark-none`, `hive-delta-spark-trino`, `hive-delta-spark-thrift`, `hive-delta-spark-none` ([Recipes](recipes.md)).

## Version and image

- The metastore runs Stackable's Hive 3.1.3 image ([version matrix](compatibility-matrix.md#component-version-matrix)). Hive 4 is not used: it fails Iceberg `get_table` with TApplicationException and breaks Trino `ANALYZE`. No config key selects the version; `images.hive` is removed (1.7.0 in the [changelog](../CHANGELOG.md#170---2026-10-06)).

## Stackable operator

The Stackable Hive Operator, plus the commons, listener and secret operators, must be on the cluster before a Hive catalog deploys. A cluster admin installs all four once, under the [cluster lease](operations.md#installing-the-shared-pieces):

```bash
lakebench admin install --component stackable lakebench.yaml
```

```yaml
architecture:
  catalog:
    hive:
      operator:
        namespace: "stackable"      # Where the operators run
        version: "25.7.0"           # SDP chart version a fresh install uses
```

- An SDP already installed in any namespace is left as it is. A partial install from an interrupted run is completed at the installed version.
- `lakebench deploy` never installs the operators. It checks for the `hiveclusters.hive.stackable.tech` and `secretclasses.secrets.stackable.tech` CRDs and that their operators run. If either is missing, the Hive step fails with the `admin install` command.
- `operator.install: true` is refused by the commands that change data; `destroy`, `stop`, `admin` and the read-only commands load it as false.
- SDP upgrades are not automated: helm does not update the CRDs the charts ship in `crds/`.
- The resource is API `hive.stackable.tech/v1alpha1`, kind `HiveCluster`. S3 is set through `spec.clusterConfig.s3` and credentials through `secretClass`.

## Configuration keys

| Key | Default | Effect |
|---|---|---|
| `architecture.catalog.type` | `hive` | `hive` or `polaris`. `unity` and `none` have no supported recipe and are refused. |
| `architecture.catalog.hive.resources.cpu_min` | `500m` | HiveCluster CPU request |
| `architecture.catalog.hive.resources.cpu_max` | `2` | HiveCluster CPU limit |
| `architecture.catalog.hive.resources.memory` | `4Gi` | HiveCluster memory request and limit |
| `architecture.catalog.hive.operator.namespace` | `stackable` | Namespace a fresh operator install uses |
| `architecture.catalog.hive.operator.version` | [matrix](compatibility-matrix.md#component-version-matrix) | SDP chart version a fresh install uses |

### Settings fixed in the template

No config key changes these: metastore server threads 10-50, client socket timeout 300 s, JDO connection pool `maxPoolSize` 20, `maxActive` 15, `maxIdle` 5, `minIdle` 2.

## Deploy and destroy

`lakebench deploy` creates three resources, plus a CA-certificate SecretClass `lakebench-s3-ca-cert-<namespace>` when `platform.storage.s3.ca_cert` is set:

| Resource | Kind | Purpose |
|---|---|---|
| `lakebench-s3-credentials-<namespace>` | SecretClass (cluster-scoped) | Stackable secret backend that finds S3 credentials via `k8sSearch` |
| `lakebench-hive` | HiveCluster | Metastore with S3 and PostgreSQL integration |
| `lakebench-hive-metastore` | Service (ClusterIP) | Stable DNS name for thrift connections |

- Spark and Trino connect at `thrift://lakebench-hive-metastore.<namespace>.svc.cluster.local:9083`.
- PostgreSQL must be healthy before the HiveCluster can initialize its schema; the deployment engine enforces the order. Database and user are both `hive`: `jdbc:postgresql://lakebench-postgres:5432/hive?connectTimeout=30&socketTimeout=300&loginTimeout=30`. See [PostgreSQL](component-postgres.md) for storage and credentials.
- Destroy order is in [deployment.md](deployment.md#destroy-order).

## Troubleshooting

- [Hive Metastore DNS resolution fails](troubleshooting.md#hive-metastore-dns-resolution-fails)
- [Hive or Polaris cannot log in to PostgreSQL](troubleshooting.md#hive-or-polaris-cannot-log-in-to-postgresql)

## See also

[Recipes](recipes.md), [Polaris](component-polaris.md), [PostgreSQL](component-postgres.md), [Architecture](architecture.md), [Deployment](deployment.md).
