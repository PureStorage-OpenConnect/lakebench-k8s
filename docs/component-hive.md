# Hive Metastore

Reference: configure the Hive Metastore catalog: Stackable operator, config keys and deploy behaviour.

## What it does

Hive Metastore (HMS) is the metadata catalog for Iceberg and Delta tables. It tracks table schemas, partition layouts and data file locations, so Spark and Trino find tables without managing metadata themselves.

- HMS stores its catalog in PostgreSQL and references data files in the S3-compatible store (FlashBlade, MinIO or AWS S3).
- Lakebench deploys it through the **Stackable Hive Operator** as a `HiveCluster` resource. The operator handles the image, S3 credential injection and PostgreSQL connectivity.
- Hive is the catalog when `architecture.catalog.type` is `hive` or unset. `lakebench init` writes a Polaris recipe instead.
- Recipes: `hive-iceberg-spark-trino` (`recipe: default`), `hive-iceberg-spark-thrift`, `hive-iceberg-spark-duckdb`, `hive-iceberg-spark-none`, `hive-delta-spark-trino`, `hive-delta-spark-thrift`, `hive-delta-spark-none` ([Recipes](recipes.md)).

## Version and image

- The metastore runs Stackable's image `oci.stackable.tech/sdp/hive:<hive-version>-stackable<sdp-version>`. `STACKABLE_HIVE_VERSION` in `config/schema.py` sets the HiveCluster `productVersion`; run output records it. Version and why Hive 4 is not used: [version matrix](compatibility-matrix.md#component-version-matrix). Hive 4 fails Iceberg `get_table` with TApplicationException.
- No config key selects it. `images.hive` is removed ([UPGRADING-1.7.md](../UPGRADING-1.7.md#config-fields-nothing-read-are-removed)): a config that names 3.1.3 there loads with a note; another version is refused by the commands that change data.

## Stackable operator

The Stackable Hive Operator, plus the commons, listener and secret operators, must be on the cluster before a Hive catalog deploys. A cluster admin installs all four once, under the cluster lease:

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
- `operator.install: true` is refused by the commands that change data; `destroy`, `status` and `admin` load it as false.
- Lakebench does not automate an SDP upgrade: helm does not update the CRDs the charts ship in `crds/`.
- The resource is API `hive.stackable.tech/v1alpha1`, kind `HiveCluster`. S3 is set through `spec.clusterConfig.s3` and credentials through `secretClass`.
- The raw `apache/hive:3.1.3` image does not work: it has no hadoop-aws jars for S3A (`S3AFileSystem not found`). It fails when Iceberg creates a namespace with an S3 location.

## Configuration keys

Defaults from `HiveConfig` and `HiveResourcesConfig` in `config/schema.py`.

| Key | Default | Effect |
|---|---|---|
| `architecture.catalog.type` | `hive` | `hive` or `polaris`. `unity` and `none` have no supported recipe and are refused. |
| `architecture.catalog.hive.resources.cpu_min` | `500m` | HiveCluster CPU request |
| `architecture.catalog.hive.resources.cpu_max` | `2` | HiveCluster CPU limit |
| `architecture.catalog.hive.resources.memory` | `4Gi` | HiveCluster memory request and limit |
| `architecture.catalog.hive.operator.namespace` | `stackable` | Namespace a fresh operator install uses |
| `architecture.catalog.hive.operator.version` | [matrix](compatibility-matrix.md#component-version-matrix) | SDP chart version a fresh install uses |

### Settings fixed in the template

The HiveCluster template sets these `hive-site.xml` values, proven at 1TB+ scale. No YAML field changes them.

- **Thrift server:** `hive.metastore.server.min.threads=10`, `max.threads=50` (up to 50 simultaneous catalog operations), `hive.metastore.client.socket.timeout=300s`.
- **Connection pool:** maxPoolSize=20, maxActive=15, maxIdle=5, minIdle=2.
- **Batch retrieval:** batch.retrieve.max=500, table.partition.max=1000.
- **Reliability:** tcp.keepalive=true, failure.retries=3, connect.retry.delay=5s.
- **Concurrency:** hive.support.concurrency=true, dynamic.partition.mode=nonstrict.

The removed `hive.thrift` block never changed the thread or timeout values. A config that carries it at those values loads with a note; any other value is refused by the commands that change data.

## Deploy and destroy

`lakebench deploy` creates three resources, plus a CA-certificate SecretClass `lakebench-s3-ca-cert-<namespace>` when `platform.storage.s3.ca_cert` is set:

| Resource | Kind | Purpose |
|---|---|---|
| `lakebench-s3-credentials-<namespace>` | SecretClass (cluster-scoped) | Stackable secret backend that finds S3 credentials via `k8sSearch` |
| `lakebench-hive` | HiveCluster | Metastore with S3 and PostgreSQL integration |
| `lakebench-hive-metastore` | Service (ClusterIP) | Stable DNS name for thrift connections |

- Spark and Trino connect at `thrift://lakebench-hive-metastore.<namespace>.svc.cluster.local:9083`.
- PostgreSQL must be healthy before the HiveCluster can initialize its schema; the deployment engine enforces the order. Database and user are both `hive`: `jdbc:postgresql://lakebench-postgres.<namespace>.svc.cluster.local:5432/hive`. See [PostgreSQL](component-postgres.md) for storage and credentials.
- Destroy order is in [deployment.md](deployment.md#destroy-order).

Hive or Polaris: [Choosing a catalog](recipes.md#choosing-a-catalog).

## Troubleshooting

- [Hive Metastore DNS resolution fails](troubleshooting.md#hive-metastore-dns-resolution-fails)
- [Hive or Polaris cannot log in to PostgreSQL](troubleshooting.md#hive-or-polaris-cannot-log-in-to-postgresql)

## See also

[Recipes](recipes.md), [Polaris](component-polaris.md), [PostgreSQL](component-postgres.md), [Architecture](architecture.md), [Deployment](deployment.md).
