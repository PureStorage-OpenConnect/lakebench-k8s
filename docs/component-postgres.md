# PostgreSQL

Reference: configure the PostgreSQL metadata backend: config keys, credentials, deploy and destroy behaviour.

## What it does

PostgreSQL is the metadata backend for every recipe. Hive Metastore, Apache Polaris and Unity Catalog store their catalog metadata (table definitions, partition info, Iceberg snapshots) in it.

- One instance per deployment, as a StatefulSet with a PersistentVolumeClaim, so metadata survives pod restarts.
- The deployer (`src/lakebench/deploy/postgres.py`) renders a ServiceAccount, a StatefulSet and a headless Service from Jinja2 templates.
- Service DNS: `lakebench-postgres.<namespace>.svc.cluster.local:5432`.
- Databases: `hive` (user `hive`); Polaris and Unity use their own `polaris` and `unity` databases on the same instance.

## Version and image

`images.postgres` sets the image. Default and tested versions: [version matrix](compatibility-matrix.md#component-version-matrix).

## Configuration keys

Defaults from `PostgresConfig` in `config/schema.py`.

| Key | Default | Effect |
|---|---|---|
| `images.postgres` | [matrix](compatibility-matrix.md#component-version-matrix) | Container image |
| `platform.compute.postgres.storage` | `10Gi` | PVC size for the data directory. Catalog metadata is compact; 10Gi fits most deployments. |
| `platform.compute.postgres.storage_class` | `""` | PVC StorageClass. Empty uses the cluster default. |

### Choosing a StorageClass

- **Production or bare-metal:** a replicated class such as `px-csi-db` (Portworx `repl=3`). Losing this volume loses every table definition and partition record.
- **Development or ephemeral clusters:** the cluster default is usually fine. On cloud providers it often maps to a network volume with provider-managed replication.
- **Never a scratch class.** `px-csi-scratch` (`repl=1`) is for expendable data such as Spark shuffle.

## Credentials

Each deployment gets its own passwords, generated once and stored in Secrets in its namespace (`deploy/deployment_secrets.py`):

| Secret | Holds |
|---|---|
| `lakebench-postgres-secret` (key `password`) | The `hive` role password. PostgreSQL reads it only at initdb, so a stored value wins. With no Secret but an existing `data-lakebench-postgres-0` PVC, the 1.6 default is kept. |
| `lakebench-polaris-db` | The `polaris` role password. With no Secret, an existing `polaris` role (a 1.6 deployment) keeps its 1.6 password. |

- User `hive` and database `hive` are fixed.
- Each deploy re-syncs the `hive` and `polaris` role passwords from the Secrets.
- Hive, Polaris and Unity get the JDBC connection string through template rendering.

## Deploy order

`deploy/engine.py` runs these steps in order:

1. Namespace, Secrets, silver-state ConfigMap, S3 buckets
2. Scratch StorageClass check (verify only; a cluster admin installs it with `lakebench admin install --component scratch-storage-class`)
3. **PostgreSQL** (StatefulSet and Service)
4. Hive Metastore or Polaris, then Spark RBAC, then Unity Catalog when that is the catalog
5. Spark Operator check and watch-list entry
6. Dependency server (`lb-deps`)
7. Query engine (Trino, Spark Thrift Server or DuckDB)
8. Observability stack (if enabled)

Every catalog needs a running PostgreSQL. The deployer waits for two signals: the StatefulSet reports all replicas ready, and `pg_isready` against the `hive` database on pod `lakebench-postgres-0` succeeds.

## Destroy

- `lakebench destroy` removes PostgreSQL after the catalog and query engine, before the namespace ([destroy order](deployment.md#destroy-order)).
- The StatefulSet, the Service and the data PVC are deleted by name.
- The PVC comes from the volumeClaimTemplate and carries no labels, so destroy matches `data-lakebench-postgres-<n>` exactly. Another application's claim in the namespace is never selected.
- The PVC and its metadata are deleted also with `platform.kubernetes.create_namespace: false`, so the next deploy starts with an empty metastore. Before 1.7 the PVC survived such a destroy.

## Troubleshooting

- [PostgreSQL PVC stuck in Pending](troubleshooting.md#postgresql-pvc-stuck-in-pending)
- [Hive or Polaris cannot log in to PostgreSQL](troubleshooting.md#hive-or-polaris-cannot-log-in-to-postgresql)

## See also

[Hive Metastore](component-hive.md), [Polaris](component-polaris.md), [Deployment](deployment.md), [Architecture](architecture.md).
