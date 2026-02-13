# Component: PostgreSQL

## Overview

PostgreSQL serves as the relational metadata backend for Lakebench. Both Hive Metastore and Apache Polaris persist their catalog metadata (table definitions, partition info, Iceberg snapshots) in PostgreSQL. Lakebench deploys PostgreSQL as a Kubernetes StatefulSet with a PersistentVolumeClaim for durable storage, ensuring metadata survives pod restarts and node failures.

The deployer (`src/lakebench/deploy/postgres.py`) renders three Kubernetes manifests from Jinja2 templates -- a ServiceAccount, a StatefulSet, and a headless Service -- then waits for the pod to pass `pg_isready` health checks before returning.

## YAML Configuration

PostgreSQL settings are split across two sections of the config file:

```yaml
images:
  postgres: "postgres:17"               # Default container image

platform:
  compute:
    postgres:
      storage: "10Gi"                   # PVC size for the data directory
      storage_class: ""                 # StorageClass (empty = cluster default)
```

### Field Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `images.postgres` | string | `postgres:17` | Container image for the PostgreSQL pod. |
| `platform.compute.postgres.storage` | string | `10Gi` | Size of the PersistentVolumeClaim attached to the StatefulSet. Hive and Polaris metadata is compact; 10 Gi is sufficient for most deployments. |
| `platform.compute.postgres.storage_class` | string | `""` | Kubernetes StorageClass for the PVC. When left empty, the cluster default StorageClass is used. |

## Storage Class Guidance

The StorageClass you choose for PostgreSQL directly affects metadata durability:

- **Production / bare-metal clusters:** Use a replicated StorageClass such as `px-csi-db` (Portworx with `repl=3`). PostgreSQL holds Hive and Polaris metadata that must survive node failures. Losing this data means losing all table definitions and partition tracking.
- **Development / ephemeral clusters:** The cluster default (empty string) is usually fine. On cloud providers this often maps to a network-attached volume with provider-managed replication.
- **Scratch classes are not appropriate.** StorageClasses like `px-csi-scratch` (`repl=1`) are designed for expendable data such as Spark shuffle. Never use them for PostgreSQL.

## Role in the Deployment Order

Lakebench deploys infrastructure in a fixed sequence defined in the deployment engine (`src/lakebench/deploy/engine.py`):

1. Namespace, Secrets
2. Scratch StorageClass
3. **PostgreSQL** (StatefulSet + Service)
4. Catalog service (Hive Metastore or Polaris)
5. Query engine (Trino)
6. Spark RBAC
7. Monitoring stack (if enabled)

PostgreSQL is deployed third, immediately after namespace setup and scratch StorageClass creation, because both Hive Metastore and Polaris depend on a running PostgreSQL instance before they can start. The deployer waits for two readiness signals before proceeding: the StatefulSet reports all replicas ready, and a `pg_isready` probe against the `hive` database on pod `lakebench-postgres-0` succeeds.

## Credentials

PostgreSQL credentials are auto-generated during deployment and stored in a Kubernetes Secret in the target namespace. Downstream components (Hive, Polaris) receive the JDBC connection string via template rendering. The default database name is `hive` and the default user is `hive`. The internal service DNS name follows the pattern:

```
lakebench-postgres.<namespace>.svc.cluster.local:5432
```

## Teardown

During `lakebench destroy`, PostgreSQL is torn down in reverse deployment order -- after the catalog service and query engine have been removed, but before the namespace itself is deleted. The PVC is deleted along with the StatefulSet, so all metadata is permanently removed.

## See Also

- [Hive Metastore Component](component-hive.md) -- Hive depends on PostgreSQL for metadata persistence
- [Polaris Quickstart](quickstart-polaris.md) -- Polaris REST catalog backed by PostgreSQL
- [Deployment Guide](deployment.md) -- Full deployment walkthrough including PostgreSQL
- [Architecture Overview](architecture.md) -- Where PostgreSQL fits in the stack
