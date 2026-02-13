# Deployment Guide

Lakebench manages the full lifecycle of a lakehouse test environment on
Kubernetes: deploying infrastructure, checking status, and tearing everything
down cleanly. All operations are driven by a single YAML configuration file
(see [Configuration Reference](configuration.md)).

## Prerequisites

Before deploying, ensure you have:

- A running Kubernetes cluster (tested on OpenShift 4.x and vanilla K8s 1.26+)
- `kubectl` configured and pointing at the target cluster
- An S3-compatible object store (FlashBlade, MinIO, or AWS S3)
- S3 credentials with permission to create/delete buckets and objects
- A valid Lakebench config file with `name` and S3 settings filled in

## Deploying Infrastructure

```bash
lakebench deploy my-config.yaml
```

This creates all infrastructure components in a deterministic order. Each step
depends on the previous one completing successfully. If any step fails, the
engine stops and reports the error.

### Deployment Order

The deployment engine follows this fixed sequence:

1. **Namespace** -- Creates the Kubernetes namespace (or reuses if it exists). If a
   namespace is in `Terminating` state from a previous destroy, the engine waits
   for it to finish before re-creating.
2. **Secrets** -- Creates S3 credential secrets and PostgreSQL credential secrets
   from the config values (or references an existing secret via `secret_ref`).
3. **Scratch StorageClass** -- Creates a Portworx `repl=1` StorageClass for Spark
   shuffle volumes. Skipped if `platform.storage.scratch.enabled` is false.
   Non-fatal if creation fails (may need cluster-admin).
4. **PostgreSQL** -- Deploys a PostgreSQL StatefulSet as the metadata backend for
   the catalog service.
5. **Hive Metastore or Polaris** -- Deploys the catalog selected by
   `architecture.catalog.type`. If `hive`, deploys a Stackable HiveCluster CRD.
   If `polaris`, deploys an Apache Polaris REST catalog Deployment. The
   non-selected catalog is automatically skipped.
6. **Trino** -- Deploys the Trino coordinator (Deployment) and workers
   (StatefulSet) with the Iceberg connector configured to point at the catalog.
   Skipped if `architecture.query_engine.type` is `none`.
7. **Spark RBAC** -- Creates the ServiceAccount, Role, and RoleBinding for Spark
   job submission. On OpenShift, also binds the `anyuid` SCC to the service
   account.
8. **Prometheus** -- Deploys Prometheus for metrics collection. Only deployed when
   `observability.metrics.prometheus.enabled` is true or the `--include-observability`
   flag is used.
9. **Grafana** -- Deploys Grafana with pre-configured dashboards. Same activation
   conditions as Prometheus.

### Command Flags

| Flag | Short | Description |
|---|---|---|
| `--dry-run` | | Show what would be deployed without making changes |
| `--include-observability` | | Deploy Prometheus and Grafana monitoring stack |
| `--yes` | `-y` | Skip the confirmation prompt |

### Examples

Deploy with confirmation prompt:

```bash
lakebench deploy my-config.yaml
```

Deploy without confirmation:

```bash
lakebench deploy my-config.yaml --yes
```

Deploy with the full observability stack:

```bash
lakebench deploy my-config.yaml --include-observability --yes
```

Dry run (show plan without deploying):

```bash
lakebench deploy my-config.yaml --dry-run
```

## Checking Status

After deploying, verify that all components are healthy:

```bash
lakebench status my-config.yaml
```

This queries the Kubernetes API and displays a table of component statuses
(PostgreSQL, Hive Metastore, Trino coordinator, Prometheus, Grafana) with
replica counts and readiness indicators.

You can also check status by namespace without a config file:

```bash
lakebench status --namespace my-lakehouse
```

## Accessing Monitoring (Observability Stack)

When deployed with `--include-observability`, Prometheus and Grafana are
accessible within the cluster:

- Grafana: `http://lakebench-grafana.<namespace>.svc:3000` (default credentials: `admin` / `lakebench`)
- Prometheus: `http://lakebench-prometheus.<namespace>.svc:9090`

For local access, use port-forwarding:

```bash
kubectl port-forward svc/lakebench-grafana 3000:3000 -n <namespace>
kubectl port-forward svc/lakebench-prometheus 9090:9090 -n <namespace>
```

Pre-configured dashboards include Spark job metrics, Trino query performance,
storage throughput, and cluster resource utilization.

## Destroying Infrastructure

```bash
lakebench destroy my-config.yaml
```

Destroy tears down everything in the correct order. This is destructive and
irreversible. Without `--force`, you will be prompted for confirmation.

### Destroy Order

The destroy engine follows a specific sequence to ensure clean removal:

1. **SparkApplications** -- Deletes all running and completed Spark jobs.
2. **Spark pods** -- Force-deletes any orphaned driver and executor pods.
3. **Datagen jobs** -- Deletes Kubernetes batch Jobs and pods from data generation.
4. **Iceberg maintenance** -- Runs `expire_snapshots` and `remove_orphan_files`
   on all tables via Trino to clean up S3 metadata files.
5. **Drop tables** -- Drops all Iceberg tables (`bronze`, `silver`, `gold`) via Trino.
6. **S3 buckets** -- Empties all three S3 buckets, including aborting incomplete
   multipart uploads.
7. **Grafana** -- Removes the Grafana Deployment and ConfigMaps.
8. **Prometheus** -- Removes the Prometheus StatefulSet and PVCs.
9. **Trino** -- Removes the coordinator Deployment, worker StatefulSet, PVCs,
   Services, and ConfigMaps.
10. **Hive/Polaris** -- Removes the HiveCluster CRD or Polaris Deployment.
11. **PostgreSQL** -- Removes the StatefulSet, Service, and PVCs.
12. **RBAC and Secrets** -- Removes the Spark ServiceAccount, Role, RoleBinding,
    and all Secrets.
13. **Scratch StorageClass** -- Removes the cluster-scoped StorageClass (best-effort).
14. **Namespace** -- Deletes the Kubernetes namespace.

### Command Flags

| Flag | Short | Description |
|---|---|---|
| `--force` | `-f` | Skip the confirmation prompt |

### Examples

Destroy with confirmation:

```bash
lakebench destroy my-config.yaml
```

Destroy without confirmation:

```bash
lakebench destroy my-config.yaml --force
```

## Selective Cleanup

If you want to delete data without destroying infrastructure (for example,
to re-run data generation), use the `clean` command:

```bash
# Empty all S3 buckets (bronze + silver + gold)
lakebench clean data my-config.yaml

# Empty a single layer
lakebench clean bronze my-config.yaml
lakebench clean silver my-config.yaml
lakebench clean gold my-config.yaml

# Delete local metrics and reports
lakebench clean metrics my-config.yaml

# Delete journal session files
lakebench clean journal my-config.yaml
```

All `clean` targets prompt for confirmation unless `--force` is passed.
Infrastructure components (Kubernetes resources, catalog entries) are not
affected by `clean`.
