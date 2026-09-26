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
   When `s3.ca_cert` is set, also creates a CA certificate secret for HTTPS
   endpoints (used by all components for TLS verification).
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

> Before running Spark jobs, Lakebench checks that the Spark Operator is
> watching the target namespace. If `spark.operator.install: true`, it
> auto-adds the namespace and restarts the operator. If `install: false`
> (the default), it reports the fix command. Run `lakebench validate`
> to check this before deploying.

8. **Prometheus** -- Deploys Prometheus for metrics collection. Only deployed when
   `observability.enabled` is true or the `--include-observability`
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

1. **Ownership check** -- Refuses to continue if the namespace carries
   another deployment's identity annotations, targets a different cluster, or
   has no lakebench annotations at all (unless `--force-legacy`).
2. **SparkApplications** -- Deletes all running and completed Spark jobs.
3. **Spark pods** -- Force-deletes any orphaned driver and executor pods.
4. **Datagen jobs** -- Deletes Kubernetes batch Jobs and pods from data generation.
5. **Drop tables** -- Drops the workload's tables via the deployed query engine
   (Trino or Spark Thrift Server). Destroy runs no table maintenance: no
   Iceberg `expire_snapshots` or `remove_orphan_files` and no Delta `VACUUM`,
   because the buckets are emptied right after. Skipped when the engine is
   DuckDB or `none`.
6. **S3 buckets** -- Empties the buckets, including aborting incomplete
   multipart uploads, then deletes only the buckets this deployment created
   (see below).
7. **Observability** -- Uninstalls the kube-prometheus-stack release if
   observability was enabled.
8. **Query engine** -- Removes the configured engine (Trino, Spark Thrift
   Server, or DuckDB).
9. **Catalog** -- Removes the HiveCluster, Polaris, or Unity deployment.
10. **PostgreSQL** -- Removes the StatefulSet, Service, and PVCs.
11. **RBAC and Secrets** -- Removes the Spark ServiceAccount, Role,
    RoleBinding, and Secrets.
12. **Namespace** -- Removes the namespace from the Spark Operator watch list,
    deletes it (only when `create_namespace` is true), and waits until it is
    NotFound before reporting it deleted.

Destroy never deletes the scratch StorageClass. It is shared, cluster-scoped
infrastructure that other deployments on the same cluster use.

Only buckets lakebench created are deleted. Deploy records each bucket it
creates (the `lakebench.deployment/created-buckets` namespace annotation, plus
a `lakebench.created` tag where the backend supports tagging), and destroy
deletes a bucket only when it is in that record and its ownership checks out.
Pre-provisioned buckets (`create_buckets: false`), buckets deploy adopted, and
`--keep-buckets` runs are emptied but kept. If a recorded bucket cannot be
emptied or deleted, destroy keeps the namespace, because its annotations are
the only ownership record a re-run can use to finish the job.

### Command Flags

| Flag | Short | Description |
|---|---|---|
| `--force` / `--yes` | `-y` | Skip the confirmation prompt |

See the [CLI reference](cli-reference.md#destroy) for the full flag list
(`--keep-buckets`, `--namespace-timeout`, `--force-legacy`, and others).

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
