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
   from the config's `access_key` and `secret_key` (`secret_ref` is not supported).
   When `s3.ca_cert` is set, also creates a CA certificate secret for HTTPS
   endpoints (used by all components for TLS verification).
3. **S3 buckets** -- Creates the bronze, silver and gold buckets, or adopts
   existing ones, and records which buckets this deployment created.
4. **Scratch StorageClass** -- Verifies that the scratch StorageClass for Spark
   shuffle volumes exists. Deploy never creates it: it is shared cluster-scoped
   infrastructure. If it is missing, deploy stops and points to
   `lakebench admin install-scratch-storage-class`, which a cluster admin runs
   once. Skipped if `platform.storage.scratch.enabled` is false.
5. **PostgreSQL** -- Deploys a PostgreSQL StatefulSet as the metadata backend for
   the catalog service.
6. **Hive Metastore or Polaris** -- Deploys the catalog selected by
   `architecture.catalog.type`. If `hive`, deploys a Stackable HiveCluster CRD.
   If `polaris`, deploys an Apache Polaris REST catalog Deployment. The
   non-selected catalog is automatically skipped.
7. **Spark RBAC** -- Creates the ServiceAccount, Role, and RoleBinding for Spark
   job submission. On OpenShift, also binds the `anyuid` SCC to the service
   account.
8. **Unity Catalog** -- Skipped unless `architecture.catalog.type` is `unity`.
   No shipped recipe uses Unity.
9. **Spark Operator** -- Checks that the shared Spark Operator is running and
   watches the deployment namespace. If the namespace is not watched, deploy
   adds it to `spark.jobNamespaces` with `helm upgrade`, under the
   `lakebench-cluster-lock` lease so concurrent deploys do not overwrite each
   other. With `platform.compute.spark.operator.install: true`, deploy also
   installs the operator when it is missing; with `install: false` (the
   default), a missing or broken operator fails the deploy.
10. **Trino** -- Deploys the Trino coordinator (Deployment) and workers
    (StatefulSet) with the connector configured to point at the catalog.
    Skipped unless `architecture.query_engine.type` is `trino`.
11. **Spark Thrift Server** -- Deployed when the query engine is `spark-thrift`.
12. **DuckDB** -- Deployed when the query engine is `duckdb`.
13. **Observability** -- Only when `observability.enabled` is true or the
    `--include-observability` flag is used. Prometheus and Grafana come from
    one shared `kube-prometheus-stack` release in the `lakebench-observability`
    namespace. Deploy installs it only if no such release exists on the
    cluster, never modifies an existing one, and applies this deployment's
    PodMonitors and dashboard in its own namespace. Destroy never uninstalls
    the shared release.

Run `lakebench validate` to check the operator and StorageClass before
deploying.

### Command Flags

| Flag | Short | Description |
|---|---|---|
| `--file` | `-f` | Config file (alternative to the positional argument) |
| `--dry-run` | | Show what would be deployed without making changes |
| `--include-observability` | | Deploy Prometheus and Grafana monitoring stack |
| `--yes` | `-y` | Skip the confirmation prompt |
| `--timeout` | `-t` | Global deployment timeout in seconds (default 3600, `0` = no timeout). Every wait inside a step, including the wait for the cluster lease, is bounded by it; when it runs out, the step fails naming the component and what it was waiting for. No shared change (Spark Operator install or watch-list upgrade, Stackable or observability install) starts after it; one already started is completed with its rollout and verify, which can run several minutes past it while holding the cluster lease. Helm calls already running finish first |
| `--local` | | Deploy locally with podman or docker instead of Kubernetes |
| `--workdir` | | Host directory for local mode state (default `~/.lakebench/local/<name>`) |
| `--force-legacy` | | Claim ownership of a pre-1.5 namespace or untagged bucket without tag proof. Use only for resources you have confirmed are yours |

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

This queries the Kubernetes API and displays a table of the deployment's
components (PostgreSQL, the configured catalog and query engine) with replica
counts and readiness indicators, and the datagen job's progress while it
runs.

You can also check status by namespace without a config file; the table
then lists every component lakebench can deploy, plus the shared Prometheus
and Grafana:

```bash
lakebench status --namespace my-lakehouse
```

## Accessing Monitoring (Observability Stack)

When deployed with `--include-observability`, Prometheus and Grafana run in
the shared `lakebench-observability` namespace (Grafana credentials:
`admin` / `lakebench`). The chart shortens service names, so list them:

```bash
kubectl get svc -n lakebench-observability -l release=lakebench-observability
kubectl port-forward -n lakebench-observability svc/<grafana service> 3000:80
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
5. **Table removal** -- Removes the workload's tables from the catalog without
   deleting files. On Trino destroy runs
   `CALL <catalog>.system.unregister_table(...)` rather than `DROP TABLE`,
   because Trino's `DROP TABLE` deletes table files (on Polaris, when the
   namespace is being deleted, it runs nothing: the catalog database goes
   with the namespace). On Spark Thrift Server an
   Iceberg `DROP TABLE` (no `PURGE`) removes only the catalog entry; a Delta
   table is dropped only when `DESCRIBE DETAIL` puts its location in a bucket
   destroy is about to empty, and any table left registered is listed in the
   summary. Destroy runs no table maintenance: no Iceberg `expire_snapshots`
   or `remove_orphan_files` and no Delta `VACUUM`. Skipped when the engine is
   DuckDB or `none`.
6. **S3 buckets** -- Empties the buckets, including aborting incomplete
   multipart uploads, then deletes only the buckets this deployment created
   (see below).
7. **Observability** -- Leaves the shared kube-prometheus-stack release in
   place, since other deployments may use it. Only a release an older
   lakebench installed into this deployment's own namespace is uninstalled.
8. **Query engine** -- Removes the configured engine (Trino, Spark Thrift
   Server, or DuckDB).
9. **Catalog** -- Removes the HiveCluster or Polaris deployment (or a Unity
   Catalog deployment, if the config selected `unity`; no recipe does).
10. **PostgreSQL** -- Removes the StatefulSet, the Service and the data PVC
    (`data-lakebench-postgres-<n>`, found by name), also when
    `create_namespace: false` keeps the namespace.
11. **RBAC and Secrets** -- Removes the Spark ServiceAccount, Role,
    RoleBinding, and Secrets (`lakebench-s3-credentials`,
    `lakebench-postgres-secret`, and `lakebench-ca-certificate` when
    `s3.ca_cert` is set).
12. **Remaining namespaced objects** -- Deletes, by name, what no step above
    removes: the PostgreSQL ServiceAccount, and with observability enabled
    the Pushgateway Deployment, Service and PVC, the Prometheus ConfigMap and
    the PodMonitors. When the namespace survives it also removes the
    `lakebench.deployment/state-schema` namespace annotation. The list is the
    Category-1 registry in `src/lakebench/deploy/category1.py`; a unit test
    runs deploy and the objects `run` creates against it, and checks every
    template, so an object destroy would leave fails it. With
    `create_namespace: false` one object is kept on purpose: the
    `lakebench-silver-state` ConfigMap, whose silver rebuild-epoch counters
    must not go back while table data written under them may outlive destroy
    (a reset counter makes Delta skip writes as already committed). The
    deployment's identity annotations (`lakebench.deployment/name` and the
    rest) also stay, so a re-run of a destroy that stopped half way still
    finds its record; a deployment with another name cannot deploy into that
    namespace until they are removed.
13. **Namespace** -- Removes the namespace from the Spark Operator watch list,
    then, still inside the cluster lease, waits up to 120 s until no running
    operator pod and no operator Deployment template lists it (a stale pod
    that nothing is replacing gets one more restart of the shared operator),
    deletes it (only when `create_namespace` is true), and waits until it is
    NotFound before reporting it deleted. If something still lists it, the
    namespace is kept and destroy exits 1.

Destroy never deletes the scratch StorageClass. It is shared, cluster-scoped
infrastructure that other deployments on the same cluster use.

Only buckets lakebench created are deleted. Deploy records each bucket it
creates (the `lakebench.deployment/created-buckets` namespace annotation, plus
a `lakebench.created` tag where the backend supports tagging), and destroy
deletes a bucket only when it is in that record and its ownership checks out.
Pre-provisioned buckets (`create_buckets: false`), buckets deploy adopted, and
`--keep-buckets` runs are emptied but kept. On a backend without bucket
tagging (FlashBlade) the bucket name is the only other ownership evidence, so
a pre-existing bucket is emptied only when deploy found it empty and recorded
that (`lakebench.deployment/adopted-empty-buckets`); one that already held
objects is left in place and reported unless you pass `--force-legacy`. If a recorded bucket cannot be
emptied or deleted, destroy keeps the namespace, because its annotations are
the only ownership record a re-run can use to finish the job.

### Command Flags

| Flag | Short | Description |
|---|---|---|
| `--force` / `--yes` | `-y` | Skip the confirmation prompt |
| `--keep-buckets` | | Empty the S3 buckets but do not delete them |
| `--namespace-timeout` | | Seconds to wait for the namespace to terminate (default 600) |
| `--force-legacy` | | Proceed on a namespace or bucket with no lakebench ownership record |

See the [CLI reference](cli-reference.md#destroy) for the full flag list,
including `--local`, `--workdir`, `--remove-data`,
`--allow-unverified-cluster` and `--file`, and for exit codes.

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
