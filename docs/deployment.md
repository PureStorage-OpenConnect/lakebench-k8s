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

The cluster-side prerequisites, each with its check and fix, are listed on the
generated [Prerequisites](prerequisites.md) page.

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
   job submission. On OpenShift, also grants the `anyuid` SCC to the service
   account (the PostgreSQL step does the same for `lakebench-postgres`); a
   refused grant fails the step.
8. **Unity Catalog** -- Skipped unless `architecture.catalog.type` is `unity`.
   No shipped recipe uses Unity.
9. **Spark Operator** -- Checks that the shared Spark Operator is running and
   watches the deployment namespace. If the namespace is not watched, deploy
   adds it to `spark.jobNamespaces` with `helm upgrade`, under the
   `lakebench-cluster-lock` lease so concurrent deploys do not overwrite each
   other. Deploy never installs the shared operator: a missing or broken
   operator fails the deploy, and a cluster admin installs it once with
   `lakebench admin install-spark-operator`
   (`platform.compute.spark.operator.install: true` is refused).
10. **Dependency server** -- Starts `lb-deps` (a Deployment, a Service and the
    5Gi PVC `lb-deps-data`) in the namespace, on the stock Spark image. Its init
    containers resolve the jars (and the AML reference wheels, and the DuckDB
    wheel and extensions when DuckDB is the engine) onto the PVC with a sha256
    for every file; the serving container re-hashes the set at every start and
    then serves it read-only. Deploy waits for it to be Ready (up to 900 s,
    within `--timeout`), reads the set's manifest from the pod with `kubectl
    exec`, recomputes the set hash from its entries and checks it, and records
    it in the ConfigMap `lb-deps-manifest` and the namespace annotation
    `lakebench.deployment/deps-set`. A redeploy with the same config renders
    the same pod template, so the pod is not restarted and nothing is resolved
    again; a changed image, table format version, workload, query engine,
    mirror key or Lakebench resolver resolves once more. The step reads pods,
    pod logs, events and the named StorageClass, and execs into the server
    pod. See
    [Dependency server failures](#dependency-server-failures).
11. **Trino** -- Deploys the Trino coordinator (Deployment) and workers
    (StatefulSet) with the connector configured to point at the catalog.
    Skipped unless `architecture.query_engine.type` is `trino`.
12. **Spark Thrift Server** -- Deployed when the query engine is `spark-thrift`.
    Its init container copies the dependency set from `lb-deps`, checking
    each file's sha256; deploy waits until the rollout is complete and the
    pod runs this deploy's set (Recreate: one pod at a time), and fails at
    once on a fetch that cannot recover by retrying.
13. **DuckDB** -- Deployed when the query engine is `duckdb`; its wheel and
    extensions come from the dependency set the same way.
14. **Observability** -- Only when `observability.enabled` is true.
    Prometheus and Grafana come from
    one shared `kube-prometheus-stack` release in the `lakebench-observability`
    namespace. Deploy installs it only if no such release exists on the
    cluster, never modifies an existing one, and applies this deployment's
    PodMonitors and dashboard in its own namespace. Destroy never uninstalls
    the shared release.

Run `lakebench validate` to check the operator and StorageClass before
deploying.

### Dependency server failures

The `deps` step fails at once, naming the container and its `LB_DEPS_ERROR`
line, when a resolve exits non-zero; it does not wait out the 900 s. The
common causes and their fixes:

| Message | Fix |
|---|---|
| `missing <coordinate> from <repositories> egress: ...` | The cluster cannot reach Maven Central or PyPI. Set `platform.deps.maven_repository` and `platform.deps.pypi_index` (and `duckdb_extension_repository` for DuckDB) to a mirror |
| `missing ...` without `egress:` | The repository answered but has no such artifact; check the image and table format versions |
| `pvc lb-deps-data has N MiB free` | Delete PVC `lb-deps-data` and re-run deploy, or set `platform.deps.storage_class` |
| `Permission denied` on `/deps` | The PVC's StorageClass ignores `fsGroup`; set `platform.deps.storage_class` to one that honours it, delete the PVC and re-run deploy |
| `serve exited 4: ... hash mismatch` | A file of the set changed on the PVC. Re-run deploy: the pod is replaced and its init containers resolve the set again |
| `volume node affinity conflict` | The PVC's node is gone (an unreplicated StorageClass). Delete PVC `lb-deps-data` and re-run deploy |
| `no StorageClass and the cluster has no default one` | Set `platform.deps.storage_class` |
| `ReplicaSet cannot create its pod ... security context constraint` | On OpenShift the pod runs as UID 185 through the `lakebench-spark-runner` ServiceAccount, which needs the `anyuid` SCC that the Spark RBAC step grants |
| `exited 127 ... python3 is not on <image>` | `images.spark` (and `images.duckdb`) must be images that ship `python3` |

A re-run after a failure replaces a failing `lb-deps` pod, so it starts a
fresh resolve rather than reporting the old pod's error. Delete the PVC with
`kubectl delete pvc lb-deps-data -n <namespace> --wait=false` (a plain delete
waits while the server pod still mounts it); the re-run scales `lb-deps` to zero so the claim can go, then
creates a new one and resolves the set onto it; if a pod that mounts the
claim is stuck Terminating on a lost node, the step says so and the claim
goes once that pod does.

### Command Flags

| Flag | Short | Description |
|---|---|---|
| `--file` | `-f` | Config file (alternative to the positional argument) |
| `--dry-run` | | Show what would be deployed without making changes |
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

Deploy with the full observability stack: set `observability.enabled: true`
in the config, then

```bash
lakebench deploy my-config.yaml --yes
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

With `observability.enabled: true`, Prometheus and Grafana run in
the shared `lakebench-observability` namespace. Grafana's user is `admin`; the
chart generates its password per install, and `deploy` prints the command
that reads it (`kubectl get secret -n lakebench-observability
lakebench-observability-grafana -o jsonpath='{.data.admin-password}' | base64
-d`). The chart shortens service names, so list them:

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
    the PodMonitors, and last the dependency server (the `lb-deps`
    Deployment and Service, the `lb-deps-manifest` and `lb-deps-tools-*`
    ConfigMaps, then the `lb-deps-data` PVC). When the namespace survives it
    also removes the `lakebench.deployment/state-schema` and
    `lakebench.deployment/deps-set` namespace annotations. The list is the
    Category-1 registry in `src/lakebench/deploy/category1.py`; a unit test
    runs deploy and the objects `run` creates against it, and checks every
    template, so an object destroy would leave fails it. With
    `create_namespace: false` one object is kept on purpose: the
    `lakebench-silver-state` ConfigMap, whose silver rebuild-epoch counters
    must not go back while table data written under them may outlive destroy
    (a reset counter makes Delta skip writes as already committed); its
    `bronze_data_clock` is cleared when destroy empties the bronze bucket. The
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
that, which from 1.7 deploy does only with `--force-legacy` and with an
owner marker (`.lakebench/owner.json`); a bucket 1.6 recorded in
`lakebench.deployment/adopted-empty-buckets` without a marker is kept. One that already held objects is left in place
and reported unless you pass `--force-legacy`. A bucket stamped by another
cluster (`lakebench.cluster`), or one that carries this deployment's name but
no cluster stamp and is not in this namespace's record, is always kept. If a recorded bucket cannot be
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
Kubernetes resources are not affected by `clean`. Before it empties a
layer's bucket, `clean` removes that layer's workload tables from the
catalog, so the next run creates them afresh instead of failing on entries
whose files are gone. It uses the deployment's query engine pod, the
configured one first: Trino runs `system.unregister_table` (the catalog
entry only), Spark Thrift runs `DROP TABLE` (for a Delta table only when its
data is in the bucket being emptied). With neither running (DuckDB or no
query engine) it warns and leaves the entries. When a table that still has
its files cannot be unregistered, `clean` leaves that bucket as it is and
exits 1, so a re-run can finish; an entry whose files are already gone is
reported (exit 1) and the bucket is emptied.
