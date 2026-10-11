# Deployment Guide

Guide: what `deploy`, `status`, `destroy` and `clean` do, step by step.

One YAML config drives every command (see
[Configuration](configuration.md)). Each command's flags and exit codes
are in the [CLI reference](cli-reference.md).

## Prerequisites

- Cluster, CLI tools and S3 store:
  [Getting Started](getting-started.md#prerequisites).
- The cluster checks, each with its fix:
  [Prerequisites](prerequisites.md).
- The shared pieces a cluster admin installs once:
  [Operations](operations.md#before-the-first-deploy).
- S3 credentials that can create and delete buckets and objects.
- A config with `name` and the S3 settings filled in.

## Installing the CLI

| Method | Command | Notes |
|---|---|---|
| pip | `pip install lakebench-k8s` | Or `pipx install lakebench-k8s` for an isolated install |
| Install script | `curl -fsSL https://raw.githubusercontent.com/PureStorage-OpenConnect/lakebench-k8s/main/install.sh \| bash` | Detects OS and architecture |
| Manual download | from [GitHub Releases](https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases) | See below |
| From source | `git clone https://github.com/PureStorage-OpenConnect/lakebench-k8s.git && cd lakebench-k8s && pip install -e ".[dev]"` | |

Check the install with `lakebench version`.

Binaries are single files for Linux (amd64) and macOS (amd64, arm64), and
need no Python. There is no Linux arm64 binary; on that platform install
from PyPI.

What the install script checks:

- It verifies the download against the release's `SHA256SUMS` file and runs
  the downloaded binary's `version` before installing it.
- It installs into `INSTALL_DIR` (default `/usr/local/bin`). Run it with
  `sudo bash`, or set `INSTALL_DIR` to a directory you can write.
- If the download fails, the checksum does not match or the binary does not
  run, it installs nothing and keeps any lakebench already there.
- This catches a corrupted or incomplete download. The checksum file comes
  from the same release, so it does not prove who built the binary.
- `SHA256SUMS` is published from 1.7.0 on. An older release installs
  unverified, and the script says so on stderr. A release at 1.7.0 or later
  without `SHA256SUMS` is refused.

Set `INSTALL_DIR` or `VERSION` on the `bash` side of the pipe, and pass them
through `sudo` with `env`:

```bash
curl -fsSL https://raw.githubusercontent.com/PureStorage-OpenConnect/lakebench-k8s/main/install.sh | INSTALL_DIR="$HOME/.local/bin" bash
curl -fsSL https://raw.githubusercontent.com/PureStorage-OpenConnect/lakebench-k8s/main/install.sh | sudo env VERSION=1.7.0 bash
```

Manual download:

```bash
# Linux
curl -LO https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases/latest/download/lakebench-linux-amd64

# macOS (Apple Silicon)
curl -LO https://github.com/PureStorage-OpenConnect/lakebench-k8s/releases/latest/download/lakebench-macos-arm64

sudo install -m 755 lakebench-* /usr/local/bin/lakebench
```

## Deploying Infrastructure

```bash
lakebench validate my-config.yaml          # checks the operator and StorageClass first
lakebench deploy my-config.yaml            # prompts for confirmation
lakebench deploy my-config.yaml --yes      # no prompt
lakebench deploy my-config.yaml --dry-run  # show the plan, change nothing
```

Deploy runs its steps in a fixed order. Each step needs the one before it.
The first step that fails stops the deploy and reports the error.

With `--local`, the same pipeline runs in podman or docker containers on
this machine, to try a recipe without a cluster.

### Capacity check

Before it creates anything, deploy runs `run`'s read-only capacity check,
without datagen. Details: [The capacity check](sizing.md#the-capacity-check).

### Timeout

`--timeout` (default 3600 s, `0` for none) bounds every wait inside every
step, including the wait for the cluster lease.

- When it runs out, the step fails and names the component and what it was
  waiting for.
- No shared change starts after it: a Spark Operator install or watch-list
  upgrade, or a Stackable or observability install.
- A shared change already started finishes with its rollout and verify.
  That can run several minutes past the timeout while it holds the cluster
  lease.
- Helm calls already running finish first.

### Deployment Order

1. **Namespace** -- Creates the namespace, or reuses one this deployment
   stamped. Any other is refused unless `--force-legacy`. A namespace still
   `Terminating` from an earlier destroy is waited out, then re-created.
2. **Secrets** -- Creates the S3 and PostgreSQL credential secrets from the
   config's `access_key` and `secret_key` (`secret_ref` is not supported).
   With `s3.ca_cert` set, it also creates a CA certificate secret that every
   component uses to verify TLS to an HTTPS endpoint.
3. **Silver state** -- Creates the `lakebench-silver-state` ConfigMap: the
   silver rebuild counters and bronze's data clock. A redeploy keeps the
   existing values.
4. **S3 buckets** -- Creates the bronze, silver and gold buckets, or adopts
   existing ones. It records which buckets this deployment created.
5. **Scratch StorageClass** -- Checks that the StorageClass for Spark
   shuffle volumes exists. Deploy never creates it: it is shared,
   cluster-scoped infrastructure. If it is missing, deploy stops and names
   `lakebench admin install --component scratch-storage-class`, which a
   cluster admin runs once. Skipped when scratch is off (see
   [Scratch StorageClass](operations.md#installing-the-shared-pieces)).
6. **PostgreSQL** -- A StatefulSet, the catalog's metadata backend.
7. **Hive Metastore or Polaris** -- The catalog `architecture.catalog.type`
   selects: a Stackable HiveCluster CRD for `hive`, an Apache Polaris REST
   catalog Deployment for `polaris`. The other is skipped.
8. **Spark RBAC** -- The ServiceAccount, Role and RoleBinding for Spark job
   submission. On OpenShift it also grants the `anyuid` SCC to the service
   account; the PostgreSQL step does the same for `lakebench-postgres`. A
   refused grant fails the step.
9. **Unity Catalog** -- Skipped unless `architecture.catalog.type` is
   `unity`. No shipped recipe uses Unity.
10. **Spark Operator** -- Checks that the shared Spark Operator runs and
    watches the namespace.
    - If it does not, deploy adds the namespace to `spark.jobNamespaces`
      with `helm upgrade`, under the `lakebench-cluster-lock` lease, so
      concurrent deploys do not overwrite each other.
    - Deploy never installs, upgrades or repairs the operator: a missing or
      broken one fails the deploy. A cluster admin installs it once with
      `lakebench admin install --component spark-operator`.
    - The commands that change data refuse the 1.6 key
      `platform.compute.spark.operator.install: true`.
    - With `observability.enabled`, the preflight stops before creating
      anything when the shared observability stack is missing.
11. **Dependency server** -- Starts `lb-deps` (a Deployment, a Service and
    the 5Gi PVC `lb-deps-data`) on the stock Spark image.
    - Its init containers resolve the jars onto the PVC, with a sha256 for
      every file. They also resolve the AML reference wheels, and the DuckDB
      wheel and extensions when DuckDB is the engine.
    - The serving container re-hashes the set at every start, then serves
      it read-only.
    - Deploy waits up to 900 s (within `--timeout`) for it to be Ready. It
      reads the set's manifest from the pod with `kubectl exec`, recomputes
      the set hash from its entries and checks it. It records the hash in
      the ConfigMap `lb-deps-manifest` and the namespace annotation
      `lakebench.deployment/deps-set`.
    - A redeploy with the same config renders the same pod template: the pod
      is not restarted and nothing is resolved again. A changed image, table
      format version, workload, query engine, mirror key or Lakebench
      resolver resolves again.
    - The step reads pods, pod logs, events and the named StorageClass, and
      execs into the server pod.
    - See [Dependency server failures](#dependency-server-failures).
12. **Trino** -- The coordinator (Deployment) and workers (StatefulSet), with
    the connector pointed at the catalog. Skipped unless
    `architecture.query_engine.type` is `trino`.
13. **Spark Thrift Server** -- When the query engine is `spark-thrift`. Its
    init container copies the dependency set from `lb-deps` and checks each
    file's sha256. Deploy waits until the rollout is complete and the pod
    runs this deploy's set (Recreate: one pod at a time). A fetch that cannot
    recover by retrying fails at once.
14. **DuckDB** -- When the query engine is `duckdb`. Its wheel and
    extensions come from the dependency set the same way.
15. **Observability** -- Only with `observability.enabled: true`.
    - Prometheus and Grafana come from one shared `kube-prometheus-stack`
      release in the `lakebench-observability` namespace.
    - A cluster admin installs it once with
      `lakebench admin install --component observability`, which also
      applies the shared Grafana dashboard.
    - Deploy never installs or changes it: a missing release fails the
      step. Deploy applies only this deployment's PodMonitors and
      Pushgateway, in its own namespace.
    - Destroy never uninstalls the shared release.
    - The Pushgateway volume uses StorageClass `px-csi-scratch` unless
      `observability.pushgateway_storage_class` names another; set it on a
      cluster without Portworx.

### Dependency server failures

When a resolve exits non-zero, the `deps` step fails at once and names the
container and its `LB_DEPS_ERROR` line. It does not wait out the 900 s.

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

A re-run after a failure replaces a failing `lb-deps` pod. It starts a
fresh resolve rather than reporting the old pod's error.

To replace the PVC:

1. Run `kubectl delete pvc lb-deps-data -n <namespace> --wait=false`. A
   plain delete waits while the server pod still mounts the claim.
2. Re-run deploy. It scales `lb-deps` to zero so the claim can go, then
   creates a new claim and resolves the set onto it.
3. If a pod that mounts the claim is stuck Terminating on a lost node, the
   step says so. The claim goes once that pod does.

## Checking Status

```bash
lakebench status my-config.yaml
lakebench status --namespace my-lakehouse
```

With a config, `status` reads the Kubernetes API and shows the deployment's
components (PostgreSQL, the configured catalog and query engine) with
replica counts and readiness, and the datagen job's progress while it runs.
With only `--namespace`, it lists every component Lakebench can deploy,
plus the shared Prometheus and Grafana. Exit codes are under
[status](cli-reference.md#status).

## Accessing Monitoring (Observability Stack)

With `observability.enabled: true`, Prometheus and Grafana run in the
shared `lakebench-observability` namespace.

- Grafana's user is `admin`. The chart generates the password per install,
  and `deploy` prints the command that reads it:
  `kubectl get secret -n lakebench-observability lakebench-observability-grafana -o jsonpath='{.data.admin-password}' | base64 -d`.
- The chart shortens service names, so list them first:

```bash
kubectl get svc -n lakebench-observability -l release=lakebench-observability
kubectl port-forward -n lakebench-observability svc/<grafana service> 3000:80
```

The dashboards cover Spark job metrics, Trino query performance, storage
throughput and cluster resource use.

## Destroying Infrastructure

```bash
lakebench destroy my-config.yaml           # prompts for confirmation
lakebench destroy my-config.yaml --force   # no prompt
```

Destroy is irreversible. Flags and exit codes are under
[destroy](cli-reference.md#destroy).

### Destroy Order

1. **Ownership check** -- Refuses to continue if the namespace carries
   another deployment's identity annotations, targets a different cluster,
   or has no lakebench annotations at all (unless `--force-legacy`).
2. **SparkApplications** -- Deletes all running and completed Spark jobs.
3. **Spark pods** -- Force-deletes orphaned driver and executor pods.
4. **Datagen jobs** -- Deletes the datagen batch Jobs and their pods.
5. **Table removal** -- Removes the workload's tables from the catalog and
   never deletes files. Skipped when the engine is DuckDB or `none`.
   - Trino: `CALL <catalog>.system.unregister_table(...)`, not
     `DROP TABLE`. Trino's `DROP TABLE` deletes every file an Iceberg table
     references (datagen files registered with `add_files` included) and a
     managed Delta table's directory. On Polaris, when the namespace is
     being deleted, it runs nothing: the catalog database goes with the
     namespace.
   - Spark Thrift, Iceberg: `DROP TABLE` without `PURGE` removes only the
     catalog entry.
   - Spark Thrift, Delta: `DROP TABLE` deletes the table directory, so it
     runs only when `DESCRIBE DETAIL` puts the table's location in a bucket
     destroy is about to empty, or the table's files are already gone. Any table left registered is listed in the
     summary.
   - Destroy runs no table maintenance: no Iceberg `expire_snapshots` or
     `remove_orphan_files`, no Delta `VACUUM`.
   - Files go only through the bucket step, from buckets destroy proves it
     owns. A later run refuses to create a Delta table over a `_delta_log`
     that is not in the catalog (for example after `--keep-buckets` with
     the namespace deleted): delete that table directory, or use other
     buckets.
6. **S3 buckets** -- Empties the buckets, aborting incomplete multipart
   uploads, then deletes only the buckets this deployment created (see
   [Bucket ownership](#bucket-ownership)).
7. **Observability** -- Leaves the shared kube-prometheus-stack release in
   place, since other deployments may use it. Only a release an older
   lakebench installed into this deployment's own namespace is uninstalled.
8. **Query engine** -- Removes Trino, Spark Thrift Server or DuckDB.
9. **Catalog** -- Removes the HiveCluster or Polaris deployment (or a Unity
   Catalog deployment if the config selected `unity`; no recipe does).
10. **PostgreSQL** -- Removes the StatefulSet, the Service and the data PVC
    (`data-lakebench-postgres-<n>`, found by name), also when
    `create_namespace: false` keeps the namespace.
11. **RBAC and Secrets** -- Removes the Spark ServiceAccount, Role,
    RoleBinding, and the Secrets `lakebench-s3-credentials`,
    `lakebench-postgres-secret`, and `lakebench-ca-certificate` when
    `s3.ca_cert` is set.
12. **Remaining namespaced objects** -- Deletes by name what no step above
    removes:
    - the PostgreSQL ServiceAccount;
    - with observability enabled, the Pushgateway Deployment, Service and
      PVC, the Prometheus ConfigMap and the PodMonitors;
    - last, the dependency server: the `lb-deps` Deployment and Service, the
      `lb-deps-manifest` and `lb-deps-tools-*` ConfigMaps, then the
      `lb-deps-data` PVC;
    - when the namespace survives, the `lakebench.deployment/state-schema`
      and `lakebench.deployment/deps-set` namespace annotations.

    The list is the Category-1 registry in
    `src/lakebench/deploy/category1.py`. A unit test runs deploy and the
    objects `run` creates against it and checks every template, so an
    object destroy would leave fails the test.

    With `create_namespace: false`, two things stay on purpose:
    - The `lakebench-silver-state` ConfigMap. Its silver rebuild-epoch
      counters must not go back while table data written under them may
      outlive destroy: a reset counter makes Delta skip writes as already
      committed. Its `bronze_data_clock` is cleared when destroy empties the
      bronze bucket.
    - The identity annotations (`lakebench.deployment/name` and the rest),
      so a re-run of a destroy that stopped half way still finds its record.
      A deployment with another name cannot deploy into that namespace until
      they are removed.
13. **Namespace** -- Removes the namespace from the Spark Operator watch
    list.
    - Still inside the cluster lease, it waits up to 120 s until no running
      operator pod and no operator Deployment template lists it. A stale pod
      that nothing is replacing gets one more restart of the shared operator.
    - It then deletes the namespace (only when `create_namespace` is true)
      and waits until it is NotFound before reporting it deleted.
    - If something still lists it, the namespace is kept and destroy exits 1.
    - A namespace still terminating at `--namespace-timeout` (default
      600 s) exits 6.
    - If a concurrent destroy of the same deployment finished first and a
      redeploy re-created the name, destroy stops and leaves the new
      deployment alone.

Destroy never deletes the scratch StorageClass. It is shared,
cluster-scoped infrastructure that other deployments on the cluster use.

### Bucket ownership

Destroy deletes only buckets lakebench created.

- Deploy records each bucket it creates in the
  `lakebench.deployment/created-buckets` namespace annotation, plus a
  `lakebench.created` tag where the backend supports tagging.
- Destroy deletes a bucket only when it is in that record and its ownership
  checks out.
- Emptied but kept: pre-provisioned buckets (`create_buckets: false`),
  buckets deploy adopted, buckets emptied under `--force-legacy`, and every
  bucket under `--keep-buckets`.
- Without bucket tagging (FlashBlade), the bucket name is the only other
  ownership evidence. A pre-existing bucket is emptied only when deploy
  found it empty and recorded that. Deploy records that only with
  `--force-legacy` and an owner marker (`.lakebench/owner.json`). A bucket
  listed in `lakebench.deployment/adopted-empty-buckets` (written by 1.6)
  without a marker is kept.
- Emptied means a config pointed at a shared bucket gets that bucket
  emptied.
- A bucket that already held objects is left in place and reported, unless
  you pass `--force-legacy`.
- Always kept: a bucket stamped by another cluster (`lakebench.cluster`),
  and one that carries this deployment's name but no cluster stamp and is
  not in this namespace's record.
- If a recorded bucket cannot be emptied or deleted, destroy keeps the
  namespace. Its annotations are the only ownership record a re-run can use
  to finish the job.

## Selective Cleanup

`clean` deletes a layer's data and leaves the infrastructure up:

```bash
# Empty the silver or the gold layer
lakebench clean silver my-config.yaml
lakebench clean gold my-config.yaml

# Regenerate bronze: the run clears its datagen prefix, then generates
lakebench run my-config.yaml --generate --regenerate
```

- `clean` prompts for confirmation unless `--force` is passed.
- It does not touch Kubernetes resources.
- Without bucket tagging, `clean` (like `destroy` and the continuous reset)
  empties a bucket only when the namespace records creating it or adopting
  it empty.

Refused targets (exit 2, and the refusal echoes no argument):

- `clean bronze` and `clean data` name the `run --generate --regenerate`
  command above (`data` also names `clean silver` and `clean gold`). A run
  regenerates its own corpus, so the corpus a record names is the one it
  read.
- On a bucket the deployment did not create, `--regenerate` refuses too; an
  owner runs `lakebench admin reclaim-bucket` first.
- `clean metrics`, `clean journal` and the old `--metrics-dir`/`-m`: run
  records and journals are evidence, and the CLI does not delete them.

Catalog entries:

- Before it empties a layer's bucket, `clean` removes that layer's workload
  tables from the catalog. The next run then creates them afresh instead of
  failing on entries whose files are gone.
- It uses the deployment's query engine pod, the configured one first.
  Trino runs `system.unregister_table` (the catalog entry only). Spark
  Thrift runs `DROP TABLE`, for a Delta table only when its data is in the
  bucket being emptied.
- With neither running (DuckDB or no query engine), it warns and leaves the
  entries.
- When a table that still has its files cannot be unregistered, `clean`
  leaves that bucket as it is and exits 1, so a re-run can finish.
- An entry whose files are already gone is reported (exit 1), and the
  bucket is emptied.
