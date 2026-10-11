# Operations

Guide: run Lakebench on a shared cluster: one-time setup, what each command owns, parallel deployments, and cleanup that touches only yours.

- First run on a new cluster: [Getting Started](getting-started.md).
- Deploy and destroy steps in order: [Deployment](deployment.md).

---

## Before the first deploy

**A cluster admin installs the shared pieces once.** Every deployment on the cluster shares the Spark Operator, the Stackable operators (Hive recipes), the scratch StorageClass and the observability stack.

- `deploy` never installs them. It checks they exist and stops with the install command when one is missing.
- The admin installs each with `lakebench admin install --component <name>`. On OpenShift this also grants the Spark Operator's service accounts the `anyuid` SCC.
- [Prerequisites](prerequisites.md) lists every check, generated from the code that runs it.

**Size the cluster with `lakebench plan`, not by estimate.**

- `lakebench plan <config>` prints the components, the minimum cluster and the prerequisites for a config.
- It checks against the current cluster, or without one with `--offline` or `--cores`/`--memory`.
- Its figures come from the same sizing function as the `run` capacity preflight.
- The generated tables in [Sizing](sizing.md) show the defaults per workload, mode and scale.

**Give scratch and metadata different storage classes.**

- When scratch PVCs are on: [Scratch StorageClass](#installing-the-shared-pieces).
- Spark recomputes shuffle and spill on failure, so the scratch class should keep one replica (`platform.storage.scratch.storage_class`, `px-csi-scratch` with `repl=1` on Portworx). More replicas only multiply the storage used.
- PostgreSQL holds the catalog metadata for every table, so give it a replicated class (`platform.compute.postgres.storage_class`, for example `px-csi-db` with `repl=3`).
- See [PostgreSQL](component-postgres.md) and [Spark](component-spark.md#scratch-storage-shuffle-pvcs).

---

## Installing the shared pieces

A cluster admin runs this once per cluster:

```bash
lakebench admin install --component all lakebench.yaml --dry-run   # what it would install
lakebench admin install --component all lakebench.yaml
lakebench admin doctor lakebench.yaml                              # confirm everything is in place
```

`--component all` installs what the config uses:

- the scratch StorageClass, when scratch is on (below)
- the Spark Operator
- the Stackable operators (Hive recipes)
- the observability stack, when `observability.enabled` is set

Name components one at a time with `--component spark-operator` and so on.

Re-running is safe:

- A component that is already installed is left as it is, whatever version
  the config names. It warns when the versions differ.
- On a cluster with everything installed and ready, it changes nothing and
  exits 0.
- The one thing it refreshes is the shared Grafana dashboard ConfigMap, when
  it differs from this Lakebench's.

Developers then run `lakebench deploy`, `run` and `destroy` without
cluster-admin rights.

**The cluster lease.** Every `admin` mutation takes a cluster-wide lease, so
concurrent admins on different workstations do not race each other.

- A deploy or destroy waits for the lease up to 37.5 minutes (2,250 s,
  within its `--timeout`).
- One that waits longer fails, naming the holder, and changes nothing
  shared.
- `lakebench admin --help` shows the full subcommand tree.

**Scratch StorageClass.** Scratch PVCs (Portworx-backed shuffle volumes on OpenShift, for example) are off by default below batch scale 50 and on at and above it.

- Set `platform.storage.scratch.enabled` to choose explicitly. Setting only the class does nothing.
- When scratch is on, the named `StorageClass` must exist before `deploy` runs.
- `deploy` refuses with an error naming the fix rather than creating it: parallel deploys racing to create it could strip it out from under an in-flight run.
- Install it with `lakebench admin install --component scratch-storage-class lakebench.yaml`.

**Spark Operator.** The Kubeflow Spark Operator v2.x (2.5.1 is the current
default) must be installed cluster-wide before any `lakebench deploy`.

- `deploy` adds its own namespace to the operator's `spark.jobNamespaces`
  watch list under the `lakebench-cluster-lock` lease. `destroy` removes it
  again.
- Never edit that list by hand with `helm upgrade --reuse-values`. It skips
  the lease and can drop another deployment's entry.
- `admin install --component spark-operator` takes the lease and runs
  `helm install` (never an upgrade) at the config's
  `platform.compute.spark.operator.version`.
- `--version` naming another version than the installed one is refused: exit 2, or exit 3 with `--allow-version-change`, which lists the
  deployments using it and any deleted namespaces still in its watch list.
- Lakebench does not automate a version change, because `helm upgrade`
  leaves the CRDs the chart ships in `crds/` at the installed version.

**Stackable operators** (Hive recipes only).

- The admin installs all four (commons, listener, secret, hive) once with `lakebench admin install --component stackable lakebench.yaml`.
- The version is the config's `architecture.catalog.hive.operator.version` (SDP 25.7.0 by default).
- The 1.6 key `architecture.catalog.hive.operator.install: true` is refused, and the error names this command.
- Polaris recipes need no catalog operator; Lakebench deploys Polaris directly.

---

## What each command owns

**One config is one deployment.** A deployment is its namespace, everything in it, and the S3 buckets it owns.

- `deploy` creates the namespace and stamps it with the deployment's identity (its name and the cluster's API-server fingerprint) and a per-deploy nonce. It adds the namespace to the Spark Operator's watch list.
- `destroy` re-checks the nonce and the namespace UID before every step. It stops with `Destroy NOT completed` when another deploy has taken the namespace over.
- A destroy racing a redeploy of the same name therefore stops instead of deleting the new deployment. Re-running it is safe.
- Deploy keeps its state in `.lakebench/` beside the config file. Keep the config and that directory together, or a later `run` or `destroy` cannot find the deployment.

**Let `deploy` create the namespace.**

- A namespace you create by hand carries no identity stamp, so `deploy` refuses to adopt it unless you pass `--force-legacy`.
- Run `lakebench deploy <config>` (or `lakebench run <config> --yes`, which deploys first) against a namespace that does not exist yet. It creates, stamps and registers the namespace.
- Where users cannot create namespaces, an admin creates an empty one and the config sets `platform.kubernetes.create_namespace: false`. The first deploy claims it with `--force-legacy`.
- Pass that flag only when the namespace and every bucket the config names are empty and yours. It also claims the config's existing untagged buckets, and `destroy` later empties them.

**`run` checks its inputs before it starts.** Each refusal names the flag or command that resolves it. It refuses when:

- the cluster is too small for the run (the capacity preflight). `deploy` runs the same check without datagen, and warns and goes on when it cannot read the cluster's capacity.
- the deployment's dependency set is not verified.
- on a run that generates, the datagen prefix of the bronze bucket already holds objects. On a bucket the deployment can prove it owns, `--regenerate` clears the prefix and `--skip-generate` reuses it. On one it cannot prove it owns, `--allow-stale-bronze` generates over the objects and records that.

A run that exits 0 still needs reading: the record's verdict and the stage row counts say whether it produced the output it should.

---

## Several deployments on one cluster

**Use one namespace and one config per deployment.** Deployments in different namespaces share only the cluster-wide pieces above.

- Changes to shared mutable state (the Spark Operator's `spark.jobNamespaces` and the observability release) go through one lock.
- The lock is the `lakebench-cluster-lock` ConfigMap in `lakebench-system`. Two deploys or destroys at once cannot lose each other's watch-list entry.

**A deploy or destroy can wait on the lock.**

- One holder keeps it for at most `LEASE_MAX_HOLD_S` (longer for `admin` commands).
- A watch-list change waits for up to three such holders, then gives up with nothing changed.
- A crashed holder is reclaimed when its lease expires (`DEFAULT_TTL_SEC`). That can be later than a waiting deploy gives up; re-run the deploy once `lakebench admin status` shows the lease expired.
- `lakebench admin status` shows who holds the lock. `lakebench admin release-lock` clears a stale one; it is for a cluster admin who has checked the holder is gone.

**Leave headroom.**

- The capacity preflight counts free allocatable capacity when `run` starts, not what other deployments will request later. Deployments started together can each pass and then compete.
- Check the sum of their `lakebench plan` minimums against the cluster before starting them.
- `run --timeout` is per job. It defaults to an hour or two minutes per scale unit, whichever is larger (for AML, longer, and never under 90 minutes).
- A value below the default shortens it. When runs share the cluster, raise it above the default rather than set a small one.

**Stagger deploys.**

- Each deploy resolves its jars and wheels once, in the deployment's dependency server, from Maven Central (with its Google mirror) and PyPI. Many deploys at once can be rate-limited.
- Later runs read the set from the dependency server and fetch nothing from outside.
- On a cluster that runs many, point `platform.deps.maven_repository` and `platform.deps.pypi_index` at a mirror. See [Prerequisites](prerequisites.md#egress-for-the-dependency-resolve).

---

## Cleaning up

**Check where you are pointing before anything destructive.**

- Lakebench pins one kubeconfig context per command (`platform.kubernetes.context`, or the current context when that is empty) and refuses to switch mid-command.
- `destroy` also refuses when the cluster's API server does not match the one the deployment was made on.
- Still confirm the context yourself first (`kubectl config current-context`, and `oc whoami` on OpenShift).

**Destroy with `lakebench destroy <config>`, never `kubectl delete namespace`.**

- Destroy takes the namespace off the operator's watch list under the lock before it deletes it.
- It keeps the namespace when an operator pod still watches it. Deleting a watched namespace crash-loops the operator for every deployment on the cluster.
- Which buckets it empties and deletes: [Bucket ownership](deployment.md#bucket-ownership).
- A `kubectl delete namespace`, or a delete by name prefix, skips the watch-list removal and the ownership checks.
- Step by step: [Deployment](deployment.md#destroy-order).

**Remove data without removing the deployment with `lakebench clean`.** It
empties the silver or gold layer and leaves the infrastructure up. Bronze
regeneration and deleting local evidence are refused. See
[Deployment](deployment.md#selective-cleanup).

---

## Where the output goes

Every command writes under `lakebench-output/` in the working directory.

- `runs/run-<id>/` holds each run's `metrics.json` and `report.html`.
- `journal/` holds the session provenance logs.
- Keep that directory: `report` reads the records in it.
- Layout: [Run records](benchmarking/records.md).
