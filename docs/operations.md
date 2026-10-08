# Operations

How to run Lakebench on a cluster other people use too: what to set up
once, what each command owns, how to run several deployments side by side,
and how to clean up without touching anyone else's. The first run on a new
cluster is walked through in [Getting Started](getting-started.md); the
deploy and destroy steps in order are in [Deployment](deployment.md).

---

## Before the first deploy

**A cluster admin installs the shared pieces once.** The Spark Operator,
the Stackable operators (Hive recipes), the scratch StorageClass and the
observability stack are shared by every deployment on the cluster, and
`deploy` never installs them: it checks they exist and stops with the
install command when one is missing. The admin installs each with
`lakebench admin install --component <name>`, which also grants the Spark
Operator's service accounts the `anyuid` SCC on OpenShift.
[Prerequisites](prerequisites.md) lists every check, generated from the
code that runs it (`src/lakebench/deploy/prereqs.py`,
`src/lakebench/cli/_admin.py`).

**Size the cluster with `lakebench plan`, not by estimate.** `lakebench
plan <config>` prints the components, the minimum cluster and the
prerequisites for a config, against the current cluster or, with
`--offline` or `--cores`/`--memory`, without one. Its figures come from the
same sizing function as the `run` capacity preflight
(`src/lakebench/config/sizing.py:plan_requirements`, called from
`src/lakebench/cli/_plan.py:plan_one`), and the generated tables in
[Getting Started](getting-started.md) show the defaults per workload, mode
and scale.

**Give scratch and metadata different storage classes.** Scratch PVCs are
off by default below batch scale 50 and on at and above it; set
`platform.storage.scratch.enabled` to choose explicitly
(setting only the class does nothing). Spark shuffle and spill are
recomputed on failure, so the scratch class should keep one replica
(`platform.storage.scratch.storage_class`, `px-csi-scratch` with `repl=1`
on Portworx); more replicas only multiply the storage used.
PostgreSQL holds the catalog metadata for every table, so it should get a
replicated class (`platform.compute.postgres.storage_class`, for example
`px-csi-db` with `repl=3`). Code:
`src/lakebench/config/schema.py:ScratchStorageConfig`,
`src/lakebench/config/schema.py:PostgresConfig`; see
[PostgreSQL](component-postgres.md) and
[Spark](component-spark.md#scratch-storage-shuffle-pvcs).

---

## What each command owns

**One config is one deployment.** A deployment is its namespace, everything
in it, and the S3 buckets it owns. `deploy` creates the namespace, stamps
it with the deployment's identity (its name and the cluster's API-server
fingerprint) and a per-deploy nonce, and adds it to the Spark Operator's
watch list. `destroy` re-checks the nonce and the namespace UID before
every step and stops with `Destroy NOT completed` when another deploy has
taken the namespace over, so a destroy racing a redeploy of the same name
stops instead of deleting the new deployment; re-running it is safe. Deploy
also keeps its state in `.lakebench/` beside the config file; keep the
config and that directory together, or a later `run` or `destroy` cannot
find the deployment. Code:
`src/lakebench/deploy/ownership.py:stamp_namespace`,
`src/lakebench/deploy/ownership.py:write_deploy_nonce`,
`src/lakebench/deploy/destroy.py:_check_same_namespace`,
`src/lakebench/config/deploy_state.py`; the model is in
[Namespace isolation](design/namespace-isolation.md).

**Let `deploy` create the namespace.** A namespace you create by hand
carries no identity stamp, so `deploy` refuses to adopt it unless you pass
`--force-legacy`. Run `lakebench deploy <config>` (or `lakebench run
<config> --yes`, which deploys first) against a namespace that does not
exist yet, and let it create, stamp and register the namespace. Where users
cannot create namespaces, an admin creates an empty one, the config sets
`platform.kubernetes.create_namespace: false`, and the first deploy claims
it with `--force-legacy`. Pass that flag only when the namespace and every
bucket the config names are empty and yours: it also claims the config's
existing untagged buckets, and `destroy` later empties them. Code:
`src/lakebench/deploy/ownership.py:stamp_namespace`,
`src/lakebench/modules/pipeline_engines/spark/operator.py:SparkOperatorManager`.

**`run` checks its inputs before it starts.** It refuses when the cluster
is too small for the run (the capacity preflight; `deploy` runs the same
check without datagen, and warns and goes on when it cannot read the
cluster's capacity), when the deployment's dependency set is not verified,
or, on a run that generates, when the datagen prefix of the bronze bucket
already holds objects. On a bucket the deployment can prove it owns,
`--regenerate` clears the prefix and `--skip-generate` reuses it; on one it
cannot prove it owns, `--allow-stale-bronze` generates over the objects and
records that. Each
refusal names the flag or command that resolves it. A run that exits 0 still needs reading: the record's verdict
and the stage row counts say whether it produced the output it should.
Code: `src/lakebench/cli/_run.py`, `src/lakebench/metrics/verdict.py:compute_verdict`.

---

## Several deployments on one cluster

**Use one namespace and one config per deployment.** Deployments in
different namespaces share only the cluster-wide pieces above. Changes to
shared mutable state (the Spark Operator's `spark.jobNamespaces` and the
observability release) go through one lock, held in the `lakebench-cluster-lock` ConfigMap in `lakebench-system`,
so two deploys or destroys at once cannot lose each other's watch-list
entry. Code: `src/lakebench/deploy/cluster_lock.py:LOCK_CONFIGMAP_NAME`.

**A deploy or destroy can wait on the lock.** One holder keeps it for at
most `LEASE_MAX_HOLD_S` (longer for `admin` commands), and a watch-list
change waits for up to three such holders before it gives up with nothing
changed. A holder that crashed is reclaimed when its lease expires
(`DEFAULT_TTL_SEC`), which can be later than a waiting deploy gives up; re-run
the deploy once `lakebench admin status` shows the lease expired.
`lakebench admin status` shows who holds the lock;
`lakebench admin release-lock` clears a stale one and is for a cluster
admin who has checked the holder is gone. Code:
`src/lakebench/deploy/cluster_lock.py:LEASE_MAX_HOLD_S`,
`src/lakebench/deploy/cluster_lock.py:DEFAULT_TTL_SEC`,
`src/lakebench/modules/pipeline_engines/spark/operator.py:_WATCH_LIST_LOCK_TIMEOUT_S`.

**Leave headroom.** The capacity preflight counts the cluster's free
allocatable capacity when `run` starts, not what other deployments will
request later, so deployments started together can each pass and then
compete. Check the sum of their `lakebench plan` minimums against the
cluster before starting them. `run --timeout` is per job and defaults to
an hour or two minutes per scale unit, whichever is larger (for AML,
longer, and never under 90 minutes); a value
below that shortens it, so when runs
share the cluster raise it above the default rather than set a small one.
Code: `src/lakebench/cli/_run.py`, `src/lakebench/cli/_deploy.py`.

**Stagger deploys.** Each deploy resolves its jars and wheels from Maven
Central (with its Google mirror) and PyPI once, in the deployment's
dependency server; many deploys at once can be rate-limited. Later runs
read the set from the dependency server and fetch nothing from outside.
Point `platform.deps.maven_repository` and `platform.deps.pypi_index` at a
mirror on a cluster that runs many. See
[Prerequisites](prerequisites.md#egress-for-the-dependency-resolve) and
`src/lakebench/deps/request.py`.

---

## Cleaning up

**Check where you are pointing before anything destructive.** Lakebench
pins one kubeconfig context per command (`platform.kubernetes.context`, or
the current context when that is empty) and refuses to switch mid-command;
`destroy` also refuses when the cluster's API server does not match the one
the deployment was made on. Still confirm the context yourself first
(`kubectl config current-context`, and `oc whoami` on OpenShift). Code:
`src/lakebench/k8s/target.py:ClusterTarget`.

**Destroy with `lakebench destroy <config>`, never `kubectl delete
namespace`.** Destroy takes the namespace off the operator's watch list
under the lock before it deletes it, and keeps the namespace when an
operator pod still watches it, because deleting a watched namespace
crash-loops the operator for every deployment on the cluster. It also
empties every bucket it can prove the deployment owns, including a bucket
the config names with `create_buckets: false`, and deletes only the ones
deploy created; a config pointed at a shared bucket will have that bucket
emptied. A `kubectl delete namespace`, or a delete by name prefix, skips
the watch-list removal and the ownership checks. What destroy does, step by
step, is in [Deployment](deployment.md#destroy-order).
Code: `src/lakebench/deploy/destroy.py:destroy_all`,
`src/lakebench/deploy/destroy.py:_await_operator_unwatch`,
`src/lakebench/deploy/destroy.py:_delete_owned_buckets`.

**Remove data without removing the deployment with `lakebench clean`.** It
empties the silver or gold layer and leaves the infrastructure up. Bronze
regeneration and deleting local evidence are refused (`src/lakebench/cli/_clean.py`). See
[Deployment](deployment.md#selective-cleanup).

---

## Where the output goes

Every command writes under `lakebench-output/` in the working directory:
`runs/run-<id>/` holds each run's `metrics.json` and `report.html`, and
`journal/` the session provenance logs. Keep that directory: `report`
and `reproduce` read the records in it. The root is
`src/lakebench/_constants.py:DEFAULT_OUTPUT_DIR`; the layout is described in
[Benchmarking](benchmarking.md).
