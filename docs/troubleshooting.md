# Troubleshooting

Common issues when deploying and running Lakebench on Kubernetes (OpenShift
or vanilla), grouped by area and listed by symptom. Each entry ends with the
code or test that implements the behaviour it describes, so you can check it
against the version you run. Maintainer background (why the code is the way
it is) is in [Internals](internals.md).

---

## Config and versions

### A config is refused with "'...' was removed"

**Symptom:** `deploy`, `generate` or `run` stops at config load with
`'hive' was removed: ... Delete it from the config (destroy, status and the
read-only commands still load it)`, or the same for `prometheus`, `grafana`
or another key.

**Cause:** the key once existed and no longer does anything. `images.hive`
never chose the Hive that runs (the Stackable HiveCluster always runs Hive
3.1.3); `images.prometheus` and `images.grafana` were never read, because
Prometheus and Grafana come from the kube-prometheus-stack chart. A removed
key still at its old default is dropped with a note; any other value is
refused by the commands that change data, so a config that seems to choose
something cannot run as if it did.

**Fix:** delete the key. To pin Prometheus and Grafana, set
`observability.chart_version`. `destroy`, `status` and the read-only
commands still load the old config, so you can tear down or inspect a
deployment made with it.

Code: `src/lakebench/config/schema.py:ImagesConfig`,
`src/lakebench/config/schema.py:STACKABLE_HIVE_VERSION`.

### "Unsupported component combination"

**Symptom:** config load fails with `Unsupported component combination:
catalog=..., table_format=..., engine=..., query_engine=...`, followed by a
`Why:` line.

**Cause:** the four components cannot produce correct results together, so
Lakebench refuses them instead of running a comparison it cannot trust
(DuckDB with Delta, Polaris with Delta, and every Unity combination among
them). The `Why:` line, where there is one, gives the reason, and a
`Closest supported:` line names the nearest combination that works. The
[Compatibility Matrix](compatibility-matrix.md#excluded-combinations) lists
every exclusion with its reason.

**Fix:** pick a combination from [Recipes](recipes.md) or the
compatibility matrix, or take the one the `Closest supported:` line names.

Code: `src/lakebench/config/schema.py:_SUPPORTED_COMBINATIONS`,
`src/lakebench/config/schema.py:_COMBINATION_NOTES`.

### "Unsupported Spark version 4.2"

**Symptom:** config load fails with `Unsupported Spark version 4.2 in image
'...'. Tested versions: 3.5.x ..., 4.0.x ..., 4.1.x ...`.

**Cause:** no Iceberg release (as of 2026-09) ships a Spark 4.2 runtime, and borrowing the
4.1 runtime fails at the first table write with
`IncompatibleClassChangeError`. Only the Spark minors in the list are
accepted.

**Fix:** use a Spark 4.1, 4.0 or 3.5 image.

Code: `src/lakebench/modules/pipeline_engines/spark/job.py:_SUPPORTED_SPARK_VERSIONS`.

### "Iceberg 1.11.0 requires Java 17"

**Symptom:** config load fails with `Iceberg 1.11.0 requires Java 17, but
the Spark image '...' ships Java 11`, or loading a Spark 3.5 config logs
that it is using Iceberg 1.10.1 instead of the default.

**Cause:** Iceberg 1.11 jars are built for Java 17, and the plain
`apache/spark:3.5.x-python3` images ship Java 11. On such an image Lakebench
picks Iceberg 1.10.1 when you did not choose a version, and refuses when you
did (otherwise the job would fail inside the driver with
`UnsupportedClassVersionError`). Spark 4.x images ship Java 17.

**Fix:** use a java17 tag such as `apache/spark:3.5.9-java17-python3`, or
set `table_format.iceberg.version` to 1.10.1.

Code: `src/lakebench/modules/pipeline_engines/spark/job.py:validate_iceberg_java_runtime`,
`src/lakebench/config/schema.py:resolve_format_versions`.

### "Delta 4.0.0 is not compatible with Spark 4.1"

**Symptom:** config load fails with `Delta 4.0.0 is not compatible with
Spark 4.1. Compatible versions: 4.1.0.` (or the same for another pair).

**Cause:** each Delta line is built for one Spark minor: Delta 4.0 for Spark
4.0, Delta 4.1 for Spark 4.1. Delta does not run on Spark 3.5 at all.

**Fix:** leave `table_format.delta.version` at `auto` (the default), which
picks the Delta that matches the Spark image, or set the compatible version
the message names.

Code: `src/lakebench/modules/pipeline_engines/spark/job.py:_FORMAT_VERSION_COMPAT`,
`src/lakebench/modules/pipeline_engines/spark/job.py:resolve_format_version`.

### A Maven artifact seems not to exist

**Symptom:** while choosing a version to pin, a Maven Central search returns
nothing for an artifact Lakebench builds, such as `delta-spark_4.1_2.13`.

**Cause:** the search index lags and misses some artifacts. The artifact is
real if its POM downloads.

**Fix:** fetch the POM and check the status code:

```bash
curl -s -o /dev/null -w "%{http_code}\n" \
  https://repo1.maven.org/maven2/io/delta/delta-spark_4.1_2.13/4.1.0/delta-spark_4.1_2.13-4.1.0.pom
# 200 = exists
```

Code: `src/lakebench/modules/pipeline_engines/spark/job.py:_delta_spark_artifact`,
`src/lakebench/modules/pipeline_engines/spark/job.py:iceberg_runtime_suffix_for`.

---

## Deploy and the cluster

### PostgreSQL PVC stuck in Pending

**Symptom:** after `lakebench deploy`, the `lakebench-postgres-0` pod stays
in `Pending`. Events show `waiting for a volume to be created` or `no
persistent volumes available for this claim`, and deploy then fails with
`PostgreSQL StatefulSet not ready`.

**Cause:** the cluster has no default StorageClass. The PostgreSQL claim
(`data-lakebench-postgres-0`) names no `storageClassName` unless you set
one, so Kubernetes looks for the class annotated
`storageclass.kubernetes.io/is-default-class: "true"`. Most managed
distributions have one; self-managed clusters (kubeadm, bare metal) may not.

**Diagnosis:**

```bash
# Look for "(default)" next to one of the class names
kubectl get storageclass
kubectl get pvc data-lakebench-postgres-0 -n <namespace>
```

**Fix:** either mark a default StorageClass for the cluster, or set the
PostgreSQL class in your config:

```yaml
platform:
  compute:
    postgres:
      storage_class: "your-storage-class"   # e.g. local-path, thin-csi, gp3
```

A claim's class cannot change once it exists, and Kubernetes refuses a new
volume claim template on an existing StatefulSet. So after changing
`storage_class`, run `lakebench destroy <config>` and then `lakebench deploy
<config>`. On Kubernetes 1.26 and later, marking a default class instead
lets the Pending claim bind without a redeploy.

Code: `src/lakebench/config/schema.py:PostgresConfig`,
`src/lakebench/deploy/deployment_secrets.py:POSTGRES_PVC`,
`src/lakebench/templates/postgres/statefulset.yaml.j2`.

### OpenShift SCC permission denied

**Symptom:** Spark or PostgreSQL pods fail with `CreateContainerError` or
permission-denied errors on OpenShift, or deploy fails with `cannot grant
SCC anyuid to SA ... in namespace ...`.

**Cause:** Spark pods run as UID 185 (the `spark` user of the
`apache/spark` images) and PostgreSQL as UID 999. OpenShift's default
`restricted` SCC allows neither, so both service accounts need the `anyuid`
SCC.

**Fix:** `lakebench deploy` detects OpenShift and grants `anyuid` to the
`lakebench-spark-runner` and `lakebench-postgres` service accounts through
the Kubernetes API, and fails the step if the grant is refused. The Spark
Operator's own service accounts get the same grant from `lakebench admin
install --component spark-operator`. If the deploying user cannot make the
grant, a cluster admin runs the command the error prints:

```bash
oc adm policy add-scc-to-user anyuid -z lakebench-spark-runner -n <namespace>
oc adm policy add-scc-to-user anyuid -z lakebench-postgres -n <namespace>
```

Verify the binding. On OpenShift 4.10 and later the grant is a namespaced
RoleBinding, not an entry in the SCC's `.users` list (which stays empty):

```bash
oc get rolebinding system:openshift:scc:anyuid -n <namespace> -o yaml
```

Code: `src/lakebench/k8s/security.py:ensure_scc_rolebinding`,
`src/lakebench/deploy/postgres.py:PostgresDeployer`.

### DuckDB pod never becomes ready

**Symptom:** deploy waits on DuckDB and then fails with a not-ready message
that describes the pod's state.

**Cause:** the DuckDB pod installs its Python wheel from the deployment's
dependency server in an init container, then a startup probe allows about
five minutes for the import and the Iceberg and httpfs extension load. Under
a busy cluster that can take most of the window. On OpenShift the container
runs as a non-root UID, so `HOME` is set to `/tmp` for both containers.

**Fix:** check the init container's log first (`kubectl logs -n <namespace>
deploy/lakebench-duckdb -c lb-deps-fetch`); a failure there usually means
the dependency server was not ready, and re-running `lakebench deploy`
retries. Deploy waits up to 900 s for DuckDB, within the overall deploy
deadline.

Code: `src/lakebench/templates/duckdb/deployment.yaml.j2`,
`src/lakebench/modules/query_engines/duckdb/deployer.py:DuckDBDeployer`.

---

## Spark Operator and Spark jobs

### SparkApplication stuck with no status

**Symptom:** after `lakebench run`, the SparkApplication exists in the
namespace but never reaches `SUBMITTED` or `RUNNING`. The `STATUS` column
is blank, no driver pod is created, and the job eventually times out.

**Cause:** the Spark Operator is not watching the namespace. The operator's
`spark.jobNamespaces` Helm value lists the namespaces it reconciles; it
ignores SparkApplications anywhere else.

**Diagnosis:**

```bash
# What the operator watches
helm get values spark-operator -n spark-operator --all -o json | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('spark',{}).get('jobNamespaces','all'))"

# Controller logs, for RBAC errors
kubectl logs -n spark-operator -l app.kubernetes.io/component=controller --tail=20
```

**Fix:** re-run `lakebench deploy <config>`. Deploy adds the namespace to
`spark.jobNamespaces`, and `lakebench run` re-adds it before submitting
jobs. Both first take the cluster lock, a lease held in the
`lakebench-cluster-lock` ConfigMap in `lakebench-system`, so a concurrent
deploy or destroy of another deployment cannot lose its entry.
`lakebench validate` reports a missing entry as a warning (or as expected
before the first deploy), and fails when your credentials cannot edit the
operator release at all.

If the lock is held, `lakebench admin status` shows who holds it. If the
watch list still names namespaces that no longer exist, run `lakebench
admin repair-operator` (with `--dry-run` first) and then deploy again. When
deploy or run says the watch list "could not be read", they stop rather
than submit jobs the operator may never reconcile; check `helm status
spark-operator -n spark-operator` and the operator pods.

Do not edit `spark.jobNamespaces` with `helm upgrade --reuse-values` by
hand. That skips the lock, and a list copied from an earlier read silently
drops any namespace another deployment added in the meantime.

Code: `src/lakebench/modules/pipeline_engines/spark/operator.py:SparkOperatorManager`,
`src/lakebench/deploy/cluster_lock.py:LOCK_CONFIGMAP_NAME`.

### Destroy says "operator pods [...] still watch it"

**Symptom:** destroy exits 1 with `Namespace '...' NOT deleted: operator
pods [...] still watch it`.

**Cause:** destroy removed the namespace from the watch list and restarted
the operator, but 120 s later a pod with the old `--namespaces=` list was
still there (usually one still terminating, or one a restart did not
replace; destroy restarts the operator once more for that). Deleting a
watched namespace crash-loops the operator for every deployment, so destroy
keeps the namespace.

**Fix:** check `kubectl get pods -n spark-operator`; once the old pods are
gone, re-run `lakebench destroy`. When the list names a `deployment/...`, an
operator Deployment's pod template still lists the namespace while the Helm
values do not, usually an upgrade that did not apply (check `helm history
spark-operator -n spark-operator`). `lakebench admin repair-operator` reads
the Helm values and the `--namespaces` of the controller and webhook
Deployments, and sets the list with one upgrade to the namespaces any of
them names that still exist; run it, then destroy again. Destroy also keeps
the namespace (exit 1) when the operator restart after the removal fails;
the namespace is already off the list then, so re-run destroy once the
operator pods are Ready.

Code: `src/lakebench/deploy/destroy.py:_await_operator_unwatch`,
`src/lakebench/deploy/destroy.py:_OPERATOR_POD_WAIT_S`.

### Ctrl-C does not stop the command at once

**Symptom:** after Ctrl-C the command prints `interrupt received while
holding the cluster lease; finishing the shared change` and keeps running.

**Cause:** the command holds the cluster lock and is part way through a
shared change. It finishes that change first (its hold budget is 750 s, or
1800 s for `admin` commands), then releases the lock and stops.

**Fix:** wait, or press Ctrl-C twice more to abort at once; the lock is
still released. If a Helm upgrade was running, run `helm history
spark-operator -n spark-operator`: a `pending-upgrade` revision blocks every
deployment's watch-list change. `lakebench admin repair-operator` rolls it
back once the pending revision is at least 10 minutes old by the API
server's clock (a Helm call may still be running before that), to the
newest deployed revision that watches no deleted namespace (an operator that
watches every namespace only to a revision that does too), then sets the
list it read before the rollback, so a namespace the interrupted upgrade
added is kept. Otherwise it exits 3 with the reason; `--dry-run` shows the
verdict. Do not run `helm rollback` by hand: it skips the deleted-namespace
check and the lock. The one exception is when every earlier revision names
a deleted namespace: the message then gives the `helm rollback` to run,
followed at once by `repair-operator`, with no deploy or destroy running
(the operator restarts in a loop in between).

Code: `src/lakebench/deploy/cluster_lock.py:LEASE_MAX_HOLD_S`,
`src/lakebench/deploy/cluster_lock.py:ADMIN_MAX_HOLD_S`,
`src/lakebench/cli/_admin.py:_PENDING_STALE_S`.

### An interrupted `run` left a job running

**Symptom:** after an interrupted `run`, a SparkApplication or datagen Job
is still in the namespace, and the run printed a `kubectl delete` line for
it.

**Cause:** `run` deletes the jobs it created when it is interrupted, but
leaves any it cannot show to be its own (another invocation recreated it,
or the interrupt landed while it was being created) or could not reach
within its cleanup budget of about 60 s. The record's `interrupted.left`
lists each with the reason.

**Fix:** run the printed `kubectl delete`, or let the next `run` handle it:
a later `run` that submits the same stage, or deploys datagen, deletes the
left object by name first.

Code: `src/lakebench/cli/_interrupt.py:CLEANUP_DEADLINE_S`.

### Stages slow at random, SUBMISSION_FAILED, Spark Operator controller evicted

**Symptom:** a stage that usually takes about a minute takes two or more,
and `lakebench run` prints `submission attempt N failed` before the stage
ends. The stage line reads `completed in 150.0s (includes 60s waiting on 1
failed operator submission)`, and metrics.json records each failure under
the stage's `submission_failures` with `submission_retry_seconds` as the
total. The SparkApplication's status shows `SUBMISSION_FAILED` with a Maven
message such as `Downloaded file size (0) doesn't match expected Content
Length`, or `driver pod already exist`. It affects every deployment on the
cluster, not only yours.

**Cause:** the Spark Operator runs spark-submit inside its controller pod,
and spark-submit resolves `spark.jars.packages` into `/tmp/.ivy2`. The
controller's root filesystem is read-only, so `/tmp` is the chart's `tmp`
emptyDir, which chart 2.5.1 caps at 1Gi. One Spark line's Iceberg or Delta
runtime, hadoop-aws and AWS SDK bundle come to about 1.2 GB. The kubelet
evicts the controller (`Usage of EmptyDir volume "tmp" exceeds the limit
"1Gi"`); the next leader starts with an empty cache and retries any
submission the old one had in flight about 60 s later.

**Diagnosis:** `lakebench admin doctor` (or `lakebench admin status`)
reports the controller's `/tmp` size limit and any storage evictions still
on record (evicted pods and events last about an hour).

**Fix (cluster admin):** `lakebench admin repair-operator --dry-run`, then
`lakebench admin repair-operator`. When the controller's `/tmp` is smaller
than 8Gi it raises it to 8Gi under the cluster lock with `--reuse-values`,
keeps the installed chart version, and rolls the controller. The same run
also reconciles the watch list (dropping deleted or Terminating
namespaces), which restarts the operator before the resize when it changes.
`lakebench admin install --component spark-operator` sets the same size on
a fresh install (`--controller-tmp-size` chooses another). The size is
stored in the release's values, so later watch-list edits carry it forward.

Since 1.7 the jobs name their jars as URLs on the deployment's dependency
server (`spark.jars`) and set no `spark.jars.packages`, so the controller
downloads nothing for a 1.7 deployment; the larger `/tmp` still protects the
controller from applications other deployments submit with packages.

Code: `src/lakebench/modules/pipeline_engines/spark/operator_scratch.py:DEFAULT_CONTROLLER_TMP_SIZE`,
`src/lakebench/cli/_run.py:_retry_note`.

### A stage fails with "dependency server does not serve this set"

**Symptom:** a Spark stage fails with `dependency server does not serve
this set (<url> not found); the deployment's set changed since the run
started: re-run deploy, then run`, or says the dependency server is
unreachable.

**Cause:** the deployment's dependency set changed after the run started (a
redeploy with another image, format version or mirror), or the `lb-deps`
pod was restarting when the driver started.

**Fix:** let `lakebench deploy <config>` finish, then run again. `run`
checks the set before it starts and refuses (exit 3 or 4) when it is not
verified.

Code: `src/lakebench/modules/pipeline_engines/spark/monitor.py:classify_dependency_failure`,
`src/lakebench/deps/runtime.py:load_handle`.

### Spark Operator volume mounting fails

**Symptom:** Spark driver or executor pods fail to start. Events show
errors about ConfigMap volumes or missing volume mounts, or the Spark
scripts are not found at the expected path inside the container.

**Cause:** an unsupported Spark Operator: Lakebench needs the Kubeflow
Spark Operator v2.x line, and puts its volumes in the driver and executor
pod templates (see [Internals](internals.md#spark-operator) for why).

**Fix:** use Kubeflow Spark Operator v2.x (2.5.1 is the default), installed
with `lakebench admin install --component spark-operator`.

```bash
kubectl get deployment -A | grep spark-operator
```

Code: `src/lakebench/modules/pipeline_engines/spark/job.py:_build_manifest`.

### S3A requests fail with a 400 and a null message

**Symptom:** a Spark stage fails with `AWSBadRequestException ... Status
Code: 400` and a null message and request ID. The object store's own log
shows `Authorization header malformed, unexpected scope` or similar.

**Cause:** the store checks the region in the signature. S3A signs with
`fs.s3a.endpoint.region`, which Lakebench sets on every job from
`platform.storage.s3.region` (default `us-east-1`). A store that expects
another region rejects the request; FlashBlade accepts any region, so the
mismatch is invisible there.

**Fix:** set `platform.storage.s3.region` to the region your store expects.
`lakebench config storage <config>` reports whether the backend is
region-strict.

Code: `src/lakebench/modules/pipeline_engines/spark/job.py:_build_manifest`,
`src/lakebench/config/schema.py:S3Config`.

### "No space left on device" on silver-build

**Symptom:** the silver-build (or gold-finalize) job fails with executor
errors reporting no space left on device. Pods may be evicted.

**Cause:** shuffle and spill outgrew the executors' local storage.
Silver-build is the most storage-hungry stage: it joins and aggregates the
whole interaction corpus. With `platform.storage.scratch.enabled: true`
each executor gets its own scratch PVC sized by the job profile; without it
(the default) spill goes to Spark's default local directories on the
node's ephemeral storage, which a large scale can fill. Data per executor
does not stay constant: executor counts are fixed up to scale 10 and capped
at 28 above it, so per-executor spill grows with scale inside those ranges.

**Fix:** enable scratch storage on a class with one replica (for Portworx,
`px-csi-scratch` with `repl=1`; shuffle data is recomputed on failure, so
more replicas only double the storage used):

```yaml
platform:
  storage:
    scratch:
      enabled: true
      storage_class: px-csi-scratch
```

The per-executor scratch size comes from the job profile and should not be
reduced; see [Job Profiles](component-spark.md#job-profiles). Check the
cluster can hold the scratch total in the generated sizing table in
[Getting Started](getting-started.md).

Code: `src/lakebench/modules/pipeline_engines/spark/job.py:_JOB_PROFILES`,
`src/lakebench/config/schema.py:ScratchStorageConfig`.

---

## Object storage

### FlashBlade shows objects after bucket cleanup

**Symptom:** after `lakebench destroy`, the FlashBlade UI still shows a
non-zero object count in a bucket, though `list_objects_v2` returns no keys.

**Cause:** FlashBlade garbage-collects aborted multipart uploads
asynchronously, and the UI can count them for a while.

**Fix:** destroy handles this. Its bucket cleanup deletes every object,
aborts every in-progress multipart upload, then re-checks both listings
every few seconds until both are empty. Wait a few minutes for the
asynchronous collection. If the listings are still not empty after 300 s,
destroy fails with `Investigate before considering this bucket clean`
instead of reporting a clean bucket; look at what is left before re-running
it. Destroy only empties buckets the deployment created and owns.

Code: `src/lakebench/s3/client.py:S3Client`,
`src/lakebench/deploy/destroy.py:_delete_owned_buckets`.

---

## Catalogs

### Hive Metastore DNS resolution fails

**Symptom:** Spark jobs or Trino fail to connect to the Hive Metastore,
with DNS resolution failures or connection refused on port 9083.

**Cause:** the metastore pod is not running. `lakebench-hive-metastore` is
a ClusterIP Service that Lakebench creates itself (not one the Stackable
operator generates). It selects the metastore pods of the `lakebench-hive`
HiveCluster (`app.kubernetes.io/instance=lakebench-hive`,
`app.kubernetes.io/component=metastore`), so it has no endpoints until
those pods are ready.

**Expected DNS name:**

```
lakebench-hive-metastore.<namespace>.svc.cluster.local:9083
```

**Diagnosis:**

```bash
lakebench status <config>
kubectl get hivecluster -n <namespace>
kubectl get pods -n <namespace> -l app.kubernetes.io/name=hive,app.kubernetes.io/instance=lakebench-hive
```

If the HiveCluster is not ready, check its events and PostgreSQL: the
metastore cannot start until PostgreSQL is healthy. When the Stackable
operators themselves are missing, deploy says so and names `lakebench admin
install --component stackable`.

Code: `src/lakebench/modules/catalogs/hive/deployer.py:HiveDeployer`,
`src/lakebench/templates/hive/service.yaml.j2`.

### Polaris bootstrap Job fails or times out

**Symptom:** deploy fails with `Polaris bootstrap job did not complete in
time`. (Older versions failed with `IllegalArgumentException: already been
bootstrapped`.)

**Cause:** the bootstrap Job could not finish: usually PostgreSQL or the
Polaris server was not ready, or the Job failed three times. The message is
the same for a failed Job and for a timeout. A realm that was bootstrapped
before is not the cause: the Job's script treats `already been
bootstrapped` as success and prints `Realm already bootstrapped (OK)`, and
deploy deletes the old Job before creating a new one (Kubernetes Jobs are
immutable).

**Fix:** read the Job's log (`kubectl logs -n <namespace>
job/lakebench-polaris-bootstrap`), fix what it reports, and re-run
`lakebench deploy <config>`; no destroy is needed.

Code: `src/lakebench/modules/catalogs/polaris/deployer.py:PolarisDeployer`,
`src/lakebench/templates/polaris/bootstrap-job.yaml.j2`.

### Polaris credential vending fails

**Symptom:** Spark jobs connecting to Polaris fail with STS or
credential-vending errors, or Polaris's server-side S3 access goes to
`s3.amazonaws.com` instead of your endpoint.

**Cause:** FlashBlade and most on-premises stores have no STS, so Polaris
cannot vend temporary credentials. Releases before 1.3.0 could also try STS
in one code path when told not to (the upstream report is
[apache/polaris#379](https://github.com/apache/polaris/issues/379)).

**Fix:** use the default Polaris (1.6.0) or any release from
1.3.0-incubating on. (Polaris dropped the `-incubating` suffix at 1.4.0, so
`apache/polaris:1.3.0` does not exist but `1.6.0` does.) The STS skip is per
catalog: the bootstrap Job creates the catalog with `stsUnavailable: true`
and `pathStyleAccess: true`, and the server gets static S3 credentials in
its ConfigMap. Do not add a server-wide credential-subscoping override: it
drops the endpoint and path-style settings, and Polaris's server-side
S3FileIO then falls back to `s3.amazonaws.com`.

Code: `src/lakebench/templates/polaris/bootstrap-job.yaml.j2`,
`src/lakebench/templates/polaris/configmap.yaml.j2`,
`src/lakebench/config/schema.py:ImagesConfig`.

### Hive or Polaris cannot log in to PostgreSQL

**Symptom:** the metastore or Polaris logs `password authentication
failed` for its role.

**Cause:** PostgreSQL uses SCRAM-SHA-256 authentication, and each role's
password comes from the deployment's Secret. A role whose stored password
no longer matches the Secret (an older deployment, or a manual change)
cannot log in.

**Fix:** re-run `lakebench deploy <config>`. Deploy re-syncs the `hive` and
`polaris` role passwords from the Secret with SCRAM verifiers, so the
plaintext password never reaches psql.

Code: `src/lakebench/deploy/deployment_secrets.py:sync_role_password`,
`src/lakebench/templates/postgres/statefulset.yaml.j2`.

---

## Query engines

### Trino queries fail with Polaris: "scope not valid"

**Symptom:** Trino queries against Iceberg tables fail with OAuth2 errors;
Polaris rejects the token request because the scope is invalid.

**Cause:** Trino's Iceberg REST client asks for `scope=catalog` by default,
and Polaris requires `scope=PRINCIPAL_ROLE:ALL`.

**Fix:** Lakebench sets `iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL`
in the Trino catalog when the catalog is Polaris. If you configure Trino by
hand, add the property. It exists from Trino 454
([trinodb/trino#22961](https://github.com/trinodb/trino/pull/22961));
Lakebench's default Trino has it; if you pin another image, see the
version constraints in [Trino](component-trino.md).

Code: `src/lakebench/templates/trino/configmap.yaml.j2`,
`src/lakebench/config/schema.py:ImagesConfig`.

### Trino coordinator stays in Init

**Symptom:** the Trino coordinator pod stays in `Init` and the deploy waits
on Trino.

**Cause:** its init container waits until the catalog answers on its port:
Polaris for a Polaris recipe, the Hive Metastore (9083) otherwise. Spark
Thrift has the same wait. The catalog is not up yet.

**Fix:** look at the catalog first (see the Hive and Polaris entries
above); Trino starts once the catalog does.

Code: `src/lakebench/templates/trino/coordinator.yaml.j2`,
`src/lakebench/templates/spark-thrift/sparkapplication.yaml.j2`.

### Trino metrics show only JVM metrics

**Symptom:** with observability on, Prometheus has JVM metrics for Trino
but no `trino_execution_*` series.

**Cause:** the JMX exporter emits whitelisted beans only through a rule;
with no rules it exports only the JVM defaults.

**Fix:** Lakebench's Trino ConfigMap carries a catch-all rule
(`pattern: ".*"`). If you replace the exporter config, keep one.

Code: `src/lakebench/templates/trino/configmap.yaml.j2`.

---

## Table formats

### Delta: a MIN or MAX query fails with ClassCastException

**Symptom:** on a Delta recipe with Spark Thrift or in a Spark job, a query
using `MIN` or `MAX` of `interaction_date` (Q2 among them) fails with
`ClassCastException: java.time.LocalDate cannot be cast to java.sql.Date`.

**Cause:** delta-spark answers MIN/MAX on a date partition column from its
metadata, and on Delta 4.0 that path threw this error in Lakebench's own
runs (the upstream report closest to it is
[delta-io/delta#4201](https://github.com/delta-io/delta/issues/4201)).
Trino uses its own Delta connector and is not affected.

**Fix:** Lakebench sets `spark.databricks.delta.optimizeMetadataQuery.enabled=false`
for Delta in the Spark job conf and the Spark Thrift template, so the query
scans instead. If you see the error, a conf override has removed that
setting.

Code: `src/lakebench/modules/pipeline_engines/spark/job.py:_build_manifest`,
`src/lakebench/templates/spark-thrift/sparkapplication.yaml.j2`.

### Iceberg compaction fails on open writers or per-node memory

**Symptom:** a run's maintenance outcomes show compaction partial, and the
log or `effective_maintenance.reasons` has "compaction failed on <table>"
with "Exceeded limit of 100 open writers for partitions" or "Query
exceeded per-node memory limit of ... [TableWriterOperator=...]".

**Cause:** one Trino `optimize` rewrote too many partitions at once. Each
partition keeps an open Parquet writer; Trino caps the partitions one
writer may open at 100, and Lakebench sets a query's memory per node to
35% of the worker heap.
Continuous runs leave small files in every partition, so a long window
makes every partition a rewrite: Customer 360 silver has a partition per
day, and the AML silver `transactions` and `account_statements`
tables a partition per month, about 160 MB of writer memory each at scale
1.

**Fix:** none needed on current code: these tables are compacted in
chunks (90 days, or one month with files to merge, per statement). If it
still appears, check the record's `detail.compaction_statements` and
`reasons`: a failed table compacted by one statement without a `WHERE`
means the partition read failed (a reason says "partition read failed")
or the table was renamed in `architecture.tables`, which the chunking does
not follow. The month chunking is sized on the single scale-1 Trino
worker; on a failure at a larger scale, report the run.

Code: `src/lakebench/modules/table_formats/iceberg/maintenance.py:build_compaction_plan`,
`src/lakebench/cli/_sustained.py:_compaction_partitions`.

### Delta: no compaction, and VACUUM only on Trino

**Symptom:** a Delta run's record shows compaction skipped, and on Delta
with Spark Thrift no table maintenance at all.

**Cause:** OPTIMIZE rewrites the whole table in one pass and runs Trino
workers and Spark Thrift out of memory, so Lakebench never runs it on
Delta. VACUUM runs on Delta with Trino only; on Spark Thrift it also runs
out of memory, so that recipe skips it. Delta silver is clustered by
`interaction_date` at build time instead of relying on OPTIMIZE. The record
says what was skipped and why, and the maintenance class reflects it.

On Trino, VACUUM below Delta's 7-day minimum retention needs the session
property in the same submission as the call, so Lakebench sends `SET
SESSION <catalog>.vacuum_min_retention = '0s'; CALL
<catalog>.system.vacuum(...)` as one `--execute`; two separate CLI calls
would drop the setting and the VACUUM would fail.

Code: `src/lakebench/cli/_sustained.py:_run_iceberg_maintenance`,
`src/lakebench/modules/table_formats/delta/maintenance.py:build_delta_maintenance_sql`,
`src/lakebench/metrics/maintenance_policy.py:effective_maintenance`.

---

## Results

### Pipeline runs but the benchmark returns 0 rows

**Symptom:** the query benchmark completes but every query returns 0 rows.

**Cause:** the tables are empty or missing. Usually one of:

- silver-build or gold-finalize failed (check their pod logs);
- a `lakebench clean` or `destroy` removed the tables and no pipeline ran
  since;
- the catalog (Hive or Polaris) holds stale metadata pointing at deleted S3
  data.

**Fix:**

1. Check the three Spark jobs completed:

   ```bash
   kubectl get sparkapplications -n <namespace>
   ```

   `lakebench-bronze-verify`, `lakebench-silver-build` and
   `lakebench-gold-finalize` should show `COMPLETED`.

2. Check the tables exist and have rows. The Customer 360 defaults are
   `silver.customer_interactions_enriched` and
   `gold.customer_executive_dashboard`; for AML they are
   `silver.transactions` and `gold.daily_dashboards` (detection writes
   `gold.alerts`).

3. If they are missing or empty, run the pipeline again from fresh data:

   ```bash
   lakebench destroy <config>
   lakebench deploy <config>
   lakebench generate <config> --timeout 14400
   lakebench run <config> --timeout 7200
   ```

Do not skip the generate step: the pipeline needs source data in the bronze
bucket.

Code: `src/lakebench/config/schema.py:TableNamesConfig`,
`src/lakebench/spark/scripts/silver_build_financial.py:SILVER_TRANSACTIONS`,
`src/lakebench/modules/pipeline_engines/spark/job.py:_build_manifest`.
