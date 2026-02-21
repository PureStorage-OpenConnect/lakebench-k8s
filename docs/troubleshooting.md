# Troubleshooting

Common issues encountered when deploying and running Lakebench on Kubernetes
(OpenShift or vanilla). Organized by symptom.

---

## PostgreSQL PVC stuck in Pending

**Symptom:** After `lakebench deploy`, the `lakebench-postgres-0` pod stays in
`Pending`. Events show `waiting for a volume to be created` or `no persistent
volumes available for this claim`.

**Cause:** The cluster does not have a default StorageClass. PostgreSQL uses
a PVC with no explicit `storageClassName`, which means Kubernetes looks for a
StorageClass annotated with `storageclass.kubernetes.io/is-default-class: "true"`.
Most managed Kubernetes distributions (EKS, GKE, AKS, OpenShift) include one by
default, but self-managed clusters (kubeadm, bare metal) may not.

**Diagnosis:**

```bash
# Check if any StorageClass is marked as default
kubectl get storageclass -o wide
# Look for "(default)" next to one of the class names

# Check the PVC status
kubectl get pvc -n <namespace> -l app=lakebench-postgres
```

**Fix:** Either create a default StorageClass for your cluster, or set the
PostgreSQL storage class explicitly in your config:

```yaml
platform:
  compute:
    postgres:
      storage_class: "your-storage-class"   # e.g., local-path, thin-csi, gp3
```

Then re-run `lakebench deploy`. The deploy command is idempotent -- it picks
up where it left off.

---

## "No space left on device" on silver-build

**Symptom:** The silver-build Spark job fails with executor errors reporting
no space left on device. Pods may be evicted or OOMKilled.

**Cause:** The Portworx PVC allocated per executor is too small for the
silver-build shuffle and spill data. Silver-build is the most storage-intensive
stage in the pipeline because it performs heavy joins and aggregations across
the enriched customer interactions dataset.

**Fix:** Use at least 150Gi per silver executor PVC. This sizing is proven at
1TB+ scale and should not be reduced. The per-executor PVC size does not need
to scale with data volume because adding more executors (driven by the scale
factor) keeps data-per-executor constant.

For the storage class, use `repl=1` (e.g., `px-csi-scratch`) for all Spark
scratch PVCs. Using `repl=2` or higher doubles consumed storage with zero
benefit -- shuffle data is ephemeral and recomputed on failure.

Reference resource profiles:

| Job | Cores | Memory | Overhead | PVC |
|---|---|---|---|---|
| bronze-verify | 2 | 4g | 2g | 50Gi |
| silver-build | 4 | 48g | 12g | 150Gi |
| gold-finalize | 4 | 32g | 8g | 100Gi |

---

## FlashBlade shows objects after bucket cleanup

**Symptom:** After running `lakebench destroy`, the Pure Storage FlashBlade UI
still shows a non-zero object count in S3 buckets, even though
`list_objects_v2` returns no keys.

**Cause:** Multipart upload artifacts are garbage-collected asynchronously by
FlashBlade. After `abort_multipart_upload` calls, the UI may report ghost
object counts that do not correspond to actual listable objects.

**Fix:** `lakebench destroy` handles this automatically. The `empty_bucket()`
method in `s3/client.py` aborts all in-progress multipart uploads and then
enters a retry loop that verifies both `list_objects_v2` and
`list_multipart_uploads` return empty before returning success. If the
FlashBlade UI still shows objects after destroy completes, wait a few minutes
for the async GC to finish. No manual intervention is needed.

---

## Spark Operator volume mounting fails

**Symptom:** Spark driver or executor pods fail to start. Events show errors
related to ConfigMap volume injection, missing volume mounts, or the Spark
scripts not being found at the expected path inside the container.

**Cause:** Spark Operator v1.1.27 has a known bug where ConfigMap volume
injection is broken. Volumes declared in the SparkApplication CR are silently
dropped.

**Fix:** Use Kubeflow Spark Operator v2.4.0 or later. The v2.x line uses
webhook-based pod mutation for volume injection and works correctly. Do not
downgrade to v1.x.

```bash
# Verify your Spark Operator version
kubectl get deployment -A | grep spark-operator
```

---

## SparkApplication stuck with no status

**Symptom:** After `lakebench run`, the SparkApplication CR exists in the
namespace but never transitions to `SUBMITTED` or `RUNNING`. The `STATUS`
column is blank. No driver pods are created. The job eventually times out.

**Cause:** The Spark Operator is not watching the target namespace. The
operator's `spark.jobNamespaces` Helm value controls which namespaces it
monitors. If your deployment namespace is not in that list, the operator
ignores SparkApplications created there.

**Diagnosis:**

```bash
# Check what namespaces the operator is watching
helm get values spark-operator -n spark-operator --all -o json | \
  python3 -c "import sys,json; print(json.load(sys.stdin).get('spark',{}).get('jobNamespaces','all'))"

# Check operator controller logs for RBAC errors
kubectl logs -n spark-operator -l app.kubernetes.io/component=controller --tail=20
```

**Fix:** `lakebench validate` detects this automatically. If
`spark.operator.install: true`, the deploy/run commands auto-add the
namespace and restart the operator. If `install: false` (the default),
validate prints the exact fix command:

```bash
helm upgrade spark-operator spark-operator/spark-operator \
  -n spark-operator --reuse-values \
  --set 'spark.jobNamespaces={default,your-namespace}'
```

After updating, restart the operator controller (it reads jobNamespaces at
startup only):

```bash
kubectl rollout restart deployment/spark-operator-controller -n spark-operator
```

On OpenShift, you may also need to re-patch the deployment to remove
`fsGroup` and `seccompProfile` after any `helm upgrade`.

---

## OpenShift SCC permission denied

**Symptom:** Spark pods fail with `CreateContainerError` or permission denied
errors on OpenShift. Container logs may show UID/GID mismatch errors.

**Cause:** Spark pods run as UID 185 (the `spark` user in the
`apache/spark` base image). OpenShift's default `restricted` SCC does not
allow this UID. The pod needs the `anyuid` Security Context Constraint.

**Fix:** `lakebench deploy` detects OpenShift automatically and binds the
`anyuid` SCC to the `lakebench-spark-runner` service account. If you need to
do this manually:

```bash
oc adm policy add-scc-to-user anyuid \
  -z lakebench-spark-runner \
  -n <namespace>
```

Verify the binding:

```bash
oc get scc anyuid -o json | jq '.users'
```

---

## Polaris: "already been bootstrapped" error

**Symptom:** The Polaris bootstrap Job fails with
`IllegalArgumentException: already been bootstrapped`.

**Cause:** The `polaris-admin-tool bootstrap` command is not idempotent. If
Polaris was previously bootstrapped (even from a failed deployment), running
it again throws this error. Kubernetes Jobs are immutable --
you cannot update an existing Job spec.

**Fix:** Run a full destroy before re-deploying:

```bash
lakebench destroy test-config.yaml --force
lakebench deploy test-config.yaml
```

The deployer handles this by deleting the old bootstrap Job before creating
a new one, and by wrapping the bootstrap call in a shell script that catches
the "already been bootstrapped" error and exits 0.

---

## Polaris: credential vending fails with 1.1.0 or 1.2.0

**Symptom:** Spark jobs connecting to Polaris fail with STS or credential
vending errors. Polaris server logs show failures in `TaskFileIOSupplier`
related to credential subscoping.

**Cause:** Upstream bug
[apache/polaris#379](https://github.com/apache/polaris/issues/379). In Polaris
1.1.0 and 1.2.0, the `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` configuration
is silently ignored by `TaskFileIOSupplier`, causing the server to attempt
STS credential vending. FlashBlade has no STS endpoint, so this always fails.

**Fix:** Use Polaris 1.3.0-incubating or later. The Docker tag includes the
`-incubating` suffix:

```
apache/polaris:1.3.0-incubating
apache/polaris-admin-tool:1.3.0-incubating
```

Note: `apache/polaris:1.3.0` (without the suffix) does not exist. The
Quarkus/SmallRye Config environment variable also requires a double underscore:

```
POLARIS_FEATURES__SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION=true
```

The pre-Quarkus name `SKIP_CREDENTIAL_SUBSCOPING_INDIRECTION` (single
underscore prefix) is silently ignored.

---

## Trino queries fail with Polaris: "scope not valid"

**Symptom:** Trino queries against Iceberg tables fail with OAuth2 errors.
Polaris rejects the token request because the scope is invalid.

**Cause:** Trino's Iceberg REST client defaults to `scope=catalog` (from
Iceberg's `OAuth2Properties.CATALOG_SCOPE`), but Polaris requires
`scope=PRINCIPAL_ROLE:ALL`.

**Fix:** Lakebench sets `iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL`
automatically in the Trino ConfigMap when the catalog type is `polaris`. If you
are configuring Trino manually, add this property to the Iceberg catalog
configuration.

This property was introduced in Trino 454
([PR #22961](https://github.com/trinodb/trino/pull/22961)). Trino versions
prior to 454 do not support it and cannot work with Polaris. Upgrade to Trino
454 or later (Lakebench uses 479).

---

## Hive Metastore DNS resolution fails

**Symptom:** Spark jobs or Trino fail to connect to the Hive Metastore. Errors
reference DNS resolution failure or connection refused on port 9083.

**Cause:** The Hive Metastore pod is not running, or the DNS name is wrong.
The Stackable Hive Operator names the service after the HiveCluster custom
resource plus the role name.

**Expected DNS name:**

```
lakebench-hive-metastore.<namespace>.svc.cluster.local:9083
```

**Diagnosis:**

```bash
# Check Hive status via lakebench
lakebench status test-config.yaml

# Check the HiveCluster CR directly
kubectl get hivecluster -n <namespace>

# Check if the metastore pod is running
kubectl get pods -n <namespace> -l app.kubernetes.io/name=hive
```

If the HiveCluster shows as not ready, check its events and the PostgreSQL
backend. The Hive Metastore requires PostgreSQL to be healthy before it can
start.

---

## Pipeline runs but benchmark returns 0 rows

**Symptom:** The Trino benchmark completes but all queries return 0 rows.
Metric scores show zero throughput.

**Cause:** The Iceberg tables are empty or were not created. This typically
happens when:

- The silver-build or gold-finalize Spark jobs failed silently (check their
  pod logs)
- A previous `lakebench destroy` cleaned the tables but a fresh pipeline run
  was not executed
- The Iceberg catalog (Hive or Polaris) has stale metadata pointing to deleted
  S3 data

**Fix:**

1. Check that all three Spark jobs completed successfully:

```bash
kubectl get sparkapplications -n <namespace>
```

All three (`bronze-verify`, `silver-build`, `gold-finalize`) should show
status `COMPLETED`.

2. Verify that the Iceberg tables exist and have data. The expected table
names are:
   - Silver: `customer_interactions_enriched`
   - Gold: `customer_executive_dashboard`

3. If tables are missing or empty, run a fresh pipeline:

```bash
lakebench destroy test-config.yaml --force
lakebench deploy test-config.yaml
lakebench generate test-config.yaml --wait --timeout 14400
lakebench run test-config.yaml --timeout 7200
```

Do not skip the generate step -- the pipeline needs source data in the bronze
bucket to produce silver and gold tables.
