# Lakebench Operators and Catalogs

This document tracks tested operators, catalog implementations, and migration plans.

## Spark Operator

### Tested Versions

| Version | Status | Notes |
|---------|--------|-------|
| v1.1.27 | Broken | Does NOT inject volumes from `spec.volumes` into pods. Webhook mutation doesn't work. |
| 2.4.0 | Working with workaround | Webhook mutation works for pod labels, but ConfigMap volumes are not injected through Spark's `spark.kubernetes.*.volumes.*` conf-property path (`KubernetesVolumeUtils` has no `configMap` case). Lakebench routes ConfigMap volumes through `driver.template`/`executor.template` pod templates instead. On OpenShift needs the `anyuid` SCC and the fsGroup/seccompProfile patch, which lakebench applies. |
| 2.5.1 | Working with workaround | Current default. Verified against the operator's own source: the volume-injection code paths relevant to this gap are unchanged from 2.4.0, so the same pod-template workaround is still required. See [component-spark.md](component-spark.md#spark-operator) for the mechanism. |

### Current Default
- **Version:** 2.5.1
- **Helm Chart:** `spark-operator/spark-operator`
- **Namespace:** `spark-operator`

### Key Configuration

Install or upgrade the shared operator with lakebench rather than raw Helm:

```bash
lakebench admin install-spark-operator \
  --version 2.5.1 \
  --operator-namespace spark-operator \
  --controller-tmp-size 8Gi
```

`--controller-tmp-size` sets the sizeLimit of the controller's `/tmp`
emptyDir, which holds spark-submit's Ivy jar cache for applications that set
`spark.jars.packages` (Lakebench's own jobs set none since 1.7). The default is 8Gi, the
floor is 4Gi, and an upgrade without the flag keeps a larger size already
set. The command runs under the cluster lease, and an upgrade keeps the
stored Helm values, including the watch list.

On OpenShift, lakebench grants the `anyuid` SCC to the
`spark-operator-controller` and `spark-operator-webhook` service accounts
and patches `fsGroup` and `seccompProfile` out of the operator Deployments
(the chart hardcodes them and Helm values cannot remove them).

Do not edit `spark.jobNamespaces` by hand. `lakebench deploy` adds its
namespace to the watch list and `lakebench destroy` removes it, both under
the `lakebench-cluster-lock` lease in `lakebench-system`. Deploy never
installs the operator; `platform.compute.spark.operator.install: true` is
refused by every command that changes data.

### Learnings
- Spark Operator version (2.5.1) is different from Apache Spark runtime version (3.5.x / 4.0.x / 4.1.x)
- `local://` URIs required for `mainApplicationFile` - scripts must be in container filesystem
- ConfigMap volumes need the pod-template workaround, not the webhook's local-dir conf-property injection -- see [component-spark.md](component-spark.md#spark-operator)

---

## Hive Metastore

### Implementations Tested

| Implementation | Status | Notes |
|----------------|--------|-------|
| Raw `apache/hive:3.1.3` | Broken | Missing hadoop-aws JARs for S3A filesystem. Fails when Iceberg tries to create namespace with S3 location. |
| Stackable Hive Operator 25.7.0 | Working | Built-in S3 support, credential injection via SecretClass, auto-managed. |

### Stackable Hive Operator
- **API:** `hive.stackable.tech/v1alpha1`
- **Kind:** `HiveCluster`
- **Features:**
  - Built-in S3 support via `spec.clusterConfig.s3`
  - Automatic credential injection via `secretClass`
  - Connection pooling, resource management
  - Tested in enterprise deployments

### Migration Plan
1. Identify root cause (S3A JARs missing) -- done
2. Switch to Stackable Hive Operator -- done
3. Verify silver/gold pipeline works -- done
4. Document Stackable configuration -- done

---

## Catalog Roadmap

### Hive Metastore
- Iceberg table format support
- Stackable operator for S3 support
- PostgreSQL backend

### Apache Polaris
- Pure Iceberg REST catalog
- OAuth2 client credentials
- No STS credential vending (FlashBlade limitation)
- PostgreSQL backend (separate `polaris` database)

### Catalog Comparison

Lakebench supports Hive Metastore and Apache Polaris. The others are listed
for ecosystem context only -- they are not supported.

| Catalog | Format Support | Governance | Lakebench Status |
|---------|---------------|------------|-----------------|
| Hive Metastore | Iceberg, Delta | Basic | **Supported** (default) |
| Apache Polaris | Iceberg only | Access control | **Supported** |
| Unity Catalog OSS | Iceberg, Delta | Lineage, governance | Not supported (no Unity combination is accepted) |
| Nessie | Iceberg | Git-style versioning | Not implemented |

### Choosing Between Hive and Polaris
- Both are fully supported and tested
- Hive is the default -- well-established, broad compatibility
- Polaris is the modern option -- pure REST, better for multi-cloud setups
- See [quickstart-polaris.md](quickstart-polaris.md) for Polaris setup

---

## RBAC Requirements

### Spark Runner ServiceAccount
The `lakebench-spark-runner` service account needs these permissions for Spark 3.5.x cleanup:

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "create", "delete", "deletecollection", "patch", "update"]
  - apiGroups: [""]
    resources: ["services", "configmaps", "persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "create", "delete", "deletecollection"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get", "list"]
```

The full Role is `templates/rbac/role.yaml.j2`.

**Note:** `deletecollection` is required for Spark driver cleanup on termination.

---

## Configuration Reference

### Test Config (test-config.yaml)
```yaml
platform:
  storage:
    scratch:
      enabled: true
      storage_class: px-csi-scratch  # Portworx, repl=1 (PVC size per job profile)
```

### Spark Conf Essentials

Example for a Java 11 Spark 3.5 image such as `apache/spark:3.5.4-python3`.
The Iceberg runtime suffix and Hadoop AWS version both depend on the Spark
minor version -- see the Version Matrix below. Iceberg 1.11 needs Java 17, so
with the default Iceberg version a Java 11 Spark 3.5 image falls back to
Iceberg 1.10.1 with a warning; a java17 Spark 3.5 tag gets 1.11.0.

The jars are resolved once at deploy by the deployment's dependency server
(`lb-deps`) and named by URL, so the coordinates below are what it resolves:
`org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1` and
`org.apache.hadoop:hadoop-aws:3.3.4`.

```yaml
spark.jars: http://lb-deps.<namespace>.svc.cluster.local:8080/sets/<pinset>/jars/org.apache.iceberg_iceberg-spark-runtime-3.5_2.12-1.10.1.jar,...
spark.sql.catalog.lakehouse: org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type: hive
spark.sql.catalog.lakehouse.uri: thrift://lakebench-hive-metastore:9083
```

---

## Version Matrix

| Component | Version | Source |
|-----------|---------|--------|
| Spark Operator | 2.5.1 | Kubeflow helm chart |
| Apache Spark | 3.5.x / 4.0.x / 4.1.x (default image 4.0.2; 4.2 unsupported) | apache/spark image |
| Iceberg | 1.11.0 (1.10.1 on a Java 11 Spark 3.5 image) | resolved by lb-deps |
| Delta Lake | 4.0.0 on Spark 4.0, 4.1.0 on Spark 4.1 (none on 3.5) | resolved by lb-deps |
| Apache Polaris | 1.6.0 | apache/polaris image |
| Stackable Hive Operator | 25.7.0 | oci://oci.stackable.tech/sdp-charts |
| Stackable Commons Operator | 25.7.0 | oci://oci.stackable.tech/sdp-charts |
| Stackable Secret Operator | 25.7.0 | oci://oci.stackable.tech/sdp-charts |
| Stackable Listener Operator | 25.7.0 | oci://oci.stackable.tech/sdp-charts |
| Hive Metastore | 3.1.3 | Managed by Stackable |
| Hadoop AWS | 3.3.4 / 3.4.1 / 3.4.2 (by Spark minor) | resolved by lb-deps |
| PostgreSQL | 17 | postgres:17 |
| Trino | 483 | trinodb/trino image |
| DuckDB | 1.5.5 | wheel resolved by lb-deps, run on python:3.11-slim |

## Stackable Installation

The Stackable operators are shared cluster infrastructure: a cluster admin
installs them once, and `lakebench deploy` never does
(`architecture.catalog.hive.operator.install: true` is refused).

```bash
# Install Stackable operators (required for Hive)
helm install commons-operator oci://oci.stackable.tech/sdp-charts/commons-operator --version 25.7.0 --namespace stackable --create-namespace
helm install secret-operator oci://oci.stackable.tech/sdp-charts/secret-operator --version 25.7.0 --namespace stackable
helm install listener-operator oci://oci.stackable.tech/sdp-charts/listener-operator --version 25.7.0 --namespace stackable
helm install hive-operator oci://oci.stackable.tech/sdp-charts/hive-operator --version 25.7.0 --namespace stackable
```

---

## Platform Security

Lakebench includes platform-aware security verification. Use `lakebench validate --verbose` to check security requirements.

### Platform Detection

Lakebench automatically detects the platform type:
- **OpenShift:** Detected via SCC CRD presence
- **Vanilla Kubernetes:** Default fallback

### OpenShift Security Context Constraints (SCC)

On OpenShift, Spark pods require the `anyuid` SCC because they run as UID 185 (spark user).

**Automatic Configuration:**
- During `lakebench deploy`, the RBAC and PostgreSQL steps grant `anyuid` to `lakebench-spark-runner` and `lakebench-postgres` through the RBAC API (the RoleBinding `system:openshift:scc:anyuid`; no `oc` needed). A refused grant fails the step; see [Prerequisites](prerequisites.md#openshift-scc-clusterrole)
- The `lakebench validate` command checks if SCCs are already configured

**Manual Configuration:**
```bash
# Grant anyuid SCC to lakebench-spark-runner service account
oc adm policy add-scc-to-user anyuid -z lakebench-spark-runner -n <namespace>

# Verify SCC assignment. On OpenShift 4.10+ the grant is a namespaced
# RoleBinding, not an entry in the SCC's .users list (which stays empty).
oc get rolebinding system:openshift:scc:anyuid -n <namespace> -o yaml
```

### Validation Output

```bash
lakebench validate test-config.yaml --verbose

# Output includes:
# Platform Security
# + Platform detected: openshift 4.19.0
# + SCC 'anyuid' assigned to 'lakebench-spark-runner'
# + Platform security checks passed
```

### Production Recommendations

1. **Custom SCC:** Consider creating a custom SCC with minimal privileges instead of using `anyuid`
2. **Namespace isolation:** Use dedicated namespaces per deployment
3. **RBAC scoping:** The lakebench-spark-runner Role is namespace-scoped (not ClusterRole)

---

## Troubleshooting

### Volume not mounted in Spark pods
- **Cause:** Old Spark operator (v1.1.x) doesn't inject volumes
- **Fix:** Upgrade to Spark operator v2.x (2.5.1 is the current default). Note: even on v2.x, ConfigMap volumes specifically still need the pod-template workaround, not raw `.spec.volumes` -- see [component-spark.md](component-spark.md#spark-operator).

### S3AFileSystem not found in Hive
- **Cause:** Raw Hive image lacks hadoop-aws JARs
- **Fix:** Use Stackable Hive Operator with S3 config

### PVC provisioning failed
- **Cause:** Wrong storage class name
- **Fix:** Use a storage class that exists on the cluster. The scratch default is `px-csi-scratch` (Portworx, repl=1), which a cluster admin installs once with `lakebench admin install-scratch-storage-class`

### deletecollection forbidden
- **Cause:** RBAC missing `deletecollection` verb
- **Fix:** Update Role with deletecollection permission

### OpenShift SCC forbidden (UID 185)
- **Cause:** lakebench-spark-runner ServiceAccount lacks `anyuid` SCC
- **Fix:**
  - `lakebench deploy` makes the grant itself and fails the RBAC step if it is refused
  - A cluster admin can make it instead: `oc adm policy add-scc-to-user anyuid -z lakebench-spark-runner -n <namespace>` (and the same with `-z lakebench-postgres`)
- **Verification:** `oc get events -n <namespace> --sort-by='.lastTimestamp'` shows SCC errors
