# Lakebench Operators and Catalogs

**Last Updated:** 2026-01-29

This document tracks tested operators, catalog implementations, and migration plans.

## Status: All Tests Passing

Pipeline successfully tested with 1GB data:
- bronze-verify: 53s
- silver-build: 1m 47s (500K records to Iceberg)
- gold-finalize: 58s (table: customer_executive_dashboard)

---

## Spark Operator

### Tested Versions

| Version | Status | Notes |
|---------|--------|-------|
| v1.1.27 | Broken | Does NOT inject volumes from `spec.volumes` into pods. Webhook mutation doesn't work. |
| 2.4.0 | Working | Latest version. Properly injects ConfigMap volumes via webhook. Requires privileged SCC on OpenShift. |

### Current Default
- **Version:** 2.4.0
- **Helm Chart:** `spark-operator/spark-operator`
- **Namespace:** `spark-operator`

### Key Configuration
```bash
# Install on OpenShift with all-namespace watching
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator \
  --create-namespace \
  --set spark.jobNamespaces="" \
  --set webhook.enable=true

# Required SCCs for OpenShift
oc adm policy add-scc-to-user privileged -z spark-operator-controller -n spark-operator
oc adm policy add-scc-to-user privileged -z spark-operator-webhook -n spark-operator
```

### Learnings
- Spark Operator version (2.4.0) is different from Apache Spark runtime version (3.5.4)
- `local://` URIs required for `mainApplicationFile` - scripts must be in container filesystem
- ConfigMap volumes work correctly with v2.4.0 webhook

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

### Phase 1: Hive Metastore (Current)
- Universal compatibility (Iceberg, Delta, Hudi)
- Stackable operator for S3 support
- PostgreSQL backend

### Phase 2: Apache Polaris
- Pure Iceberg REST catalog
- Credential vending
- Multi-cloud native
- Migration after Hive is stable

### Catalog Comparison

| Catalog | Format Support | Governance | Deployment |
|---------|---------------|------------|------------|
| Hive Metastore | Iceberg, Delta, Hudi | Basic | Stackable operator |
| Apache Polaris | Iceberg only | Access control | K8s deployment |
| Unity Catalog OSS | Iceberg, Delta, Hudi | Lineage, governance | K8s deployment |
| Nessie | Iceberg | Git-style versioning | K8s deployment |

### Why Polaris Later
- Iceberg-only (we may need mixed format support initially)
- Need stable baseline first with Hive
- Polaris is the modern direction once data paths are proven

---

## RBAC Requirements

### Spark Runner ServiceAccount
The `lakebench-spark-runner` service account needs these permissions for Spark 3.5.x cleanup:

```yaml
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps", "persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "create", "delete", "deletecollection", "patch", "update"]
```

**Note:** `deletecollection` is required for Spark driver cleanup on termination.

---

## Configuration Reference

### Test Config (test-config.yaml)
```yaml
platform:
  storage:
    scratch:
      enabled: true
      storage_class: px-csi-replicated  # Not px-spark-scratch
      size: 50Gi
```

### Spark Conf Essentials
```yaml
spark.jars.packages: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.apache.hadoop:hadoop-aws:3.3.4
spark.sql.catalog.lakehouse: org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.lakehouse.type: hive
spark.sql.catalog.lakehouse.uri: thrift://lakebench-hive-metastore:9083
```

---

## Version Matrix

| Component | Version | Source |
|-----------|---------|--------|
| Spark Operator | 2.4.0 | Kubeflow helm chart |
| Apache Spark | 3.5.4 | apache/spark image |
| Iceberg | 1.10.1 | spark.jars.packages |
| Stackable Hive Operator | 25.7.0 | oci://oci.stackable.tech/sdp-charts |
| Stackable Commons Operator | 25.7.0 | oci://oci.stackable.tech/sdp-charts |
| Stackable Secret Operator | 25.7.0 | oci://oci.stackable.tech/sdp-charts |
| Stackable Listener Operator | 25.7.0 | oci://oci.stackable.tech/sdp-charts |
| Hive Metastore | 3.1.3 | Managed by Stackable |
| Hadoop AWS | 3.3.4 | spark.jars.packages |
| PostgreSQL | 15 | bitnami/postgresql |
| Trino | 479 | trinodb/trino image |

## Stackable Installation

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
- During `lakebench deploy`, the RBAC deployer automatically grants `anyuid` SCC to `lakebench-spark-runner`
- The `lakebench validate` command checks if SCCs are already configured

**Manual Configuration:**
```bash
# Grant anyuid SCC to lakebench-spark-runner service account
oc adm policy add-scc-to-user anyuid -z lakebench-spark-runner -n <namespace>

# Verify SCC assignment
oc get scc anyuid -o jsonpath='{.users}'
```

### Validation Output

```bash
lakebench validate test-config.yaml --verbose

# Output includes:
# Platform Security
# ✓ Platform detected: openshift 4.19.0
# ✓ SCC 'anyuid' assigned to 'lakebench-spark-runner'
# ✓ Platform security checks passed
```

### Production Recommendations

1. **Custom SCC:** Consider creating a custom SCC with minimal privileges instead of using `anyuid`
2. **Namespace isolation:** Use dedicated namespaces per deployment
3. **RBAC scoping:** The lakebench-spark-runner Role is namespace-scoped (not ClusterRole)

---

## Troubleshooting

### Volume not mounted in Spark pods
- **Cause:** Old Spark operator (v1.1.x) doesn't inject volumes
- **Fix:** Upgrade to Spark operator 2.4.0

### S3AFileSystem not found in Hive
- **Cause:** Raw Hive image lacks hadoop-aws JARs
- **Fix:** Use Stackable Hive Operator with S3 config

### PVC provisioning failed
- **Cause:** Wrong storage class name
- **Fix:** Use actual storage class (e.g., `px-csi-replicated`)

### deletecollection forbidden
- **Cause:** RBAC missing `deletecollection` verb
- **Fix:** Update Role with deletecollection permission

### OpenShift SCC forbidden (UID 185)
- **Cause:** lakebench-spark-runner ServiceAccount lacks `anyuid` SCC
- **Fix:**
  - Run `lakebench deploy` to auto-configure SCC
  - Or manually: `oc adm policy add-scc-to-user anyuid -z lakebench-spark-runner -n <namespace>`
- **Verification:** `oc get events -n <namespace> --sort-by='.lastTimestamp'` shows SCC errors
