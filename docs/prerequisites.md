# Prerequisites

<!-- Generated from source. Do not edit by hand. -->

What a cluster needs before `lakebench deploy` can succeed. Each entry is a
read-only check in `src/lakebench/deploy/prereqs.py`, and the preflight of
`lakebench run` runs these same checks, except the ones marked as checked at
deploy, so this page and the checks cannot disagree. Besides these, `kubectl` and `helm` must be on `PATH`. `oc` is not
needed: Lakebench makes its OpenShift SCC grants through the Kubernetes API.

| Check | Needed when |
|---|---|
| [Scratch StorageClass](#scratch-storage-class) | `platform.storage.scratch.enabled: true`, or a batch run at scale 50 and above with it unset |
| [Kubeflow Spark Operator 2.x](#spark-operator) | Always |
| [Stackable operators (Hive catalog)](#stackable) | `architecture.catalog.type: hive` |
| [Shared observability stack](#observability-stack) | `observability.enabled: true` |
| [OpenShift anyuid SCC](#openshift-scc-clusterrole) | OpenShift |
| [Dependency server StorageClass](#deps-storage-class) | Always |
| [Egress for the dependency resolve](#egress-hosts) | Always |
| [S3 endpoint and credentials](#s3-reachable-and-credentials) | Always |

<a id="scratch-storage-class"></a>

## Scratch StorageClass

Check id `scratch-storage-class`. Needed when: `platform.storage.scratch.enabled: true`, or a batch run at scale 50 and above with it unset.

Spark executors put shuffle and spill on per-executor PVCs from the StorageClass named by `platform.storage.scratch.storage_class` (default `px-csi-scratch`, Portworx with one replica). The StorageClass is shared cluster infrastructure: Lakebench uses it and never creates or deletes it during `deploy` or `destroy`.

**Fix:** A cluster admin runs `lakebench admin install --component scratch-storage-class <config>` once per cluster, or set `platform.storage.scratch.enabled: false`.

<a id="spark-operator"></a>

## Kubeflow Spark Operator 2.x

Check id `spark-operator`. Needed when: Always.

Spark jobs are `SparkApplication` resources run by one shared Kubeflow Spark Operator. The check needs the `SparkApplication` CRD, a controller Deployment with a ready replica, and a 2.x chart (2.5.1 is the tested release; 1.x cannot mount the scripts volume). `deploy` adds its namespace to the operator's watch list under the cluster lease; never edit `spark.jobNamespaces` by hand.

**Fix:** A cluster admin runs `lakebench admin install --component spark-operator <config>` once per cluster. Do not install or upgrade it with a raw `helm` command: the managed path holds the cluster lease and never resets the operator's namespace watch list.

<a id="stackable"></a>

## Stackable operators (Hive catalog)

Check id `stackable`. Needed when: `architecture.catalog.type: hive`.

The Hive Metastore is a Stackable `HiveCluster`. The check needs the `HiveCluster` and `SecretClass` CRDs and a running hive-operator and secret-operator pod. CRDs alone are not enough, because helm leaves them behind when an operator is uninstalled.

**Fix:** A cluster admin runs `lakebench admin install --component stackable <config>` once per cluster (the commons, listener, secret and hive operators, SDP 25.7.0), or use a Polaris recipe, which needs no operator.

<a id="observability-stack"></a>

## Shared observability stack

Check id `observability-stack`. Needed when: `observability.enabled: true`.

Metrics go to one shared Prometheus and Grafana (kube-prometheus-stack, release `lakebench-observability`). Each deployment adds only its own PodMonitors; `destroy` never removes the shared release.

**Fix:** A cluster admin runs `lakebench admin install --component observability <config>` once per cluster, or set `observability.enabled: false`. To remove the stack later, a cluster admin runs `helm uninstall lakebench-observability -n lakebench-observability` once no deployment uses it.

<a id="openshift-scc-clusterrole"></a>

## OpenShift anyuid SCC

Check id `openshift-scc-clusterrole`. Needed when: OpenShift.

Spark pods run as UID 185 and PostgreSQL as UID 999, so on OpenShift both ServiceAccounts need the `anyuid` SCC. The Spark Operator's ServiceAccounts get it at operator install.

- `deploy` grants it as `oc adm policy add-scc-to-user` does on OpenShift 4.10 and later: the RoleBinding `system:openshift:scc:anyuid` in the ServiceAccount's namespace, bound to the ClusterRole of the same name.
- Before writing, a LocalSubjectAccessReview checks whether an admin already granted it. Afterwards `deploy` checks that the grant took effect.
- The deploying user needs to create RoleBindings in the namespace and to bind that ClusterRole.
- A grant that cannot be made fails the deploy step. It is never a warning.
- OpenShift before 4.10 has no such ClusterRole and is not supported.

**Fix:** If `deploy` stops with `cannot grant SCC anyuid to SA <sa> in namespace <ns>`, a cluster admin runs `oc adm policy add-scc-to-user anyuid -z <sa> -n <ns>` for `lakebench-spark-runner` and `lakebench-postgres`, then `deploy` is re-run.

<a id="deps-storage-class"></a>

## Dependency server StorageClass

Check id `deps-storage-class`. Needed when: Always. Checked at deploy, not by the `run` preflight.

Each deployment runs its own dependency server, `lb-deps`. It keeps the resolved jars, wheels and DuckDB extensions on a 5Gi ReadWriteOnce PVC, `lb-deps-data`.

- The class is `platform.deps.storage_class`, or the cluster default when that is empty.
- The volume must be writable by UID 185 through `fsGroup`.
- Use a replicated class. If the volume is lost with its node, the server pod stays Pending and every `run` stops. Delete the PVC and re-run `deploy`, which resolves the set again.
- The class is read only when the PVC is created. To move a set, delete the PVC and re-run `deploy`.
- When the PVC exists, the check reports its class and nothing else.

**Fix:** Set `platform.deps.storage_class` to an existing StorageClass, or have a cluster admin mark one as the cluster default. Prefer a replicated one.

<a id="egress-hosts"></a>

## Egress for the dependency resolve

Check id `egress-hosts`. Needed when: Always. Checked at deploy, not by the `run` preflight.

`deploy` resolves every jar, wheel and DuckDB extension once, in the `lb-deps` pod. After that no pod fetches a dependency from outside the deployment.

- Sources: Maven Central and its Google mirror; PyPI (pypi.org and files.pythonhosted.org) for the AML reference and DuckDB wheels; extensions.duckdb.org for DuckDB.
- Spark jobs, Spark Thrift and DuckDB read the set from `lb-deps`.
- Egress is needed only at a deploy that resolves again: a new image, version or mirror, or a new, lost or damaged set.
- The check lists the hosts this config reads and does not probe them.
- An unreachable host fails the `deps` step of `deploy`, naming the repository and the mirror keys. A proxy that answers with an error fails it, naming the artifact and the repository.

On a cluster without that egress, set the mirror keys under `platform.deps`:

- `maven_repository` becomes the only Maven repository.
- `pypi_index` replaces pypi.org as a PyPI simple index.
- `duckdb_extension_repository` replaces extensions.duckdb.org.

Mirrors are read anonymously, over HTTP or over HTTPS with a publicly trusted certificate. Mirror credentials and a private CA are not supported. Changing a mirror re-resolves at the next `deploy`. A mirror that serves the same bytes gives the same set hash, so runs stay like-for-like. Other bytes give a different set, and those runs are not like-for-like.

Image pulls are separate: the nodes pull the images named under `images` (and the Stackable Hive image for a Hive catalog) from their registries at every pod start.

**Fix:** Allow egress from the deployment's namespace to the listed hosts during `deploy`, or point `platform.deps.maven_repository`, `platform.deps.pypi_index` and `platform.deps.duckdb_extension_repository` at mirrors the cluster can reach.

<a id="s3-reachable-and-credentials"></a>

## S3 endpoint and credentials

Check id `s3-reachable-and-credentials`. Needed when: Always.

Lakebench needs an S3-compatible endpoint and credentials that can list, create and write buckets. The check lists buckets with the configured credentials.

**Fix:** Set `platform.storage.s3.endpoint`, `access_key` and `secret_key`, and check the endpoint is reachable from this machine; `lakebench config storage <config>` probes the backend.
