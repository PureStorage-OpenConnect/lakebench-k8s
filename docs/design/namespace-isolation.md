# Namespace isolation and cluster ownership taxonomy

Status: shipped in v1.5.0 (commits `30be5ba`, `9992e6a`, `8398745`).

## Problem

Lakebench runs on shared Kubernetes clusters. Before v1.5, two parallel deployments could poison each other's runs: they both created `SecretClass/lakebench-s3-credentials-class` (cluster-scoped, fixed name), they both raced the same Spark Operator `spark.jobNamespaces` Helm value, and either destroy could delete resources the other still needed. The failure mode was silent: a destroy that reported SUCCESS while breaking every other deployment on the cluster.

The design goal is one invariant:

> **Destroying deployment A does not affect deployment B running in parallel.**

This is a testable proposition. The verification plan (`dev-artifacts/uat-scenarios/`) exercises the six failure modes on a live cluster before each release.

## The four categories

Every resource lakebench creates or reads falls into exactly one of these categories. The category dictates the ownership rule; the ownership rule dictates the code path.

### Category 1 -- per-deployment

Created on `deploy`, destroyed on `destroy`, never touched with a foreign identity.

Namespace, PostgreSQL, Hive Metastore, Polaris, Trino, Spark Thrift, DuckDB, S3 buckets, Iceberg catalogs and tables, ConfigMaps, ServiceAccount, RoleBindings, PVCs, Stackable SecretClass. Namespaced resources are isolated by their namespace; cluster-scoped resources (SecretClass in Stackable's model) include the namespace as a suffix in `metadata.name`.

Identity carrier: a `lakebench.deployment/name` annotation on the namespace, a `lakebench.deployment=<name>` tag on each bucket. Set on deploy, verified before every destructive mutation, refused on mismatch.

### Category 2 -- shared read-only infrastructure

Version-asserted at preflight. Never created, never destroyed by lakebench.

Kubernetes API server, CNI, StorageClasses (`px-csi-scratch`, `px-csi-db`, `thin-csi`), CRDs (`SparkApplication`, `HiveCluster`), FlashBlade endpoint, S3 credentials rotation policy.

Rule: `deploy` preflight refuses with an actionable error when any of these is missing. A cluster admin installs them once with `lakebench admin install-*`. `destroy` never touches them.

### Category 3 -- shared operator installations

Single installation per cluster. Version-asserted at preflight.

Spark Operator, Stackable operators, Prometheus/Grafana Helm release. `deploy` refuses if the operator is absent or at an unsupported version. `lakebench admin install-spark-operator` (and friends) install them; a cluster admin runs `admin` once, a developer runs `deploy` many times without touching the operator install.

### Category 4 -- shared mutable state

Cluster-wide state lakebench MUST mutate to work, but that other lakebench deployments also depend on.

Examples: Spark Operator's `spark.jobNamespaces` watch list, the observability release's `podMonitorSelector`, any future cluster-wide `PriorityClass` / `NetworkPolicy` / admission webhook lakebench installs.

Rule: any mutation goes through a cluster-wide lease (see below), reads live state, computes the diff, writes atomically with retry on conflict, and verifies the shared component is still healthy after. Failure raises; never warns.

## The cluster lease

A ConfigMap `lakebench-cluster-lock` in namespace `lakebench-system`, acquired via optimistic-concurrency create-or-replace, released in a `finally` block, reclaimable by `lakebench admin release-lock` when a holder crashes.

Contents:
- `holder`: `<hostname>@<user>@<git-sha>`
- `acquired-at`: ISO 8601
- `ttl-seconds`: 3600 default
- resourceVersion-based CAS for atomic acquire against an expired lease

The lease is NOT a distributed lock in the CAP sense: a partitioned holder that cannot reach the API server can still complete its mutation locally. What it does prevent is two `lakebench` processes racing the same Helm upgrade or the same SCC install through the apiserver -- which is what actually broke under parallel UAT.

Every mutating `admin` command acquires the lease. The strict variant of the Spark Operator watch-list mutation (`remove_namespace_from_watch(strict=True)`) also acquires it, so destroy paths and admin operations serialise against each other.

## Deployment identity

On `deploy`, `_deploy_namespace` writes three annotations to the namespace:

- `lakebench.deployment/name: <cfg.name>`
- `lakebench.deployment/api-server: <sha256(cluster-CA-cert)>` -- workstation and in-cluster paths to the same cluster produce the same fingerprint because they read the same CA
- `lakebench.deployment/committed-sha: <git rev-parse HEAD>` (best-effort, informational)

The write uses an optimistic-concurrency PATCH keyed on the namespace's `resourceVersion`. If the PATCH conflicts, we re-read; if the re-read shows a foreign name, refuse.

Legacy pre-v1.5 namespaces (annotation-less but containing lakebench-labelled resources) are treated as UNSAFE, not legacy: `deploy --force-legacy` is the only way to claim them, and `destroy` refuses on missing annotations. Migration goes through `lakebench admin migrate-deployment <namespace>`, which stamps the annotations and renames the legacy fixed-name SecretClasses to the namespaced form.

## Watch-list hardening

`spark.jobNamespaces` is Category 4. Every mutation is a read-modify-write on shared Helm state; without serialisation, two deploys can each read the list before the other writes, and the second silently drops the first's namespace.

The strict path lease-gates the mutation and raises `WatchListMutationError` on failure. Destroy calls this path. On failure, destroy explicitly BLOCKS the namespace delete: deleting a namespace the operator still watches crash-loops the operator globally (`failed to wait for spark-application-controller caches to sync`), taking down SparkApplication reconciliation for every namespace on the cluster. Refusing to delete is the only safe response. The user is directed to `lakebench admin repair-operator` to reconcile.

The add path (`_add_namespace_to_watch`) is lease-gated too, with three outcomes on lease acquire: `locked` (proceed under the lease), `unlocked` (genuine no-cluster case, e.g. workstation without kubeconfig -- safe to proceed with no other writer possible), or `refuse` (`ClusterLockHeld` or RBAC denial -- return False rather than proceed unlocked into the race).

## Bucket ownership

Every `ensure_buckets` call unconditionally writes:

- `lakebench.deployment=<cfg.name>`
- `lakebench.workload=<cfg.workload_schema>`

Then reads the tags back and verifies the round trip; a bucket that does not accept tags is refused rather than trusted. Destroy and clean read the tag before mutating: a mismatch is a hard refusal (`--force` does not bypass), an absent tag on a legacy bucket requires `--force-legacy`, a missing bucket is a no-op.

`lakebench admin reclaim-bucket <name>` rewrites the tag under the cluster lease with an object-count check inside the lease scope; a bucket with objects requires `--force-nonempty` acknowledgement.

## Verification

Unit-level proxies run on every commit:
- `tests/test_ownership.py` -- identity annotations, bucket tags, api-server fingerprint parity between workstation and in-cluster paths.
- `tests/test_cluster_lock.py` -- lease acquire/steal/release, ttl and force-release semantics.
- `tests/test_admin.py` -- every admin subcommand, migration idempotence, reclaim refusal on non-empty.
- `tests/test_watch_list_strict.py` -- strict-mode dispatch, lease-held refusal, WatchListMutationError text pointing at `admin repair-operator`.
- `tests/test_destroy_unwatches_namespace.py` -- destroy blocks namespace delete on watch-list failure.

Six live UAT scenarios (`dev-artifacts/uat-scenarios/`, gitignored by design) run once per release:
- **S-P1**: Destroy A while B is running; B finishes with `success=True` and `scale_ratio > 0.95`.
- **S-P2**: Two deploys within 5 seconds; both reach infrastructure-ready with distinct identity stamps.
- **S-P3**: Destroy B mid-Generate on A; A finishes clean, B destroys without touching A.
- **S-P4**: Two `destroy A --force` within seconds; one succeeds, other reports "already gone" cleanly.
- **S-P5**: Untagged bucket refuses destroy without `--force-legacy`.
- **S-P6**: Two configs targeting the same bucket name; second deploy refuses at `ensure_buckets` with tag-mismatch.

## Non-goals

- Multi-tenancy inside a single lakebench deployment.
- Global cluster policy enforcement.
- Removing shared operator installations (Spark Operator, Stackable, Prometheus stay one-per-cluster).

## Migration from pre-v1.5

`lakebench admin migrate-deployment <namespace>` stamps the annotations and copies the legacy fixed-name SecretClasses (`lakebench-s3-credentials-class`, `lakebench-s3-ca-cert-class`) to the new `-<namespace>` form. Idempotent. Mandatory before `destroy` on any pre-v1.5 namespace -- destroy refuses without the annotations.

Destroy cleans up the legacy fixed-name SecretClasses only when this is the last `managed-by: lakebench` namespace cluster-wide (keyed on the label so pre-v1.5 annotationless deployments still boot). Until every deployment on a cluster has migrated, the legacy names remain reserved.
