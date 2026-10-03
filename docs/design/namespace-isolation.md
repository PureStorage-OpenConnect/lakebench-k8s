# Namespace isolation and cluster ownership taxonomy

## Problem

Lakebench runs on shared Kubernetes clusters. Before v1.5, two parallel deployments could poison each other's runs: they both created `SecretClass/lakebench-s3-credentials-class` (cluster-scoped, fixed name), they both raced the same Spark Operator `spark.jobNamespaces` Helm value, and either destroy could delete resources the other still needed. The failure mode was silent: a destroy that reported SUCCESS while breaking every other deployment on the cluster.

The design goal is one invariant:

> **Destroying deployment A does not affect deployment B running in parallel.**

This is a testable proposition. The verification plan (six live scenarios, listed under Verification below) exercises the six failure modes on a live cluster before each release.

## The four categories

Every resource lakebench creates or reads falls into exactly one of these categories. The category dictates the ownership rule; the ownership rule dictates the code path.

### Category 1 -- per-deployment

Created on `deploy`, destroyed on `destroy`, never touched with a foreign identity.

Namespace, PostgreSQL, Hive Metastore, Polaris, Trino, Spark Thrift, DuckDB, S3 buckets, Iceberg catalogs and tables, ConfigMaps, ServiceAccount, RoleBindings, PVCs, Stackable SecretClass. Namespaced resources are isolated by their namespace; cluster-scoped resources (SecretClass in Stackable's model) include the namespace as a suffix in `metadata.name`.

When `create_namespace: false` keeps the namespace, destroy must delete each namespaced object itself. `src/lakebench/deploy/category1.py` lists every object deploy and run create there (`CATEGORY1_OBJECTS`, each with the work item that creates it and the destroy step that deletes it), the namespace annotations destroy removes (`CATEGORY1_ANNOTATIONS`), and the objects kept on purpose with their reason (`KEPT_ON_DESTROY`). Destroy's `category1` step deletes the entries no component step covers, by name or by an `app.kubernetes.io/instance=<deployment>` selector, after the component steps and before the namespace step. A work item that creates a namespaced object adds its entry in the same change. `tests/test_category1_teardown.py` fails otherwise for every object a template declares and every object deploy and the run-time creators it drives (datagen Jobs, scripts maps, SparkApplications) create; a new creator outside templates has to be added to that test.

Identity carrier: a `lakebench.deployment/name` annotation on the namespace, a `lakebench.deployment=<name>` tag on each bucket. Set on deploy, verified before every destructive mutation, refused on mismatch.

**S3 backends that do not implement bucket tagging (LB-164).** Pure Storage FlashBlade returns HTTP 501 `NotImplemented` on both `GetBucketTagging` and `PutBucketTagging`, so the tag-based identity carrier is unavailable. The fallback is a name-prefix check: the bucket name must be exactly the deployment name or start with `{deployment_name}-`. Longest prefix wins: a bucket that also matches a live sibling deployment with a longer name (`lb16-base-bronze` against `lb16` and `lb16-base`) belongs to the sibling. Deploy warns once per run and skips the tag write. The name is not proof of creation, because deploy adopts a pre-existing matching bucket on these backends without the `--force-legacy` a tagged backend demands. Destroy therefore empties and deletes a bucket only when the name matches AND the namespace's created-buckets record (`lakebench.deployment/created-buckets`, written by deploy when it creates the bucket) lists it. A pre-existing matching bucket that deploy found empty is recorded in `lakebench.deployment/adopted-empty-buckets` and may be emptied, never deleted. Any other matching but unrecorded bucket (one that held data when deploy adopted it, or left by an earlier deployment whose namespace is gone) is left in place and reported; `--force-legacy` empties it and never deletes it. `clean` and the continuous reset apply the same rule. Users who follow the example-config bucket-naming convention (`{deployment}-bronze`, `{deployment}-silver`, `{deployment}-gold`) and let deploy create the buckets get destroy safety on FlashBlade close to tagged backends; the gap is that an adopted bucket that already held data is not emptied, where a tagged backend empties an adopted bucket it has tagged. Users who name buckets outside the convention on FlashBlade must pass `--force-legacy` on destroy: at that point ownership rests on operator vigilance, not code. `lakebench config storage` reports the backend's tagging support in the `bucket-tagging` ADVISORY row so this trade-off is visible before deploy.

### Category 2 -- shared read-only infrastructure

Version-asserted at preflight. Never created, never destroyed by lakebench.

Kubernetes API server, CNI, StorageClasses (`px-csi-scratch`, `px-csi-db`, `thin-csi`), CRDs (`SparkApplication`, `HiveCluster`), FlashBlade endpoint, S3 credentials rotation policy.

Rule: `deploy` preflight refuses with an actionable error when any of these is missing. A cluster admin installs them once with `lakebench admin install --component`. `destroy` never touches them.

### Category 3 -- shared operator installations

Single installation per cluster. Version-asserted at preflight.

Spark Operator, Stackable operators, Prometheus/Grafana Helm release. `deploy` refuses if one is absent and never installs one (`operator.install: true` is refused by the commands that change data). `lakebench admin install --component spark-operator|stackable|observability` installs them under the cluster lease and never changes an installed one; a cluster admin runs `admin` once, a developer runs `deploy` many times without touching the operator install.

### Category 4 -- shared mutable state

Cluster-wide state lakebench MUST mutate to work, but that other lakebench deployments also depend on.

Examples: Spark Operator's `spark.jobNamespaces` watch list, the one-time install of the shared observability release (a check-then-install run under the lease), any future cluster-wide `PriorityClass` / `NetworkPolicy` / admission webhook lakebench installs. The observability release's `podMonitorSelector` is no longer mutated: the shared Prometheus uses the chart's default namespace selector and selects every deployment's PodMonitors by their `release: lakebench-observability` label.

Rule: any mutation goes through a cluster-wide lease (see below), reads live state, computes the diff, writes atomically with retry on conflict, and verifies the shared component is still healthy after. Failure raises; never warns.

## The cluster lease

A ConfigMap `lakebench-cluster-lock` in namespace `lakebench-system`, acquired via optimistic-concurrency create-or-replace, released in a `finally` block, reclaimable by `lakebench admin release-lock` when a holder crashes.

Contents:
- `holder`: `<hostname>@<user>@<git-sha>#<pid>-<8 hex>`; the suffix names the process for `admin status` and is new on every acquire
- `write-nonce`: random per write; release deletes only the lease its own write created
- `acquired-at`: ISO 8601
- `ttl-seconds`: 3600 default
- resourceVersion-based CAS for atomic acquire against an expired lease

The lease is NOT a distributed lock in the CAP sense: a partitioned holder that cannot reach the API server can still complete its mutation locally. What it does prevent is two `lakebench` processes racing the same Helm upgrade or the same SCC install through the apiserver -- which is what actually broke under parallel UAT.

Every mutating `admin` command acquires the lease. The strict variant of the Spark Operator watch-list mutation (`remove_namespace_from_watch(strict=True)`) also acquires it, so destroy paths and admin operations serialise against each other.

While a process holds the lease:

- **Interrupts are deferred.** A Ctrl-C, SIGTERM or SIGHUP prints "interrupt received while holding the cluster lease; finishing the shared change (hold budget N s left), then stopping", lets the shared change finish, releases the lease, and only then stops the command. A second interrupt repeats the budget left. A third aborts at once: the lease is still released (interrupts during the release are only recorded), and a running `kubectl`, `helm` or `oc` child and its process group get SIGTERM and 20 s to stop before SIGKILL (25 s at most in all). This applies on the main thread, where every lakebench command takes the lease; an ignored signal (`nohup`) stays ignored. While the lease is being acquired, a SIGTERM or SIGHUP left at its default stops the command as a Ctrl-C does, instead of killing it, so a lease the acquire already wrote is released.
- **Children run in their own session with a timeout.** `kubectl`, `helm` and `oc` started under the lease do not receive a terminal's Ctrl-C directly. Each gets a timeout from the hold budget: 750 s for deploy, destroy and run, 1800 s for `admin` commands. A mutating `helm` call keeps 60 s of the budget back, is not started with less than 60 s left for it, and gets its own `--timeout` at least 30 s shorter than its subprocess timeout (helm's 300 s default when that fits), so helm stops itself first. On a timeout the child gets the same SIGTERM-first stop; helm handles SIGTERM by cancelling the operation and marking the release failed, rather than the `pending-upgrade` a SIGKILL leaves. For helm the error then says to run `lakebench admin repair-operator`, which rolls a stale pending release back when the revision is safe (a `helm rollback` by hand skips that check and the lease). Deploy and destroy fail closed (destroy keeps the namespace), and `admin` commands exit 1 with the error.
- **API calls are bounded.** Every Kubernetes API call the lease makes, the namespace reads and delete destroy makes inside it, destroy's operator pod list and legacy SecretClass calls, and the namespace reads of the watch-list add, carry a 10 s connect and 60 s read timeout per attempt. The Kubernetes client retries a GET, PUT or DELETE whose read timed out up to three times, so one call can take about four minutes in the worst case. A lease write whose reply timed out but which landed is kept and released normally: the acquire recognises the lease by its write nonce, or by its holder id, which is unique to the acquire. An acquire that fails or is interrupted after its write committed deletes that lease rather than leaving it to the TTL.

The watch-list mutation's waits are on the hold budget too. The design splits the 750 s as 180 s for the helm upgrade (conflict retries included), 180 s for the OpenShift patch rollout, 180 s for the operator restart and its two rollout waits, 120 s for destroy's pod poll, 60 s kept back for recovery and 30 s for the rest. Each phase is one deadline for its steps and is also bounded by what the hold has left; a step with too little left raises instead of starting, and the strict remove then keeps the namespace. The split is a plan, not a guarantee: a destroy whose pod poll restarts the operator again, or an add that retries after an eviction, can need more than 750 s, and then fails closed with the watch-list change made and the namespace kept. A helm attempt is at most 120 s and is not started with under 60 s of its phase left; outside the lease helm gets no subprocess timeout. A deploy, run or destroy waiting for the lease to change the watch list waits three holds at that budget (37.5 min).

A `kill -9` or a lost host is not covered: the TTL reclaims the lease, and `lakebench admin repair-operator` repairs a half-applied upgrade. An interrupt that lands inside the acquire, after the lease was written, releases it: the holder id is unique to the acquire, so only that write matches.

## Deployment identity

On `deploy`, `_deploy_namespace` writes these identity annotations to the namespace:

- `lakebench.deployment/name: <cfg.name>`
- `lakebench.deployment/api-server: <sha256(cluster-CA-cert)>` -- workstation and in-cluster paths to the same cluster produce the same fingerprint because they read the same CA
- `lakebench.deployment/committed-sha: <short commit of the lakebench checkout that first stamped the namespace>` (informational; `-dirty` when that checkout had uncommitted changes or its state could not be read; never the shell's working directory; from a wheel install, the commit its build info names, marked `-buildinfo`; absent when no commit can be read; a later redeploy into the stamped namespace keeps the first value)
- `lakebench.deployment/stamped-at: <ISO 8601 timestamp>`

Deploy also writes three bookkeeping annotations to the same namespace:

- `lakebench.deployment/deploy-nonce` -- a fresh random value on every deploy. Destroy records it at start and stops if it changes (a redeploy into the same namespace, which the namespace UID cannot show).
- `lakebench.deployment/created-buckets` -- the buckets this deployment created. Destroy deletes only these.
- `lakebench.deployment/adopted-empty-buckets` -- pre-existing buckets that were empty when deploy adopted them on a backend without tagging. Destroy may empty them, never delete them.

The write uses an optimistic-concurrency PATCH keyed on the namespace's `resourceVersion`. If the PATCH conflicts, we re-read; if the re-read shows a foreign name, refuse.

Legacy pre-v1.5 namespaces (annotation-less but containing lakebench-labelled resources) are treated as UNSAFE, not legacy: `deploy --force-legacy` is the only way to claim them, and `destroy` refuses on missing annotations. Migration goes through `lakebench admin migrate-deployment <namespace>`, which stamps the annotations and renames the legacy fixed-name SecretClasses to the namespaced form.

## Watch-list hardening

`spark.jobNamespaces` is Category 4. Every mutation is a read-modify-write on shared Helm state; without serialisation, two deploys can each read the list before the other writes, and the second silently drops the first's namespace.

The strict path lease-gates the mutation and raises `WatchListMutationError` on failure. Destroy calls this path. On failure, destroy explicitly BLOCKS the namespace delete: deleting a namespace the operator still watches crash-loops the operator globally (`failed to wait for spark-application-controller caches to sync`), taking down SparkApplication reconciliation for every namespace on the cluster. Refusing to delete is the only safe response. The user is directed to `lakebench admin repair-operator` to reconcile.

A successful removal is not enough on its own: after the helm upgrade and the operator restart, a terminating operator pod can still carry the old `--namespaces=` list. Still inside the lease, so no deploy can re-add the namespace, destroy lists every pod in the operator namespace and waits, polling every 3 s for up to 120 s (less if the hold budget has less left), until no pod that is still running lists the namespace. A finished pod (an evicted controller, `Failed`) and an empty `--namespaces=` (watch everything) do not count. An operator Deployment whose pod template still lists the namespace counts too, and ends the wait at once. A stale pod that is running and not being deleted means no restart replaced it, so destroy restarts the operator once more, inside the lease. If something still lists the namespace, destroy keeps it and exits 1: "Namespace X NOT deleted: operator pods [p] still watch it; ...", pointing at `helm history` and `admin repair-operator` when a Deployment is listed (repair reads the helm values and both Deployments' `--namespaces`). If the pods or Deployments cannot be listed, the namespace is kept too.

The add path (`_add_namespace_to_watch`) is lease-gated too, with three outcomes on lease acquire: `locked` (proceed under the lease), `unlocked` (genuine no-cluster case, e.g. workstation without kubeconfig -- safe to proceed with no other writer possible), or `refuse` (`ClusterLockHeld` or RBAC denial -- return False rather than proceed unlocked into the race).

## Bucket ownership

Every `ensure_buckets` call writes, on each bucket this deployment owns:

- `lakebench.deployment=<cfg.name>`
- `lakebench.workload=<cfg.workload_schema>`
- `lakebench.created=true`, only on a bucket this deployment created
- `lakebench.cluster=<fingerprint>`, this cluster's API-server fingerprint

Then reads the tags back and verifies the round trip, the cluster tag included. Deploy refuses when it cannot compute the fingerprint (kubeconfig with no CA data).

**The cluster stamp.** A deployment name alone is not unique across clusters that share an object store: a deployment of the same name on another cluster used to adopt this one's still-empty bucket and later empty it. Every verdict now also checks the cluster:

| Row | Stamp found | Verdict | Deploy | Destroy, clean, continuous reset |
|---|---|---|---|---|
| 1 | name and cluster ours | MATCH | use | may empty; delete if created |
| 2 | name ours, cluster not | FOREIGN_CLUSTER | refuse | keep, FAILED |
| 3 | name ours (or no marker on a tagless backend), no cluster stamp, in this namespace's created-buckets record (not the adopted-empty one, which 1.6 also wrote for another cluster's empty bucket), and this run has a fingerprint | LEGACY_PROVEN | stamp the cluster, then as row 1 | stamp the cluster, then as row 1 |
| 4 | name ours, no cluster stamp, not in the record (a bucket an earlier lakebench adopted) | LEGACY_UNPROVEN | use for reads and writes, never stamp | keep; `admin reclaim-bucket` (owner) can claim it |
| 5 | the stamp is this cluster's old fingerprint (CA rotated) | FOREIGN_CLUSTER, hint names the CA | refuse | keep |
| 6 | name not ours | MISMATCH | refuse | keep |
| 7 | no stamp, not in the record | ABSENT on a tagged backend (needs `--force-legacy`, as before); on a tagless one, used but adopted only with `--force-legacy` | | keep |
| 8 | a cluster stamp (or row 3's record), but this run has no fingerprint | UNVERIFIED_CLUSTER | (deploy already refused) | keep: "Destroy NOT completed: this cluster has no fingerprint" |

`--allow-unverified-cluster` waives the namespace check only; with no fingerprint every stamped bucket is still kept. On a tagless backend the stamp is the owner marker object `.lakebench/owner.json` (`{deployment, cluster, namespace, namespace_uid, created_at, lakebench_version}`). Deploy writes it on a bucket it created, one this namespace's record lists, or one it adopts while empty with `--force-legacy`, and `admin reclaim-bucket` writes it as the owner's override. The write is a conditional PUT (`IfNoneMatch="*"`) where the backend enforces it, so two clusters racing for one empty bucket get one winner; enforcement is first proved by a probe on a throwaway key, `.lakebench/probe-<uuid>` (200 then 412, then deleted), never on the marker key, so the probe cannot overwrite another cluster's marker. A backend that ignores the header or has no conditional writes gets a read, a plain PUT, a read-back, a 2 s wait and a second read; two clusters writing in that window can still both believe they won (open risk R12). Deploy records how it wrote markers in the namespace annotation `lakebench.deployment/marker-write` (`conditional` or `unconditional`). Keys under `.lakebench/` are never user data: every emptiness check, object count and size skips them (a backend that ignores `StartAfter` gets a full listing), `clean`, `--regenerate` and destroy's emptying keep them, and destroy deletes them only right before it deletes the bucket itself. A bucket destroy keeps (`--keep-buckets`, `create_buckets: false`, one it did not create) keeps its marker, and destroy stamps a row-3 bucket first, so the bucket stays provably this deployment's after the namespace and its record are gone. A backend that accepts the write but drops it (the read-back returns no tags or a different name) is refused rather than trusted. A backend that does not implement tagging at all (`NotImplemented`, as on FlashBlade) takes the name-prefix and namespace-record fallback described under Category 1. Destroy and clean read the tag before mutating: a mismatch is a hard refusal (`--force` does not bypass), an absent tag on a legacy bucket requires `--force-legacy`, a missing bucket is a no-op.

`lakebench admin reclaim-bucket <name>` rewrites the tag under the cluster lease with an object-count check inside the lease scope; a bucket with objects requires `--force-nonempty` acknowledgement.

## Verification

Unit-level proxies run on every commit:
- `tests/test_ownership.py` -- identity annotations, bucket tags, api-server fingerprint parity between workstation and in-cluster paths.
- `tests/test_cluster_lock.py` -- lease acquire/steal/release, ttl and force-release semantics.
- `tests/test_admin.py` -- every admin subcommand, migration idempotence, reclaim refusal on non-empty.
- `tests/test_watch_list_strict.py` -- strict-mode dispatch, lease-held refusal, WatchListMutationError text pointing at `admin repair-operator`.
- `tests/test_destroy_unwatches_namespace.py` -- destroy blocks namespace delete on watch-list failure.

Six live UAT scenarios run once per release against a real cluster (maintainer-run; the scripts are not in this repository):
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

Destroy cleans up the legacy fixed-name SecretClasses only when no other lakebench namespace is left cluster-wide: one with the `lakebench.deployment/name` annotation, or the `app.kubernetes.io/managed-by` or `app.kubernetes.io/name` label set to `lakebench` (so pre-v1.5 annotationless deployments still count), in any phase. The count and the deletes run under the cluster lease, where `migrate-deployment` also works; when neither legacy SecretClass exists the lease is not taken, and when the lease stays held for 600 s or the namespace list fails the cleanup is skipped and the legacy names are kept. Until every deployment on a cluster has migrated, the legacy names remain reserved.
