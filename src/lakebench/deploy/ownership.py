"""Deployment identity + resource ownership for shared clusters.

Every lakebench deployment carries an identity: a name and a fingerprint of
the Kubernetes API server it was deployed to. That identity is stamped onto
every resource lakebench creates and checked before any destructive
mutation. If the stamp on a resource does not match the current
deployment, we refuse rather than warn.

This is the machinery behind the invariant "destroying deployment A does
not affect deployment B running in parallel." See
``docs/internal/namespace-isolation.md`` for the design rationale
and category taxonomy this module enforces.

The pieces:

- ``api_server_fingerprint`` produces a stable identifier for the target
  cluster from the kubeconfig context. Local kubectl-context names differ
  per workstation, so we hash the cluster's CA certificate material: two
  engineers on the same cluster produce the same fingerprint, and the
  workstation and in-cluster paths against the same cluster also agree
  (both see the same CA cert, even though they use different endpoint
  URLs).
- ``stamp_namespace`` writes deployment-identity annotations on the
  namespace under optimistic-concurrency. A conflict resolves to either
  "already claimed by us -- proceed" or "claimed by someone else --
  refuse".
- ``verify_namespace_identity`` reads those annotations and returns a
  verdict that destroy paths and other identity-sensitive commands act on.
- ``write_bucket_ownership_tag`` and ``read_bucket_ownership_tag`` are the
  S3 side: the tag is written unconditionally on every ensure_buckets
  call, the read verifies the round trip, and destroy refuses on mismatch.

The failure modes this module explicitly forbids (silent-warn-and-proceed
was the class the shared-cluster review caught last time):

- Cross-context destroy: the api-server fingerprint on the namespace does
  not match the current cluster.
- Cross-deployment destroy: name annotation on the namespace does not
  match ``cfg.name``.
- Cross-deployment bucket empty: bucket tag does not match ``cfg.name``.
- Legacy namespace or bucket: no annotation / tag present, refuse by
  default. Explicit opt-in flag on ``deploy`` re-stamps it; ``destroy``
  never bypasses.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# Annotation keys stamped onto every lakebench-managed namespace.
ANNOTATION_DEPLOYMENT_NAME = "lakebench.deployment/name"
ANNOTATION_API_SERVER = "lakebench.deployment/api-server"
ANNOTATION_COMMITTED_SHA = "lakebench.deployment/committed-sha"
ANNOTATION_STAMPED_AT = "lakebench.deployment/stamped-at"

# Bucket tag keys.
TAG_DEPLOYMENT_NAME = "lakebench.deployment"
TAG_WORKLOAD_SCHEMA = "lakebench.workload"
# Set only on buckets deploy itself created. Destroy deletes a bucket
# only when it carries this marker (or is listed in the namespace annotation
# below); buckets deploy adopted are emptied but kept.
TAG_CREATED_BY_LAKEBENCH = "lakebench.created"
# The cluster that claimed a bucket. Tagged
# backends carry it as a tag, tagless ones (FlashBlade) in the owner marker
# object. Without it, a deployment of the same name on another cluster that
# shares the object store could adopt, and later empty, this one's bucket.
TAG_CLUSTER = "lakebench.cluster"
# Keys under this prefix are Lakebench's own bookkeeping, never user data.
MARKER_PREFIX = ".lakebench/"
OWNER_MARKER_KEY = ".lakebench/owner.json"
ANNOTATION_CREATED_BUCKETS = "lakebench.deployment/created-buckets"
# Buckets deploy adopted while they held no objects, on a backend without
# bucket tagging. Everything in them was written by this deployment, so
# destroy may empty them (never delete: lakebench did not create them).
ANNOTATION_ADOPTED_EMPTY_BUCKETS = "lakebench.deployment/adopted-empty-buckets"
# A fresh random value written by every deploy. Destroy records it at start
# and stops if it changes: that is a redeploy into the same namespace, which
# the UID cannot show (same incarnation, or create_namespace=false).
ANNOTATION_DEPLOY_NONCE = "lakebench.deployment/deploy-nonce"
# Written beside the nonce by a v1.7 deploy that recorded the nonce in its
# directory's state. A v1.6 directory with no state is refused a
# nameless teardown of a namespace carrying it: the deployment moved on.
ANNOTATION_STATE_SCHEMA = "lakebench.deployment/state-schema"
# How this deployment's owner markers were written ("conditional" or
# "unconditional"), so a backend that ignores IfNoneMatch is on record (R12).
ANNOTATION_MARKER_WRITE = "lakebench.deployment/marker-write"

# Namespace name max length (matches K8s + doubles as the bucket-tag length
# guard: AWS caps tag values at 256, so 63 chars is well within bounds).
DEPLOYMENT_NAME_MAX = 63


class IdentityVerdict(str, Enum):
    """Outcome of comparing a resource's identity against the current run."""

    #: Identity annotations/tags match this deployment. Safe to proceed.
    MATCH = "match"
    #: Resource carries a foreign identity. Refuse.
    MISMATCH = "mismatch"
    #: Resource has no identity annotations/tags. Legacy; caller decides.
    ABSENT = "absent"
    #: Resource does not exist at all.
    NOT_FOUND = "not_found"
    #: Backend does not implement the tagging API (e.g. FlashBlade returns
    #: ``NotImplemented`` on ``GetBucketTagging`` / ``PutBucketTagging``).
    #: Tag-based ownership is impossible; callers must fall back to a
    #: weaker check (name-prefix on buckets) or refuse.
    #: Among the ownership verdicts it means: tagless, no owner marker, and not
    #: in this namespace's created or adopted-empty record (row 7 of the
    #: matrix in ``verify_bucket_ownership``).
    UNSUPPORTED = "unsupported"
    #: Ownership row 2 (and 5): the bucket is this deployment's by name but
    #: was claimed from another cluster (or this cluster's API-server CA
    #: changed). Refuse; never empty it.
    FOREIGN_CLUSTER = "foreign_cluster"
    #: Ownership row 3: no cluster stamp, but this namespace's created or
    #: adopted-empty record proves this cluster made or adopted it. Stamp
    #: the cluster, then treat it as MATCH.
    LEGACY_PROVEN = "legacy_proven"
    #: Ownership row 4: tagged with this deployment's name but no cluster stamp
    #: and not in the record (a bucket an earlier lakebench adopted). Usable
    #: for reads and writes, never stamped, never emptied or deleted.
    LEGACY_UNPROVEN = "legacy_unproven"
    #: Ownership row 8: the bucket carries a cluster stamp, but this run cannot
    #: compute its own cluster fingerprint. Keep it.
    UNVERIFIED_CLUSTER = "unverified_cluster"


class BucketTaggingUnsupported(Exception):
    """Raised when the backend does not implement bucket tagging.

    Distinguishes an inability to check from a check that ran and returned
    "no tags." The former means we cannot use tagging as the ownership
    signal at all; the latter is the legacy-bucket case.
    """


@dataclass(frozen=True)
class IdentityReport:
    """What a verify call found. All fields together, no dangling defaults.

    ``verdict`` is the actionable answer. The rest is diagnostics for the
    caller to include in a user-visible refusal message.
    """

    verdict: IdentityVerdict
    resource_name: str
    expected_deployment: str
    found_deployment: str | None = None
    found_api_server: str | None = None
    current_api_server: str | None = None
    hint: str | None = None
    #: Buckets only: False when the backend has no bucket tagging (the
    #: verdict then came from the owner marker or the namespace record).
    tagged: bool = True


# ---------------------------------------------------------------------------
# API server fingerprint
# ---------------------------------------------------------------------------


def api_server_fingerprint(context: str | None = None) -> str | None:
    """Return a stable 12-hex-digit identifier for the cluster.

    We hash the cluster's CA certificate material. Two engineers hitting
    the same cluster produce the same fingerprint even though their local
    context names differ; more importantly, a workstation context and an
    in-cluster pod context targeting the same cluster ALSO produce the
    same fingerprint, because the cluster's CA is stable regardless of
    how you reach it (F2a: earlier revision hashed server URL + CA,
    which false-mismatched workstation vs in-cluster against the same
    cluster because they see different server URLs).

    **Known limitation (F-3)**: two clusters that share a CA
    (e.g. dev rings bootstrapped from the same self-signed org CA)
    collide on this fingerprint. If your workflow includes multiple
    clusters that share PKI, verify the current ``kubectl`` context
    manually before running ``lakebench destroy``. The name annotation
    still provides second-line defence -- destroy will refuse if the
    deployment name does not match -- but two clusters with the same
    name AND the same CA would pass the check. Adding the API-server
    hostname would distinguish them, but hostnames differ between
    workstation and in-cluster reachability paths (external URL vs
    KUBERNETES_SERVICE_HOST), so we cannot include it without
    reintroducing F2a. This is a documented trade-off.

    Returns None only when no CA material can be located at all. None is
    treated as "cannot verify" by callers; refusal on asymmetric None
    still fires unless the caller opts in with allow_unverified_cluster.
    """
    try:
        from kubernetes import config as _kube_config
    except ImportError:  # pragma: no cover -- kubernetes lib is a hard dep
        return None

    # The cluster this process's API clients are pinned to: its CA
    # was hashed when the context was loaded, so a kubeconfig rewritten
    # since cannot change what a deploy stamps or a destroy compares.
    from lakebench.k8s.target import active_target

    pinned = active_target()
    if pinned is not None and (not context or context == pinned.context):
        if pinned.ca_fp_known:
            return pinned.ca_fingerprint
        # The cluster entry could not be read at activation: read it now,
        # but only from an entry that still names the pinned server; a
        # rewritten one would hash another cluster's CA.
        if pinned.in_cluster:
            return _try_incluster_fingerprint()
        from lakebench.k8s.target import _ca_fingerprint, _kubeconfig_cluster_block

        block = _kubeconfig_cluster_block(str(pinned.context))
        server = str((block or {}).get("server") or "").rstrip("/")
        if block is None or (pinned.api_server and server != pinned.api_server):
            logger.debug(
                "api_server_fingerprint: context %r no longer names %s",
                pinned.context,
                pinned.api_server,
            )
            return None
        # Hash the entry just checked, not a second read of the file.
        return _ca_fingerprint(block)

    try:
        contexts, active = _kube_config.list_kube_config_contexts()
    except Exception as e:  # noqa: BLE001 -- kubeconfig may be missing
        logger.debug("api_server_fingerprint: cannot list contexts: %s", e)
        return _try_incluster_fingerprint()

    if not contexts:
        return _try_incluster_fingerprint()

    cluster_name: str | None = None
    if context:
        # Find the requested context's cluster.
        for ctx in contexts:
            if ctx.get("name") == context:
                cluster_name = ctx.get("context", {}).get("cluster")
                break
        if cluster_name is None:
            logger.debug("api_server_fingerprint: context %r not found", context)
            return None
    else:
        if active is None:
            return _try_incluster_fingerprint()
        cluster_name = active.get("context", {}).get("cluster")

    if not cluster_name:
        return None

    # Reach into the raw kubeconfig to pull the cluster block. The
    # high-level loader doesn't expose it; the low-level KubeConfigMerger
    # does. Different kubernetes-client versions wrap this in a ConfigNode
    # (list-of-dicts under `.value` per entry) vs a plain dict, so
    # tolerate both.
    try:
        merger = _kube_config.kube_config.KubeConfigMerger(
            _kube_config.KUBE_CONFIG_DEFAULT_LOCATION
        )
        raw = merger.config.value
        raw_clusters = raw.get("clusters", []) if isinstance(raw, dict) else []
    except Exception as e:  # noqa: BLE001
        logger.debug("api_server_fingerprint: cannot read raw kubeconfig: %s", e)
        return None

    for entry in raw_clusters:
        # ConfigNode wraps per-entry dicts under `.value` on some client
        # versions; unwrap when needed.
        entry_dict = entry.value if hasattr(entry, "value") else entry
        if not isinstance(entry_dict, dict):
            continue
        if entry_dict.get("name") != cluster_name:
            continue
        block_raw = entry_dict.get("cluster", {})
        block = block_raw.value if hasattr(block_raw, "value") else block_raw
        if not isinstance(block, dict):
            block = {}
        # Normalise to raw CA bytes so the workstation path and the
        # in-cluster path (which reads bytes off disk) hash the SAME
        # material for the same cluster (F2a).
        ca_bytes = _load_ca_bytes(block)
        if not ca_bytes:
            return None
        return hashlib.sha256(ca_bytes).hexdigest()[:12]

    return None


def _load_ca_bytes(cluster_block: dict[str, Any]) -> bytes:
    """CA bytes of a kubeconfig cluster block (``k8s.target.load_ca_bytes``)."""
    from lakebench.k8s.target import load_ca_bytes

    return load_ca_bytes(cluster_block)


def _try_incluster_fingerprint() -> str | None:
    """Fingerprint an in-cluster (pod) context via the mounted service-account
    CA. Returns None if the path does not exist (running outside a pod).

    Hashes ONLY the CA bytes -- same as the kubeconfig path (F2a) so a
    pod-context fingerprint matches the workstation fingerprint for the
    same cluster.
    """
    try:
        with open("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt", "rb") as f:
            ca = f.read()
    except OSError:
        return None
    return hashlib.sha256(ca).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Namespace identity
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def stamp_namespace(
    core_v1: Any,
    namespace: str,
    deployment_name: str,
    api_server: str | None,
    committed_sha: str | None = None,
    force_legacy: bool = False,
    max_retries: int = 3,
) -> IdentityReport:
    """Stamp deployment-identity annotations on a namespace.

    Uses an optimistic-concurrency PATCH keyed on the current
    ``resourceVersion`` so two parallel deploys racing an annotation-less
    namespace cannot both silently claim it -- the loser sees a 409 and
    re-reads. If the re-read shows the same deployment already stamped,
    we proceed; if it shows a different deployment, we refuse.

    Legacy annotation-less namespaces are treated as UNSAFE by default:
    the caller must pass ``force_legacy=True`` to overwrite them. This
    catches the "annotation-less namespace already has running lakebench
    resources inside" case that revision-1 of the design missed.
    """
    if len(deployment_name) > DEPLOYMENT_NAME_MAX:
        raise ValueError(
            f"deployment_name is {len(deployment_name)} chars; max is "
            f"{DEPLOYMENT_NAME_MAX} (matches K8s namespace + S3 tag limits)"
        )

    from kubernetes.client.rest import ApiException  # local import: light dep

    from lakebench.k8s.lease_state import request_timeout_kw

    for attempt in range(max_retries):
        try:
            ns = core_v1.read_namespace(namespace, **request_timeout_kw())
        except ApiException as e:
            if e.status == 404:
                return IdentityReport(
                    verdict=IdentityVerdict.NOT_FOUND,
                    resource_name=namespace,
                    expected_deployment=deployment_name,
                    hint=f"namespace {namespace!r} does not exist",
                )
            raise

        existing = (ns.metadata.annotations or {}) if ns.metadata else {}
        existing_name = existing.get(ANNOTATION_DEPLOYMENT_NAME)
        existing_server = existing.get(ANNOTATION_API_SERVER)

        # Already stamped with a foreign identity -- refuse cold.
        if existing_name and existing_name != deployment_name:
            return IdentityReport(
                verdict=IdentityVerdict.MISMATCH,
                resource_name=namespace,
                expected_deployment=deployment_name,
                found_deployment=existing_name,
                found_api_server=existing_server,
                current_api_server=api_server,
                hint=(
                    f"namespace {namespace!r} is already claimed by "
                    f"deployment {existing_name!r}. Pick a different "
                    "namespace, or destroy the existing deployment first."
                ),
            )

        # Already stamped with our identity. The committed-sha stamp says
        # which code deployed last, so a redeploy from other code refreshes
        # it (or drops it when this deploy cannot name its commit); the
        # identity annotations and stamped-at are left as they are.
        if existing_name == deployment_name and existing_server == api_server:
            if (existing.get(ANNOTATION_COMMITTED_SHA) or None) == (committed_sha or None):
                return IdentityReport(
                    verdict=IdentityVerdict.MATCH,
                    resource_name=namespace,
                    expected_deployment=deployment_name,
                    found_deployment=existing_name,
                    found_api_server=existing_server,
                    current_api_server=api_server,
                )
            # Only that annotation, under the read's resourceVersion: a
            # strategic merge patch leaves the other keys alone and deletes a
            # key set to None.
            body = {
                "metadata": {
                    "annotations": {ANNOTATION_COMMITTED_SHA: committed_sha or None},
                    "resourceVersion": ns.metadata.resource_version,
                }
            }
            try:
                core_v1.patch_namespace(namespace, body, **request_timeout_kw())
            except Exception as e:  # noqa: BLE001 -- see below
                if isinstance(e, ApiException) and e.status == 409 and attempt + 1 < max_retries:
                    # Re-read: the identity checks above run again on it.
                    time.sleep(0.2 * (attempt + 1))
                    continue
                # The identity is verified; the sha stamp is informational,
                # so failing to refresh it (API or transport error) never
                # refuses the deploy.
                logger.warning(
                    "stamp_namespace: could not refresh %s on %s: %s",
                    ANNOTATION_COMMITTED_SHA,
                    namespace,
                    e,
                )
            return IdentityReport(
                verdict=IdentityVerdict.MATCH,
                resource_name=namespace,
                expected_deployment=deployment_name,
                found_deployment=existing_name,
                found_api_server=existing_server,
                current_api_server=api_server,
            )

        # Legacy annotation-less namespace: opt in explicitly.
        if not existing_name and not force_legacy:
            return IdentityReport(
                verdict=IdentityVerdict.ABSENT,
                resource_name=namespace,
                expected_deployment=deployment_name,
                found_deployment=None,
                hint=(
                    f"namespace {namespace!r} exists without lakebench "
                    "identity annotations. FIRST verify your cluster "
                    "context with `oc whoami && kubectl config "
                    "current-context` and confirm it matches this "
                    "deployment's expected cluster. Only after that "
                    "check, and only as a last resort, pass "
                    "--force-legacy on deploy to claim it (destroy "
                    "still refuses without migration)."
                ),
            )

        # Build the annotation patch. Use resourceVersion-based OCC.
        annotations = dict(existing)
        annotations[ANNOTATION_DEPLOYMENT_NAME] = deployment_name
        if api_server:
            annotations[ANNOTATION_API_SERVER] = api_server
        if committed_sha:
            annotations[ANNOTATION_COMMITTED_SHA] = committed_sha
        annotations[ANNOTATION_STAMPED_AT] = _now_iso()

        body = {
            "metadata": {
                "annotations": annotations,
                "resourceVersion": ns.metadata.resource_version,
            }
        }
        try:
            core_v1.patch_namespace(namespace, body, **request_timeout_kw())
        except ApiException as e:
            if e.status == 409:
                # Conflict: someone else patched the namespace between our
                # read and our write. Re-read and reconsider.
                logger.info(
                    "stamp_namespace: 409 on attempt %d/%d for %s; retrying",
                    attempt + 1,
                    max_retries,
                    namespace,
                )
                time.sleep(0.2 * (attempt + 1))
                continue
            raise

        return IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name=namespace,
            expected_deployment=deployment_name,
            found_deployment=deployment_name,
            found_api_server=api_server,
            current_api_server=api_server,
        )

    # Exhausted retries -- last thing we saw. Report as mismatch so the
    # caller refuses to proceed rather than deploying blind.
    return IdentityReport(
        verdict=IdentityVerdict.MISMATCH,
        resource_name=namespace,
        expected_deployment=deployment_name,
        hint=(
            f"failed to stamp namespace {namespace!r} after {max_retries} "
            "OCC retries; another process may be claiming it concurrently"
        ),
    )


def verify_namespace_identity(
    core_v1: Any,
    namespace: str,
    expected_deployment: str,
    expected_api_server: str | None,
    allow_unverified_cluster: bool = False,
) -> IdentityReport:
    """Read namespace annotations and return the verdict.

    This never mutates. Callers (destroy, admin migrate) decide what to do
    with an ABSENT verdict; MISMATCH is unconditional refuse.
    """
    from kubernetes.client.rest import ApiException

    try:
        ns = core_v1.read_namespace(namespace)
    except ApiException as e:
        if e.status == 404:
            return IdentityReport(
                verdict=IdentityVerdict.NOT_FOUND,
                resource_name=namespace,
                expected_deployment=expected_deployment,
            )
        raise

    annotations = (ns.metadata.annotations or {}) if ns.metadata else {}
    found_name = annotations.get(ANNOTATION_DEPLOYMENT_NAME)
    found_server = annotations.get(ANNOTATION_API_SERVER)

    if not found_name:
        return IdentityReport(
            verdict=IdentityVerdict.ABSENT,
            resource_name=namespace,
            expected_deployment=expected_deployment,
            hint=(
                f"namespace {namespace!r} has no lakebench identity. "
                "Run `lakebench admin migrate-deployment` to claim it."
            ),
        )

    if found_name != expected_deployment:
        return IdentityReport(
            verdict=IdentityVerdict.MISMATCH,
            resource_name=namespace,
            expected_deployment=expected_deployment,
            found_deployment=found_name,
            found_api_server=found_server,
            current_api_server=expected_api_server,
            hint=(
                f"namespace {namespace!r} is owned by deployment "
                f"{found_name!r}, not {expected_deployment!r}. Refusing."
            ),
        )

    # Symmetric-both-present: enforce fingerprint match.
    # Asymmetric (one side None): refuse unless --allow-unverified-cluster.
    # Symmetric-both-None: refuse unless --allow-unverified-cluster (F3:
    # a fully-offline destroy against a fully-offline deploy would
    # otherwise match by name alone -- and namespace-name collisions
    # across clusters absolutely happen at Tier-1 shops).
    if found_server is not None and expected_api_server is not None:
        if found_server != expected_api_server:
            return IdentityReport(
                verdict=IdentityVerdict.MISMATCH,
                resource_name=namespace,
                expected_deployment=expected_deployment,
                found_deployment=found_name,
                found_api_server=found_server,
                current_api_server=expected_api_server,
                hint=(
                    f"namespace {namespace!r} was deployed to cluster "
                    f"{found_server[:12]!r}, current context is "
                    f"{expected_api_server[:12]!r}. Wrong kubectl context?"
                ),
            )
    elif not allow_unverified_cluster:
        # F2a/F3: asymmetric OR both-None. Cannot prove same cluster.
        # F-5: split the hint so the user knows which side broke.
        if found_server is None and expected_api_server is None:
            broken = (
                "neither the deploy nor this destroy could compute a "
                "cluster fingerprint (both kubeconfigs missing CA data?)"
            )
        elif found_server is None:
            broken = (
                "the deploy did not stamp a cluster fingerprint "
                "(deploy-time kubeconfig had no CA data)"
            )
        else:
            broken = (
                "this destroy cannot compute a cluster fingerprint "
                "(current kubeconfig has no CA data)"
            )
        return IdentityReport(
            verdict=IdentityVerdict.MISMATCH,
            resource_name=namespace,
            expected_deployment=expected_deployment,
            found_deployment=found_name,
            found_api_server=found_server,
            current_api_server=expected_api_server,
            hint=(
                f"namespace {namespace!r}: {broken}. Cannot verify same "
                "cluster. Pass --allow-unverified-cluster to override "
                "(only when you are certain the current context is "
                "correct)."
            ),
        )

    return IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name=namespace,
        expected_deployment=expected_deployment,
        found_deployment=found_name,
        found_api_server=found_server,
        current_api_server=expected_api_server,
    )


# ---------------------------------------------------------------------------
# Bucket ownership tag
# ---------------------------------------------------------------------------


class BucketOwnershipError(Exception):
    """Raised when a bucket cannot be tagged, or its tag disagrees."""


def write_bucket_ownership_tag(
    boto_client: Any,
    bucket: str,
    deployment_name: str,
    workload_schema: str | None = None,
    created: bool = False,
    cluster: str | None = None,
) -> None:
    """Write the ownership tag and verify the round trip.

    ``cluster`` is this cluster's stamp (``cluster_stamp``); when
    given it is written as ``lakebench.cluster`` and verified too. A row-4
    bucket (``LEGACY_UNPROVEN``) is never passed here.

    ``created`` adds the created-by-lakebench marker. The whole tag
    set is rewritten, so a caller re-tagging a bucket lakebench created on an
    earlier deploy must pass ``created=True`` again to keep the marker.

    Called by ``ensure_buckets`` unconditionally: not only on bucket create.
    A bucket created by a prior deploy that raced ours (or a legacy bucket
    reclaimed by admin) would otherwise sit tag-less and read as legacy on
    every subsequent destroy.

    Round-trip verify catches:
    - Backends that silently drop tagging (SeaweedFS class).
    - Truncation on unusually long deployment_name (guarded at schema, but
      belt-and-braces).
    - PutBucketTagging succeeded, GetBucketTagging returns 404 (async
      propagation on some object stores).

    **TOCTOU note**: the read-back proves *we* were the last writer at the
    time of our GET, not that we are still the last writer when destroy
    reads later. Two deploys racing an untagged bucket -- one wins the
    final PUT, the other's later destroy sees the winner's tag and
    refuses. That refusal is the correct outcome (last-writer-wins on
    ownership; the loser's data still lives in the winner's bucket, so
    the loser cannot safely wipe it anyway). Callers must not treat a
    successful round-trip verify as "no one else can claim this bucket
    later" -- it is only "no one had claimed it before our PUT completed."
    """
    if len(deployment_name) > DEPLOYMENT_NAME_MAX:
        raise BucketOwnershipError(
            f"deployment_name {len(deployment_name)} chars exceeds "
            f"{DEPLOYMENT_NAME_MAX}; would truncate in the bucket tag."
        )

    tag_set = [{"Key": TAG_DEPLOYMENT_NAME, "Value": deployment_name}]
    if workload_schema:
        tag_set.append({"Key": TAG_WORKLOAD_SCHEMA, "Value": workload_schema})
    if created:
        tag_set.append({"Key": TAG_CREATED_BY_LAKEBENCH, "Value": "true"})
    if cluster:
        tag_set.append({"Key": TAG_CLUSTER, "Value": cluster})

    from botocore.exceptions import ClientError

    try:
        boto_client.put_bucket_tagging(
            Bucket=bucket,
            Tagging={"TagSet": tag_set},
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "NotImplemented":
            # Backend does not implement bucket tagging at all (e.g.
            # FlashBlade returns HTTP 501 NotImplemented). Cannot
            # enforce tag-based ownership. Caller handles the fallback
            # identity check. Narrow to NotImplemented only: HTTP 405
            # MethodNotAllowed and other 4xx codes indicate a
            # permissions or policy problem, not a missing feature.
            raise BucketTaggingUnsupported(
                f"bucket {bucket!r}: PutBucketTagging returned NotImplemented. "
                "Backend does not implement bucket tagging."
            ) from e
        raise

    # Read back and verify. If the backend does not support tagging, or
    # dropped the write, we surface it here instead of pretending the
    # bucket is now owned.
    try:
        got = read_bucket_ownership_tag(boto_client, bucket)
    except BucketTaggingUnsupported:
        # Should not happen if PUT succeeded above, but handle to keep
        # the invariant "raise Unsupported, never lie about ownership."
        raise
    if got is None:
        raise BucketOwnershipError(
            f"bucket {bucket!r}: PutBucketTagging succeeded but "
            "GetBucketTagging returned no tags. Backend may not support "
            "bucket tagging (see docs/storage-backends.md)."
        )
    if got.get(TAG_DEPLOYMENT_NAME) != deployment_name:
        raise BucketOwnershipError(
            f"bucket {bucket!r}: tag round-trip mismatch. Wrote "
            f"{deployment_name!r}, read {got.get(TAG_DEPLOYMENT_NAME)!r}."
        )
    if cluster and got.get(TAG_CLUSTER) != cluster:
        raise BucketOwnershipError(
            f"bucket {bucket!r}: cluster tag round-trip mismatch. Wrote "
            f"{cluster!r}, read {got.get(TAG_CLUSTER)!r}."
        )


def read_bucket_ownership_tag(boto_client: Any, bucket: str) -> dict[str, str] | None:
    """Return the tag map for a bucket, or None if the bucket has no tags.

    Never raises for the "no tags on this bucket" case (S3 returns
    NoSuchTagSet). Raises ``BucketTaggingUnsupported`` for backends that
    do not implement the tagging API at all (e.g. FlashBlade returns
    ``NotImplemented``, not ``NoSuchTagSet``). Other errors propagate.
    """
    from botocore.exceptions import ClientError

    try:
        resp = boto_client.get_bucket_tagging(Bucket=bucket)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchTagSet", "NoSuchTagSetError"):
            return None
        if code == "NotImplemented":
            # FlashBlade returns HTTP 501 NotImplemented.
            # Narrow to this code only: MethodNotAllowed and other
            # 4xx codes indicate permissions / policy problems, not a
            # missing feature.
            raise BucketTaggingUnsupported(
                f"bucket {bucket!r}: GetBucketTagging returned NotImplemented. "
                "Backend does not implement bucket tagging."
            ) from e
        raise

    return {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}


def read_created_buckets(core_v1: Any, namespace: str) -> set[str]:
    """Buckets the namespace annotation records as created by lakebench.

    Raises on read errors other than NotFound so a caller cannot mistake
    "could not read" for "none were created" in a way that matters: the
    only effect of an empty set is that buckets are kept.
    """
    from kubernetes.client.rest import ApiException

    try:
        ns = core_v1.read_namespace(namespace)
    except ApiException as e:
        if e.status == 404:
            return set()
        raise
    anns = (ns.metadata.annotations if ns and ns.metadata else None) or {}
    raw = anns.get(ANNOTATION_CREATED_BUCKETS) or ""
    if not isinstance(raw, str):
        return set()
    return {b.strip() for b in raw.split(",") if b.strip()}


def read_adopted_empty_buckets(core_v1: Any, namespace: str) -> set[str]:
    """Buckets recorded as adopted while empty (see ANNOTATION_ADOPTED_EMPTY_BUCKETS).

    Same error contract as ``read_created_buckets``.
    """
    from kubernetes.client.rest import ApiException

    try:
        ns = core_v1.read_namespace(namespace)
    except ApiException as e:
        if e.status == 404:
            return set()
        raise
    anns = (ns.metadata.annotations if ns and ns.metadata else None) or {}
    raw = anns.get(ANNOTATION_ADOPTED_EMPTY_BUCKETS) or ""
    if not isinstance(raw, str):
        return set()
    return {b.strip() for b in raw.split(",") if b.strip()}


def record_adopted_empty_buckets(core_v1: Any, namespace: str, buckets: list[str]) -> None:
    """Add ``buckets`` to the adopted-while-empty annotation (union)."""
    if not buckets:
        return
    merged = read_adopted_empty_buckets(core_v1, namespace) | set(buckets)
    body = {
        "metadata": {"annotations": {ANNOTATION_ADOPTED_EMPTY_BUCKETS: ",".join(sorted(merged))}}
    }
    core_v1.patch_namespace(namespace, body)


def tagless_contents_are_ours(core_v1: Any, namespace: str, bucket: str) -> bool:
    """On a backend without tagging: may this deployment empty ``bucket``?

    The name alone is not proof (deploy adopts a pre-existing bucket that
    merely prefix-matches). True only when the namespace records that
    lakebench created the bucket (the adopted-empty record a 1.6
    deploy wrote is not proof; a 1.7 adoption carries an owner marker
    instead). Callers still apply the longest-prefix name check. Read errors
    propagate; the caller refuses on them.
    """
    return bucket in read_created_buckets(core_v1, namespace)


def record_created_buckets(core_v1: Any, namespace: str, buckets: list[str]) -> None:
    """Add ``buckets`` to the namespace's created-buckets annotation.

    Union with what is already recorded, so a redeploy (which sees the
    buckets as existing) does not forget that an earlier deploy created them.
    """
    if not buckets:
        return
    merged = read_created_buckets(core_v1, namespace) | set(buckets)
    body = {"metadata": {"annotations": {ANNOTATION_CREATED_BUCKETS: ",".join(sorted(merged))}}}
    core_v1.patch_namespace(namespace, body)


# ---------------------------------------------------------------------------
# Owner marker on backends without bucket tagging
# ---------------------------------------------------------------------------

# How this process writes markers, per S3 endpoint: "conditional" when the
# backend enforced IfNoneMatch on the probe, "unconditional" otherwise.
_MARKER_WRITE_MODE: dict[str, str] = {}
# Codes a backend that has no conditional writes answers with.
_NO_CONDITIONAL_CODES = frozenset({"NotImplemented", "InvalidArgument", "InvalidRequest"})
# Fallback (no conditional writes): wait this long and read the marker again,
# so a second cluster writing in the same window is seen by at least one.
MARKER_FALLBACK_WAIT_S = 2.0
_marker_sleep = time.sleep


@dataclass(frozen=True)
class MarkerResult:
    """What ``write_owner_marker`` found: whose marker the bucket now carries."""

    ours: bool
    marker: dict[str, Any] | None
    mode: str  # "conditional" or "unconditional"


def _error_code(e: Exception) -> str:
    response = getattr(e, "response", None) or {}
    return str(response.get("Error", {}).get("Code", ""))


def _http_status(e: Exception) -> int:
    response = getattr(e, "response", None) or {}
    try:
        return int(response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
    except (TypeError, ValueError):
        return 0


def _endpoint_of(boto_client: Any) -> str:
    return str(getattr(getattr(boto_client, "meta", None), "endpoint_url", "") or "")


def probe_conditional_put(boto_client: Any, bucket: str) -> bool:
    """Whether the backend enforces ``IfNoneMatch="*"`` on PutObject.

    Writes a throwaway key, ``.lakebench/probe-<uuid4>``, twice with the
    header: enforced means 200 then 412. The probe key is deleted after. It
    never touches the owner marker, so it cannot overwrite a marker another
    cluster wrote meanwhile.
    """
    import uuid

    from botocore.exceptions import ClientError

    key = f"{MARKER_PREFIX}probe-{uuid.uuid4().hex}"
    try:
        try:
            boto_client.put_object(Bucket=bucket, Key=key, Body=b"", IfNoneMatch="*")
        except ClientError as e:
            if _error_code(e) in _NO_CONDITIONAL_CODES:
                return False
            raise
        try:
            boto_client.put_object(Bucket=bucket, Key=key, Body=b"", IfNoneMatch="*")
        except ClientError as e:
            if _error_code(e) == "PreconditionFailed" or _http_status(e) == 412:
                return True
            if _error_code(e) in _NO_CONDITIONAL_CODES:
                return False
            raise
        return False  # a second 200: the header was ignored
    finally:
        try:
            boto_client.delete_object(Bucket=bucket, Key=key)
        except Exception as e:  # noqa: BLE001 -- a leftover probe key is harmless
            logger.debug("could not delete probe key %s/%s: %s", bucket, key, e)


def _marker_is_ours(marker: dict[str, Any] | None, identity: dict[str, Any]) -> bool:
    return (
        marker is not None
        and marker.get("deployment") == identity["deployment"]
        and marker.get("cluster") == identity["cluster"]
    )


def write_owner_marker(
    boto_client: Any, bucket: str, identity: dict[str, Any], *, mode: str | None = None
) -> MarkerResult:
    """Claim a tagless bucket with ``.lakebench/owner.json``.

    ``identity`` carries at least ``deployment`` and ``cluster`` (and the
    namespace, its uid, the time and the Lakebench version). The marker key
    is written once per attempt:

    - ``conditional`` (the backend enforces ``IfNoneMatch``, proved by
      ``probe_conditional_put`` on a throwaway key): a conditional PUT; a 412
      means a marker already exists, and that marker decides. Two clusters
      racing for one empty bucket get one winner.
    - ``unconditional`` (no conditional writes): read first and stop on a
      foreign marker, then a plain PUT, a read and compare, a
      ``MARKER_FALLBACK_WAIT_S`` wait and a second read. Two clusters writing
      inside that window can both believe they won (open risk R12).

    ``mode`` skips the probe (the caller's cached answer); otherwise the
    answer is cached per endpoint for this process. Returns whose marker the
    bucket carries afterwards. Read and write errors raise.
    """
    import json

    from botocore.exceptions import ClientError

    endpoint = _endpoint_of(boto_client)
    if mode is None:
        mode = _MARKER_WRITE_MODE.get(endpoint)
    if mode is None:
        mode = "conditional" if probe_conditional_put(boto_client, bucket) else "unconditional"
        _MARKER_WRITE_MODE[endpoint] = mode
        if mode == "unconditional":
            logger.warning(
                "S3 endpoint %s does not enforce conditional writes; owner markers are "
                "written with a read-back check only (two clusters claiming one empty "
                "bucket at the same moment could both believe they won)",
                endpoint or "(default)",
            )
    body = json.dumps(identity, sort_keys=True).encode("utf-8")
    if mode == "conditional":
        try:
            boto_client.put_object(
                Bucket=bucket,
                Key=OWNER_MARKER_KEY,
                Body=body,
                IfNoneMatch="*",
                ContentType="application/json",
            )
        except ClientError as e:
            if not (_error_code(e) == "PreconditionFailed" or _http_status(e) == 412):
                raise
            existing = read_owner_marker(boto_client, bucket)
            return MarkerResult(_marker_is_ours(existing, identity), existing, mode)
        got = read_owner_marker(boto_client, bucket)
        return MarkerResult(_marker_is_ours(got, identity), got, mode)
    existing = read_owner_marker(boto_client, bucket)
    if existing is not None and not _marker_is_ours(existing, identity):
        return MarkerResult(False, existing, mode)
    boto_client.put_object(
        Bucket=bucket, Key=OWNER_MARKER_KEY, Body=body, ContentType="application/json"
    )
    got = read_owner_marker(boto_client, bucket)
    if not _marker_is_ours(got, identity):
        return MarkerResult(False, got, mode)
    _marker_sleep(MARKER_FALLBACK_WAIT_S)
    got = read_owner_marker(boto_client, bucket)
    return MarkerResult(_marker_is_ours(got, identity), got, mode)


def owner_marker_identity(
    deployment: str, cluster: str, namespace: str, namespace_uid: str = ""
) -> dict[str, Any]:
    """The marker body for this deployment on this cluster."""
    from datetime import datetime, timezone

    try:
        from lakebench import __version__ as lb_version
    except Exception:  # noqa: BLE001 -- informational only
        lb_version = "unknown"
    return {
        "deployment": deployment,
        "cluster": cluster,
        "namespace": namespace,
        "namespace_uid": namespace_uid,
        "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "lakebench_version": lb_version,
    }


def bucket_may_be_emptied(
    boto_client: Any,
    bucket: str,
    deployment: str,
    *,
    cluster_fp: str | None,
    created_record: Iterable[str],
    other_deployments: Iterable[str] | None,
) -> bool:
    """The one rule for "may this deployment delete data in ``bucket``".

    True for MATCH (this deployment's and this cluster's stamp: tag, or
    owner marker), and for LEGACY_PROVEN (no cluster stamp, in the created
    record) on a tagged backend; on a tagless one LEGACY_PROVEN also needs
    the longest-prefix name claim, which needs the other deployments' names
    (``None``: they could not be listed, so False), as destroy requires.
    Everything else is False. Read errors raise.
    """
    v = verify_bucket_ownership(
        boto_client,
        bucket,
        deployment,
        expected_cluster=cluster_fp,
        created_record=created_record,
    )
    if v.verdict is IdentityVerdict.MATCH:
        return True
    if v.verdict is IdentityVerdict.LEGACY_PROVEN:
        if v.tagged:
            return True
        return other_deployments is not None and bucket_name_matches_deployment(
            bucket, deployment, other_deployments
        )
    return False


def deployment_may_empty(cfg: Any, bucket: str, s3: Any, *, strict: bool = False) -> bool:
    """``bucket_may_be_emptied`` for a config, on its own cluster context. Fail-safe.

    The kube client is loaded for the config's context first, so the
    namespace record, the other deployments and the fingerprint all come
    from the same cluster. Any error is False, or raises with ``strict``
    (a caller that must tell "not ours" from "could not check").
    """
    try:
        from kubernetes import client as k8s_client

        from lakebench.k8s import get_k8s_client

        if getattr(s3, "_init_error", None):
            return False
        context = cfg.platform.kubernetes.context or ""
        namespace = cfg.get_namespace()
        get_k8s_client(context=context, namespace=namespace)
        core_v1 = k8s_client.CoreV1Api()
        return bucket_may_be_emptied(
            s3.raw_client,
            bucket,
            cfg.name,
            cluster_fp=api_server_fingerprint(context),
            created_record=read_created_buckets(core_v1, namespace),
            other_deployments=list_lakebench_deployment_names(core_v1, exclude=namespace),
        )
    except Exception as e:  # noqa: BLE001
        if strict:
            raise
        logger.info("could not prove this deployment may empty %s (%s); it may not", bucket, e)
        return False


def write_deploy_nonce(core_v1: Any, namespace: str, nonce: str | None = None) -> str:
    """Stamp a deploy nonce on the namespace and return it.

    ``nonce`` is the one ``deploy`` already recorded in the directory's state
    so a nameless teardown can prove it; the namespace then also gets
    ``state-schema: lb-state/1``, which
    says a v1.7 deploy recorded this nonce in some directory. Without one a
    fresh nonce is stamped and the schema annotation is left as it is.
    """
    import uuid

    annotations = {ANNOTATION_DEPLOY_NONCE: nonce or uuid.uuid4().hex}
    if nonce:
        from lakebench.config.deploy_state import STATE_SCHEMA

        annotations[ANNOTATION_STATE_SCHEMA] = STATE_SCHEMA
    core_v1.patch_namespace(namespace, {"metadata": {"annotations": annotations}})
    return annotations[ANNOTATION_DEPLOY_NONCE]


def forget_created_buckets(core_v1: Any, namespace: str, buckets: list[str]) -> None:
    """Drop ``buckets`` from the created-buckets annotation after deleting them.

    A namespace that outlives destroy (create_namespace=false, or kept after a
    failure) would otherwise keep claiming the names, and a bucket later
    pre-provisioned or adopted under one of them would be deleted by the next
    destroy. A namespace that is already gone has nothing to forget.
    """
    from kubernetes.client.rest import ApiException

    if not buckets:
        return
    recorded = read_created_buckets(core_v1, namespace)
    remaining = recorded - set(buckets)
    if remaining == recorded:
        return
    # A JSON merge patch with null removes the key.
    value = ",".join(sorted(remaining)) if remaining else None
    body = {"metadata": {"annotations": {ANNOTATION_CREATED_BUCKETS: value}}}
    try:
        core_v1.patch_namespace(namespace, body)
    except ApiException as e:
        if e.status != 404:
            raise


def cluster_stamp(fingerprint: str | None) -> str | None:
    """The cluster stamp a bucket carries: ``api_server_fingerprint(...)[:32]``."""
    return fingerprint[:32] if fingerprint else None


def read_owner_marker(boto_client: Any, bucket: str) -> dict[str, Any] | None:
    """The bucket's ``.lakebench/owner.json``, or None when it has none.

    Raises on any other error, including a marker that is not a JSON
    object (a corrupt claim is never read as "no claim").
    """
    import json

    from botocore.exceptions import ClientError

    try:
        resp = boto_client.get_object(Bucket=bucket, Key=OWNER_MARKER_KEY)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404", "NotFound"):
            return None
        raise
    body = resp["Body"].read()
    marker = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
    if not isinstance(marker, dict):
        raise BucketOwnershipError(f"bucket {bucket!r}: owner marker is not a JSON object")
    return marker


def _cluster_verdict(
    bucket: str,
    expected_deployment: str,
    found_cluster: str | None,
    expected_cluster: str | None,
    record: set[str],
    *,
    tagged: bool,
) -> IdentityReport:
    """Rows 1 to 5 and 8 of the ownership matrix, for a bucket whose name stamp is ours."""
    if found_cluster:
        mine = cluster_stamp(expected_cluster)
        if mine is None:
            return IdentityReport(
                verdict=IdentityVerdict.UNVERIFIED_CLUSTER,
                resource_name=bucket,
                expected_deployment=expected_deployment,
                found_deployment=expected_deployment,
                found_api_server=found_cluster,
                tagged=tagged,
                hint=(
                    f"bucket {bucket!r}: cannot compute this cluster's fingerprint; "
                    "buckets kept. `lakebench admin reclaim-bucket` (owner) can release them"
                ),
            )
        if found_cluster != mine:
            return IdentityReport(
                verdict=IdentityVerdict.FOREIGN_CLUSTER,
                resource_name=bucket,
                expected_deployment=expected_deployment,
                found_deployment=expected_deployment,
                found_api_server=found_cluster,
                current_api_server=mine,
                tagged=tagged,
                hint=(
                    f"bucket {bucket!r} belongs to deployment {expected_deployment!r} on "
                    f"another cluster (fp {found_cluster}, this cluster {mine}). If this "
                    "cluster's API-server CA changed, an owner can re-claim it with "
                    "`lakebench admin reclaim-bucket`"
                ),
            )
        return IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            found_deployment=expected_deployment,
            found_api_server=found_cluster,
            tagged=tagged,
        )
    if bucket in record and cluster_stamp(expected_cluster) is None:
        # Row 3 would stamp this cluster; with no fingerprint nothing may be
        # claimed or emptied on the record's word (design: "no bucket is
        # emptied or deleted").
        return IdentityReport(
            verdict=IdentityVerdict.UNVERIFIED_CLUSTER,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            found_deployment=expected_deployment,
            tagged=tagged,
            hint=(
                f"bucket {bucket!r}: cannot compute this cluster's fingerprint; "
                "buckets kept. `lakebench admin reclaim-bucket` (owner) can release them"
            ),
        )
    if bucket in record:
        return IdentityReport(
            verdict=IdentityVerdict.LEGACY_PROVEN,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            found_deployment=expected_deployment,
            tagged=tagged,
            hint=(
                f"bucket {bucket!r} has no cluster stamp; this namespace's record "
                "proves this cluster created or adopted it"
            ),
        )
    return IdentityReport(
        verdict=IdentityVerdict.LEGACY_UNPROVEN,
        resource_name=bucket,
        expected_deployment=expected_deployment,
        found_deployment=expected_deployment,
        tagged=tagged,
        hint=(
            f"bucket {bucket!r} carries this deployment's name but no cluster stamp, "
            "and this namespace does not record creating or adopting it (a bucket an "
            "earlier lakebench adopted). It is used but never emptied or deleted; an "
            "owner can claim it with `lakebench admin reclaim-bucket`"
        ),
    )


def verify_bucket_ownership(
    boto_client: Any,
    bucket: str,
    expected_deployment: str,
    *,
    expected_cluster: str | None,
    created_record: Iterable[str],
) -> IdentityReport:
    """Read a bucket's ownership stamp and return its ownership verdict.

    ``expected_cluster`` is this run's ``api_server_fingerprint`` (None when
    it cannot be computed); ``created_record`` is this namespace's
    created-buckets record. Only that record proves this cluster made a
    bucket (the cross-cluster ownership rule): the adopted-empty record is what 1.6 wrote when it
    adopted another cluster's empty bucket, so it proves nothing. The
    matrix:

    - row 1, name and cluster ours: MATCH;
    - rows 2 and 5, name ours, cluster not: FOREIGN_CLUSTER;
    - row 3, name ours (or tagless with no marker), no cluster stamp, in the
      record: LEGACY_PROVEN;
    - row 4, tagged with our name, no cluster stamp, not in the record:
      LEGACY_UNPROVEN;
    - row 6, name not ours: MISMATCH;
    - row 7, no stamp and not in the record: ABSENT on a tagged backend,
      UNSUPPORTED on a tagless one;
    - row 8, a cluster stamp (or row 3's record) but no fingerprint for this
      run: UNVERIFIED_CLUSTER.

    On a backend without tagging the stamp is the owner marker
    (``.lakebench/owner.json``) and ``tagged`` is False on the report. A
    missing bucket is NOT_FOUND. Read errors raise.
    """
    from botocore.exceptions import ClientError

    record = set(created_record)
    try:
        tags = read_bucket_ownership_tag(boto_client, bucket)
    except BucketTaggingUnsupported as e:
        return _verify_tagless(
            boto_client, bucket, expected_deployment, expected_cluster, record, str(e)
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchBucket", "404"):
            return IdentityReport(
                verdict=IdentityVerdict.NOT_FOUND,
                resource_name=bucket,
                expected_deployment=expected_deployment,
            )
        raise

    if tags is None:
        return IdentityReport(
            verdict=IdentityVerdict.ABSENT,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            hint=(
                f"bucket {bucket!r} has no lakebench.deployment tag. "
                "Legacy bucket. FIRST verify your cluster context with "
                "`oc whoami && kubectl config current-context` and "
                "confirm it matches the expected cluster for this "
                "deployment. Only after that check, and only as a last "
                "resort, pass --force-legacy on deploy to claim it "
                "(destroy always refuses without migration)."
            ),
        )

    found = tags.get(TAG_DEPLOYMENT_NAME)
    if found is None:
        return IdentityReport(
            verdict=IdentityVerdict.ABSENT,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            hint=(f"bucket {bucket!r} has tags but no {TAG_DEPLOYMENT_NAME!r}. Legacy bucket."),
        )

    if found != expected_deployment:
        return IdentityReport(
            verdict=IdentityVerdict.MISMATCH,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            found_deployment=found,
            hint=(
                f"bucket {bucket!r} is owned by deployment {found!r}, "
                f"not {expected_deployment!r}. Refusing."
            ),
        )

    return _cluster_verdict(
        bucket, expected_deployment, tags.get(TAG_CLUSTER), expected_cluster, record, tagged=True
    )


def _verify_tagless(
    boto_client: Any,
    bucket: str,
    expected_deployment: str,
    expected_cluster: str | None,
    record: set[str],
    unsupported_hint: str,
) -> IdentityReport:
    """The ownership verdict on a backend without tagging, from the owner marker."""
    from botocore.exceptions import ClientError

    try:
        marker = read_owner_marker(boto_client, bucket)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "NoSuchBucket":
            return IdentityReport(
                verdict=IdentityVerdict.NOT_FOUND,
                resource_name=bucket,
                expected_deployment=expected_deployment,
                tagged=False,
            )
        raise
    if marker is None:
        if bucket in record:
            return _cluster_verdict(
                bucket, expected_deployment, None, expected_cluster, record, tagged=False
            )
        return IdentityReport(
            verdict=IdentityVerdict.UNSUPPORTED,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            tagged=False,
            hint=unsupported_hint,
        )
    found = marker.get("deployment")
    if found != expected_deployment:
        return IdentityReport(
            verdict=IdentityVerdict.MISMATCH,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            found_deployment=str(found),
            tagged=False,
            hint=(
                f"bucket {bucket!r} is claimed by deployment {found!r} "
                f"({OWNER_MARKER_KEY}), not {expected_deployment!r}. Refusing."
            ),
        )
    return _cluster_verdict(
        bucket,
        expected_deployment,
        str(marker.get("cluster") or "") or None,
        expected_cluster,
        record,
        tagged=False,
    )


# ---------------------------------------------------------------------------
# Convenience: gather the current run's identity in one call
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeploymentIdentity:
    """Identity of the current run. Passed to every stamp / verify call."""

    name: str
    api_server: str | None
    committed_sha: str | None = None
    workload_schema: str | None = None


def build_identity_from_config(cfg: Any, context: str | None = None) -> DeploymentIdentity:
    """Assemble a DeploymentIdentity from a loaded LakebenchConfig.

    The committed sha is the short commit of the checkout the lakebench
    package runs from (found from the package's own path, the reading the
    run record's ``provenance.git_sha`` makes in a checkout), never the
    working directory's repository, with ``-dirty`` when the package has
    uncommitted changes or its state could not be read. With no checkout,
    a wheel's build-info commit (``_build_info.py``, written by the build
    hook) is stamped with ``-buildinfo`` so it is never read as a checkout
    commit, plus ``-dirty`` when the build was not from a clean tree. It is
    None when no commit can be read (an unknown install, a wheel built
    without the hook, git unavailable)."""
    from lakebench.metrics.provenance import INSTALL_CHECKOUT, INSTALL_WHEEL, sample

    committed_sha: str | None = None
    try:
        code = sample()
    except Exception:  # noqa: BLE001 -- a stamp, never a reason to fail the caller
        code = {}
    sha = code.get("git_sha")
    if code.get("install") == INSTALL_CHECKOUT and isinstance(sha, str) and sha:
        # An unknown state (git status failed) is not proven clean.
        committed_sha = sha[:7] + ("" if code.get("git_dirty") is False else "-dirty")
    elif code.get("install") == INSTALL_WHEEL and isinstance(sha, str) and sha:
        committed_sha = (
            sha[:7] + "-buildinfo" + ("" if code.get("git_dirty") is False else "-dirty")
        )

    workload_schema: str | None = None
    try:
        ws = cfg.architecture.workload.schema_type
        # Only accept a real enum or string; a Mock proxy would `str()` to
        # something like "<MagicMock id=...>" and end up in the bucket tag.
        candidate = getattr(ws, "value", None)
        if candidate is None and isinstance(ws, str):
            candidate = ws
        if isinstance(candidate, str) and candidate:
            workload_schema = candidate
    except Exception:  # noqa: BLE001 -- best-effort; None is a valid value
        pass

    return DeploymentIdentity(
        name=cfg.name,
        api_server=api_server_fingerprint(context),
        committed_sha=committed_sha,
        workload_schema=workload_schema,
    )


def _prefix_matches(bucket: str, name: str) -> bool:
    """Bucket name is exactly ``name`` or starts with ``name-``.

    Empty ``name`` never matches (fail-safe). Trailing hyphen required
    so ``foobar-bronze`` cannot be mistaken for deployment ``foo``.
    """
    if not name:
        return False
    if bucket == name:
        return True
    return bucket.startswith(name + "-")


def bucket_name_matches_deployment(
    bucket: str,
    deployment_name: str,
    other_deployment_names: Iterable[str] = (),
) -> bool:
    """Weaker ownership check for backends that lack bucket tagging.

    Used only when ``verify_bucket_ownership`` returns
    ``IdentityVerdict.UNSUPPORTED``. Returns True when the bucket name
    matches this deployment via ``_prefix_matches`` AND no other
    deployment on the cluster has a longer-prefix match on the same
    bucket. Longest-prefix-wins: if deployments ``prod`` and
    ``prod-eu`` coexist, only ``prod-eu`` may claim bucket
    ``prod-eu-bronze``.

    Rationale: on tagged backends the ownership tag is authoritative
    and the bucket name is cosmetic. On untagged backends we have no
    server-side proof of ownership at all, so we fall back to the
    naming convention. Without cluster-scoped disambiguation a
    deployment ``prod`` would silently adopt (and on destroy, empty)
    every bucket owned by a deployment whose name begins with
    ``prod``. That is the cross-team data-loss shape this helper
    exists to prevent -- so the fallback MUST consult other
    lakebench-annotated namespaces on the cluster before granting
    ownership. Callers pass the list of other deployment names they
    discovered via ``list_lakebench_deployment_names``.

    ``other_deployment_names`` should not include the current
    deployment. Callers that cannot enumerate (no k8s client, offline
    unit test) may pass an empty iterable; in that case the caller
    accepts the shorter-prefix collision risk explicitly.
    """
    if not _prefix_matches(bucket, deployment_name):
        return False
    my_len = len(deployment_name)
    for other in other_deployment_names:
        if not other or other == deployment_name:
            continue
        if _prefix_matches(bucket, other) and len(other) > my_len:
            return False
    return True


def list_lakebench_deployment_names(core_v1: Any, exclude: str | None = None) -> list[str] | None:
    """Return the deployment names carried by other lakebench namespaces.

    Mirrors the ``_is_other_lakebench`` pattern used elsewhere in
    ``destroy.py``: reads the ``lakebench.deployment/name`` annotation
    when present, and falls back to the namespace name for pre-PR-1
    legacy namespaces that carry the ``managed-by=lakebench`` label
    but no annotation. Legacy names still count as "another deployment
    on the cluster" for prefix-collision purposes.

    ``exclude`` skips the caller's own namespace name from the result.

    **Return-value contract, load-bearing.** Returns:

    - ``list[str]`` (possibly empty) when the enumeration ran to
      completion and the result is trustworthy. An empty list means
      "no other lakebench deployments exist on this cluster" and the
      name-prefix fallback may proceed.
    - ``None`` when enumeration failed (RBAC denied, k8s API
      unreachable, kubeconfig missing). Callers MUST NOT treat this
      the same as an empty list: doing so would let the name-prefix
      fallback fire under a namespace-scoped token, silently
      re-opening the sibling-collision hole the round-2 fix closed.
      Callers should refuse the UNSUPPORTED verdict entirely unless
      the operator has opted in with ``--force-legacy``.

    Narrow catch: only ``ApiException`` and ``ConfigException`` are
    treated as enumeration-failure. Any other exception is a bug and
    bubbles up.
    """
    from kubernetes.client.rest import ApiException
    from kubernetes.config.config_exception import ConfigException

    try:
        items = core_v1.list_namespace().items
    except ApiException as e:
        logger.warning(
            "list_lakebench_deployment_names: k8s API refused namespace list "
            "(status=%s reason=%s). Returning None so callers refuse the "
            "name-prefix fallback rather than silently accepting.",
            getattr(e, "status", "?"),
            getattr(e, "reason", "?"),
        )
        return None
    except ConfigException:
        logger.warning(
            "list_lakebench_deployment_names: kubeconfig unavailable. "
            "Returning None so callers refuse the name-prefix fallback."
        )
        return None

    names: list[str] = []
    for ns in items:
        if not ns or not ns.metadata:
            continue
        if exclude and ns.metadata.name == exclude:
            continue
        anns = ns.metadata.annotations or {}
        labels = ns.metadata.labels or {}
        deployment = anns.get(ANNOTATION_DEPLOYMENT_NAME)
        if deployment:
            names.append(deployment)
            continue
        if labels.get("app.kubernetes.io/managed-by") == "lakebench":
            names.append(ns.metadata.name)
            continue
        if labels.get("app.kubernetes.io/name") == "lakebench":
            names.append(ns.metadata.name)
    return names


@dataclass
class DataOwnershipDecision:
    """Whether a command may drop tables or empty buckets for a deployment."""

    allowed: bool
    #: Why not (or, when allowed, any caveat worth printing). Never empty
    #: when ``allowed`` is False.
    hint: str = ""
    #: True when ``allowed`` is False because a check could not run (the
    #: namespace list was unreadable), not because ownership was disproved.
    #: The CLI exits 1 for these and 3 for a refusal.
    unverifiable: bool = False


def check_data_ownership(
    core_v1: Any,
    *,
    namespace: str,
    deployment_name: str,
    namespace_present: bool,
    namespace_verified: bool,
    force_legacy: bool,
    context_name: str = "",
) -> DataOwnershipDecision:
    """Decide whether destroy/clean may touch a deployment's tables and buckets.

    Shared by ``destroy`` and ``clean`` so neither can bypass the other.

    Rules, in order:
    1. Another live namespace carrying the same deployment name refuses, with
       no bypass: the two share the same buckets (ownership is keyed on the
       name), so cleaning them from either side deletes the other's data.
       ``force_legacy`` does not waive this.
    2. A missing namespace refuses unless ``force_legacy``: with a stale or
       wrong kube context a live deployment's namespace looks absent while
       its buckets are still reachable by name.
    3. Otherwise the namespace identity must have been verified by the
       caller (``namespace_verified``).

    If the namespace list is forbidden (namespace-scoped RBAC), rule 1
    cannot be checked; that is allowed but returned as a caveat so callers
    print it.
    """
    caveat = ""
    try:
        if core_v1 is None:
            raise RuntimeError("the cluster could not be reached")
        for n in core_v1.list_namespace().items:
            name = n.metadata.name
            if name == namespace or getattr(n.metadata, "deletion_timestamp", None):
                continue
            anns = n.metadata.annotations or {}
            labels = n.metadata.labels or {}
            legacy_same = (
                not anns.get(ANNOTATION_DEPLOYMENT_NAME)
                and labels.get("app.kubernetes.io/managed-by") == "lakebench"
                and name == deployment_name
            )
            if anns.get(ANNOTATION_DEPLOYMENT_NAME) == deployment_name or legacy_same:
                return DataOwnershipDecision(
                    allowed=False,
                    hint=(
                        f"Namespace {name!r} is a live lakebench deployment with the "
                        f"same name {deployment_name!r}. The two share the same "
                        "buckets, so cleaning them from either namespace would "
                        "delete the other's data. Tables and buckets were left "
                        "untouched. Destroy or retire the other deployment first; "
                        "--force-legacy does not override this."
                    ),
                )
    except Exception as e:  # noqa: BLE001
        if not namespace_present or force_legacy:
            # Without the namespace (or with the escape hatch that waives it),
            # the shared-name check is the only protection left, so it must
            # not silently disappear.
            return DataOwnershipDecision(
                allowed=False,
                hint=(
                    "Could not list namespaces to check whether another live "
                    f"deployment is named {deployment_name!r} ({e}), and this "
                    "destroy has no verified namespace to rely on instead. Tables "
                    "and buckets were left untouched. Re-run with credentials that "
                    "can list namespaces."
                ),
                unverifiable=True,
            )
        caveat = (
            "Could not list namespaces to check for another deployment with the "
            f"same name ({e}); proceeding on the verified namespace identity alone."
        )
        logger.warning(caveat)

    if not namespace_present:
        if force_legacy:
            return DataOwnershipDecision(
                allowed=True,
                hint=(
                    f"--force-legacy: namespace {namespace!r} is absent; tables and "
                    "buckets are handled by name without an identity record."
                ),
            )
        if not context_name:
            from lakebench.k8s.target import active_target

            pinned = active_target()
            context_name = pinned.label if pinned is not None else ""
        ctx = context_name or "(current context)"
        return DataOwnershipDecision(
            allowed=False,
            hint=(
                f"Namespace {namespace!r} does not exist on kubeconfig context "
                f"{ctx}, so ownership of the tables and buckets it names cannot be "
                "proven: with a stale or wrong context they may belong to a live "
                "deployment on another cluster that shares the object store. They "
                "were left untouched. FIRST verify your cluster context: run "
                "`oc whoami && kubectl config current-context` and confirm the "
                "output matches the cluster this deployment is expected to live "
                "on. Only after confirming your kubeconfig context is the "
                "expected cluster, and the buckets are genuinely yours (for "
                "example an earlier destroy removed the namespace but not "
                "the buckets), re-run with --force-legacy as a last resort."
            ),
        )
    if not namespace_verified:
        return DataOwnershipDecision(
            allowed=False,
            hint=f"Namespace {namespace!r} ownership was not verified.",
        )
    return DataOwnershipDecision(allowed=True, hint=caveat)
