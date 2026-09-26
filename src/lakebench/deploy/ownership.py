"""Deployment identity + resource ownership for shared clusters.

Every lakebench deployment carries an identity: a name and a fingerprint of
the Kubernetes API server it was deployed to. That identity is stamped onto
every resource lakebench creates and checked before any destructive
mutation. If the stamp on a resource does not match the current
deployment, we refuse rather than warn.

This is the machinery behind the invariant "destroying deployment A does
not affect deployment B running in parallel." See
``dev-artifacts/DESIGN-namespace-isolation.md`` for the design rationale
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
# LB-159: set only on buckets deploy itself created. Destroy deletes a bucket
# only when it carries this marker (or is listed in the namespace annotation
# below); buckets deploy adopted are emptied but kept.
TAG_CREATED_BY_LAKEBENCH = "lakebench.created"
ANNOTATION_CREATED_BUCKETS = "lakebench.deployment/created-buckets"

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
    #: weaker check (name-prefix on buckets) or refuse. See LB-088.
    UNSUPPORTED = "unsupported"


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
    """Return the CA certificate bytes for a kubeconfig cluster block.

    kubeconfig stores the CA as either:
    - ``certificate-authority-data``: base64-encoded PEM (inline),
    - ``certificate-authority``: path to a PEM file on disk.

    Some producers write hex instead of base64 into
    ``certificate-authority-data`` when they emit the file. We treat
    both encodings as valid CA material and decode to raw bytes so all
    paths (workstation, in-cluster, inline, file) hash the same thing.
    """
    import base64
    import binascii

    inline = cluster_block.get("certificate-authority-data", "")
    if isinstance(inline, bytes):
        return inline
    if isinstance(inline, str) and inline:
        # Try base64 first (the standard); fall back to hex.
        try:
            return base64.b64decode(inline, validate=True)
        except (binascii.Error, ValueError):
            try:
                return bytes.fromhex(inline)
            except ValueError:
                # Not decodable -- treat as the raw string bytes.
                return inline.encode()

    ca_path = cluster_block.get("certificate-authority")
    if isinstance(ca_path, str) and ca_path:
        try:
            with open(ca_path, "rb") as f:
                return f.read()
        except OSError:
            return b""

    return b""


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

    for attempt in range(max_retries):
        try:
            ns = core_v1.read_namespace(namespace)
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

        # Already stamped with our identity -- idempotent no-op.
        if existing_name == deployment_name and existing_server == api_server:
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
                    "identity annotations. Pass --force-legacy on deploy "
                    "to claim it (destroy still refuses without migration)."
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
            core_v1.patch_namespace(namespace, body)
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
) -> None:
    """Write the ownership tag and verify the round trip.

    ``created`` adds the created-by-lakebench marker (LB-159). The whole tag
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

    from botocore.exceptions import ClientError

    try:
        boto_client.put_bucket_tagging(
            Bucket=bucket,
            Tagging={"TagSet": tag_set},
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "NotImplemented":
            # Backend does not implement bucket tagging at all (LB-088:
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


def read_bucket_ownership_tag(boto_client: Any, bucket: str) -> dict[str, str] | None:
    """Return the tag map for a bucket, or None if the bucket has no tags.

    Never raises for the "no tags on this bucket" case (S3 returns
    NoSuchTagSet). Raises ``BucketTaggingUnsupported`` for backends that
    do not implement the tagging API at all (LB-088: FlashBlade returns
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
            # LB-088: FlashBlade returns HTTP 501 NotImplemented.
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


def record_created_buckets(core_v1: Any, namespace: str, buckets: list[str]) -> None:
    """Add ``buckets`` to the namespace's created-buckets annotation (LB-159).

    Union with what is already recorded, so a redeploy (which sees the
    buckets as existing) does not forget that an earlier deploy created them.
    """
    if not buckets:
        return
    merged = read_created_buckets(core_v1, namespace) | set(buckets)
    body = {"metadata": {"annotations": {ANNOTATION_CREATED_BUCKETS: ",".join(sorted(merged))}}}
    core_v1.patch_namespace(namespace, body)


def verify_bucket_ownership(
    boto_client: Any,
    bucket: str,
    expected_deployment: str,
) -> IdentityReport:
    """Read a bucket's ownership tag and return the verdict.

    Returns ``IdentityVerdict.UNSUPPORTED`` when the backend does not
    implement the tagging API (LB-088). Callers MUST handle this verdict
    explicitly: it is not "no tag found" (that is ABSENT) and it is not
    "cannot reach the bucket" (that is NOT_FOUND). It is "the answer to
    'who owns this?' cannot be obtained from tags on this backend at all."
    Deploy s3-buckets and destroy s3-buckets fall back to a weaker
    name-prefix check.
    """
    from botocore.exceptions import ClientError

    try:
        tags = read_bucket_ownership_tag(boto_client, bucket)
    except BucketTaggingUnsupported as e:
        return IdentityReport(
            verdict=IdentityVerdict.UNSUPPORTED,
            resource_name=bucket,
            expected_deployment=expected_deployment,
            hint=str(e),
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
                "Legacy bucket. Pass --force-legacy on deploy to claim "
                "it (destroy always refuses without migration)."
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

    return IdentityReport(
        verdict=IdentityVerdict.MATCH,
        resource_name=bucket,
        expected_deployment=expected_deployment,
        found_deployment=found,
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
    """Assemble a DeploymentIdentity from a loaded LakebenchConfig."""
    import subprocess

    committed_sha: str | None = None
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short=7", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
        committed_sha = out.stdout.strip() or None
    except (FileNotFoundError, subprocess.SubprocessError):
        pass

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
        ctx = context_name or "(current context)"
        return DataOwnershipDecision(
            allowed=False,
            hint=(
                f"Namespace {namespace!r} does not exist on kubeconfig context "
                f"{ctx}, so ownership of the tables and buckets it names cannot be "
                "proven: with a stale or wrong context they may belong to a live "
                "deployment on another cluster that shares the object store. They "
                "were left untouched. If you have confirmed they are yours (for "
                "example a previous destroy removed the namespace but not the "
                "buckets), re-run with --force-legacy."
            ),
        )
    if not namespace_verified:
        return DataOwnershipDecision(
            allowed=False,
            hint=f"Namespace {namespace!r} ownership was not verified.",
        )
    return DataOwnershipDecision(allowed=True, hint=caveat)
