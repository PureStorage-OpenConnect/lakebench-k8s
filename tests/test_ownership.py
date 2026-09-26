"""Tests for deployment identity + resource ownership machinery.

The invariant these tests defend is:

    Destroying deployment A does not affect deployment B running in parallel.

Each test names the concrete failure mode from
``dev-artifacts/DESIGN-namespace-isolation.md`` review-round F1-F9 that it
guards against, so future edits that regress the fix will trip a named test.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from kubernetes.client.rest import ApiException

from lakebench.deploy.ownership import (
    ANNOTATION_API_SERVER,
    ANNOTATION_DEPLOYMENT_NAME,
    DEPLOYMENT_NAME_MAX,
    TAG_DEPLOYMENT_NAME,
    TAG_WORKLOAD_SCHEMA,
    BucketOwnershipError,
    BucketTaggingUnsupported,
    DeploymentIdentity,
    IdentityVerdict,
    api_server_fingerprint,
    bucket_name_matches_deployment,
    read_bucket_ownership_tag,
    stamp_namespace,
    verify_bucket_ownership,
    verify_namespace_identity,
    write_bucket_ownership_tag,
)

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _ns_response(annotations: dict[str, str] | None = None, resource_version: str = "1"):
    """Build a minimal V1Namespace-shaped mock."""
    return SimpleNamespace(
        metadata=SimpleNamespace(
            annotations=dict(annotations) if annotations else None,
            resource_version=resource_version,
        )
    )


def _api_exception(status: int, reason: str = "") -> ApiException:
    exc = ApiException(status=status, reason=reason)
    return exc


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, "SomeOp")


# ---------------------------------------------------------------------------
# api_server_fingerprint
# ---------------------------------------------------------------------------


class TestApiServerFingerprint:
    def test_returns_stable_hash_for_same_input(self):
        """Two invocations against the same cluster must produce the same
        fingerprint. That's the whole point -- two engineers on different
        workstations get the same identity for the same cluster."""
        with mock.patch("kubernetes.config.list_kube_config_contexts") as mock_list:
            mock_list.return_value = (
                [{"name": "c1", "context": {"cluster": "prod"}}],
                {"name": "c1", "context": {"cluster": "prod"}},
            )
            with mock.patch("kubernetes.config.kube_config.KubeConfigMerger") as MockMerger:
                MockMerger.return_value.config.value = {
                    "clusters": [
                        {
                            "name": "prod",
                            "cluster": {
                                "server": "https://api.example.com:6443",
                                "certificate-authority-data": "AAAA",
                            },
                        }
                    ]
                }
                a = api_server_fingerprint()
                b = api_server_fingerprint()
        assert a == b
        assert a is not None
        assert len(a) == 12

    def test_different_clusters_produce_different_hashes(self):
        """Two clusters at the same URL but different CAs must not collide."""
        with mock.patch("kubernetes.config.list_kube_config_contexts") as mock_list:
            mock_list.return_value = (
                [{"name": "c", "context": {"cluster": "same-server"}}],
                {"name": "c", "context": {"cluster": "same-server"}},
            )
            with mock.patch("kubernetes.config.kube_config.KubeConfigMerger") as MockMerger:
                MockMerger.return_value.config.value = {
                    "clusters": [
                        {
                            "name": "same-server",
                            "cluster": {
                                "server": "https://same.example.com:6443",
                                "certificate-authority-data": "CA_A",
                            },
                        }
                    ]
                }
                a = api_server_fingerprint()
                MockMerger.return_value.config.value = {
                    "clusters": [
                        {
                            "name": "same-server",
                            "cluster": {
                                "server": "https://same.example.com:6443",
                                "certificate-authority-data": "CA_B",
                            },
                        }
                    ]
                }
                b = api_server_fingerprint()
        assert a != b

    def test_missing_kubeconfig_returns_none(self):
        with mock.patch(
            "kubernetes.config.list_kube_config_contexts",
            side_effect=Exception("no config"),
        ):
            with mock.patch(
                "builtins.open",
                side_effect=OSError("no in-cluster CA either"),
            ):
                got = api_server_fingerprint()
        assert got is None

    def test_workstation_and_in_cluster_produce_same_hash_for_same_ca(self):
        """F2a: workstation-context (via kubeconfig base64 CA data) and
        in-cluster pod-context (via mounted /var/run/secrets CA bytes)
        against the SAME cluster must produce the same fingerprint.
        Before F2a they differed because we combined CA with the
        endpoint URL, which differs between reachability paths."""
        import base64

        ca_bytes = b"pem-ca-bytes"
        ca_b64 = base64.b64encode(ca_bytes).decode()

        # Path A: workstation context reading kubeconfig with base64 CA.
        with mock.patch("kubernetes.config.list_kube_config_contexts") as mock_list:
            mock_list.return_value = (
                [{"name": "wsc", "context": {"cluster": "prod"}}],
                {"name": "wsc", "context": {"cluster": "prod"}},
            )
            with mock.patch("kubernetes.config.kube_config.KubeConfigMerger") as MockMerger:
                MockMerger.return_value.config.value = {
                    "clusters": [
                        {
                            "name": "prod",
                            "cluster": {
                                "certificate-authority-data": ca_b64,
                                "server": "https://external.example.com:6443",
                            },
                        }
                    ]
                }
                workstation_hash = api_server_fingerprint()

        # Path B: in-cluster context reads /var/run/secrets/.../ca.crt
        # as raw bytes.
        with mock.patch(
            "kubernetes.config.list_kube_config_contexts",
            side_effect=Exception("no kubeconfig in pod"),
        ):
            m = mock.mock_open(read_data=ca_bytes)
            with mock.patch("builtins.open", m):
                in_cluster_hash = api_server_fingerprint()

        assert workstation_hash is not None
        assert in_cluster_hash is not None
        # Both paths hash the SAME CA bytes; the hashes must match.
        assert workstation_hash == in_cluster_hash


# ---------------------------------------------------------------------------
# stamp_namespace -- optimistic-concurrency PATCH
# ---------------------------------------------------------------------------


class TestStampNamespace:
    def test_fresh_namespace_gets_stamped(self):
        """Guards F2: fresh (annotation-less) namespace stamps under
        force_legacy=True."""
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(annotations=None, resource_version="42")
        r = stamp_namespace(
            core,
            "ns",
            "my-config",
            api_server="deadbeef1234",
            force_legacy=True,
        )
        assert r.verdict is IdentityVerdict.MATCH
        core.patch_namespace.assert_called_once()
        body = core.patch_namespace.call_args[0][1]
        assert body["metadata"]["annotations"][ANNOTATION_DEPLOYMENT_NAME] == "my-config"
        assert body["metadata"]["annotations"][ANNOTATION_API_SERVER] == "deadbeef1234"
        assert body["metadata"]["resourceVersion"] == "42"

    def test_legacy_namespace_refuses_without_force(self):
        """Guards F2 second half: legacy annotation-less namespace is UNSAFE."""
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(annotations={})
        r = stamp_namespace(
            core,
            "ns",
            "my-config",
            api_server="deadbeef1234",
            force_legacy=False,
        )
        assert r.verdict is IdentityVerdict.ABSENT
        core.patch_namespace.assert_not_called()

    def test_already_stamped_same_identity_is_idempotent(self):
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={
                ANNOTATION_DEPLOYMENT_NAME: "my-config",
                ANNOTATION_API_SERVER: "deadbeef1234",
            }
        )
        r = stamp_namespace(core, "ns", "my-config", api_server="deadbeef1234")
        assert r.verdict is IdentityVerdict.MATCH
        # Idempotent: no PATCH issued when nothing changes.
        core.patch_namespace.assert_not_called()

    def test_foreign_identity_refuses(self):
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={
                ANNOTATION_DEPLOYMENT_NAME: "someone-else",
                ANNOTATION_API_SERVER: "deadbeef1234",
            }
        )
        r = stamp_namespace(core, "ns", "my-config", api_server="deadbeef1234")
        assert r.verdict is IdentityVerdict.MISMATCH
        assert r.found_deployment == "someone-else"
        core.patch_namespace.assert_not_called()

    def test_conflict_retries_and_succeeds(self):
        """Guards F2: 409 on write triggers re-read + retry, not silent overwrite."""
        core = mock.MagicMock()
        # First read: fresh; PATCH fails with 409.
        # Second read: still fresh; PATCH succeeds.
        core.read_namespace.side_effect = [
            _ns_response(annotations=None, resource_version="1"),
            _ns_response(annotations=None, resource_version="2"),
        ]
        core.patch_namespace.side_effect = [_api_exception(409), None]
        r = stamp_namespace(
            core,
            "ns",
            "my-config",
            api_server="deadbeef1234",
            force_legacy=True,
            max_retries=3,
        )
        assert r.verdict is IdentityVerdict.MATCH
        assert core.patch_namespace.call_count == 2

    def test_conflict_reveals_foreign_identity(self):
        """Guards F2: conflict resolves to a foreign identity that appeared
        during our write -- must refuse, not overwrite."""
        core = mock.MagicMock()
        core.read_namespace.side_effect = [
            _ns_response(annotations=None, resource_version="1"),
            _ns_response(
                annotations={ANNOTATION_DEPLOYMENT_NAME: "someone-else"},
                resource_version="2",
            ),
        ]
        core.patch_namespace.side_effect = [_api_exception(409)]
        r = stamp_namespace(
            core,
            "ns",
            "my-config",
            api_server="deadbeef1234",
            force_legacy=True,
            max_retries=3,
        )
        assert r.verdict is IdentityVerdict.MISMATCH
        # Only the first PATCH attempt fired; second read caught the
        # foreign stamp and we refused before trying again.
        assert core.patch_namespace.call_count == 1

    def test_exhausted_retries_reports_mismatch(self):
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(annotations=None)
        core.patch_namespace.side_effect = _api_exception(409)
        r = stamp_namespace(
            core,
            "ns",
            "my-config",
            api_server="deadbeef1234",
            force_legacy=True,
            max_retries=2,
        )
        assert r.verdict is IdentityVerdict.MISMATCH
        assert core.patch_namespace.call_count == 2

    def test_missing_namespace(self):
        core = mock.MagicMock()
        core.read_namespace.side_effect = _api_exception(404)
        r = stamp_namespace(core, "ns", "my-config", api_server="deadbeef1234")
        assert r.verdict is IdentityVerdict.NOT_FOUND
        core.patch_namespace.assert_not_called()

    def test_deployment_name_length_guard(self):
        """Length assertion prevents S3 tag truncation later."""
        long_name = "a" * (DEPLOYMENT_NAME_MAX + 1)
        with pytest.raises(ValueError, match="chars; max"):
            stamp_namespace(mock.MagicMock(), "ns", long_name, api_server=None)


# ---------------------------------------------------------------------------
# verify_namespace_identity
# ---------------------------------------------------------------------------


class TestVerifyNamespaceIdentity:
    def test_match(self):
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={
                ANNOTATION_DEPLOYMENT_NAME: "my-config",
                ANNOTATION_API_SERVER: "deadbeef1234",
            }
        )
        r = verify_namespace_identity(core, "ns", "my-config", "deadbeef1234")
        assert r.verdict is IdentityVerdict.MATCH

    def test_deployment_name_mismatch_refuses(self):
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={ANNOTATION_DEPLOYMENT_NAME: "other"},
        )
        r = verify_namespace_identity(core, "ns", "mine", "deadbeef1234")
        assert r.verdict is IdentityVerdict.MISMATCH
        assert r.found_deployment == "other"

    def test_api_server_mismatch_refuses(self):
        """Guards F6: wrong kubectl context after deploy."""
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={
                ANNOTATION_DEPLOYMENT_NAME: "mine",
                ANNOTATION_API_SERVER: "prod-cluster1",
            }
        )
        r = verify_namespace_identity(core, "ns", "mine", "different-cluster-2")
        assert r.verdict is IdentityVerdict.MISMATCH
        assert "context" in (r.hint or "")

    def test_asymmetric_none_refuses_by_default(self):
        """Guards PR-1-R1: one side has an api-server fingerprint, the
        other does not. Cannot verify same cluster; must refuse unless
        allow_unverified_cluster explicitly opts in."""
        core = mock.MagicMock()
        # Stamped with a fingerprint; current run has none.
        core.read_namespace.return_value = _ns_response(
            annotations={
                ANNOTATION_DEPLOYMENT_NAME: "mine",
                ANNOTATION_API_SERVER: "sha-abc123",
            }
        )
        r = verify_namespace_identity(core, "ns", "mine", None)
        assert r.verdict is IdentityVerdict.MISMATCH
        assert "verify" in (r.hint or "").lower()

    def test_asymmetric_none_the_other_way(self):
        """Stamped with no fingerprint (broken kubeconfig at deploy),
        current run has one. Also refuses."""
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={ANNOTATION_DEPLOYMENT_NAME: "mine"}
        )
        r = verify_namespace_identity(core, "ns", "mine", "sha-abc123")
        assert r.verdict is IdentityVerdict.MISMATCH

    def test_asymmetric_none_bypassed_with_flag(self):
        """Explicit opt-in reduces MISMATCH to MATCH so a user who knows
        they cannot verify (dev environment) can still proceed."""
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={
                ANNOTATION_DEPLOYMENT_NAME: "mine",
                ANNOTATION_API_SERVER: "sha-abc123",
            }
        )
        r = verify_namespace_identity(
            core,
            "ns",
            "mine",
            None,
            allow_unverified_cluster=True,
        )
        assert r.verdict is IdentityVerdict.MATCH

    def test_both_sides_none_refuses_without_flag(self):
        """F3: neither side has a fingerprint. Name-only match would let
        a destroy target the wrong cluster in the (narrow) case where
        both environments have broken kubeconfig parsing. Refuse without
        the explicit override."""
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={ANNOTATION_DEPLOYMENT_NAME: "mine"}
        )
        r = verify_namespace_identity(core, "ns", "mine", None)
        assert r.verdict is IdentityVerdict.MISMATCH

    def test_both_sides_none_matches_with_flag(self):
        """With --allow-unverified-cluster explicitly set, both-None
        proceeds so the same-cluster dev workflow still works."""
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={ANNOTATION_DEPLOYMENT_NAME: "mine"}
        )
        r = verify_namespace_identity(
            core,
            "ns",
            "mine",
            None,
            allow_unverified_cluster=True,
        )
        assert r.verdict is IdentityVerdict.MATCH

    def test_missing_annotations_reports_absent(self):
        """Guards F1: legacy namespaces are ABSENT -- destroy refuses,
        migration is required."""
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(annotations={})
        r = verify_namespace_identity(core, "ns", "mine", "deadbeef1234")
        assert r.verdict is IdentityVerdict.ABSENT
        assert "migrate" in (r.hint or "")

    def test_missing_namespace(self):
        core = mock.MagicMock()
        core.read_namespace.side_effect = _api_exception(404)
        r = verify_namespace_identity(core, "ns", "mine", "sha")
        assert r.verdict is IdentityVerdict.NOT_FOUND


# ---------------------------------------------------------------------------
# Bucket ownership tag
# ---------------------------------------------------------------------------


class TestBucketOwnershipTag:
    def test_write_and_readback(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.return_value = {
            "TagSet": [
                {"Key": TAG_DEPLOYMENT_NAME, "Value": "my-config"},
                {"Key": TAG_WORKLOAD_SCHEMA, "Value": "aml"},
            ]
        }
        write_bucket_ownership_tag(s3, "b1", "my-config", workload_schema="aml")
        s3.put_bucket_tagging.assert_called_once()

    def test_readback_returns_none_when_no_tags(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NoSuchTagSet")
        got = read_bucket_ownership_tag(s3, "b1")
        assert got is None

    def test_write_raises_when_backend_drops_tags(self):
        """Guards F3a: backend that accepts PutBucketTagging but returns
        empty on GetBucketTagging must not be treated as 'owned'."""
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NoSuchTagSet")
        with pytest.raises(BucketOwnershipError, match="returned no tags"):
            write_bucket_ownership_tag(s3, "b1", "my-config")

    def test_write_raises_on_round_trip_mismatch(self):
        """Guards F3b: PutBucketTagging succeeds but GetBucketTagging shows
        a different value (async prop, truncation) -- refuse."""
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.return_value = {
            "TagSet": [{"Key": TAG_DEPLOYMENT_NAME, "Value": "mangled"}]
        }
        with pytest.raises(BucketOwnershipError, match="round-trip mismatch"):
            write_bucket_ownership_tag(s3, "b1", "my-config")

    def test_write_refuses_oversize_name(self):
        s3 = mock.MagicMock()
        with pytest.raises(BucketOwnershipError, match="truncate"):
            write_bucket_ownership_tag(s3, "b1", "a" * (DEPLOYMENT_NAME_MAX + 1))
        s3.put_bucket_tagging.assert_not_called()

    def test_verify_match(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.return_value = {
            "TagSet": [{"Key": TAG_DEPLOYMENT_NAME, "Value": "mine"}]
        }
        r = verify_bucket_ownership(s3, "b1", "mine")
        assert r.verdict is IdentityVerdict.MATCH

    def test_verify_mismatch_refuses(self):
        """Guards F4: bucket owned by another deployment must never be
        emptied, --force or not."""
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.return_value = {
            "TagSet": [{"Key": TAG_DEPLOYMENT_NAME, "Value": "someone-else"}]
        }
        r = verify_bucket_ownership(s3, "b1", "mine")
        assert r.verdict is IdentityVerdict.MISMATCH
        assert "someone-else" in (r.hint or "")

    def test_verify_legacy_untagged_bucket_absent(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NoSuchTagSet")
        r = verify_bucket_ownership(s3, "b1", "mine")
        assert r.verdict is IdentityVerdict.ABSENT
        assert "force-legacy" in (r.hint or "")

    def test_verify_missing_bucket(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NoSuchBucket")
        r = verify_bucket_ownership(s3, "b1", "mine")
        assert r.verdict is IdentityVerdict.NOT_FOUND


class TestBucketTaggingUnsupported:
    """LB-088: FlashBlade returns NotImplemented on GetBucketTagging /
    PutBucketTagging. Never seen before because the unit tests used moto,
    which implements tagging cleanly. Every path must handle it
    explicitly rather than falling through to a generic error.
    """

    def test_read_raises_unsupported_on_not_implemented(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NotImplemented")
        with pytest.raises(BucketTaggingUnsupported, match="NotImplemented"):
            read_bucket_ownership_tag(s3, "b1")

    def test_read_propagates_method_not_allowed_as_client_error(self):
        """MethodNotAllowed (HTTP 405) is a permissions / policy
        problem, NOT a missing feature. It must NOT downgrade to the
        Unsupported fallback, because doing so would silently switch
        to name-prefix ownership on a backend where the API works
        but the caller isn't authorised. Propagate as ClientError so
        the caller sees the real error."""
        from botocore.exceptions import ClientError as _CE

        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("MethodNotAllowed")
        with pytest.raises(_CE):
            read_bucket_ownership_tag(s3, "b1")

    def test_write_raises_unsupported_on_not_implemented(self):
        """PutBucketTagging NotImplemented must surface distinctly from
        a permissions error or a payload rejection -- the caller may
        want to fall back to name-prefix ownership rather than fail."""
        s3 = mock.MagicMock()
        s3.put_bucket_tagging.side_effect = _client_error("NotImplemented")
        with pytest.raises(BucketTaggingUnsupported, match="NotImplemented"):
            write_bucket_ownership_tag(s3, "b1", "my-config")

    def test_verify_returns_unsupported_verdict(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NotImplemented")
        r = verify_bucket_ownership(s3, "b1", "mine")
        assert r.verdict is IdentityVerdict.UNSUPPORTED
        assert "NotImplemented" in (r.hint or "")

    def test_verify_unsupported_distinct_from_absent(self):
        """Load-bearing: destroy relies on this to fall back to name-
        prefix instead of the --force-legacy migration path."""
        s3_unsupported = mock.MagicMock()
        s3_unsupported.get_bucket_tagging.side_effect = _client_error("NotImplemented")
        s3_absent = mock.MagicMock()
        s3_absent.get_bucket_tagging.side_effect = _client_error("NoSuchTagSet")
        assert (
            verify_bucket_ownership(s3_unsupported, "b1", "mine").verdict
            is IdentityVerdict.UNSUPPORTED
        )
        assert verify_bucket_ownership(s3_absent, "b1", "mine").verdict is IdentityVerdict.ABSENT


class TestBucketNamePrefixFallback:
    """Name-prefix ownership check used on UNSUPPORTED backends.

    The safety property is: a deployment's fallback ownership check
    must never grant it a bucket that a longer-prefix sibling
    deployment owns. Otherwise ``prod`` silently adopts ``prod-eu``'s
    buckets on backends without tag support.
    """

    def test_exact_deployment_name_matches(self):
        assert bucket_name_matches_deployment("mydeploy", "mydeploy") is True

    def test_prefix_with_hyphen_matches(self):
        assert bucket_name_matches_deployment("mydeploy-bronze", "mydeploy") is True
        assert bucket_name_matches_deployment("mydeploy-silver", "mydeploy") is True
        assert bucket_name_matches_deployment("mydeploy-gold", "mydeploy") is True

    def test_prefix_without_hyphen_does_not_match(self):
        """Load-bearing safety: ``mydeploybar-bronze`` must not match
        deployment ``mydeploy``. Without the required hyphen separator
        an adjacent deployment's bucket would be adopted."""
        assert bucket_name_matches_deployment("mydeploybar-bronze", "mydeploy") is False
        assert bucket_name_matches_deployment("mydeploy2-bronze", "mydeploy") is False

    def test_unrelated_name_does_not_match(self):
        assert bucket_name_matches_deployment("otherteam-bronze", "mydeploy") is False
        assert bucket_name_matches_deployment("legacy-data", "mydeploy") is False

    def test_empty_deployment_name_never_matches(self):
        """Fail-safe: an empty deployment name (misconfig) must not
        adopt any bucket."""
        assert bucket_name_matches_deployment("anything", "") is False
        assert bucket_name_matches_deployment("", "") is False

    def test_longer_prefix_sibling_wins(self):
        """The finding that motivated the round-2 rewrite: deployment
        ``prod`` and deployment ``prod-eu`` coexist on the same
        cluster. Bucket ``prod-eu-bronze`` prefix-matches BOTH names.
        Longest-prefix-wins: ``prod-eu`` owns it, ``prod`` does not.
        Without this rule, ``prod`` destroy silently empties ``prod-eu``'s
        bronze layer on a backend that cannot tag (LB-088)."""
        # From prod's perspective, prod-eu-bronze is NOT ours.
        assert (
            bucket_name_matches_deployment(
                "prod-eu-bronze",
                "prod",
                other_deployment_names=["prod-eu"],
            )
            is False
        )
        # From prod-eu's perspective, prod-eu-bronze IS ours.
        assert (
            bucket_name_matches_deployment(
                "prod-eu-bronze",
                "prod-eu",
                other_deployment_names=["prod"],
            )
            is True
        )

    def test_longer_prefix_sibling_wins_multiple(self):
        """Three-way: prod, prod-eu, prod-eu-preview coexist. Bucket
        prod-eu-preview-silver belongs only to the longest match."""
        others = ["prod", "prod-eu"]
        assert (
            bucket_name_matches_deployment("prod-eu-preview-silver", "prod-eu-preview", others)
            is True
        )
        assert (
            bucket_name_matches_deployment(
                "prod-eu-preview-silver", "prod-eu", others + ["prod-eu-preview"]
            )
            is False
        )

    def test_same_length_sibling_does_not_block(self):
        """A sibling that prefix-matches only via being a substring
        of the bucket but does NOT have a longer name does not block
        the current deployment. (In practice a same-length prefix
        collision is a name conflict at deploy time, not a fallback
        issue.)"""
        assert (
            bucket_name_matches_deployment(
                "myapp-bronze",
                "myapp",
                other_deployment_names=["other"],  # unrelated name
            )
            is True
        )

    def test_self_in_others_is_ignored(self):
        """A defensive-copy corner case: if the caller accidentally
        passes the current deployment name in ``other_deployment_names``,
        the check should not falsely refuse."""
        assert (
            bucket_name_matches_deployment(
                "myapp-bronze",
                "myapp",
                other_deployment_names=["myapp", "other"],
            )
            is True
        )


class TestListLakebenchDeploymentNames:
    """The enumerator must distinguish 'no other deployments' from
    'cannot tell'. Both cases arise in production: multi-tenant
    OpenShift denies cluster-wide namespace list under a
    namespace-scoped token; that must NOT downgrade to naive
    prefix ownership (round-3 F2)."""

    def _ns(self, name, annotations=None, labels=None):
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=name,
                annotations=annotations,
                labels=labels,
            )
        )

    def test_returns_empty_list_when_no_other_lakebench_namespaces(self):
        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.return_value = SimpleNamespace(
            items=[
                self._ns("default"),
                self._ns("kube-system"),
            ]
        )
        assert list_lakebench_deployment_names(core_v1) == []

    def test_returns_deployment_names_from_annotation(self):
        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.return_value = SimpleNamespace(
            items=[
                self._ns(
                    "team-a-ns",
                    annotations={ANNOTATION_DEPLOYMENT_NAME: "team-a"},
                ),
                self._ns(
                    "team-b-ns",
                    annotations={ANNOTATION_DEPLOYMENT_NAME: "team-b"},
                ),
            ]
        )
        assert sorted(list_lakebench_deployment_names(core_v1)) == ["team-a", "team-b"]

    def test_returns_namespace_name_for_legacy_managed_by(self):
        """Pre-PR-1 namespaces have no annotation but carry the label."""
        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.return_value = SimpleNamespace(
            items=[
                self._ns(
                    "legacy-ns",
                    labels={"app.kubernetes.io/managed-by": "lakebench"},
                ),
            ]
        )
        assert list_lakebench_deployment_names(core_v1) == ["legacy-ns"]

    def test_exclude_skips_self(self):
        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.return_value = SimpleNamespace(
            items=[
                self._ns(
                    "team-a-ns",
                    annotations={ANNOTATION_DEPLOYMENT_NAME: "team-a"},
                ),
                self._ns(
                    "team-b-ns",
                    annotations={ANNOTATION_DEPLOYMENT_NAME: "team-b"},
                ),
            ]
        )
        assert list_lakebench_deployment_names(core_v1, exclude="team-a-ns") == ["team-b"]

    def test_returns_none_on_api_exception(self):
        """Load-bearing: RBAC 403 must return None (not []) so callers
        refuse the name-prefix fallback rather than silently accept."""
        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.side_effect = _api_exception(403, "Forbidden")
        assert list_lakebench_deployment_names(core_v1) is None

    def test_returns_none_on_config_exception(self):
        """Kubeconfig missing / broken must return None."""
        from kubernetes.config.config_exception import ConfigException

        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.side_effect = ConfigException("no config")
        assert list_lakebench_deployment_names(core_v1) is None

    def test_unexpected_exception_propagates(self):
        """A genuine bug (KeyError from a rename, etc.) must NOT be
        swallowed as 'enumeration failed' -- that would mask real
        problems as safety refusals. Only ApiException and
        ConfigException are treated as enumeration failure."""
        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.side_effect = RuntimeError("bug")
        with pytest.raises(RuntimeError):
            list_lakebench_deployment_names(core_v1)


# ---------------------------------------------------------------------------
# DeploymentIdentity assembly
# ---------------------------------------------------------------------------


class TestDeploymentIdentity:
    def test_freeze(self):
        """Identity is passed all the way through the deploy path; make it
        immutable so no caller can quietly rewrite it."""
        d = DeploymentIdentity(name="x", api_server="y")
        with pytest.raises(FrozenInstanceError):
            d.name = "z"  # type: ignore[misc]


class TestCreatedBucketsRecord:
    def _ns(self, anns):
        ns = mock.MagicMock()
        ns.metadata.annotations = anns
        return ns

    def test_record_unions_with_existing(self):
        from lakebench.deploy.ownership import (
            ANNOTATION_CREATED_BUCKETS,
            read_created_buckets,
            record_created_buckets,
        )

        core = mock.MagicMock()
        core.read_namespace.return_value = self._ns({ANNOTATION_CREATED_BUCKETS: "a-bronze"})
        record_created_buckets(core, "a", ["a-gold"])
        body = core.patch_namespace.call_args.args[1]
        assert body["metadata"]["annotations"][ANNOTATION_CREATED_BUCKETS] == "a-bronze,a-gold"
        core.read_namespace.return_value = self._ns(
            {ANNOTATION_CREATED_BUCKETS: " a-bronze, a-gold ,"}
        )
        assert read_created_buckets(core, "a") == {"a-bronze", "a-gold"}

    def test_created_tag_written_only_when_asked(self):
        from lakebench.deploy.ownership import TAG_CREATED_BY_LAKEBENCH

        s3 = mock.MagicMock()
        s3.get_bucket_tagging.return_value = {
            "TagSet": [{"Key": TAG_DEPLOYMENT_NAME, "Value": "my-config"}]
        }
        write_bucket_ownership_tag(s3, "b1", "my-config", created=True)
        keys = [t["Key"] for t in s3.put_bucket_tagging.call_args.kwargs["Tagging"]["TagSet"]]
        assert TAG_CREATED_BY_LAKEBENCH in keys
        write_bucket_ownership_tag(s3, "b1", "my-config")
        keys = [t["Key"] for t in s3.put_bucket_tagging.call_args.kwargs["Tagging"]["TagSet"]]
        assert TAG_CREATED_BY_LAKEBENCH not in keys
