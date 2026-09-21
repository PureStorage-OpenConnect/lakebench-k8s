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
    DeploymentIdentity,
    IdentityVerdict,
    api_server_fingerprint,
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
                {"Key": TAG_WORKLOAD_SCHEMA, "Value": "faml"},
            ]
        }
        write_bucket_ownership_tag(s3, "b1", "my-config", workload_schema="faml")
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
