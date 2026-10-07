"""Tests for deployment identity + resource ownership machinery.

The invariant these tests defend is:

    Destroying deployment A does not affect deployment B running in parallel.

Each test names the concrete failure mode (design-review findings F1-F9;
the categories are in ``docs/design/namespace-isolation.md``) that it
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
    @pytest.mark.parametrize(
        ("stored", "current", "allow", "verdict"),
        [
            (
                {ANNOTATION_DEPLOYMENT_NAME: "mine", ANNOTATION_API_SERVER: "deadbeef1234"},
                "deadbeef1234",
                False,
                IdentityVerdict.MATCH,
            ),
            # wrong kubectl context after deploy
            (
                {ANNOTATION_DEPLOYMENT_NAME: "mine", ANNOTATION_API_SERVER: "prod-cluster1"},
                "different-cluster-2",
                False,
                IdentityVerdict.MISMATCH,
            ),
            # one side has an api-server fingerprint, the other does not
            (
                {ANNOTATION_DEPLOYMENT_NAME: "mine", ANNOTATION_API_SERVER: "sha-abc123"},
                None,
                False,
                IdentityVerdict.MISMATCH,
            ),
            ({ANNOTATION_DEPLOYMENT_NAME: "mine"}, "sha-abc123", False, IdentityVerdict.MISMATCH),
            (
                {ANNOTATION_DEPLOYMENT_NAME: "mine", ANNOTATION_API_SERVER: "sha-abc123"},
                None,
                True,
                IdentityVerdict.MATCH,
            ),
            # neither side has a fingerprint: refuse unless explicitly allowed
            ({ANNOTATION_DEPLOYMENT_NAME: "mine"}, None, False, IdentityVerdict.MISMATCH),
            ({ANNOTATION_DEPLOYMENT_NAME: "mine"}, None, True, IdentityVerdict.MATCH),
            # legacy namespace: destroy refuses, migration is required
            ({}, "deadbeef1234", False, IdentityVerdict.ABSENT),
        ],
    )
    def test_verdict(self, stored, current, allow, verdict):
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(annotations=stored)
        kw = {"allow_unverified_cluster": True} if allow else {}
        r = verify_namespace_identity(core, "ns", "mine", current, **kw)
        assert r.verdict is verdict

    def test_deployment_name_mismatch_refuses(self):
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={ANNOTATION_DEPLOYMENT_NAME: "other"},
        )
        r = verify_namespace_identity(core, "ns", "mine", "deadbeef1234")
        assert r.verdict is IdentityVerdict.MISMATCH
        assert r.found_deployment == "other"

    def test_missing_namespace(self):
        core = mock.MagicMock()
        core.read_namespace.side_effect = _api_exception(404)
        r = verify_namespace_identity(core, "ns", "mine", "sha")
        assert r.verdict is IdentityVerdict.NOT_FOUND


# ---------------------------------------------------------------------------
# Bucket ownership tag
# ---------------------------------------------------------------------------


class TestBucketOwnershipTag:
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

    def test_verify_missing_bucket(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NoSuchBucket")
        r = verify_bucket_ownership(s3, "b1", "mine", expected_cluster=None, created_record=())
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


class TestBucketNamePrefixFallback:
    """Name-prefix ownership check used on UNSUPPORTED backends.

    The safety property is: a deployment's fallback ownership check
    must never grant it a bucket that a longer-prefix sibling
    deployment owns. Otherwise ``prod`` silently adopts ``prod-eu``'s
    buckets on backends without tag support.
    """

    @pytest.mark.parametrize(
        ("bucket", "deployment", "others", "owned"),
        [
            ("mydeploy", "mydeploy", None, True),
            ("mydeploy-bronze", "mydeploy", None, True),
            ("mydeploy-silver", "mydeploy", None, True),
            ("mydeploy-gold", "mydeploy", None, True),
            # the hyphen is required: an adjacent deployment's bucket is never adopted
            ("mydeploybar-bronze", "mydeploy", None, False),
            ("mydeploy2-bronze", "mydeploy", None, False),
            ("otherteam-bronze", "mydeploy", None, False),
            ("legacy-data", "mydeploy", None, False),
            # an empty deployment name (misconfig) adopts nothing
            ("anything", "", None, False),
            ("", "", None, False),
            # longest live prefix wins
            ("prod-eu-bronze", "prod", ["prod-eu"], False),
            ("prod-eu-bronze", "prod-eu", ["prod"], True),
            ("prod-eu-preview-silver", "prod-eu-preview", ["prod", "prod-eu"], True),
            ("prod-eu-preview-silver", "prod-eu", ["prod", "prod-eu", "prod-eu-preview"], False),
            ("myapp-bronze", "myapp", ["other"], True),
            # the deployment itself among the others is ignored
            ("myapp-bronze", "myapp", ["myapp", "other"], True),
        ],
    )
    def test_claim(self, bucket, deployment, others, owned):
        kw = {} if others is None else {"other_deployment_names": others}
        assert bucket_name_matches_deployment(bucket, deployment, **kw) is owned


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

    @pytest.mark.parametrize(
        ("items", "exclude", "names"),
        [
            ([("default", {}, {}), ("kube-system", {}, {})], None, []),
            (
                [
                    ("team-a-ns", {ANNOTATION_DEPLOYMENT_NAME: "team-a"}, {}),
                    ("team-b-ns", {ANNOTATION_DEPLOYMENT_NAME: "team-b"}, {}),
                ],
                None,
                ["team-a", "team-b"],
            ),
            # pre-PR-1 namespaces carry only the managed-by label
            (
                [("legacy-ns", {}, {"app.kubernetes.io/managed-by": "lakebench"})],
                None,
                ["legacy-ns"],
            ),
            (
                [
                    ("team-a-ns", {ANNOTATION_DEPLOYMENT_NAME: "team-a"}, {}),
                    ("team-b-ns", {ANNOTATION_DEPLOYMENT_NAME: "team-b"}, {}),
                ],
                "team-a-ns",
                ["team-b"],
            ),
        ],
    )
    def test_names(self, items, exclude, names):
        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.return_value = SimpleNamespace(
            items=[self._ns(n, annotations=a, labels=lab) for n, a, lab in items]
        )
        kw = {"exclude": exclude} if exclude else {}
        assert sorted(list_lakebench_deployment_names(core_v1, **kw)) == names

    @pytest.mark.parametrize("unreadable", ["rbac-403", "no-kubeconfig"])
    def test_unreadable_list_is_none_not_empty(self, unreadable):
        """None (not []) so callers refuse the name-prefix fallback rather
        than silently accept it."""
        from kubernetes.config.config_exception import ConfigException

        from lakebench.deploy.ownership import list_lakebench_deployment_names

        core_v1 = mock.MagicMock()
        core_v1.list_namespace.side_effect = (
            _api_exception(403, "Forbidden") if unreadable == "rbac-403" else ConfigException("x")
        )
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

    def test_forget_drops_deleted_names_and_clears_when_empty(self):
        from lakebench.deploy.ownership import (
            ANNOTATION_CREATED_BUCKETS,
            forget_created_buckets,
        )

        core = mock.MagicMock()
        core.read_namespace.return_value = self._ns({ANNOTATION_CREATED_BUCKETS: "a-bronze,a-gold"})
        forget_created_buckets(core, "a", ["a-gold"])
        body = core.patch_namespace.call_args.args[1]
        assert body["metadata"]["annotations"][ANNOTATION_CREATED_BUCKETS] == "a-bronze"
        core.read_namespace.return_value = self._ns({ANNOTATION_CREATED_BUCKETS: "a-bronze"})
        forget_created_buckets(core, "a", ["a-bronze"])
        body = core.patch_namespace.call_args.args[1]
        assert body["metadata"]["annotations"][ANNOTATION_CREATED_BUCKETS] is None

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


# -- CLI-1: a check that could not run is not a refusal --------------------


def test_unlistable_namespaces_without_a_namespace_is_unverifiable():
    from lakebench.deploy.ownership import check_data_ownership

    d = check_data_ownership(
        None,
        namespace="a",
        deployment_name="a",
        namespace_present=False,
        namespace_verified=False,
        force_legacy=False,
    )
    assert not d.allowed and d.unverifiable


def test_same_name_live_deployment_is_a_refusal_not_unverifiable():
    from types import SimpleNamespace
    from unittest.mock import MagicMock

    from lakebench.deploy.ownership import ANNOTATION_DEPLOYMENT_NAME, check_data_ownership

    other = SimpleNamespace(
        metadata=SimpleNamespace(
            name="b",
            deletion_timestamp=None,
            annotations={ANNOTATION_DEPLOYMENT_NAME: "a"},
            labels={},
        )
    )
    core = MagicMock()
    core.list_namespace.return_value = SimpleNamespace(items=[other])
    d = check_data_ownership(
        core,
        namespace="a",
        deployment_name="a",
        namespace_present=True,
        namespace_verified=True,
        force_legacy=False,
    )
    assert not d.allowed and not d.unverifiable


# ---------------------------------------------------------------------------
# A redeploy refreshes the committed-sha stamp; identity checks unchanged
# ---------------------------------------------------------------------------


class TestStampNamespaceRefreshesCommittedSha:
    _OURS = {ANNOTATION_DEPLOYMENT_NAME: "my-config", ANNOTATION_API_SERVER: "deadbeef1234"}

    def _stamp(self, core, sha, **kw):
        return stamp_namespace(
            core, "ns", "my-config", api_server="deadbeef1234", committed_sha=sha, **kw
        )

    def test_redeploy_from_other_code_refreshes_the_sha(self):
        from lakebench.deploy.ownership import ANNOTATION_COMMITTED_SHA, ANNOTATION_STAMPED_AT

        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={
                **self._OURS,
                ANNOTATION_COMMITTED_SHA: "90a8478",
                ANNOTATION_STAMPED_AT: "2026-10-01T00:00:00Z",
            },
            resource_version="7",
        )
        r = self._stamp(core, "69ee2fc")
        assert r.verdict is IdentityVerdict.MATCH
        core.patch_namespace.assert_called_once()
        body = core.patch_namespace.call_args[0][1]
        anns = body["metadata"]["annotations"]
        # Only the sha is sent: identity and the first claim's time stay as
        # stored, and the write is OCC on the read's resourceVersion.
        assert anns == {ANNOTATION_COMMITTED_SHA: "69ee2fc"}
        assert ANNOTATION_STAMPED_AT not in anns
        assert body["metadata"]["resourceVersion"] == "7"

    def test_redeploy_that_cannot_name_its_commit_drops_the_stale_sha(self):
        from lakebench.deploy.ownership import ANNOTATION_COMMITTED_SHA

        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={**self._OURS, ANNOTATION_COMMITTED_SHA: "90a8478"}
        )
        r = self._stamp(core, None)
        assert r.verdict is IdentityVerdict.MATCH
        anns = core.patch_namespace.call_args[0][1]["metadata"]["annotations"]
        assert anns[ANNOTATION_COMMITTED_SHA] is None  # merge patch: delete

    def test_foreign_identity_is_never_refreshed(self):
        from lakebench.deploy.ownership import ANNOTATION_COMMITTED_SHA

        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={
                ANNOTATION_DEPLOYMENT_NAME: "someone-else",
                ANNOTATION_API_SERVER: "deadbeef1234",
                ANNOTATION_COMMITTED_SHA: "90a8478",
            }
        )
        assert self._stamp(core, "69ee2fc").verdict is IdentityVerdict.MISMATCH
        core.patch_namespace.assert_not_called()

    def test_refresh_conflict_rereads_and_rechecks_identity(self):
        from lakebench.deploy.ownership import ANNOTATION_COMMITTED_SHA

        core = mock.MagicMock()
        core.read_namespace.side_effect = [
            _ns_response(annotations={**self._OURS, ANNOTATION_COMMITTED_SHA: "90a8478"}),
            _ns_response(
                annotations={
                    ANNOTATION_DEPLOYMENT_NAME: "someone-else",
                    ANNOTATION_API_SERVER: "deadbeef1234",
                }
            ),
        ]
        core.patch_namespace.side_effect = [_api_exception(409)]
        assert self._stamp(core, "69ee2fc").verdict is IdentityVerdict.MISMATCH
        assert core.patch_namespace.call_count == 1

    @pytest.mark.parametrize("error", [409, 500, "timeout"])
    def test_a_failed_refresh_never_refuses_the_deploy(self, error):
        from lakebench.deploy.ownership import ANNOTATION_COMMITTED_SHA

        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(
            annotations={**self._OURS, ANNOTATION_COMMITTED_SHA: "90a8478"}
        )
        core.patch_namespace.side_effect = (
            TimeoutError("read timed out") if error == "timeout" else _api_exception(error)
        )
        assert self._stamp(core, "69ee2fc", max_retries=2).verdict is IdentityVerdict.MATCH


def test_owner_marker_records_the_lakebench_version():
    """The distribution is lakebench-k8s, so version("lakebench") raised and
    every marker said "unknown"; the marker carries the package version."""
    import lakebench
    from lakebench.deploy.ownership import owner_marker_identity

    body = owner_marker_identity("my-config", "deadbeef1234", "ns")
    assert body["lakebench_version"] == lakebench.__version__
    assert body["lakebench_version"] != "unknown"
