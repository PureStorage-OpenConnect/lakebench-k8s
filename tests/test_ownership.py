"""Deployment identity and resource ownership: destroying deployment A never
affects deployment B. Each test names the ownership behaviour it checks."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from kubernetes.client.rest import ApiException
from typer.testing import CliRunner

from lakebench.deploy.engine import DeploymentStatus
from lakebench.deploy.ownership import (
    ANNOTATION_API_SERVER,
    ANNOTATION_DEPLOY_NONCE,
    ANNOTATION_DEPLOYMENT_NAME,
    DEPLOYMENT_NAME_MAX,
    TAG_DEPLOYMENT_NAME,
    BucketOwnershipError,
    BucketTaggingUnsupported,
    IdentityVerdict,
    api_server_fingerprint,
    bucket_name_matches_deployment,
    read_bucket_ownership_tag,
    stamp_namespace,
    verify_bucket_ownership,
    verify_namespace_identity,
    write_bucket_ownership_tag,
)
from lakebench.exit_codes import REFUSAL_DETAIL
from tests.fixtures.recording_k8s import recording

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


def _kubeconfig_fingerprint(ca_b64: str, server: str = "https://api.example.com:6443"):
    """The fingerprint of a workstation context whose kubeconfig carries this CA."""
    with mock.patch("kubernetes.config.list_kube_config_contexts") as mock_list:
        mock_list.return_value = (
            [{"name": "c", "context": {"cluster": "prod"}}],
            {"name": "c", "context": {"cluster": "prod"}},
        )
        with mock.patch("kubernetes.config.kube_config.KubeConfigMerger") as MockMerger:
            MockMerger.return_value.config.value = {
                "clusters": [
                    {
                        "name": "prod",
                        "cluster": {"server": server, "certificate-authority-data": ca_b64},
                    }
                ]
            }
            return api_server_fingerprint()


def _in_cluster_fingerprint(ca_bytes: bytes):
    """The fingerprint of a pod context that reads the mounted CA bytes."""
    with mock.patch(
        "kubernetes.config.list_kube_config_contexts",
        side_effect=Exception("no kubeconfig in pod"),
    ):
        with mock.patch("builtins.open", mock.mock_open(read_data=ca_bytes)):
            return api_server_fingerprint()


class TestApiServerFingerprint:
    @pytest.mark.parametrize(
        ("ca_a", "path_a", "ca_b", "path_b", "equal"),
        [
            # The same CA reached through the kubeconfig and through the pod's mounted CA
            # (the endpoint URL differs between those paths) is one cluster.
            (b"pem-ca-bytes", "kubeconfig", b"pem-ca-bytes", "in_cluster", True),
            # Two clusters at the same URL but different CAs must not collide.
            (b"CA_A", "kubeconfig", b"CA_B", "in_cluster", False),
        ],
        ids=["same_ca_across_paths", "different_ca_across_paths"],
    )
    def test_fingerprint_follows_the_ca(self, ca_a, path_a, ca_b, path_b, equal):
        import base64

        def fingerprint(ca: bytes, path: str):
            if path == "kubeconfig":
                return _kubeconfig_fingerprint(base64.b64encode(ca).decode())
            return _in_cluster_fingerprint(ca)

        a = fingerprint(ca_a, path_a)
        b = fingerprint(ca_b, path_b)
        assert a is not None and b is not None
        assert (a == b) is equal

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


# ---------------------------------------------------------------------------
# stamp_namespace -- optimistic-concurrency PATCH
# ---------------------------------------------------------------------------


class TestStampNamespace:
    def test_fresh_namespace_gets_stamped(self):
        """A fresh (annotation-less) namespace stamps under force_legacy=True."""
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
        """A legacy annotation-less namespace is refused without force_legacy."""
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
        """A conflict that resolves to a foreign identity written during our
        write is refused, not overwritten."""
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


@pytest.mark.parametrize(
    ("kind", "exc"), [("namespace", ValueError), ("bucket", BucketOwnershipError)]
)
def test_oversize_deployment_name_is_refused_before_any_write(kind, exc):
    """An over-long name would be truncated in the stamp: refuse, write nothing."""
    long_name = "a" * (DEPLOYMENT_NAME_MAX + 1)
    if kind == "namespace":
        core = mock.MagicMock()
        with pytest.raises(exc):
            stamp_namespace(core, "ns", long_name, api_server=None)
        core.patch_namespace.assert_not_called()
    else:
        s3 = mock.MagicMock()
        with pytest.raises(exc):
            write_bucket_ownership_tag(s3, "b1", long_name)
        s3.put_bucket_tagging.assert_not_called()


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
            # another deployment's namespace is never ours
            (
                {ANNOTATION_DEPLOYMENT_NAME: "other"},
                "deadbeef1234",
                False,
                IdentityVerdict.MISMATCH,
            ),
        ],
    )
    def test_verdict(self, stored, current, allow, verdict):
        core = mock.MagicMock()
        core.read_namespace.return_value = _ns_response(annotations=stored)
        kw = {"allow_unverified_cluster": True} if allow else {}
        r = verify_namespace_identity(core, "ns", "mine", current, **kw)
        assert r.verdict is verdict
        if stored.get(ANNOTATION_DEPLOYMENT_NAME) == "other":
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
    def test_write_raises_when_backend_drops_tags(self):
        """A backend that accepts PutBucketTagging but returns empty on
        GetBucketTagging must not be treated as 'owned'."""
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NoSuchTagSet")
        with pytest.raises(BucketOwnershipError, match="returned no tags"):
            write_bucket_ownership_tag(s3, "b1", "my-config")

    def test_write_raises_on_round_trip_mismatch(self):
        """PutBucketTagging succeeds but GetBucketTagging shows a different
        value (async propagation, truncation): refuse."""
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.return_value = {
            "TagSet": [{"Key": TAG_DEPLOYMENT_NAME, "Value": "mangled"}]
        }
        with pytest.raises(BucketOwnershipError, match="round-trip mismatch"):
            write_bucket_ownership_tag(s3, "b1", "my-config")

    def test_verify_missing_bucket(self):
        s3 = mock.MagicMock()
        s3.get_bucket_tagging.side_effect = _client_error("NoSuchBucket")
        r = verify_bucket_ownership(s3, "b1", "mine", expected_cluster=None, created_record=())
        assert r.verdict is IdentityVerdict.NOT_FOUND


class TestBucketTaggingUnsupported:
    """FlashBlade returns NotImplemented on GetBucketTagging /
    PutBucketTagging. Every path must handle it explicitly rather than
    falling through to a generic error.
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
            # the hyphen is required: an adjacent deployment's bucket is never adopted
            ("mydeploybar-bronze", "mydeploy", None, False),
            ("mydeploy2-bronze", "mydeploy", None, False),
            # an empty deployment name (misconfig) adopts nothing
            ("anything", "", None, False),
            # longest live prefix wins
            ("prod-eu-bronze", "prod", ["prod-eu"], False),
            ("prod-eu-bronze", "prod-eu", ["prod"], True),
            ("prod-eu-preview-silver", "prod-eu-preview", ["prod", "prod-eu"], True),
            ("prod-eu-preview-silver", "prod-eu", ["prod", "prod-eu", "prod-eu-preview"], False),
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
    prefix ownership."""

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
            # older namespaces carry only the managed-by label
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


# -- a check that could not run is not a refusal ---------------------------


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
    """The marker carries the package version, never "unknown"."""
    import lakebench
    from lakebench.deploy.ownership import owner_marker_identity

    body = owner_marker_identity("my-config", "deadbeef1234", "ns")
    assert body["lakebench_version"] == lakebench.__version__
    assert body["lakebench_version"] != "unknown"


# ---------------------------------------------------------------------------
# A harness destroys only what it created
# ---------------------------------------------------------------------------

NAME = "lbreqnew"
CFG = (
    f"name: {NAME}\n"
    "platform:\n"
    "  storage:\n"
    "    s3:\n"
    "      endpoint: http://10.0.1.50:80\n"
    "      access_key: test-access\n"
    "      secret_key: test-secret\n"
)


@pytest.fixture
def cfg_path(tmp_path: Path) -> Path:
    p = tmp_path / "reqnew.yaml"
    p.write_text(CFG)
    return p


def _cfg(path: Path):
    from lakebench.config import LoadPurpose, load_config

    return load_config(path, purpose=LoadPurpose.RUN)


# ---------------------------------------------------------------------------
# deploy --require-new (hidden): refuse an existing namespace or bucket,
# never adopt it (exit 3, deploy.existing_namespace). Cluster and S3 calls go
# to the recording fake (tests/fixtures/recording_k8s.py), so "no delete of
# any kind" is checked on every call.
# ---------------------------------------------------------------------------


def _engine(require_new: bool = True, exists: bool = False):
    from lakebench.deploy.engine import DeploymentEngine
    from tests.fixtures.deploy_helpers import _make_config, _mock_k8s

    k8s = _mock_k8s()
    k8s.namespace_exists.return_value = exists
    with patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False):
        eng = DeploymentEngine(_make_config(), k8s_client=k8s, require_new=require_new)
    return eng, k8s


@pytest.mark.parametrize(
    ("created_this_run", "annotation"),
    [
        (False, None),  # an existing namespace
        (True, "theirs"),  # a seeded name carrying another nonce
        # The nonce alone is not proof: the namespace must be one this engine
        # tried to create.
        (False, "mine"),
    ],
    ids=["existing", "seeded-other-nonce", "matching-nonce-not-created"],
)
def test_engine_require_new_refuses_a_namespace_it_did_not_create(created_this_run, annotation):
    eng, k8s = _engine(exists=True)
    eng.deploy_nonce = "mine"
    if created_this_run:
        eng._namespace_created_this_run.add(eng.config.get_namespace())
    k8s.get_namespace_annotation.return_value = annotation
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
    ):
        result = eng._deploy_namespace()
    assert result.status is DeploymentStatus.FAILED
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
    stamp.assert_not_called()


def test_engine_require_new_refuses_a_namespace_created_meanwhile():
    """The check passed, then a competing create landed: the plain create gets
    409 and the namespace is refused, not adopted and stamped."""
    from kubernetes.client.rest import ApiException

    eng, k8s = _engine(exists=False)
    core = MagicMock()
    core.create_namespace.side_effect = ApiException(status=409, reason="AlreadyExists")
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("kubernetes.client.CoreV1Api", return_value=core),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
        patch("lakebench.deploy.ownership.write_deploy_nonce") as nonce,
    ):
        result = eng._deploy_namespace()
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
    k8s.apply_manifest.assert_not_called()
    stamp.assert_not_called()
    nonce.assert_not_called()
    assert eng.config.get_namespace() not in eng._namespace_created_this_run


@patch("lakebench.deploy.ownership.write_bucket_ownership_tag")
@patch("lakebench.deploy.ownership.record_created_buckets")
@patch("lakebench.s3.S3Client")
def test_engine_require_new_refuses_an_existing_bucket(s3_cls, record, write_tag):
    eng, _ = _engine()
    client = MagicMock()
    client._init_error = None
    # The check passes; silver then appears before its create (a race).
    client.bucket_exists.return_value = False
    client.ensure_buckets.return_value = {
        "lakebench-bronze": True,
        "lakebench-silver": False,
        "lakebench-gold": True,
    }
    s3_cls.return_value = client
    with (
        patch("lakebench.k8s.get_k8s_client"),
        patch("kubernetes.client.CoreV1Api"),
        # Deploy stamps buckets with this cluster's fingerprint.
        patch("lakebench.deploy.ownership.api_server_fingerprint", return_value="fp-test"),
    ):
        result = eng._deploy_buckets()
    assert result.status is DeploymentStatus.FAILED
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
    assert "lakebench-silver" in result.message
    write_tag.assert_not_called()
    # The two it created are recorded, so a destroy can remove them.
    assert sorted(record.call_args.args[2]) == ["lakebench-bronze", "lakebench-gold"]


# ---------------------------------------------------------------------------
# destroy --expect-incarnation (hidden): only a refusal
# ---------------------------------------------------------------------------


class _Engine:
    """Real destroy_all over the recorder's fake API."""

    def __init__(self, cfg: Any, **_: Any):
        from lakebench.k8s.client import K8sClient

        self.config = cfg
        self.k8s = K8sClient(namespace=cfg.get_namespace())

    def destroy_all(self, **kw: Any):
        from lakebench.deploy.destroy import destroy_all

        return destroy_all(self, **kw)


def test_destroy_cli_expect_incarnation_mismatch(cfg_path, monkeypatch):
    from kubernetes import client

    from lakebench.cli import app

    cfg = _cfg(cfg_path)
    ns = cfg.get_namespace()
    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", _Engine)
    with recording(ns) as rec:
        rec.for_config(cfg)
        rec.add_namespace(
            ns, annotations={ANNOTATION_DEPLOYMENT_NAME: NAME, ANNOTATION_DEPLOY_NONCE: "Y"}
        )
        uid = client.CoreV1Api().read_namespace(ns).metadata.uid
        res = CliRunner().invoke(
            app,
            ["destroy", str(cfg_path), "--yes", "--force", "--expect-incarnation", f"{uid}#X"],
        )
        assert res.exit_code == 3, res.output
        assert rec.mutations() == []
    assert "Destroy NOT started" in res.output


@pytest.mark.parametrize("token", ["", "u#n#m"])
def test_destroy_expect_incarnation_malformed_is_a_usage_error(cfg_path, token):
    from lakebench.cli import app

    with recording() as rec:
        res = CliRunner().invoke(
            app, ["destroy", str(cfg_path), "--yes", "--expect-incarnation", token]
        )
        assert rec.calls == []
    assert res.exit_code == 2, res.output


def test_destroy_expect_incarnation_refuses_local(cfg_path):
    from lakebench.cli import app

    res = CliRunner().invoke(
        app, ["destroy", str(cfg_path), "--yes", "--local", "--expect-incarnation", "u#n"]
    )
    assert res.exit_code == 2, res.output


def test_destroy_expect_incarnation_must_equal_the_nameless_check(tmp_path, monkeypatch):
    """A nameless config's verified incarnation and the flag must agree."""
    import lakebench.cli._nameless as nameless
    from lakebench.cli import app
    from tests.fixtures import deploy_state_helpers as t

    core = t.FakeCore()
    monkeypatch.setattr(nameless, "_core_v1_factory", lambda cfg: lambda: core)
    monkeypatch.setattr(nameless, "_bucket_owned_factory", lambda cfg: lambda b: False)
    cfg = t._nameless(tmp_path)
    t._legacy_state(tmp_path)
    t._v16_namespace(core)
    called: list = []

    class Engine:
        def __init__(self, cfg, **_):
            pass

        def destroy_all(self, **kw):  # pragma: no cover -- must not be reached
            called.append(kw)
            return []

    monkeypatch.setattr("lakebench.deploy.DeploymentEngine", Engine)
    res = CliRunner().invoke(
        app,
        ["destroy", str(cfg), "--force", "--name", t.NAME, "--expect-incarnation", "u1#other"],
    )
    assert res.exit_code == 3, res.output
    assert called == []


def test_engine_require_new_accepts_its_own_create_after_a_lost_response():
    """The create landed but its response was lost: the retry finds the
    namespace carrying this deploy's nonce and goes on, instead of refusing
    (and leaking) its own namespace."""
    eng, k8s = _engine(exists=True)
    eng.deploy_nonce = "mine"
    eng._namespace_created_this_run.add(eng.config.get_namespace())
    k8s.get_namespace_annotation.return_value = "mine"
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
        patch("lakebench.deploy.ownership.write_deploy_nonce") as nonce,
        patch("kubernetes.client.CoreV1Api") as core,
    ):
        from lakebench.deploy.ownership import IdentityVerdict

        stamp.return_value = MagicMock(verdict=IdentityVerdict.MATCH)
        result = eng._deploy_namespace()
    assert not (result.details or {}).get(REFUSAL_DETAIL), result.message
    core.return_value.create_namespace.assert_not_called()
    assert stamp.call_args.kwargs["force_legacy"] is True
    assert nonce.call_args.kwargs["nonce"] == "mine"


def test_engine_stamps_the_recorded_nonce_not_a_fresh_one():
    """The real namespace step writes engine.deploy_nonce (the one deploy
    recorded); --expect-incarnation and the nameless checks depend on it."""
    eng, k8s = _engine(require_new=False, exists=False)
    eng.deploy_nonce = "recorded"
    with (
        patch.object(eng, "_namespace_already_using_name", return_value=None),
        patch("lakebench.deploy.ownership.stamp_namespace") as stamp,
        patch("lakebench.deploy.ownership.write_deploy_nonce") as nonce,
        patch("kubernetes.client.CoreV1Api"),
    ):
        from lakebench.deploy.ownership import IdentityVerdict

        stamp.return_value = MagicMock(verdict=IdentityVerdict.MATCH)
        eng._deploy_namespace()
    assert nonce.call_args.kwargs["nonce"] == "recorded"


@patch("lakebench.deploy.ownership.record_created_buckets")
@patch("lakebench.s3.S3Client")
def test_engine_require_new_checks_every_bucket_before_creating_any(s3_cls, record):
    eng, _ = _engine()
    client = MagicMock()
    client._init_error = None
    client.bucket_exists.side_effect = lambda b: b == "lakebench-gold"
    s3_cls.return_value = client
    result = eng._deploy_buckets()
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
    assert "lakebench-gold" in result.message
    client.ensure_buckets.assert_not_called()
    client.create_bucket.assert_not_called()
    record.assert_not_called()


def test_engine_require_new_refuses_preprovisioned_buckets():
    eng, _ = _engine()
    eng.config.platform.storage.s3.create_buckets = False
    with patch.object(eng, "_record_preprovisioned_empty_buckets") as adopt:
        result = eng._deploy_buckets()
    assert result.details[REFUSAL_DETAIL] == "deploy.existing_namespace"
    adopt.assert_not_called()
