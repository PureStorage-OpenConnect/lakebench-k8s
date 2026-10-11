"""Tests for deployment module."""

from unittest.mock import MagicMock, patch

import pytest

from lakebench.deploy.engine import (
    DeploymentEngine,
    DeploymentStatus,
)
from tests.fixtures.deploy_helpers import _make_config as _make_config
from tests.fixtures.deploy_helpers import _mock_k8s as _mock_k8s

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _no_live_namespace_listing():
    """These tests exercise ownership logic that lists namespaces. Without
    this they reached whatever cluster the developer's kubeconfig pointed at
    (a live, read-only call); in CI they failed. An empty cluster is the
    neutral answer; tests that need specific namespaces patch CoreV1Api
    themselves (inner patches win)."""
    with patch("kubernetes.client.CoreV1Api") as core:
        core.return_value.list_namespace.return_value.items = []
        yield core


@pytest.fixture(autouse=True)
def _cluster_fingerprint():
    """Deploy stamps buckets with this cluster's fingerprint and
    refuses without one; give the tests a cluster (tests that need none
    patch it themselves)."""
    with patch("lakebench.deploy.ownership.api_server_fingerprint", return_value="fp-test"):
        yield


@patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
def test_missing_scratch_storage_class_fails_deploy(_mock_ocp):
    from kubernetes.client.exceptions import ApiException

    config = _make_config()
    config.platform.storage.scratch.enabled = True
    engine = DeploymentEngine(config, k8s_client=_mock_k8s())

    with patch(
        "kubernetes.client.StorageV1Api.read_storage_class",
        side_effect=ApiException(status=404, reason="Not Found"),
    ):
        result = engine._deploy_scratch_storageclass()

    assert result.status == DeploymentStatus.FAILED


@patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
@patch("lakebench.s3.S3Client")
def test_bucket_deploy_fails_when_the_s3_client_cannot_init(mock_s3_cls, _mock_ocp):
    engine = DeploymentEngine(_make_config(), k8s_client=_mock_k8s())

    mock_client = MagicMock()
    mock_client._init_error = "bad endpoint"
    mock_s3_cls.return_value = mock_client

    result = engine._deploy_buckets()
    assert result.status == DeploymentStatus.FAILED


# ---------------------------------------------------------------------------
# Integration: ownership hooks actually fire in deploy + destroy
#
# These guard against silent removal of the hook call sites in engine.py
# and destroy.py. Deleting them would leave the ownership module intact
# and its unit tests still passing, so we need call-site assertions.
# ---------------------------------------------------------------------------


class TestOwnershipHooksFire:
    """The ownership hooks are wired into the deploy and clean paths."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.write_deploy_nonce")
    @patch("lakebench.deploy.ownership.stamp_namespace")
    def test_deploy_fails_when_the_nonce_cannot_be_written(self, mock_stamp, mock_nonce, _mock_ocp):
        """A destroy already running on this namespace compares the nonce; a
        redeploy that cannot change it must not go on."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        k8s.namespace_exists.return_value = True
        engine = DeploymentEngine(config, k8s_client=k8s)
        mock_stamp.return_value = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="test-deploy",
            expected_deployment="test-deploy",
        )
        with patch("kubernetes.client.CoreV1Api"):
            assert engine._deploy_namespace().status != DeploymentStatus.FAILED

        mock_nonce.side_effect = RuntimeError("apiserver 503")
        with patch("kubernetes.client.CoreV1Api"):
            assert engine._deploy_namespace().status == DeploymentStatus.FAILED

    def test_write_deploy_nonce_is_fresh_each_time(self):
        from lakebench.deploy.ownership import ANNOTATION_DEPLOY_NONCE, write_deploy_nonce

        core = MagicMock()
        a = write_deploy_nonce(core, "ns")
        b = write_deploy_nonce(core, "ns")
        assert a != b
        body = core.patch_namespace.call_args.args[1]
        assert body["metadata"]["annotations"][ANNOTATION_DEPLOY_NONCE] == b

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.stamp_namespace")
    def test_deploy_namespace_refuses_on_mismatch(self, mock_stamp, _mock_ocp):
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        k8s.namespace_exists.return_value = True
        engine = DeploymentEngine(config, k8s_client=k8s)
        mock_stamp.return_value = IdentityReport(
            verdict=IdentityVerdict.MISMATCH,
            resource_name="test-deploy",
            expected_deployment="test-deploy",
            found_deployment="someone-else",
            hint="claimed by someone-else",
        )
        result = engine._deploy_namespace()
        assert result.status == DeploymentStatus.FAILED
        assert "ownership refused" in result.message
        assert result.details["refusal"] == "deploy.identity_foreign"  # exit 3 (CLI-1)

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_deploy_buckets_refuses_legacy_bucket_without_force(
        self, mock_s3_cls, mock_verify, _mock_ocp
    ):
        """A pre-existing untagged bucket refuses without --force-legacy."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        mock_client = MagicMock()
        mock_client._init_error = None
        mock_client.raw_client = MagicMock()
        # Bucket already existed (was_created=False for all)
        mock_client.ensure_buckets.return_value = {
            "lakebench-bronze": False,
            "lakebench-silver": False,
            "lakebench-gold": False,
        }
        mock_s3_cls.return_value = mock_client
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.ABSENT,
            resource_name="b",
            expected_deployment="test-deploy",
            hint="untagged bucket",
        )

        result = engine._deploy_buckets(force_legacy=False)
        assert result.status == DeploymentStatus.FAILED
        assert "force-legacy" in result.message

    @pytest.mark.parametrize("first_apply_raises", [False, True])
    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.stamp_namespace")
    def test_retry_after_creating_the_namespace_stamps_as_its_owner(
        self, mock_stamp, _mock_ocp, first_apply_raises
    ):
        """The first attempt creates the namespace (and either the stamp or
        the apply fails transiently after the API accepted the create). On the
        retry the namespace exists, but this run made it: the stamp is made
        with force_legacy, not refused for want of a flag the user never
        needed."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        k8s.namespace_exists.side_effect = [False, True]
        if first_apply_raises:
            k8s.apply_manifest.side_effect = [ConnectionError("reset"), None]
        engine = DeploymentEngine(config, k8s_client=k8s)
        mock_stamp.return_value = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="test-deploy",
            expected_deployment="test-deploy",
        )

        if first_apply_raises:
            with pytest.raises(ConnectionError):
                engine._deploy_namespace(force_legacy=False)
        else:
            assert engine._deploy_namespace(force_legacy=False).status == DeploymentStatus.SUCCESS

        result = engine._deploy_namespace(force_legacy=False)
        assert result.status == DeploymentStatus.SUCCESS
        assert mock_stamp.call_args.kwargs["force_legacy"] is True

    @pytest.mark.parametrize(
        ("target", "verdict", "force_legacy", "refused"),
        [
            # A bucket owned by another deployment is never emptied, whichever
            # bucket the target names.
            ("silver", "MISMATCH", False, True),
            ("gold", "MISMATCH", False, True),
            # An untagged bucket is refused without --force-legacy and
            # emptied with it.
            ("silver", "ABSENT", False, True),
            ("silver", "ABSENT", True, False),
        ],
    )
    def test_clean_empties_only_buckets_this_deployment_may(
        self, tmp_path, monkeypatch, target, verdict, force_legacy, refused
    ):
        import typer

        from lakebench.cli._clean import clean
        from lakebench.deploy import ownership
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        cfg_path = tmp_path / "my-clean.yaml"
        cfg_path.write_text(
            "name: my-clean\n"
            "platform:\n"
            "  storage:\n"
            "    s3:\n"
            "      endpoint: http://minio:9000\n"
            "      access_key: k\n"
            "      secret_key: s\n"
        )
        emptied: list[str] = []
        s3 = MagicMock()
        s3._init_error = None
        s3.empty_bucket.side_effect = lambda bucket, **_k: emptied.append(bucket) or 0
        monkeypatch.setattr("lakebench.s3.S3Client", lambda *a, **k: s3)
        monkeypatch.setattr(
            ownership,
            "verify_bucket_ownership",
            lambda _c, name, _id, **_k: IdentityReport(
                verdict=IdentityVerdict[verdict],
                resource_name=name,
                expected_deployment="my-clean",
                found_deployment="someone-else",
                hint="not ours",
            ),
        )
        monkeypatch.setattr(
            ownership,
            "verify_namespace_identity",
            lambda *a, **k: IdentityReport(
                verdict=IdentityVerdict.MATCH, resource_name="ns", expected_deployment="my-clean"
            ),
        )
        monkeypatch.setattr(ownership, "build_identity_from_config", lambda *a, **k: MagicMock())
        monkeypatch.setattr(
            "lakebench.modules.table_formats.iceberg.maintenance.find_maintenance_engine",
            lambda *a, **k: (None, None, None),
        )
        batch = MagicMock()
        batch.read_namespaced_job.side_effect = Exception("no k8s")
        monkeypatch.setattr("kubernetes.client.BatchV1Api", lambda *a, **k: batch)

        def run_clean():
            clean(
                target=target,
                config_file=cfg_path,
                file_option=None,
                force=True,
                force_legacy=force_legacy,
            )

        if refused:
            with pytest.raises(typer.Exit) as exc:
                run_clean()
            assert exc.value.exit_code == 3
            assert emptied == []
        else:
            run_clean()
            assert len(emptied) == 1


class TestBucketCreationRecord:
    """Deploy records which buckets it created; destroy deletes only those."""

    def _deploy(self, ensure_result, prior_tags=None, mismatch=(), absent=()):
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        engine = DeploymentEngine(config, k8s_client=_mock_k8s())
        client = MagicMock()
        client._init_error = None
        client.ensure_buckets.return_value = ensure_result
        with (
            patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False),
            patch("lakebench.s3.S3Client", return_value=client),
            patch(
                "lakebench.deploy.ownership.verify_bucket_ownership",
                side_effect=lambda _b, name, _id, **_k: IdentityReport(
                    verdict=(
                        IdentityVerdict.MISMATCH
                        if name in mismatch
                        else IdentityVerdict.ABSENT
                        if name in absent
                        else IdentityVerdict.MATCH
                    ),
                    resource_name=name,
                    expected_deployment=config.name,
                    hint="owned by someone else",
                ),
            ),
            patch(
                "lakebench.deploy.ownership.read_bucket_ownership_tag",
                side_effect=lambda _b, name: (prior_tags or {}).get(name),
            ),
            patch("lakebench.deploy.ownership.write_bucket_ownership_tag") as write,
            patch("lakebench.deploy.ownership.record_created_buckets") as record,
            patch("lakebench.k8s.get_k8s_client"),
            patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=[]),
        ):
            result = engine._deploy_buckets()
        created_flags = {c.args[1]: c.kwargs["created"] for c in write.call_args_list}
        return result, created_flags, record, config

    @pytest.mark.parametrize(
        ("created", "absent"),
        [
            # Some buckets already existed.
            (["lakebench-bronze", "lakebench-gold"], []),
            # A bucket just created is untagged: tagged and recorded without
            # --force-legacy.
            (
                ["lakebench-bronze", "lakebench-silver", "lakebench-gold"],
                ["lakebench-bronze", "lakebench-silver", "lakebench-gold"],
            ),
        ],
    )
    def test_created_buckets_are_tagged_and_recorded(self, created, absent):
        names = ["lakebench-bronze", "lakebench-silver", "lakebench-gold"]
        result, flags, record, config = self._deploy(
            {n: n in created for n in names}, absent=absent
        )
        assert result.status == DeploymentStatus.SUCCESS
        assert flags == {n: n in created for n in names}
        record.assert_called_once()
        assert record.call_args.args[2] == created

    def test_redeploy_keeps_the_created_marker(self):
        from lakebench.deploy.ownership import TAG_CREATED_BY_LAKEBENCH, TAG_DEPLOYMENT_NAME

        config_name = _make_config().name
        prior = {
            "lakebench-bronze": {
                TAG_DEPLOYMENT_NAME: config_name,
                TAG_CREATED_BY_LAKEBENCH: "true",
            },
            "lakebench-silver": {TAG_DEPLOYMENT_NAME: config_name},
        }
        _, flags, record, _ = self._deploy(
            {"lakebench-bronze": False, "lakebench-silver": False, "lakebench-gold": False},
            prior_tags=prior,
        )
        assert flags == {
            "lakebench-bronze": True,
            "lakebench-silver": False,
            "lakebench-gold": False,
        }
        record.assert_not_called()

    def test_failure_on_a_later_bucket_still_records_the_created_ones(self):
        """A deploy failing on silver still records bronze, or every later
        destroy would keep it for good on FlashBlade."""
        result, _, record, _ = self._deploy(
            {"lakebench-bronze": True, "lakebench-silver": False, "lakebench-gold": False},
            mismatch={"lakebench-silver"},
        )
        assert result.status == DeploymentStatus.FAILED
        record.assert_called_once()
        assert record.call_args.args[2] == ["lakebench-bronze"]


class TestTaglessAdoptionRecord:
    """Backends without bucket tagging: with --force-legacy deploy records a
    pre-existing bucket it adopts while empty, so destroy may empty it later;
    one that already holds objects is not recorded (its data may not be
    lakebench's). Without the flag nothing is adopted: an empty, unmarked
    bucket may be another cluster's not yet written."""

    @pytest.mark.parametrize("force_legacy", [True, False])
    def test_empty_adopted_bucket_is_recorded_non_empty_is_not(self, force_legacy):
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config(name="td")
        b = config.platform.storage.s3.buckets
        b.bronze, b.silver, b.gold = "td-bronze", "td-silver", "td-gold"
        engine = DeploymentEngine(config, k8s_client=_mock_k8s())
        client = MagicMock()
        client._init_error = None
        client.ensure_buckets.return_value = {
            "td-bronze": False,
            "td-silver": False,
            "td-gold": True,
        }
        client.raw_client.list_objects_v2.side_effect = lambda Bucket, MaxKeys, **_k: (
            {"KeyCount": 1, "Contents": [{"Key": "d/1"}]} if Bucket == "td-silver" else {}
        )
        with (
            patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False),
            # The marker itself is tested on the S3 fake (test_owner_marker.py).
            patch("lakebench.deploy.engine.DeploymentEngine._stamp_owner_marker", return_value=""),
            patch("lakebench.s3.S3Client", return_value=client),
            patch(
                "lakebench.deploy.ownership.verify_bucket_ownership",
                side_effect=lambda _b, name, _id, **_k: IdentityReport(
                    verdict=IdentityVerdict.UNSUPPORTED,
                    resource_name=name,
                    expected_deployment="td",
                ),
            ),
            patch("lakebench.deploy.ownership.record_created_buckets"),
            patch("lakebench.deploy.ownership.read_created_buckets", return_value=set()),
            patch("lakebench.deploy.ownership.record_adopted_empty_buckets") as adopted,
            patch("lakebench.k8s.get_k8s_client"),
            patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=[]),
        ):
            result = engine._deploy_buckets(force_legacy=force_legacy)
        assert result.status == DeploymentStatus.SUCCESS, result.message
        if force_legacy:
            adopted.assert_called_once()
            assert adopted.call_args.args[2] == ["td-bronze"]
        else:
            adopted.assert_not_called()


@pytest.mark.parametrize("force_legacy", [True, False])
def test_preprovisioned_empty_tagless_buckets_are_recorded(force_legacy):
    """With create_buckets=false the empty tagless buckets are recorded, or
    clean and the continuous reset would refuse pre-provisioned FlashBlade
    buckets forever: only with --force-legacy (the operator's word that no
    other cluster uses the names)."""
    from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

    config = _make_config(name="td")
    s3c = config.platform.storage.s3
    s3c.create_buckets = False
    s3c.buckets.bronze, s3c.buckets.silver, s3c.buckets.gold = "td-bronze", "other", "td-gold"
    engine = DeploymentEngine(config, k8s_client=_mock_k8s())
    client = MagicMock()
    client._init_error = None
    client.has_user_objects.side_effect = lambda bucket: bucket == "td-gold"
    with (
        patch("lakebench.s3.S3Client", return_value=client),
        patch("lakebench.deploy.engine.DeploymentEngine._stamp_owner_marker", return_value=""),
        patch(
            "lakebench.deploy.ownership.verify_bucket_ownership",
            side_effect=lambda _b, name, _id, **_k: IdentityReport(
                verdict=IdentityVerdict.UNSUPPORTED, resource_name=name, expected_deployment="td"
            ),
        ),
        patch("lakebench.deploy.ownership.record_adopted_empty_buckets") as adopted,
        patch("lakebench.k8s.get_k8s_client"),
        patch("lakebench.deploy.ownership.list_lakebench_deployment_names", return_value=[]),
    ):
        result = engine._deploy_buckets(force_legacy=force_legacy)
    assert result.status == DeploymentStatus.SKIPPED
    if force_legacy:
        assert adopted.call_args.args[2] == ["td-bronze"]
    else:
        adopted.assert_not_called()
