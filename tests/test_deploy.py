"""Tests for deployment module."""

from unittest.mock import MagicMock, patch

import pytest

from lakebench.config import LakebenchConfig
from lakebench.deploy.engine import (
    DeploymentEngine,
    DeploymentStatus,
)

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
    """SAF-10: deploy stamps buckets with this cluster's fingerprint and
    refuses without one; give the tests a cluster (tests that need none
    patch it themselves)."""
    with patch("lakebench.deploy.ownership.api_server_fingerprint", return_value="fp-test"):
        yield


def _make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig with sensible defaults for testing.

    LB-090: auto-fills the Polaris client_secret for tests whose
    architecture selects the Polaris catalog, mirroring the top-level
    conftest helper. Production configs must supply their own.
    """
    from lakebench.config.schema import CatalogType

    base = {
        "name": "test-deploy",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://minio:9000",
                    "access_key": "minioadmin",
                    "secret_key": "minioadmin",
                    # Explicit: the default is now <name>-<layer>.
                    "buckets": {
                        "bronze": "lakebench-bronze",
                        "silver": "lakebench-silver",
                        "gold": "lakebench-gold",
                    },
                }
            }
        },
    }
    base.update(overrides)
    cfg = LakebenchConfig(**base)
    if (
        cfg.architecture.catalog.type == CatalogType.POLARIS
        and not cfg.architecture.catalog.polaris.client_secret
    ):
        cfg.architecture.catalog.polaris.client_secret = "test-only-secret"
    return cfg


def _mock_k8s():
    """Create a mock K8sClient."""
    k8s = MagicMock()
    k8s.namespace_exists.return_value = True
    k8s.apply_manifest.return_value = True
    k8s.get_cluster_capacity.return_value = None
    return k8s


# ---------------------------------------------------------------------------
# TemplateRenderer
# ---------------------------------------------------------------------------


class TestTemplateRenderer:
    """Tests for Jinja2 template rendering."""


# ---------------------------------------------------------------------------
# DeploymentResult / DeploymentStatus
# ---------------------------------------------------------------------------


class TestDeploymentResult:
    """Tests for DeploymentResult dataclass."""


# ---------------------------------------------------------------------------
# DeploymentEngine
# ---------------------------------------------------------------------------


class TestDeploymentEngine:
    """Tests for DeploymentEngine initialisation and context building."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_deploy_all_stops_on_failure(self, _mock_ocp):
        """deploy_all should stop deploying after the first failure."""
        config = _make_config()
        k8s = _mock_k8s()
        # Make namespace check fail
        k8s.namespace_exists.return_value = False
        engine = DeploymentEngine(config, k8s_client=k8s)
        # Override create_namespace to False so it fails
        engine.config.platform.kubernetes.create_namespace = False

        results = engine.deploy_all()
        # Should have stopped after namespace failure
        assert len(results) == 1
        assert results[0].status == DeploymentStatus.FAILED


# ---------------------------------------------------------------------------
# Individual Deployers (template rendering + dry run)
# ---------------------------------------------------------------------------


class TestContextStorageVars:
    """Tests for storage-related context variables."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_scratch_sc_refuses_when_absent(self, _mock_ocp):
        """Scratch SC verify FAILS with actionable hint when SC is missing."""
        from kubernetes.client.exceptions import ApiException

        config = _make_config()
        config.platform.storage.scratch.enabled = True
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        with patch(
            "kubernetes.client.StorageV1Api.read_storage_class",
            side_effect=ApiException(status=404, reason="Not Found"),
        ):
            result = engine._deploy_scratch_storageclass()

        assert result.status == DeploymentStatus.FAILED
        assert "admin install --component scratch-storage-class" in result.message


class TestDeployBuckets:
    """Tests for _deploy_buckets() -- S3 bucket creation during deploy."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.write_bucket_ownership_tag")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_buckets_created(self, mock_s3_cls, mock_verify, _mock_write, _mock_ocp):
        """Buckets are created via S3Client.ensure_buckets(). Ownership tag
        write is mocked -- see tests/test_ownership.py for its own tests."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        mock_client = MagicMock()
        mock_client._init_error = None
        mock_client.ensure_buckets.return_value = {
            "lakebench-bronze": True,
            "lakebench-silver": True,
            "lakebench-gold": True,
        }
        mock_s3_cls.return_value = mock_client
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="b",
            expected_deployment="test-deploy",
        )

        result = engine._deploy_buckets()
        assert result.status == DeploymentStatus.SUCCESS
        assert "created" in result.message
        mock_client.ensure_buckets.assert_called_once_with(
            ["lakebench-bronze", "lakebench-silver", "lakebench-gold"]
        )

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.s3.S3Client")
    def test_buckets_s3_init_failure(self, mock_s3_cls, _mock_ocp):
        """S3 client init failure returns FAILED result."""
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        mock_client = MagicMock()
        mock_client._init_error = "bad endpoint"
        mock_s3_cls.return_value = mock_client

        result = engine._deploy_buckets()
        assert result.status == DeploymentStatus.FAILED
        assert "init failed" in result.message


class TestAutoSizerIntegration:
    """Tests that autosizer runs during engine construction."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_autosizer_called_before_context(self, _mock_ocp):
        """Engine context should reflect auto-sized values for scale=1."""
        config = LakebenchConfig(
            name="test-auto",
            architecture={"workload": {"datagen": {"scale": 1}}},
            platform={
                "storage": {
                    "s3": {
                        "endpoint": "http://minio:9000",
                        "access_key": "minioadmin",
                        "secret_key": "minioadmin",
                    }
                }
            },
        )
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)

        # scale=1 → minimal tier: 1 Trino worker
        assert engine.context["trino_worker_replicas"] == 1


class TestPostgresDeployer:
    """Tests for PostgresDeployer."""


class TestRBACDeployer:
    """Tests for RBACDeployer."""


class TestTrinoDeployer:
    """Tests for TrinoDeployer."""


class TestHiveDeployer:
    """Tests for HiveDeployer."""


class TestPolarisDeployer:
    """Tests for PolarisDeployer."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_polaris_dry_run(self, _mock_ocp):
        from lakebench.deploy.polaris import PolarisDeployer

        config = _make_config(
            architecture={
                "catalog": {"type": "polaris"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            }
        )
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)
        deployer = PolarisDeployer(engine)

        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would" in result.message


# ---------------------------------------------------------------------------
# Iceberg SQL builders (v1.1.0)
# ---------------------------------------------------------------------------


class TestBuildCompactionSql:
    """Tests for build_compaction_sql() in deploy/iceberg.py."""


class TestBuildTableHealthSql:
    """Tests for build_table_health_sql() in deploy/iceberg.py."""


# ---------------------------------------------------------------------------
# Datagen cycle timestamp range (v1.1.0)
# ---------------------------------------------------------------------------


class TestDatagenCycleTimestampRange:
    """Tests for DatagenDeployer._cycle_timestamp_range()."""


# ---------------------------------------------------------------------------
# Integration: ownership hooks actually fire in deploy + destroy
#
# These guard against silent removal of the hook call sites in engine.py
# and destroy.py. Deleting them would leave the ownership module intact
# and its unit tests still passing, so we need call-site assertions.
# ---------------------------------------------------------------------------


class TestOwnershipHooksFire:
    """PR-1-R6: prove the hooks are wired into deploy and destroy paths."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.stamp_namespace")
    def test_deploy_namespace_calls_stamp(self, mock_stamp, _mock_ocp):
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

        engine._deploy_namespace()

        mock_stamp.assert_called_once()
        _args, kwargs = mock_stamp.call_args
        assert kwargs["deployment_name"] == "test-deploy"

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.write_deploy_nonce")
    @patch("lakebench.deploy.ownership.stamp_namespace")
    def test_every_deploy_stamps_a_fresh_nonce(self, mock_stamp, mock_nonce, _mock_ocp):
        """A destroy already running on this namespace compares the nonce; a
        redeploy into the same (still Active, or kept) namespace must change it."""
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
            result = engine._deploy_namespace()
        assert result.status != DeploymentStatus.FAILED
        mock_nonce.assert_called_once()

        mock_nonce.side_effect = RuntimeError("apiserver 503")
        with patch("kubernetes.client.CoreV1Api"):
            result = engine._deploy_namespace()
        assert result.status == DeploymentStatus.FAILED
        assert "nonce" in result.message

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
    @patch("lakebench.deploy.ownership.write_bucket_ownership_tag")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_deploy_buckets_calls_verify_and_write(
        self, mock_s3_cls, mock_verify, mock_write, _mock_ocp
    ):
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        mock_client = MagicMock()
        mock_client._init_error = None
        mock_client.raw_client = MagicMock()
        mock_client.ensure_buckets.return_value = {
            "lakebench-bronze": True,
            "lakebench-silver": True,
            "lakebench-gold": True,
        }
        mock_s3_cls.return_value = mock_client
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="b",
            expected_deployment="test-deploy",
        )

        engine._deploy_buckets()

        # Verify called on every bucket
        assert mock_verify.call_count == 3
        # Tag written on every bucket
        assert mock_write.call_count == 3
        # Tags include our deployment name
        for call in mock_write.call_args_list:
            assert call.args[2] == "test-deploy"  # (client, bucket, deployment_name)

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_deploy_buckets_refuses_legacy_bucket_without_force(
        self, mock_s3_cls, mock_verify, _mock_ocp
    ):
        """PR-1-R2: pre-existing untagged bucket refuses without --force-legacy."""
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

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.write_bucket_ownership_tag")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_deploy_buckets_freshly_created_untagged_is_ok(
        self, mock_s3_cls, mock_verify, mock_write, _mock_ocp
    ):
        """A bucket ensure_buckets JUST created is untagged; we tag it
        without needing --force-legacy. F4: assert the write actually
        fires so a future refactor cannot silently drop tagging."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        mock_client = MagicMock()
        mock_client._init_error = None
        mock_client.raw_client = MagicMock()
        mock_client.ensure_buckets.return_value = {
            "lakebench-bronze": True,  # was_created=True
            "lakebench-silver": True,
            "lakebench-gold": True,
        }
        mock_s3_cls.return_value = mock_client
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.ABSENT,  # freshly created is untagged
            resource_name="b",
            expected_deployment="test-deploy",
        )

        result = engine._deploy_buckets(force_legacy=False)
        assert result.status == DeploymentStatus.SUCCESS
        # F4: the tag write must fire on every bucket. If a future refactor
        # gates the write on force_legacy (or any other flag), this
        # assertion breaks the build. F-6: tolerate both positional and
        # keyword calling conventions.
        assert mock_write.call_count == 3
        deployment_names_written: set[str] = set()
        for call in mock_write.call_args_list:
            if len(call.args) >= 3:
                deployment_names_written.add(call.args[2])
            elif "deployment_name" in call.kwargs:
                deployment_names_written.add(call.kwargs["deployment_name"])
        assert deployment_names_written == {"test-deploy"}

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.stamp_namespace")
    def test_namespace_retry_after_transient_stamp_failure_does_not_brick(
        self, mock_stamp, _mock_ocp
    ):
        """F1: first attempt creates the namespace, stamp raises transient;
        deploy retries. On retry, `namespace_exists=True` (we just made it)
        but the engine remembers "we created it this run" so the retry
        stamps with force_legacy=True implicitly, not the dangerous
        --force-legacy prompt the user never asked for."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        # Simulate the first call: namespace does not exist. Second call:
        # after we apply_manifest it does exist.
        k8s.namespace_exists.side_effect = [False, True]
        engine = DeploymentEngine(config, k8s_client=k8s)

        # First stamp: raise a transient failure. Second stamp: MATCH.
        # We can't easily replay the engine's retry loop here without
        # driving deploy_all, so we validate the smaller property: given
        # we tracked "created this run", a subsequent _deploy_namespace
        # call sees pre_existing=True but must still force_legacy the
        # stamp because we own it.
        mock_stamp.return_value = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="test-deploy",
            expected_deployment="test-deploy",
        )

        # First call: creates the namespace, calls stamp, we return MATCH.
        result1 = engine._deploy_namespace(force_legacy=False)
        assert result1.status == DeploymentStatus.SUCCESS
        # Retain the tracking that we created it.
        assert "test-deploy" in engine._namespace_created_this_run

        # Simulate retry: namespace now exists. We must still stamp with
        # force_legacy=True implicitly because we own it.
        result2 = engine._deploy_namespace(force_legacy=False)
        assert result2.status == DeploymentStatus.SUCCESS
        # Second stamp call: force_legacy must be True.
        second_call = mock_stamp.call_args_list[-1]
        assert second_call.kwargs["force_legacy"] is True

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    @patch("lakebench.deploy.ownership.stamp_namespace")
    def test_apply_manifest_transient_error_retry_is_covered(self, mock_stamp, _mock_ocp):
        """F-2: apply_manifest can raise transient error AFTER the K8s
        API accepted the create. The retry sees namespace_exists=True
        but must know we own it. Seed of _namespace_created_this_run
        BEFORE apply is the guarantee. Without F-2 fix, retry would
        refuse without --force-legacy."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        # First namespace_exists check: not there. apply_manifest raises
        # a transient ConnectionError (K8s accepted, network reset).
        # Second attempt: namespace_exists returns True (K8s did create).
        k8s.namespace_exists.side_effect = [False, True]
        k8s.apply_manifest.side_effect = [ConnectionError("reset"), None]
        engine = DeploymentEngine(config, k8s_client=k8s)

        mock_stamp.return_value = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="test-deploy",
            expected_deployment="test-deploy",
        )

        # First attempt: apply raises, but the seed happened first.
        with pytest.raises(ConnectionError):
            engine._deploy_namespace(force_legacy=False)
        assert "test-deploy" in engine._namespace_created_this_run

        # Second attempt (simulates deploy_all's retry): namespace now
        # exists. Must still stamp with force_legacy=True implicitly.
        result = engine._deploy_namespace(force_legacy=False)
        assert result.status == DeploymentStatus.SUCCESS
        assert mock_stamp.call_args.kwargs["force_legacy"] is True

    @patch("kubernetes.client.BatchV1Api")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_clean_refuses_foreign_tagged_bucket(self, mock_s3_cls, mock_verify, _mock_batch):
        """F-1: `lakebench clean` must call verify_bucket_ownership.
        Foreign-tagged buckets refuse; --force alone is not enough.
        This closes the bypass hole where clean could be used to
        wipe another team's data by pointing config at their bucket."""
        from pathlib import Path
        from tempfile import NamedTemporaryFile

        from lakebench.cli._clean import clean
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        # Build a minimal valid config file on disk.
        cfg_yaml = (
            "name: my-clean\n"
            "platform:\n"
            "  storage:\n"
            "    s3:\n"
            "      endpoint: http://minio:9000\n"
            "      access_key: k\n"
            "      secret_key: s\n"
        )
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(cfg_yaml)
            cfg_path = f.name

        s3 = MagicMock()
        s3._init_error = None
        s3.raw_client = MagicMock()
        s3.empty_bucket.return_value = 0
        mock_s3_cls.return_value = s3
        # verify_bucket_ownership returns MISMATCH: the bucket belongs
        # to someone else.
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.MISMATCH,
            resource_name="lakebench-bronze",
            expected_deployment="my-clean",
            found_deployment="someone-else",
            hint="owned by someone-else",
        )

        # BatchV1Api mocked to raise on read_namespaced_job -- clean.py
        # swallows this and proceeds (no datagen check).
        _mock_batch.return_value.read_namespaced_job.side_effect = Exception("no k8s")

        # Run clean with --force (skip confirmation) but NOT
        # --force-legacy. Expect refusal on every bucket, no empty_bucket
        # calls, and a nonzero typer.Exit at the end.
        import typer

        with pytest.raises(typer.Exit) as exc:
            clean(
                target="silver",
                config_file=Path(cfg_path),
                file_option=None,
                force=True,
                force_legacy=False,
            )
        assert exc.value.exit_code == 3  # refused: the bucket belongs to another deployment

        # empty_bucket must never fire on any bucket.
        assert s3.empty_bucket.call_count == 0

    @pytest.mark.parametrize("target", ["silver", "gold"])
    @patch("lakebench.deploy.ownership.verify_namespace_identity")
    @patch("lakebench.deploy.ownership.build_identity_from_config")
    @patch("kubernetes.client.CoreV1Api")
    @patch("kubernetes.client.BatchV1Api")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_clean_individual_target_uses_same_gate(
        self,
        mock_s3_cls,
        mock_verify,
        _mock_batch,
        _mock_core,
        _mock_ident,
        _mock_ns_identity,
        target,
    ):
        """F-1 test gap: prove the ownership gate fires for individual
        bronze/silver/gold targets, not only the aggregate 'data' target.
        A regression in bucket_targets construction would slip through
        the data-only test."""
        from pathlib import Path
        from tempfile import NamedTemporaryFile

        from lakebench.cli._clean import clean
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        cfg_yaml = (
            "name: my-clean\n"
            "platform:\n"
            "  storage:\n"
            "    s3:\n"
            "      endpoint: http://minio:9000\n"
            "      access_key: k\n"
            "      secret_key: s\n"
        )
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(cfg_yaml)
            cfg_path = f.name

        s3 = MagicMock()
        s3._init_error = None
        s3.raw_client = MagicMock()
        s3.empty_bucket.return_value = 0
        mock_s3_cls.return_value = s3
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.MISMATCH,
            resource_name=f"lakebench-{target}",
            expected_deployment="my-clean",
            found_deployment="someone-else",
            hint="owned by someone-else",
        )
        _mock_batch.return_value.read_namespaced_job.side_effect = Exception("no k8s")

        import typer

        from lakebench.deploy.ownership import IdentityReport as _IR
        from lakebench.deploy.ownership import IdentityVerdict as _IV

        _mock_ns_identity.return_value = _IR(
            verdict=_IV.MATCH, resource_name="ns", expected_deployment="my-clean", hint=""
        )
        with pytest.raises(typer.Exit):
            clean(
                target=target,
                config_file=Path(cfg_path),
                file_option=None,
                force=True,
                force_legacy=False,
            )

        # verify_bucket_ownership was called with the specific bucket.
        assert mock_verify.called
        assert s3.empty_bucket.call_count == 0

    @patch("kubernetes.client.BatchV1Api")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_clean_absent_bucket_refuses_without_force_legacy(
        self, mock_s3_cls, mock_verify, _mock_batch
    ):
        """F-1 test gap: legacy (untagged) bucket refuses without
        --force-legacy on the clean path. Mirror of the deploy-side gate."""
        from pathlib import Path
        from tempfile import NamedTemporaryFile

        from lakebench.cli._clean import clean
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        cfg_yaml = (
            "name: my-clean\n"
            "platform:\n"
            "  storage:\n"
            "    s3:\n"
            "      endpoint: http://minio:9000\n"
            "      access_key: k\n"
            "      secret_key: s\n"
        )
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(cfg_yaml)
            cfg_path = f.name

        s3 = MagicMock()
        s3._init_error = None
        s3.raw_client = MagicMock()
        s3.empty_bucket.return_value = 0
        mock_s3_cls.return_value = s3
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.ABSENT,
            resource_name="lakebench-bronze",
            expected_deployment="my-clean",
            hint="no lakebench tag",
        )
        _mock_batch.return_value.read_namespaced_job.side_effect = Exception("no k8s")

        import typer

        # Without --force-legacy: refuse.
        with pytest.raises(typer.Exit):
            clean(
                target="silver",
                config_file=Path(cfg_path),
                file_option=None,
                force=True,
                force_legacy=False,
            )
        assert s3.empty_bucket.call_count == 0

    @patch(
        "lakebench.modules.table_formats.iceberg.maintenance.find_maintenance_engine",
        return_value=(None, None, None),
    )
    @patch("lakebench.deploy.ownership.verify_namespace_identity")
    @patch("lakebench.deploy.ownership.build_identity_from_config")
    @patch("kubernetes.client.BatchV1Api")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_clean_absent_bucket_proceeds_with_force_legacy(
        self, mock_s3_cls, mock_verify, _mock_batch, _mock_ident, mock_ns_identity, _mock_engine
    ):
        """--force-legacy on an untagged bucket proceeds. Confirms the
        opt-in escape hatch works so users can clean legacy state."""
        from lakebench.deploy.ownership import IdentityReport as _IR
        from lakebench.deploy.ownership import IdentityVerdict as _IV

        mock_ns_identity.return_value = _IR(
            verdict=_IV.MATCH, resource_name="ns", expected_deployment="my-clean", hint=""
        )
        from pathlib import Path
        from tempfile import NamedTemporaryFile

        from lakebench.cli._clean import clean
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        cfg_yaml = (
            "name: my-clean\n"
            "platform:\n"
            "  storage:\n"
            "    s3:\n"
            "      endpoint: http://minio:9000\n"
            "      access_key: k\n"
            "      secret_key: s\n"
        )
        with NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(cfg_yaml)
            cfg_path = f.name

        s3 = MagicMock()
        s3._init_error = None
        s3.raw_client = MagicMock()
        s3.empty_bucket.return_value = 5
        mock_s3_cls.return_value = s3
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.ABSENT,
            resource_name="lakebench-bronze",
            expected_deployment="my-clean",
            hint="no lakebench tag",
        )
        _mock_batch.return_value.read_namespaced_job.side_effect = Exception("no k8s")

        # With --force-legacy: empty_bucket fires.
        clean(
            target="silver",
            config_file=Path(cfg_path),
            file_option=None,
            force=True,
            force_legacy=True,
        )
        assert s3.empty_bucket.call_count == 1


class TestBucketCreationRecord:
    """LB-159: deploy records which buckets it created; destroy deletes only those."""

    def _deploy(self, ensure_result, prior_tags=None, mismatch=()):
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
                        IdentityVerdict.MISMATCH if name in mismatch else IdentityVerdict.MATCH
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

    def test_created_buckets_are_tagged_and_recorded(self):
        result, flags, record, config = self._deploy(
            {"lakebench-bronze": True, "lakebench-silver": False, "lakebench-gold": True}
        )
        assert result.status == DeploymentStatus.SUCCESS
        assert flags == {
            "lakebench-bronze": True,
            "lakebench-silver": False,
            "lakebench-gold": True,
        }
        record.assert_called_once()
        assert record.call_args.args[2] == ["lakebench-bronze", "lakebench-gold"]

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
        """Review finding: the record used to be written after the ownership
        loop, so a deploy failing on silver left bronze unrecorded and every
        later destroy kept it for good on FlashBlade."""
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
    bucket may be another cluster's not yet written (SAF-10 row 7)."""

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
    """Review: with create_buckets=false nothing was recorded, so clean and
    the continuous reset refused pre-provisioned FlashBlade buckets forever.
    SAF-10 row 7: only with --force-legacy (the operator's word that no
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
