"""Tests for deployment module."""

from unittest.mock import MagicMock, patch

import pytest

from lakebench.config import LakebenchConfig
from lakebench.deploy.engine import (
    DeploymentEngine,
    DeploymentResult,
    DeploymentStatus,
    TemplateRenderer,
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

    def test_render_uses_package_templates(self):
        """Renderer should locate the package templates directory."""
        renderer = TemplateRenderer()
        # The renderer should have been initialised without error and
        # the internal Jinja2 environment should be usable.
        assert renderer.env is not None

    def test_render_namespace_template(self):
        """Render the namespace template with basic context."""
        renderer = TemplateRenderer()
        result = renderer.render(
            "namespace.yaml.j2",
            {
                "name": "test",
                "namespace": "test-ns",
            },
        )
        assert "test-ns" in result
        assert "Namespace" in result

    def test_render_all_returns_list(self):
        """render_all should return a list of rendered strings."""
        renderer = TemplateRenderer()
        ctx = {
            "name": "test",
            "namespace": "test-ns",
            "s3_access_key": "key",
            "s3_secret_key": "secret",
            "s3_endpoint": "http://minio:9000",
            "s3_host": "minio",
            "s3_port": 9000,
            "s3_region": "us-east-1",
        }
        results = renderer.render_all(["namespace.yaml.j2"], ctx)
        assert isinstance(results, list)
        assert len(results) == 1


# ---------------------------------------------------------------------------
# DeploymentResult / DeploymentStatus
# ---------------------------------------------------------------------------


class TestDeploymentResult:
    """Tests for DeploymentResult dataclass."""

    def test_basic_result(self):
        result = DeploymentResult(
            component="postgres",
            status=DeploymentStatus.SUCCESS,
            message="Deployed",
        )
        assert result.component == "postgres"
        assert result.status == DeploymentStatus.SUCCESS
        assert result.elapsed_seconds == 0.0
        assert result.details == {}

    def test_result_with_details(self):
        result = DeploymentResult(
            component="rbac",
            status=DeploymentStatus.FAILED,
            message="Missing SCC",
            elapsed_seconds=1.5,
            details={"scc": "anyuid"},
        )
        assert result.details["scc"] == "anyuid"
        assert result.elapsed_seconds == 1.5

    def test_deployment_status_values(self):
        assert DeploymentStatus.PENDING.value == "pending"
        assert DeploymentStatus.IN_PROGRESS.value == "in_progress"
        assert DeploymentStatus.SUCCESS.value == "success"
        assert DeploymentStatus.FAILED.value == "failed"
        assert DeploymentStatus.SKIPPED.value == "skipped"


# ---------------------------------------------------------------------------
# DeploymentEngine
# ---------------------------------------------------------------------------


class TestDeploymentEngine:
    """Tests for DeploymentEngine initialisation and context building."""

    def test_engine_init_with_mock_k8s(self):
        """Engine should accept an injected K8sClient."""
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        assert engine.config is config
        assert engine.k8s is k8s
        assert engine.dry_run is False

    def test_engine_dry_run_flag(self):
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)
        assert engine.dry_run is True

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_context_contains_core_keys(self, _mock_ocp):
        """The template context should contain essential keys."""
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        ctx = engine.context
        assert ctx["name"] == "test-deploy"
        assert ctx["namespace"] == "test-deploy"
        assert ctx["s3_endpoint"] == "http://minio:9000"
        assert ctx["s3_access_key"] == "minioadmin"
        assert "spark_executor_instances" in ctx
        assert "trino_worker_replicas" in ctx

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_context_parses_s3_host_port(self, _mock_ocp):
        """S3 host and port should be extracted from the endpoint URL."""
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        assert engine.context["s3_host"] == "minio"
        assert engine.context["s3_port"] == 9000

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_deploy_namespace_dry_run(self, _mock_ocp):
        """Dry-run should return success without calling K8s."""
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)

        result = engine._deploy_namespace()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would create" in result.message
        k8s.apply_manifest.assert_not_called()

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_deploy_namespace_already_exists(self, _mock_ocp):
        """Existing namespace should return success without creating.
        The ownership stamp is mocked -- its own tests live in
        tests/test_ownership.py."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        k8s.namespace_exists.return_value = True
        engine = DeploymentEngine(config, k8s_client=k8s)

        with patch(
            "lakebench.deploy.ownership.stamp_namespace",
            return_value=IdentityReport(
                verdict=IdentityVerdict.MATCH,
                resource_name="test-deploy",
                expected_deployment="test-deploy",
                found_deployment="test-deploy",
            ),
        ):
            result = engine._deploy_namespace()
        assert result.status == DeploymentStatus.SUCCESS
        assert "already exists" in result.message

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

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_deploy_all_progress_callback(self, _mock_ocp):
        """Progress callback should be called during deployment."""
        config = _make_config()
        k8s = _mock_k8s()
        k8s.namespace_exists.return_value = True
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)

        callbacks = []
        engine.deploy_all(progress_callback=lambda c, s, m: callbacks.append((c, s, m)))
        # Should have received callbacks for each step
        assert len(callbacks) > 0
        # First callback should be namespace IN_PROGRESS
        assert callbacks[0][0] == "namespace"


# ---------------------------------------------------------------------------
# Individual Deployers (template rendering + dry run)
# ---------------------------------------------------------------------------


class TestContextStorageVars:
    """Tests for storage-related context variables."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_context_has_trino_storage_vars(self, _mock_ocp):
        """Context should include Trino worker storage variables."""
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        ctx = engine.context
        assert "trino_worker_spill_enabled" in ctx
        assert "trino_worker_spill_max" in ctx
        assert "trino_worker_storage" in ctx
        assert "trino_worker_storage_class" in ctx
        assert ctx["trino_worker_spill_enabled"] is True
        assert ctx["trino_worker_storage"] == "50Gi"

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_context_has_scratch_storage_class(self, _mock_ocp):
        """Context should include scratch_storage_class."""
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        assert "scratch_storage_class" in engine.context
        assert engine.context["scratch_storage_class"] == "px-csi-scratch"

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_scratch_sc_dry_run(self, _mock_ocp):
        """Dry-run scratch SC verify returns success without K8s calls."""
        config = _make_config()
        # Enable scratch
        config.platform.storage.scratch.enabled = True
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)

        result = engine._deploy_scratch_storageclass()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would verify" in result.message

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_scratch_sc_skipped_when_disabled(self, _mock_ocp):
        """Scratch SC verify skipped when scratch is disabled."""
        config = _make_config()
        # scratch.enabled defaults to False
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        result = engine._deploy_scratch_storageclass()
        assert result.status == DeploymentStatus.SKIPPED

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
        assert "admin install-scratch-storage-class" in result.message

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_scratch_sc_verified_when_present(self, _mock_ocp):
        """Scratch SC verify succeeds when SC exists."""
        config = _make_config()
        config.platform.storage.scratch.enabled = True
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        with patch("kubernetes.client.StorageV1Api.read_storage_class"):
            result = engine._deploy_scratch_storageclass()

        assert result.status == DeploymentStatus.SUCCESS
        assert "verified" in result.message


class TestDeployBuckets:
    """Tests for _deploy_buckets() -- S3 bucket creation during deploy."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_buckets_dry_run(self, _mock_ocp):
        """Dry-run returns success without calling S3."""
        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)

        result = engine._deploy_buckets()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would create" in result.message

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_buckets_skipped_when_disabled(self, _mock_ocp):
        """Bucket creation skipped when create_buckets=false."""
        config = _make_config()
        config.platform.storage.s3.create_buckets = False
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        result = engine._deploy_buckets()
        assert result.status == DeploymentStatus.SKIPPED
        assert "create_buckets=false" in result.message

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_buckets_skipped_when_no_endpoint(self, _mock_ocp):
        """Bucket creation skipped when no S3 endpoint configured."""
        config = _make_config()
        config.platform.storage.s3.endpoint = ""
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        result = engine._deploy_buckets()
        assert result.status == DeploymentStatus.SKIPPED
        assert "No S3 endpoint" in result.message

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
    @patch("lakebench.deploy.ownership.write_bucket_ownership_tag")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_buckets_already_exist(self, mock_s3_cls, mock_verify, _mock_write, _mock_ocp):
        """Already-existing buckets are reported correctly."""
        from lakebench.deploy.ownership import IdentityReport, IdentityVerdict

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        mock_client = MagicMock()
        mock_client._init_error = None
        mock_client.ensure_buckets.return_value = {
            "lakebench-bronze": False,
            "lakebench-silver": False,
            "lakebench-gold": False,
        }
        mock_s3_cls.return_value = mock_client
        mock_verify.return_value = IdentityReport(
            verdict=IdentityVerdict.MATCH,
            resource_name="b",
            expected_deployment="test-deploy",
        )

        result = engine._deploy_buckets()
        assert result.status == DeploymentStatus.SUCCESS
        assert "already existed" in result.message

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

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_deploy_all_includes_bucket_step(self, _mock_ocp):
        """deploy_all() should include the s3-buckets step."""
        config = _make_config()
        k8s = _mock_k8s()
        k8s.namespace_exists.return_value = True
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)

        results = engine.deploy_all()
        components = [r.component for r in results]
        assert "s3-buckets" in components
        # Buckets should come after secrets and before scratch-sc
        bucket_idx = components.index("s3-buckets")
        assert components[bucket_idx - 1] == "secrets"


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

        # scale=1 → minimal tier: 2 executors, 4g memory
        assert engine.context["spark_executor_instances"] == 2
        assert engine.context["spark_executor_memory"] == "4g"
        assert engine.context["trino_worker_replicas"] == 1


class TestPostgresDeployer:
    """Tests for PostgresDeployer."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_postgres_dry_run(self, _mock_ocp):
        from lakebench.deploy.postgres import PostgresDeployer

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)
        deployer = PostgresDeployer(engine)

        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would" in result.message

    def test_postgres_templates_defined(self):
        from lakebench.deploy.postgres import PostgresDeployer

        assert len(PostgresDeployer.TEMPLATES) >= 2
        assert any("statefulset" in t for t in PostgresDeployer.TEMPLATES)
        assert any("service" in t for t in PostgresDeployer.TEMPLATES)


class TestRBACDeployer:
    """Tests for RBACDeployer."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_rbac_dry_run(self, _mock_ocp):
        from lakebench.deploy.rbac import RBACDeployer

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)
        deployer = RBACDeployer(engine)

        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS

    def test_rbac_templates_defined(self):
        from lakebench.deploy.rbac import RBACDeployer

        assert len(RBACDeployer.TEMPLATES) == 3
        template_names = " ".join(RBACDeployer.TEMPLATES)
        assert "serviceaccount" in template_names
        assert "role.yaml" in template_names
        assert "rolebinding" in template_names


class TestTrinoDeployer:
    """Tests for TrinoDeployer."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_trino_dry_run(self, _mock_ocp):
        from lakebench.deploy.trino import TrinoDeployer

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)
        deployer = TrinoDeployer(engine)

        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS

    def test_trino_templates_defined(self):
        from lakebench.deploy.trino import TrinoDeployer

        assert len(TrinoDeployer.TEMPLATES) >= 3
        template_names = " ".join(TrinoDeployer.TEMPLATES)
        assert "coordinator" in template_names
        assert "worker" in template_names


class TestHiveDeployer:
    """Tests for HiveDeployer."""

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_hive_dry_run(self, _mock_ocp):
        from lakebench.deploy.hive import HiveDeployer

        config = _make_config()
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)
        deployer = HiveDeployer(engine)

        result = deployer.deploy()
        assert result.status == DeploymentStatus.SUCCESS

    def test_hive_has_template_sets(self):
        from lakebench.deploy.hive import HiveDeployer

        assert hasattr(HiveDeployer, "STACKABLE_TEMPLATES")
        assert hasattr(HiveDeployer, "LEGACY_TEMPLATES")
        assert len(HiveDeployer.STACKABLE_TEMPLATES) >= 2


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

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_polaris_skipped_when_hive(self, _mock_ocp):
        from lakebench.deploy.polaris import PolarisDeployer

        config = _make_config()  # Default catalog is hive
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)
        deployer = PolarisDeployer(engine)

        result = deployer.deploy()
        assert result.status == DeploymentStatus.SKIPPED

    def test_polaris_has_templates(self):
        from lakebench.deploy.polaris import PolarisDeployer

        assert len(PolarisDeployer.TEMPLATES) >= 2
        template_names = " ".join(PolarisDeployer.TEMPLATES)
        assert "deployment" in template_names
        assert "service" in template_names

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_polaris_get_rest_uri(self, _mock_ocp):
        from lakebench.deploy.polaris import PolarisDeployer

        config = _make_config(
            architecture={
                "catalog": {"type": "polaris"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            }
        )
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)
        deployer = PolarisDeployer(engine)

        uri = deployer.get_rest_uri()
        assert "lakebench-polaris" in uri
        assert "8181" in uri
        assert "/api/catalog" in uri

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_context_has_polaris_vars(self, _mock_ocp):
        """Engine context should include Polaris template variables."""
        config = _make_config(
            architecture={
                "catalog": {"type": "polaris"},
                "table_format": {"type": "iceberg"},
                "query_engine": {"type": "trino"},
            }
        )
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        ctx = engine.context
        assert ctx["catalog_type"] == "polaris"
        assert ctx["polaris_port"] == 8181
        assert ctx["polaris_version"] == "1.6.0"
        assert ctx["polaris_cpu"] == "1"
        assert ctx["polaris_memory"] == "2Gi"


# ---------------------------------------------------------------------------
# Iceberg SQL builders (v1.1.0)
# ---------------------------------------------------------------------------


class TestBuildCompactionSql:
    """Tests for build_compaction_sql() in deploy/iceberg.py."""

    def test_trino_compaction(self):
        from lakebench.deploy.iceberg import build_compaction_sql

        sqls = build_compaction_sql(
            "trino", "lakehouse", "lakehouse.silver.customer_interactions_enriched"
        )
        assert len(sqls) == 1
        assert "optimize" in sqls[0].lower()
        assert "128MB" in sqls[0]

    def test_trino_compaction_custom_threshold(self):
        from lakebench.deploy.iceberg import build_compaction_sql

        sqls = build_compaction_sql(
            "trino", "lakehouse", "lakehouse.silver.t", file_size_threshold="256MB"
        )
        assert "256MB" in sqls[0]

    def test_spark_thrift_compaction(self):
        from lakebench.deploy.iceberg import build_compaction_sql

        sqls = build_compaction_sql(
            "spark-thrift", "lakehouse", "lakehouse.silver.customer_interactions_enriched"
        )
        assert len(sqls) == 1
        assert "rewrite_data_files" in sqls[0]


class TestBuildTableHealthSql:
    """Tests for build_table_health_sql() in deploy/iceberg.py."""

    def test_trino_health_queries(self):
        from lakebench.deploy.iceberg import build_table_health_sql

        result = build_table_health_sql("trino", "lakehouse.silver.t")
        assert "data_file_count" in result
        assert "snapshot_count" in result
        assert "$files" in result["data_file_count"]
        assert "$snapshots" in result["snapshot_count"]

    def test_spark_thrift_health_queries(self):
        from lakebench.deploy.iceberg import build_table_health_sql

        result = build_table_health_sql("spark-thrift", "lakehouse.silver.t")
        assert "data_file_count" in result
        assert "snapshot_count" in result
        assert ".files" in result["data_file_count"]
        assert ".snapshots" in result["snapshot_count"]


# ---------------------------------------------------------------------------
# Datagen cycle timestamp range (v1.1.0)
# ---------------------------------------------------------------------------


class TestDatagenCycleTimestampRange:
    """Tests for DatagenDeployer._cycle_timestamp_range()."""

    def test_single_cycle(self):
        from lakebench.deploy.datagen import DatagenDeployer

        start, end = DatagenDeployer._cycle_timestamp_range(0, 1, "2024-01-01", "2024-12-31")
        assert start == "2024-01-01"
        assert end == "2024-12-31"

    def test_two_cycles_first(self):
        from lakebench.deploy.datagen import DatagenDeployer

        start, end = DatagenDeployer._cycle_timestamp_range(0, 2, "2024-01-01", "2024-12-31")
        assert start == "2024-01-01"
        # ~182 days per cycle
        assert end < "2024-12-31"

    def test_two_cycles_last_gets_remainder(self):
        from lakebench.deploy.datagen import DatagenDeployer

        start, end = DatagenDeployer._cycle_timestamp_range(1, 2, "2024-01-01", "2024-12-31")
        assert end == "2024-12-31"

    def test_cycles_non_overlapping(self):
        from lakebench.deploy.datagen import DatagenDeployer

        ranges = []
        for i in range(3):
            s, e = DatagenDeployer._cycle_timestamp_range(i, 3, "2024-01-01", "2024-12-31")
            ranges.append((s, e))
        # Each cycle starts at or after previous cycle ends
        for i in range(1, len(ranges)):
            assert ranges[i][0] >= ranges[i - 1][1]

    def test_default_timestamps(self):
        from lakebench.deploy.datagen import DatagenDeployer

        start, end = DatagenDeployer._cycle_timestamp_range(0, 1)
        assert start == "2024-01-01"
        assert end == "2025-12-31"


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
                target="data",
                config_file=Path(cfg_path),
                file_option=None,
                force=True,
                force_legacy=False,
                metrics_dir=Path("/tmp/nonexistent-metrics"),
            )
        assert exc.value.exit_code == 1

        # empty_bucket must never fire on any bucket.
        assert s3.empty_bucket.call_count == 0

    @pytest.mark.parametrize("target", ["bronze", "silver", "gold"])
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
                metrics_dir=Path("/tmp/nonexistent-metrics"),
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
                target="data",
                config_file=Path(cfg_path),
                file_option=None,
                force=True,
                force_legacy=False,
                metrics_dir=Path("/tmp/nonexistent-metrics"),
            )
        assert s3.empty_bucket.call_count == 0

    @patch("lakebench.deploy.ownership.verify_namespace_identity")
    @patch("lakebench.deploy.ownership.build_identity_from_config")
    @patch("kubernetes.client.BatchV1Api")
    @patch("lakebench.deploy.ownership.verify_bucket_ownership")
    @patch("lakebench.s3.S3Client")
    def test_clean_absent_bucket_proceeds_with_force_legacy(
        self, mock_s3_cls, mock_verify, _mock_batch, _mock_ident, mock_ns_identity
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
            target="data",
            config_file=Path(cfg_path),
            file_option=None,
            force=True,
            force_legacy=True,
            metrics_dir=Path("/tmp/nonexistent-metrics"),
        )
        assert s3.empty_bucket.call_count == 3


class TestBucketCreationRecord:
    """LB-159: deploy records which buckets it created; destroy deletes only those."""

    def _deploy(self, ensure_result, prior_tags=None):
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
                return_value=IdentityReport(
                    verdict=IdentityVerdict.MATCH,
                    resource_name="b",
                    expected_deployment=config.name,
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
