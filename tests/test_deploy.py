"""Tests for deployment module."""

from unittest.mock import MagicMock, patch

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


def _make_config(**overrides) -> LakebenchConfig:
    """Create a LakebenchConfig with sensible defaults for testing."""
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
    return LakebenchConfig(**base)


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
        """Existing namespace should return success without creating."""
        config = _make_config()
        k8s = _mock_k8s()
        k8s.namespace_exists.return_value = True
        engine = DeploymentEngine(config, k8s_client=k8s)

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
        """Dry-run scratch SC deploy returns success without K8s calls."""
        config = _make_config()
        # Enable scratch
        config.platform.storage.scratch.enabled = True
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s, dry_run=True)

        result = engine._deploy_scratch_storageclass()
        assert result.status == DeploymentStatus.SUCCESS
        assert "Would create" in result.message

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_scratch_sc_skipped_when_disabled(self, _mock_ocp):
        """Scratch SC creation skipped when scratch is disabled."""
        config = _make_config()
        # scratch.enabled defaults to False
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        result = engine._deploy_scratch_storageclass()
        assert result.status == DeploymentStatus.SKIPPED

    @patch("lakebench.deploy.engine.DeploymentEngine._detect_openshift", return_value=False)
    def test_scratch_sc_skipped_when_create_false(self, _mock_ocp):
        """Scratch SC creation skipped when create_storage_class=False."""
        config = _make_config()
        config.platform.storage.scratch.enabled = True
        config.platform.storage.scratch.create_storage_class = False
        k8s = _mock_k8s()
        engine = DeploymentEngine(config, k8s_client=k8s)

        result = engine._deploy_scratch_storageclass()
        assert result.status == DeploymentStatus.SKIPPED


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
        assert ctx["polaris_version"] == "1.3.0-incubating"
        assert ctx["polaris_cpu"] == "1"
        assert ctx["polaris_memory"] == "2Gi"
