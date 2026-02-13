"""Functional tests that render all Jinja2 templates and validate the YAML output.

These tests exercise the full rendering pipeline:
    TemplateRenderer  +  _build_context()  -->  rendered YAML  -->  yaml.safe_load()

They catch:
- Missing template variables (Jinja2 UndefinedError)
- Broken YAML syntax in rendered output
- Incorrect config-to-template value plumbing
- Conditional block regressions (OpenShift SCC, observability)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import yaml

from lakebench.config.recipes import RECIPES
from lakebench.deploy.engine import DeploymentEngine, TemplateRenderer
from tests.conftest import make_config

# ---------------------------------------------------------------------------
# Template inventory (mirrors deployer TEMPLATES constants)
# ---------------------------------------------------------------------------

DEPLOYER_TEMPLATES: dict[str, list[str]] = {
    "postgres": [
        "postgres/serviceaccount.yaml.j2",
        "postgres/statefulset.yaml.j2",
        "postgres/service.yaml.j2",
    ],
    "trino": [
        "trino/configmap.yaml.j2",
        "trino/service.yaml.j2",
        "trino/coordinator.yaml.j2",
        "trino/worker.yaml.j2",
    ],
    "spark-thrift": [
        "spark-thrift/service.yaml.j2",
        "spark-thrift/sparkapplication.yaml.j2",
    ],
    "duckdb": [
        "duckdb/deployment.yaml.j2",
        "duckdb/service.yaml.j2",
    ],
    "rbac": [
        "rbac/serviceaccount.yaml.j2",
        "rbac/role.yaml.j2",
        "rbac/rolebinding.yaml.j2",
    ],
    "prometheus": [
        "prometheus/rbac.yaml.j2",
        "prometheus/configmap.yaml.j2",
        "prometheus/service.yaml.j2",
        "prometheus/statefulset.yaml.j2",
    ],
    "grafana": [
        "grafana/configmap-datasources.yaml.j2",
        "grafana/configmap-dashboard-provider.yaml.j2",
        "grafana/configmap-dashboards.yaml.j2",
        "grafana/service.yaml.j2",
        "grafana/deployment.yaml.j2",
    ],
    "polaris": [
        "polaris/deployment.yaml.j2",
        "polaris/service.yaml.j2",
        "polaris/configmap.yaml.j2",
        "polaris/bootstrap-job.yaml.j2",
    ],
    "datagen": ["datagen/job.yaml.j2"],
    "storageclass": ["storageclass/px-csi-scratch.yaml.j2"],
    "namespace": ["namespace.yaml.j2"],
    "secrets": ["secrets.yaml.j2"],
}

ALL_TEMPLATES: list[str] = [tpl for templates in DEPLOYER_TEMPLATES.values() for tpl in templates]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_k8s() -> MagicMock:
    """Create a mock K8sClient for engine construction."""
    k8s = MagicMock()
    k8s.namespace_exists.return_value = True
    k8s.apply_manifest.return_value = True
    k8s.get_cluster_capacity.return_value = None
    return k8s


def _make_engine(cfg=None, **config_overrides) -> DeploymentEngine:
    """Build a DeploymentEngine with mocked K8s + OpenShift detection."""
    if cfg is None:
        cfg = make_config(**config_overrides)
    with patch(
        "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
        return_value=False,
    ):
        return DeploymentEngine(config=cfg, k8s_client=_mock_k8s(), dry_run=True)


def _enrich_context(engine: DeploymentEngine) -> dict:
    """Build the full template context, including deployer-specific variables.

    The base _build_context() omits variables that individual deployers inject
    (e.g. grafana_image, prometheus_image). This helper adds those so every
    template can be rendered without encountering undefined variables.
    """
    ctx = dict(engine.context)
    cfg = engine.config

    # Grafana deployer injects these (see grafana.py line 107)
    ctx.setdefault("grafana_image", cfg.images.grafana)
    # Prometheus deployer injects these (see prometheus.py lines 127-130)
    ctx.setdefault("prometheus_image", cfg.images.prometheus)
    ctx.setdefault("prometheus_retention", cfg.observability.retention)
    ctx.setdefault("prometheus_storage", cfg.observability.storage)
    ctx.setdefault("prometheus_storage_class", cfg.observability.storage_class or "")
    # Both grafana and prometheus templates use ``pull_policy`` (not image_pull_policy)
    ctx.setdefault("pull_policy", cfg.images.pull_policy.value)

    return ctx


def _parse_yaml_docs(rendered: str) -> list[dict]:
    """Parse rendered YAML using safe_load_all (handles both single and multi-doc).

    Returns a list of non-None parsed documents.
    """
    return [doc for doc in yaml.safe_load_all(rendered) if doc is not None]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def renderer() -> TemplateRenderer:
    """A TemplateRenderer pointed at the package templates directory."""
    return TemplateRenderer()


@pytest.fixture
def default_engine() -> DeploymentEngine:
    """DeploymentEngine with default (hive-iceberg-trino) config."""
    return _make_engine()


@pytest.fixture
def default_context(default_engine: DeploymentEngine) -> dict:
    """Full template context from the default engine, enriched with deployer vars."""
    return _enrich_context(default_engine)


# ===========================================================================
# 1. TestTemplateRendering - Core rendering tests
# ===========================================================================


class TestTemplateRendering:
    """Verify every template renders without error and produces valid YAML."""

    @pytest.mark.parametrize("template_name", ALL_TEMPLATES)
    def test_all_templates_render_without_error(
        self,
        renderer: TemplateRenderer,
        default_context: dict,
        template_name: str,
    ):
        """Render every template with a full context. No Jinja2 UndefinedError."""
        rendered = renderer.render(template_name, default_context)
        assert isinstance(rendered, str)
        assert len(rendered) > 0

    @pytest.mark.parametrize("template_name", ALL_TEMPLATES)
    def test_all_templates_produce_valid_yaml(
        self,
        renderer: TemplateRenderer,
        default_context: dict,
        template_name: str,
    ):
        """Render every template and parse with yaml.safe_load_all. Must not raise."""
        rendered = renderer.render(template_name, default_context)
        docs = _parse_yaml_docs(rendered)
        assert len(docs) > 0, f"{template_name} produced no YAML documents"
        for doc in docs:
            assert doc is not None

    @pytest.mark.parametrize("template_name", ALL_TEMPLATES)
    def test_rendered_templates_have_expected_kind(
        self,
        renderer: TemplateRenderer,
        default_context: dict,
        template_name: str,
    ):
        """Every rendered K8s manifest should have a 'kind' field."""
        rendered = renderer.render(template_name, default_context)
        docs = _parse_yaml_docs(rendered)
        for doc in docs:
            assert "kind" in doc, (
                f"{template_name} produced a document without 'kind': {list(doc.keys())[:5]}"
            )


# ===========================================================================
# 2. TestTemplateVariableSubstitution - Config values flow through
# ===========================================================================


class TestTemplateVariableSubstitution:
    """Verify that config values are correctly substituted into rendered output."""

    def test_postgres_image_substituted(
        self,
        renderer: TemplateRenderer,
        default_context: dict,
    ):
        """Postgres statefulset should contain the configured postgres image."""
        rendered = renderer.render("postgres/statefulset.yaml.j2", default_context)
        assert "postgres:17" in rendered

    def test_namespace_substituted(
        self,
        renderer: TemplateRenderer,
        default_context: dict,
    ):
        """Namespace template should contain the config name as namespace."""
        rendered = renderer.render("namespace.yaml.j2", default_context)
        assert default_context["namespace"] in rendered
        assert default_context["name"] in rendered

    def test_s3_credentials_in_secrets(
        self,
        renderer: TemplateRenderer,
        default_context: dict,
    ):
        """Secrets template should contain the S3 access key."""
        rendered = renderer.render("secrets.yaml.j2", default_context)
        assert default_context["s3_access_key"] in rendered

    def test_trino_worker_replicas(
        self,
        renderer: TemplateRenderer,
        default_context: dict,
    ):
        """Trino worker template should contain the configured replica count."""
        rendered = renderer.render("trino/worker.yaml.j2", default_context)
        parsed = yaml.safe_load(rendered)
        expected = int(default_context["trino_worker_replicas"])
        assert parsed["spec"]["replicas"] == expected

    def test_duckdb_resources(self, renderer: TemplateRenderer):
        """DuckDB deployment should contain CPU/memory from config."""
        engine = _make_engine(recipe="hive-iceberg-duckdb")
        ctx = _enrich_context(engine)
        rendered = renderer.render("duckdb/deployment.yaml.j2", ctx)
        parsed = yaml.safe_load(rendered)
        container = parsed["spec"]["template"]["spec"]["containers"][0]
        assert container["resources"]["requests"]["cpu"] == str(ctx["duckdb_cores"])
        assert container["resources"]["requests"]["memory"] == ctx["duckdb_memory_k8s"]
        assert container["resources"]["limits"]["cpu"] == str(ctx["duckdb_cores"])
        assert container["resources"]["limits"]["memory"] == ctx["duckdb_memory_k8s"]


# ===========================================================================
# 3. TestTemplateConditionals - Conditional rendering
# ===========================================================================


class TestTemplateConditionals:
    """Verify conditional template blocks respond to config flags."""

    def test_openshift_mode_removes_security_context(self, renderer: TemplateRenderer):
        """With openshift_mode=True, securityContext/runAsUser should be absent."""
        cfg = make_config()
        with patch(
            "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
            return_value=True,
        ):
            engine = DeploymentEngine(config=cfg, k8s_client=_mock_k8s(), dry_run=True)
        ctx = _enrich_context(engine)
        assert ctx["openshift_mode"] is True

        rendered = renderer.render("postgres/statefulset.yaml.j2", ctx)
        assert "runAsUser" not in rendered

    def test_non_openshift_has_security_context(self, renderer: TemplateRenderer):
        """With openshift_mode=False, securityContext/runAsUser should be present."""
        engine = _make_engine()
        ctx = _enrich_context(engine)
        assert ctx["openshift_mode"] is False

        rendered = renderer.render("postgres/statefulset.yaml.j2", ctx)
        assert "runAsUser" in rendered

    def test_observability_enabled_in_spark_thrift(self, renderer: TemplateRenderer):
        """With observability_enabled=True, prometheus config should appear."""
        cfg = make_config(observability={"enabled": True})
        with patch(
            "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
            return_value=False,
        ):
            engine = DeploymentEngine(config=cfg, k8s_client=_mock_k8s(), dry_run=True)
        ctx = _enrich_context(engine)
        assert ctx["observability_enabled"] is True

        rendered = renderer.render("spark-thrift/sparkapplication.yaml.j2", ctx)
        assert "spark.ui.prometheus.enabled=true" in rendered
        assert "prometheusServlet" in rendered

    def test_observability_disabled_in_spark_thrift(self, renderer: TemplateRenderer):
        """With observability_enabled=False, prometheus config should be absent."""
        engine = _make_engine(observability={"enabled": False})
        ctx = _enrich_context(engine)
        assert ctx["observability_enabled"] is False

        rendered = renderer.render("spark-thrift/sparkapplication.yaml.j2", ctx)
        assert "prometheusServlet" not in rendered


# ===========================================================================
# 4. TestPerRecipeTemplateRendering - All recipes produce valid templates
# ===========================================================================


# Exclude the "default" alias to avoid duplicate testing (it maps to hive-iceberg-trino)
_RECIPE_NAMES: list[str] = [r for r in RECIPES if r != "default"]


class TestPerRecipeTemplateRendering:
    """Verify every recipe produces valid rendered YAML for all deployer templates."""

    @pytest.mark.parametrize("recipe_name", _RECIPE_NAMES)
    def test_all_recipes_render_all_templates(
        self,
        renderer: TemplateRenderer,
        recipe_name: str,
    ):
        """For each recipe, build context and render ALL templates, validating YAML."""
        cfg = make_config(recipe=recipe_name)
        with patch(
            "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
            return_value=False,
        ):
            engine = DeploymentEngine(config=cfg, k8s_client=_mock_k8s(), dry_run=True)
        ctx = _enrich_context(engine)

        for template_name in ALL_TEMPLATES:
            rendered = renderer.render(template_name, ctx)
            assert len(rendered) > 0, (
                f"Recipe '{recipe_name}': template '{template_name}' produced empty output"
            )

            docs = _parse_yaml_docs(rendered)
            assert len(docs) > 0, (
                f"Recipe '{recipe_name}': template '{template_name}' produced no YAML documents"
            )
            for doc in docs:
                assert doc is not None
                assert "kind" in doc, (
                    f"Recipe '{recipe_name}': template '{template_name}' "
                    f"produced a document without 'kind'"
                )
