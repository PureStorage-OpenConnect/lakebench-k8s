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

import subprocess
from unittest.mock import patch

import pytest
import yaml

from lakebench._resources import get_templates_dir
from lakebench.config.recipes import RECIPES
from lakebench.deploy.engine import DeploymentEngine, TemplateRenderer
from tests.conftest import make_config
from tests.fixtures.functional_templates_helpers import _enrich_context as _enrich_context
from tests.fixtures.functional_templates_helpers import _make_engine as _make_engine
from tests.fixtures.functional_templates_helpers import _mock_k8s as _mock_k8s

# Templates the deployers render with their own context (deps/) are not swept.
_TEMPLATES_DIR = get_templates_dir()
ALL_TEMPLATES: list[str] = sorted(
    str(p.relative_to(_TEMPLATES_DIR))
    for p in _TEMPLATES_DIR.rglob("*.j2")
    if p.relative_to(_TEMPLATES_DIR).parts[0] != "deps"
)

_CA_SECRET = "lakebench-ca-certificate"
_TRUSTSTORE_PATH = "/truststore/truststore.jks"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_yaml_docs(rendered: str) -> list[dict]:
    """Parse rendered YAML using safe_load_all (handles both single and multi-doc).

    Returns a list of non-None parsed documents.
    """
    return [doc for doc in yaml.safe_load_all(rendered) if doc is not None]


def _pod_spec(rendered: str) -> dict:
    """Pod spec of the Deployment or StatefulSet in a rendered template."""
    for doc in _parse_yaml_docs(rendered):
        if doc["kind"] in ("Deployment", "StatefulSet"):
            return doc["spec"]["template"]["spec"]
    raise AssertionError("no Deployment or StatefulSet rendered")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def renderer() -> TemplateRenderer:
    """A TemplateRenderer pointed at the package templates directory."""
    return TemplateRenderer()


@pytest.fixture
def default_engine() -> DeploymentEngine:
    """DeploymentEngine with default (hive-iceberg-spark-trino) config."""
    return _make_engine()


@pytest.fixture
def default_context(default_engine: DeploymentEngine) -> dict:
    """Full template context from the default engine, enriched with deployer vars."""
    return _enrich_context(default_engine)


@pytest.fixture
def https_ctx():
    """Build the template context for an HTTPS S3 endpoint with a CA cert."""

    def build(recipe: str = "hive-iceberg-spark-trino") -> dict:
        platform = {
            "storage": {
                "s3": {
                    "endpoint": "https://s3.example.com:443",
                    "access_key": "AK",
                    "secret_key": "SK",
                    "ca_cert": "/tmp/ca.pem",
                }
            }
        }
        cfg = make_config(recipe=recipe, platform=platform)
        with (
            patch(
                "lakebench.deploy.engine.DeploymentEngine._detect_openshift",
                return_value=False,
            ),
            patch(
                "lakebench.deploy.engine.DeploymentEngine._read_ca_cert_pem",
                return_value="-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----",
            ),
        ):
            engine = DeploymentEngine(config=cfg, k8s_client=_mock_k8s(), dry_run=True)
        return _enrich_context(engine)

    return build


# ===========================================================================
# Config values flow through
# ===========================================================================


class TestTemplateVariableSubstitution:
    """Verify that config values are correctly substituted into rendered output."""

    def test_s3_credentials_in_secrets(
        self,
        renderer: TemplateRenderer,
        default_context: dict,
    ):
        """Secrets template should contain the S3 access key."""
        rendered = renderer.render("secrets.yaml.j2", default_context)
        assert default_context["s3_access_key"] in rendered

    def test_duckdb_resources(self, renderer: TemplateRenderer):
        """DuckDB deployment should contain CPU/memory from config."""
        cfg = make_config(
            recipe="hive-iceberg-spark-duckdb",
            architecture={
                "query_engine": {"type": "duckdb", "duckdb": {"cores": 4, "memory": "8g"}}
            },
        )
        ctx = _enrich_context(_make_engine(cfg=cfg))
        assert ctx["duckdb_cores"] == 4
        rendered = renderer.render("duckdb/deployment.yaml.j2", ctx)
        parsed = yaml.safe_load(rendered)
        container = parsed["spec"]["template"]["spec"]["containers"][0]
        assert container["resources"]["requests"]["cpu"] == str(ctx["duckdb_cores"])
        assert container["resources"]["requests"]["memory"] == ctx["duckdb_memory_k8s"]
        assert container["resources"]["limits"]["cpu"] == str(ctx["duckdb_cores"])
        assert container["resources"]["limits"]["memory"] == ctx["duckdb_memory_k8s"]


# ===========================================================================
# Conditional rendering
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

    def test_trino_worker_pvc_when_storage_class_set(self, renderer: TemplateRenderer):
        """With an explicit storage_class, workers use PVC volumeClaimTemplates."""
        cfg = make_config(
            architecture={
                "query_engine": {
                    "trino": {
                        "worker": {"storage_class": "px-csi-scratch"},
                    },
                },
            },
        )
        engine = _make_engine(cfg=cfg)
        ctx = _enrich_context(engine)
        assert ctx["trino_worker_storage_class"] == "px-csi-scratch"

        rendered = renderer.render("trino/worker.yaml.j2", ctx)
        parsed = yaml.safe_load(rendered)

        # Should have volumeClaimTemplates
        vcts = parsed["spec"].get("volumeClaimTemplates", [])
        assert len(vcts) == 1, "Expected one volumeClaimTemplate"
        assert vcts[0]["metadata"]["name"] == "data"
        assert vcts[0]["spec"]["storageClassName"] == "px-csi-scratch"

        # Should NOT have emptyDir volume named 'data'
        volumes = parsed["spec"]["template"]["spec"].get("volumes", [])
        data_vols = [v for v in volumes if v.get("name") == "data"]
        assert len(data_vols) == 0, "Expected no emptyDir 'data' volume when using PVC"

    # -- HTTPS / TLS conditional tests --

    @pytest.mark.parametrize("https", [True, False])
    def test_spark_thrift_ssl_follows_the_endpoint_scheme(
        self, renderer: TemplateRenderer, https_ctx, https
    ):
        """spark-thrift ssl.enabled is true for an HTTPS endpoint, false for HTTP."""
        ctx = https_ctx() if https else _enrich_context(_make_engine())
        assert ctx["s3_use_ssl"] is https
        rendered = renderer.render("spark-thrift/sparkapplication.yaml.j2", ctx)
        want = "true" if https else "false"
        assert f"spark.hadoop.fs.s3a.connection.ssl.enabled={want}" in rendered

    @pytest.mark.parametrize(
        ("recipe", "template", "main_container"),
        [
            ("hive-iceberg-spark-trino", "trino/coordinator.yaml.j2", "trino"),
            ("hive-iceberg-spark-trino", "trino/worker.yaml.j2", "trino"),
            ("polaris-iceberg-spark-trino", "polaris/deployment.yaml.j2", "polaris"),
        ],
    )
    def test_ca_cert_truststore_wired_into_pod(
        self, renderer: TemplateRenderer, https_ctx, recipe, template, main_container
    ):
        """The truststore init container mounts the CA secret and the main container the store."""
        spec = _pod_spec(renderer.render(template, https_ctx(recipe)))
        volumes = {v["name"]: v for v in spec["volumes"]}
        init = {c["name"]: c for c in spec["initContainers"]}["import-ca-cert"]
        init_mounts = {m["name"]: m["mountPath"] for m in init["volumeMounts"]}
        assert set(init_mounts) <= set(volumes)

        secret_vols = [n for n in init_mounts if "secret" in volumes[n]]
        store_vols = [n for n in init_mounts if "emptyDir" in volumes[n]]
        assert len(secret_vols) == 1
        assert volumes[secret_vols[0]]["secret"]["secretName"] == _CA_SECRET
        assert len(store_vols) == 1

        main = {c["name"]: c for c in spec["containers"]}[main_container]
        main_mounts = {m["name"]: m["mountPath"] for m in main["volumeMounts"]}
        assert main_mounts[store_vols[0]] == init_mounts[store_vols[0]]
        # A failed import fails the pod here, not later as a TLS error.
        assert "|| true" not in init["command"][-1]

        # The JVM is pointed at the store the init container builds. Trino's
        # JVM config lives in its own ConfigMap (both roles); Polaris reads it
        # from JAVA_TOOL_OPTIONS on the deployment.
        trust = f"-Djavax.net.ssl.trustStore={_TRUSTSTORE_PATH}"
        if main_container == "trino":
            configmap = yaml.safe_load(renderer.render("trino/configmap.yaml.j2", https_ctx()))
            for role in ("coordinator", "worker"):
                assert trust in configmap["data"][f"jvm.config.{role}"].split()
        else:
            env = {e["name"]: e.get("value") for e in spec["containers"][0]["env"]}
            assert trust in env["JAVA_TOOL_OPTIONS"].split()

    def test_polaris_catalog_allows_per_zone_namespace_locations(self, renderer: TemplateRenderer):
        """The bootstrap creates the catalog with
        namespace-custom-location enabled, so the silver and gold namespace
        locations in their own buckets take effect. Without it Polaris refuses
        a location outside the catalog's default base and every zone roots in
        the bronze bucket."""
        ctx = _enrich_context(_make_engine(recipe="polaris-iceberg-spark-trino"))
        rendered = renderer.render("polaris/bootstrap-job.yaml.j2", ctx)
        assert '"polaris.config.namespace-custom-location.enabled": "true"' in rendered

    def test_polaris_bootstrap_accepts_existing_catalog_and_namespaces(
        self, renderer: TemplateRenderer, tmp_path
    ):
        """A rerun of the bootstrap sees HTTP 409 for the catalog and every
        namespace and succeeds. With ``curl -f`` an HTTP error exits 22 after
        printing the code, so a ``|| echo 000`` fallback read 409 as 409000
        and the namespace step failed the Job."""
        ctx = _enrich_context(_make_engine(recipe="polaris-iceberg-spark-trino"))
        job = yaml.safe_load(renderer.render("polaris/bootstrap-job.yaml.j2", ctx))
        script = job["spec"]["template"]["spec"]["containers"][0]["args"][0]
        assert "curl -sf -o /dev/null" not in script
        fake_curl = tmp_path / "curl"
        fake_curl.write_text(
            "#!/bin/sh\n"
            'case "$*" in\n'
            '  *oauth/tokens*) echo \'{"access_token":"t"}\' ;;\n'
            "  *grants*) printf 201 ;;\n"
            '  *-w*) printf 409; case "$1" in -sf) exit 22 ;; esac ;;\n'
            "esac\n"
        )
        fake_curl.chmod(0o755)
        env = {"PATH": f"{tmp_path}:/usr/bin:/bin", "CATALOG_NAME": "lakehouse"}
        result = subprocess.run(
            ["/bin/sh", "-c", script], env=env, capture_output=True, text=True, timeout=30
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "Catalog 'lakehouse' already exists (OK)" in result.stdout
        assert result.stdout.count("already exists (OK)") == 5

    @pytest.mark.parametrize("ca_cert", [True, False])
    def test_datagen_ca_cert_env_vars_follow_the_ca(
        self, renderer: TemplateRenderer, https_ctx, ca_cert
    ):
        """The datagen job carries S3_CA_CERT/SSL_CERT_FILE only when a CA cert
        is set; otherwise neither appears."""
        ctx = https_ctx() if ca_cert else _enrich_context(_make_engine())
        rendered = renderer.render("datagen/job.yaml.j2", ctx)
        if not ca_cert:
            assert "S3_CA_CERT" not in rendered
            assert "SSL_CERT_FILE" not in rendered
            return
        assert "S3_CA_CERT" in rendered
        assert _CA_SECRET in rendered
        env = {
            e["name"]: e.get("value")
            for d in _parse_yaml_docs(rendered)
            if d.get("kind") == "Job"
            for e in d["spec"]["template"]["spec"]["containers"][0]["env"]
        }
        # An image without S3_CA_CERT support trusts the CA through
        # rustls-native-certs, which reads SSL_CERT_FILE.
        assert env["SSL_CERT_FILE"] == env["S3_CA_CERT"] == "/etc/ssl/certs/custom-ca/ca.crt"

    @pytest.mark.parametrize("https", [True, False])
    def test_hive_tls_block_follows_the_endpoint(
        self, renderer: TemplateRenderer, https_ctx, https
    ):
        """The Hive cluster trusts the CA secret class on an HTTPS + CA
        endpoint, and carries no TLS block over HTTP."""
        ctx = https_ctx() if https else _enrich_context(_make_engine())
        hive = yaml.safe_load(renderer.render("hive/stackable-hivecluster.yaml.j2", ctx))
        inline = hive["spec"]["clusterConfig"]["s3"]["inline"]
        if not https:
            assert "tls" not in inline
            return
        assert inline["tls"]["verification"]["server"]["caCert"]["secretClass"] == (
            f"lakebench-s3-ca-cert-{ctx['namespace']}"
        )


# ===========================================================================
# Every recipe renders every template over HTTP; one recipe per catalog,
# table format and query engine also over HTTPS with a CA cert
# ===========================================================================


# Exclude the "default" alias to avoid duplicate testing (it maps to hive-iceberg-spark-trino)
_RECIPE_NAMES: list[str] = [r for r in RECIPES if r != "default"]
_HTTPS_RECIPES = (
    "polaris-iceberg-spark-trino",
    "hive-delta-spark-thrift",
    "hive-iceberg-spark-duckdb",
)


@pytest.mark.parametrize(
    ("recipe_name", "https"),
    [(r, False) for r in _RECIPE_NAMES] + [(r, True) for r in _HTTPS_RECIPES],
)
def test_recipe_renders_every_template(renderer: TemplateRenderer, https_ctx, recipe_name, https):
    """Each template renders to valid YAML documents, each with a kind."""
    ctx = https_ctx(recipe_name) if https else _enrich_context(_make_engine(recipe=recipe_name))
    for template_name in ALL_TEMPLATES:
        docs = _parse_yaml_docs(renderer.render(template_name, ctx))
        assert docs, f"{recipe_name} https={https}: {template_name} rendered no YAML documents"
        for doc in docs:
            assert "kind" in doc, (
                f"{recipe_name} https={https}: {template_name} has a document without 'kind'"
            )


# ===========================================================================
# Connector name depends on table format
# ===========================================================================


@pytest.mark.parametrize(
    ("recipe", "connector"),
    [
        ("hive-iceberg-spark-trino", "iceberg"),
        ("hive-delta-spark-trino", "delta_lake"),
    ],
)
def test_trino_lakehouse_connector_follows_table_format(
    renderer: TemplateRenderer, recipe: str, connector: str
):
    """The lakehouse catalog uses the connector for the recipe's table format."""
    ctx = _enrich_context(_make_engine(recipe=recipe))
    configmap = yaml.safe_load(renderer.render("trino/configmap.yaml.j2", ctx))
    props = dict(
        line.split("=", 1)
        for line in configmap["data"]["lakehouse.properties"].splitlines()
        if "=" in line
    )
    assert props["connector.name"] == connector
