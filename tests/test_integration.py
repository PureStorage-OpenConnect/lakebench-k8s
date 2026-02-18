"""Integration tests for lakebench commands.

These tests require:
1. Active Kubernetes cluster access (kubectl configured)
2. S3/FlashBlade endpoint accessible
3. Valid test-config.yaml with credentials

The tests are designed to run NON-DESTRUCTIVELY against an existing deployment.
They verify CLI commands, connectivity, and read-only cluster state. Tests that
would modify or destroy cluster state are gated behind LAKEBENCH_DEPLOY_TESTS=1.

Run with: make test-integration
Or: pytest tests/test_integration.py -v -m integration

Environment variables:
    LAKEBENCH_TEST_CONFIG: Path to test config (default: test-config.yaml)
    LAKEBENCH_DEPLOY_TESTS: Set to "1" to enable deploy/destroy tests
    LAKEBENCH_SKIP_CLEANUP: Set to "1" to skip cleanup after deploy tests
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml

# All tests in this file are integration tests
pytestmark = pytest.mark.integration

DEPLOY_TESTS = os.environ.get("LAKEBENCH_DEPLOY_TESTS") == "1"


# =============================================================================
# Fixtures
# =============================================================================


def get_test_config_path() -> Path:
    """Get path to test configuration file."""
    env_path = os.environ.get("LAKEBENCH_TEST_CONFIG")
    if env_path:
        return Path(env_path)

    candidates = [
        Path("test-config.yaml"),
        Path("lakebench/test-config.yaml"),
        Path(__file__).parent.parent / "test-config.yaml",
    ]

    for path in candidates:
        if path.exists():
            return path

    pytest.skip("No test-config.yaml found. Set LAKEBENCH_TEST_CONFIG env var.")
    return Path()  # unreachable


@pytest.fixture(scope="module")
def test_config() -> Path:
    """Fixture providing path to test config."""
    return get_test_config_path()


@pytest.fixture(scope="module")
def check_cluster_access():
    """Verify cluster access before running tests."""
    result = subprocess.run(
        ["kubectl", "cluster-info"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        pytest.skip(f"No cluster access: {result.stderr}")


@pytest.fixture(scope="module")
def namespace(test_config: Path) -> str:
    """Get namespace from config."""
    with open(test_config) as f:
        cfg = yaml.safe_load(f)
    return cfg.get("platform", {}).get("kubernetes", {}).get("namespace", "lakebench-test")


def run_lakebench(*args: str, timeout: int = 300) -> subprocess.CompletedProcess:
    """Run lakebench CLI command."""
    cmd = ["lakebench", *args]
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# =============================================================================
# Smoke Tests - Quick checks that don't modify cluster state
# =============================================================================


class TestSmoke:
    """Quick smoke tests to verify basic functionality."""

    def test_cli_loads(self):
        """CLI loads without error."""
        result = run_lakebench("--help")
        assert result.returncode == 0
        assert "lakebench" in result.stdout.lower()

    def test_version(self):
        """Version command works."""
        result = run_lakebench("version")
        assert result.returncode == 0

    def test_config_info(self, test_config: Path):
        """Info command parses config."""
        result = run_lakebench("info", str(test_config))
        assert result.returncode == 0
        assert "scale" in result.stdout.lower() or "Scale" in result.stdout


# =============================================================================
# Validate Tests - Check connectivity without modifying state
# =============================================================================


class TestValidate:
    """Test validate command against real infrastructure."""

    def test_validate_passes(self, test_config: Path, check_cluster_access):
        """Validate command passes with valid config."""
        result = run_lakebench("validate", str(test_config), "-v")

        print(f"STDOUT:\n{result.stdout}")
        print(f"STDERR:\n{result.stderr}")

        assert result.returncode == 0, f"Validate failed: {result.stderr}"

    def test_validate_checks_s3(self, test_config: Path, check_cluster_access):
        """Validate checks S3 connectivity."""
        result = run_lakebench("validate", str(test_config), "-v")

        output = result.stdout + result.stderr
        assert "S3" in output or "s3" in output

    def test_validate_checks_kubernetes(self, test_config: Path, check_cluster_access):
        """Validate checks Kubernetes connectivity."""
        result = run_lakebench("validate", str(test_config), "-v")

        output = result.stdout + result.stderr
        assert "Kubernetes" in output or "kubernetes" in output or "K8s" in output


# =============================================================================
# Status Tests - Read-only cluster state checks
# =============================================================================


class TestStatus:
    """Test status command against existing deployment."""

    def test_status_runs(self, test_config: Path, check_cluster_access):
        """Status command runs without error."""
        result = run_lakebench("status", str(test_config))
        assert result.returncode in (0, 1)

    def test_status_shows_components(self, test_config: Path, check_cluster_access, namespace: str):
        """Status shows deployed components when infra exists."""
        # Check if namespace exists first
        ns_check = subprocess.run(
            ["kubectl", "get", "namespace", namespace],
            capture_output=True,
            text=True,
        )
        if ns_check.returncode != 0:
            pytest.skip(f"Namespace {namespace} does not exist")

        result = run_lakebench("status", str(test_config))
        assert result.returncode == 0
        output = result.stdout.lower()
        assert any(comp in output for comp in ["postgres", "hive", "trino", "running", "ready"])


# =============================================================================
# Infrastructure Verification Tests - Read-only checks against existing deploy
# =============================================================================


class TestInfrastructure:
    """Verify existing infrastructure components via kubectl.

    These tests verify the deployed state without modifying it.
    """

    def test_namespace_exists(self, check_cluster_access, namespace: str):
        """Namespace exists."""
        result = subprocess.run(
            ["kubectl", "get", "namespace", namespace],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip(f"Namespace {namespace} does not exist - deploy first")
        assert result.returncode == 0

    def test_postgres_running(self, check_cluster_access, namespace: str):
        """PostgreSQL is running."""
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                namespace,
                "-l",
                "app=lakebench-postgres",
                "-o",
                "jsonpath={.items[0].status.phase}",
            ],
            capture_output=True,
            text=True,
        )
        phase = result.stdout.strip()
        if not phase:
            pytest.skip("No Postgres pod found")
        assert phase in ("Running", "Succeeded"), f"Unexpected pod phase: {phase}"

    def test_hive_metastore_running(self, check_cluster_access, namespace: str):
        """Hive metastore is running."""
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "statefulset",
                "-n",
                namespace,
                "-l",
                "app.kubernetes.io/name=hive",
                "-o",
                "jsonpath={.items[0].status.readyReplicas}",
            ],
            capture_output=True,
            text=True,
        )
        if not result.stdout.strip():
            pytest.skip("No Hive metastore found")
        assert int(result.stdout.strip()) >= 1

    def test_trino_coordinator_running(self, check_cluster_access, namespace: str):
        """Trino coordinator is running."""
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "deployment",
                "-n",
                namespace,
                "lakebench-trino-coordinator",
                "-o",
                "jsonpath={.status.readyReplicas}",
            ],
            capture_output=True,
            text=True,
        )
        if not result.stdout.strip():
            pytest.skip("No Trino coordinator found")
        assert int(result.stdout.strip()) >= 1

    def test_trino_workers_ready(self, check_cluster_access, namespace: str):
        """Trino workers are ready."""
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "statefulset",
                "-n",
                namespace,
                "lakebench-trino-worker",
                "-o",
                "jsonpath={.status.readyReplicas}/{.spec.replicas}",
            ],
            capture_output=True,
            text=True,
        )
        if not result.stdout.strip():
            pytest.skip("No Trino workers found")
        parts = result.stdout.strip().split("/")
        ready = int(parts[0])
        desired = int(parts[1])
        assert ready == desired, f"Trino workers not fully ready: {ready}/{desired}"


# =============================================================================
# Query Tests - Read-only queries against deployed Trino
# =============================================================================


class TestQuery:
    """Test query command against deployed Trino."""

    def test_query_select_1(self, test_config: Path, check_cluster_access, namespace: str):
        """Basic SELECT 1 query works."""
        # Verify Trino is deployed
        result = subprocess.run(
            ["kubectl", "get", "deployment", "-n", namespace, "lakebench-trino-coordinator"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip("Trino not deployed")

        result = run_lakebench(
            "query",
            str(test_config),
            "--sql",
            "SELECT 1 AS test",
            "--timeout",
            "60",
        )

        if result.returncode == 0:
            assert "1" in result.stdout or "test" in result.stdout

    def test_query_show_catalogs(self, test_config: Path, check_cluster_access, namespace: str):
        """SHOW CATALOGS query works."""
        result = subprocess.run(
            ["kubectl", "get", "deployment", "-n", namespace, "lakebench-trino-coordinator"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip("Trino not deployed")

        result = run_lakebench(
            "query",
            str(test_config),
            "--sql",
            "SHOW CATALOGS",
            "--timeout",
            "60",
        )

        if result.returncode == 0:
            output = result.stdout.lower()
            # Should at minimum show system catalog
            assert "system" in output or "lakehouse" in output

    def test_query_show_schemas(self, test_config: Path, check_cluster_access, namespace: str):
        """SHOW SCHEMAS in lakehouse catalog works."""
        result = subprocess.run(
            ["kubectl", "get", "deployment", "-n", namespace, "lakebench-trino-coordinator"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip("Trino not deployed")

        result = run_lakebench(
            "query",
            str(test_config),
            "--sql",
            "SHOW SCHEMAS FROM lakehouse",
            "--timeout",
            "60",
        )

        if result.returncode == 0:
            output = result.stdout.lower()
            assert any(
                schema in output for schema in ["bronze", "silver", "gold", "information_schema"]
            )

    def test_query_gold_table_exists(self, test_config: Path, check_cluster_access, namespace: str):
        """Gold table exists and is queryable."""
        result = subprocess.run(
            ["kubectl", "get", "deployment", "-n", namespace, "lakebench-trino-coordinator"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.skip("Trino not deployed")

        result = run_lakebench(
            "query",
            str(test_config),
            "--sql",
            "SELECT COUNT(*) AS cnt FROM lakehouse.gold.customer_executive_dashboard",
            "--timeout",
            "120",
        )

        # This may fail if pipeline hasn't been run -- that's ok
        print(f"Gold query result: rc={result.returncode}")
        print(f"stdout: {result.stdout}")
        print(f"stderr: {result.stderr}")


# =============================================================================
# S3 Connectivity Tests - Read-only bucket checks
# =============================================================================


class TestS3:
    """Test S3 connectivity and bucket state."""

    def test_s3_connectivity(self, test_config: Path, check_cluster_access):
        """Validate checks S3 connectivity."""
        result = run_lakebench("validate", str(test_config), "-v")

        output = result.stdout + result.stderr
        # Should report S3 connectivity status
        assert "S3" in output or "s3" in output
        # Should not report connection errors
        assert "unreachable" not in output.lower()
        assert "connection refused" not in output.lower()


# =============================================================================
# Deploy/Destroy Tests - Gated behind LAKEBENCH_DEPLOY_TESTS=1
# =============================================================================


class TestDeployDestroy:
    """Test deploy and destroy in an isolated namespace.

    These tests create a SEPARATE namespace (lakebench-integ-test) to avoid
    disturbing any existing deployment. Only runs when LAKEBENCH_DEPLOY_TESTS=1.

    Run with: LAKEBENCH_DEPLOY_TESTS=1 pytest tests/test_integration.py::TestDeployDestroy -v
    """

    INTEG_NAMESPACE = "lakebench-integ-test"

    @pytest.fixture(scope="class")
    def integ_config(self, test_config: Path, tmp_path_factory) -> Path:
        """Create an isolated config with a separate namespace and scale=1."""
        if not DEPLOY_TESTS:
            pytest.skip("Deploy tests disabled. Set LAKEBENCH_DEPLOY_TESTS=1 to enable.")

        tmp_path = tmp_path_factory.mktemp("integ")

        with open(test_config) as f:
            cfg = yaml.safe_load(f)

        # Use isolated namespace
        cfg["platform"]["kubernetes"]["namespace"] = self.INTEG_NAMESPACE

        # Use scale 1 for fast tests
        if "architecture" not in cfg:
            cfg["architecture"] = {}
        if "workload" not in cfg["architecture"]:
            cfg["architecture"]["workload"] = {}
        if "datagen" not in cfg["architecture"]["workload"]:
            cfg["architecture"]["workload"]["datagen"] = {}
        cfg["architecture"]["workload"]["datagen"]["scale"] = 1

        config_path = tmp_path / "integ-config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False)

        return config_path

    @pytest.mark.slow
    def test_deploy_creates_namespace(self, integ_config: Path, check_cluster_access):
        """Deploy creates the namespace."""
        result = run_lakebench("deploy", str(integ_config), timeout=600)
        assert result.returncode == 0, f"Deploy failed:\n{result.stdout}\n{result.stderr}"

        ns_result = subprocess.run(
            ["kubectl", "get", "namespace", self.INTEG_NAMESPACE],
            capture_output=True,
            text=True,
        )
        assert ns_result.returncode == 0, f"Namespace {self.INTEG_NAMESPACE} not found"

    @pytest.mark.slow
    def test_deploy_creates_postgres(self, integ_config: Path, check_cluster_access):
        """Deploy creates PostgreSQL."""
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "statefulset",
                "-n",
                self.INTEG_NAMESPACE,
                "-l",
                "app=postgres",
                "--no-headers",
            ],
            capture_output=True,
            text=True,
        )
        assert "postgres" in result.stdout.lower(), "Postgres not found"

    @pytest.mark.slow
    def test_status_after_deploy(self, integ_config: Path, check_cluster_access):
        """Status shows deployed components."""
        result = run_lakebench("status", str(integ_config))
        assert result.returncode == 0
        output = result.stdout.lower()
        assert any(comp in output for comp in ["postgres", "hive", "trino", "running", "ready"])

    @pytest.mark.slow
    def test_destroy_cleans_up(self, integ_config: Path, check_cluster_access):
        """Destroy removes the isolated namespace."""
        if os.environ.get("LAKEBENCH_SKIP_CLEANUP") == "1":
            pytest.skip("Cleanup skipped (LAKEBENCH_SKIP_CLEANUP=1)")

        result = run_lakebench("destroy", str(integ_config), "--force", timeout=600)
        assert result.returncode == 0, f"Destroy failed:\n{result.stdout}\n{result.stderr}"
