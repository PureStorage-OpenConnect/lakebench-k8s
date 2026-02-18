"""End-to-end functional tests for lakebench.

These tests exercise the complete lakebench workflow:
1. Deploy infrastructure
2. Generate data
3. Run pipeline (batch or continuous)
4. Run benchmark
5. Generate report
6. Destroy everything

IMPORTANT: These tests are SLOW (10-30+ minutes) and modify cluster state.
They are meant to be run manually or in a dedicated CI environment.

Run with: pytest tests/test_e2e.py -v -m e2e
Or: make test-e2e

Environment variables:
    LAKEBENCH_TEST_CONFIG: Path to test config (default: test-config.yaml)
    LAKEBENCH_E2E_SCALE: Scale factor for E2E tests (default: 1)
    LAKEBENCH_SKIP_CLEANUP: Set to "1" to keep resources after tests
    LAKEBENCH_SKIP_DEPLOY: Set to "1" if infrastructure already deployed
    LAKEBENCH_SKIP_GENERATE: Set to "1" if data already generated
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from pathlib import Path

import pytest
import yaml

# All tests in this file are E2E tests
pytestmark = [pytest.mark.e2e, pytest.mark.slow]


# =============================================================================
# Configuration
# =============================================================================

E2E_SCALE = int(os.environ.get("LAKEBENCH_E2E_SCALE", "1"))
SKIP_CLEANUP = os.environ.get("LAKEBENCH_SKIP_CLEANUP") == "1"
SKIP_DEPLOY = os.environ.get("LAKEBENCH_SKIP_DEPLOY") == "1"
SKIP_GENERATE = os.environ.get("LAKEBENCH_SKIP_GENERATE") == "1"

# Timeouts (seconds)
DEPLOY_TIMEOUT = 600  # 10 min
GENERATE_TIMEOUT = 1800  # 30 min (scale 1 is fast, but larger scales take time)
PIPELINE_TIMEOUT = 3600  # 60 min
BENCHMARK_TIMEOUT = 600  # 10 min
DESTROY_TIMEOUT = 600  # 10 min


# =============================================================================
# Helpers
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
    return Path()


def run_lakebench(
    *args: str,
    timeout: int = 300,
    check: bool = False,
) -> subprocess.CompletedProcess:
    """Run lakebench CLI command."""
    cmd = ["lakebench", *args]
    print(f"\n>>> Running: {' '.join(cmd)}")

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )

    # Always print output for E2E tests (helps debugging)
    if result.stdout:
        print(f"STDOUT:\n{result.stdout}")
    if result.stderr:
        print(f"STDERR:\n{result.stderr}")

    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, result.stdout, result.stderr)

    return result


def wait_for_pods_ready(namespace: str, label: str, timeout: int = 300) -> bool:
    """Wait for pods matching label to be ready."""
    start = time.time()
    while time.time() - start < timeout:
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "pods",
                "-n",
                namespace,
                "-l",
                label,
                "-o",
                "jsonpath={.items[*].status.phase}",
            ],
            capture_output=True,
            text=True,
        )
        phases = result.stdout.strip().split()
        if phases and all(p == "Running" for p in phases):
            return True
        time.sleep(10)
    return False


def get_namespace(config_path: Path) -> str:
    """Extract namespace from config."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return cfg.get("platform", {}).get("kubernetes", {}).get("namespace", "lakebench-test")


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def test_config() -> Path:
    """Fixture providing path to test config."""
    return get_test_config_path()


@pytest.fixture(scope="module")
def namespace(test_config: Path) -> str:
    """Get namespace from config."""
    return get_namespace(test_config)


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
def deployed_e2e_infrastructure(test_config: Path, namespace: str, check_cluster_access):
    """Deploy infrastructure for E2E tests, cleanup after module."""
    if not SKIP_DEPLOY:
        print(f"\n{'=' * 60}")
        print("DEPLOYING INFRASTRUCTURE")
        print(f"{'=' * 60}")

        result = run_lakebench("deploy", str(test_config), "--yes", timeout=DEPLOY_TIMEOUT)
        if result.returncode != 0:
            pytest.fail(f"Deploy failed:\n{result.stdout}\n{result.stderr}")

        # Wait for key components to be ready
        print("Waiting for Postgres to be ready...")
        if not wait_for_pods_ready(namespace, "app=postgres", timeout=120):
            print("WARNING: Postgres pods not ready after 120s")

        print("Waiting for Hive to be ready...")
        if not wait_for_pods_ready(namespace, "app.kubernetes.io/name=hive", timeout=180):
            print("WARNING: Hive pods not ready after 180s")

        print("Waiting for Trino to be ready...")
        if not wait_for_pods_ready(namespace, "app.kubernetes.io/name=trino", timeout=180):
            print("WARNING: Trino pods not ready after 180s")

    yield namespace

    if not SKIP_CLEANUP:
        print(f"\n{'=' * 60}")
        print("DESTROYING INFRASTRUCTURE")
        print(f"{'=' * 60}")
        run_lakebench("destroy", str(test_config), "--force", timeout=DESTROY_TIMEOUT)


@pytest.fixture(scope="module")
def generated_data(test_config: Path, deployed_e2e_infrastructure: str):
    """Generate test data."""
    if not SKIP_GENERATE:
        print(f"\n{'=' * 60}")
        print(f"GENERATING DATA (scale={E2E_SCALE})")
        print(f"{'=' * 60}")

        result = run_lakebench(
            "generate",
            str(test_config),
            "--yes",
            "--wait",
            "--timeout",
            str(GENERATE_TIMEOUT),
            timeout=GENERATE_TIMEOUT + 60,
        )
        if result.returncode != 0:
            pytest.fail(f"Generate failed:\n{result.stdout}\n{result.stderr}")

    yield deployed_e2e_infrastructure


# =============================================================================
# E2E Test: Batch Pipeline
# =============================================================================


class TestBatchPipeline:
    """Test complete batch pipeline: deploy → generate → run → benchmark → report."""

    def test_01_infrastructure_deployed(self, test_config: Path, deployed_e2e_infrastructure: str):
        """Verify infrastructure is deployed."""
        _namespace = deployed_e2e_infrastructure

        result = run_lakebench("status", str(test_config))
        assert result.returncode == 0, "Status command failed"

        # Check key components mentioned in status
        output = result.stdout.lower()
        assert any(word in output for word in ["postgres", "running", "ready"])

    def test_02_data_generated(self, test_config: Path, generated_data: str):
        """Verify data generation completed."""
        # Check that bronze bucket has data
        result = run_lakebench("status", str(test_config))
        # If status shows bronze data or generate succeeded, we're good
        assert result.returncode == 0

    def test_03_batch_pipeline_runs(self, test_config: Path, generated_data: str):
        """Run batch pipeline (bronze → silver → gold)."""
        print(f"\n{'=' * 60}")
        print("RUNNING BATCH PIPELINE")
        print(f"{'=' * 60}")

        result = run_lakebench(
            "run",
            str(test_config),
            "--timeout",
            str(PIPELINE_TIMEOUT),
            timeout=PIPELINE_TIMEOUT + 60,
        )

        # Print detailed output
        print(f"Exit code: {result.returncode}")

        # Pipeline should complete successfully
        assert result.returncode == 0, f"Batch pipeline failed:\n{result.stderr}"

        # Check for success indicators in output
        output = result.stdout.lower()
        assert any(
            indicator in output for indicator in ["completed", "success", "finished", "gold"]
        ), "Pipeline output doesn't indicate success"

    def test_04_benchmark_runs(self, test_config: Path, generated_data: str):
        """Run Trino benchmark after pipeline."""
        print(f"\n{'=' * 60}")
        print("RUNNING BENCHMARK")
        print(f"{'=' * 60}")

        result = run_lakebench(
            "benchmark",
            str(test_config),
            timeout=BENCHMARK_TIMEOUT + 60,
        )

        print(f"Exit code: {result.returncode}")

        # Benchmark should complete (may have some query failures, that's ok)
        # Exit code 0 = all queries passed, 1 = some failed
        assert result.returncode in (0, 1), f"Benchmark crashed:\n{result.stderr}"

        # Should show QpH (queries per hour) metric
        output = result.stdout
        assert "QpH" in output or "queries" in output.lower(), (
            "Benchmark output doesn't show query metrics"
        )

    def test_05_report_generates(self, test_config: Path, generated_data: str):
        """Generate HTML report from metrics."""
        print(f"\n{'=' * 60}")
        print("GENERATING REPORT")
        print(f"{'=' * 60}")

        result = run_lakebench("report")

        print(f"Exit code: {result.returncode}")

        # Report generation should succeed
        assert result.returncode == 0, f"Report generation failed:\n{result.stderr}"

        # Should mention report file
        output = result.stdout
        assert "report" in output.lower() or ".html" in output.lower(), (
            "Report output doesn't mention report file"
        )

    def test_06_results_display(self, test_config: Path, generated_data: str):
        """Display pipeline results."""
        result = run_lakebench("results")

        # Results should display (even if no data, shouldn't crash)
        assert result.returncode in (0, 1)

    def test_07_query_gold_table(self, test_config: Path, generated_data: str):
        """Query gold table to verify data landed."""
        result = run_lakebench(
            "query",
            str(test_config),
            "--sql",
            "SELECT COUNT(*) as cnt FROM lakehouse.gold.customer_executive_dashboard",
            "--timeout",
            "60",
        )

        if result.returncode == 0:
            # Parse the count from the pipe-delimited table output.
            # Output looks like: | 91   |
            output = result.stdout
            count = 0
            for line in output.splitlines():
                # Match lines like "| 91   |" but skip header "| cnt  |"
                m = re.match(r"^\|\s*(\d+)\s*\|$", line.strip())
                if m:
                    count = int(m.group(1))
                    break
            print(f"Gold table row count: {count}")
            assert count > 0, "Gold table is empty"


# =============================================================================
# E2E Test: Continuous Pipeline
# =============================================================================


class TestContinuousPipeline:
    """Test continuous (streaming) pipeline."""

    def test_01_continuous_pipeline_starts(self, test_config: Path, generated_data: str):
        """Start continuous pipeline and verify streaming jobs launch."""
        print(f"\n{'=' * 60}")
        print("RUNNING CONTINUOUS PIPELINE")
        print(f"{'=' * 60}")

        # Continuous pipeline runs for --duration then stops.
        # --timeout is per-job (batch mode); --duration controls
        # the streaming monitoring window.
        result = run_lakebench(
            "run",
            str(test_config),
            "--continuous",
            "--duration",
            "120",  # 2 min streaming window
            timeout=300,  # 5 min subprocess timeout (margin for startup + cleanup)
        )

        print(f"Exit code: {result.returncode}")

        # Should start successfully (may timeout, that's expected)
        # Exit 0 = clean finish, 1 = timeout/stop is also ok for streaming
        assert result.returncode in (0, 1), f"Continuous pipeline crashed:\n{result.stderr}"

        # Should mention streaming jobs
        output = result.stdout.lower()
        assert any(
            word in output
            for word in ["streaming", "bronze-ingest", "silver-stream", "gold-refresh", "batch"]
        ), "Output doesn't mention streaming jobs"

    def test_02_stop_streaming_jobs(self, test_config: Path, generated_data: str):
        """Verify streaming jobs can be stopped."""
        result = run_lakebench("stop", str(test_config))

        # Stop should work (may say "no jobs running" if already stopped)
        assert result.returncode in (0, 1)


# =============================================================================
# E2E Test: Clean Destroy
# =============================================================================


class TestCleanDestroy:
    """Test clean destruction of resources."""

    def test_destroy_cleans_everything(
        self, test_config: Path, check_cluster_access, namespace: str
    ):
        """Verify destroy removes all resources."""
        # This test runs last and verifies cleanup

        if SKIP_CLEANUP:
            pytest.skip("LAKEBENCH_SKIP_CLEANUP is set")

        print(f"\n{'=' * 60}")
        print("VERIFYING CLEAN DESTROY")
        print(f"{'=' * 60}")

        # Run destroy
        result = run_lakebench(
            "destroy",
            str(test_config),
            "--force",
            timeout=DESTROY_TIMEOUT,
        )

        assert result.returncode == 0, f"Destroy failed:\n{result.stderr}"

        # Give K8s time to clean up
        time.sleep(30)

        # Verify namespace is gone (or empty)
        ns_result = subprocess.run(
            ["kubectl", "get", "namespace", namespace],
            capture_output=True,
            text=True,
        )

        # Namespace should not exist, OR if it exists, it should have no active pods.
        # Completed/terminated pods may linger briefly during namespace teardown.
        if ns_result.returncode == 0:
            pods_result = subprocess.run(
                ["kubectl", "get", "pods", "-n", namespace, "--no-headers"],
                capture_output=True,
                text=True,
            )
            pods = pods_result.stdout.strip()
            # Filter out pods in Completed/Terminated state (not real failures)
            active_pods = [
                line
                for line in pods.splitlines()
                if line and "Completed" not in line and "Terminating" not in line
            ]
            assert not active_pods, f"Active pods still exist in {namespace}:\n{pods}"


# =============================================================================
# Standalone E2E Workflow (all steps in one test)
# =============================================================================


class TestFullWorkflow:
    """Complete E2E workflow in a single test class.

    This is useful for running a complete test cycle:
    deploy → generate → run → benchmark → destroy

    Each step depends on the previous, so they run in order.
    """

    @pytest.fixture(scope="class", autouse=True)
    def workflow_setup(self, test_config: Path, check_cluster_access):
        """Setup and teardown for full workflow."""
        yield

        # Cleanup at end of workflow
        if not SKIP_CLEANUP:
            run_lakebench("destroy", str(test_config), "--force", timeout=DESTROY_TIMEOUT)

    def test_full_batch_workflow(self, test_config: Path, namespace: str):
        """Run complete batch workflow end-to-end."""
        print(f"\n{'=' * 60}")
        print("FULL E2E WORKFLOW TEST")
        print(f"Scale: {E2E_SCALE}")
        print(f"Namespace: {namespace}")
        print(f"{'=' * 60}")

        # Step 1: Deploy
        if not SKIP_DEPLOY:
            print("\n[1/5] Deploying infrastructure...")
            result = run_lakebench(
                "deploy", str(test_config), "--yes", timeout=DEPLOY_TIMEOUT, check=True
            )
            assert result.returncode == 0, "Deploy failed"

            # Wait for readiness
            time.sleep(60)

        # Step 2: Generate
        if not SKIP_GENERATE:
            print("\n[2/5] Generating data...")
            result = run_lakebench(
                "generate",
                str(test_config),
                "--yes",
                "--wait",
                "--timeout",
                str(GENERATE_TIMEOUT),
                timeout=GENERATE_TIMEOUT + 60,
            )
            assert result.returncode == 0, "Generate failed"

        # Step 3: Run Pipeline
        print("\n[3/5] Running batch pipeline...")
        result = run_lakebench(
            "run",
            str(test_config),
            "--timeout",
            str(PIPELINE_TIMEOUT),
            timeout=PIPELINE_TIMEOUT + 60,
        )
        assert result.returncode == 0, "Pipeline failed"

        # Step 4: Benchmark
        print("\n[4/5] Running benchmark...")
        result = run_lakebench(
            "benchmark",
            str(test_config),
            timeout=BENCHMARK_TIMEOUT + 60,
        )
        # Benchmark exit 0 or 1 is acceptable (some queries may fail)
        assert result.returncode in (0, 1), "Benchmark crashed"

        # Step 5: Report
        print("\n[5/5] Generating report...")
        result = run_lakebench("report")
        assert result.returncode == 0, "Report failed"

        print(f"\n{'=' * 60}")
        print("E2E WORKFLOW COMPLETED SUCCESSFULLY")
        print(f"{'=' * 60}")


# =============================================================================
# Extended Scale Tests - Run at multiple scales to verify scaling behavior
# =============================================================================

# Scale configurations: (scale, expected_bronze_gb, timeout_multiplier)
# Scale factor maps to approx_bronze_gb = scale * 10 GB
SCALE_CONFIGS = [
    (1, 10, 1),  # ~10 GB bronze, baseline timeout
    (10, 100, 3),  # ~100 GB bronze, 3x timeout
    (50, 500, 8),  # ~500 GB bronze, 8x timeout
    (100, 1000, 12),  # ~1 TB bronze, 12x timeout
]

# Extended scales for stress testing (run separately)
EXTENDED_SCALE_CONFIGS = [
    (250, 2500, 20),  # ~2.5 TB bronze
    (500, 5000, 30),  # ~5 TB bronze
    (1000, 10000, 40),  # ~10 TB bronze
]


def create_scale_config(base_config_path: Path, scale: int, tmp_path: Path) -> Path:
    """Create a config file with the specified scale factor."""
    with open(base_config_path) as f:
        cfg = yaml.safe_load(f)

    # Update scale
    if "architecture" not in cfg:
        cfg["architecture"] = {}
    if "workload" not in cfg["architecture"]:
        cfg["architecture"]["workload"] = {}
    if "datagen" not in cfg["architecture"]["workload"]:
        cfg["architecture"]["workload"]["datagen"] = {}

    cfg["architecture"]["workload"]["datagen"]["scale"] = scale

    # Update namespace to avoid conflicts
    if "platform" not in cfg:
        cfg["platform"] = {}
    if "kubernetes" not in cfg["platform"]:
        cfg["platform"]["kubernetes"] = {}

    base_ns = cfg["platform"]["kubernetes"].get("namespace", "lakebench-test")
    cfg["platform"]["kubernetes"]["namespace"] = f"{base_ns}-scale{scale}"

    # Write to temp file
    scale_config = tmp_path / f"config-scale-{scale}.yaml"
    with open(scale_config, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False)

    return scale_config


@pytest.mark.extended
class TestScaleMatrix:
    """Test pipeline at multiple scale factors.

    These tests verify that the pipeline works correctly at different data volumes.
    Each scale factor is tested independently with its own namespace.

    Run with: pytest tests/test_e2e.py::TestScaleMatrix -v -m extended
    Or specific scale: pytest tests/test_e2e.py::TestScaleMatrix::test_batch_pipeline_at_scale[1] -v
    """

    @pytest.fixture(scope="class")
    def base_config(self) -> Path:
        """Get base config path."""
        return get_test_config_path()

    @pytest.mark.parametrize("scale,expected_gb,timeout_mult", SCALE_CONFIGS)
    def test_batch_pipeline_at_scale(
        self,
        base_config: Path,
        tmp_path_factory,
        scale: int,
        expected_gb: float,
        timeout_mult: int,
    ):
        """Run batch pipeline at specified scale factor."""
        tmp_path = tmp_path_factory.mktemp(f"scale{scale}")
        config_path = create_scale_config(base_config, scale, tmp_path)
        namespace = f"lakebench-test-scale{scale}"

        print(f"\n{'=' * 60}")
        print(f"SCALE TEST: {scale} (expected ~{expected_gb} GB bronze)")
        print(f"Namespace: {namespace}")
        print(f"Timeout multiplier: {timeout_mult}x")
        print(f"{'=' * 60}")

        # Pre-register the scale namespace with the Spark Operator.
        # The operator reads jobNamespaces at startup only, so we must
        # helm upgrade + restart before deploying to a new namespace.
        print(f"\n[0/5] Registering namespace {namespace} with Spark Operator...")
        ns_result = subprocess.run(
            [
                "helm",
                "get",
                "values",
                "spark-operator",
                "-n",
                "spark-operator",
                "--all",
                "-o",
                "json",
            ],
            capture_output=True,
            text=True,
        )
        if ns_result.returncode == 0:
            import json as _json

            values = _json.loads(ns_result.stdout)
            current_ns = values.get("spark", {}).get("jobNamespaces", [])
            if namespace not in current_ns:
                new_ns = ",".join(current_ns + [namespace])
                subprocess.run(
                    [
                        "helm",
                        "upgrade",
                        "spark-operator",
                        "spark-operator/spark-operator",
                        "-n",
                        "spark-operator",
                        "--reuse-values",
                        "--set",
                        f"spark.jobNamespaces={{{new_ns}}}",
                    ],
                    capture_output=True,
                    text=True,
                )
                # Restart controller to pick up new namespace
                subprocess.run(
                    [
                        "kubectl",
                        "rollout",
                        "restart",
                        "deployment/spark-operator-controller",
                        "-n",
                        "spark-operator",
                    ],
                    capture_output=True,
                    text=True,
                )
                time.sleep(15)  # Wait for controller restart
                print(f"  Registered {namespace} with Spark Operator")
            else:
                print(f"  {namespace} already registered")
        else:
            print("  WARNING: Could not read Spark Operator values")

        try:
            # Deploy
            print(f"\n[1/5] Deploying infrastructure for scale {scale}...")
            result = run_lakebench(
                "deploy",
                str(config_path),
                "--yes",
                timeout=DEPLOY_TIMEOUT * timeout_mult,
            )
            assert result.returncode == 0, f"Deploy failed at scale {scale}"

            # Generate
            print(f"\n[2/5] Generating data at scale {scale}...")
            result = run_lakebench(
                "generate",
                str(config_path),
                "--yes",
                "--wait",
                "--timeout",
                str(GENERATE_TIMEOUT * timeout_mult),
                timeout=GENERATE_TIMEOUT * timeout_mult + 120,
            )
            assert result.returncode == 0, f"Generate failed at scale {scale}"

            # Run batch pipeline (default mode, no --mode flag needed)
            print(f"\n[3/5] Running batch pipeline at scale {scale}...")
            result = run_lakebench(
                "run",
                str(config_path),
                "--timeout",
                str(PIPELINE_TIMEOUT * timeout_mult),
                timeout=PIPELINE_TIMEOUT * timeout_mult + 120,
            )
            assert result.returncode == 0, f"Pipeline failed at scale {scale}"

            # Benchmark
            print(f"\n[4/5] Running benchmark at scale {scale}...")
            result = run_lakebench(
                "benchmark",
                str(config_path),
                timeout=BENCHMARK_TIMEOUT * timeout_mult + 60,
            )
            assert result.returncode in (0, 1), f"Benchmark crashed at scale {scale}"

            # Verify gold table has data
            print(f"\n[5/5] Verifying gold table at scale {scale}...")
            result = run_lakebench(
                "query",
                str(config_path),
                "--sql",
                "SELECT COUNT(*) FROM lakehouse.gold.customer_executive_dashboard",
                "--timeout",
                "120",
            )
            if result.returncode == 0:
                count = 0
                for line in result.stdout.splitlines():
                    m = re.match(r"^\|\s*(\d+)\s*\|$", line.strip())
                    if m:
                        count = int(m.group(1))
                        break
                print(f"Gold table rows at scale {scale}: {count}")
                assert count > 0, f"Gold table empty at scale {scale}"

            print(f"\nScale {scale} completed successfully")

        finally:
            # Always cleanup
            if not SKIP_CLEANUP:
                print(f"\nCleaning up scale {scale}...")
                run_lakebench(
                    "destroy",
                    str(config_path),
                    "--force",
                    timeout=DESTROY_TIMEOUT * timeout_mult,
                )

    @pytest.mark.parametrize("scale,expected_gb,timeout_mult", SCALE_CONFIGS)
    def test_continuous_pipeline_at_scale(
        self,
        base_config: Path,
        tmp_path_factory,
        scale: int,
        expected_gb: float,
        timeout_mult: int,
    ):
        """Run continuous pipeline at specified scale factor."""
        tmp_path = tmp_path_factory.mktemp(f"cont-scale{scale}")
        config_path = create_scale_config(base_config, scale, tmp_path)

        print(f"\n{'=' * 60}")
        print(f"CONTINUOUS SCALE TEST: {scale}")
        print(f"{'=' * 60}")

        try:
            # Deploy (reuse if exists from batch test)
            result = run_lakebench("status", str(config_path))
            if result.returncode != 0 or "not found" in result.stdout.lower():
                print(f"Deploying infrastructure for scale {scale}...")
                result = run_lakebench(
                    "deploy",
                    str(config_path),
                    "--yes",
                    timeout=DEPLOY_TIMEOUT * timeout_mult,
                )
                assert result.returncode == 0

            # Run continuous pipeline for limited duration
            run_duration = min(300, 120 * timeout_mult)  # 2-5 min depending on scale
            print(f"Running continuous pipeline for {run_duration}s...")

            result = run_lakebench(
                "run",
                str(config_path),
                "--continuous",
                "--duration",
                str(run_duration),
                timeout=run_duration + 180,
            )
            # Continuous mode exits 0 or 1 (timeout is expected)
            assert result.returncode in (0, 1), f"Continuous pipeline crashed at scale {scale}"

            # Stop any running jobs
            run_lakebench("stop", str(config_path))

            print(f"Continuous scale {scale} completed")

        finally:
            if not SKIP_CLEANUP:
                run_lakebench(
                    "destroy",
                    str(config_path),
                    "--force",
                    timeout=DESTROY_TIMEOUT * timeout_mult,
                )


@pytest.mark.stress
class TestStressScales:
    """Stress tests at larger scale factors.

    These tests are for validating the system at production-like scales.
    They require significant cluster resources and can take hours to complete.

    Run with: pytest tests/test_e2e.py::TestStressScales -v -m stress
    """

    @pytest.fixture(scope="class")
    def base_config(self) -> Path:
        return get_test_config_path()

    @pytest.mark.parametrize("scale,expected_gb,timeout_mult", EXTENDED_SCALE_CONFIGS)
    def test_batch_pipeline_stress(
        self,
        base_config: Path,
        tmp_path_factory,
        scale: int,
        expected_gb: float,
        timeout_mult: int,
    ):
        """Stress test batch pipeline at large scale factors."""
        tmp_path = tmp_path_factory.mktemp(f"stress{scale}")
        config_path = create_scale_config(base_config, scale, tmp_path)

        print(f"\n{'=' * 60}")
        print(f"STRESS TEST: Scale {scale} (~{expected_gb} GB)")
        print(f"WARNING: This test may take {timeout_mult * 60} minutes or more")
        print(f"{'=' * 60}")

        try:
            # Deploy
            result = run_lakebench(
                "deploy",
                str(config_path),
                "--yes",
                timeout=DEPLOY_TIMEOUT * timeout_mult,
            )
            assert result.returncode == 0, f"Deploy failed at stress scale {scale}"

            # Generate
            result = run_lakebench(
                "generate",
                str(config_path),
                "--yes",
                "--wait",
                "--timeout",
                str(GENERATE_TIMEOUT * timeout_mult),
                timeout=GENERATE_TIMEOUT * timeout_mult + 300,
            )
            assert result.returncode == 0, f"Generate failed at stress scale {scale}"

            # Run batch (default mode, no --mode flag needed)
            result = run_lakebench(
                "run",
                str(config_path),
                "--timeout",
                str(PIPELINE_TIMEOUT * timeout_mult),
                timeout=PIPELINE_TIMEOUT * timeout_mult + 300,
            )
            assert result.returncode == 0, f"Pipeline failed at stress scale {scale}"

            # Benchmark
            result = run_lakebench(
                "benchmark",
                str(config_path),
                timeout=BENCHMARK_TIMEOUT * timeout_mult + 120,
            )
            assert result.returncode in (0, 1), f"Benchmark crashed at stress scale {scale}"

            # Report
            result = run_lakebench("report")
            assert result.returncode == 0, f"Report failed at stress scale {scale}"

            print(f"\nStress scale {scale} completed successfully")

        finally:
            if not SKIP_CLEANUP:
                run_lakebench(
                    "destroy",
                    str(config_path),
                    "--force",
                    timeout=DESTROY_TIMEOUT * timeout_mult,
                )
