"""Tests for the local (non-Kubernetes) deploy and Spark submission path.

Mocked, so they run in the default suite. The assertions about the Ivy cache
(LB-053) and the S3A region (LB-052) encode failures found by running against
podman and Garage, not by reading the code. Both are silent or opaque at
runtime, so they are worth pinning down here.
"""

from pathlib import Path
from unittest import mock

import pytest

from lakebench.deploy.garage import GarageCredentials
from lakebench.deploy.local import SKIPPED_COMPONENTS, LocalDeployer, LocalDeployment
from lakebench.modules.pipeline_engines.spark.local_job import (
    LocalSparkConfig,
    LocalSparkRunner,
)


def _creds(endpoint="http://localhost:3900"):
    return GarageCredentials(
        access_key="GK123",
        secret_key="secret",
        endpoint=endpoint,
        region="us-east-1",
    )


def _completed(returncode=0, stdout="", stderr=""):
    return mock.Mock(returncode=returncode, stdout=stdout, stderr=stderr)


class TestLocalDeployer:
    def test_deploy_creates_workdir_and_returns_credentials(self, tmp_path):
        workdir = tmp_path / "lb"
        deployer = LocalDeployer(workdir=workdir, cli="podman")

        with mock.patch("lakebench.deploy.local.GarageDeployer", autospec=True) as garage_cls:
            garage_cls.return_value.deploy.return_value = _creds()
            result = deployer.deploy()

        assert workdir.is_dir()
        assert result.endpoint == "http://localhost:3900"
        assert result.credentials.access_key == "GK123"
        assert result.runtime_cli == "podman"

    def test_garage_gets_the_configured_region(self, tmp_path):
        """LB-052: Garage validates the sigv4 scope, so region must propagate."""
        deployer = LocalDeployer(workdir=tmp_path, region="eu-west-1", cli="podman")

        with mock.patch("lakebench.deploy.local.GarageDeployer", autospec=True) as garage_cls:
            garage_cls.return_value.deploy.return_value = _creds()
            deployer.deploy()

        assert garage_cls.call_args.kwargs["region"] == "eu-west-1"

    def test_namespace_isolates_concurrent_runs(self, tmp_path):
        deployer = LocalDeployer(workdir=tmp_path, namespace="run-a", cli="podman")
        assert deployer.runtime.namespace == "run-a"

    def test_destroy_keeps_data_by_default(self, tmp_path):
        marker = tmp_path / "data.parquet"
        marker.write_text("x")
        deployer = LocalDeployer(workdir=tmp_path, cli="podman")
        deployer.runtime = mock.Mock(delete_all=mock.Mock(return_value=2))

        assert deployer.destroy() == 2
        assert marker.exists(), "destroy must not discard generated data by default"

    def test_destroy_removes_data_when_asked(self, tmp_path):
        workdir = tmp_path / "lb"
        workdir.mkdir()
        (workdir / "data.parquet").write_text("x")
        deployer = LocalDeployer(workdir=workdir, cli="podman")
        deployer.runtime = mock.Mock(delete_all=mock.Mock(return_value=1))

        deployer.destroy(remove_data=True)
        assert not workdir.exists()

    def test_status_lists_managed_components(self, tmp_path):
        deployer = LocalDeployer(workdir=tmp_path, cli="podman")
        deployer.runtime = mock.Mock(list_managed=mock.Mock(return_value=["garage"]))
        assert deployer.status() == ["garage"]


class TestSkippedComponents:
    def test_skipped_components_carry_reasons(self):
        for name, reason in SKIPPED_COMPONENTS:
            assert name and reason, f"{name} must explain why it is skipped"

    def test_catalog_services_are_skipped(self):
        """The hadoop catalog is what removes postgres, hive, and polaris."""
        skipped = dict(SKIPPED_COMPONENTS)
        for component in ("postgres", "hive", "polaris"):
            assert component in skipped
            assert "hadoop catalog" in skipped[component]

    def test_deployment_reports_skips(self, tmp_path):
        deployment = LocalDeployment(
            credentials=_creds(),
            runtime_cli="podman",
            workdir=tmp_path,
            buckets=("lb-bronze",),
        )
        assert dict(deployment.skipped)["spark-operator"]


class TestLocalSparkCommand:
    def _runner(self, tmp_path, **overrides):
        settings = {
            "endpoint": "http://localhost:3900",
            "access_key": "GK123",
            "secret_key": "secret",
        }
        settings.update(overrides)
        return LocalSparkRunner(LocalSparkConfig(**settings), workdir=tmp_path, cli="podman")

    def test_unknown_job_type_lists_valid_options(self, tmp_path):
        with pytest.raises(ValueError) as exc:
            self._runner(tmp_path).build_command("bronze-verifyy")
        assert "bronze-verify" in str(exc.value)

    @pytest.mark.parametrize(
        "job_type,script",
        [
            ("bronze-verify", "bronze_verify.py"),
            ("silver-build", "silver_build.py"),
            ("gold-finalize", "gold_finalize.py"),
        ],
    )
    def test_each_job_maps_to_its_script(self, tmp_path, job_type, script):
        cmd = self._runner(tmp_path).build_command(job_type)
        assert cmd[-1] == f"/scripts/{script}"

    def test_ivy_cache_is_writable(self, tmp_path):
        """LB-053: the Spark image user has no home, so Ivy cannot write."""
        cmd = self._runner(tmp_path).build_command("bronze-verify")
        joined = " ".join(cmd)
        assert "spark.jars.ivy=/work/ivy" in joined
        assert "HOME=/work" in joined

    def test_s3a_region_is_set(self, tmp_path):
        """LB-052: without this, S3A signs with a default region and Garage 400s."""
        cmd = self._runner(tmp_path, region="eu-west-1").build_command("silver-build")
        assert "spark.hadoop.fs.s3a.endpoint.region=eu-west-1" in " ".join(cmd)

    def test_path_style_access_is_enabled(self, tmp_path):
        cmd = self._runner(tmp_path).build_command("bronze-verify")
        assert "spark.hadoop.fs.s3a.path.style.access=true" in " ".join(cmd)

    def test_ssl_follows_the_endpoint_scheme(self, tmp_path):
        http = self._runner(tmp_path).build_command("bronze-verify")
        assert "spark.hadoop.fs.s3a.connection.ssl.enabled=false" in " ".join(http)

        https = self._runner(tmp_path, endpoint="https://localhost:3900").build_command(
            "bronze-verify"
        )
        assert "spark.hadoop.fs.s3a.connection.ssl.enabled=true" in " ".join(https)

    def test_hadoop_catalog_needs_no_catalog_service(self, tmp_path):
        joined = " ".join(self._runner(tmp_path).build_command("silver-build"))
        assert "spark.sql.catalog.lb.type=hadoop" in joined
        assert "spark.sql.catalog.lb.warehouse=s3a://lb-bronze/warehouse" in joined

    def test_common_module_is_shipped_to_the_driver(self, tmp_path):
        cmd = self._runner(tmp_path).build_command("gold-finalize")
        assert "--py-files" in cmd
        assert cmd[cmd.index("--py-files") + 1] == "/scripts/common.py"

    def test_master_uses_configured_cores(self, tmp_path):
        cmd = self._runner(tmp_path, cores=8).build_command("bronze-verify")
        assert cmd[cmd.index("--master") + 1] == "local[8]"

    def test_table_names_match_the_kubernetes_path(self, tmp_path):
        joined = " ".join(self._runner(tmp_path).build_command("silver-build"))
        assert "silver.customer_interactions_enriched" in joined
        assert "gold.customer_executive_dashboard" in joined


class TestLocalSparkRun:
    def _runner(self, tmp_path):
        config = LocalSparkConfig(
            endpoint="http://localhost:3900",
            access_key="GK123",
            secret_key="secret",
        )
        return LocalSparkRunner(config, workdir=tmp_path, cli="podman")

    def test_success_returns_elapsed_and_output(self, tmp_path):
        runner = self._runner(tmp_path)
        with mock.patch("subprocess.run", return_value=_completed(stdout="done")):
            result = runner.run_job("bronze-verify")

        assert result.success
        assert result.job_type == "bronze-verify"
        assert result.elapsed_seconds >= 0
        assert result.details["output"] == "done"

    def test_ivy_directory_is_created_before_submission(self, tmp_path):
        runner = self._runner(tmp_path)
        with mock.patch("subprocess.run", return_value=_completed()):
            runner.run_job("bronze-verify")
        assert (Path(tmp_path) / "ivy").is_dir()

    def test_failure_surfaces_the_useful_line(self, tmp_path):
        runner = self._runner(tmp_path)
        noise = "\n".join(["INFO starting"] * 50)
        trace = f"{noise}\nCaused by: java.net.ConnectException: refused\n" + noise
        with mock.patch("subprocess.run", return_value=_completed(returncode=1, stderr=trace)):
            result = runner.run_job("silver-build")

        assert not result.success
        assert "ConnectException" in result.error_message
        assert len(result.error_message) <= 300

    def test_failure_with_no_output_still_explains_itself(self, tmp_path):
        runner = self._runner(tmp_path)
        with mock.patch("subprocess.run", return_value=_completed(returncode=1)):
            result = runner.run_job("gold-finalize")
        assert not result.success
        assert result.error_message

    def test_timeout_is_reported_not_raised(self, tmp_path):
        import subprocess

        runner = self._runner(tmp_path)
        with mock.patch("subprocess.run", side_effect=subprocess.TimeoutExpired("podman", 5)):
            result = runner.run_job("silver-build", timeout=5)

        assert not result.success
        assert "5s" in result.error_message
