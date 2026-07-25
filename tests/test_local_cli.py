"""Tests for `deploy --local` and `run --local`.

The guards matter more than the happy path here: local mode must refuse a
config it cannot honour before it starts containers, because the failure
otherwise surfaces much later as an opaque Spark or DuckDB error.
"""

from pathlib import Path
from unittest import mock

import pytest
import typer

from lakebench.cli._local import (
    LOCAL_JOB_ORDER,
    LocalModeError,
    LocalRunResult,
    check_local_supported,
    default_workdir,
    deploy_local,
    run_local,
    scale_advisory,
)
from lakebench.config import load_config
from lakebench.deploy.garage import GarageCredentials
from lakebench.deploy.local import LocalDeployment
from lakebench.engine.protocol import JobResult

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.fixture
def iceberg_config():
    return load_config(EXAMPLES / "hive-iceberg-spark-duckdb.yaml")


@pytest.fixture
def delta_config():
    return load_config(EXAMPLES / "hive-delta-spark-trino.yaml")


def _deployment(tmp_path):
    return LocalDeployment(
        credentials=GarageCredentials(
            access_key="GK123",
            secret_key="secret",
            endpoint="http://localhost:3900",
            region="us-east-1",
        ),
        runtime_cli="podman",
        workdir=tmp_path,
        buckets=("lb-bronze", "lb-silver", "lb-gold"),
    )


class TestFormatGuard:
    def test_iceberg_is_supported(self, iceberg_config):
        check_local_supported(iceberg_config)

    def test_delta_is_refused_with_the_reason(self, delta_config):
        """Gotcha 18: DuckDB's delta kernel hangs on IMDS against non-AWS S3."""
        with pytest.raises(LocalModeError) as exc:
            check_local_supported(delta_config)
        message = str(exc.value)
        assert "delta" in message.lower()
        assert "iceberg" in message.lower()

    def test_run_refuses_delta_before_starting_anything(self, delta_config, tmp_path):
        with mock.patch("lakebench.cli._local.LocalSparkRunner") as runner_cls:
            with pytest.raises(LocalModeError):
                run_local(delta_config, _deployment(tmp_path), workdir=tmp_path)
        runner_cls.assert_not_called()


class TestScaleAdvisory:
    def test_quiet_at_local_scales(self, iceberg_config):
        iceberg_config.architecture.workload.datagen.scale = 0.1
        assert scale_advisory(iceberg_config) == ""

    def test_warns_above_the_threshold(self, iceberg_config):
        iceberg_config.architecture.workload.datagen.scale = 50
        advisory = scale_advisory(iceberg_config)
        assert "500 GB" in advisory

    def test_advisory_does_not_block(self, iceberg_config):
        """Large scales are slow, not invalid -- the user may still mean it."""
        iceberg_config.architecture.workload.datagen.scale = 100
        assert scale_advisory(iceberg_config)
        check_local_supported(iceberg_config)


class TestWorkdir:
    def test_default_is_stable_for_a_config(self, iceberg_config):
        assert default_workdir(iceberg_config.name) == default_workdir(iceberg_config.name)

    def test_default_survives_reboot(self, iceberg_config):
        """The ~1.2 GB Ivy cache must not live in /tmp."""
        assert Path("/tmp") not in default_workdir(iceberg_config.name).parents

    def test_unsafe_characters_are_stripped(self):
        assert "/" not in default_workdir("a/b c").name


class TestDeployLocal:
    def test_buckets_come_from_config(self, iceberg_config, tmp_path):
        with mock.patch("lakebench.cli._local.LocalDeployer", autospec=True) as deployer_cls:
            deployer_cls.return_value.deploy.return_value = _deployment(tmp_path)
            deploy_local(iceberg_config, workdir=tmp_path)

        buckets = deployer_cls.call_args.kwargs["buckets"]
        s3 = iceberg_config.platform.storage.s3.buckets
        assert buckets == (s3.bronze, s3.silver, s3.gold)

    def test_region_comes_from_config(self, iceberg_config, tmp_path):
        """LB-052: the region must match what Spark signs with."""
        iceberg_config.platform.storage.s3.region = "eu-west-1"
        with mock.patch("lakebench.cli._local.LocalDeployer", autospec=True) as deployer_cls:
            deployer_cls.return_value.deploy.return_value = _deployment(tmp_path)
            deploy_local(iceberg_config, workdir=tmp_path)

        assert deployer_cls.call_args.kwargs["region"] == "eu-west-1"


class TestRunLocal:
    def _runner_returning(self, *outcomes):
        runner = mock.Mock()
        runner.run_job.side_effect = [
            JobResult(
                job_id=name,
                job_type=name,
                success=ok,
                elapsed_seconds=1.0,
                error_message="" if ok else "boom",
            )
            for name, ok in outcomes
        ]
        return runner

    def test_runs_every_stage_in_order(self, iceberg_config, tmp_path):
        runner = self._runner_returning(*[(s, True) for s in LOCAL_JOB_ORDER])
        with mock.patch("lakebench.cli._local.LocalSparkRunner", return_value=runner):
            result = run_local(iceberg_config, _deployment(tmp_path), workdir=tmp_path)

        assert result.success
        assert [c.args[0] for c in runner.run_job.call_args_list] == list(LOCAL_JOB_ORDER)

    def test_stops_at_the_first_failure(self, iceberg_config, tmp_path):
        """Silver cannot build on a bronze that did not verify."""
        runner = self._runner_returning(("bronze-verify", False))
        with mock.patch("lakebench.cli._local.LocalSparkRunner", return_value=runner):
            result = run_local(iceberg_config, _deployment(tmp_path), workdir=tmp_path)

        assert not result.success
        assert result.failed_stage == "bronze-verify"
        assert runner.run_job.call_count == 1

    def test_single_stage_runs_alone(self, iceberg_config, tmp_path):
        runner = self._runner_returning(("silver-build", True))
        with mock.patch("lakebench.cli._local.LocalSparkRunner", return_value=runner):
            result = run_local(
                iceberg_config,
                _deployment(tmp_path),
                workdir=tmp_path,
                stages=("silver-build",),
            )

        assert result.success
        assert runner.run_job.call_count == 1

    def test_credentials_reach_the_runner(self, iceberg_config, tmp_path):
        runner = self._runner_returning(*[(s, True) for s in LOCAL_JOB_ORDER])
        with mock.patch("lakebench.cli._local.LocalSparkRunner", return_value=runner) as runner_cls:
            run_local(iceberg_config, _deployment(tmp_path), workdir=tmp_path)

        spark_config = runner_cls.call_args.args[0]
        assert spark_config.access_key == "GK123"
        assert spark_config.endpoint == "http://localhost:3900"


class TestLocalRunResult:
    def test_failed_stage_is_empty_when_everything_passed(self):
        result = LocalRunResult(
            success=True, elapsed_seconds=1.0, stages=[("bronze-verify", True, 1.0)]
        )
        assert result.failed_stage == ""

    def test_failed_stage_names_the_first_failure(self):
        result = LocalRunResult(
            success=False,
            elapsed_seconds=2.0,
            stages=[("bronze-verify", True, 1.0), ("silver-build", False, 1.0)],
        )
        assert result.failed_stage == "silver-build"


class TestRunLocalModeCommand:
    """The CLI wrapper's guards, which run before any container starts."""

    def _invoke(self, cfg, tmp_path, stage=None):
        from lakebench.cli._run import _run_local_mode

        return _run_local_mode(
            cfg, Path("config.yaml"), tmp_path, timeout=60, stage=stage, yes=True
        )

    def test_unknown_stage_exits_without_deploying(self, iceberg_config, tmp_path):
        with mock.patch("lakebench.cli._local.deploy_local") as deploy_fn:
            with pytest.raises(typer.Exit):
                self._invoke(iceberg_config, tmp_path, stage="not-a-stage")
        deploy_fn.assert_not_called()

    def test_delta_exits_without_deploying(self, delta_config, tmp_path):
        with mock.patch("lakebench.cli._local.deploy_local") as deploy_fn:
            with pytest.raises(typer.Exit):
                self._invoke(delta_config, tmp_path)
        deploy_fn.assert_not_called()
