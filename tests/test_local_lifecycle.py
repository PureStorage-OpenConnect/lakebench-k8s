"""Tests for local destroy, status, datagen, and the DuckDB benchmark.

The assertions about empty results and template substitution encode failures
found by running against podman. Both produced green output that measured
nothing, which is the failure mode worth guarding hardest.
"""

from pathlib import Path
from unittest import mock

import pytest

from lakebench.benchmark import BENCHMARK_QUERIES
from lakebench.cli._local import (
    benchmark_local,
    destroy_local,
    generate_local,
    status_local,
)
from lakebench.config import load_config
from lakebench.deploy.garage import (
    _CONTAINER_S3_PORT,
    GarageCredentials,
    GarageDeployer,
    GarageDeployError,
    find_free_port,
)
from lakebench.deploy.local import LocalDeployment
from lakebench.modules.query_engines.duckdb.local_executor import (
    LocalDuckDBExecutor,
    _summarise_duckdb_error,
)

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.fixture
def cfg():
    return load_config(EXAMPLES / "hive-iceberg-spark-duckdb.yaml")


def _deployment(tmp_path):
    return LocalDeployment(
        credentials=GarageCredentials(
            access_key="GK1",
            secret_key="sec",
            endpoint="http://localhost:3900",
            region="us-east-1",
        ),
        runtime_cli="podman",
        workdir=tmp_path,
        buckets=("b", "s", "g"),
    )


def _completed(returncode=0, stdout="", stderr=""):
    return mock.Mock(returncode=returncode, stdout=stdout, stderr=stderr)


class TestDestroyLocal:
    def test_keeps_data_by_default(self, cfg, tmp_path):
        with mock.patch("lakebench.cli._local.LocalDeployer", autospec=True) as cls:
            cls.return_value.destroy.return_value = 1
            destroy_local(cfg, workdir=tmp_path)
        assert cls.return_value.destroy.call_args.kwargs["remove_data"] is False

    def test_removes_data_when_asked(self, cfg, tmp_path):
        with mock.patch("lakebench.cli._local.LocalDeployer", autospec=True) as cls:
            cls.return_value.destroy.return_value = 1
            destroy_local(cfg, workdir=tmp_path, remove_data=True)
        assert cls.return_value.destroy.call_args.kwargs["remove_data"] is True

    def test_returns_count_and_workdir(self, cfg, tmp_path):
        with mock.patch("lakebench.cli._local.LocalDeployer", autospec=True) as cls:
            cls.return_value.destroy.return_value = 2
            removed, used = destroy_local(cfg, workdir=tmp_path)
        assert removed == 2
        assert used == tmp_path


class TestStatusLocal:
    def test_nothing_running_is_not_an_error(self, cfg, tmp_path):
        with mock.patch("lakebench.cli._local.LocalDeployer", autospec=True) as cls:
            cls.return_value.status.return_value = []
            info = status_local(cfg, workdir=tmp_path)
        assert info["running"] == []
        assert info["error"] == ""

    def test_missing_container_cli_is_reported_not_raised(self, cfg, tmp_path):
        with mock.patch(
            "lakebench.cli._local.LocalDeployer", side_effect=RuntimeError("no podman")
        ):
            info = status_local(cfg, workdir=tmp_path)
        assert "no podman" in info["error"]

    def test_does_not_deploy_as_a_side_effect(self, cfg, tmp_path):
        """Asking for status must never start containers."""
        with mock.patch("lakebench.cli._local.LocalDeployer", autospec=True) as cls:
            cls.return_value.status.return_value = []
            status_local(cfg, workdir=tmp_path)
        cls.return_value.deploy.assert_not_called()

    def test_reports_endpoint_when_running(self, cfg, tmp_path):
        with mock.patch("lakebench.cli._local.LocalDeployer", autospec=True) as cls:
            cls.return_value.status.return_value = ["garage"]
            with mock.patch("lakebench.cli._local._running_credentials", return_value=None):
                info = status_local(cfg, workdir=tmp_path)
        assert "3900" in info["endpoint"]


class TestGenerateLocal:
    def test_clears_bronze_before_generating(self, cfg, tmp_path):
        """Datagen part keys do not collide, so a second run would add, not replace."""
        with mock.patch("lakebench.cli._local._empty_bronze", return_value=True) as empty:
            with mock.patch("subprocess.run", return_value=_completed()):
                generate_local(cfg, _deployment(tmp_path))
        empty.assert_called_once()

    def test_replace_false_keeps_existing_data(self, cfg, tmp_path):
        with mock.patch("lakebench.cli._local._empty_bronze") as empty:
            with mock.patch("subprocess.run", return_value=_completed()):
                generate_local(cfg, _deployment(tmp_path), replace=False)
        empty.assert_not_called()

    def test_aborts_when_bronze_cannot_be_cleared(self, cfg, tmp_path):
        with mock.patch("lakebench.cli._local._empty_bronze", return_value=False):
            with mock.patch("subprocess.run") as run:
                assert generate_local(cfg, _deployment(tmp_path)) is False
        run.assert_not_called()

    def test_passes_aws_credential_env_names(self, cfg, tmp_path):
        """The datagen image reads AWS_*, not S3_ACCESS_KEY."""
        with mock.patch("lakebench.cli._local._empty_bronze", return_value=True):
            with mock.patch("subprocess.run", return_value=_completed()) as run:
                generate_local(cfg, _deployment(tmp_path))
        joined = " ".join(run.call_args.args[0])
        assert "AWS_ACCESS_KEY_ID=GK1" in joined
        assert "AWS_SECRET_ACCESS_KEY=sec" in joined

    def test_failure_is_reported(self, cfg, tmp_path):
        with mock.patch("lakebench.cli._local._empty_bronze", return_value=True):
            with mock.patch("subprocess.run", return_value=_completed(returncode=1, stderr="boom")):
                assert generate_local(cfg, _deployment(tmp_path)) is False


class TestPortAllocation:
    """LB-059: two local stacks on one host must not collide."""

    def test_preferred_port_wins_when_free(self):
        with mock.patch("lakebench.deploy.garage.port_is_free", return_value=True):
            assert find_free_port(3900) == 3900

    def test_falls_through_to_the_next_free_port(self):
        taken = {3900, 3901}
        with mock.patch(
            "lakebench.deploy.garage.port_is_free", side_effect=lambda p: p not in taken
        ):
            assert find_free_port(3900) == 3902

    def test_exhausted_range_names_the_remedy(self):
        with mock.patch("lakebench.deploy.garage.port_is_free", return_value=False):
            with pytest.raises(GarageDeployError) as exc:
                find_free_port(3900, attempts=3)
        assert "3900" in str(exc.value)

    def test_running_instance_keeps_its_port(self):
        """Reallocating would hand callers an endpoint nothing listens on."""
        runtime = mock.Mock()
        runtime.published_ports.return_value = [3907]
        deployer = GarageDeployer(runtime, config_dir="/tmp/x")

        with mock.patch("lakebench.deploy.garage.find_free_port") as find:
            assert deployer._resolve_port() == 3907
        find.assert_not_called()

    def test_fresh_deploy_searches_for_a_port(self):
        runtime = mock.Mock()
        runtime.published_ports.return_value = []
        deployer = GarageDeployer(runtime, config_dir="/tmp/x")

        with mock.patch("lakebench.deploy.garage.find_free_port", return_value=3903) as find:
            assert deployer._resolve_port() == 3903
        find.assert_called_once()

    def test_container_port_never_moves(self):
        """3901 is Garage's RPC port; moving S3 onto it hangs the bootstrap."""
        assert _CONTAINER_S3_PORT == 3900

    def test_credentials_carry_the_resolved_port(self):
        deployment = LocalDeployment(
            credentials=GarageCredentials(
                access_key="k",
                secret_key="s",
                endpoint="http://localhost:3907",
                region="us-east-1",
            ),
            runtime_cli="podman",
            workdir=Path("/tmp"),
            buckets=(),
        )
        assert deployment.port == 3907


class TestPublishedPorts:
    def test_returns_every_published_port(self):
        from lakebench.runtime.container import ContainerRuntime

        runtime = ContainerRuntime(cli="podman", namespace="ns")
        payload = '{"Ports": {"3900/tcp": [{"HostPort": "3901"}]}}'
        with mock.patch("subprocess.run", return_value=_completed(stdout=payload)):
            assert runtime.published_ports("garage") == [3901]

    def test_missing_container_returns_empty(self):
        from lakebench.runtime.container import ContainerRuntime

        runtime = ContainerRuntime(cli="podman", namespace="ns")
        with mock.patch("subprocess.run", return_value=_completed(returncode=1)):
            assert runtime.published_ports("garage") == []

    def test_malformed_output_returns_empty(self):
        from lakebench.runtime.container import ContainerRuntime

        runtime = ContainerRuntime(cli="podman", namespace="ns")
        with mock.patch("subprocess.run", return_value=_completed(stdout="not json")):
            assert runtime.published_ports("garage") == []


class TestDuckDBExecutor:
    def _executor(self, tmp_path):
        return LocalDuckDBExecutor(
            endpoint="http://localhost:3900",
            access_key="GK1",
            secret_key="sec",
            warehouse_bucket="lb-silver",
            workdir=str(tmp_path),
            table_names={
                "silver": "silver.customer_interactions_enriched",
                "gold": "gold.customer_executive_dashboard",
            },
        )

    def test_hadoop_paths_have_no_db_suffix(self, tmp_path):
        """The Hive layout uses <namespace>.db; the hadoop catalog does not."""
        sql = self._executor(tmp_path).adapt_query(
            "SELECT * FROM lb.silver.customer_interactions_enriched"
        )
        assert "warehouse/silver/customer_interactions_enriched" in sql
        assert ".db/" not in sql

    def test_both_layers_resolve_to_the_warehouse_bucket(self, tmp_path):
        executor = self._executor(tmp_path)
        for ref in (
            "lb.silver.customer_interactions_enriched",
            "lb.gold.customer_executive_dashboard",
        ):
            assert "s3://lb-silver/warehouse/" in executor.adapt_query(f"SELECT * FROM {ref}")

    def test_query_is_staged_to_a_file(self, tmp_path):
        """Embedding SQL in python -c breaks on queries containing quotes."""
        executor = self._executor(tmp_path)
        sql = "SELECT 'a', \"b\" FROM t"
        with mock.patch("subprocess.run", return_value=_completed(stdout='{"rows": 1}')):
            executor.execute_query(sql)
        assert (tmp_path / "query.sql").read_text() == sql

    def test_empty_output_is_a_failure_not_a_fast_query(self, tmp_path):
        """A zero exit with no payload means the query never ran."""
        executor = self._executor(tmp_path)
        with mock.patch("subprocess.run", return_value=_completed(returncode=0, stdout="")):
            result = executor.execute_query("SELECT 1")
        assert not result.success
        assert "did not run" in result.error

    def test_row_count_comes_from_the_payload(self, tmp_path):
        executor = self._executor(tmp_path)
        with mock.patch(
            "subprocess.run", return_value=_completed(stdout='{"rows": 42, "data": []}')
        ):
            result = executor.execute_query("SELECT 1")
        assert result.success
        assert result.rows_returned == 42

    def test_workdir_is_required(self):
        executor = LocalDuckDBExecutor(
            endpoint="http://localhost:3900",
            access_key="k",
            secret_key="s",
            warehouse_bucket="b",
        )
        with pytest.raises(ValueError, match="workdir"):
            executor.execute_query("SELECT 1")


class TestDuckDBErrorSummary:
    def test_prefers_the_named_exception_over_a_caret(self):
        text = 'Traceback:\n  File "x"\n_duckdb.ParserException: Parser Error: near "{"\n     ^'
        assert "ParserException" in _summarise_duckdb_error(text)

    def test_never_returns_a_bare_caret(self):
        """A caret points at a column and says nothing on its own."""
        assert _summarise_duckdb_error("something failed\n   ^").strip() != "^"

    def test_empty_input_still_explains_itself(self):
        assert _summarise_duckdb_error("")


class TestBenchmarkLocal:
    def test_templates_are_substituted_before_adapting(self, cfg, tmp_path):
        """Unsubstituted {catalog} reaches DuckDB as a literal brace and fails."""
        executed: list[str] = []

        class FakeExecutor:
            catalog_name = "lb"

            def __init__(self, *a, **k):
                pass

            def health_check(self):
                return True

            def adapt_query(self, sql):
                return sql

            def execute_query(self, sql, timeout=300):
                executed.append(sql)
                return mock.Mock(success=True, duration_seconds=1.0, error=None, rows_returned=42)

        with mock.patch(
            "lakebench.modules.query_engines.duckdb.local_executor.LocalDuckDBExecutor",
            FakeExecutor,
        ):
            results, qph = benchmark_local(cfg, _deployment(tmp_path), workdir=tmp_path)

        assert len(results) == len(BENCHMARK_QUERIES)
        assert executed, "no queries ran"
        for sql in executed:
            assert "{catalog}" not in sql
            assert "{silver_table}" not in sql
            assert "{gold_table}" not in sql

    def test_rows_returned_reaches_the_recorded_queries(self, cfg, tmp_path):
        """A query that returns no rows read nothing; the report must show that."""
        from lakebench.cli._run import _record_local_queries
        from lakebench.metrics import MetricsCollector

        collector = MetricsCollector()
        collector.start_run("run-1", "test", cfg.model_dump(mode="json"))
        _record_local_queries(collector, cfg, [("Q1", True, 2.0, 1234)], qph=100.0)

        recorded = collector.current_run.benchmark.queries[0]
        assert recorded["rows_returned"] == 1234

    def test_qph_is_zero_when_everything_fails(self, cfg, tmp_path):
        class FailingExecutor:
            catalog_name = "lb"

            def __init__(self, *a, **k):
                pass

            def health_check(self):
                return True

            def adapt_query(self, sql):
                return sql

            def execute_query(self, sql, timeout=300):
                return mock.Mock(success=False, duration_seconds=1.0, error="nope", rows_returned=0)

        with mock.patch(
            "lakebench.modules.query_engines.duckdb.local_executor.LocalDuckDBExecutor",
            FailingExecutor,
        ):
            results, qph = benchmark_local(cfg, _deployment(tmp_path), workdir=tmp_path)

        assert qph == 0.0
        assert all(not ok for _, ok, _, _ in results)

    def test_unhealthy_engine_skips_rather_than_reporting_zeros(self, cfg, tmp_path):
        class DeadExecutor:
            catalog_name = "lb"

            def __init__(self, *a, **k):
                pass

            def health_check(self):
                return False

        with mock.patch(
            "lakebench.modules.query_engines.duckdb.local_executor.LocalDuckDBExecutor",
            DeadExecutor,
        ):
            results, qph = benchmark_local(cfg, _deployment(tmp_path), workdir=tmp_path)

        assert results == []
        assert qph == 0.0


class TestDuckDBVersionPin:
    """An unpinned engine turns `compare` into a comparison of two engines.

    Both install sites ran a bare `pip install duckdb`, so a run in July and a
    run in August could query with different versions while the tool reported
    the difference as a benchmark result. Measured local spread is 0.9%, far
    below what an engine change would move, so the drift was invisible.
    """

    def test_local_executor_pins_the_version(self):
        from lakebench.modules.query_engines.duckdb.local_executor import LocalDuckDBExecutor

        executor = LocalDuckDBExecutor(
            endpoint="http://localhost:3900",
            access_key="a",
            secret_key="b",
            warehouse_bucket="w",
            duckdb_version="1.5.5",
            workdir="/tmp/duckdb",
        )
        assert executor.duckdb_version == "1.5.5"

    def test_config_version_reaches_the_executor(self, cfg, tmp_path):
        """A pin nobody threads through the config is not a pin."""
        from lakebench.cli._local import benchmark_local

        cfg.architecture.query_engine.duckdb.version = "1.5.4"
        captured = {}

        class RecordingExecutor:
            catalog_name = "lb"

            def __init__(self, *a, **k):
                captured.update(k)

            def health_check(self):
                return False

        with mock.patch(
            "lakebench.modules.query_engines.duckdb.local_executor.LocalDuckDBExecutor",
            RecordingExecutor,
        ):
            benchmark_local(cfg, _deployment(tmp_path), workdir=tmp_path)

        assert captured.get("duckdb_version") == "1.5.4"

    def test_schema_default_is_pinned_not_floating(self):
        from lakebench.config.schema import DuckDBConfig

        version = DuckDBConfig().version
        assert version and version[0].isdigit(), "version must be concrete, not 'latest'"

    def test_running_version_comes_from_the_engine_not_config(self):
        """Config states an intention; only the engine can confirm it."""
        from lakebench.modules.query_engines.duckdb.local_executor import LocalDuckDBExecutor

        executor = LocalDuckDBExecutor(
            endpoint="http://localhost:3900",
            access_key="a",
            secret_key="b",
            warehouse_bucket="w",
            duckdb_version="1.5.5",
            workdir="/tmp/duckdb",
        )
        # Engine reports something different from what was requested -- exactly
        # the drift the pin exists to catch.
        executor._reported_version = "1.5.4"
        with mock.patch.object(executor, "execute_query", return_value=mock.Mock(success=True)):
            assert executor.running_version() == "1.5.4"

    def test_running_version_is_empty_when_the_query_fails(self):
        """Distinguish "could not ask" from a mismatch."""
        from lakebench.modules.query_engines.duckdb.local_executor import LocalDuckDBExecutor

        executor = LocalDuckDBExecutor(
            endpoint="http://localhost:3900",
            access_key="a",
            secret_key="b",
            warehouse_bucket="w",
            workdir="/tmp/duckdb",
        )
        executor._reported_version = "1.5.5"
        with mock.patch.object(executor, "execute_query", return_value=mock.Mock(success=False)):
            assert executor.running_version() == ""
