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
from lakebench.deploy.garage import GarageCredentials
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
                return mock.Mock(success=True, duration_seconds=1.0, error=None)

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
                return mock.Mock(success=False, duration_seconds=1.0, error="nope")

        with mock.patch(
            "lakebench.modules.query_engines.duckdb.local_executor.LocalDuckDBExecutor",
            FailingExecutor,
        ):
            results, qph = benchmark_local(cfg, _deployment(tmp_path), workdir=tmp_path)

        assert qph == 0.0
        assert all(not ok for _, ok, _ in results)

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
