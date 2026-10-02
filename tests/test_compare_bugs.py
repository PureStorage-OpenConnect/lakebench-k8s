"""A5 bugfix tests: compare and standalone benchmark hygiene gates.

Each case guards a bug where the CLI silently misled a user or hid a
failure behind exit 0. Reverting the corresponding fix makes each test fail.

  1. `compare --scale` only rewrote the in-memory configs. compare now reads
     stored records only, so --scale (with every other flag of the command
     that ran both configs) is refused with exit 2 and the replacement.
  2. `compare --format html` silently fell back to JSON. It now refuses
     cleanly with exit 2 and points at `report --render`.
  3. Standalone `benchmark` exited 0 even when every query failed or the
     query set was empty. It now fails the run (exit 1), matching what
     `run` enforces via _benchmark_gate_problems.
"""

from __future__ import annotations

from unittest import mock

from typer.testing import CliRunner

from lakebench.benchmark.runner import BenchmarkResult, QueryResult
from lakebench.cli import app
from tests.conftest import make_config

runner = CliRunner()


# ----------------------------------------------------------------------------
# 1. --scale: refused, compare no longer runs configs.
# ----------------------------------------------------------------------------


class TestCompareScaleRefused:
    def test_compare_scale_refused_with_exit_2(self, tmp_path):
        result = runner.invoke(app, ["compare", "a.yaml", "b.yaml", "--scale", "50"])
        assert result.exit_code == 2, result.output
        assert "no longer runs configs" in " ".join(result.output.split())


# ----------------------------------------------------------------------------
# 2. --format html: refuse and point at report --render.
# ----------------------------------------------------------------------------


class TestCompareFormatHtmlRefused:
    def test_compare_format_html_refused_or_writes_html(self, tmp_path):
        result = runner.invoke(app, ["compare", "a", "b", "--format", "html"])
        assert result.exit_code == 2, result.output
        assert "report --render" in result.output


# ----------------------------------------------------------------------------
# 3. benchmark with no successful query exits 1.
# ----------------------------------------------------------------------------


class TestBenchmarkEmptyQuerySetGate:
    @staticmethod
    def _empty_benchmark_result() -> BenchmarkResult:
        return BenchmarkResult(
            mode="power",
            cache="hot",
            scale=1.0,
            queries=[],
            total_seconds=0.0,
            qph=0.0,
            iterations=1,
            streams=1,
            stream_results=[],
            engine="trino",
        )

    @staticmethod
    def _all_failed_result() -> BenchmarkResult:
        from lakebench.benchmark.queries import BenchmarkQuery

        q = BenchmarkQuery(
            name="Q1_full_aggregation_scan",
            display_name="Q1",
            query_class="scan",
            sql="SELECT 1",
        )
        return BenchmarkResult(
            mode="power",
            cache="hot",
            scale=1.0,
            queries=[
                QueryResult(
                    query=q,
                    elapsed_seconds=0.0,
                    rows_returned=0,
                    success=False,
                    error_message="boom",
                )
            ],
            total_seconds=0.0,
            qph=0.0,
            iterations=1,
            streams=1,
            stream_results=[],
            engine="trino",
        )

    def _invoke(self, tmp_path, bench_result) -> mock.Mock:
        cfg_path = tmp_path / "cfg.yaml"
        cfg_path.write_text("name: bench-empty\n")
        cfg = make_config(name="bench-empty")

        fake_runner = mock.Mock()
        fake_runner.tm_run_id = None
        fake_runner.run.return_value = bench_result

        with (
            mock.patch("lakebench.cli._query.load_config", return_value=cfg),
            # benchmark() does `from lakebench.benchmark import BenchmarkRunner`
            # inside the function; patch at the source module.
            mock.patch("lakebench.benchmark.BenchmarkRunner", return_value=fake_runner),
            mock.patch("lakebench.cli._query._latest_tm_run_id", return_value=None),
            mock.patch("lakebench.cli._query.journal_open") as journal_open,
        ):
            journal_open.return_value = mock.Mock()
            return runner.invoke(app, ["benchmark", str(cfg_path)])

    def test_benchmark_empty_query_set_exits_1(self, tmp_path):
        """An empty query set gives QpH=0 with no evidence -- exit 1, not 0."""
        result = self._invoke(tmp_path, self._empty_benchmark_result())
        assert result.exit_code == 1, result.output
        assert "no query results" in result.output.lower()

    def test_benchmark_all_failed_queries_exits_1(self, tmp_path):
        """Zero successful queries is the same shape: no valid score."""
        result = self._invoke(tmp_path, self._all_failed_result())
        assert result.exit_code == 1, result.output
        assert "no query results" in result.output.lower()
