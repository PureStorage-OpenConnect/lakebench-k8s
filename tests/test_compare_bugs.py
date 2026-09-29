"""A5 bugfix tests: compare and standalone benchmark hygiene gates.

These four cases each guard a bug where the CLI silently misled a user or
hid a failure behind exit 0. Reverting the corresponding fix makes each
test fail.

  1. `compare --scale` only rewrote the in-memory configs; the subprocess
     `run` calls reloaded from disk and benchmarked at whatever the file
     said. The option now refuses with exit 2 pointing at the config file.
  2. `compare --format html` silently fell back to JSON in `_save_comparison`.
     It now refuses cleanly with exit 2 and points at `report --render`.
  3. `compare` swallowed the destroy subprocess exit code, so an orphaned
     namespace or bucket only showed up when the next run tripped over it.
     Compare now captures the destroy exit code, surfaces the failure, and
     exits non-zero.
  4. Standalone `benchmark` exited 0 even when every query failed or the
     query set was empty. It now fails the run (exit 1), matching what
     `run` enforces via _benchmark_gate_problems.
"""

from __future__ import annotations

from pathlib import Path
from unittest import mock

from typer.testing import CliRunner

from lakebench.benchmark.runner import BenchmarkResult, QueryResult
from lakebench.cli import _compare as compare_mod
from lakebench.cli import app
from tests.conftest import make_config

runner = CliRunner()


def _yaml_pair(tmp_path: Path) -> tuple[Path, Path]:
    """Two empty YAML paths, distinct so the local-mode name check does not fire."""
    a = tmp_path / "a.yaml"
    b = tmp_path / "b.yaml"
    a.write_text("name: a\n")
    b.write_text("name: b\n")
    return a, b


# ----------------------------------------------------------------------------
# 1. --scale: refuse with exit 2 rather than silently rewriting in-memory only.
# ----------------------------------------------------------------------------


class TestCompareScaleRefused:
    def test_compare_scale_refused_with_exit_2(self, tmp_path):
        """--scale is deferred: the subprocess runs would ignore it and the
        printed plan would lie. Exit 2 with a message pointing at the config
        file, before any subprocess is launched."""
        a, b = _yaml_pair(tmp_path)
        cfg_a = make_config(name="a")
        cfg_b = make_config(name="b")

        with (
            mock.patch("lakebench.cli._compare.load_config", side_effect=[cfg_a, cfg_b]),
            mock.patch("lakebench.cli._compare._run_single") as run_single,
        ):
            result = runner.invoke(
                app,
                ["compare", str(a), str(b), "--scale", "100", "--yes"],
            )

        assert result.exit_code == 2, result.output
        assert "--scale" in result.output
        assert "config" in result.output.lower()
        # Refusal happens before any run is launched.
        assert run_single.call_count == 0


# ----------------------------------------------------------------------------
# 2. --format html: refuse cleanly rather than silently writing JSON.
# ----------------------------------------------------------------------------


class TestCompareFormatHtmlRefused:
    def test_compare_format_html_refused_or_writes_html(self, tmp_path):
        """`_save_comparison`'s else-branch silently wrote JSON for html. Refuse
        with exit 2 and point at `report --render`, the real HTML surface."""
        a, b = _yaml_pair(tmp_path)
        cfg_a = make_config(name="a")
        cfg_b = make_config(name="b")

        with (
            mock.patch("lakebench.cli._compare.load_config", side_effect=[cfg_a, cfg_b]),
            mock.patch("lakebench.cli._compare._run_single") as run_single,
        ):
            result = runner.invoke(
                app,
                ["compare", str(a), str(b), "--format", "html", "--yes"],
            )

        assert result.exit_code == 2, result.output
        assert "html" in result.output.lower()
        assert "report --render" in result.output
        assert run_single.call_count == 0


# ----------------------------------------------------------------------------
# 3. Destroy failures: capture the subprocess exit code and surface it.
# ----------------------------------------------------------------------------


class TestCompareSurfacesDestroyFailure:
    def test_run_single_records_destroy_exit_code(self, tmp_path):
        """_run_single now captures a non-zero destroy exit code and threads
        the reason back via the returned metrics dict."""

        def _subprocess_run(cmd, *args, **kwargs):
            if "destroy" in cmd:
                return mock.Mock(
                    returncode=1,
                    stderr=b"namespace still contains foreign pods",
                    stdout=b"",
                )
            return mock.Mock(returncode=0, stderr=b"", stdout=b"")

        with (
            mock.patch("subprocess.run", side_effect=_subprocess_run),
            mock.patch(
                "lakebench.cli._compare._load_latest_metrics",
                return_value={"run_id": "r1"},
            ),
        ):
            result = compare_mod._run_single(
                tmp_path / "c.yaml",
                timeout=60,
                skip_benchmark=False,
                keep=False,
                local=False,
            )

        assert isinstance(result, dict)
        assert "_destroy_error" in result, result
        assert "exit" in result["_destroy_error"].lower()
        assert "foreign pods" in result["_destroy_error"]

    def test_compare_surfaces_destroy_failure(self, tmp_path):
        """A destroy that failed inside _run_single must surface at compare's
        level with a red message and a non-zero exit code, even when the
        comparison itself would have been comparable."""
        a, b = _yaml_pair(tmp_path)
        cfg_a = make_config(name="a")
        cfg_b = make_config(name="b")

        # Two metrics dicts already carrying the failure sentinel that
        # _run_single would set when subprocess destroy returned non-zero.
        metrics_a = {"run_id": "r-a", "_destroy_error": "destroy exited 1: leaked bucket"}
        metrics_b = {"run_id": "r-b"}

        with (
            mock.patch("lakebench.cli._compare.load_config", side_effect=[cfg_a, cfg_b]),
            mock.patch("lakebench.cli._compare._run_single", side_effect=[metrics_a, metrics_b]),
            mock.patch("lakebench.cli._compare.DEFAULT_OUTPUT_DIR", str(tmp_path / "out")),
        ):
            result = runner.invoke(app, ["compare", str(a), str(b), "--yes"])

        assert result.exit_code != 0, result.output
        assert "Destroy for run A failed" in result.output
        assert "leaked bucket" in result.output


# ----------------------------------------------------------------------------
# 4. Standalone `benchmark`: zero-query-result gate.
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
