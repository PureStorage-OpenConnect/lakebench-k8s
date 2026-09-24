"""The AML batch stages emit a JOB METRICS block the collector can parse.

Before 2026-09-24 none of the three financial stage scripts emitted one, so
bronze and silver reported zero input and the pipeline throughput came from
gold alone. The block is built by ``common.log_job_metrics``; these tests
drive the real emitter and the real parser together.
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

from lakebench.metrics.collector import MetricsCollector

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _common():
    spec = importlib.util.spec_from_file_location("lb_common_job_metrics", SCRIPTS / "common.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _emit(capsys, **kw) -> str:
    _common().log_job_metrics("silver-build", **kw)
    return capsys.readouterr().out


def test_block_round_trips_through_the_collector(capsys):
    logs = _emit(
        capsys,
        input_size_gb=8.37,
        input_rows=26_666_639,
        output_rows=26_600_001,
        elapsed_seconds=412.4,
    )
    m = MetricsCollector().parse_driver_logs(logs, "silver-build")
    assert m.input_size_gb == pytest.approx(8.37)
    assert m.input_rows == 26_666_639
    assert m.output_rows == 26_600_001
    assert m.elapsed_seconds == pytest.approx(412.4)
    assert m.throughput_gb_per_second == pytest.approx(8.37 / 412.4)


def test_loose_completed_line_does_not_override_block_elapsed(capsys):
    """A JVM line matching the fallback pattern must not replace the
    measured elapsed_seconds from the block."""
    block = _emit(capsys, input_size_gb=1.0, input_rows=10, output_rows=10, elapsed_seconds=300.0)
    noise = "26/09/24 2026-09-24 08:55:04 INFO Task 3 completed in 2.0s\n"
    m = MetricsCollector().parse_driver_logs(noise + block, "silver-build")
    assert m.elapsed_seconds == pytest.approx(300.0)


def test_fallback_timing_accepts_complete_in():
    """Scripts that log 'complete in' (no d) still get a fallback elapsed."""
    logs = "2026-09-24 08:59:36 Silver build complete in 120.5s\n"
    m = MetricsCollector().parse_driver_logs(logs, "silver-build")
    assert m.elapsed_seconds == pytest.approx(120.5)


@pytest.mark.parametrize(
    ("script", "job"),
    [
        ("bronze_verify_financial.py", "bronze-verify"),
        ("silver_build_financial.py", "silver-build"),
        ("gold_finalize_financial.py", "gold-finalize"),
    ],
)
def test_each_financial_stage_emits_its_block(script, job):
    src = (SCRIPTS / script).read_text()
    assert "log_job_metrics(" in src
    assert re.search(rf'"{job}"', src), f"{script} must emit the {job} block"
