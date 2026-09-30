"""A2c: `lakebench report --render` never rewrites the delivered artifact.

The delivered ``run-<id>/report.html`` is written once at the end of a
benchmark run. A later ``lakebench report`` invocation prints its summary
and points at the delivered file; a later ``lakebench report --render``
writes a fresh timestamped file under ``lakebench-output/reports/`` without
mutating ``report.html``. Only an explicit ``--render --output <path>
--force`` may overwrite a pre-existing file.
"""

from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from lakebench.cli import app
from lakebench.metrics import MetricsStorage, PipelineMetrics
from lakebench.reports.generator import ReportGenerator

runner = CliRunner()


def _metrics(run_id: str = "20260928-101010-a2ctst") -> PipelineMetrics:
    return PipelineMetrics(
        run_id=run_id,
        deployment_name="a2c-rr",
        start_time=datetime(2026, 9, 28, 10, 10, 10),
        end_time=datetime(2026, 9, 28, 10, 40, 10),
        success=True,
    )


def _seed_run_with_delivered_report(
    tmp_path: Path,
) -> tuple[MetricsStorage, PipelineMetrics, Path]:
    """Save a run and deliver its ``report.html`` (the one legitimate site).

    Uses the same code path a real run takes: explicit ``output_path`` and
    ``force=True`` on the first delivery, matching ``write_run_report``.
    """
    metrics_dir = tmp_path / "runs"
    metrics_dir.mkdir(parents=True)
    storage = MetricsStorage(metrics_dir)
    m = _metrics()
    storage.save_run(m)
    delivered = storage.run_dir(m.run_id) / "report.html"
    ReportGenerator(metrics_dir).generate_report(m.run_id, output_path=delivered, force=True)
    assert delivered.exists()
    return storage, m, delivered


def _reports_dir(tmp_path: Path) -> Path:
    return tmp_path / "reports"


# ---------------------------------------------------------------------------
# CLI behaviour
# ---------------------------------------------------------------------------


def test_report_default_prints_summary_and_leaves_report_html_untouched(tmp_path):
    """Bare ``report --run <id>`` never rewrites the delivered HTML."""
    _, m, delivered = _seed_run_with_delivered_report(tmp_path)
    mtime_before = delivered.stat().st_mtime_ns
    size_before = delivered.stat().st_size
    body_before = delivered.read_text()

    result = runner.invoke(
        app,
        ["report", "--metrics", str(tmp_path / "runs"), "--run", m.run_id],
    )

    assert result.exit_code == 0, result.stdout
    assert delivered.stat().st_mtime_ns == mtime_before
    assert delivered.stat().st_size == size_before
    assert delivered.read_text() == body_before


def test_report_render_writes_new_timestamped_file_leaves_delivered_untouched(tmp_path):
    """``report --render`` writes a fresh file under ``lakebench-output/reports/``."""
    _, m, delivered = _seed_run_with_delivered_report(tmp_path)
    mtime_before = delivered.stat().st_mtime_ns
    body_before = delivered.read_text()

    reports_dir = _reports_dir(tmp_path)
    assert not reports_dir.exists()

    result = runner.invoke(
        app,
        [
            "report",
            "--metrics",
            str(tmp_path / "runs"),
            "--run",
            m.run_id,
            "--render",
        ],
    )
    assert result.exit_code == 0, result.stdout

    # Delivered artifact is byte-identical.
    assert delivered.stat().st_mtime_ns == mtime_before
    assert delivered.read_text() == body_before

    # A fresh timestamped file exists under reports/.
    assert reports_dir.exists()
    fresh = list(reports_dir.glob(f"report-{m.run_id}-*.html"))
    assert len(fresh) == 1, [p.name for p in fresh]
    assert "<html" in fresh[0].read_text()


def test_report_render_twice_writes_two_files(tmp_path):
    """Two ``--render`` invocations produce two distinct files."""
    _, m, delivered = _seed_run_with_delivered_report(tmp_path)
    body_before = delivered.read_text()
    reports_dir = _reports_dir(tmp_path)

    r1 = runner.invoke(
        app,
        ["report", "--metrics", str(tmp_path / "runs"), "--run", m.run_id, "--render"],
    )
    assert r1.exit_code == 0, r1.stdout

    # Second-granularity timestamp; ensure a distinct second so the target
    # path differs and no --force is needed.
    time.sleep(1.1)

    r2 = runner.invoke(
        app,
        ["report", "--metrics", str(tmp_path / "runs"), "--run", m.run_id, "--render"],
    )
    assert r2.exit_code == 0, r2.stdout

    files = sorted(reports_dir.glob(f"report-{m.run_id}-*.html"))
    assert len(files) == 2, [p.name for p in files]

    # Delivered still untouched.
    assert delivered.read_text() == body_before


def test_report_render_output_refuses_without_force_at_existing_path(tmp_path):
    """``--render --output <existing>`` without ``--force`` refuses to clobber."""
    _, m, _ = _seed_run_with_delivered_report(tmp_path)

    existing = tmp_path / "custom.html"
    existing.write_text("keep-me-marker")

    result = runner.invoke(
        app,
        [
            "report",
            "--metrics",
            str(tmp_path / "runs"),
            "--run",
            m.run_id,
            "--render",
            "--output",
            str(existing),
        ],
    )
    assert result.exit_code != 0
    assert existing.read_text() == "keep-me-marker"


def test_report_render_output_with_force_overwrites(tmp_path):
    """``--render --output <existing> --force`` may overwrite."""
    _, m, _ = _seed_run_with_delivered_report(tmp_path)

    existing = tmp_path / "custom.html"
    existing.write_text("keep-me-marker")

    result = runner.invoke(
        app,
        [
            "report",
            "--metrics",
            str(tmp_path / "runs"),
            "--run",
            m.run_id,
            "--render",
            "--output",
            str(existing),
            "--force",
        ],
    )
    assert result.exit_code == 0, result.stdout
    body = existing.read_text()
    assert body != "keep-me-marker"
    assert "<html" in body


def test_force_without_render_is_rejected(tmp_path):
    """``--force`` alone (no ``--render``) is a usage error, not a no-op."""
    _, m, delivered = _seed_run_with_delivered_report(tmp_path)
    body_before = delivered.read_text()

    result = runner.invoke(
        app,
        [
            "report",
            "--metrics",
            str(tmp_path / "runs"),
            "--run",
            m.run_id,
            "--force",
        ],
    )
    assert result.exit_code != 0
    assert delivered.read_text() == body_before


# ---------------------------------------------------------------------------
# API behaviour
# ---------------------------------------------------------------------------


def test_generate_report_default_writes_scratch_not_delivered_report_html(tmp_path):
    """``generate_report(run_id)`` without an override never targets ``report.html``."""
    metrics_dir = tmp_path / "runs"
    metrics_dir.mkdir(parents=True)
    storage = MetricsStorage(metrics_dir)
    m = _metrics(run_id="20260928-000000-scratch")
    storage.save_run(m)
    delivered = storage.run_dir(m.run_id) / "report.html"
    assert not delivered.exists()

    path = ReportGenerator(metrics_dir).generate_report(m.run_id)

    assert path != delivered
    assert not delivered.exists()
    # Default path lands in the sibling ``reports/`` directory with the
    # documented naming scheme.
    assert path.parent == tmp_path / "reports"
    assert path.name.startswith(f"report-{m.run_id}-")
    assert path.name.endswith(".html")
    assert path.exists() and "<html" in path.read_text()


def test_generate_report_refuses_to_overwrite_existing_output_without_force(tmp_path):
    metrics_dir = tmp_path / "runs"
    metrics_dir.mkdir(parents=True)
    storage = MetricsStorage(metrics_dir)
    m = _metrics(run_id="20260928-000000-refuse")
    storage.save_run(m)

    target = tmp_path / "custom.html"
    target.write_text("keep")

    with pytest.raises(FileExistsError):
        ReportGenerator(metrics_dir).generate_report(m.run_id, output_path=target)
    assert target.read_text() == "keep"

    # Force overrides.
    result = ReportGenerator(metrics_dir).generate_report(m.run_id, output_path=target, force=True)
    assert result == target
    assert "<html" in target.read_text()


def test_generate_report_refuses_delivered_report_html_without_force(tmp_path):
    """Even given the explicit path, overwriting the delivered artifact needs ``force``."""
    _, m, delivered = _seed_run_with_delivered_report(tmp_path)
    metrics_dir = tmp_path / "runs"
    body_before = delivered.read_text()

    with pytest.raises(FileExistsError):
        ReportGenerator(metrics_dir).generate_report(m.run_id, output_path=delivered)
    assert delivered.read_text() == body_before
