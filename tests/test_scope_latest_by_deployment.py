"""Deployment-scoped "latest run" lookup.

Parallel deployments share one ``lakebench-output/runs/`` tree, so an
unscoped ``get_latest_run()`` reads whichever deployment finished last.
Covered here: ``MetricsStorage.get_latest_run_for_deployment`` (including
the legacy fallback for records with no deployment_name) and the report
generator's deployment scope. The ``benchmark`` and ``query`` callsites are
not driven by this file.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from lakebench.metrics import MetricsStorage, PipelineMetrics

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _write_run(
    storage: MetricsStorage,
    run_id: str,
    deployment_name: str,
    start_time: datetime,
) -> Path:
    """Persist a minimal PipelineMetrics record and return its metrics.json."""
    metrics = PipelineMetrics(
        run_id=run_id,
        deployment_name=deployment_name,
        start_time=start_time,
        success=True,
        jobs=[],
    )
    return storage.save_run(metrics)


def _write_legacy_run(
    metrics_dir: Path,
    run_id: str,
    start_time: datetime,
) -> Path:
    """Write a pre-v1.6 style record with no ``deployment_name`` field.

    The old records used the flat legacy layout ``run-<id>.json`` and did not
    persist a deployment name; both attributes matter to the fallback path in
    ``get_latest_run_for_deployment``.
    """
    metrics_dir.mkdir(parents=True, exist_ok=True)
    filepath = metrics_dir / f"run-{run_id}.json"
    filepath.write_text(
        json.dumps(
            {
                "run_id": run_id,
                # deliberately omit deployment_name -- legacy behaviour
                "start_time": start_time.isoformat(),
                "success": True,
                "jobs": [],
            }
        )
    )
    return filepath


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


class TestGetLatestRunForDeployment:
    """Direct tests of the storage helper."""

    def test_returns_none_when_no_records_match_and_no_legacy(self, tmp_path: Path) -> None:
        storage = MetricsStorage(tmp_path / "runs")
        _write_run(storage, "b-001", "dep-b", datetime(2026, 9, 28, tzinfo=timezone.utc))

        assert storage.get_latest_run_for_deployment("dep-a") is None

    def test_legacy_fallback_when_no_named_match(self, tmp_path: Path) -> None:
        """A pre-v1.6 record (no deployment_name) is returned when the scoped
        lookup finds no exact match. B's newer record must NOT be returned."""
        runs_dir = tmp_path / "runs"
        storage = MetricsStorage(runs_dir)
        base = datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc)

        # B's record is newest, but must be ignored (its name != dep-a).
        _write_run(storage, "b-001", "dep-b", base + timedelta(hours=2))
        # Legacy record has no deployment_name -- it is the fallback.
        _write_legacy_run(runs_dir, "legacy-001", base)

        latest = storage.get_latest_run_for_deployment("dep-a")

        assert latest is not None
        assert latest.run_id == "legacy-001"

    def test_prefers_named_match_over_newer_legacy(self, tmp_path: Path) -> None:
        """An exact deployment_name match wins over a newer legacy record."""
        runs_dir = tmp_path / "runs"
        storage = MetricsStorage(runs_dir)
        base = datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc)

        _write_run(storage, "a-001", "dep-a", base)
        _write_legacy_run(runs_dir, "legacy-001", base + timedelta(hours=5))

        latest = storage.get_latest_run_for_deployment("dep-a")

        assert latest is not None
        assert latest.run_id == "a-001"


# ---------------------------------------------------------------------------
# Two deployments sharing one runs tree
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Generator scope: the report renderer takes a deployment_name too
# ---------------------------------------------------------------------------


class TestGeneratorScope:
    """``ReportGenerator.generate_report(deployment_name=...)`` scopes the
    "latest run" lookup so ``lakebench report --render`` under parallel
    deployments cannot render another deployment's record."""

    def test_generator_scopes_to_the_deployment_else_latest(self, tmp_path: Path) -> None:
        """With two deployments sharing one runs tree (dep-b strictly newer),
        ``deployment_name="dep-a"`` renders a-001 (not the newer b-001), and
        ``deployment_name=None`` keeps the previous unscoped behaviour: the
        newest record, b-001."""
        from lakebench.reports.generator import ReportGenerator

        runs_dir = tmp_path / "runs"
        storage = MetricsStorage(runs_dir)
        base = datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc)
        _write_run(storage, "a-001", "dep-a", base)
        # B is newer: an unscoped get_latest_run() returns it.
        _write_run(storage, "b-001", "dep-b", base + timedelta(hours=1))
        gen = ReportGenerator(runs_dir)

        scoped = gen.generate_report(deployment_name="dep-a").read_text()
        assert "a-001" in scoped, "scoped lookup must pick dep-a's run"
        assert "b-001" not in scoped, "scoped lookup must ignore the newer dep-b"

        unscoped = gen.generate_report().read_text()
        assert "b-001" in unscoped, "unscoped default must be the newest run"
