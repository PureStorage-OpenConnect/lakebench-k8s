"""Deployment-scoped "latest run" lookup (A1: SP-1 interim fix).

Parallel deployments share a single ``lakebench-output/runs/`` tree, so an
unscoped ``get_latest_run()`` reads whichever deployment happened to finish
last. Callsites that then rewrite the record (``lakebench benchmark`` and
``lakebench query``) corrupt another deployment's history.

The interim fix here is ``MetricsStorage.get_latest_run_for_deployment(name)``
plus a switch of every unscoped callsite to it. SP-2 owns the durable
deployment_id follow-up.

These tests fix the callsites in place: they exercise ``benchmark`` against
deployment A and assert deployment B's ``metrics.json`` file is byte-for-byte
untouched (mtime AND sha256 unchanged); same for ``query``. A separate case
covers the legacy fallback where a record has no recorded deployment_name.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _snapshot(path: Path) -> tuple[float, str]:
    """(mtime, sha256) -- both must be unchanged for a file "not rewritten"."""
    return (path.stat().st_mtime, _sha256(path))


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


class TestGetLatestRunForDeployment:
    """Direct tests of the storage helper."""

    def test_returns_matching_deployment_ignoring_newer_other(self, tmp_path: Path) -> None:
        storage = MetricsStorage(tmp_path / "runs")
        base = datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc)
        _write_run(storage, "a-001", "dep-a", base)
        # B is strictly newer -- an unscoped lookup would return it.
        _write_run(storage, "b-001", "dep-b", base + timedelta(hours=1))

        latest = storage.get_latest_run_for_deployment("dep-a")

        assert latest is not None
        assert latest.run_id == "a-001"
        assert latest.deployment_name == "dep-a"

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

    def test_none_argument_falls_back_to_unscoped(self, tmp_path: Path) -> None:
        """A ``None`` (or empty) deployment_name is the unscoped path."""
        storage = MetricsStorage(tmp_path / "runs")
        base = datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc)
        _write_run(storage, "a-001", "dep-a", base)
        _write_run(storage, "b-001", "dep-b", base + timedelta(hours=1))

        latest = storage.get_latest_run_for_deployment(None)
        assert latest is not None
        assert latest.run_id == "b-001"

        latest_empty = storage.get_latest_run_for_deployment("")
        assert latest_empty is not None
        assert latest_empty.run_id == "b-001"


# ---------------------------------------------------------------------------
# Callsite tests: benchmark and query must not rewrite the wrong record
# ---------------------------------------------------------------------------


def _make_two_deployments(
    tmp_path: Path,
) -> tuple[MetricsStorage, Path, Path]:
    """Create runs for dep-a and (strictly newer) dep-b. Return storage and
    both metrics.json paths."""
    storage = MetricsStorage(tmp_path / "runs")
    base = datetime(2026, 9, 28, 12, 0, 0, tzinfo=timezone.utc)
    a_path = _write_run(storage, "a-001", "dep-a", base)
    # B is newer: an unscoped get_latest_run() returns it.
    b_path = _write_run(storage, "b-001", "dep-b", base + timedelta(hours=1))
    return storage, a_path, b_path


def _fake_cfg(name: str) -> Any:
    """Small shim standing in for LakebenchConfig -- only ``.name`` is read
    by the code under test."""

    class _Cfg:
        pass

    cfg = _Cfg()
    cfg.name = name  # type: ignore[attr-defined]
    return cfg


class TestBenchmarkScope:
    """The benchmark path (``_query.benchmark``) scopes its parent lookup by
    deployment name and writes its own record: neither A's run record nor
    B's is rewritten."""

    def test_benchmark_for_a_does_not_rewrite_b(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from types import SimpleNamespace

        from lakebench.cli._query import _save_benchmark_record

        storage, a_path, b_path = _make_two_deployments(tmp_path)
        b_before = _snapshot(b_path)
        a_before = _snapshot(a_path)

        cfg = _fake_cfg("dep-a")
        latest = storage.get_latest_run_for_deployment(cfg.name, writable=True)
        assert latest is not None and latest.run_id == "a-001"
        result = SimpleNamespace(
            mode="power",
            cache="hot",
            scale=1,
            qph=123.4,
            total_seconds=42.0,
            queries=[],
            iterations=1,
            streams=1,
            stream_results=[],
            engine="trino",
        )
        path = _save_benchmark_record(storage, latest, result)

        # Both run records are byte-for-byte identical: neither mtime nor sha256 changed.
        assert _snapshot(b_path) == b_before
        assert _snapshot(a_path) == a_before
        rec = storage.load_run(path.parent.name.removeprefix("run-"))
        assert rec is not None and rec.run_id != "a-001"
        assert (rec.record_kind, rec.parent_run_id, rec.deployment_name) == (
            "benchmark",
            "a-001",
            "dep-a",
        )
        assert rec.benchmark is not None and rec.benchmark.qph == 123.4
        # The benchmark record is never A's latest run.
        assert storage.get_latest_run_for_deployment("dep-a").run_id == "a-001"


class TestQueryScope:
    """The query path (``_query.query``) must scope its "append to latest run"
    step by deployment name so the wrong deployment's record is not read or
    rewritten."""

    def test_query_for_a_returns_a_not_b(self, tmp_path: Path) -> None:
        storage, _a_path, _b_path = _make_two_deployments(tmp_path)
        cfg = _fake_cfg("dep-a")
        latest = storage.get_latest_run_for_deployment(cfg.name)
        assert latest is not None
        assert latest.run_id == "a-001"
        assert latest.deployment_name == "dep-a"

    def test_rewriting_a_record_is_refused(self, tmp_path: Path) -> None:
        """``query`` no longer writes into a record; and a writer that tried
        (not the owning run) is refused by ``save_run``, both records intact."""
        from lakebench.metrics import QueryMetrics
        from lakebench.metrics.storage import RecordExistsError

        storage, a_path, b_path = _make_two_deployments(tmp_path)
        a_before, b_before = _snapshot(a_path), _snapshot(b_path)

        latest = storage.get_latest_run_for_deployment("dep-a")
        assert latest is not None and latest.run_id == "a-001"
        latest.queries.append(
            QueryMetrics(
                query_name="count",
                query_text="SELECT 1",
                elapsed_seconds=0.5,
                rows_returned=1,
                success=True,
            )
        )
        with pytest.raises(RecordExistsError):
            storage.save_run(latest)

        assert _snapshot(a_path) == a_before
        assert _snapshot(b_path) == b_before
        assert not list(a_path.parent.glob("*.tmp"))


# ---------------------------------------------------------------------------
# Generator scope: the report renderer takes a deployment_name too
# ---------------------------------------------------------------------------


class TestGeneratorScope:
    """``ReportGenerator.generate_report(deployment_name=...)`` scopes the
    "latest run" lookup so ``lakebench report --render`` under parallel
    deployments cannot render another deployment's record."""

    def test_generator_latest_is_scoped(self, tmp_path: Path) -> None:
        from lakebench.reports.generator import ReportGenerator

        runs_dir = tmp_path / "runs"
        _storage, _a_path, _b_path = _make_two_deployments(tmp_path)
        gen = ReportGenerator(runs_dir)

        # Without a run_id the generator picks the latest -- scoped to dep-a
        # this must be a-001, not the newer b-001.
        report_path = gen.generate_report(deployment_name="dep-a")
        assert report_path.exists()
        html = report_path.read_text()
        assert "a-001" in html
        assert "b-001" not in html

    def test_generator_none_is_unscoped(self, tmp_path: Path) -> None:
        """Backwards-compat: ``deployment_name=None`` matches the previous
        unscoped behaviour."""
        from lakebench.reports.generator import ReportGenerator

        runs_dir = tmp_path / "runs"
        _storage, _a_path, _b_path = _make_two_deployments(tmp_path)
        gen = ReportGenerator(runs_dir)

        report_path = gen.generate_report()
        html = report_path.read_text()
        # B is newer, so the unscoped default is B.
        assert "b-001" in html


# ---------------------------------------------------------------------------
# Callsite wire-up guard (LB-safety net)
#
# Behavioural tests above cover the helper and the generator. The four
# unscoped callsites the brief calls out (two in _query.py, one in
# generator.py, one in cli/__init__.py:report) sit behind heavy CLI plumbing
# and are impractical to drive end-to-end here. A source-level guard catches
# a revert of the wire-up itself: if a future patch reintroduces a bare
# ``get_latest_run()`` at one of these callsites, this test fails.
# ---------------------------------------------------------------------------


class TestCallsiteWireup:
    """Fail loudly if any of the four scoped callsites are reverted to the
    unscoped ``get_latest_run()`` call."""

    def _source_of(self, module: str) -> str:
        import importlib
        import inspect

        return inspect.getsource(importlib.import_module(module))

    def test_query_module_has_no_unscoped_latest_calls(self) -> None:
        src = self._source_of("lakebench.cli._query")
        # Both callsites in this file (query and benchmark paths) must use
        # the scoped helper. No bare get_latest_run( calls should remain --
        # the helper itself lives in lakebench.metrics.storage.
        assert "storage.get_latest_run(" not in src, (
            "cli/_query.py has an unscoped storage.get_latest_run() call -- "
            "the scope-by-deployment wire-up was reverted at query or "
            "benchmark path"
        )
        # Both scoped calls must be present (regex tolerates a trailing
        # writable=True flag on the write path).
        import re

        scoped = re.findall(
            r"get_latest_run_for_deployment\(\s*cfg\.name\s*(?:,\s*writable\s*=\s*True\s*)?\)",
            src,
        )
        assert len(scoped) >= 2, (
            "cli/_query.py should scope both benchmark and query paths by "
            "cfg.name; one of the two sites is missing"
        )
        # And both write callsites must pass writable=True so the legacy
        # fallback cannot let a query/benchmark rewrite another
        # deployment's legacy record.
        writable = re.findall(
            r"get_latest_run_for_deployment\(\s*cfg\.name\s*,\s*writable\s*=\s*True\s*\)",
            src,
        )
        assert len(writable) >= 2, (
            "cli/_query.py write callsites must pass writable=True to "
            "prevent legacy-fallback rewrite of another deployment's record"
        )

    def test_generator_scopes_latest_lookup(self) -> None:
        src = self._source_of("lakebench.reports.generator")
        assert "self.storage.get_latest_run(" not in src, (
            "reports/generator.py generate_report() has an unscoped "
            "get_latest_run() call -- the deployment_name path was reverted"
        )
        assert "get_latest_run_for_deployment(deployment_name)" in src, (
            "reports/generator.py should route through "
            "get_latest_run_for_deployment(deployment_name)"
        )

    def test_report_cli_scopes_latest_lookup(self) -> None:
        src = self._source_of("lakebench.cli")
        # The two callsites inside the `report()` command (default action and
        # --render --summary) plus the one inside `results()` must all use
        # the scoped helper. Any bare storage.get_latest_run() call in this
        # module is a regression.
        assert "storage.get_latest_run(" not in src, (
            "cli/__init__.py has an unscoped storage.get_latest_run() call "
            "-- one of the report/results scope wire-ups was reverted"
        )
