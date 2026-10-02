"""Tests for A2b: raw-``success`` readers wired to prefer ``verdict.status``.

Owner decision OD-6: v1.6 records get ``success == (verdict.status ==
"PASSED")``. A record without a verdict block is a legacy v1.5 record and
falls back to raw ``success``. LB-044 (the CLI exits 0 with silver never
keeping pace) is the archetypal record where the raw flag is True but the
verdict is FAILED; the wired readers must catch it.

Also covers the c360 correctness verdict gate wired into
``compute_verdict``: a gated c360 check that failed or did not run seals a
FAILED verdict via the ``c360`` gate; a failure outside the gating list
does not.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from lakebench.metrics.collector import (
    BenchmarkMetrics,
    JobMetrics,
    PipelineMetrics,
)
from lakebench.metrics.storage import MetricsStorage
from lakebench.metrics.verdict import (
    compute_verdict,
    has_verdict,
    passed,
    verdict_status,
)

FIXTURES = Path(__file__).parent / "fixtures" / "verdict"

FIXTURE_NAMES = (
    "run-20260925-104452-21bf3a.metrics.json",
    "run-20260925-135005-4b7a97.metrics.json",
)


# ---------------------------------------------------------------------------
# Helpers under test
# ---------------------------------------------------------------------------


class TestHasVerdict:
    def test_true_when_block_present_with_status(self) -> None:
        assert has_verdict({"verdict": {"status": "PASSED"}})
        assert has_verdict({"verdict": {"status": "FAILED", "reasons": ["x"]}})

    def test_false_on_legacy_record(self) -> None:
        assert not has_verdict({"success": True})
        assert not has_verdict({})

    def test_false_on_malformed_block(self) -> None:
        # A block without a string status is not a usable verdict.
        assert not has_verdict({"verdict": {}})
        assert not has_verdict({"verdict": {"status": None}})
        assert not has_verdict({"verdict": None})

    def test_none_input(self) -> None:
        assert not has_verdict(None)


class TestVerdictStatus:
    def test_returns_status_when_present(self) -> None:
        assert verdict_status({"verdict": {"status": "PASSED"}}) == "PASSED"
        assert verdict_status({"verdict": {"status": "FAILED"}}) == "FAILED"

    def test_returns_none_when_absent(self) -> None:
        assert verdict_status({"success": True}) is None
        assert verdict_status(None) is None


class TestPassed:
    def test_prefers_verdict_over_success(self) -> None:
        """LB-044 shape: raw success True, verdict FAILED -> not passed."""
        rec = {"success": True, "verdict": {"status": "FAILED"}}
        assert not passed(rec)

    def test_prefers_verdict_over_success_passed(self) -> None:
        rec = {"success": False, "verdict": {"status": "PASSED"}}
        assert passed(rec)

    def test_falls_back_to_success_when_no_verdict(self) -> None:
        assert passed({"success": True})
        assert not passed({"success": False})

    def test_missing_success_defaults_to_false(self) -> None:
        assert not passed({})

    def test_none_input(self) -> None:
        assert not passed(None)


# ---------------------------------------------------------------------------
# Site 1: reports.generator badge computation
# ---------------------------------------------------------------------------


def _make_passing_metrics() -> PipelineMetrics:
    return PipelineMetrics(
        run_id="test-run",
        deployment_name="test-deploy",
        start_time=datetime(2026, 9, 27, tzinfo=timezone.utc),
        end_time=datetime(2026, 9, 27, 0, 1, tzinfo=timezone.utc),
        total_elapsed_seconds=60.0,
        success=True,
        jobs=[
            JobMetrics(
                job_name="lakebench-bronze-verify",
                job_type="bronze-verify",
                success=True,
            )
        ],
    )


class TestReportGeneratorBadge:
    def test_badge_failed_when_verdict_failed(self) -> None:
        """A c360-failed run has success=True but the verdict is FAILED;
        the badge computed by the report generator must agree."""
        from lakebench.reports.generator import ReportGenerator

        m = _make_passing_metrics()
        # A c360 failure fails the verdict but does not set any of the
        # badge-only inputs, so the pass/fail bit here comes from the
        # verdict, not the badge's own reason list.
        m.c360_correctness = {
            "status": "fail",
            "failed": ["silver_to_gold_days"],
            "reason": "silver differs from gold",
        }
        rg = ReportGenerator.__new__(ReportGenerator)
        rg._fallback_output_dir = None
        passed_bit, reasons, _warnings = rg._compute_overall_status(m)
        assert passed_bit is False
        assert reasons, "FAILED badge must carry a reason"

    def test_badge_passed_on_happy_path(self) -> None:
        from lakebench.reports.generator import ReportGenerator

        m = _make_passing_metrics()
        rg = ReportGenerator.__new__(ReportGenerator)
        rg._fallback_output_dir = None
        passed_bit, reasons, _warnings = rg._compute_overall_status(m)
        assert passed_bit is True
        assert reasons == []


# ---------------------------------------------------------------------------
# Site 2: CLI ``report --list`` status column
# ---------------------------------------------------------------------------


class TestCliReportListRow:
    """The ``lakebench report --list`` status column reads through
    ``metrics.verdict.passed``. These tests exercise the helper directly
    for the shape check, plus one integration test that goes end-to-end
    through ``MetricsStorage.list_runs`` + the CLI's actual reader."""

    def test_verdict_failed_reads_failed_even_when_success_true(self) -> None:
        summary = {"success": True, "verdict": {"status": "FAILED"}}
        assert not passed(summary)

    def test_legacy_success_true_reads_passed(self) -> None:
        summary = {"success": True}
        assert passed(summary)

    def test_legacy_success_false_reads_failed(self) -> None:
        summary = {"success": False}
        assert not passed(summary)

    def test_list_runs_summary_carries_verdict(self, tmp_path: Path) -> None:
        """MetricsStorage.list_runs must surface the verdict block so the
        CLI's status column can read it. Without this, the wire in
        cli/__init__.py has nothing to prefer over ``success``."""
        run_id = "20260925-000000-abcxyz"
        run_dir = tmp_path / f"run-{run_id}"
        run_dir.mkdir(parents=True)
        (run_dir / "metrics.json").write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "deployment_name": "d",
                    "start_time": "2026-09-25T00:00:00+00:00",
                    "total_elapsed_seconds": 1.0,
                    "success": True,
                    "verdict": {
                        "status": "FAILED",
                        "reasons": ["fabricated"],
                        "gates": {},
                        "qualifiers": {},
                    },
                }
            )
        )
        storage = MetricsStorage(tmp_path)
        runs = storage.list_runs()
        assert len(runs) == 1
        summary = runs[0]
        # The verdict must be carried through; passed() then reads FAILED.
        assert summary.get("verdict", {}).get("status") == "FAILED"
        assert not passed(summary), "the summary row must read FAILED via the verdict wire"


# ---------------------------------------------------------------------------
# Site 3: perf_gate ``run_refusals`` and ``latest_candidate``
# ---------------------------------------------------------------------------


class TestPerfGateSuccessReaders:
    """The perf gate reads run success at two sites: ``run_refusals`` (the
    first refusal reason a comparable run is refused on) and
    ``latest_candidate`` (the newest passing run of the pinned config
    picked to gate on). Both must read the persisted verdict when present
    and fall back to raw ``success`` for legacy records."""

    @staticmethod
    def _write_run(dir_path: Path, raw: dict[str, Any]) -> Path:
        rid = raw.get("run_id") or "test-run"
        run_dir = dir_path / f"run-{rid}"
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "metrics.json").write_text(json.dumps(raw))
        return run_dir / "metrics.json"

    def _make_raw(
        self,
        *,
        run_id: str,
        success: bool,
        verdict_status_value: str | None,
    ) -> dict[str, Any]:
        raw: dict[str, Any] = {
            "run_id": run_id,
            "deployment_name": "d",
            "start_time": "2026-09-27T00:00:00+00:00",
            "success": success,
            "total_elapsed_seconds": 60.0,
        }
        if verdict_status_value is not None:
            raw["verdict"] = {
                "status": verdict_status_value,
                "reasons": [] if verdict_status_value == "PASSED" else ["fabricated"],
                "gates": {},
                "qualifiers": {},
            }
        return raw

    def test_run_refusals_flags_verdict_failed(self, tmp_path: Path) -> None:
        """LB-044 shape: raw success True, verdict FAILED. run_refusals
        must include "run did not succeed" as a reason."""
        from lakebench.metrics import perf_gate as pg

        raw = self._make_raw(run_id="lb044", success=True, verdict_status_value="FAILED")
        self._write_run(tmp_path, raw)
        run = pg.load_run(tmp_path / "run-lb044")

        # Build a PinnedConfig that only tests the ``success`` branch
        # without triggering fingerprint / policy comparisons.
        class _Pinned:
            mode = run.mode
            fingerprint: dict[str, Any] = {}
            fingerprint_hash = ""
            file_sha256 = ""

        reasons = pg.run_refusals(run, _Pinned())  # type: ignore[arg-type]
        assert "run did not succeed" in reasons

    def test_run_refusals_legacy_success_true_not_flagged(self, tmp_path: Path) -> None:
        """Legacy v1.5 record (no verdict block) with success=True: the
        success reader must not flag it."""
        from lakebench.metrics import perf_gate as pg

        raw = self._make_raw(run_id="legacy-pass", success=True, verdict_status_value=None)
        self._write_run(tmp_path, raw)
        run = pg.load_run(tmp_path / "run-legacy-pass")

        class _Pinned:
            mode = run.mode
            fingerprint: dict[str, Any] = {}
            fingerprint_hash = ""
            file_sha256 = ""

        reasons = pg.run_refusals(run, _Pinned())  # type: ignore[arg-type]
        assert "run did not succeed" not in reasons

    def test_run_refusals_legacy_success_false_flagged(self, tmp_path: Path) -> None:
        from lakebench.metrics import perf_gate as pg

        raw = self._make_raw(run_id="legacy-fail", success=False, verdict_status_value=None)
        self._write_run(tmp_path, raw)
        run = pg.load_run(tmp_path / "run-legacy-fail")

        class _Pinned:
            mode = run.mode
            fingerprint: dict[str, Any] = {}
            fingerprint_hash = ""
            file_sha256 = ""

        reasons = pg.run_refusals(run, _Pinned())  # type: ignore[arg-type]
        assert "run did not succeed" in reasons

    def test_latest_candidate_prefers_verdict(self, tmp_path: Path) -> None:
        """A newer run with verdict FAILED must NOT be chosen as the
        latest candidate over an older one with verdict PASSED, even
        when the newer's raw success flag is True."""
        from lakebench.metrics import perf_gate as pg

        # Older passing run, newer FAILED-verdict run (same fingerprint).
        old_raw = self._make_raw(
            run_id="20260101-000000-oldabc",
            success=True,
            verdict_status_value="PASSED",
        )
        new_raw = self._make_raw(
            run_id="20260201-000000-newxyz",
            success=True,
            verdict_status_value="FAILED",
        )
        self._write_run(tmp_path, old_raw)
        self._write_run(tmp_path, new_raw)

        class _Pinned:
            mode = "batch"
            fingerprint_hash = ""
            fingerprint: dict[str, Any] = {}

        # Bypass fingerprint filtering: hash always matches.
        import lakebench.metrics.perf_gate as pg_mod

        original_hash = pg_mod.fingerprint_hash
        original_not_current = pg_mod.not_current
        pg_mod.fingerprint_hash = lambda fp: ""  # type: ignore[assignment]
        pg_mod.not_current = lambda p: None  # type: ignore[assignment]
        try:
            picked = pg.latest_candidate(_Pinned(), tmp_path)  # type: ignore[arg-type]
        finally:
            pg_mod.fingerprint_hash = original_hash  # type: ignore[assignment]
            pg_mod.not_current = original_not_current  # type: ignore[assignment]
        assert picked is not None
        assert picked.run_id.endswith("oldabc"), (
            f"expected the passing older run, got {picked.run_id}"
        )


# ---------------------------------------------------------------------------
# Site 4: compare comparability ladder
# ---------------------------------------------------------------------------


class TestCompareVerdictRefusal:
    def test_verdict_failed_side_refuses_comparison(self) -> None:
        from lakebench.cli._compare import _build_comparison

        failed = {
            "run_id": "run-a",
            "success": True,
            "verdict": {"status": "FAILED", "reasons": ["ingest saturated"]},
            "pipeline_benchmark": {"scores": {"composite_qph": 100}},
        }
        good = {
            "run_id": "run-b",
            "success": True,
            "verdict": {"status": "PASSED"},
            "pipeline_benchmark": {"scores": {"composite_qph": 200}},
        }
        comparison = _build_comparison("a", failed, "b", good)
        assert comparison["verdict"] == "not_comparable"
        provenance = comparison["refusals"]["provenance"]
        assert any("did not pass its verdict" in r for r in provenance)
        # The raw scores are still visible; the invariant is that no delta
        # is drawn from them.
        for row in comparison["metrics"]:
            assert row.get("not_comparable") is True

    def test_legacy_success_true_still_admitted(self) -> None:
        """A v1.5 record with success=True and no verdict block is a legacy
        record; the compare readers must accept it under the fallback."""
        from lakebench.cli._compare import _build_comparison

        a = {"run_id": "run-a", "success": True}
        b = {"run_id": "run-b", "success": True}
        comparison = _build_comparison("a", a, "b", b)
        # Nothing here reads success -> FAILED, so no verdict-based refusal.
        refused = comparison["refusals"]["provenance"]
        assert not any("did not pass its verdict" in r for r in refused)

    def test_success_field_is_never_removed(self) -> None:
        """A2b hard requirement: the raw ``success`` field survives on
        every record it touches (compare is read-only over its inputs)."""
        from lakebench.cli._compare import _build_comparison

        rec = {"run_id": "run-a", "success": True, "verdict": {"status": "FAILED"}}
        _build_comparison("a", rec, "b", {})
        assert "success" in rec  # not popped by the caller


# ---------------------------------------------------------------------------
# c360 gate: a c360-failed run seals a FAILED verdict via the c360 gate
# ---------------------------------------------------------------------------


def _c360_record(
    fail: tuple[str, ...] = (),
    unchecked: tuple[str, ...] = (),
    drop: tuple[str, ...] = (),
    extra: tuple[str, ...] = ("interaction_mix", "high_churn_share"),
    facts: bool = True,
) -> dict[str, Any]:
    """A c360_correctness record as evaluate_run builds it: every gated
    check plus ``extra`` (non-gated) checks, each passing unless named."""
    from lakebench.metrics import c360_correctness as c3

    ids = sorted(c3.GATING_CHECKS) + list(extra)
    checks = []
    for cid in ids:
        if cid in drop:
            continue
        ok: bool | None = None if cid in unchecked else cid not in fail
        checks.append(c3._check(cid, "invariant", ok, 1, 0))
    rec = c3.verdict(checks if facts else [], "" if facts else "no [c360-check] line")
    rec["facts_present"] = facts
    return rec


class TestC360Gate:
    """The verdict's c360 gate applies the owner-approved gating list
    (checks 0-14 and 17), through c360_correctness.gating_outcome."""

    def test_failing_gated_check_reads_failed(self) -> None:
        m = _make_passing_metrics()
        m.c360_correctness = _c360_record(fail=("silver_to_gold_counts",))
        v = compute_verdict(m)
        assert v.status == "FAILED"
        assert v.gates.get("c360") == "FAIL"
        assert any("silver_to_gold_counts fail" in r for r in v.reasons)

    def test_only_nongating_failures_pass(self) -> None:
        """A statistical miss outside the list is shown, never a FAIL. With
        the old gate (any status == "fail") this read FAILED."""
        m = _make_passing_metrics()
        m.c360_correctness = _c360_record(fail=("interaction_mix",))
        assert m.c360_correctness["status"] == "fail"
        v = compute_verdict(m)
        assert v.status == "PASSED"
        assert "c360" not in v.gates
        assert v.qualifiers.get("c360_failed_not_gating") == ["interaction_mix"]

    def test_unchecked_gated_check_reads_failed(self) -> None:
        m = _make_passing_metrics()
        m.c360_correctness = _c360_record(unchecked=("bronze_rows_match_datagen",))
        assert m.c360_correctness["status"] == "unknown"
        v = compute_verdict(m)
        assert v.status == "FAILED"
        assert any("bronze_rows_match_datagen unchecked" in r for r in v.reasons)

    def test_absent_gated_check_reads_failed(self) -> None:
        m = _make_passing_metrics()
        m.c360_correctness = _c360_record(drop=("avg_transaction_value_overall",))
        v = compute_verdict(m)
        assert v.status == "FAILED"
        assert any("avg_transaction_value_overall not evaluated" in r for r in v.reasons)

    def test_record_without_facts_reads_failed(self) -> None:
        """Fail closed: a c360 run whose gold-finalize logged no facts is not
        a pass (it used to read PASSED as "unknown")."""
        m = _make_passing_metrics()
        m.c360_correctness = _c360_record(facts=False)
        assert m.c360_correctness["status"] == "unknown"
        v = compute_verdict(m)
        assert v.status == "FAILED"
        assert v.gates.get("c360") == "FAIL"
        assert any("no expected-result facts" in r for r in v.reasons)

    def test_c360_pass_does_not_add_gate(self) -> None:
        m = _make_passing_metrics()
        m.c360_correctness = _c360_record()
        v = compute_verdict(m)
        assert "c360" not in v.gates
        assert "c360_failed_not_gating" not in v.qualifiers
        assert v.status == "PASSED"

    def test_c360_absent_scoped_out(self) -> None:
        m = _make_passing_metrics()
        m.c360_correctness = None
        v = compute_verdict(m)
        assert "c360" not in v.gates
        assert v.status == "PASSED"

    def test_reporting_only_record_never_fails(self) -> None:
        m = _make_passing_metrics()
        m.c360_correctness = dict(_c360_record(fail=("silver_to_gold_days",)), reporting_only=True)
        v = compute_verdict(m)
        assert "c360" not in v.gates
        assert v.status == "PASSED"

    def test_empty_gating_list_is_reporting_only(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from lakebench.metrics import c360_correctness as c3

        rec = _c360_record(fail=("silver_to_gold_days",))
        monkeypatch.setattr(c3, "GATING_CHECKS", frozenset())
        m = _make_passing_metrics()
        m.c360_correctness = rec
        v = compute_verdict(m)
        assert "c360" not in v.gates
        assert v.qualifiers.get("c360_failed_not_gating") == ["silver_to_gold_days"]

    def test_gated_shape_judged_only_with_benchmark_checks(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """As in the CLI, a gated benchmark shape is judged only when the
        benchmark ran (the record holds shape checks)."""
        from lakebench.metrics import c360_correctness as c3

        rec = _c360_record()
        monkeypatch.setattr(c3, "GATING_CHECKS", c3.GATING_CHECKS | {"benchmark_rows_Q1"})
        m = _make_passing_metrics()
        m.c360_correctness = rec
        assert compute_verdict(m).status == "PASSED"
        rec = dict(
            rec, checks=rec["checks"] + [c3._check("benchmark_rows_Q2", "shape", True, 1, 1)]
        )
        m.c360_correctness = rec
        v = compute_verdict(m)
        assert v.status == "FAILED"
        assert any("benchmark_rows_Q1 not evaluated" in r for r in v.reasons)

    def test_stored_record_nongating_miss_passes(self) -> None:
        """A stored scale-10 record (5105a0) recomputes PASSED, and still
        PASSED with a statistical check flipped to fail; FAILED with a
        gated one flipped."""
        import copy

        rec = json.loads(
            (
                Path(__file__).parent / "fixtures/records/run-20260929-212900-5105a0/metrics.json"
            ).read_text()
        )
        storage = MetricsStorage.__new__(MetricsStorage)
        assert compute_verdict(storage._dict_to_metrics(rec)).status == "PASSED"
        for cid, want in (("duplicate_filter_share", "PASSED"), ("dates_in_window", "FAILED")):
            r = copy.deepcopy(rec)
            for c in r["c360_correctness"]["checks"]:
                if c["id"] == cid:
                    c["status"] = "fail"
            r["c360_correctness"]["status"] = "fail"
            assert compute_verdict(storage._dict_to_metrics(r)).status == want, cid


# ---------------------------------------------------------------------------
# Fixture regression: both LB-044-shape fixtures must read FAILED at every
# wired site now that they carry a verdict block.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fixture_records() -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for name in FIXTURE_NAMES:
        fixture = FIXTURES / name
        assert fixture.exists(), f"Fixture missing: {fixture}"
        records.append(json.loads(fixture.read_text()))
    return records


class TestFixturesReadFailed:
    def test_fixtures_carry_verdict_failed(self, fixture_records: list[dict[str, Any]]) -> None:
        for r in fixture_records:
            assert r.get("success") is True, "fixture invariant: raw success is True"
            assert has_verdict(r), "fixture invariant: verdict block present (A2b wire-in)"
            assert verdict_status(r) == "FAILED"

    def test_persisted_verdict_matches_fresh_compute(
        self, fixture_records: list[dict[str, Any]]
    ) -> None:
        """Persisted fixture verdict blocks must match what compute_verdict
        would produce today. Catches silent drift when the badge rules or
        compute_verdict changes but the static fixtures are not regenerated.
        """
        from lakebench.metrics.verdict import compute_verdict

        for r in fixture_records:
            storage = MetricsStorage.__new__(MetricsStorage)
            m = storage._dict_to_metrics(r)
            fresh = compute_verdict(m).to_dict()
            persisted = r["verdict"]
            assert fresh["status"] == persisted["status"], (
                f"verdict drift for {r.get('run_id')}: "
                f"persisted status={persisted['status']!r}, "
                f"fresh status={fresh['status']!r}. "
                "Regenerate the fixture verdict block or fix the drift."
            )
            if fresh["status"] == "FAILED":
                assert fresh["reasons"], "FAILED verdict must carry reasons"

    def test_badge_reads_failed(self, fixture_records: list[dict[str, Any]]) -> None:
        from lakebench.reports.generator import ReportGenerator

        rg = ReportGenerator.__new__(ReportGenerator)
        rg._fallback_output_dir = None
        for r in fixture_records:
            storage = MetricsStorage.__new__(MetricsStorage)
            m = storage._dict_to_metrics(r)
            passed_bit, reasons, _warnings = rg._compute_overall_status(m)
            assert passed_bit is False
            assert reasons

    def test_report_list_reads_failed(self, fixture_records: list[dict[str, Any]]) -> None:
        for r in fixture_records:
            summary = {"success": r["success"], "verdict": r["verdict"]}
            assert not passed(summary)

    def test_perf_gate_run_refusals_reads_failed(
        self, fixture_records: list[dict[str, Any]], tmp_path: Path
    ) -> None:
        from lakebench.metrics import perf_gate as pg

        for i, r in enumerate(fixture_records):
            run_id = f"fixture-{i}"
            run_dir = tmp_path / f"run-{run_id}"
            run_dir.mkdir(parents=True, exist_ok=True)
            copy = dict(r)
            copy["run_id"] = run_id
            (run_dir / "metrics.json").write_text(json.dumps(copy))
            run = pg.load_run(run_dir)

            class _Pinned:
                mode = run.mode
                fingerprint: dict[str, Any] = {}
                fingerprint_hash = ""
                file_sha256 = ""

            reasons = pg.run_refusals(run, _Pinned())  # type: ignore[arg-type]
            assert "run did not succeed" in reasons


# ---------------------------------------------------------------------------
# The raw ``success`` field is never removed at any site (invariant).
# ---------------------------------------------------------------------------


def test_success_survives_verdict_roundtrip(tmp_path: Path) -> None:
    """Writing a run through the collector keeps the raw success field
    alongside verdict.status. The wired readers prefer verdict but the
    raw flag must stay for legacy record comparisons and for auditors."""
    m = PipelineMetrics(
        run_id="rt",
        deployment_name="d",
        start_time=datetime(2026, 9, 27, tzinfo=timezone.utc),
        end_time=datetime(2026, 9, 27, 0, 1, tzinfo=timezone.utc),
        total_elapsed_seconds=60.0,
        success=True,
        jobs=[JobMetrics(job_name="j", job_type="bronze-verify", success=False)],
    )
    # A c360 fail alongside a job fail; both must surface without losing
    # the raw success bit.
    m.c360_correctness = {"status": "fail", "failed": ["x"], "reason": "boom"}
    d = m.to_dict()
    assert "success" in d
    assert d["success"] is True
    assert d["verdict"]["status"] == "FAILED"
    assert d["verdict"]["gates"].get("c360") == "FAIL"


def test_benchmark_query_success_still_read_locally() -> None:
    """A single benchmark query's own ``success`` field is not touched
    by A2b: it means the query returned rows, not the run's overall
    verdict. The verdict fails when any of them is false."""
    m = _make_passing_metrics()
    m.benchmark = BenchmarkMetrics(
        mode="power",
        cache="hot",
        scale=1.0,
        qph=1.0,
        total_seconds=1.0,
        queries=[{"name": "q1", "success": False, "elapsed_seconds": 1.0}],
    )
    v = compute_verdict(m)
    assert v.status == "FAILED"
    assert v.gates.get("benchmark") == "FAIL"
