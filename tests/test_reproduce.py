"""Tests for `lakebench reproduce`.

The reproduce contract is documented in docs/deep-dive/reproduce.md. The
interesting failures here are silent ones: a package that promotes a
performance metric into the correctness band would let a real bug pass; a
package that misses a stage would silently omit the reproduction of that
stage's cost. These tests pin the classification and the exit-code shape
that the CLI relies on.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest import mock

import pytest
import typer
import yaml

from lakebench.cli._reproduce import (
    DEFAULT_TOLERANCES,
    SCHEMA_VERSION,
    ReproduceError,
    _build_package,
    _compare,
    _extract_expected_numbers,
    _load_package,
    _resolve_config_path,
    reproduce,
)
from lakebench.metrics.maintenance_policy import MAINTENANCE_POLICY_ID
from tests.conftest import stub_experiment

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# The fixture packages carry one sample per query, so the verify config must
# ask for one or the sample-count check refuses before anything else runs.
_ONE_SAMPLE_CFG = "name: x\narchitecture:\n  benchmark:\n    iterations: 1\n"


def _stage(name: str, elapsed: float) -> SimpleNamespace:
    return SimpleNamespace(stage_name=name, elapsed_seconds=elapsed)


def _pb(**overrides):
    """Build a PipelineBenchmark-shaped SimpleNamespace with sensible defaults."""
    defaults = {
        "pipeline_mode": "batch",
        "time_to_value_seconds": 405.0,
        "pipeline_throughput_gb_per_second": 0.030,
        "compute_efficiency_gb_per_core_hour": 1.2,
        "scale_ratio": 0.992,
        "data_freshness_seconds": None,
        "sustained_throughput_rps": 0.0,
        "ingest_ratio": 0.0,
        "post_compaction_qph": 0.0,
        "query_benchmark": SimpleNamespace(qph=1305.8),
        "stages": [
            _stage("bronze-verify", 240.0),
            _stage("silver-build", 90.0),
            _stage("gold-finalize", 75.0),
        ],
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _metrics(**overrides):
    defaults = {
        "run_id": "20260920-210120-9d4b92",
        "deployment_name": "c360-scale-0-1",
        "pipeline_benchmark": _pb(),
        "config_snapshot": {"name": "c360-scale-0-1", "scale": 0.1},
        # A run from this code carries the current policy (PipelineMetrics default).
        "maintenance_policy_id": MAINTENANCE_POLICY_ID,
        "experiment": stub_experiment(["Q1"]),
        "datagen_fleet": {
            "pods_reported": 2,
            "aggregate_mbps": 154.4,
            "cpu_hr_per_tb": 14.36,
            "data_quality": "complete",
        },
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# Extraction: expected numbers must classify cleanly per mode
# ---------------------------------------------------------------------------


class TestExtractExpectedNumbers:
    def test_time_to_value_never_treated_as_stage_seconds(self):
        """time_to_value_seconds is a pipeline-level score, not a stage duration."""
        pb = _pb(stages=[_stage("bronze", 100.0)])
        got = _extract_expected_numbers(_metrics(pipeline_benchmark=pb))
        # Both live in the same numbers dict but must classify separately.
        assert got["time_to_value_seconds"] == 405.0
        assert got["bronze_seconds"] == 100.0

    def test_post_compaction_qph_beats_query_benchmark_qph(self):
        pb = _pb(
            post_compaction_qph=2000.0,
            query_benchmark=SimpleNamespace(qph=1305.8),
        )
        got = _extract_expected_numbers(_metrics(pipeline_benchmark=pb))
        assert got["composite_qph"] == 2000.0


# ---------------------------------------------------------------------------
# Classification: correctness metrics must never drop into performance band
# ---------------------------------------------------------------------------


class TestClassification:
    # ---------------------------------------------------------------------------
    # Drift math + banding
    # ---------------------------------------------------------------------------

    pass


class TestDriftMath:
    pass


class TestBandingAsymmetry:
    """Faster-than-expected is not a regression on a lower-is-better score.
    Slower-than-expected is."""


# ---------------------------------------------------------------------------
# Compare: exit-code shape (0 / 1 / 2)
# ---------------------------------------------------------------------------


class TestCompare:
    def test_performance_drift_exits_1(self):
        expected = {"time_to_value_seconds": 100.0}
        actual = {"time_to_value_seconds": 150.0}
        _rows, exit_code = _compare(expected, actual, DEFAULT_TOLERANCES)
        assert exit_code == 1

    def test_correctness_drift_exits_2(self):
        """scale_ratio drift has zero tolerance -- any drift trips correctness."""
        expected = {"scale_ratio": 1.0}
        actual = {"scale_ratio": 0.9}
        _rows, exit_code = _compare(expected, actual, DEFAULT_TOLERANCES)
        assert exit_code == 2

    def test_correctness_drift_beats_performance_drift(self):
        """A correctness violation must eclipse any performance drift in the
        exit code -- fixing perf while breaking correctness is not a pass."""
        expected = {"scale_ratio": 1.0, "time_to_value_seconds": 100.0}
        actual = {"scale_ratio": 0.9, "time_to_value_seconds": 150.0}
        _rows, exit_code = _compare(expected, actual, DEFAULT_TOLERANCES)
        assert exit_code == 2

    def test_missing_actual_is_a_failure(self):
        expected = {"time_to_value_seconds": 100.0}
        rows, exit_code = _compare(expected, {}, DEFAULT_TOLERANCES)
        assert exit_code == 1
        assert rows[0]["status"] == "missing"

    def test_missing_correctness_metric_exits_2(self):
        expected = {"scale_ratio": 1.0}
        _rows, exit_code = _compare(expected, {}, DEFAULT_TOLERANCES)
        assert exit_code == 2


# ---------------------------------------------------------------------------
# Package: build, load, drift-detect
# ---------------------------------------------------------------------------


class TestBuildPackage:
    pass


class TestLoadPackage:
    def test_wrong_schema_version_refused(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(
            yaml.safe_dump(
                {
                    "schema_version": 999,
                    "reproduction_metadata": {"expected_numbers": {"x": 1.0}},
                }
            )
        )
        with pytest.raises(ReproduceError, match="schema_version"):
            _load_package(p)

    def test_non_numeric_expected_refused(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(
            yaml.safe_dump(
                {
                    "schema_version": SCHEMA_VERSION,
                    "reproduction_metadata": {"expected_numbers": {"x": "fast"}},
                }
            )
        )
        with pytest.raises(ReproduceError, match="must be a number"):
            _load_package(p)


class TestResolveConfigPath:
    # ---------------------------------------------------------------------------
    # CLI: record mode and verify --dry-run
    # ---------------------------------------------------------------------------

    pass


class TestReproduceCli:
    # ---------------------------------------------------------------------------
    # Utc time on the package is timezone-aware -- silent tz drift would corrupt
    # recorded_at comparisons across machines.
    # ---------------------------------------------------------------------------

    pass


class TestRecordedAtTimezone:
    # ---------------------------------------------------------------------------
    # Regressions for adversarial-review findings (2026-09-21).
    # Each test names the finding number it defends. Do not delete one without
    # confirming the defect it names cannot recur.
    # ---------------------------------------------------------------------------

    pass


class TestF1CorrectnessBandTwoSided:
    """Correctness band must fail on drift in EITHER direction. A scale_ratio
    of 2.0 vs expected 1.0 is just as broken as 0.5 -- both mean the pipeline
    processed the wrong amount of data."""

    def test_scale_ratio_above_one_fails(self):
        from lakebench.cli._reproduce import DEFAULT_TOLERANCES, _compare

        _rows, exit_code = _compare({"scale_ratio": 1.0}, {"scale_ratio": 2.0}, DEFAULT_TOLERANCES)
        assert exit_code == 2

    def test_ingest_ratio_above_one_fails(self):
        from lakebench.cli._reproduce import DEFAULT_TOLERANCES, _compare

        _rows, exit_code = _compare(
            {"ingest_ratio": 1.0}, {"ingest_ratio": 1.5}, DEFAULT_TOLERANCES
        )
        assert exit_code == 2


class TestF2NonFiniteValuesRejected:
    """NaN and Infinity in expected_numbers or tolerance_pct would silently
    pass every comparison. Reject them at load time."""

    def test_nan_expected_rejected(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(
            yaml.safe_dump(
                {
                    "schema_version": SCHEMA_VERSION,
                    "reproduction_metadata": {
                        "expected_numbers": {"time_to_value_seconds": float("nan")}
                    },
                }
            )
        )
        with pytest.raises(ReproduceError, match="finite"):
            _load_package(p)

    def test_infinity_expected_rejected(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(
            "schema_version: 1\n"
            "reproduction_metadata:\n"
            "  expected_numbers:\n"
            "    time_to_value_seconds: .inf\n"
        )
        with pytest.raises(ReproduceError, match="finite"):
            _load_package(p)

    def test_nan_tolerance_rejected(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(
            "schema_version: 1\n"
            "reproduction_metadata:\n"
            "  expected_numbers:\n"
            "    time_to_value_seconds: 100.0\n"
            "  tolerance_pct:\n"
            "    performance: .nan\n"
        )
        with pytest.raises(ReproduceError, match="finite"):
            _load_package(p)

    def test_negative_tolerance_rejected(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(
            yaml.safe_dump(
                {
                    "schema_version": SCHEMA_VERSION,
                    "reproduction_metadata": {
                        "expected_numbers": {"time_to_value_seconds": 100.0},
                        "tolerance_pct": {"performance": -10.0},
                    },
                }
            )
        )
        with pytest.raises(ReproduceError, match="non-negative"):
            _load_package(p)


class TestF3CommitDriftIsCorrectnessFailure:
    """A reproduce against a different commit measures a different code path.
    Refuse by default; allow only with --allow-commit-drift."""

    def test_commit_drift_exits_requirement_unmet_by_default(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text(_ONE_SAMPLE_CFG)
        pkg = _build_package(_metrics(), config_reference="cfg.yaml", commit_sha="AAA1111")
        pkg_path = tmp_path / "pkg.yaml"
        pkg_path.write_text(yaml.safe_dump(pkg))

        with mock.patch("lakebench.cli._reproduce._current_commit_sha", return_value="BBB2222"):
            with pytest.raises(typer.Exit) as exc:
                reproduce(package=pkg_path, dry_run=True)
            assert exc.value.exit_code == 14  # requirement unmet (CLI-1; 2 in 1.6)


class TestF4RunFingerprintingSurvivesConcurrentRuns:
    """The reproduce must pick its own run, not a concurrent run that finished
    faster. Filter by deployment_name and start_time watermark."""

    def test_picks_matching_deployment_after_watermark(self):
        from lakebench.cli._reproduce import _find_reproduce_run

        class FakeStorage:
            def list_runs(self):
                # A concurrent unrelated run started earlier and finished
                # after the watermark. It has a later start_time. We must
                # not pick it.
                return [
                    {
                        "run_id": "unrelated-later",
                        "deployment_name": "somebody-elses-config",
                        "start_time": "2026-09-21T05:00:00+00:00",
                    },
                    {
                        "run_id": "ours",
                        "deployment_name": "my-config",
                        "start_time": "2026-09-21T04:30:00+00:00",
                    },
                    {
                        "run_id": "old-mine",
                        "deployment_name": "my-config",
                        "start_time": "2026-09-21T03:00:00+00:00",
                    },
                ]

            def load_run(self, run_id):
                return f"loaded:{run_id}"

        watermark = datetime.fromisoformat("2026-09-21T04:00:00+00:00")
        got = _find_reproduce_run(FakeStorage(), "my-config", watermark)
        assert got == "loaded:ours"

    def test_no_matching_run_errors(self):
        from lakebench.cli._reproduce import _find_reproduce_run

        class FakeStorage:
            def list_runs(self):
                return [
                    {
                        "run_id": "old-mine",
                        "deployment_name": "my-config",
                        "start_time": "2026-09-21T03:00:00+00:00",
                    },
                ]

            def load_run(self, run_id):
                return None

        watermark = datetime.fromisoformat("2026-09-21T04:00:00+00:00")
        with pytest.raises(ReproduceError, match="No new"):
            _find_reproduce_run(FakeStorage(), "my-config", watermark)


class TestNoPreRunDestroy:
    """reproduce never destroys before its run (SAF-1): it refuses an
    existing namespace or bucket instead (tests/test_saf1_reproduce.py), and
    destroys at the end only the incarnation it deployed, unless --keep."""

    def _run(self, tmp_path, keep):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text("name: my-config\n")
        call_log: list[tuple[str, object]] = []

        def track(name):
            def _fn(*a, **kw):
                call_log.append((name, kw.get("expected_incarnation", kw.get("nonce"))))
                return kw.get("nonce")

            return _fn

        class FakeStorage:
            metrics_dir = tmp_path

            def list_runs(self):
                return [
                    {
                        "run_id": "produced",
                        "deployment_name": "my-config",
                        "start_time": datetime.now(timezone.utc).isoformat(),
                    }
                ]

            def load_run(self, rid):
                return SimpleNamespace(run_id=rid)

        fake_cfg = SimpleNamespace(
            name="my-config",
            get_namespace=lambda: "my-config",
            architecture=SimpleNamespace(pipeline=SimpleNamespace(cycles=1)),
        )
        with (
            mock.patch("lakebench.cli._destroy.destroy", track("destroy-command")),
            mock.patch("lakebench.cli._destroy._destroy_impl", track("destroy")),
            mock.patch("lakebench.cli._deploy._deploy_impl", track("deploy")),
            mock.patch("lakebench.cli._generate.generate", track("generate")),
            mock.patch("lakebench.cli._run.run", track("run")),
            mock.patch("lakebench.cli._reproduce._refuse_existing"),
            # run's argument rules read a real config (tests/test_run_args.py).
            mock.patch("lakebench.cli._run_args.validate_run_args"),
            mock.patch("lakebench.aml.look_guard.refuse_if_protected"),
            mock.patch(
                "lakebench.cli._reproduce._own_incarnation",
                side_effect=lambda cfg, path, own, **k: f"uid#{own}",
            ),
            mock.patch("lakebench.cli._helpers.journal_open"),
            mock.patch("lakebench.config.load_config", return_value=fake_cfg),
            mock.patch("lakebench.metrics.MetricsStorage", return_value=FakeStorage()),
        ):
            from lakebench.cli._reproduce import _run_pipeline

            _run_pipeline(cfg, timeout=None, keep=keep)
        return call_log


class TestF6RecordRequiresCorrectnessMetric:
    """A source run without scale_ratio (batch) or ingest_ratio (sustained)
    would publish a package with no correctness gate -- every reproduce would
    exit 0 against genuinely broken pipelines."""

    def test_batch_without_scale_ratio_refused(self):
        pb = _pb(scale_ratio=0.0)  # zero -> dropped -> missing
        with pytest.raises(ReproduceError, match="scale_ratio"):
            _build_package(
                _metrics(pipeline_benchmark=pb),
                config_reference=None,
                commit_sha=None,
            )

    def test_sustained_without_ingest_ratio_refused(self):
        pb = _pb(
            pipeline_mode="sustained",
            time_to_value_seconds=0.0,
            pipeline_throughput_gb_per_second=0.0,
            scale_ratio=0.0,
            data_freshness_seconds=12.0,
            sustained_throughput_rps=1000.0,
            ingest_ratio=0.0,  # missing
        )
        with pytest.raises(ReproduceError, match="ingest_ratio"):
            _build_package(
                _metrics(pipeline_benchmark=pb),
                config_reference=None,
                commit_sha=None,
            )

    def test_sustained_with_ingest_ratio_accepted(self):
        pb = _pb(
            pipeline_mode="sustained",
            time_to_value_seconds=0.0,
            pipeline_throughput_gb_per_second=0.0,
            scale_ratio=0.0,
            data_freshness_seconds=12.0,
            sustained_throughput_rps=1000.0,
            ingest_ratio=0.99,
        )
        pkg = _build_package(
            _metrics(pipeline_benchmark=pb),
            config_reference=None,
            commit_sha=None,
        )
        assert pkg["reproduction_metadata"]["expected_numbers"]["ingest_ratio"] == 0.99


class TestF7DirectionTableCompleteness:
    """Every enumerated performance metric must have an explicit direction
    entry. A missing entry historically silently defaulted to higher-is-better,
    which would treat a 10x latency regression as a pass."""


class TestF8ConfigOverrideExists:
    """A --config typo used to blow up minutes later inside the deployer."""

    def test_missing_override_raises_at_resolve(self, tmp_path):
        pkg = {"reproduction_metadata": {"config_reference": None}}
        with pytest.raises(ReproduceError, match="does not exist"):
            _resolve_config_path(pkg, tmp_path / "typo.yml", tmp_path / "pkg.yaml")


# ---------------------------------------------------------------------------
# Second-pass adversarial-review findings (2026-09-21, R1-R5).
# ---------------------------------------------------------------------------


class TestR1CorrectnessToleranceCannotBeInflated:
    """A package's tolerance_pct.correctness must not be usable to widen
    the correctness gate above zero. That would rebuild F1 -- a hostile
    or hand-edited package could quietly re-legitimise data loss."""

    def test_correctness_tolerance_above_zero_refused_at_load(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text(
            yaml.safe_dump(
                {
                    "schema_version": SCHEMA_VERSION,
                    "reproduction_metadata": {
                        "expected_numbers": {"scale_ratio": 1.0},
                        "tolerance_pct": {"performance": 20.0, "correctness": 20.0},
                    },
                }
            )
        )
        with pytest.raises(ReproduceError, match="correctness"):
            _load_package(p)

    def test_compare_ignores_correctness_tolerance_from_package(self):
        """Belt-and-braces: even if a caller bypasses _load_package and
        hands _compare an inflated correctness tolerance, _compare must
        still gate correctness at zero."""
        from lakebench.cli._reproduce import _compare

        _rows, exit_code = _compare(
            {"scale_ratio": 1.0},
            {"scale_ratio": 1.15},
            {"performance": 20.0, "correctness": 20.0},  # 15% drift < 20% tol
        )
        assert exit_code == 2


class TestR3NaiveLocalTimestampsHandled:
    """MetricsCollector.start_run stores naive local datetimes. Labelling
    them as UTC shifts them by the host offset. Treat naive as local."""


class TestR4CommitShaLengthNormalisation:
    """Package can carry a full 40-char SHA; git rev-parse returns 7-char.
    Naive equality would false-positive drift for the same commit."""


class TestR5LegitZeroFreshnessPreserved:
    """A sustained pipeline with instant freshness (0.0) is a legitimate
    measurement, not missing data."""

    def test_zero_expected_regressing_to_nonzero_fails(self):
        """A freshness expected=0 measured as 100s is a real regression,
        not a match. _drift_pct returns inf; _is_over_band trips."""
        from lakebench.cli._reproduce import _compare

        _rows, exit_code = _compare(
            {"data_freshness_seconds": 0.0},
            {"data_freshness_seconds": 100.0},
            DEFAULT_TOLERANCES,
        )
        assert exit_code == 1


class TestBenchmarkSampleCount:
    """A package's QpH must be verified with the same samples per query (LB-150)."""

    @staticmethod
    def _qb(samples):
        q = {"name": "Q1", "elapsed_seconds": 2.0, "success": True}
        if samples:
            q["samples"] = [2.0] * samples
        return SimpleNamespace(qph=1800.0, queries=[q])

    def test_mismatch_is_refused_and_match_passes(self):
        from lakebench.cli._reproduce import _sample_mismatch

        meta = {"pipeline_mode": "batch", "expected_numbers": {"composite_qph": 100.0}}
        # A package without the key predates repeats: one sample.
        assert "iterations: 1" in _sample_mismatch(meta, 3)
        assert _sample_mismatch(meta, 1) is None
        meta["benchmark_samples_per_query"] = 3
        assert _sample_mismatch(meta, 3) is None
        assert _sample_mismatch(meta, None) is None
        # No QpH to compare, or a continuous package: nothing to refuse.
        assert _sample_mismatch({"expected_numbers": {"scale_ratio": 1.0}}, 1) is None
        sustained = dict(meta, pipeline_mode="sustained")
        assert _sample_mismatch(sustained, 1) is None


def test_verify_refuses_config_with_other_sample_count_before_running(tmp_path):
    """The check fires before the pipeline, not after hours of it (LB-150)."""
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text("name: x\n")  # default iterations: 3
    pkg = _build_package(_metrics(), config_reference="cfg.yaml", commit_sha="abc")
    pkg_path = tmp_path / "pkg.yaml"
    pkg_path.write_text(yaml.safe_dump(pkg))
    with (
        mock.patch("lakebench.cli._reproduce._current_commit_sha", return_value="abc"),
        mock.patch(
            "lakebench.cli._reproduce._run_pipeline",
            side_effect=AssertionError("must refuse before running"),
        ),
        pytest.raises(typer.Exit) as exc,
    ):
        reproduce(package=pkg_path)
    assert exc.value.exit_code == 2


class TestExperimentChecks:
    """A reproduce verifies the same experiment returned the same results
    (invariant 1) before any number is compared."""

    def _pkg(self, metrics=None):
        return _build_package(metrics or _metrics(), config_reference="c.yaml", commit_sha="abc")

    def test_different_results_refuse(self):
        from lakebench.benchmark.fingerprint import fingerprint_rows
        from lakebench.cli._reproduce import _experiment_refusal

        meta = self._pkg(_metrics(experiment=stub_experiment(["Q1"])))["reproduction_metadata"]
        other = stub_experiment(["Q1"])
        other["results"]["fingerprints"]["Q1"] = fingerprint_rows([("Q1", 2)])
        why = _experiment_refusal(meta, _metrics(experiment=other))
        assert why and "Q1 results not shown equal" in why

    def test_different_seed_refuses(self):
        from lakebench.cli._reproduce import _experiment_refusal

        meta = self._pkg()["reproduction_metadata"]
        why = _experiment_refusal(meta, _metrics(experiment=stub_experiment(seed=7)))
        assert why and "seed differs" in why

    def test_source_run_without_provenance_cannot_be_packaged(self):
        with pytest.raises(ReproduceError, match="no provenance"):
            self._pkg(_metrics(experiment=None))

    def test_legacy_package_refused_before_running(self, tmp_path):
        cfg = tmp_path / "cfg.yaml"
        cfg.write_text(_ONE_SAMPLE_CFG)
        pkg = _build_package(_metrics(), config_reference="cfg.yaml", commit_sha="abc")
        del pkg["reproduction_metadata"]["experiment_identity"]
        pkg_path = tmp_path / "pkg.yaml"
        pkg_path.write_text(yaml.safe_dump(pkg))
        with (
            mock.patch("lakebench.cli._reproduce._current_commit_sha", return_value="abc"),
            mock.patch(
                "lakebench.cli._reproduce._run_pipeline",
                side_effect=AssertionError("must refuse before running"),
            ),
            pytest.raises(typer.Exit) as exc,
        ):
            reproduce(package=pkg_path)
        assert exc.value.exit_code == 2


# ---------------------------------------------------------------------------
# EVD-11 (ER-13): ingest_ratio is a range guard, config-bound values are not
# packaged, a registered look is verify-only
# ---------------------------------------------------------------------------


def _stored(run_id):
    from tests.fixtures import stored_records as sr

    return sr.load_metrics(run_id)


def test_honest_continuous_rerun_passes():
    """A package from e338c5 (ingest_ratio 1.0167) against 095006's 1.0339:
    two honest runs of one corpus; it failed as an exact correctness check."""
    from lakebench.cli._reproduce import _compare, _extract_expected_numbers

    expected = _extract_expected_numbers(_stored("011043-e338c5"))
    actual = _extract_expected_numbers(_stored("095006-71b4a3"))
    rows, _outcome = _compare(
        {"ingest_ratio": expected["ingest_ratio"]},
        {"ingest_ratio": actual["ingest_ratio"]},
        {},
        mode="sustained",
    )
    assert rows[0]["band"] == "guard" and rows[0]["status"] == "pass"
    assert _outcome == 0


@pytest.mark.parametrize("value", [1.08, 0.94])
def test_ratio_outside_range_fails(value):
    from lakebench.cli._reproduce import _compare

    rows, outcome = _compare({"ingest_ratio": 1.0}, {"ingest_ratio": value}, {}, mode="sustained")
    assert rows[0]["status"] == "fail" and outcome == 2


def test_missing_ratio_fails():
    from lakebench.cli._reproduce import _compare

    rows, outcome = _compare({"ingest_ratio": 1.0}, {}, {}, mode="sustained")
    assert rows[0]["status"] == "missing" and outcome == 2


def test_package_records_corpus_role():
    from lakebench.cli._reproduce import _build_package

    pkg = _build_package(_stored("212900-5105a0"), config_reference=None, commit_sha="abc1234")
    assert "corpus_role" in pkg["reproduction_metadata"]


def _look_package(tmp_path, role, seed, workload="financial"):
    pkg = {
        "schema_version": 1,
        "reproduction_metadata": {
            "commit_sha": "unknown",
            "pipeline_mode": "batch",
            "corpus_role": role,
            "expected_numbers": {"scale_ratio": 1.0},
            "experiment_identity": {"workload": workload, "seed": seed},
        },
    }
    p = tmp_path / "pkg.yaml"
    p.write_text(yaml.safe_dump(pkg))
    return p


def _stub_looks(monkeypatch, looks, spent=()):
    from lakebench.config import datagen_seed

    monkeypatch.setattr(datagen_seed, "load_looks", lambda path=None: list(looks))
    monkeypatch.setattr(datagen_seed, "spent_seeds", lambda: frozenset(spent))


def _no_run(monkeypatch):
    import lakebench.cli._reproduce as r

    def boom(*a, **k):
        raise AssertionError("a registered look must never run the pipeline")

    monkeypatch.setattr(r, "_run_pipeline", boom)
    monkeypatch.setattr(r.subprocess, "run", boom)


def test_spent_look_verify_only(tmp_path, monkeypatch, capsys):
    import hashlib

    from lakebench.cli._reproduce import _verify

    report = tmp_path / "report.json"
    report.write_text('{"look": "done"}')
    digest = hashlib.sha256(report.read_bytes()).hexdigest()
    seed = 987654
    _stub_looks(
        monkeypatch,
        [{"role": "evaluation", "seed": seed, "state": "complete", "report_sha256": digest}],
        {seed},
    )
    _no_run(monkeypatch)
    pkg = _look_package(tmp_path, "evaluation", seed)
    _verify(pkg, None, None, False, False, False, report=report)  # exit 0: returns
    with pytest.raises(typer.Exit) as e:
        _verify(pkg, None, None, False, False, False, report=None)
    assert e.value.exit_code == 2
    other = tmp_path / "other.json"
    other.write_text("{}")
    with pytest.raises(typer.Exit) as e:
        _verify(pkg, None, None, False, False, False, report=other)
    assert e.value.exit_code == 14
    out = capsys.readouterr()
    assert str(seed) not in out.out + out.err


def test_unspent_held_out_package_refused(tmp_path, monkeypatch):
    from lakebench.cli._reproduce import _verify

    _stub_looks(monkeypatch, [], ())
    _no_run(monkeypatch)
    with pytest.raises(typer.Exit) as e:
        _verify(_look_package(tmp_path, "robustness", 555), None, None, False, False, False)
    assert e.value.exit_code == 3


def test_roleless_financial_package_with_a_look_is_verify_only(tmp_path, monkeypatch):
    from lakebench.cli._reproduce import _verify

    _stub_looks(monkeypatch, [{"role": "evaluation", "seed": 777, "state": "started"}], {777})
    _no_run(monkeypatch)
    with pytest.raises(typer.Exit) as e:
        _verify(_look_package(tmp_path, None, 777), None, None, False, False, False, report=None)
    assert e.value.exit_code == 2


def test_ordinary_package_is_not_a_look(tmp_path, monkeypatch):
    from lakebench.cli._reproduce import _spent_look

    _stub_looks(monkeypatch, [], ())
    _stub_protected(monkeypatch, {999: "evaluation"})
    meta = {
        "corpus_role": "calibration",
        "experiment_identity": {"workload": "financial", "seed": 43},
    }
    assert _spent_look(meta) is None
    # A roleless financial package on an ordinary seed runs as before.
    meta = {"corpus_role": None, "experiment_identity": {"workload": "financial", "seed": 43}}
    assert _spent_look(meta) is None
    meta = {"corpus_role": None, "experiment_identity": {"workload": "customer360", "seed": 42}}
    assert _spent_look(meta) is None


def test_report_flag_refused_for_an_ordinary_package(tmp_path, monkeypatch):
    from lakebench.cli._reproduce import _verify

    _stub_looks(monkeypatch, [], ())
    with pytest.raises(typer.Exit) as e:
        _verify(
            _look_package(tmp_path, "calibration", 43),
            None,
            None,
            False,
            False,
            False,
            report=tmp_path / "x",
        )
    assert e.value.exit_code == 2


def _stub_protected(monkeypatch, protected):
    """Held-out seeds by role (test values), in place of the hash record."""
    from lakebench.config import datagen_seed

    held = dict(protected)
    monkeypatch.setattr(datagen_seed, "_heldout", lambda: SimpleNamespace(spent=frozenset()))
    monkeypatch.setattr(datagen_seed, "heldout_role", lambda s, h=None: held.get(s))
    monkeypatch.setattr(datagen_seed, "recorded_seeds", lambda path=None: frozenset())


def test_roleless_spent_seed_is_verify_only(monkeypatch):
    from lakebench.cli._reproduce import _spent_look

    _stub_looks(monkeypatch, [], {321})
    _stub_protected(monkeypatch, {})
    meta = {"experiment_identity": {"workload": "financial", "seed": 321}}
    assert _spent_look(meta) == ("verify", None)


def test_burned_seed_package_is_refused(monkeypatch):
    from lakebench.cli._reproduce import _spent_look

    burned = {"role": "evaluation", "seed": 555, "state": "burned", "reason": "public"}
    _stub_looks(monkeypatch, [burned], {555})
    _stub_protected(monkeypatch, {555: "evaluation"})
    meta = {
        "corpus_role": "evaluation",
        "experiment_identity": {"workload": "financial", "seed": 555},
    }
    verdict = _spent_look(meta)
    assert verdict[0] == "refuse" and "burned" in verdict[1] and "555" not in verdict[1]
    # A completed look beside a burn (it cannot happen, but) is still verified.
    done = {"role": "evaluation", "seed": 555, "state": "complete", "report_sha256": "a" * 64}
    _stub_looks(monkeypatch, [burned, done], {555})
    assert _spent_look(meta) == ("verify", done)


def test_role_read_from_the_identity(monkeypatch):
    from lakebench.cli._reproduce import _spent_look

    _stub_looks(monkeypatch, [], ())
    _stub_protected(monkeypatch, {})
    meta = {
        "experiment_identity": {"workload": "financial", "seed": 5, "corpus role": "evaluation"}
    }
    assert _spent_look(meta)[0] == "refuse"


def test_unreadable_look_record_refuses_financial(monkeypatch):
    from lakebench.cli._reproduce import _spent_look
    from lakebench.config import datagen_seed

    def broken(path=None):
        raise FileNotFoundError("aml_registered_looks.json")

    monkeypatch.setattr(datagen_seed, "load_looks", broken)
    meta = {"experiment_identity": {"workload": "financial", "seed": 43}}
    assert _spent_look(meta)[0] == "refuse"
    meta = {"experiment_identity": {"workload": "customer360", "seed": 42}}
    assert _spent_look(meta) is None


def test_config_naming_a_held_out_corpus_is_refused(monkeypatch):
    """The package may be ordinary while --config generates a held-out
    corpus: the config is checked too, through the look guard (exit 2)."""
    from lakebench.aml.look_guard import protected_corpus_reason

    _stub_looks(monkeypatch, [], ())
    _stub_protected(monkeypatch, {777: "evaluation"})

    def cfg(role=None, seed=None, schema="financial"):
        dg = SimpleNamespace(corpus_role=role, seed=seed)
        wl = SimpleNamespace(datagen=dg, schema_type=SimpleNamespace(value=schema))
        return SimpleNamespace(architecture=SimpleNamespace(workload=wl))

    assert protected_corpus_reason(cfg(role="evaluation"))
    assert protected_corpus_reason(cfg(seed=777))
    assert protected_corpus_reason(cfg(seed=43)) is None
    # Another workload with a held-out seed would publish it: refused too.
    assert protected_corpus_reason(cfg(seed=777, schema="customer360"))


def test_config_error_text_hides_held_out_seeds(monkeypatch):
    from lakebench.cli._reproduce import _redact_seed_text

    _stub_looks(monkeypatch, [], {321})
    _stub_protected(monkeypatch, {654: "robustness"})
    out = _redact_seed_text("seed 654 is held out; seed 321 is listed as spent; scale 10")
    assert "654" not in out and "321" not in out and "scale 10" in out


def test_stated_role_cannot_hide_a_held_out_seed(monkeypatch):
    """A package whose metadata says calibration while its identity holds a
    held-out seed (a hand edit) is still refused."""
    from lakebench.cli._reproduce import _spent_look

    _stub_looks(monkeypatch, [], ())
    _stub_protected(monkeypatch, {654: "evaluation"})
    meta = {
        "corpus_role": "calibration",
        "experiment_identity": {"workload": "financial", "seed": 654, "corpus role": "calibration"},
    }
    assert _spent_look(meta)[0] == "refuse"
    meta["experiment_identity"]["corpus role"] = "evaluation"
    meta["experiment_identity"]["seed"] = 1
    assert _spent_look(meta)[0] == "refuse"


def test_unreadable_record_refuses_a_calibration_package(monkeypatch):
    from lakebench.cli._reproduce import _spent_look
    from lakebench.config import datagen_seed

    def broken(path=None):
        raise FileNotFoundError("aml_registered_looks.json")

    monkeypatch.setattr(datagen_seed, "load_looks", broken)
    meta = {
        "corpus_role": "calibration",
        "experiment_identity": {"workload": "financial", "seed": 43},
    }
    assert _spent_look(meta)[0] == "refuse"
