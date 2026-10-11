"""Tests for `lakebench reproduce`.

The reproduce contract is documented in docs/development.md#reproduction-packages. The
interesting failures here are silent ones: a package that promotes a
performance metric into the correctness band would let a real bug pass; a
package that misses a stage would silently omit the reproduction of that
stage's cost. These tests pin the classification and the exit-code shape
that the CLI relies on.
"""

from __future__ import annotations

from datetime import datetime
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
    reproduce,
)
from tests.conftest import stub_experiment
from tests.fixtures.reproduce_helpers import _ONE_SAMPLE_CFG as _ONE_SAMPLE_CFG
from tests.fixtures.reproduce_helpers import _metrics as _metrics
from tests.fixtures.reproduce_helpers import _pb as _pb
from tests.fixtures.reproduce_helpers import _stage as _stage

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Compare: exit-code shape (0 / 1 / 2)
# ---------------------------------------------------------------------------


class TestCompare:
    @pytest.mark.parametrize(
        ("expected", "actual", "tolerances", "mode", "code", "status"),
        [
            (
                {"time_to_value_seconds": 100.0},
                {"time_to_value_seconds": 150.0},
                None,
                None,
                1,
                None,
            ),
            # correctness has zero tolerance, and eclipses any performance drift
            ({"scale_ratio": 1.0}, {"scale_ratio": 0.9}, None, None, 2, None),
            (
                {"scale_ratio": 1.0, "time_to_value_seconds": 100.0},
                {"scale_ratio": 0.9, "time_to_value_seconds": 150.0},
                None,
                None,
                2,
                None,
            ),
            ({"time_to_value_seconds": 100.0}, {}, None, None, 1, "missing"),
            ({"scale_ratio": 1.0}, {}, None, None, 2, None),
            # the correctness band is two-sided
            ({"scale_ratio": 1.0}, {"scale_ratio": 2.0}, None, None, 2, None),
            ({"ingest_ratio": 1.0}, {"ingest_ratio": 1.5}, None, None, 2, None),
            # an inflated correctness tolerance handed to _compare is ignored
            (
                {"scale_ratio": 1.0},
                {"scale_ratio": 1.15},
                {"performance": 20.0, "correctness": 20.0},
                None,
                2,
                None,
            ),
            # an expected 0 freshness measured as 100 s is a regression, not a match
            (
                {"data_freshness_seconds": 0.0},
                {"data_freshness_seconds": 100.0},
                None,
                None,
                1,
                None,
            ),
            ({"ingest_ratio": 1.0}, {}, {}, "sustained", 2, "missing"),
            ({"ingest_ratio": 1.0}, {"ingest_ratio": 1.08}, {}, "sustained", 2, "fail"),
            ({"ingest_ratio": 1.0}, {"ingest_ratio": 0.94}, {}, "sustained", 2, "fail"),
            # two honest runs of one corpus differ by a small ratio: inside the
            # guard band it passes
            ({"ingest_ratio": 1.0167}, {"ingest_ratio": 1.0339}, {}, "sustained", 0, "pass"),
        ],
    )
    def test_outcome(self, expected, actual, tolerances, mode, code, status):
        kw = {"mode": mode} if mode else {}
        rows, exit_code = _compare(
            expected, actual, DEFAULT_TOLERANCES if tolerances is None else tolerances, **kw
        )
        assert exit_code == code
        if status:
            assert rows[0]["status"] == status


# ---------------------------------------------------------------------------
# Package: build, load, drift-detect
# ---------------------------------------------------------------------------


class TestLoadPackage:
    @pytest.mark.parametrize(
        "meta",
        [
            {"expected_numbers": {"x": "fast"}},
            {"expected_numbers": {"time_to_value_seconds": float("nan")}},
            {"expected_numbers": {"time_to_value_seconds": float("inf")}},
            {
                "expected_numbers": {"time_to_value_seconds": 100.0},
                "tolerance_pct": {"performance": float("nan")},
            },
            {
                "expected_numbers": {"time_to_value_seconds": 100.0},
                "tolerance_pct": {"performance": -10.0},
            },
            # a correctness tolerance above zero would let wrong answers pass
            {
                "expected_numbers": {"scale_ratio": 1.0},
                "tolerance_pct": {"performance": 20.0, "correctness": 20.0},
            },
        ],
    )
    def test_bad_package_is_refused_at_load(self, tmp_path, meta):
        p = tmp_path / "bad.yaml"
        p.write_text(
            yaml.safe_dump({"schema_version": SCHEMA_VERSION, "reproduction_metadata": meta})
        )
        with pytest.raises(ReproduceError):
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

    def test_commit_is_lakebench_not_the_callers_repo(self, tmp_path, monkeypatch):
        """Run from inside another git repo, the commit compared is the
        running lakebench's, not that repo's HEAD (a false drift)."""
        import subprocess

        from lakebench.cli._reproduce import _current_commit_sha
        from lakebench.metrics import provenance

        git = ["git", "-c", "user.name=t", "-c", "user.email=t@example.com"]
        subprocess.run([*git, "init", "-q", str(tmp_path)], check=True)
        subprocess.run(
            [*git, "-C", str(tmp_path), "commit", "-q", "--allow-empty", "-m", "x"], check=True
        )
        other = subprocess.run(
            ["git", "-C", str(tmp_path), "rev-parse", "--short=7", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        monkeypatch.chdir(tmp_path)
        expected = provenance.sample().get("git_sha")
        assert _current_commit_sha() == (expected[:7] if expected else None)
        assert _current_commit_sha() != other


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


# ---------------------------------------------------------------------------
# Second-pass adversarial-review findings (2026-09-21, R1-R5).
# ---------------------------------------------------------------------------


class TestBenchmarkSampleCount:
    """A package's QpH must be verified with the same samples per query."""

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


def _drop_identity(pkg):
    del pkg["reproduction_metadata"]["experiment_identity"]


@pytest.mark.parametrize(
    ("cfg_text", "mutate"),
    [
        # default iterations is 3; the package carries one sample per query
        pytest.param("name: x\n", None, id="other-sample-count"),
        pytest.param(_ONE_SAMPLE_CFG, _drop_identity, id="legacy-package"),
    ],
)
def test_verify_refuses_before_running(tmp_path, cfg_text, mutate):
    """A package that cannot be verified is refused before the pipeline runs,
    not after hours of it."""
    (tmp_path / "cfg.yaml").write_text(cfg_text)
    pkg = _build_package(_metrics(), config_reference="cfg.yaml", commit_sha="abc")
    if mutate:
        mutate(pkg)
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


@pytest.mark.parametrize("role", [None, "calibration", "evaluation"])
def test_package_records_corpus_role(role):
    """The package carries the stored record's corpus role: it decides
    whether a package is verify-only."""
    from lakebench.cli._reproduce import _build_package

    metrics = _stored("212900-5105a0")
    metrics.experiment["corpus"]["corpus_role"] = role
    pkg = _build_package(metrics, config_reference=None, commit_sha="abc1234")
    assert pkg["reproduction_metadata"]["corpus_role"] == role


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


def _stub_protected(monkeypatch, protected):
    """Held-out seeds by role (test values), in place of the hash record."""
    from lakebench.config import datagen_seed

    held = dict(protected)
    monkeypatch.setattr(datagen_seed, "_heldout", lambda: SimpleNamespace(spent=frozenset()))
    monkeypatch.setattr(datagen_seed, "heldout_role", lambda s, h=None: held.get(s))
    monkeypatch.setattr(datagen_seed, "recorded_seeds", lambda path=None: frozenset())


def _unreadable(monkeypatch):
    from lakebench.config import datagen_seed

    def broken(path=None):
        raise FileNotFoundError("aml_registered_looks.json")

    monkeypatch.setattr(datagen_seed, "load_looks", broken)


@pytest.mark.parametrize(
    ("looks", "spent", "meta", "expected"),
    [
        pytest.param(
            [],
            {321},
            {"experiment_identity": {"workload": "financial", "seed": 321}},
            ("verify", False),
            id="roleless-spent-seed",
        ),
        pytest.param(
            [{"role": "evaluation", "seed": 777, "state": "started"}],
            {777},
            {"experiment_identity": {"workload": "financial", "seed": 777}},
            ("verify", True),
            id="roleless-financial-with-a-look",
        ),
        pytest.param(
            None,
            (),
            {"experiment_identity": {"workload": "financial", "seed": 43}},
            ("refuse", None),
            id="unreadable-record-financial",
        ),
        pytest.param(
            None,
            (),
            {
                "corpus_role": "calibration",
                "experiment_identity": {"workload": "financial", "seed": 43},
            },
            ("refuse", None),
            id="unreadable-record-calibration",
        ),
        pytest.param(
            None,
            (),
            {"experiment_identity": {"workload": "customer360", "seed": 42}},
            None,
            id="unreadable-record-other-workload",
        ),
    ],
)
def test_spent_look_decision(monkeypatch, looks, spent, meta, expected):
    """Verify-only for a spent or looked-at seed, refused when the look
    record cannot be read (a financial package), ordinary otherwise."""
    from lakebench.cli._reproduce import _spent_look

    if looks is None:
        _unreadable(monkeypatch)
    else:
        _stub_looks(monkeypatch, looks, spent)
        _stub_protected(monkeypatch, {})
    got = _spent_look(meta)
    if expected is None:
        assert got is None
    else:
        assert got[0] == expected[0]
        if expected[0] == "verify":
            assert (got[1] is not None) is expected[1]


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
