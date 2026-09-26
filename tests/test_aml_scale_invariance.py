"""AML D8 scale invariance (AML-GOALS 5a D8, R7).

Two gate runs are produced by the real gate (fidelity_gate.evaluate_gate with
collect_outputs) on synthetic frames whose answer is known, written the way
scripts/aml_gate.py writes them, and compared by scale_invariance: the same
distribution passes; a shifted feature, a weaker typology signal, a missing
typology, a missing file, an underpowered typology or a provenance mismatch
fails. Closes D8's harness; the statistics helpers are checked against
scikit-learn and scipy.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")
pytest.importorskip("pyarrow")

from lakebench.aml import fidelity_gate as fg  # noqa: E402
from lakebench.aml import scale_invariance as si  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "src/lakebench/spark/data/aml/aml_preregistration.json"
SEED = 43


@pytest.fixture(autouse=True)
def _few_threads(monkeypatch):
    from threadpoolctl import threadpool_limits

    monkeypatch.setenv("LB_AML_GATE_JOBS", "2")
    with threadpool_limits(limits=2):
        yield


def _prereg_dict():
    """The real pre-registration with its D8 tolerances untouched, cut down to
    three features, one behavioural typology and fewer resamples."""
    p = copy.deepcopy(json.loads(PREREG.read_text()))
    p["features"] = ["planted", "noise_a", "noise_b"]
    p["behavioural_subset"] = ["beh"]
    p["definitional_subset"] = []
    p["level2"] = {**p["level2"], "n": 1, "k_in_band": 1}
    p["power"] = {**p["power"], "bootstrap_iterations": 200, "min_positives": 40}
    return p


@pytest.fixture
def prereg_path(tmp_path, monkeypatch):
    """The cut-down pre-registration, standing in for the packaged file."""
    path = tmp_path / "prereg.json"
    path.write_text(json.dumps(_prereg_dict()))
    monkeypatch.setattr(si, "_packaged_prereg_path", lambda: path)
    return path


def _frame(n, seed, signal=6.0, prev=0.05, shift=0.0, customers_per=1):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < prev).astype(int)
    return pd.DataFrame(
        {
            "group": np.arange(n) // customers_per,
            "month": np.arange(n) % customers_per,
            "planted": y * signal + rng.normal(0, 1, n),
            "noise_a": rng.normal(shift, 1, n),
            "noise_b": rng.normal(0, 1, n),
            "is_customer": True,
            "label:beh": y,
        }
    )


def _run(tmp_path, name, frame, prereg_path, *, scale, seed=SEED, mutate=None):
    """Score ``frame`` with the gate and write report + outputs like
    scripts/aml_gate.py does. Returns the report path."""
    prereg, sha = fg.load_preregistration(prereg_path)
    eps = prereg["corpora"]["entities_per_scale_unit"]
    rep = fg.evaluate_gate(
        frame,
        prereg,
        prereg_sha256=sha,
        provenance={
            "corpus_seed": seed,
            "corpus_seed_check": {"claimed_seed": seed, "matched_share": 1},
            "corpus_scale": {"n_entities": round(eps * scale), "scale": scale},
            "label_role": prereg["unit_of_scoring"]["label_role"],
            "aml_features_sha256": "f" * 64,
            "model_versions": ["test-model"],
        },
        collect_outputs=True,
    )
    fg.add_pass(rep, "corpus_fully_keyed", True)
    fg.add_pass(rep, "registered_label_role", True)
    outputs = rep.pop("_model_outputs")
    d = tmp_path / name
    d.mkdir()
    paths = fg.write_model_outputs(outputs, str(d / "gate"))
    fps = fg.output_fingerprints(outputs)
    rep["model_outputs"] = {k: {"path": v, "fingerprint": fps.get(k)} for k, v in paths.items()}
    if mutate:
        mutate(rep, paths)
    out = d / "gate.json"
    out.write_text(json.dumps(rep, default=str))
    return out


def _gate_scale():
    return _prereg_dict()["corpora"]["gate_scale"]


def _large_scale():
    return max(_prereg_dict()["density"]["scales"])


def _pair(tmp_path, prereg_path, small, large, **kw):
    a = _run(tmp_path, "small", small, prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", large, prereg_path, scale=_large_scale(), **kw)
    return si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))


# ---------------------------------------------------------------------------
# Known answers
# ---------------------------------------------------------------------------


def test_same_distribution_passes(tmp_path, prereg_path):
    v = _pair(tmp_path, prereg_path, _frame(6000, 1), _frame(15000, 2))
    assert v["verdict"] == "pass" and v["pass"] is True, v["errors"]
    r = v["typologies"]["beh"]
    lo, hi = r["diff_ci"]
    bound = _prereg_dict()["scale_invariance"]["ap_diff_abs_max"]
    assert -bound <= lo <= hi <= bound
    assert all(f["pass"] for f in v["features"].values())
    # Tolerances are the registered ones, and say where they came from.
    assert v["tolerances"]["ks_stat_max"] == _prereg_dict()["scale_invariance"]["ks_stat_max"]
    assert "aml_preregistration.json" in v["tolerances"]["source"]


def test_identical_run_passes(tmp_path, prereg_path):
    df = _frame(6000, 1)
    v = _pair(tmp_path, prereg_path, df, df.copy())
    assert v["pass"] is True, v["errors"]
    assert all(f["ks"] == 0 for f in v["features"].values())
    assert v["typologies"]["beh"]["ap_diff"] == 0


def test_shifted_feature_distribution_fails(tmp_path, prereg_path):
    v = _pair(tmp_path, prereg_path, _frame(6000, 1), _frame(15000, 2, shift=0.5))
    assert v["verdict"] == "fail" and v["pass"] is False
    assert v["features"]["noise_a"]["pass"] is False
    assert v["features"]["planted"]["pass"] is True
    assert v["checks"]["ks_all_features"] is False
    # The typology itself did not move.
    assert v["typologies"]["beh"]["pass"] is True


def test_weaker_typology_signal_at_scale_fails(tmp_path, prereg_path):
    v = _pair(tmp_path, prereg_path, _frame(6000, 1), _frame(15000, 2, signal=2.0))
    r = v["typologies"]["beh"]
    assert v["pass"] is False and r["pass"] is False and r["within_bounds"] is False
    assert r["diff_ci"][1] < 0


def test_missing_typology_fails(tmp_path, prereg_path):
    def drop(rep, paths):
        rep["typologies"].pop("beh")

    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale(), mutate=drop)
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is False
    assert any("missing" in w for w in v["typologies"]["beh"]["reasons"])


def test_typology_absent_from_scores_fails(tmp_path, prereg_path):
    def empty_scores(rep, paths):
        sc = pd.read_parquet(paths["oof_scores"])
        sc[sc["typology"] != "beh"].to_parquet(paths["oof_scores"], index=False)

    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(
        tmp_path,
        "large",
        _frame(15000, 2),
        prereg_path,
        scale=_large_scale(),
        mutate=empty_scores,
    )
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is False and v["typologies"]["beh"]["pass"] is False


def test_underpowered_typology_fails_never_passes(tmp_path, prereg_path):
    # About 15 positives, under min_positives = 40.
    v = _pair(tmp_path, prereg_path, _frame(300, 1), _frame(300, 2))
    r = v["typologies"]["beh"]
    assert v["pass"] is False and r["pass"] is False
    assert any("underpowered" in w for w in r["reasons"])


def test_missing_report_is_an_error(tmp_path, prereg_path):
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    v = si.evaluate_d8(str(a), str(tmp_path / "nope.json"), prereg_path=str(prereg_path))
    assert v["verdict"] == "error" and v["pass"] is False and v["errors"]


def test_missing_unit_features_is_an_error(tmp_path, prereg_path):
    def drop(rep, paths):
        rep["model_outputs"].pop("unit_features")

    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale(), mutate=drop)
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["verdict"] == "error" and v["pass"] is False
    assert "unit_features" in v["errors"][0]


def test_seed_mismatch_fails(tmp_path, prereg_path):
    v = _pair(tmp_path, prereg_path, _frame(6000, 1), _frame(15000, 2), seed=SEED + 1)
    assert v["pass"] is False and v["checks"]["same_seed"] is False


def test_small_not_at_gate_scale_fails(tmp_path, prereg_path):
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale() * 3)
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale())
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is False and v["checks"]["small_at_gate_scale"] is False


def test_changed_preregistration_fails(tmp_path, prereg_path):
    """A report scored under another pre-registration (a tolerance tuned after
    the fact) cannot be compared under this one."""
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale())
    p = _prereg_dict()
    p["scale_invariance"]["ap_diff_abs_max"] = p["scale_invariance"]["ap_diff_abs_max"] * 2
    other = tmp_path / "other.json"
    other.write_text(json.dumps(p))
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(other))
    assert v["pass"] is False
    assert any("different pre-registration" in e for e in v["errors"])


def test_report_verdict_not_ok_fails(tmp_path, prereg_path):
    def bad(rep, paths):
        rep["verdict"] = "error"

    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale(), mutate=bad)
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is False


def test_sampled_weighted_large_run_matches_full(tmp_path, prereg_path):
    """The cluster samples negatives by customer with weight 1/fraction; the
    weighted AP and KS must still see the same distribution."""
    full = _frame(30000, 2)
    rng = np.random.default_rng(5)
    frac = 0.5
    keep = (full["label:beh"] == 1) | (rng.random(len(full)) < frac)
    sampled = full[keep].copy()
    sampled["weight"] = np.where(sampled["label:beh"] == 1, 1.0, 1 / frac)
    v = _pair(tmp_path, prereg_path, _frame(6000, 1), sampled)
    assert v["pass"] is True, v["errors"]


# ---------------------------------------------------------------------------
# Statistics helpers against reference implementations
# ---------------------------------------------------------------------------


def test_resampler_ap_equals_sklearn_with_ties_and_integer_weights():
    from sklearn.metrics import average_precision_score

    rng = np.random.default_rng(0)
    n = 3000
    y = (rng.random(n) < 0.1).astype(int)
    score = np.round(y * 0.3 + rng.random(n), 2)  # many ties
    w = rng.integers(1, 4, n).astype(float)
    groups = np.arange(n) // 3
    r = si._APResampler(y, score, w, groups)
    assert r.ap() == pytest.approx(average_precision_score(y, score, sample_weight=w), abs=1e-12)
    # A resample by multiplicity equals the replicated rows.
    # (The resampler holds rows sorted by score; r.codes are its row groups.)
    mult = r.draw(np.random.default_rng(1))
    order = np.argsort(-score, kind="stable")
    ys, ss, ws = y[order], score[order], w[order]
    idx = np.repeat(np.arange(n), mult[r.codes].astype(int))
    want = average_precision_score(ys[idx], ss[idx], sample_weight=ws[idx])
    assert r.ap(mult) == pytest.approx(want, abs=1e-12)


def test_weighted_ks_matches_scipy_and_counts_nan_mass():
    from scipy.stats import ks_2samp

    rng = np.random.default_rng(0)
    a, b = rng.normal(0, 1, 2000), rng.normal(0.3, 1, 3000)
    got = si.weighted_ks(a, np.ones(len(a)), b, np.ones(len(b)))
    assert got == pytest.approx(ks_2samp(a, b).statistic, abs=1e-12)
    # Integer weights equal replication.
    wa = rng.integers(1, 3, len(a)).astype(float)
    rep = np.repeat(a, wa.astype(int))
    assert si.weighted_ks(a, wa, b, np.ones(len(b))) == pytest.approx(
        ks_2samp(rep, b).statistic, abs=1e-12
    )
    # Same finite values, 20% more undefined: the NaN mass is the difference.
    c = a.copy()
    c[: len(c) // 5] = np.nan
    assert si.weighted_ks(a, np.ones(len(a)), c, np.ones(len(c))) >= 0.19


def test_s3_uris_resolve_without_network():
    from pyarrow import fs

    fsys, path = si._resolve("s3a://lb-gold/aml/aml_gate_report.json", "http://127.0.0.1:9")
    assert isinstance(fsys, fs.S3FileSystem) and path == "lb-gold/aml/aml_gate_report.json"
    fsys, path = si._resolve("relative/gate.json", None)
    assert isinstance(fsys, fs.LocalFileSystem) and Path(path).is_absolute()


def test_cli_exit_codes(tmp_path, prereg_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location("aml_d8", ROOT / "scripts/aml_d8.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale())
    out = tmp_path / "d8.json"
    args = ["--small", str(a), "--large", str(b), "--prereg", str(prereg_path)]
    assert mod.main([*args, "--out", str(out)]) == 0
    assert json.loads(out.read_text())["verdict"] == "pass"
    missing = ["--small", str(a), "--large", str(tmp_path / "x.json")]
    assert mod.main([*missing, "--prereg", str(prereg_path)]) == 1
    c = tmp_path / "shifted"
    c.mkdir()
    s = _run(c, "large", _frame(15000, 2, shift=0.5), prereg_path, scale=_large_scale())
    assert mod.main(["--small", str(a), "--large", str(s), "--prereg", str(prereg_path)]) == 2


def test_verdict_records_report_hashes(tmp_path, prereg_path):
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale())
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["inputs"]["small"]["report_sha256"] == hashlib.sha256(a.read_bytes()).hexdigest()
    assert v["prereg_sha256"] == hashlib.sha256(prereg_path.read_bytes()).hexdigest()


def test_cluster_style_string_typology_column_reads(tmp_path, prereg_path):
    """The cluster writes typology as a plain string (the local runner as a
    pandas category); both must filter to the same rows."""

    def as_string(rep, paths):
        sc = pd.read_parquet(paths["oof_scores"])
        sc["typology"] = sc["typology"].astype(str)
        sc.to_parquet(paths["oof_scores"], index=False)

    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(
        tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale(), mutate=as_string
    )
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is True, v["errors"]
    assert v["typologies"]["beh"]["large"]["n_scored"] == 15000


# ---------------------------------------------------------------------------
# Review findings: provenance and stale-file cases that must not pass
# ---------------------------------------------------------------------------


def _perturb_scores(rep, paths):
    sc = pd.read_parquet(paths["oof_scores"])
    sc["score"] = (sc["score"] * 0.999).astype("float32")
    sc.to_parquet(paths["oof_scores"], index=False)


def _perturb_feature(rep, paths):
    uf = pd.read_parquet(paths["unit_features"])
    uf["noise_b"] = uf["noise_b"] + 1e-9
    uf.to_parquet(paths["unit_features"], index=False)


def _set_pass(name, value):
    def f(rep, paths):
        rep["passes"][name] = value

    return f


def _set_prov(name, value):
    def f(rep, paths):
        rep["provenance"][name] = value

    return f


def _set_scale(factor):
    def f(rep, paths):
        eps = _prereg_dict()["corpora"]["entities_per_scale_unit"]
        rep["provenance"]["corpus_scale"]["n_entities"] = round(eps * _large_scale() * factor)

    return f


@pytest.mark.parametrize(
    "mutate,expect",
    [
        (_perturb_scores, "fingerprint"),
        (_perturb_feature, None),
        (_set_pass("corpus_fully_keyed", False), "corpus_fully_keyed"),
        (_set_pass("registered_label_role", False), "registered_label_role"),
        (_set_pass("library_versions_match", False), "library_versions_match"),
        (_set_prov("label_role", "participant"), "label_role"),
        (_set_prov("model_versions", ["other-model"]), "same_generator"),
        (_set_prov("aml_features_sha256", "e" * 64), "same_feature_code"),
        (_set_scale(0.5), "large_at_registered_scale"),
    ],
    ids=[
        "stale_scores",
        "stale_unit_features",
        "not_fully_keyed",
        "label_role_override_pass",
        "library_mismatch",
        "label_role_participant",
        "other_generator",
        "other_feature_code",
        "large_not_at_registered_scale",
    ],
)
def test_provenance_and_stale_outputs_fail(tmp_path, prereg_path, mutate, expect):
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale(), mutate=mutate)
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is False and v["verdict"] in ("fail", "error")
    text = json.dumps(v)
    if expect:
        assert expect in text
    if mutate is _perturb_feature:
        assert v["features"]["noise_b"]["fingerprint_match"] is False


def test_different_gate_code_fails(tmp_path, prereg_path):
    def other_code(rep, paths):
        rep["gate_code_sha256"] = "d" * 64

    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(
        tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale(), mutate=other_code
    )
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is False and v["checks"]["same_gate_code"] is False


def test_non_calibration_seed_fails(tmp_path, prereg_path):
    other = SEED + 1  # role "other"
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale(), seed=other)
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale(), seed=other)
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is False
    assert any("corpus_role" in e for e in v["errors"])


def test_unpackaged_preregistration_is_refused(tmp_path, prereg_path, monkeypatch):
    """Both runs and D8 under the same overridden file still fail: only the
    packaged pre-registration can certify."""
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    b = _run(tmp_path, "large", _frame(15000, 2), prereg_path, scale=_large_scale())
    monkeypatch.setattr(si, "_packaged_prereg_path", lambda: PREREG)
    v = si.evaluate_d8(str(a), str(b), prereg_path=str(prereg_path))
    assert v["pass"] is False and v["checks"]["packaged_preregistration"] is False
    assert v["prereg_override"]["path"] == str(prereg_path)


def test_same_report_twice_fails(tmp_path, prereg_path):
    a = _run(tmp_path, "small", _frame(6000, 1), prereg_path, scale=_gate_scale())
    v = si.evaluate_d8(str(a), str(a), prereg_path=str(prereg_path))
    assert v["pass"] is False


def test_fingerprint_is_order_and_dtype_independent():
    df = pd.DataFrame(
        {
            "group": np.arange(6, dtype=np.int64),
            "label": np.array([0, 1, 0, 1, 0, 0], dtype=np.int8),
            "score": np.array([0.1, 0.9, -0.0, np.nan, 0.2, 0.3], dtype=np.float32),
            "weight": np.ones(6),
        }
    )
    cols = list(fg.SCORES_FINGERPRINT_COLUMNS)
    base = fg.fingerprint(df, cols)
    other = df.sample(frac=1, random_state=3).astype({"group": np.int32, "label": np.int64})
    other["score"] = other["score"].astype(np.float64)
    other.loc[other["score"] == 0, "score"] = 0.0
    assert fg.fingerprint(other, cols) == base
    changed = df.copy()
    changed.loc[0, "score"] = np.float32(0.1000001)
    assert fg.fingerprint(changed, cols) != base
