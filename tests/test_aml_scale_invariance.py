"""AML D8 scale invariance (AML-GOALS 5a D8, section 9 #45, prereg 3.6.0).

Known answers for the registered rule (scale_estimate + typology_rule on
synthetic run statistics): an invariant typology passes, a 0.10 mid-band
shift fails, a band crossing fails, an underpowered pair is a miss, and
out-of-band typologies need only agree on side. The shard helpers refuse a
plan in which an instance spans shards. End to end, ten real gate runs
(fidelity_gate.evaluate_gate on synthetic frames, written the way
scripts/aml_gate.py writes them) pass D8, and provenance, stale-file, seed
set, shard-plan and sampling defects fail it.
"""

from __future__ import annotations

import copy
import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")
pytest.importorskip("pyarrow")
pytest.importorskip("scipy")

from lakebench.aml import d8_shards  # noqa: E402
from lakebench.aml import fidelity_gate as fg  # noqa: E402
from lakebench.aml import scale_invariance as si  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "src/lakebench/spark/data/aml/aml_preregistration.json"
REAL = json.loads(PREREG.read_text())
CAL = REAL["corpora"]["calibration_seed"]
S2_SEEDS = [CAL, *REAL["corpora"]["calibration_replicate_seeds"]]
N_SHARDS = REAL["scale_invariance"]["n_shards"]


def _logit(p):
    return float(np.log(p / (1 - p)))


# ---------------------------------------------------------------------------
# Known answers: the registered rule on synthetic run statistics
# ---------------------------------------------------------------------------


def _runs(ap, n, sd=0.05, spread=0.0):
    """n runs at ``ap`` with bootstrap sd ``sd`` (logit); ``spread`` spaces
    the run logits symmetrically (between-run variation)."""
    offs = np.linspace(-spread, spread, n) if n > 1 else [0.0]
    return [{"logit": _logit(ap) + o, "boot_sd_logit": sd} for o in offs]


def _verdict(ap2, ap10, sd=0.05, spread2=0.0, spread10=0.0):
    ci = REAL["power"]["ci_level"]
    s2 = si.scale_estimate(_runs(ap2, len(S2_SEEDS), sd, spread2), ci)
    s10 = si.scale_estimate(_runs(ap10, N_SHARDS, sd, spread10), ci)
    return si.typology_rule(s2, s10, REAL)


def test_invariant_typology_passes():
    r = _verdict(0.60, 0.60, sd=0.10)
    assert r["gated"] and r["pass"], r["reasons"]
    assert r["logit_diff"] == pytest.approx(0.0)
    assert r["logit_diff_ci_width"] <= REAL["scale_invariance"]["logit_diff_ci_width_max"]
    assert r["detectable_logit_shift"] > REAL["scale_invariance"]["logit_diff_abs_max"]


def test_mid_band_shift_of_0_10_fails():
    r = _verdict(0.5865, 0.6865, sd=0.10)
    assert r["gated"] and not r["pass"]
    assert r["within_logit_tolerance"] is False
    assert abs(r["logit_diff"]) == pytest.approx(_logit(0.6865) - _logit(0.5865))


def test_band_crossing_fails_even_within_the_logit_tolerance():
    # 0.78 -> 0.82 moves logit by 0.25 (inside 0.30) but crosses ap_max with
    # both CIs clear of the edge: a confident flip.
    r = _verdict(0.78, 0.82, sd=0.005)
    assert r["within_logit_tolerance"] is True
    assert r["confident_flip"] is True and not r["pass"]
    assert any("flip" in x for x in r["reasons"])


def test_band_crossing_with_a_large_shift_fails():
    r = _verdict(0.76, 0.84, sd=0.05)
    assert not r["pass"] and r["within_logit_tolerance"] is False


def test_crossing_near_the_edge_is_not_a_confident_flip():
    r = _verdict(0.795, 0.805, sd=0.05)
    assert r["confident_flip"] is False and r["pass"], r["reasons"]


def test_underpowered_pair_is_a_miss_not_a_pass():
    r = _verdict(0.60, 0.60, sd=0.45)
    assert r["gated"] and not r["pass"]
    assert r["powered"] is False and r["within_logit_tolerance"] is True
    assert any("underpowered" in x for x in r["reasons"])


def test_between_run_spread_sets_the_se_when_larger():
    from scipy.stats import t

    ci = REAL["power"]["ci_level"]
    est = si.scale_estimate(_runs(0.6, len(S2_SEEDS), sd=0.01, spread=0.3), ci)
    assert est["se_source"] == "between"
    lg = np.array(est["run_logits"])
    assert est["se"] == pytest.approx(np.std(lg, ddof=1) / np.sqrt(len(lg)))
    assert est["df"] == len(S2_SEEDS) - 1
    assert est["t_crit"] == pytest.approx(t.ppf((1 + ci) / 2, len(S2_SEEDS) - 1))
    tight = si.scale_estimate(_runs(0.6, N_SHARDS, sd=0.2), ci)
    assert tight["se_source"] == "bootstrap"
    assert tight["se"] == pytest.approx(np.sqrt(N_SHARDS * 0.2**2) / N_SHARDS)


def test_difference_ci_is_welch_satterthwaite():
    from scipy.stats import t

    r = _verdict(0.6, 0.6, sd=0.1, spread2=0.1)
    ci = REAL["power"]["ci_level"]
    s2 = si.scale_estimate(_runs(0.6, len(S2_SEEDS), 0.1, 0.1), ci)
    s10 = si.scale_estimate(_runs(0.6, N_SHARDS, 0.1), ci)
    v = s2["se"] ** 2 + s10["se"] ** 2
    df = v**2 / (s2["se"] ** 4 / s2["df"] + s10["se"] ** 4 / s10["df"])
    assert r["welch_df"] == pytest.approx(df)
    assert r["logit_diff_ci_width"] == pytest.approx(2 * t.ppf((1 + ci) / 2, df) * np.sqrt(v))


def test_out_of_band_same_side_passes_on_side_only():
    r = _verdict(0.005, 0.010, sd=0.07)
    assert r["gated"] is False and r["pass"], r["reasons"]
    assert r["side_s2"] == r["side_s10"] == "below"
    assert r["ap_ratio"] == pytest.approx(0.010 / 0.005, rel=1e-6)
    lo, hi = r["ap_ratio_ci"]
    assert lo < r["ap_ratio"] < hi


def test_out_of_band_opposite_sides_fail():
    r = _verdict(0.005, 0.97, sd=0.05)
    assert r["gated"] is False and not r["pass"]


def test_one_scale_touching_the_band_gates_the_typology():
    r = _verdict(0.25, 0.32, sd=0.05)
    assert r["gated"] is True


# ---------------------------------------------------------------------------
# Shard plan: components and the leakage refusal
# ---------------------------------------------------------------------------


def test_components_join_crews_and_shared_counterparties():
    instances = {
        "crew": [5, 3, 9],  # a micro_structuring crew
        "linked": [9, 12],  # shares party 9 with the crew
        "alone": [20, 21],
    }
    reps = d8_shards.components(instances.values())
    assert reps[5] == reps[3] == reps[9] == reps[12] == 3
    assert reps[20] == reps[21] == 20
    assert d8_shards.component_sizes(reps) == {3: 4, 20: 2}


def test_shard_plan_balance_and_giant_component_are_refused():
    p = copy.deepcopy(REAL)
    p["behavioural_subset"], p["definitional_subset"] = ["beh"], []
    ok = {
        "salt": p["scale_invariance"]["shard_salt"],
        "n_shards": N_SHARDS,
        "typologies": ["beh"],
        "spanning_instances": 0,
        "customers_per_shard": [1000] * N_SHARDS,
        "n_customers": 1000 * N_SHARDS,
        "largest_component": 3,
    }
    assert si.shard_plan_errors(ok, p) == []
    skew = {**ok, "customers_per_shard": [1500, 1000, 1000, 1000, 500]}
    assert any("mean" in e for e in si.shard_plan_errors(skew, p))
    giant = {**ok, "largest_component": 1000}
    assert any("percolates" in e for e in si.shard_plan_errors(giant, p))
    assert si.shard_plan_errors(None, p)


def test_shard_leakage_is_refused():
    instances = {"a": [1, 2], "b": [2, 3], "c": [7, 8]}
    reps = d8_shards.components(instances.values())
    good = {k: (0 if reps[k] == 1 else 1) for k in reps}
    d8_shards.check_plan(instances, good)
    leaky = {**good, 3: 1}  # instance b's parties in shards 0 and 1
    assert d8_shards.spanning_instances(instances, leaky) == ["b"]
    with pytest.raises(ValueError, match="span shards"):
        d8_shards.check_plan(instances, leaky)
    # A non-customer party (no shard) never counts as a span.
    d8_shards.check_plan({"x": [1, 99]}, {1: 0})


# ---------------------------------------------------------------------------
# End to end: ten real gate runs
# ---------------------------------------------------------------------------


def _prereg_dict():
    """The real pre-registration with every D8 value untouched, cut down to
    three features, one behavioural typology and fewer resamples."""
    p = copy.deepcopy(REAL)
    p["features"] = ["planted", "noise_a", "noise_b"]
    p["behavioural_subset"] = ["beh"]
    p["definitional_subset"] = []
    p["level2"] = {**p["level2"], "n": 1, "k_in_band": 1}
    p["power"] = {**p["power"], "bootstrap_iterations": 200, "min_positives": 40}
    return p


def _frame(n, seed, signal=2.0, prev=0.05, shift=0.0, group0=0):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < prev).astype(int)
    return pd.DataFrame(
        {
            "group": group0 + np.arange(n),
            "month": 0,
            "planted": y * signal + rng.normal(0, 1, n),
            "noise_a": rng.normal(shift, 1, n),
            "noise_b": rng.normal(0, 1, n),
            "is_customer": True,
            "label:beh": y,
        }
    )


N_UNITS = 5000
PLAN_FP = "123456789"
IMAGE = "docker.io/sillidata/lb-datagen@sha256:" + "ab" * 32


def _run(d: Path, frame, prereg_path, *, scale, seed, shard=None, l2=False):
    prereg, sha = fg.load_preregistration(prereg_path)
    eps = prereg["corpora"]["entities_per_scale_unit"]
    prov = {
        "adapter": "bronze",
        "corpus": f"/corpora/{scale}-{seed}",
        "corpus_seed": seed,
        "corpus_seed_check": {"claimed_seed": seed, "matched_share": 1},
        "corpus_scale": {"n_entities": round(eps * scale), "scale": scale},
        "label_role": prereg["unit_of_scoring"]["label_role"],
        "aml_features_sha256": "f" * 64,
        "model_versions": ["test-model"],
        "diagnostic": False,
        "d8_shard": None,
        "generator_image": IMAGE,
    }
    if shard is not None:
        prov["d8_shard"] = {
            "index": shard,
            "n_shards": N_SHARDS,
            "salt": prereg["scale_invariance"]["shard_salt"],
            "customers_per_shard": [N_UNITS] * N_SHARDS,
            "n_customers": N_UNITS * N_SHARDS,
            "largest_component": 3,
            "typologies": ["beh"],
            "spanning_instances": 0,
            "plan_fingerprint": PLAN_FP,
        }
    rep = fg.evaluate_gate(
        frame,
        prereg,
        prereg_sha256=sha,
        provenance=prov,
        collect_outputs=True,
        l2_values=prereg["scale_invariance"]["l2_sensitivity"]["values"] if l2 else None,
    )
    for name in ("corpus_fully_keyed", "corpus_seed_verified", "registered_label_role"):
        fg.add_pass(rep, name, True)
    outputs = rep.pop("_model_outputs")
    d.mkdir(parents=True)
    paths = fg.write_model_outputs(outputs, str(d / "gate"))
    fps = fg.output_fingerprints(outputs)
    rep["model_outputs"] = {k: {"path": v, "fingerprint": fps.get(k)} for k, v in paths.items()}
    out = d / "gate.json"
    out.write_text(json.dumps(rep, default=str))
    return out


@pytest.fixture(scope="module")
def prereg_file(tmp_path_factory):
    path = tmp_path_factory.mktemp("prereg") / "prereg.json"
    path.write_text(json.dumps(_prereg_dict()))
    return path


@pytest.fixture(autouse=True)
def _packaged(prereg_file, monkeypatch):
    monkeypatch.setattr(si, "_packaged_prereg_path", lambda: prereg_file)
    monkeypatch.setenv("LB_AML_GATE_JOBS", "2")


@pytest.fixture(scope="module")
def base(tmp_path_factory, prereg_file):
    """Five s2 runs (the registered seeds) and five s10 shards, all from the
    same generating distribution; shard customers are disjoint."""
    from threadpoolctl import threadpool_limits

    root = tmp_path_factory.mktemp("d8runs")
    p = _prereg_dict()
    s2_scale, s10_scale = p["corpora"]["gate_scale"], p["scale_invariance"]["large_scale"]
    with threadpool_limits(limits=2):
        s2 = [
            _run(
                root / f"s2-{s}",
                _frame(N_UNITS, i + 1),
                prereg_file,
                scale=s2_scale,
                seed=s,
                l2=True,
            )
            for i, s in enumerate(S2_SEEDS)
        ]
        s10 = [
            _run(
                root / f"s10-{k}",
                _frame(N_UNITS, 100 + k, group0=k * N_UNITS),
                prereg_file,
                scale=s10_scale,
                seed=CAL,
                shard=k,
                l2=True,
            )
            for k in range(N_SHARDS)
        ]
    return {"root": root, "s2": s2, "s10": s10}


def _eval(prereg_file, s2, s10):
    return si.evaluate_d8(
        [str(x) for x in s2], [str(x) for x in s10], prereg_path=str(prereg_file), jobs=2
    )


def _mutated(tmp_path, report: Path, fn, name="mut"):
    rep = json.loads(report.read_text())
    fn(rep)
    out = tmp_path / f"{name}-{hashlib.sha256(str(report).encode()).hexdigest()[:8]}.json"
    out.write_text(json.dumps(rep, default=str))
    return out


def _large_run(tmp_path, name, frame, prereg_file, shard):
    return _run(
        tmp_path / name,
        frame,
        prereg_file,
        scale=_prereg_dict()["scale_invariance"]["large_scale"],
        seed=CAL,
        shard=shard,
    )


def test_invariant_runs_pass(base, prereg_file):
    v = _eval(prereg_file, base["s2"], base["s10"])
    assert v["verdict"] == "pass" and v["pass"] is True, v["errors"]
    r = v["typologies"]["beh"]
    assert r["gated"] and r["s2"]["n_runs"] == len(S2_SEEDS) and r["s10"]["n_runs"] == N_SHARDS
    assert all(f["pass"] for f in v["features"].values())
    assert v["checks"]["s10_disjoint_customers"] is True
    assert v["l2_sensitivity"]["available"] is True and v["l2_sensitivity"]["gated"] is False
    assert "not an equivalence test" in " ".join(si.summary_lines(v))
    first = v["inputs"]["s2"][0]["report_sha256"]
    assert first == hashlib.sha256(base["s2"][0].read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def shifted(tmp_path_factory, prereg_file):
    """s10 shards with a weaker planted signal and a shifted noise feature."""
    from threadpoolctl import threadpool_limits

    root = tmp_path_factory.mktemp("d8shifted")
    with threadpool_limits(limits=2):
        return [
            _large_run(
                root,
                f"w{k}",
                _frame(N_UNITS, 200 + k, signal=1.4, shift=0.5, group0=k * N_UNITS),
                prereg_file,
                k,
            )
            for k in range(N_SHARDS)
        ]


def test_weaker_signal_at_scale_fails(base, shifted, prereg_file):
    v = _eval(prereg_file, base["s2"], shifted)
    assert v["pass"] is False
    assert v["typologies"]["beh"]["within_logit_tolerance"] is False
    assert v["l2_sensitivity"]["available"] is False  # ungated; never the reason


def test_shifted_feature_fails_ks(base, shifted, prereg_file):
    v = _eval(prereg_file, base["s2"], shifted)
    assert v["features"]["noise_a"]["pass"] is False
    assert v["features"]["noise_b"]["pass"] is True


def test_overlapping_shard_customers_are_refused(base, prereg_file, tmp_path):
    # Shard 1 scoring shard 0's customers.
    leak = _large_run(tmp_path, "leak", _frame(N_UNITS, 400, group0=0), prereg_file, 1)
    s10 = list(base["s10"])
    s10[1] = leak
    v = _eval(prereg_file, base["s2"], s10)
    assert v["pass"] is False and v["checks"]["s10_disjoint_customers"] is False


def _shard_field(name, value):
    def f(rep):
        rep["provenance"]["d8_shard"][name] = value

    return f


def _prov(name, value):
    def f(rep):
        rep["provenance"][name] = value

    return f


@pytest.mark.parametrize(
    "fn,check",
    [
        (_shard_field("spanning_instances", 3), "s10_no_spanning_instance"),
        (_shard_field("plan_fingerprint", "999"), "s10_one_plan"),
        (_shard_field("index", 0), "s10_every_index_once"),
        (_shard_field("salt", "other"), "s10_registered_plan"),
        (_shard_field("customers_per_shard", [1] * N_SHARDS), "s10_scored_within_shard"),
        (_prov("d8_shard", None), "s10_shard_plan_recorded"),
        (_shard_field("largest_component", N_UNITS), "s10_plan_sound"),
        (_shard_field("typologies", ["beh", "random"]), "s10_plan_sound"),
        (_prov("corpus", "/corpora/elsewhere"), "s10_one_corpus"),
        (_prov("model_versions", ["other-model"]), "same_model_versions"),
        (_prov("aml_features_sha256", "e" * 64), "same_aml_features_sha256"),
        (_prov("adapter", "silver"), "same_adapter"),
        (_prov("generator_image", IMAGE.replace("ab", "cd")), "same_generator_image"),
    ],
    ids=[
        "spanning_instance",
        "other_plan",
        "duplicate_index",
        "unregistered_salt",
        "scored_outside_shard",
        "no_shard_block",
        "giant_component",
        "components_from_other_typologies",
        "other_corpus",
        "other_generator",
        "other_feature_code",
        "other_adapter",
        "other_generator_image",
    ],
)
def test_shard_and_identity_defects_fail(base, prereg_file, tmp_path, fn, check):
    s10 = list(base["s10"])
    s10[2] = _mutated(tmp_path, s10[2], fn)
    v = _eval(prereg_file, base["s2"], s10)
    assert v["pass"] is False and v["checks"][check] is False


@pytest.mark.parametrize(
    "fn,expect",
    [
        (_prov("sampling", {"monthly": {"negative_fraction": 0.2}}), "driver sample cap"),
        (_prov("diagnostic", True), "diagnostic"),
        (_prov("label_role", "participant"), "label_role"),
        (lambda rep: rep["passes"].update(corpus_seed_verified=False), "corpus_seed_verified"),
        (lambda rep: rep["passes"].pop("corpus_seed_verified"), "corpus_seed_verified"),
        (lambda rep: rep["passes"].update(library_versions_match=False), "library_versions"),
        (lambda rep: rep.update(gate_code_sha256="d" * 64), "same_gate_code_sha256"),
        (lambda rep: rep.update(verdict="error"), "verdict"),
        (_prov("generator_image", "docker.io/sillidata/lb-datagen:latest"), "digest"),
        (_prov("generator_image", None), "generator_image"),
    ],
    ids=[
        "cap_bound",
        "diagnostic",
        "label_role",
        "seed_unverified",
        "seed_check_missing",
        "library_mismatch",
        "other_gate_code",
        "verdict_not_ok",
        "image_tag_not_digest",
        "host_built_no_image",
    ],
)
def test_run_defects_fail(base, prereg_file, tmp_path, fn, expect):
    s2 = list(base["s2"])
    s2[1] = _mutated(tmp_path, s2[1], fn)
    v = _eval(prereg_file, s2, base["s10"])
    assert v["pass"] is False
    assert expect in json.dumps(v)


def test_s2_seed_set_must_be_exactly_the_registered_seeds(base, prereg_file, tmp_path):
    v = _eval(prereg_file, base["s2"][:-1], base["s10"])
    assert v["pass"] is False and v["checks"]["s2_registered_seeds"] is False
    dup = list(base["s2"])
    dup[-1] = base["s2"][0]
    v = _eval(prereg_file, dup, base["s10"])
    assert v["pass"] is False and v["checks"]["distinct_reports"] is False
    other = list(base["s2"])
    other[-1] = _mutated(tmp_path, other[-1], _prov("corpus_seed", 7777))
    v = _eval(prereg_file, other, base["s10"])
    assert v["pass"] is False and v["checks"]["s2_registered_seeds"] is False


def test_scale_mismatch_fails(base, prereg_file, tmp_path):
    def half(rep):
        rep["provenance"]["corpus_scale"]["n_entities"] //= 2

    s10 = list(base["s10"])
    s10[0] = _mutated(tmp_path, s10[0], half)
    v = _eval(prereg_file, base["s2"], s10)
    assert v["pass"] is False and v["checks"]["s10_at_large_scale"] is False
    s2 = list(base["s2"])
    s2[0] = _mutated(tmp_path, s2[0], half)
    v = _eval(prereg_file, s2, base["s10"])
    assert v["pass"] is False and v["checks"]["s2_at_gate_scale"] is False


def test_stale_scores_fail_the_fingerprint(base, prereg_file, tmp_path):
    rep = json.loads(base["s2"][0].read_text())
    src = Path(rep["model_outputs"]["oof_scores"]["path"])
    dst = tmp_path / "stale_oof_scores.parquet"
    shutil.copy(src, dst)
    sc = pd.read_parquet(dst)
    sc["score"] = (sc["score"] * 0.999).astype("float32")
    sc.to_parquet(dst, index=False)
    rep["model_outputs"]["oof_scores"]["path"] = str(dst)
    p = tmp_path / "stale.json"
    p.write_text(json.dumps(rep))
    v = _eval(prereg_file, [p, *base["s2"][1:]], base["s10"])
    assert v["pass"] is False and "fingerprint" in json.dumps(v["typologies"])


def test_unpackaged_preregistration_is_refused(base, prereg_file, monkeypatch):
    monkeypatch.setattr(si, "_packaged_prereg_path", lambda: PREREG)
    v = _eval(prereg_file, base["s2"], base["s10"])
    assert v["pass"] is False and v["checks"]["packaged_preregistration"] is False


def test_missing_report_is_an_error(base, prereg_file, tmp_path):
    v = _eval(prereg_file, [*base["s2"][:-1], tmp_path / "nope.json"], base["s10"])
    assert v["verdict"] == "error" and v["pass"] is False


def test_cli_exit_codes(base, prereg_file, tmp_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location("aml_d8", ROOT / "scripts/aml_d8.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    args = ["--s2", *map(str, base["s2"]), "--s10", *map(str, base["s10"])]
    out = tmp_path / "d8.json"
    assert mod.main([*args, "--prereg", str(prereg_file), "--out", str(out), "--jobs", "2"]) == 0
    assert json.loads(out.read_text())["verdict"] == "pass"
    bad = ["--s2", *map(str, base["s2"][:-1]), "--s10", *map(str, base["s10"])]
    assert mod.main([*bad, "--prereg", str(prereg_file), "--jobs", "2"]) == 2
    missing = ["--s2", str(tmp_path / "x.json"), "--s10", *map(str, base["s10"])]
    assert mod.main([*missing, "--prereg", str(prereg_file)]) == 1


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
    mult = r.draw(np.random.default_rng(1))
    order = np.argsort(-score, kind="stable")
    ys, ss, ws = y[order], score[order], w[order]
    idx = np.repeat(np.arange(n), mult[r.codes].astype(int))
    want = average_precision_score(ys[idx], ss[idx], sample_weight=ws[idx])
    assert r.ap(mult) == pytest.approx(want, abs=1e-12)


def test_run_logit_stats_voids_the_sd_on_a_degenerate_resample():
    y = np.zeros(50, dtype=int)
    y[0] = 1  # one positive: many resamples have none
    arrays = {"y": y, "score": np.arange(50.0), "w": np.ones(50), "groups": np.arange(50)}
    res = si.run_logit_stats(arrays, 50, np.random.default_rng(0))
    assert res["boot_sd_logit"] is None and res["n_degenerate_resamples"] > 0


def test_weighted_ks_matches_scipy_and_counts_nan_mass():
    from scipy.stats import ks_2samp

    rng = np.random.default_rng(0)
    a, b = rng.normal(0, 1, 2000), rng.normal(0.3, 1, 3000)
    got = si.weighted_ks(a, np.ones(len(a)), b, np.ones(len(b)))
    assert got == pytest.approx(ks_2samp(a, b).statistic, abs=1e-12)
    wa = rng.integers(1, 3, len(a)).astype(float)
    rep = np.repeat(a, wa.astype(int))
    assert si.weighted_ks(a, wa, b, np.ones(len(b))) == pytest.approx(
        ks_2samp(rep, b).statistic, abs=1e-12
    )
    c = a.copy()
    c[: len(c) // 5] = np.nan
    assert si.weighted_ks(a, np.ones(len(a)), c, np.ones(len(c))) >= 0.19


def test_s3_uris_resolve_without_network():
    from pyarrow import fs

    fsys, path = si._resolve("s3a://lb-gold/aml/aml_gate_report.json", "http://127.0.0.1:9")
    assert isinstance(fsys, fs.S3FileSystem) and path == "lb-gold/aml/aml_gate_report.json"
    fsys, path = si._resolve("relative/gate.json", None)
    assert isinstance(fsys, fs.LocalFileSystem) and Path(path).is_absolute()


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
    base_fp = fg.fingerprint(df, cols)
    other = df.sample(frac=1, random_state=3).astype({"group": np.int32, "label": np.int64})
    other["score"] = other["score"].astype(np.float64)
    other.loc[other["score"] == 0, "score"] = 0.0
    assert fg.fingerprint(other, cols) == base_fp
    changed = df.copy()
    changed.loc[0, "score"] = np.float32(0.1000001)
    assert fg.fingerprint(changed, cols) != base_fp


# ---------------------------------------------------------------------------
# Pre-registration: the registered D8 values and the power simulation record
# ---------------------------------------------------------------------------


def test_prereg_registers_the_d8_rule():
    s = REAL["scale_invariance"]
    assert s["logit_diff_abs_max"] == 0.30 and s["logit_diff_ci_width_max"] == 0.70
    assert s["ks_stat_max"] == 0.10 and s["n_shards"] == 5 and s["large_scale"] == 10
    assert "ap_diff_abs_max" not in s
    assert s["l2_sensitivity"]["values"] == [1.0, 100.0]
    assert s["l2_sensitivity"]["gated"] is False
    assert "not an equivalence test" in s["not_an_equivalence_test"]
    assert REAL["version"] == REAL["changelog"][0]["version"] == "3.6.0"


def test_power_sim_output_hash_is_recorded():
    """The pre-registration records the sha256 of the power simulation's
    stdout; rerunning it must reproduce the hash (or the record is stale)."""
    import subprocess
    import sys

    import scipy

    rec = REAL["scale_invariance"]["power_sim"]
    pinned = (rec["produced_with"]["numpy"], rec["produced_with"]["scipy"])
    if (np.__version__, scipy.__version__) != pinned:
        pytest.skip(f"the recorded hash was produced with numpy/scipy {pinned}")
    out = subprocess.run(
        [sys.executable, str(ROOT / "scripts/aml_d8_power_sim.py")],
        capture_output=True,
        check=True,
        timeout=600,
    ).stdout
    assert hashlib.sha256(out).hexdigest() == rec["stdout_sha256"]


# ---------------------------------------------------------------------------
# Level-2 predictions from the calibration runs (#46 D-8)
# ---------------------------------------------------------------------------


def test_predictions_from_the_calibration_runs(base, prereg_file):
    from scipy.stats import t

    from lakebench.aml.predictions import compute_predictions

    pred = compute_predictions([str(x) for x in base["s2"]], prereg_path=str(prereg_file))
    r = pred["typologies"]["beh"]
    lo, hi = r["pi"]
    assert lo < r["predicted_ap"] < hi and r["n_runs"] == len(S2_SEEDS)
    n = len(S2_SEEDS)
    half = t.ppf((1 + pred["pi_level"]) / 2, n - 1) * r["s_run"] * np.sqrt(1 + 1 / n)
    assert _logit(hi) - r["logit_mean"] == pytest.approx(half)
    assert pred["generator_image"] == IMAGE and len(pred["inputs"]) == n
    with pytest.raises(ValueError, match="exactly seeds"):
        compute_predictions([str(x) for x in base["s2"][:-1]], prereg_path=str(prereg_file))
    with pytest.raises(ValueError):
        compute_predictions([str(x) for x in base["s10"]], prereg_path=str(prereg_file))
