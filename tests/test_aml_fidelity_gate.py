"""Pure-Python AML fidelity gate (AML-GOALS D5, D7, D9, D2, D11, R7).

Synthetic per-customer frames with known answers: a planted perfectly
separable feature must give AP ~ 1 and trip the single-feature leakage cap;
pure noise must give AP ~ prevalence. Plus the pre-registration contract: the
gate carries no threshold literals, and the JSON's feature list matches what
aml_features builds.
"""

from __future__ import annotations

import ast
import copy
import json
import re
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")

from lakebench.aml import fidelity_gate as fg  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
GATE_SRC = ROOT / "src/lakebench/aml/fidelity_gate.py"
RUNNER_SRC = ROOT / "scripts/aml_gate.py"
FEATURES_SRC = ROOT / "src/lakebench/spark/scripts/aml_features.py"
PREREG = ROOT / "src/lakebench/spark/data/aml/aml_preregistration.json"
TYPOLOGY_RS = ROOT / "datagen_rs/src/typology.rs"


@pytest.fixture(autouse=True)
def _few_threads():
    """Tiny frames: OpenMP fan-out across every core costs more than the fit,
    and on a shared host it oversubscribes badly."""
    from threadpoolctl import threadpool_limits

    with threadpool_limits(limits=2):
        yield


def _prereg(**over):
    """The real pre-registration, cut down so a test runs in seconds: three
    features, one typology per subset, fewer bootstrap iterations."""
    p, _ = fg.load_preregistration(PREREG)
    p = copy.deepcopy(p)
    p["features"] = ["planted", "noise_a", "noise_b"]
    p["behavioural_subset"] = ["beh"]
    p["definitional_subset"] = ["defn"]
    p["level2"] = {**p["level2"], "n": 1, "k_in_band": 1}
    p["classification"] = {**p["classification"], "defining_feature": {"defn": "planted"}}
    p["power"] = {**p["power"], "bootstrap_iterations": 50, "min_positives": 40}
    for k, v in over.items():
        p[k] = v
    return p


def _frame(n=1500, prev=0.06, seed=0, separable=True):
    rng = np.random.default_rng(seed)
    y = (rng.random(n) < prev).astype(int)
    planted = y + rng.normal(0, 0.01, n) if separable else rng.normal(0, 1, n)
    df = pd.DataFrame(
        {
            "planted": planted,
            "noise_a": rng.normal(0, 1, n),
            "noise_b": rng.normal(0, 1, n),
            "is_customer": True,
            "label:beh": y,
            "label:defn": y,
        }
    )
    return df


def test_planted_separable_feature_gives_ap_one_and_trips_single_cap():
    rep = fg.evaluate_gate(_frame(), _prereg())
    r = rep["typologies"]["beh"]
    assert r["status"] == "ok"
    assert r["ap"] > 0.99
    single = r["shortcuts"]["single_feature"]
    assert single["best"]["feature"] == "planted"
    assert single["best"]["ap"] > 0.99
    assert single["pass_abs"] is False and single["pass"] is False
    assert r["leakage_pass"] is False
    # AP 1 is above the band ceiling.
    assert r["in_band"] is False
    # A definitional typology whose defining feature separates passes that check.
    d = rep["typologies"]["defn"]["definitional_check"]
    assert d["defining_feature"] == "planted" and d["pass"] is True
    assert rep["level2"]["holds_on_this_corpus"] is False


def test_noise_gives_ap_near_prevalence():
    df = _frame(n=4000, prev=0.08, separable=False)
    rep = fg.evaluate_gate(df, _prereg())
    r = rep["typologies"]["beh"]
    prev = df["label:beh"].mean()
    assert abs(r["ap"] - prev) < 0.05, (r["ap"], prev)
    lo, hi = r["ap_ci"]
    assert lo <= r["ap"] <= hi
    # Nothing leaks: every shortcut is near prevalence, well under the 0.20 cap.
    assert r["shortcuts"]["single_feature"]["pass_abs"] is True
    assert r["shortcuts"]["feature_pair_depth2"]["pass_abs"] is True
    assert rep["typologies"]["defn"]["definitional_check"]["pass"] is False


def test_pair_shortcut_catches_a_conjunction():
    """Label = a AND b: each alone is weak, the depth-2 pair is exact."""
    rng = np.random.default_rng(3)
    n = 3000
    a = rng.random(n) < 0.3
    b = rng.random(n) < 0.3
    y = (a & b).astype(int)
    df = pd.DataFrame(
        {
            "planted": a.astype(float),
            "noise_a": b.astype(float),
            "noise_b": rng.normal(0, 1, n),
            "label:beh": y,
            "label:defn": y,
        }
    )
    rep = fg.evaluate_gate(df, _prereg())
    r = rep["typologies"]["beh"]
    pair = r["shortcuts"]["feature_pair_depth2"]
    assert set(pair["best"]["features"]) == {"planted", "noise_a"}
    assert pair["best"]["ap"] > 0.99
    assert r["shortcuts"]["single_feature"]["best"]["ap"] < 0.5
    assert pair["pass"] is False


def test_customers_only_and_power_flag():
    df = _frame(n=1500, prev=0.02)
    df.loc[: len(df) // 2, "is_customer"] = False
    rep = fg.evaluate_gate(df, _prereg())
    assert rep["n_scored_customers"] == int(df["is_customer"].sum())
    r = rep["typologies"]["beh"]
    assert r["n_scored"] == rep["n_scored_customers"]
    assert r["n_positives"] < 40 and r["underpowered"] is True
    # Underpowered counts as a miss even if the AP were in band.
    assert rep["level2"]["per_typology"][0]["counts_in_band"] is False


def test_too_few_positives_is_insufficient_not_a_crash():
    df = _frame(n=300, prev=0.0)
    df.loc[:2, "label:beh"] = 1
    rep = fg.evaluate_gate(df, _prereg())
    assert rep["typologies"]["beh"]["status"] == "insufficient_labels"
    assert rep["typologies"]["beh"]["ap"] is None
    assert rep["level2"]["all_have_ap"] is False


def test_deterministic_given_the_seed():
    a = fg.evaluate_gate(_frame(separable=False), _prereg())
    b = fg.evaluate_gate(_frame(separable=False), _prereg())
    assert json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)


def test_weights_restore_prevalence():
    """Downsampled negatives with inverse-fraction weights give the same AP
    as the full frame, within noise."""
    full = _frame(n=6000, prev=0.05, separable=False, seed=5)
    full["planted"] = full["label:beh"] * 0.7 + np.random.default_rng(9).normal(0, 1, len(full))
    p = _prereg()
    ap_full = fg.evaluate_gate(full, p)["typologies"]["beh"]["ap"]
    pos = full[full["label:beh"] == 1]
    neg = full[full["label:beh"] == 0].sample(frac=0.25, random_state=0)
    sub = pd.concat([pos.assign(weight=1.0), neg.assign(weight=4.0)])
    ap_sub = fg.evaluate_gate(sub, p)["typologies"]["beh"]["ap"]
    assert abs(ap_full - ap_sub) < 0.08, (ap_full, ap_sub)


def test_missing_column_raises_and_no_sklearn_is_reported(monkeypatch):
    df = _frame().drop(columns=["noise_b"])
    with pytest.raises(ValueError, match="noise_b"):
        fg.evaluate_gate(df, _prereg())
    monkeypatch.setattr(fg, "_sklearn_available", lambda: False)
    rep = fg.evaluate_gate(_frame(), _prereg())
    assert rep["verdict"] == "no_sklearn" and rep["typologies"] == {}


def test_empty_frame_verdict():
    rep = fg.evaluate_gate(_frame().iloc[:0], _prereg())
    assert rep["verdict"] == "empty_frame"


def test_level2_n_must_match_behavioural_subset():
    p = _prereg()
    p["level2"]["n"] = 4
    with pytest.raises(ValueError, match="level2.n"):
        fg.evaluate_gate(_frame(), p)


def test_timing_mixture_and_density():
    p = _prereg()
    tm = p["timing_mixture"]
    rep = fg.evaluate_gate(
        _frame(),
        p,
        timing_counts={"n_cohort": 100, "n_below_low": 20, "n_above_high": 30},
        density_counts={"total_rows": 1_000_000, "planted_rows": 1000, "per_typology": {}},
    )
    t = rep["timing_mixture"]
    assert t["share_below_low"] == 0.2 and t["share_above_high"] == 0.3
    assert t["pass"] is (0.2 >= tm["min_share_below_low"] and 0.3 >= tm["min_share_above_high"])
    d = rep["density"]
    assert d["frac_rows"] == 0.001 and d["pass"] is True
    rep = fg.evaluate_gate(
        _frame(),
        p,
        timing_counts={"n_cohort": 100, "n_below_low": 1, "n_above_high": 90},
        density_counts={"total_rows": 1_000_000, "planted_rows": 5000, "per_typology": {}},
    )
    assert rep["timing_mixture"]["pass"] is False
    assert rep["density"]["pass"] is False


def test_real_preregistration_runs_end_to_end():
    """The shipped JSON has every key the gate reads (small frame, all six
    typologies; the feature list is cut to keep the pair search short and is
    checked against aml_features separately)."""
    p, sha = fg.load_preregistration(PREREG)
    p = copy.deepcopy(p)
    p["power"]["bootstrap_iterations"] = 5
    p["features"] = p["features"][:4] + list(p["classification"]["defining_feature"].values())
    rng = np.random.default_rng(1)
    n = 400
    data = {f: rng.normal(0, 1, n) for f in p["features"]}
    for t in fg.in_scope_typologies(p):
        data[f"label:{t}"] = (rng.random(n) < 0.1).astype(int)
    rep = fg.evaluate_gate(pd.DataFrame(data), p, prereg_sha256=sha)
    assert rep["verdict"] == "ok"
    assert set(rep["typologies"]) == set(fg.in_scope_typologies(p))
    assert rep["prereg_version"] == p["version"] and len(rep["prereg_sha256"]) == 64
    assert all(r["status"] == "ok" for r in rep["typologies"].values())
    assert rep["level2"]["n"] == len(p["behavioural_subset"])
    assert fg.summary_lines(rep)


# ---------------------------------------------------------------------------
# R7 and pre-registration contract
# ---------------------------------------------------------------------------


def _numeric_literals(path: Path) -> set:
    tree = ast.parse(path.read_text())
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and type(node.value) in (int, float):
            out.add(node.value)
    return out


@pytest.mark.parametrize("path", [GATE_SRC, RUNNER_SRC], ids=lambda p: p.name)
def test_gate_has_no_numeric_threshold_literals(path):
    """R7: every threshold comes from the JSON. 0, 1 and 2 are structural
    (indexing, halves of a two-sided interval); anything else is suspect."""
    assert _numeric_literals(path) <= {0, 1, 2}, _numeric_literals(path) - {0, 1, 2}


def _tuple_const(name):
    tree = ast.parse(FEATURES_SRC.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            return [e.value for e in node.value.elts]
    raise AssertionError(name)


def test_prereg_features_match_aml_features():
    cols = _tuple_const("FEATURE_COLUMNS")
    hist = _tuple_const("HISTORY_FEATURE_COLUMNS")
    p = json.loads(PREREG.read_text())
    assert p["features"] == cols + hist
    assert p["unit_of_scoring"]["history_features"] == hist
    # AML-GOALS section 9 #32.
    assert "customer_type" in cols and "crr_tier" in cols and "is_customer" not in cols
    for t, f in p["classification"]["defining_feature"].items():
        assert f in cols, (t, f)


def test_high_risk_countries_match_generator_corridor_pool():
    tree = ast.parse(FEATURES_SRC.read_text())
    ours = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "HIGH_RISK_COUNTRIES" for t in node.targets
        ):
            ours = [e.value for e in node.value.elts]
    m = re.search(r"const HIGH_RISK_CC: \[&str; \d+\] = \[([^\]]*)\]", TYPOLOGY_RS.read_text())
    assert m, "HIGH_RISK_CC not found in typology.rs"
    theirs = re.findall(r'"([A-Z]{2})"', m.group(1))
    assert sorted(ours) == sorted(theirs)


def test_prereg_version_and_model_blocks():
    p = json.loads(PREREG.read_text())
    assert p["version"] == "3.4.1"
    assert "#36" in p["_doc"]
    u = p["unit_of_scoring"]
    assert (u["window"], u["label_role"]) == ("utc_calendar_month", "subject")
    assert (u["lead_in_days"], u["burn_in_months"], u["history_days"]) == (14, 14, 395)
    assert p["leakage"]["relative_cap_formula"] == "lift_over_prevalence"
    assert p["band"] == {**p["band"], "ap_min": 0.3, "ap_max": 0.8}
    assert p["reference_model"]["estimator"] == "HistGradientBoostingClassifier"
    assert p["shortcut_model"]["max_depth"] == 2
    assert {k: p["cv"][k] for k in ("folds", "stratified", "seed")} == {
        "folds": 5,
        "stratified": True,
        "seed": 7,
    }
    assert p["cv"]["group_by"] == "customer"


def test_load_preregistration_prefers_flat_copy(tmp_path, monkeypatch):
    """On the driver the JSON sits next to the module; an explicit path wins."""
    p = tmp_path / fg.PREREG_FILENAME
    p.write_text(json.dumps({"version": "x"}))
    got, sha = fg.load_preregistration(p)
    assert got == {"version": "x"} and len(sha) == 64
    monkeypatch.setenv("LB_AML_PREREG_PATH", str(p))
    assert fg.load_preregistration()[0] == {"version": "x"}


def test_passes_summary_and_corpus_role():
    p = _prereg()
    rep = fg.evaluate_gate(_frame(), p, provenance={"corpus_seed": 42})
    assert rep["corpus_role"] == "calibration"
    assert rep["passes"]["d5_leakage_behavioural"] is False
    assert rep["passes"]["all"] is False
    assert rep["passes"]["d2_timing_mixture"] is None  # not supplied, not counted
    assert fg.corpus_role(p["corpora"]["evaluation_seed"], p) == "evaluation"
    assert fg.corpus_role(7, p) == "other" and fg.corpus_role(None, p) == "unknown"


def test_reference_model_ignores_doc_keys():
    p = _prereg()
    p["reference_model"] = {**p["reference_model"], "_doc": "a note"}
    assert fg.evaluate_gate(_frame(), p)["verdict"] == "ok"


def test_add_pass_folds_into_all():
    rep = {"passes": {"all": True, "d11_density": True}}
    fg.add_pass(rep, "corpus_fully_keyed", True)
    assert rep["passes"]["all"] is True
    fg.add_pass(rep, "registered_label_role", False)
    assert rep["passes"]["registered_label_role"] is False and rep["passes"]["all"] is False


def test_rank_shortcut_is_out_of_fold():
    """A feature whose direction flips between folds scores well in sample
    (best direction chosen on all data) but not out of fold."""
    n = 400
    y = np.zeros(n, dtype=int)
    y[::10] = 1
    x = np.where(y == 1, 1.0, 0.0)
    x[n // 2 :] = -x[n // 2 :]  # second half: the relation reverses
    w = np.ones(n)
    idx = np.arange(n)
    folds = [(idx[n // 2 :], idx[: n // 2]), (idx[: n // 2], idx[n // 2 :])]
    oof = fg._oof_rank_scores(x, y, w, folds)
    ap_oof = fg._ap(y, oof, w)
    in_sample = max(fg._ap(y, x, w), fg._ap(y, -x, w))
    assert ap_oof < 0.2 < in_sample


def test_cv_never_splits_a_group():
    rng = np.random.default_rng(0)
    groups = np.repeat(np.arange(300), 4)
    y = np.repeat((rng.random(300) < 0.2).astype(int), 4)
    for train, test in fg._folds(y, groups, _prereg()):
        assert not set(groups[train]) & set(groups[test])


def test_bootstrap_resamples_groups():
    order, start, length = fg._group_index(np.array(["b", "a", "b", "c", "a", "b"]))
    # Group codes follow first appearance: b=0, a=1, c=2.
    rows = fg._resample_rows(np.array([0, 2, 0]), order, start, length)
    assert sorted(rows.tolist()) == [0, 0, 2, 2, 3, 5, 5]
    # One row per group: the resample is the draw itself (same numbers as a
    # row bootstrap).
    order, start, length = fg._group_index(np.arange(5))
    assert fg._resample_rows(np.array([4, 4, 1]), order, start, length).tolist() == [4, 4, 1]


def test_report_records_groups_and_libraries():
    df = _frame()
    df["group"] = np.arange(len(df)) // 3
    rep = fg.evaluate_gate(df, _prereg())
    assert rep["n_groups"] == len(df) // 3
    libs = rep["libraries"]
    assert libs["python"] and libs["sklearn"] and libs["numpy"]


def test_runner_version_check_reads_job_pins():
    import importlib.util

    spec = importlib.util.spec_from_file_location("aml_gate_runner", RUNNER_SRC)
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    pins = runner.pinned_deps()
    assert pins["scikit-learn"] and pins["numpy"]
    same = {"numpy": pins["numpy"], "scipy": pins["scipy"], "pandas": pins["pandas"]}
    same |= {"sklearn": pins["scikit-learn"], "joblib": pins["joblib"]}
    same["threadpoolctl"] = pins["threadpoolctl"]
    assert runner.version_mismatches(same) == {}
    off = {**same, "sklearn": "0.0"}
    assert runner.version_mismatches(off) == {
        "scikit-learn": {"pinned": pins["scikit-learn"], "installed": "0.0"}
    }


def test_relative_cap_as_lift_over_prevalence():
    """Under the lift formula a weak full model does not fail every feature
    that edges above prevalence; a real shortcut still fails."""
    df = _frame(n=4000, prev=0.08, separable=False)
    p = _prereg()
    p["leakage"] = {**p["leakage"], "relative_cap_formula": "ratio"}
    ratio = fg.evaluate_gate(df, p)["typologies"]["beh"]
    p = _prereg()
    p["leakage"] = {**p["leakage"], "relative_cap_formula": "lift_over_prevalence"}
    lift = fg.evaluate_gate(df, p)["typologies"]["beh"]
    prev, ap = lift["prevalence"], lift["ap"]
    sc = lift["shortcuts"]["single_feature"]
    assert sc["rel_cap"] == pytest.approx(prev + p["leakage"]["shortcut_ap_rel_max"] * (ap - prev))
    assert sc["rel_cap_formula"] == "lift_over_prevalence"
    assert ratio["shortcuts"]["single_feature"]["rel_cap_formula"] == "ratio"
    planted = fg.evaluate_gate(_frame(), p)["typologies"]["beh"]
    assert planted["shortcuts"]["single_feature"]["pass"] is False
    p["leakage"]["relative_cap_formula"] = "nonsense"
    with pytest.raises(ValueError, match="relative_cap_formula"):
        fg.evaluate_gate(df, p)


def test_rank_shortcut_pools_folds_that_chose_different_directions():
    """Each fold is right in its own direction; pooled on one scale the
    shortcut stays near perfect. Pooling raw signed scores put every row of the
    flipped fold (a nonnegative feature, negated) below every row of the other."""
    n = 400
    y = np.zeros(n, dtype=int)
    y[::10] = 1
    x = np.where(y == 1, 5.0, 1.0)
    half = np.arange(n) >= n // 2
    x[half] = np.where(y[half] == 1, 1.0, 5.0)  # second half: positives low
    w = np.ones(n)
    a, b = np.where(~half)[0], np.where(half)[0]
    folds = [(a, a), (b, b)]
    ap = fg._ap(y, fg._oof_rank_scores(x, y, w, folds), w)
    assert ap > 0.9


def test_unique_groups_keep_stratified_kfold():
    from sklearn.model_selection import StratifiedKFold

    y = (np.random.default_rng(1).random(500) < 0.1).astype(int)
    p = _prereg()
    got = fg._folds(y, np.arange(500), p)
    want = StratifiedKFold(n_splits=p["cv"]["folds"], shuffle=True, random_state=p["cv"]["seed"])
    for (_tr, te), (_tr2, te2) in zip(got, want.split(y.reshape(-1, 1), y), strict=True):
        assert (te == te2).all()


def test_single_class_training_fold_does_not_crash():
    X = np.zeros((10, 1))
    y = np.array([1, 1, 0, 0, 0, 0, 0, 0, 0, 0])
    w = np.ones(10)
    folds = [(np.arange(2, 10), np.arange(0, 2)), (np.arange(0, 10), np.arange(2, 10))]
    oof = fg._oof_scores(lambda: fg._reference_model(_prereg()), X, y, w, folds)
    assert (oof[:2] == 0.0).all()


def test_null_group_is_refused():
    df = _frame()
    df["group"] = None
    with pytest.raises(ValueError, match="NULL group"):
        fg.evaluate_gate(df, _prereg())


def test_exclusions_counts_only_and_lift_ratio():
    df = _frame(n=1500, prev=0.06)
    df["exclude:beh"] = 0
    df.loc[:99, "exclude:beh"] = 1
    rep = fg.evaluate_gate(df, _prereg(), score=False)
    r = rep["typologies"]["beh"]
    assert rep["verdict"] == "counts_only" and r["status"] == "counts_only"
    assert r["n_excluded"] == 100 and r["n_scored"] == 1400 and "ap" not in r
    assert rep["level2"] is None and "passes" not in rep
    assert any("n_excluded=100" in line for line in fg.summary_lines(rep))
    scored = fg.evaluate_gate(df, _prereg())["typologies"]["beh"]
    assert scored["n_scored"] == 1400
    assert scored["ap_over_prevalence"] == pytest.approx(scored["ap"] / scored["prevalence"])


def test_lifetime_prereg_drops_history_features():
    p = _prereg()
    p["unit_of_scoring"] = {"window": "utc_calendar_month", "history_features": ["noise_b"]}
    life = fg.lifetime_prereg(p)
    assert life["features"] == ["planted", "noise_a"]
    assert fg.unit_window(life) == "lifetime" and fg.unit_window(p) == "utc_calendar_month"


def test_counts_only_needs_no_sklearn(monkeypatch):
    monkeypatch.setattr(fg, "_sklearn_available", lambda: False)
    rep = fg.evaluate_gate(_frame(), _prereg(), score=False)
    assert rep["verdict"] == "counts_only" and rep["typologies"]["beh"]["n_positives"] > 0


def test_behavioural_six_and_empty_definitional_subset():
    p = json.loads(PREREG.read_text())
    assert len(p["behavioural_subset"]) == 6 and p["definitional_subset"] == []
    assert (p["level2"]["n"], p["level2"]["k_in_band"]) == (6, 4)
    assert "#36" in p["classification"]["note"]
    q = _prereg()
    q["behavioural_subset"] = ["beh", "defn"]
    q["definitional_subset"] = []
    q["level2"] = {**q["level2"], "n": 2, "k_in_band": 1}
    rep = fg.evaluate_gate(_frame(), q)
    assert rep["passes"]["definitional_check"] is None
    assert "definitional_check" not in rep["typologies"]["defn"]


def test_model_outputs_scores_importance_and_card(tmp_path):
    df = _frame(n=1200)
    df["group"] = np.arange(len(df))
    df["month"] = 3
    df["exclude:defn"] = 0
    df.loc[:9, "exclude:defn"] = 1
    rep = fg.evaluate_gate(df, _prereg(), collect_outputs=True)
    out = rep.pop("_model_outputs")
    sc = out["scores"]
    assert list(sc.columns) == ["group", "month", "typology", "label", "score", "fold"]
    assert (sc["typology"] == "beh").sum() == 1200 and (sc["typology"] == "defn").sum() == 1190
    beh = sc[sc["typology"] == "beh"].set_index("group")
    assert (beh["label"].to_numpy() == df["label:beh"].to_numpy()).all()
    assert set(beh["fold"]) == set(range(_prereg()["cv"]["folds"]))
    assert beh["score"].between(0, 1).all() and str(beh["score"].dtype) == "float32"
    imp = out["importance"]
    top = imp[imp["typology"] == "beh"].sort_values("ap_drop_mean").iloc[-1]
    assert top["feature"] == "planted" and top["ap_drop_mean"] > 0.5
    card = out["card"]
    assert card["features"] == _prereg()["features"] and len(card["features_sha256"]) == 64
    assert (
        card["fitted_hyperparameters"]["beh"]["max_iter"]
        == _prereg()["reference_model"]["max_iter"]
    )
    assert card["importance"]["method"] == "permutation_on_held_out_folds"
    paths = fg.write_model_outputs(out, str(tmp_path / "gate"))
    back = pd.read_parquet(paths["oof_scores"])
    assert len(back) == len(sc)
    assert json.loads(Path(paths["model_card"]).read_text())["unit_key_columns"] == [
        "group",
        "month",
    ]
    # Not collected unless asked, and never in counts-only mode.
    assert "_model_outputs" not in fg.evaluate_gate(df, _prereg())


def _grouped_frame():
    df = _frame(n=1200, separable=False)
    df["group"] = [f"c{i // 3:04d}" for i in range(len(df))]
    df["month"] = [14 + i % 3 for i in range(len(df))]
    return df


def _numbers(rep):
    return {
        t: (r.get("ap"), tuple(r.get("ap_cis") or r.get("ap_ci")))
        for t, r in rep["typologies"].items()
    }


def test_gate_numbers_do_not_depend_on_pulled_row_order(monkeypatch):
    """toPandas() row order depends on the partition count; the bootstrap and
    the model's binning subsample are positional, so the evaluator sorts by the
    unit key first."""
    df = _grouped_frame()
    shuffled = df.sample(frac=1, random_state=7)
    a = _numbers(fg.evaluate_gate(df, _prereg()))
    assert a == _numbers(fg.evaluate_gate(shuffled, _prereg()))
    # Without the sort the shuffle does move the numbers (the test has teeth).
    monkeypatch.setattr(fg, "UNIT_KEY_COLUMNS", ())
    assert a != _numbers(fg.evaluate_gate(shuffled, _prereg()))


def test_importance_failure_keeps_the_gate_numbers(monkeypatch):
    def boom(*a, **k):
        raise MemoryError("no room")

    monkeypatch.setattr(fg, "_permutation_importance", boom)
    df = _grouped_frame()
    rep = fg.evaluate_gate(df, _prereg(), collect_outputs=True)
    assert rep["verdict"] == "ok" and rep["typologies"]["beh"]["ap"] is not None
    card = rep["_model_outputs"]["card"]
    assert card["importance_errors"]["beh"] == "no room"
    assert len(rep["_model_outputs"]["scores"]) > 0
