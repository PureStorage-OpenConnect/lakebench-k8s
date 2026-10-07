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
D8_SRC = ROOT / "src/lakebench/aml/scale_invariance.py"
D8_RUNNER_SRC = ROOT / "scripts/aml_d8.py"
FEATURES_SRC = ROOT / "src/lakebench/spark/scripts/aml_features.py"

# Tests that take 5 s or more on the pinned reference libraries (183 s for the
# whole file, of which these are about 140 s, measured 2026-09-30). CI runs them
# in the "AML statistics (slow)" job, and the unit legs still run them too until
# the fast path (QA-6) deselects the mark there.
SLOW = pytest.mark.slow

PREREG = ROOT / "src/lakebench/spark/data/aml/aml_preregistration.json"
TYPOLOGY_RS = ROOT / "datagen_rs/src/typology.rs"


@pytest.fixture(autouse=True)
def _few_threads(monkeypatch):
    """Tiny frames: OpenMP fan-out across every core costs more than the fit,
    and on a shared host it oversubscribes badly. The gate's own worker
    count follows LB_AML_GATE_JOBS."""
    from threadpoolctl import threadpool_limits

    monkeypatch.setenv("LB_AML_GATE_JOBS", "4")
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
    # v3.6.0 fits the reference model about ten times per typology (single and
    # pair shortcuts, nuisance model, ablations); fewer boosting rounds keep a
    # test in seconds. Tests of the registered model itself restore max_iter.
    p["reference_model"] = {**p["reference_model"], "max_iter": 20}
    # The leakage sets must name real features: noise_b stands in for the
    # nuisance features, and each feature is its own ablation group.
    p["leakage"] = {
        **p["leakage"],
        "nuisance_features": ["noise_b"],
        "ablation": {
            **p["leakage"]["ablation"],
            "feature_groups": {"g_planted": ["planted"], "g_a": ["noise_a"], "g_b": ["noise_b"]},
        },
    }
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


@SLOW
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


@SLOW
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
    assert r["shortcuts"]["feature_pair"]["pass_abs"] is True
    assert rep["typologies"]["defn"]["definitional_check"]["pass"] is False


@SLOW
def test_pair_shortcut_catches_a_conjunction():
    """Label = a AND b: each alone is weak, the pair is exact."""
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
    pair = r["shortcuts"]["feature_pair"]
    assert set(pair["best"]["features"]) == {"planted", "noise_a"}
    assert pair["best"]["ap"] > 0.99
    assert r["shortcuts"]["single_feature"]["best"]["ap"] < 0.5
    assert pair["pass"] is False


@SLOW
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


@SLOW
def test_deterministic_given_the_seed():
    a = fg.evaluate_gate(_frame(separable=False), _prereg())
    b = fg.evaluate_gate(_frame(separable=False), _prereg())
    assert json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)


@SLOW
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


@SLOW
def test_level2_n_must_match_behavioural_subset():
    p = _prereg()
    p["level2"]["n"] = 4
    with pytest.raises(ValueError, match="level2.n"):
        fg.evaluate_gate(_frame(), p)


@SLOW
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


@SLOW
def test_real_preregistration_runs_end_to_end():
    """The shipped JSON has every key the gate reads (small frame, all six
    typologies; the feature list is cut to keep the pair search short and is
    checked against aml_features separately)."""
    p, sha = fg.load_preregistration(PREREG)
    p = copy.deepcopy(p)
    p["power"]["bootstrap_iterations"] = 5
    # v3.6.0 fits the reference model on every pair and every ablation: keep
    # the feature list and the boosting rounds small (the registered values
    # are pinned in test_prereg_version_and_model_blocks).
    p["reference_model"]["max_iter"] = 5
    nuis = p["leakage"]["nuisance_features"]
    p["features"] = p["features"][:1] + nuis[:2]
    p["features"] += list(p["classification"]["defining_feature"].values())
    p["leakage"]["nuisance_features"] = nuis = nuis[:2]
    kept = set(p["features"])
    groups = {
        g: [f for f in fs if f in kept]
        for g, fs in p["leakage"]["ablation"]["feature_groups"].items()
    }
    p["leakage"]["ablation"]["feature_groups"] = {g: fs for g, fs in groups.items() if fs}
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
    r = next(iter(rep["typologies"].values()))
    assert set(r["shortcuts"]) == set(p["leakage"]["shortcut_models"])
    assert r["ablation"]["nuisance"]["features"] == nuis
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


@pytest.mark.parametrize(
    "path",
    [
        GATE_SRC,
        RUNNER_SRC,
        D8_SRC,
        D8_RUNNER_SRC,
        ROOT / "src/lakebench/aml/predictions.py",
        ROOT / "src/lakebench/aml/d8_shards.py",
        ROOT / "scripts/aml_level2_predict.py",
    ],
    ids=lambda p: p.name,
)
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
    # 3.6.1 (Wave 1 A2, 2026-09-28) adds the screening block. Content of D5/D8
    # blocks is unchanged; only the version string, changelog head and
    # prereg_sha256 moved.
    assert p["version"] == "3.6.1"
    assert "#37/#38" in p["_doc"] and p["changelog"][0]["version"] == "3.6.1"
    assert [(c["version"], c.get("part")) for c in p["changelog"][:3]] == [
        ("3.6.1", None),
        ("3.6.0", "D8"),
        ("3.6.0", "D5"),
    ]
    # 3.6.1 head sits above 3.6.0's two entries, so 3.5.x lineage is at 3-5.
    assert [c["version"] for c in p["changelog"][3:6]] == ["3.5.2", "3.5.1", "3.5.0"]
    # registered_looks_open flipped to True 2026-09-27 (owner-approved
    # AML-GOALS #52); stays true through 3.6.1.
    assert p["corpora"]["registered_looks_open"] is True
    assert {42, 50000042} <= set(p["corpora"]["spent_seeds"])
    u = p["unit_of_scoring"]
    assert (u["window"], u["label_role"]) == ("utc_calendar_month", "subject")
    assert (u["lead_in_days"], u["burn_in_months"], u["history_days"]) == (14, 14, 395)
    assert p["leakage"]["relative_cap_formula"] == "lift_over_prevalence_band_floor"
    assert (p["leakage"]["shortcut_ap_abs_max"], p["leakage"]["shortcut_ap_rel_max"]) == (0.2, 0.5)
    assert p["band"] == {**p["band"], "ap_min": 0.3, "ap_max": 0.8}
    assert (p["level2"]["k_in_band"], p["level2"]["n"]) == (4, 6)
    assert p["corpora"]["gate_scale"] == 2
    assert p["reference_model"]["estimator"] == "HistGradientBoostingClassifier"
    assert p["reference_model"]["l2_regularization"] > 0
    assert p["shortcut_model"]["max_depth"] == 2
    assert p["shortcut_model"]["also_reference_model"] is True
    lk = p["leakage"]
    assert lk["shortcut_models"] == ["single_feature", "feature_pair", "nuisance_only"]
    # v3.6.0 adds no threshold: the new checks reuse the 3.5.0 caps.
    assert lk["ablation"]["gated"] == "nuisance_features"
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


@SLOW
def test_passes_summary_and_corpus_role(monkeypatch):
    from tests.fixtures import heldout_test_seeds as ts

    ts.use_fixture(monkeypatch)
    p = _prereg()
    rep = fg.evaluate_gate(
        _frame(), p, provenance={"corpus_seed": p["corpora"]["calibration_seed"]}
    )
    assert rep["corpus_role"] == "calibration"
    assert rep["passes"]["d5_leakage_behavioural"] is False
    assert rep["passes"]["all"] is False
    assert rep["passes"]["d2_timing_mixture"] is None  # not supplied, not counted
    # The evaluation seed is matched by hash (a test-only seed in the fixture).
    assert fg.corpus_role(ts.TEST_EVALUATION_SEED, p) == "evaluation"
    assert fg.corpus_role(7, p) == "other" and fg.corpus_role(None, p) == "unknown"


@SLOW
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


@SLOW
def test_report_records_groups_and_libraries():
    df = _frame()
    df["group"] = np.arange(len(df)) // 3
    df["month"] = np.arange(len(df)) % 3  # a unit is (group, month)
    rep = fg.evaluate_gate(df, _prereg())
    assert rep["n_groups"] == len(df) // 3
    libs = rep["libraries"]
    assert libs["python"] and libs["sklearn"] and libs["numpy"]


def test_runner_version_check_reads_job_pins():
    from tests.conftest import exec_repo_script

    runner = exec_repo_script(RUNNER_SRC, "aml_gate_runner")
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


@SLOW
def test_relative_cap_as_lift_over_prevalence():
    """Under the lift formula a weak full model does not fail every feature
    that edges above prevalence; a real shortcut still fails."""
    df = _frame(n=4000, prev=0.08, separable=False)
    p = _prereg()
    # The legacy ratio formula predates the nuisance ablation, which needs a
    # lift formula (_leakage_sets refuses the pairing).
    p["leakage"] = {
        **p["leakage"],
        "relative_cap_formula": "ratio",
        "shortcut_models": ["single_feature", "feature_pair"],
    }
    del p["leakage"]["ablation"]
    p["leakage"]["nuisance_features"] = []
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


@SLOW
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


@SLOW
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


@SLOW
def test_model_outputs_scores_importance_and_card(tmp_path):
    df = _frame(n=1200)
    df["group"] = np.arange(len(df))
    df["month"] = 3
    df["exclude:defn"] = 0
    df.loc[:9, "exclude:defn"] = 1
    rep = fg.evaluate_gate(df, _prereg(), collect_outputs=True)
    out = rep.pop("_model_outputs")
    sc = out["scores"]
    assert list(sc.columns) == ["group", "month", "typology", "label", "score", "fold", "weight"]
    assert (sc["weight"] == 1).all()
    units = out["unit_features"]
    assert list(units.columns) == ["group", "month", *_prereg()["features"], "weight"]
    assert len(units) == len(df)
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
    assert len(pd.read_parquet(paths["unit_features"])) == len(df)
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


@SLOW
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


@SLOW
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


def test_duplicate_units_are_refused():
    df = _grouped_frame()
    df.loc[1, ["group", "month"]] = df.loc[0, ["group", "month"]].to_numpy()
    with pytest.raises(ValueError, match="duplicate units"):
        fg.evaluate_gate(df, _prereg())


@SLOW
def test_band_floor_cap_equals_lift_in_band_and_floors_below():
    """lift_over_prevalence_band_floor measures the shortcut against
    max(ap, band floor): a weak model no longer shrinks the cap to the
    prevalence, and the cap stays under the absolute cap below band."""
    df = _frame(n=4000, prev=0.08, separable=False)
    p = _prereg()
    p["leakage"] = {**p["leakage"], "relative_cap_formula": "lift_over_prevalence_band_floor"}
    weak = fg.evaluate_gate(df, p)["typologies"]["beh"]
    prev, ap = weak["prevalence"], weak["ap"]
    assert ap < p["band"]["ap_min"]
    sc = weak["shortcuts"]["single_feature"]
    rel = p["leakage"]["shortcut_ap_rel_max"]
    assert sc["rel_cap"] == pytest.approx(prev + rel * (p["band"]["ap_min"] - prev))
    assert sc["rel_cap"] <= p["leakage"]["shortcut_ap_abs_max"]
    assert "model_beats_shortcuts" in weak
    # In band (a strong model) the rule is the v3.4 lift rule.
    strong = fg.evaluate_gate(_frame(), p)["typologies"]["beh"]
    assert strong["ap"] >= p["band"]["ap_min"]
    cap = strong["shortcuts"]["single_feature"]["rel_cap"]
    assert cap == pytest.approx(strong["prevalence"] + rel * (strong["ap"] - strong["prevalence"]))
    assert strong["shortcuts"]["single_feature"]["pass"] is False  # a planted shortcut


def test_reference_model_does_not_saturate_at_rare_prevalence():
    """The v3.4.1 model (no l2) pushed rare positives to scores of exactly
    0 or 1; the registered model keeps scores inside (0, 1) and ranks a
    learnable rare positive class well above prevalence."""
    rng = np.random.default_rng(3)
    n = 60000
    y = (rng.random(n) < 4e-4).astype(int)
    df = pd.DataFrame(
        {
            "planted": y * rng.normal(2.0, 1.0, n) + rng.normal(0, 1, n),
            "noise_a": rng.normal(0, 1, n),
            "noise_b": rng.normal(0, 1, n),
            "is_customer": True,
            "label:beh": y,
            "label:defn": y,
        }
    )
    p = _prereg()
    p["power"] = {**p["power"], "min_positives": 5}
    p["reference_model"] = fg.load_preregistration(PREREG)[0]["reference_model"]
    X, yy = df[p["features"]].to_numpy(float), df["label:beh"].to_numpy(int)
    folds = fg._folds(yy, np.arange(n), p)
    oof = fg._oof_scores(lambda: fg._reference_model(p), X, yy, np.ones(n), folds)
    assert ((oof > 0) & (oof < 1)).all()
    assert fg._ap(yy, oof, np.ones(n)) > 10 * yy.mean()


# ---------------------------------------------------------------------------
# v3.6.0 D5: reference-model shortcuts, nuisance-only model, ablation
# ---------------------------------------------------------------------------


def test_real_prereg_leakage_sets_are_consistent():
    """The shipped nuisance list names real features and the ablation groups
    partition the feature list, for the monthly and the lifetime unit."""
    p, _ = fg.load_preregistration(PREREG)
    nuis, groups = fg._leakage_sets(p)
    assert nuis == [
        "frac_overnight",
        "frac_weekend",
        "hour_of_day_entropy",
        "frac_round_amount",
        "customer_type",
        "crr_tier",
    ]
    assert set(nuis) == set(groups["clock"] + groups["rounding"] + groups["segment"])
    life = fg.lifetime_prereg(p)
    _, lgroups = fg._leakage_sets(life)
    assert "history" not in lgroups


def test_leakage_sets_refuse_a_bad_prereg():
    p = _prereg()
    p["leakage"] = {**p["leakage"], "nuisance_features": ["hour_of_day_entropy"]}
    with pytest.raises(ValueError, match="nuisance_features"):
        fg._leakage_sets(p)
    q = _prereg()
    q["leakage"]["ablation"] = {
        **q["leakage"]["ablation"],
        "feature_groups": {"a": ["planted", "noise_a"], "b": ["noise_a"]},
    }
    with pytest.raises(ValueError, match="partition"):
        fg._leakage_sets(q)


def test_lifetime_prereg_drops_history_from_ablation_groups():
    p = _prereg()
    p["unit_of_scoring"] = {"window": "utc_calendar_month", "history_features": ["noise_a"]}
    life = fg.lifetime_prereg(p)
    assert "g_a" not in life["leakage"]["ablation"]["feature_groups"]
    fg._leakage_sets(life)


@SLOW
def test_oof_many_matches_oof_scores():
    """The parallel fold fitter gives the numbers the sequential one gives,
    narrow or wide."""
    df = _frame(n=1500, prev=0.06, separable=False)
    p = _prereg()
    X = df[p["features"]].to_numpy(float)
    y = df["label:beh"].to_numpy(int)
    w = np.ones(len(y))
    folds = fg._folds(y, np.arange(len(y)), p)
    sets = [[0], [1, 2], [0, 1, 2]]
    for wide in (False, True):
        got = fg._oof_many(lambda: fg._reference_model(p), X, sets, y, w, folds, wide)
        for cols, g in zip(sets, got, strict=True):
            want = fg._oof_scores(lambda: fg._reference_model(p), X[:, cols], y, w, folds)
            np.testing.assert_array_equal(g, want)


def _band_frame(n=20000, seed=5):
    """Label = both features inside a narrow central band: needs four splits,
    so a depth-2 tree cannot isolate it and the reference model can."""
    rng = np.random.default_rng(seed)
    a = rng.normal(0, 1, n)
    b = rng.normal(0, 1, n)
    y = ((np.abs(a) < 0.25) & (np.abs(b) < 0.25)).astype(int)
    return pd.DataFrame(
        {
            "planted": a,
            "noise_a": b,
            "noise_b": rng.normal(0, 1, n),
            "label:beh": y,
            "label:defn": y,
        }
    )


@SLOW
def test_reference_model_pair_catches_what_a_depth2_tree_misses():
    rep = fg.evaluate_gate(_band_frame(), _prereg())
    pair = rep["typologies"]["beh"]["shortcuts"]["feature_pair"]
    best = pair["best"]
    assert set(best["features"]) == {"planted", "noise_a"}
    assert best["tree_ap"] < 0.5 < best["ref_ap"]
    assert best["ap"] == best["ref_ap"] and pair["pass"] is False
    # Without the registered flag the v3.5.2 tree-only check is what remains.
    p = _prereg()
    p["shortcut_model"] = {**p["shortcut_model"], "also_reference_model": False}
    old = fg.evaluate_gate(_band_frame(), p)["typologies"]["beh"]["shortcuts"]["feature_pair"]
    assert old["best"]["ap"] < 0.5 and old["best"]["ref_ap"] is None


@SLOW
def test_nuisance_only_model_and_ablation_catch_a_nuisance_leak():
    """A label readable from the nuisance feature fails nuisance_only, and
    the model loses most of its lift without it (the gated ablation)."""
    rng = np.random.default_rng(9)
    n = 6000
    y = (rng.random(n) < 0.05).astype(int)
    df = pd.DataFrame(
        {
            "planted": y * rng.normal(1.0, 1.0, n) + rng.normal(0, 1, n),
            "noise_a": rng.normal(0, 1, n),
            "noise_b": y + rng.normal(0, 0.05, n),
            "label:beh": y,
            "label:defn": y,
        }
    )
    r = fg.evaluate_gate(df, _prereg())["typologies"]["beh"]
    nu = r["shortcuts"]["nuisance_only"]
    assert nu["best"]["features"] == ["noise_b"] and nu["best"]["ap"] > 0.9
    assert nu["pass"] is False
    nab = r["ablation"]["nuisance"]
    assert nab["drop"] > nab["drop_cap"] and nab["pass"] is False
    assert r["leakage_pass"] is False
    assert {g["group"] for g in r["ablation"]["groups"]} == {
        "g_planted",
        "g_a",
        "g_b",
        "nuisance_features",
    }


@SLOW
def test_clean_nuisance_passes_ablation():
    """Signal in a behaviour feature, noise in the nuisance one: the
    nuisance-only model sits near prevalence and dropping it costs nothing."""
    rng = np.random.default_rng(11)
    n = 6000
    y = (rng.random(n) < 0.05).astype(int)
    df = pd.DataFrame(
        {
            "planted": y * rng.normal(1.5, 1.0, n) + rng.normal(0, 1, n),
            "noise_a": rng.normal(0, 1, n),
            "noise_b": rng.normal(0, 1, n),
            "label:beh": y,
            "label:defn": y,
        }
    )
    r = fg.evaluate_gate(df, _prereg())["typologies"]["beh"]
    assert r["shortcuts"]["nuisance_only"]["pass"] is True
    nab = r["ablation"]["nuisance"]
    assert nab["pass"] is True
    prev, ap = r["prevalence"], r["ap"]
    rel = _prereg()["leakage"]["shortcut_ap_rel_max"]
    assert nab["drop_cap"] == pytest.approx(rel * (max(ap, _prereg()["band"]["ap_min"]) - prev))
    planted = next(g for g in r["ablation"]["groups"] if g["group"] == "g_planted")
    assert planted["drop"] > nab["drop"]  # reported, not gated


def test_nuisance_ablation_verdict_caps_and_band_floor():
    """The drop is capped like a shortcut (lift share, never above the
    absolute cap), and an in-band AP must stay above the band floor without
    the nuisance features."""
    p = _prereg()
    lk, band = p["leakage"], p["band"]
    prev = 1e-4
    ref = 0.45
    rel_cap = prev + lk["shortcut_ap_rel_max"] * (ref - prev)
    # In band, drop 0.17 is under both caps, but the model without the
    # nuisance features falls below the floor: fail.
    v = fg._nuisance_ablation_verdict(ref, ref - 0.17, rel_cap, prev, p)
    assert v["pass_drop"] is True and v["pass_band_floor_without"] is False and v["pass"] is False
    # Same drop from a stronger model that stays in band without them: pass.
    ap = 0.7
    rel_cap = prev + lk["shortcut_ap_rel_max"] * (ap - prev)
    v = fg._nuisance_ablation_verdict(ap, ap - 0.17, rel_cap, prev, p)
    assert v["pass"] is True
    # The absolute cap bounds the drop even when the lift share is larger.
    v = fg._nuisance_ablation_verdict(ap, ap - (lk["shortcut_ap_abs_max"] + 0.01), rel_cap, prev, p)
    assert v["drop_cap"] == lk["shortcut_ap_abs_max"] and v["pass"] is False
    # Below band the floor rule does not apply (the typology is a miss anyway).
    low = band["ap_min"] / 2
    rel_cap = prev + lk["shortcut_ap_rel_max"] * (band["ap_min"] - prev)
    assert fg._nuisance_ablation_verdict(low, low - 0.01, rel_cap, prev, p)["pass"] is True


def test_leakage_block_fails_closed():
    p = _prereg()
    del p["leakage"]["ablation"]
    with pytest.raises(ValueError, match="ablation is missing"):
        fg.evaluate_gate(_frame(), p)
    # Missing ablation is refused even when nuisance_only is not registered.
    p["leakage"]["shortcut_models"] = ["single_feature", "feature_pair"]
    with pytest.raises(ValueError, match="ablation is missing"):
        fg._leakage_sets(p)
    q = _prereg()
    q["leakage"] = {**q["leakage"], "relative_cap_formula": "ratio"}
    with pytest.raises(ValueError, match="lift"):
        fg._leakage_sets(q)
    r = _prereg()
    r["leakage"]["ablation"] = {**r["leakage"]["ablation"], "gated": "clock"}
    with pytest.raises(ValueError, match="unsupported"):
        fg._leakage_sets(r)


def test_secondary_lifetime_skips_the_new_fits():
    p = _prereg()
    life = fg.lifetime_prereg(p, secondary=True)
    assert life["shortcut_model"]["also_reference_model"] is False
    assert "ablation" not in life["leakage"]
    # Only the caller can declare the secondary block: the same prereg on
    # the primary path is refused.
    with pytest.raises(ValueError, match="ablation is missing"):
        fg.evaluate_gate(_frame(), life)
    rep = fg.evaluate_gate(_frame(), life, secondary=True)
    r = rep["typologies"]["beh"]
    assert "ablation" not in r and set(r["shortcuts"]) == set(p["leakage"]["shortcut_models"])
    assert r["shortcuts"]["feature_pair"]["best"]["ref_ap"] is None
    # The primary lifetime conversion keeps every check.
    assert fg.lifetime_prereg(p)["shortcut_model"]["also_reference_model"] is True


@SLOW
def test_definitional_check_keeps_the_352_statistic():
    rep = fg.evaluate_gate(_band_frame(), _prereg())
    d = rep["typologies"]["defn"]
    row = next(r for r in d["single_feature_table"] if r["feature"] == "planted")
    assert d["definitional_check"]["single_ap"] == max(row["tree_ap"], row["rank_ap"])


def test_oof_many_one_class_fold_and_batches(monkeypatch):
    """Every positive in one customer: that fold trains on one class and
    falls back to the training prevalence, as _oof_scores does; batching
    (one set per batch) does not change any AP."""
    rng = np.random.default_rng(4)
    n = 600
    groups = rng.integers(0, 60, n)
    y = (groups == 7).astype(int)
    X = rng.normal(0, 1, (n, 3))
    w = np.ones(n)
    p = _prereg()
    folds = fg._folds(y, groups, p)
    assert any(len(np.unique(y[tr])) < 2 for tr, _ in folds)
    sets = [[0], [1], [0, 2]]
    got = fg._oof_many(lambda: fg._reference_model(p), X, sets, y, w, folds)
    for cols, g in zip(sets, got, strict=True):
        want = fg._oof_scores(lambda: fg._reference_model(p), X[:, cols], y, w, folds)
        np.testing.assert_array_equal(g, want)
    wide = fg._ap_many(lambda: fg._reference_model(p), X, sets, y, w, folds)
    monkeypatch.setenv("LB_AML_GATE_JOBS", "1")
    assert fg._ap_many(lambda: fg._reference_model(p), X, sets, y, w, folds) == wide
