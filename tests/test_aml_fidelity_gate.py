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


def test_prereg_features_match_aml_features():
    tree = ast.parse(FEATURES_SRC.read_text())
    cols = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "FEATURE_COLUMNS" for t in node.targets
        ):
            cols = [e.value for e in node.value.elts]
    p = json.loads(PREREG.read_text())
    assert cols == p["features"]
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
    assert p["version"] == "3.3"
    assert p["band"] == {**p["band"], "ap_min": 0.3, "ap_max": 0.8}
    assert p["reference_model"]["estimator"] == "HistGradientBoostingClassifier"
    assert p["shortcut_model"]["max_depth"] == 2
    assert p["cv"] == {"folds": 5, "stratified": True, "seed": 7}


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
