"""AML fidelity gate: pre-registered evaluation over a per-entity frame.

Computes what AML-GOALS sections 3-5a gate on, from a pandas frame of scored
customers (one row per customer, the pre-registered feature columns, one 0/1
``label:<typology>`` column per in-scope typology, optional ``weight``):

- D9/D7: out-of-fold average precision of the reference GBT per typology,
  with a bootstrap CI, n_positives and the min_positives power flag; band
  verdict per typology and the K-of-N Level-2 summary on this corpus.
- D5: best single-feature and best depth-2 feature-pair shortcut AP, same CV,
  each checked against the absolute and relative leakage caps.
- Definitional check: the defining feature alone must reach
  definitional_min_single_ap.
- D2: timing-mixture shares from counts the caller computed.
- D11: typology row density against the target, from counts the caller
  computed.

Every threshold, fold count, seed, iteration count and model hyperparameter
comes from aml_preregistration.json (R7). A test fails if this module carries
a numeric literal other than 0, 1 or 2.

Self-contained (stdlib plus numpy/pandas/scikit-learn imported at call time)
so it ships flat into the Spark driver ConfigMap next to the scripts, the same
way reference_score.py does.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

PREREG_FILENAME = "aml_preregistration.json"
LABEL_PREFIX = "label:"


# ---------------------------------------------------------------------------
# Pre-registration
# ---------------------------------------------------------------------------


def _prereg_candidates(explicit: str | os.PathLike | None) -> list[Path]:
    here = Path(__file__).resolve().parent
    out = []
    if explicit:
        out.append(Path(explicit))
    env = os.environ.get("LB_AML_PREREG_PATH")
    if env:
        out.append(Path(env))
    # Flat on the Spark driver: the JSON sits next to this module.
    out.append(here / PREREG_FILENAME)
    # In the lakebench package: lakebench/aml/gate.py -> lakebench/spark/data/aml.
    out.append(here.parent / "spark" / "data" / "aml" / PREREG_FILENAME)
    return out


def load_preregistration(path: str | os.PathLike | None = None) -> tuple[dict, str]:
    """(pre-registration dict, sha256 of the file bytes)."""
    for p in _prereg_candidates(path):
        if p.is_file():
            raw = p.read_bytes()
            return json.loads(raw), hashlib.sha256(raw).hexdigest()
    raise FileNotFoundError(
        f"{PREREG_FILENAME} not found (looked in: "
        + ", ".join(str(p) for p in _prereg_candidates(path))
        + ")"
    )


def in_scope_typologies(prereg: dict) -> list[str]:
    return list(prereg["behavioural_subset"]) + list(prereg["definitional_subset"])


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


def _sklearn_available() -> bool:
    try:  # pragma: no cover -- environment probe
        import sklearn  # noqa: F401

        return True
    except ImportError:
        return False


def _reference_model(prereg: dict):
    from sklearn.ensemble import HistGradientBoostingClassifier

    spec = dict(prereg["reference_model"])
    name = spec.pop("estimator")
    if name != "HistGradientBoostingClassifier":
        raise ValueError(f"unsupported reference_model.estimator {name!r}")
    return HistGradientBoostingClassifier(random_state=prereg["cv"]["seed"], **spec)


def _shortcut_model(prereg: dict):
    from sklearn.tree import DecisionTreeClassifier

    spec = prereg["shortcut_model"]
    if spec["estimator"] != "DecisionTreeClassifier":
        raise ValueError(f"unsupported shortcut_model.estimator {spec['estimator']!r}")
    return DecisionTreeClassifier(max_depth=spec["max_depth"], random_state=prereg["cv"]["seed"])


def _folds(y, prereg: dict):
    from sklearn.model_selection import StratifiedKFold

    cv = prereg["cv"]
    if not cv["stratified"]:
        raise ValueError("the pre-registered CV is stratified; stratified=false is not implemented")
    skf = StratifiedKFold(n_splits=cv["folds"], shuffle=True, random_state=cv["seed"])
    return list(skf.split(y.reshape(-1, 1), y))


def _ap(y, score, w):
    from sklearn.metrics import average_precision_score

    return float(average_precision_score(y, score, sample_weight=w))


def _oof_scores(make_model, X, y, w, folds):
    import numpy as np

    oof = np.zeros(len(y), dtype=float)
    for train, test in folds:
        m = make_model()
        m.fit(X[train], y[train], sample_weight=w[train])
        oof[test] = m.predict_proba(X[test])[:, 1]
    return oof


def _rank_ap(x, y, w):
    """AP of the raw feature as a score, best of both directions; NaN ranks
    lowest in either direction."""
    import numpy as np

    finite = np.isfinite(x)
    if not finite.any():
        return 0.0
    lo = np.nanmin(x[finite]) - 1
    hi = np.nanmax(x[finite]) + 1
    up = np.where(finite, x, lo)
    down = np.where(finite, -x, -hi)
    return max(_ap(y, up, w), _ap(y, down, w))


def _r_precision(y, score, w) -> float:
    """Precision at the cut that alerts on as much weight as there are
    positives (precision = recall at that cut): the reference model's operating
    point for the rule-vs-reference comparison, with no chosen threshold."""
    import numpy as np

    order = np.argsort(-score, kind="stable")
    ws = w[order]
    pos_w = float((w * y).sum())
    top = np.cumsum(ws) <= pos_w
    if not top.any():
        top[0] = True
    return float((ws[top] * y[order][top]).sum() / ws[top].sum())


def _bootstrap_ci(y, score, w, prereg: dict) -> tuple[float | None, float | None]:
    import numpy as np

    pw = prereg["power"]
    rng = np.random.default_rng(prereg["cv"]["seed"])
    n = len(y)
    vals = []
    for _ in range(pw["bootstrap_iterations"]):
        idx = rng.integers(0, n, n)
        if y[idx].sum() == 0:
            continue
        vals.append(_ap(y[idx], score[idx], w[idx]))
    if not vals:
        return None, None
    tail = (1 - pw["ci_level"]) / 2
    return float(np.quantile(vals, tail)), float(np.quantile(vals, 1 - tail))


# ---------------------------------------------------------------------------
# Per-typology evaluation
# ---------------------------------------------------------------------------


def _evaluate_typology(name, X, y, w, features, prereg, kind) -> dict[str, Any]:
    import numpy as np

    n_pos = int(y.sum())
    n = int(len(y))
    out: dict[str, Any] = {
        "typology": name,
        "kind": kind,
        "n_scored": n,
        "n_positives": n_pos,
        "prevalence": (float(np.average(y, weights=w)) if n else None),
        "underpowered": n_pos < prereg["power"]["min_positives"],
    }
    if n_pos < prereg["cv"]["folds"] or n_pos == n:
        out.update(status="insufficient_labels", ap=None, ap_ci=[None, None])
        return out

    folds = _folds(y, prereg)
    oof = _oof_scores(lambda: _reference_model(prereg), X, y, w, folds)
    ap = _ap(y, oof, w)
    lo, hi = _bootstrap_ci(y, oof, w, prereg)
    out.update(status="ok", ap=ap, ap_ci=[lo, hi], r_precision=_r_precision(y, oof, w))

    # D5 shortcuts: every single feature and every feature pair, same folds.
    single = []
    for j, f in enumerate(features):
        xj = X[:, [j]]
        tree = _ap(y, _oof_scores(lambda: _shortcut_model(prereg), xj, y, w, folds), w)
        rank = (
            _rank_ap(X[:, j], y, w)
            if prereg["shortcut_model"].get("single_feature_also_scores_raw_rank")
            else 0.0
        )
        single.append({"feature": f, "ap": max(tree, rank), "tree_ap": tree, "rank_ap": rank})
    single.sort(key=lambda r: -r["ap"])
    pairs = []
    for a in range(len(features)):
        for b in range(a + 1, len(features)):
            xab = X[:, [a, b]]
            pap = _ap(y, _oof_scores(lambda: _shortcut_model(prereg), xab, y, w, folds), w)
            pairs.append({"features": [features[a], features[b]], "ap": pap})
    pairs.sort(key=lambda r: -r["ap"])

    lk = prereg["leakage"]
    shortcuts = {}
    for model, best in (
        ("single_feature", single[0]),
        ("feature_pair_depth2", pairs[0] if pairs else None),
    ):
        if model not in lk["shortcut_models"] or best is None:
            continue
        s_ap = best["ap"]
        abs_ok = s_ap <= lk["shortcut_ap_abs_max"]
        rel_ok = s_ap <= lk["shortcut_ap_rel_max"] * ap
        shortcuts[model] = {
            "best": best,
            "abs_cap": lk["shortcut_ap_abs_max"],
            "rel_cap": lk["shortcut_ap_rel_max"] * ap,
            "pass_abs": abs_ok,
            "pass_rel": rel_ok,
            "pass": abs_ok and rel_ok,
        }
    out["shortcuts"] = shortcuts
    out["leakage_pass"] = all(s["pass"] for s in shortcuts.values()) and len(shortcuts) == len(
        lk["shortcut_models"]
    )
    out["single_feature_table"] = single
    out["top_pairs"] = pairs[: len(features)]

    band = prereg["band"]
    out["in_band"] = band["ap_min"] <= ap <= band["ap_max"]
    out["ci_straddles_band_edge"] = lo is not None and any(
        lo < edge < hi for edge in (band["ap_min"], band["ap_max"])
    )

    if kind == "definitional":
        cls = prereg["classification"]
        dfeat = cls["defining_feature"].get(name)
        d_ap = next((r["ap"] for r in single if r["feature"] == dfeat), None)
        out["definitional_check"] = {
            "defining_feature": dfeat,
            "single_ap": d_ap,
            "min_single_ap": cls["definitional_min_single_ap"],
            "pass": d_ap is not None and d_ap >= cls["definitional_min_single_ap"],
        }
    return out


def _level2(per: dict[str, dict], prereg: dict) -> dict[str, Any]:
    l2 = prereg["level2"]
    beh = list(prereg["behavioural_subset"])
    if len(beh) != l2["n"]:
        raise ValueError(f"level2.n = {l2['n']} but behavioural_subset has {len(beh)} entries")
    counted = []
    for t in beh:
        r = per[t]
        hit = r.get("status") == "ok" and r.get("in_band") and not r["underpowered"]
        counted.append({"typology": t, "counts_in_band": bool(hit)})
    k = sum(c["counts_in_band"] for c in counted)
    all_ap = all(per[t].get("status") == "ok" for t in beh)
    all_leak = all(per[t].get("leakage_pass") for t in beh)
    return {
        "k_in_band": k,
        "k_required": l2["k_in_band"],
        "n": l2["n"],
        "all_have_ap": all_ap,
        "all_pass_leakage": bool(all_leak),
        "per_typology": counted,
        "holds_on_this_corpus": bool(k >= l2["k_in_band"] and all_ap and all_leak),
        "note": "Level 2 also requires the evaluation and robustness corpora "
        f"({', '.join(l2['require_on_corpora'])}); this is one corpus.",
    }


def _timing_mixture(counts: dict | None, prereg: dict) -> dict | None:
    if counts is None:
        return None
    tm = prereg["timing_mixture"]
    n = counts["n_cohort"]
    below = counts["n_below_low"] / n if n else None
    above = counts["n_above_high"] / n if n else None
    return {
        **counts,
        "cohort_min_sends": tm["cohort_min_sends"],
        "low_cv_edge": tm["low_cv_edge"],
        "high_cv_edge": tm["high_cv_edge"],
        "share_below_low": below,
        "share_above_high": above,
        "pass": bool(
            n and below >= tm["min_share_below_low"] and above >= tm["min_share_above_high"]
        ),
    }


def _density(counts: dict | None, prereg: dict) -> dict | None:
    if counts is None:
        return None
    dn = prereg["density"]
    total = counts["total_rows"]
    frac = counts["planted_rows"] / total if total else None
    # tolerance_pp is stored as a fraction of rows (0.0002 = 0.02 pp), the same
    # unit as target_frac_rows.
    return {
        **counts,
        "frac_rows": frac,
        "target_frac_rows": dn["target_frac_rows"],
        "tolerance": dn["tolerance_pp"],
        "pass": frac is not None and abs(frac - dn["target_frac_rows"]) <= dn["tolerance_pp"],
    }


def evaluate_gate(
    frame,
    prereg: dict,
    *,
    prereg_sha256: str = "",
    timing_counts: dict | None = None,
    density_counts: dict | None = None,
    provenance: dict | None = None,
) -> dict[str, Any]:
    """Run every gate on ``frame`` and return one JSON-serialisable report."""
    import numpy as np

    features = list(prereg["features"])
    typologies = in_scope_typologies(prereg)
    report: dict[str, Any] = {
        "gate": "aml-fidelity",
        "prereg_version": prereg.get("version"),
        "prereg_sha256": prereg_sha256,
        "metric": prereg["metric"],
        "provenance": dict(provenance or {}),
        "features": features,
        "timing_mixture": _timing_mixture(timing_counts, prereg),
        "density": _density(density_counts, prereg),
    }
    missing = [c for c in features if c not in frame.columns]
    missing += [LABEL_PREFIX + t for t in typologies if LABEL_PREFIX + t not in frame.columns]
    if missing:
        raise ValueError(f"gate frame is missing columns {missing}")
    if "is_customer" in frame.columns:
        frame = frame[frame["is_customer"].astype(bool)]
    report["n_scored_customers"] = int(len(frame))

    if not _sklearn_available():
        report.update(verdict="no_sklearn", typologies={}, level2=None)
        return report
    if len(frame) == 0:
        report.update(verdict="empty_frame", typologies={}, level2=None)
        return report

    X = frame[features].to_numpy(dtype=float)
    w = (
        frame["weight"].to_numpy(dtype=float)
        if "weight" in frame.columns
        else np.ones(len(frame), dtype=float)
    )
    per = {}
    for t in typologies:
        y = frame[LABEL_PREFIX + t].to_numpy(dtype=int)
        kind = "behavioural" if t in prereg["behavioural_subset"] else "definitional"
        per[t] = _evaluate_typology(t, X, y, w, features, prereg, kind)
    report["typologies"] = per
    report["level2"] = _level2(per, prereg)
    report["verdict"] = "ok"
    return report


def summary_lines(report: dict) -> list[str]:
    """Human-readable lines for a driver log or terminal."""
    lines = [
        f"AML gate (prereg v{report.get('prereg_version')}, verdict {report.get('verdict')}, "
        f"{report.get('n_scored_customers')} customers)"
    ]
    for t, r in (report.get("typologies") or {}).items():
        if r.get("status") != "ok":
            lines.append(f"  {t}: {r.get('status')} n_pos={r['n_positives']}")
            continue
        sc = r["shortcuts"]
        s1 = sc.get("single_feature", {}).get("best", {})
        s2 = sc.get("feature_pair_depth2", {}).get("best", {})
        lo, hi = r["ap_ci"]
        lines.append(
            f"  {t} [{r['kind']}]: AP={r['ap']:.3f} CI=[{lo:.3f},{hi:.3f}] "
            f"n_pos={r['n_positives']} in_band={r['in_band']} "
            f"single={s1.get('feature')}:{s1.get('ap', float('nan')):.3f} "
            f"pair={'+'.join(s2.get('features', []))}:{s2.get('ap', float('nan')):.3f} "
            f"leakage_pass={r['leakage_pass']}"
        )
    l2 = report.get("level2")
    if l2:
        lines.append(
            f"  Level 2 on this corpus: {l2['k_in_band']}/{l2['n']} in band "
            f"(need {l2['k_required']}), leakage all pass={l2['all_pass_leakage']}, "
            f"holds={l2['holds_on_this_corpus']}"
        )
    tm = report.get("timing_mixture")
    if tm:
        lines.append(
            f"  D2 timing mixture: cohort={tm['n_cohort']} below={tm['share_below_low']} "
            f"above={tm['share_above_high']} pass={tm['pass']}"
        )
    dn = report.get("density")
    if dn:
        lines.append(
            f"  D11 density: {dn['frac_rows']} (target {dn['target_frac_rows']} "
            f"+/- {dn['tolerance']}) pass={dn['pass']}"
        )
    return lines
