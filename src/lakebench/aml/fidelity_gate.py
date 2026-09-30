"""AML fidelity gate: pre-registered evaluation over a per-entity frame.

Computes what AML-GOALS sections 3-5a gate on, from a pandas frame of scored
customers (one row per customer, the pre-registered feature columns, one 0/1
``label:<typology>`` column per in-scope typology, optional ``weight``):

- D9/D7: out-of-fold average precision of the reference GBT per typology,
  with a bootstrap CI, n_positives and the min_positives power flag; band
  verdict per typology and the K-of-N Level-2 summary on this corpus.
- D5: best single-feature and best feature-pair shortcut AP (depth-2 tree or
  the reference model on those columns, whichever scores higher), a
  nuisance-only reference model, same CV, each checked against the absolute
  and relative leakage caps; plus leave-one-feature-group-out ablation, with
  the drop from removing every nuisance feature capped by the same formula.
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
#: Optional per-typology exclusion column (1 = the row is not scored for that
#: typology: a partial or non-subject month under the monthly unit).
EXCLUDE_PREFIX = "exclude:"
#: Optional frame column naming the correlated unit (the customer). CV folds
#: never split a group and the bootstrap resamples groups. Absent: each row is
#: its own group.
GROUP_COLUMN = "group"

#: Libraries whose versions the report records (A6 compares like with like).
LIBRARIES = ("numpy", "scipy", "pandas", "sklearn", "joblib", "threadpoolctl", "pyspark")


def library_versions() -> dict[str, str | None]:
    """Python and library versions of this process; None when not importable."""
    import importlib
    import platform

    out: dict[str, str | None] = {"python": platform.python_version()}
    for name in LIBRARIES:
        try:
            out[name] = getattr(importlib.import_module(name), "__version__", None)
        except ImportError:
            out[name] = None
    return out


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


def unit_window(prereg: dict) -> str:
    """ "lifetime" (one row per customer) or "utc_calendar_month"."""
    return (prereg.get("unit_of_scoring") or {}).get("window", "lifetime")


def lifetime_prereg(prereg: dict, secondary: bool = False) -> dict:
    """The pre-registration as the lifetime unit sees it: the monthly history
    features dropped. ``secondary=True`` is the ungated secondary lifetime
    block, which also skips what leakage.secondary_lifetime_skips lists (the
    reference-model shortcut and ablation fits, hours of compute for a block
    that gates nothing)."""
    import copy

    p = copy.deepcopy(prereg)
    if secondary:
        skips = set((p.get("leakage") or {}).get("secondary_lifetime_skips") or [])
        if "also_reference_model" in skips:
            p["shortcut_model"]["also_reference_model"] = False
        if "ablation" in skips:
            p["leakage"].pop("ablation", None)
    hist = set((p.get("unit_of_scoring") or {}).get("history_features", []))
    p["features"] = [f for f in p["features"] if f not in hist]
    p.setdefault("unit_of_scoring", {})["window"] = "lifetime"
    abl = (p.get("leakage") or {}).get("ablation")
    if abl:
        groups = {g: [f for f in fs if f not in hist] for g, fs in abl["feature_groups"].items()}
        abl["feature_groups"] = {g: fs for g, fs in groups.items() if fs}
    return p


def _leakage_sets(prereg: dict, secondary: bool = False) -> tuple[list[str], dict[str, list[str]]]:
    """(nuisance_features, ablation feature_groups) from the leakage block,
    checked against the feature list: the nuisance features must all be
    features, and the groups must partition the features exactly, so a typo
    in the pre-registration fails loudly instead of shrinking the check.
    ``secondary`` (the ungated lifetime block, lifetime_prereg(secondary=True))
    is the only case allowed to run without the ablation."""
    lk = prereg["leakage"]
    features = list(prereg["features"])
    nuis = list(lk.get("nuisance_features") or [])
    missing = [f for f in nuis if f not in features]
    if missing:
        raise ValueError(f"leakage.nuisance_features not in features: {missing}")
    abl = lk.get("ablation")
    if abl is None and nuis and not secondary:
        # Fail closed: registered nuisance features without their ablation
        # would silently skip the gated nuisance drop. Only the caller, never
        # the pre-registration, can declare the ungated secondary block.
        raise ValueError("leakage.ablation is missing while nuisance_features is registered")
    if abl is not None:
        want = {"method": "leave_one_group_out", "model": "reference_model"}
        bad = {k: abl.get(k) for k, v in want.items() if abl.get(k) != v}
        if abl.get("gated") != "nuisance_features":
            bad["gated"] = abl.get("gated")
        if bad:
            raise ValueError(f"unsupported leakage.ablation settings {bad}")
        if lk.get("relative_cap_formula") not in (
            "lift_over_prevalence",
            "lift_over_prevalence_band_floor",
        ):
            raise ValueError("leakage.ablation needs a lift relative_cap_formula")
        if not nuis:
            raise ValueError("leakage.ablation gates nuisance_features, which is empty")
        if not abl.get("feature_groups"):
            raise ValueError("leakage.ablation.feature_groups is empty")
    groups = dict((abl or {}).get("feature_groups") or {})
    if groups:
        members = [f for fs in groups.values() for f in fs]
        if sorted(members) != sorted(features):
            raise ValueError(
                "leakage.ablation.feature_groups must partition features: "
                f"extra {sorted(set(members) - set(features))}, "
                f"missing {sorted(set(features) - set(members))}, "
                f"duplicated {sorted({f for f in members if members.count(f) > 1})}"
            )
    return nuis, groups


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

    spec = {k: v for k, v in prereg["reference_model"].items() if not k.startswith("_")}
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


def _folds(y, groups, prereg: dict):
    """Stratified folds that never split a group. With one row per group this
    is plain StratifiedKFold (same folds, and StratifiedGroupKFold's per-group
    loop costs minutes at a million customers); otherwise
    StratifiedGroupKFold. Both take the pre-registered folds and seed."""
    import numpy as np
    from sklearn.model_selection import StratifiedGroupKFold, StratifiedKFold

    cv = prereg["cv"]
    if not cv["stratified"]:
        raise ValueError("the pre-registered CV is stratified; stratified=false is not implemented")
    X = y.reshape(-1, 1)
    if len(np.unique(groups)) == len(y):
        skf = StratifiedKFold(n_splits=cv["folds"], shuffle=True, random_state=cv["seed"])
        return list(skf.split(X, y))
    sgkf = StratifiedGroupKFold(n_splits=cv["folds"], shuffle=True, random_state=cv["seed"])
    return list(sgkf.split(X, y, groups))


def _ap(y, score, w):
    from sklearn.metrics import average_precision_score

    return float(average_precision_score(y, score, sample_weight=w))


def _parallel(fn, items, jobs=None):
    """Map ``fn`` over ``items`` in threads (tree fits and AP release the
    GIL), keeping order. LB_AML_GATE_JOBS caps the threads; default all."""
    from joblib import Parallel, delayed

    return Parallel(n_jobs=jobs or _jobs(), prefer="threads")(delayed(fn)(i) for i in items)


def _jobs() -> int:
    """Worker threads: LB_AML_GATE_JOBS if a positive integer, else the CPU count."""
    try:
        jobs = int(os.environ.get("LB_AML_GATE_JOBS", ""))
    except ValueError:
        jobs = 0
    if jobs > 0:
        return jobs
    from joblib import cpu_count

    # joblib's count honours the pod's CPU quota and affinity; os.cpu_count()
    # would return every core on the node.
    return cpu_count()


def _oof_scores(make_model, X, y, w, folds, models=None):
    """Out-of-fold scores; ``models``, when a list, receives each fold's
    fitted model (None for a fold scored at the training prevalence)."""
    import numpy as np

    oof = np.zeros(len(y), dtype=float)
    for train, test in folds:
        if len(np.unique(y[train])) < 2:
            # A group holding every positive can leave a training fold with
            # one class; score its test fold as the training prevalence.
            oof[test] = float(np.average(y[train], weights=w[train]))
            if models is not None:
                models.append(None)
            continue
        m = make_model()
        m.fit(X[train], y[train], sample_weight=w[train])
        oof[test] = m.predict_proba(X[test])[:, 1]
        if models is not None:
            models.append(m)
    return oof


def _fit_predict(make_model, X, cols, y, w, train, test, n_threads, controller):
    """One fold on columns ``cols``: fit on the ``train`` rows, score the
    ``test`` rows, with OpenMP capped at ``n_threads`` in this thread
    (HistGradientBoosting output does not depend on the thread count, only
    memory and speed do). Only the fold's own rows are copied.

    ``controller`` is a ThreadpoolController built on the calling (main)
    thread: threadpool_limits() would rescan the loaded libraries here, and
    its dl_iterate_phdr callback needs the GIL while holding the loader lock,
    which deadlocks against another thread importing an extension module."""
    import numpy as np

    if len(np.unique(y[train])) < 2:
        # Same fallback as _oof_scores: a one-class training fold scores its
        # test fold at the training prevalence.
        return np.full(len(test), float(np.average(y[train], weights=w[train])))
    with controller.limit(limits=n_threads, user_api="openmp"):
        m = make_model()
        m.fit(X[np.ix_(train, cols)], y[train], sample_weight=w[train])
        return m.predict_proba(X[np.ix_(test, cols)])[:, 1]


def _oof_many(make_model, X, col_sets, y, w, folds, wide=False):
    """Out-of-fold scores for each column set in ``col_sets``, on the given
    folds (the same numbers _oof_scores gives per set). Narrow sets fan out
    over every (set, fold) with one OpenMP thread per fit. ``wide`` sets (near
    the full feature list: each fit copies its training rows) run one set at a
    time, its folds in parallel sharing the cores, so memory stays bounded."""
    import numpy as np
    from threadpoolctl import ThreadpoolController

    # Built here, on the calling thread, after the model's modules are
    # imported (make_model()), so no worker thread scans or imports libraries.
    make_model()
    controller = ThreadpoolController()
    jobs = _jobs()
    tasks = [(s, f) for s in range(len(col_sets)) for f in range(len(folds))]
    if wide:
        per_fit = max(jobs // len(folds), 1)
        outs = []
        for cols in col_sets:
            outs += _parallel(
                lambda f, cols=cols: _fit_predict(
                    make_model, X, cols, y, w, folds[f][0], folds[f][1], per_fit, controller
                ),
                range(len(folds)),
                jobs=min(len(folds), jobs),
            )
    else:
        outs = _parallel(
            lambda t: _fit_predict(
                make_model, X, col_sets[t[0]], y, w, folds[t[1]][0], folds[t[1]][1], 1, controller
            ),
            tasks,
            jobs=jobs,
        )
    res = [np.zeros(len(y), dtype=float) for _ in col_sets]
    for (s, f), pred in zip(tasks, outs, strict=True):
        res[s][folds[f][1]] = pred
    return res


def _ap_many(make_model, X, col_sets, y, w, folds, wide=False):
    """Out-of-fold AP for each column set; sets are scored a batch at a time
    (one batch per worker count) so the out-of-fold arrays of hundreds of
    pairs are never held at once."""
    step = max(_jobs(), 1)
    aps = []
    for i in range(0, len(col_sets), step):
        batch = col_sets[i : i + step]
        aps += [_ap(y, sc, w) for sc in _oof_many(make_model, X, batch, y, w, folds, wide)]
    return aps


def _permutation_importance(models, X, y, w, folds, features, prereg) -> list[dict]:
    """Per feature: the drop in held-out AP when that feature is permuted in
    each fold's test rows and scored by that fold's model, averaged over folds
    and repeats (model_outputs.importance). Folds without a fitted model are
    skipped."""
    import numpy as np

    spec = prereg["model_outputs"]["importance"]
    if spec["method"] != "permutation_on_held_out_folds":
        raise ValueError(f"unsupported importance method {spec['method']!r}")
    fitted = [(m, test) for m, (_, test) in zip(models, folds, strict=True) if m is not None]
    base = [_ap(y[test], m.predict_proba(X[test])[:, 1], w[test]) for m, test in fitted]

    def one(j):
        rng = np.random.default_rng(prereg["cv"]["seed"] + j)
        drops = []
        for (m, test), b in zip(fitted, base, strict=True):
            for _ in range(spec["n_repeats"]):
                Xp = X[test]  # fancy indexing already copies
                Xp[:, j] = Xp[rng.permutation(len(test)), j]
                drops.append(b - _ap(y[test], m.predict_proba(Xp)[:, 1], w[test]))
        return {
            "feature": features[j],
            "ap_drop_mean": float(np.mean(drops)) if drops else None,
            "ap_drop_std": float(np.std(drops)) if drops else None,
            "n": len(drops),
        }

    # Each call holds a copy of the test fold (about 330 MB per thread at
    # scale 2) and predict_proba is itself multithreaded, so cap the pool;
    # the result does not depend on the thread count.
    return _parallel(one, range(len(features)), jobs=min(_jobs(), spec["max_threads"]))


def _json_params(model) -> dict:
    """get_params() with values JSON can carry."""
    out = {}
    for k, v in model.get_params().items():
        out[k] = v if isinstance(v, (str, int, float, bool, type(None))) else repr(v)
    return out


def _oof_rank_scores(x, y, w, folds):
    """The raw feature as a score, out of fold: each fold's direction is the
    better one on its training folds, applied to its test fold, and the test
    fold's scores are turned into within-fold percentile ranks so folds that
    chose different directions pool on one scale (raw signed values would put
    every row of a flipped fold below every row of the others). NaN ranks
    lowest in either direction (the fill uses feature values only, no labels)."""
    import numpy as np
    from scipy.stats import rankdata

    finite = np.isfinite(x)
    if not finite.any():
        return np.zeros(len(x), dtype=float)
    lo = np.nanmin(x[finite]) - 1
    hi = np.nanmax(x[finite]) + 1
    up = np.where(finite, x, lo)
    down = np.where(finite, -x, -hi)
    oof = np.zeros(len(x), dtype=float)
    for train, test in folds:
        better_up = _ap(y[train], up[train], w[train]) >= _ap(y[train], down[train], w[train])
        s = up[test] if better_up else down[test]
        oof[test] = rankdata(s, method="average") / len(s)
    return oof


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


def _group_index(groups):
    """(codes order, start, length) so the rows of group g are
    order[start[g]:start[g] + length[g]]."""
    import numpy as np
    import pandas as pd

    codes, uniq = pd.factorize(groups)
    order = np.argsort(codes, kind="stable")
    length = np.bincount(codes, minlength=len(uniq))
    start = np.concatenate([[0], np.cumsum(length)[:-1]])
    return order, start, length


def _resample_rows(draw, order, start, length):
    """Row indices for a resample of groups (``draw`` holds group codes)."""
    import numpy as np

    counts = length[draw]
    total = int(counts.sum())
    offsets = np.repeat(start[draw] - np.cumsum(counts) + counts, counts) + np.arange(total)
    return order[offsets]


def _bootstrap_ci(y, score, w, groups, prereg: dict) -> tuple[float | None, float | None]:
    """Percentile CI of AP over resamples of groups (customers), not rows."""
    import numpy as np

    pw = prereg["power"]
    rng = np.random.default_rng(prereg["cv"]["seed"])
    order, start, length = _group_index(groups)
    n_groups = len(length)
    # Draw in chunks of one resample per worker so memory stays bounded at a
    # million customers; the RNG order, and so every number, is unchanged.
    chunk = _jobs()
    vals = []
    left = pw["bootstrap_iterations"]
    while left > 0:
        draws = [
            _resample_rows(rng.integers(0, n_groups, n_groups), order, start, length)
            for _ in range(min(chunk, left))
        ]
        left -= len(draws)
        draws = [idx for idx in draws if y[idx].sum() > 0]
        vals += _parallel(lambda idx: _ap(y[idx], score[idx], w[idx]), draws)
    if not vals:
        return None, None
    tail = (1 - pw["ci_level"]) / 2
    return float(np.quantile(vals, tail)), float(np.quantile(vals, 1 - tail))


# ---------------------------------------------------------------------------
# Per-typology evaluation
# ---------------------------------------------------------------------------


def _evaluate_typology(
    name, X, y, w, groups, features, prereg, kind, score=True, sink=None, secondary=False
) -> dict[str, Any]:
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
    if not score:
        out.update(status="counts_only")
        return out
    if n_pos < prereg["cv"]["folds"] or n_pos == n:
        out.update(status="insufficient_labels", ap=None, ap_ci=[None, None])
        return out

    folds = _folds(y, groups, prereg)
    models: list | None = [] if sink is not None else None
    oof = _oof_scores(lambda: _reference_model(prereg), X, y, w, folds, models)
    if sink is not None:
        fold_of = np.empty(len(y), dtype=np.int8)
        for f, (_, test) in enumerate(folds):
            fold_of[test] = f
        sink["scores"] = {"label": y.astype(np.int8), "score": oof, "fold": fold_of}
        try:
            sink["importance"] = _permutation_importance(models, X, y, w, folds, features, prereg)
        except Exception as e:  # noqa: BLE001 -- an output, never the gate numbers
            sink["importance"], sink["importance_error"] = [], str(e)
        fitted = next((m for m in models or [] if m is not None), None)
        sink["hyperparameters"] = _json_params(fitted) if fitted is not None else None
    ap = _ap(y, oof, w)
    lo, hi = _bootstrap_ci(y, oof, w, groups, prereg)
    out.update(
        status="ok",
        ap=ap,
        ap_ci=[lo, hi],
        ap_over_prevalence=ap / out["prevalence"],
        r_precision=_r_precision(y, oof, w),
    )

    # D5 shortcuts: every single feature and every feature pair, out of fold
    # on the reference model's folds. Each is fit as a depth-2 tree and, when
    # registered, as the reference model on those columns; the larger AP
    # counts (a four-leaf tree cannot isolate a few dozen positives among
    # millions of units).
    sm = prereg["shortcut_model"]
    use_rank = sm.get("single_feature_also_scores_raw_rank")
    use_ref = bool(sm.get("also_reference_model"))
    lk = prereg["leakage"]
    nuis, feat_groups = _leakage_sets(prereg, secondary)
    nf = len(features)

    def ref_model():
        return _reference_model(prereg)

    def tree_ap(cols):
        return _ap(y, _oof_scores(lambda: _shortcut_model(prereg), X[:, cols], y, w, folds), w)

    def one(j):
        tree = tree_ap([j])
        rank = _ap(y, _oof_rank_scores(X[:, j], y, w, folds), w) if use_rank else 0.0
        return {"feature": features[j], "tree_ap": tree, "rank_ap": rank}

    single = _parallel(one, range(nf))
    ref1 = _ap_many(ref_model, X, [[j] for j in range(nf)], y, w, folds) if use_ref else []
    for j, r in enumerate(single):
        r["ref_ap"] = ref1[j] if use_ref else None
        r["ap"] = max(r["tree_ap"], r["rank_ap"], ref1[j] if use_ref else r["tree_ap"])
    single.sort(key=lambda r: -r["ap"])
    combos = [[a, b] for a in range(nf) for b in range(a + 1, nf)]
    pair_tree = _parallel(tree_ap, combos)
    pair_ref = _ap_many(ref_model, X, combos, y, w, folds) if use_ref else []
    pairs = []
    for i, (a, b) in enumerate(combos):
        pr = pair_ref[i] if use_ref else None
        pairs.append(
            {
                "features": [features[a], features[b]],
                "ap": max(pair_tree[i], pr if use_ref else pair_tree[i]),
                "tree_ap": pair_tree[i],
                "ref_ap": pr,
            }
        )
    pairs.sort(key=lambda r: -r["ap"])
    nuis_best = None
    if nuis and "nuisance_only" in lk["shortcut_models"]:
        idx = [features.index(f) for f in nuis]
        nuis_best = {
            "features": nuis,
            "ap": _ap_many(ref_model, X, [idx], y, w, folds, wide=True)[0],
        }

    prev = out["prevalence"]
    formula = lk.get("relative_cap_formula", "ratio")
    if formula == "ratio":
        rel_cap = lk["shortcut_ap_rel_max"] * ap
        lift_base = 0.0  # ablation refuses this formula (_leakage_sets)
    elif formula == "lift_over_prevalence":
        # (shortcut_ap - prevalence) <= rel_max * (ap - prevalence): the cap
        # applies to what each model gains over a random ranking, so a full
        # model that barely beats prevalence does not fail every feature.
        rel_cap = prev + lk["shortcut_ap_rel_max"] * (ap - prev)
        lift_base = prev
    elif formula == "lift_over_prevalence_band_floor":
        # The same lift rule, measured against max(ap, band floor): the
        # question is whether one or two features carry a large share of the
        # signal a typology must have to count, so a weak model (AP near
        # prevalence) cannot shrink the cap to the prevalence itself. In band
        # it equals lift_over_prevalence; below band the cap is at most
        # prev + rel_max * (ap_min - prev), under the absolute cap.
        ref = max(ap, prereg["band"]["ap_min"])
        rel_cap = prev + lk["shortcut_ap_rel_max"] * (ref - prev)
        lift_base = prev
    else:
        raise ValueError(f"unknown leakage.relative_cap_formula {formula!r}")
    shortcuts = {}
    for model, best in (
        ("single_feature", single[0]),
        ("feature_pair", pairs[0] if pairs else None),
        ("nuisance_only", nuis_best),
    ):
        if model not in lk["shortcut_models"] or best is None:
            continue
        s_ap = best["ap"]
        abs_ok = s_ap <= lk["shortcut_ap_abs_max"]
        rel_ok = s_ap <= rel_cap
        shortcuts[model] = {
            "best": best,
            "abs_cap": lk["shortcut_ap_abs_max"],
            "rel_cap": rel_cap,
            "rel_cap_formula": formula,
            "pass_abs": abs_ok,
            "pass_rel": rel_ok,
            "pass": abs_ok and rel_ok,
        }
    out["shortcuts"] = shortcuts
    # Ungated: a reference model that loses to a one- or two-feature tree is
    # not measuring the typology (the D0 v3.4.1 failure mode).
    best_shortcut = max((v["best"]["ap"] for v in shortcuts.values()), default=None)
    out["model_beats_shortcuts"] = None if best_shortcut is None else bool(ap > best_shortcut)

    # Leave-one-feature-group-out: the reference model refit without each
    # group, and without every nuisance feature at once, on the same folds.
    # Every drop is reported; only the nuisance drop is gated
    # (_nuisance_ablation_verdict).
    ablation_ok = True
    if feat_groups:
        drop_sets = [(g, fs) for g, fs in feat_groups.items()]
        if nuis:
            drop_sets.append(("nuisance_features", nuis))
        col_sets = [[j for j, f in enumerate(features) if f not in set(fs)] for _, fs in drop_sets]
        aps_wo = _ap_many(ref_model, X, col_sets, y, w, folds, wide=True)
        rows = [
            {"group": g, "features": list(fs), "ap_without": a, "drop": ap - a}
            for (g, fs), a in zip(drop_sets, aps_wo, strict=True)
        ]
        abl: dict[str, Any] = {"groups": rows}
        if nuis:
            nrow = rows[-1]
            abl["nuisance"] = {
                **nrow,
                **_nuisance_ablation_verdict(ap, nrow["ap_without"], rel_cap, lift_base, prereg),
            }
            ablation_ok = bool(abl["nuisance"]["pass"])
        out["ablation"] = abl
    out["leakage_pass"] = (
        all(s["pass"] for s in shortcuts.values())
        and len(shortcuts) == len(lk["shortcut_models"])
        and ablation_ok
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
        # The 3.5.2 statistic (tree or raw rank), not the reference-model
        # shortcut: the definitional rule must not get easier to pass.
        d_ap = next(
            (max(r["tree_ap"], r["rank_ap"]) for r in single if r["feature"] == dfeat), None
        )
        out["definitional_check"] = {
            "defining_feature": dfeat,
            "single_ap": d_ap,
            "min_single_ap": cls["definitional_min_single_ap"],
            "pass": d_ap is not None and d_ap >= cls["definitional_min_single_ap"],
        }
    return out


def _l2_sensitivity(X, y, w, groups, prereg: dict, values) -> dict[str, float | None]:
    """{str(l2): out-of-fold AP} with the reference model refitted at each
    l2_regularization on the registered folds (ungated D8 secondary)."""
    import copy

    folds = _folds(y, groups, prereg)
    out: dict[str, float | None] = {}
    for v in values:
        p = copy.deepcopy(prereg)
        p["reference_model"]["l2_regularization"] = float(v)
        oof = _oof_scores(lambda p=p: _reference_model(p), X, y, w, folds)
        out[str(float(v))] = _ap(y, oof, w)
    return out


def _nuisance_ablation_verdict(ap, ap_without, rel_cap, lift_base, prereg) -> dict:
    """The gated nuisance ablation (leakage.ablation.drop_cap_definition):
    the drop from removing every nuisance feature is capped like a shortcut
    (the lift share the relative cap allows, never more than the absolute
    cap), and an in-band verdict must hold without them."""
    lk, band = prereg["leakage"], prereg["band"]
    drop = ap - ap_without
    drop_cap = min(rel_cap - lift_base, lk["shortcut_ap_abs_max"])
    full_in_band = band["ap_min"] <= ap <= band["ap_max"]
    floor_ok = (not full_in_band) or ap_without >= band["ap_min"]
    return {
        "drop_cap": drop_cap,
        "pass_drop": drop <= drop_cap,
        "pass_band_floor_without": floor_ok,
        "pass": drop <= drop_cap and floor_ok,
    }


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
    out = {
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
    of = l2.get("original_four")
    if of:
        four = list(of["typologies"])
        if all(t in beh for t in four):
            k4 = sum(c["counts_in_band"] for c in counted if c["typology"] in four)
            ok4 = all(per[t].get("status") == "ok" and per[t].get("leakage_pass") for t in four)
            out["original_four"] = {
                "gated": False,
                "typologies": four,
                "k_in_band": k4,
                "k_required": of["k_in_band"],
                "n": len(four),
                "holds_on_this_corpus": bool(k4 >= of["k_in_band"] and ok4),
                "source": of.get("source"),
            }
        else:
            out["original_four"] = {
                "gated": False,
                "available": False,
                "reason": "level2.original_four names typologies outside behavioural_subset",
            }
    return out


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


def corpus_role(seed, prereg: dict) -> str:
    """calibration / evaluation / robustness by the pre-registered seeds, or
    unknown. R3: tuning happens on calibration only; a report that says
    evaluation or robustness before the freeze is a burned seed."""
    if seed is None:
        return "unknown"
    for role in ("calibration", "evaluation", "robustness"):
        if str(prereg["corpora"].get(f"{role}_seed")) == str(seed):
            return role
    # D8's scale-2 replicates of the calibration corpus (registered in v3.6.0,
    # unchanged through v3.6.1) are calibration corpora: tuning may look at them.
    if str(seed) in {str(s) for s in prereg["corpora"].get("calibration_replicate_seeds") or []}:
        return "calibration"
    return "other"


def _passes(report: dict, prereg: dict) -> dict:
    """Every gate outcome on this corpus in one place. ``verdict == "ok"``
    only says the gate ran; these say whether each gate passed."""
    per = report["typologies"]
    beh = list(prereg["behavioural_subset"])
    dfn = list(prereg["definitional_subset"])
    tm, dn = report.get("timing_mixture"), report.get("density")
    out = {
        "level2_on_this_corpus": bool(report["level2"]["holds_on_this_corpus"]),
        "d5_leakage_behavioural": all(bool(per[t].get("leakage_pass")) for t in beh),
        "d7_k_in_band": report["level2"]["k_in_band"] >= report["level2"]["k_required"],
        # None when no typology is definitional (section 9 #36), so an empty
        # subset neither passes nor fails anything.
        "definitional_check": (
            all(bool((per[t].get("definitional_check") or {}).get("pass")) for t in dfn)
            if dfn
            else None
        ),
        "d2_timing_mixture": None if tm is None else bool(tm["pass"]),
        "d11_density": None if dn is None else bool(dn["pass"]),
    }
    out["all"] = all(v for v in out.values() if v is not None)
    return out


def add_pass(report: dict, name: str, ok: bool) -> None:
    """Record an entry-point check (corpus keying, registered label role) in
    the passes block and fold it into passes.all."""
    passes = report.setdefault("passes", {"all": True})
    passes[name] = bool(ok)
    passes["all"] = bool(passes.get("all", True) and ok)


def evaluate_gate(
    frame,
    prereg: dict,
    *,
    prereg_sha256: str = "",
    timing_counts: dict | None = None,
    density_counts: dict | None = None,
    provenance: dict | None = None,
    score: bool = True,
    collect_outputs: bool = False,
    l2_values: list | None = None,
    secondary: bool = False,
) -> dict[str, Any]:
    """Run every gate on ``frame`` and return one JSON-serialisable report.

    ``l2_values`` (scale_invariance.l2_sensitivity.values) also refits the
    reference model per typology at each l2_regularization and records the
    out-of-fold AP under report["l2_sensitivity"]: an ungated secondary that
    never enters passes.

    ``score=False`` stops before any model: per typology only n_scored,
    n_positives, prevalence and n_excluded (a smoke test that must not look at
    AP), verdict "counts_only", no level2 or passes.

    ``secondary=True`` marks the ungated secondary lifetime block (built with
    lifetime_prereg(secondary=True)), the one run allowed without the nuisance
    ablation.

    ``collect_outputs=True`` also returns the reference model's outputs under
    report["_model_outputs"] (callers pop it and write files): per-unit
    out-of-fold scores, permutation importances on the held-out folds, the
    fitted hyperparameters and a model card."""
    import numpy as np

    if unit_window(prereg) == "lifetime":
        # The lifetime unit has no history window, so no history features.
        prereg = lifetime_prereg(prereg)
    features = list(prereg["features"])
    typologies = in_scope_typologies(prereg)
    # Before any model: a malformed leakage block must not surface only after
    # AP exists (a registered look would be spent on a crash).
    _leakage_sets(prereg, secondary)
    report: dict[str, Any] = {
        "gate": "aml-fidelity",
        "prereg_version": prereg.get("version"),
        "prereg_sha256": prereg_sha256,
        # This module's bytes: D8 refuses to compare runs scored by different
        # gate code (a reference-model change would read as scale variance).
        "gate_code_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "metric": prereg["metric"],
        "unit": unit_window(prereg),
        "provenance": dict(provenance or {}),
        "libraries": library_versions(),
        "corpus_role": corpus_role((provenance or {}).get("corpus_seed"), prereg),
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
    # toPandas() hands rows back in whatever order the last shuffle left them,
    # which depends on the partition count (so on the host's cores). The
    # reference model bins on a positional subsample above 200k rows and the
    # bootstrap numbers groups by first appearance, so AP and its CI would
    # depend on that order. Sort by the unit key (unique per unit).
    order = [c for c in UNIT_KEY_COLUMNS if c in frame.columns]
    if order:
        if GROUP_COLUMN in frame.columns and frame[GROUP_COLUMN].isna().any():
            raise ValueError(f"gate frame has NULL {GROUP_COLUMN} values")
        if frame.duplicated(order).any():
            raise ValueError(f"gate frame has duplicate units on {order}")
        frame = frame.sort_values(order, kind="mergesort")
    frame = frame.reset_index(drop=True)
    report["n_scored_units"] = int(len(frame))
    report["n_scored_customers"] = int(
        frame[GROUP_COLUMN].nunique() if GROUP_COLUMN in frame.columns else len(frame)
    )

    if score and not _sklearn_available():
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
    if GROUP_COLUMN in frame.columns:
        if frame[GROUP_COLUMN].isna().any():
            raise ValueError(f"gate frame has NULL {GROUP_COLUMN} values")
        groups = frame[GROUP_COLUMN].to_numpy()
    else:
        groups = np.arange(len(frame))
    report["n_groups"] = int(len(np.unique(groups)))
    per = {}
    sinks: dict[str, dict] = {}
    sens: dict[str, dict] = {}
    for t in typologies:
        y = frame[LABEL_PREFIX + t].to_numpy(dtype=int)
        keep = np.ones(len(y), dtype=bool)
        if EXCLUDE_PREFIX + t in frame.columns:
            keep = frame[EXCLUDE_PREFIX + t].to_numpy(dtype=int) == 0
        kind = "behavioural" if t in prereg["behavioural_subset"] else "definitional"
        sink = {"rows": np.flatnonzero(keep)} if collect_outputs and score else None
        per[t] = _evaluate_typology(
            t,
            X[keep],
            y[keep],
            w[keep],
            groups[keep],
            features,
            prereg,
            kind,
            score=score,
            sink=sink,
            secondary=secondary,
        )
        if sink is not None and "scores" in sink:
            sinks[t] = sink
        per[t]["n_excluded"] = int((~keep).sum())
        if l2_values and score and per[t].get("status") == "ok":
            sens[t] = _l2_sensitivity(X[keep], y[keep], w[keep], groups[keep], prereg, l2_values)
    report["typologies"] = per
    if l2_values and score:
        report["l2_sensitivity"] = {
            "gated": False,
            "values": [float(v) for v in l2_values],
            "typologies": sens,
        }
    if not score:
        report.update(verdict="counts_only", level2=None)
        return report
    report["level2"] = _level2(per, prereg)
    report["verdict"] = "ok"
    report["passes"] = _passes(report, prereg)
    if collect_outputs:
        report["_model_outputs"] = _model_outputs(frame, sinks, features, prereg, report)
    return report


#: Unit key columns copied into the scores table when present.
UNIT_KEY_COLUMNS = (GROUP_COLUMN, "month")


def _model_outputs(frame, sinks: dict, features: list, prereg: dict, report: dict) -> dict:
    """{"scores": long DataFrame, "importance": DataFrame, "card": dict}.

    scores has one row per scored unit and typology (excluded units have no
    out-of-fold score and are absent): the unit key columns, typology,
    label, score (out-of-fold probability, stored as float32) and fold.
    """
    import numpy as np
    import pandas as pd

    parts, imp = [], []
    keys = [c for c in UNIT_KEY_COLUMNS if c in frame.columns]
    weight = (
        frame["weight"].to_numpy(dtype=float)
        if "weight" in frame.columns
        else np.ones(len(frame), dtype=float)
    )
    for t, sink in sinks.items():
        rows = sink["rows"]
        df = frame.iloc[rows][keys].reset_index(drop=True)
        df["typology"] = t
        df["label"] = sink["scores"]["label"]
        df["score"] = sink["scores"]["score"].astype(np.float32)
        df["fold"] = sink["scores"]["fold"]
        # The unit's weight in fitting and AP (1 unless the cluster sampled
        # negatives), so AP can be recomputed from this table alone (D8).
        df["weight"] = weight[rows]
        parts.append(df)
        imp += [{"typology": t, **r} for r in sink["importance"]]
    scores = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    if len(scores):
        scores["typology"] = scores["typology"].astype("category")
    # Every scored unit's features and weight: the D8 per-feature
    # distribution comparison reads this (scale_invariance.py).
    unit_features = frame[keys].reset_index(drop=True)
    for f in features:
        unit_features[f] = frame[f].to_numpy(dtype=float)
    unit_features["weight"] = weight
    card = {
        "prereg_version": report.get("prereg_version"),
        "prereg_sha256": report.get("prereg_sha256"),
        "unit": report.get("unit"),
        "unit_key_columns": keys,
        "features": features,
        "features_sha256": hashlib.sha256("\n".join(features).encode()).hexdigest(),
        "aml_features_sha256": (report.get("provenance") or {}).get("aml_features_sha256"),
        "reference_model": prereg["reference_model"],
        "fitted_hyperparameters": {t: s.get("hyperparameters") for t, s in sinks.items()},
        "importance_errors": {
            t: s["importance_error"] for t, s in sinks.items() if "importance_error" in s
        },
        "cv": prereg["cv"],
        "importance": prereg["model_outputs"]["importance"],
        "libraries": report.get("libraries"),
        "score_note": "out-of-fold probability from the fold model that did not train on the "
        "unit; excluded units are not scored",
    }
    return {
        "scores": scores,
        "importance": pd.DataFrame(imp),
        "card": card,
        "unit_features": unit_features,
    }


#: Columns of oof_scores covered by its per-typology fingerprint: what D8
#: reads back to recompute AP.
SCORES_FINGERPRINT_COLUMNS = ("group", "label", "score", "weight")


def _canonical(series):
    """A column as int64 codes (integers, bools, float bit patterns with one
    NaN and no negative zero) or str objects, so the same values hash the same
    whichever writer (pandas here, Spark on the cluster) and reader produced
    the dtype."""
    import numpy as np
    import pandas as pd

    if pd.api.types.is_bool_dtype(series) or pd.api.types.is_integer_dtype(series):
        return pd.Series(series.to_numpy(dtype=np.int64))
    if pd.api.types.is_float_dtype(series):
        a = series.to_numpy(dtype=np.float64) + 0.0
        a[np.isnan(a)] = np.nan
        return pd.Series(a.view(np.int64))
    return pd.Series(series.astype(str).to_numpy(dtype=object))


def fingerprint(frame, columns) -> str:
    """Order-independent content hash of ``columns`` of ``frame``: the
    wrapping uint64 sum of per-row hashes. Lets D8 prove a persisted table is
    the one this report scored (a stale file from another run on the same
    corpus has the same row counts)."""
    import numpy as np
    import pandas as pd

    canon = pd.DataFrame({c: _canonical(frame[c].reset_index(drop=True)) for c in columns})
    rows = pd.util.hash_pandas_object(canon, index=False).to_numpy(dtype=np.uint64)
    return format(int(np.sum(rows, dtype=np.uint64)), "016x")


def output_fingerprints(outputs: dict) -> dict:
    """{"oof_scores": {typology: fp}, "unit_features": {column: fp}}: scores
    row-wise per typology over SCORES_FINGERPRINT_COLUMNS, the unit table per
    column (D8 reads it one column at a time)."""
    out: dict[str, dict] = {"oof_scores": {}, "unit_features": {}}
    scores = outputs.get("scores")
    if scores is not None and len(scores):
        typ = scores["typology"].astype(str)
        for t in sorted(typ.unique()):
            out["oof_scores"][t] = fingerprint(scores[typ == t], SCORES_FINGERPRINT_COLUMNS)
    units = outputs.get("unit_features")
    if units is not None:
        out["unit_features"] = {c: fingerprint(units, [c]) for c in units.columns}
    return out


def write_model_outputs(outputs: dict, base: str) -> dict:
    """Write ``base``_oof_scores.parquet, ``base``_feature_importance.parquet,
    ``base``_model_card.json and (when present) ``base``_unit_features.parquet
    locally (snappy parquet). Returns {name: path}."""
    paths = {
        "oof_scores": f"{base}_oof_scores.parquet",
        "feature_importance": f"{base}_feature_importance.parquet",
        "model_card": f"{base}_model_card.json",
    }
    if outputs.get("unit_features") is not None:
        paths["unit_features"] = f"{base}_unit_features.parquet"
        outputs["unit_features"].to_parquet(
            paths["unit_features"], compression="snappy", index=False
        )
    outputs["scores"].to_parquet(paths["oof_scores"], compression="snappy", index=False)
    outputs["importance"].to_parquet(paths["feature_importance"], compression="snappy", index=False)
    with open(paths["model_card"], "w") as fh:
        json.dump(outputs["card"], fh, indent=2, default=str)
    return paths


def summary_lines(report: dict) -> list[str]:
    """Human-readable lines for a driver log or terminal."""
    lines = [
        f"AML gate (prereg v{report.get('prereg_version')}, verdict {report.get('verdict')}, "
        f"{report.get('n_scored_customers')} customers, {report.get('n_scored_units')} units)"
    ]
    for t, r in (report.get("typologies") or {}).items():
        if r.get("status") != "ok":
            lines.append(
                f"  {t}: {r.get('status')} n_scored={r['n_scored']} n_pos={r['n_positives']} "
                f"prevalence={r['prevalence']} n_excluded={r.get('n_excluded')}"
            )
            continue
        sc = r["shortcuts"]
        s1 = sc.get("single_feature", {}).get("best", {})
        s2 = sc.get("feature_pair", {}).get("best", {})
        s3 = sc.get("nuisance_only", {}).get("best", {})
        nab = (r.get("ablation") or {}).get("nuisance") or {}
        lo, hi = (v if v is not None else float("nan") for v in r["ap_ci"])
        lines.append(
            f"  {t} [{r['kind']}]: AP={r['ap']:.3f} CI=[{lo:.3f},{hi:.3f}] "
            f"n_pos={r['n_positives']} in_band={r['in_band']} "
            f"single={s1.get('feature')}:{s1.get('ap', float('nan')):.3f} "
            f"pair={'+'.join(s2.get('features', []))}:{s2.get('ap', float('nan')):.3f} "
            f"nuisance_only={s3.get('ap', float('nan')):.3f} "
            f"nuisance_drop={nab.get('drop', float('nan')):.3f} "
            f"leakage_pass={r['leakage_pass']}"
        )
    l2 = report.get("level2")
    if l2:
        lines.append(
            f"  Level 2 on this corpus: {l2['k_in_band']}/{l2['n']} in band "
            f"(need {l2['k_required']}), leakage all pass={l2['all_pass_leakage']}, "
            f"holds={l2['holds_on_this_corpus']}"
        )
        of = l2.get("original_four") or {}
        if "k_in_band" in of:
            lines.append(
                f"  Original four behavioural (ungated, {of.get('source')}): "
                f"{of['k_in_band']}/{of['n']} in band (need {of['k_required']}), "
                f"holds={of['holds_on_this_corpus']}"
            )
    rep_ = report.get("level2_replication")
    if rep_:
        for t, r in (rep_.get("typologies") or {}).items():
            lines.append(
                f"  replication {t}: predicted {r.get('predicted_ap')} PI {r.get('pi')} "
                f"observed {r.get('observed_ap')} inside={r.get('inside_pi')}"
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
