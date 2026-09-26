"""AML D8: scale invariance between two fidelity-gate runs on the same seed.

AML-GOALS section 5a, D8: "equivalence test: bootstrap 95% CI of (scale-10
AP - scale-1 AP) lies entirely within [-0.05, +0.05], per behavioural
typology with >= min_positives; KS statistic < 0.10 per feature". The gate
scale is corpora.gate_scale (section 9 #36), so the small run is the gate
scale and the large run the cluster scale.

Inputs are the two gate reports (``aml_gate_report.json`` from the cluster
job, or the ``--out`` JSON of scripts/aml_gate.py), as local paths or
``s3://`` / ``s3a://`` URIs. Each report names its persisted model outputs;
D8 reads two of them:

- ``oof_scores``: per scored unit and typology, the out-of-fold score, label,
  weight and customer (``group``). AP and its bootstrap are recomputed from it.
- ``unit_features``: per scored unit, every pre-registered feature and the
  unit's weight. The per-feature KS statistic is computed from it.

What passes, all of it required (the verdict is the conjunction):

- Provenance: both reports ran (verdict ok) under the pre-registration file
  loaded here (sha256 equal), on the same verified seed, the small run at the
  gate scale and the large run at a larger scale, on the same unit, feature
  list and numerical libraries.
- Integrity: each scores table agrees with its report on n_scored and
  n_positives per typology, and each unit table on n_scored_units, so the
  files are the ones the report scored.
- AP equivalence, per behavioural typology: both runs have status ok and at
  least power.min_positives positives, and the percentile CI (power.ci_level,
  power.bootstrap_iterations resamples of customers, drawn independently in
  each run) of AP_large - AP_small lies inside
  [-scale_invariance.ap_diff_abs_max, +scale_invariance.ap_diff_abs_max].
- Distribution match, per pre-registered feature: the weighted two-sample KS
  statistic between the runs' scored units is below
  scale_invariance.ks_stat_max. It is an effect size, not a test: at these
  sample sizes its sampling noise is about 1.36/sqrt(n_eff), far below the
  tolerance. NaN (an undefined feature, e.g. gap_cv with one send) is placed
  below every finite value, so a shift in the undefined share moves the
  statistic like any other mass.

Multiple comparisons. The claim "invariant" is the intersection of every
component, so the procedure is an intersection-union test (Berger 1982): the
chance of declaring a scale-variant corpus invariant is at most the largest
single-component error, no correction needed. The cost falls the other way:
each extra component is another chance to fail an invariant corpus, which is
why every AP CI is reported with its width.

R7: every tolerance, iteration count, CI level, seed and power floor comes
from aml_preregistration.json; a unit test fails if this module carries a
numeric literal other than 0, 1 or 2. Any missing input, missing typology,
missing column or provenance mismatch fails closed (verdict "fail" or
"error", never "pass").
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any

from lakebench.aml.fidelity_gate import load_preregistration

GATE_NAME = "aml-d8-scale-invariance"
#: Numerical packages that must agree between the runs (A6: like with like).
#: Python itself is recorded but not compared: the cluster driver runs 3.10.
COMPARED_LIBRARIES = ("numpy", "scipy", "pandas", "sklearn", "joblib", "threadpoolctl")
S3_SCHEMES = ("s3://", "s3a://")


# ---------------------------------------------------------------------------
# Reading inputs (local paths or S3 URIs)
# ---------------------------------------------------------------------------


def _is_s3(uri: str) -> bool:
    return str(uri).startswith(S3_SCHEMES)


def _s3_filesystem(endpoint: str | None):
    """pyarrow S3 filesystem, path-style (FlashBlade has no virtual host).
    Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY and the
    endpoint from ``endpoint`` or AWS_ENDPOINT_URL; nothing is logged."""
    from pyarrow import fs

    endpoint = endpoint or os.environ.get("AWS_ENDPOINT_URL")
    kwargs: dict[str, Any] = {
        "region": os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
    }
    if endpoint:
        scheme, _, host = endpoint.partition("://")
        if not host:
            scheme, host = "https", endpoint
        kwargs.update(endpoint_override=host, scheme=scheme, force_virtual_addressing=False)
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        kwargs.update(
            access_key=os.environ["AWS_ACCESS_KEY_ID"],
            secret_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        )
    return fs.S3FileSystem(**{k: v for k, v in kwargs.items() if v is not None})


def _resolve(uri: str, endpoint: str | None):
    """(pyarrow filesystem, path inside it)."""
    from pyarrow import fs

    uri = str(uri)
    if _is_s3(uri):
        return _s3_filesystem(endpoint), uri.split("://", 1)[1]
    return fs.LocalFileSystem(), os.path.abspath(uri)


def read_bytes(uri: str, endpoint: str | None = None) -> bytes:
    fsys, path = _resolve(uri, endpoint)
    with fsys.open_input_stream(path) as fh:
        return fh.read()


def read_parquet_columns(
    uri: str, columns: list[str], endpoint: str | None = None, typology: str | None = None
):
    """A pandas frame with just ``columns`` from a parquet file or a Spark
    output directory, optionally only the rows of one typology (read with a
    filter, so a 30M-row scores table is never held whole). Raises when a
    column is absent."""
    import pyarrow.dataset as ds

    fsys, path = _resolve(uri, endpoint)
    dset = ds.dataset(path, filesystem=fsys, format="parquet")
    missing = [c for c in columns if c not in dset.schema.names]
    if missing:
        raise ValueError(f"{uri}: missing columns {missing}")
    filt = None
    if typology is not None:
        # The local runner writes the typology as a pandas category
        # (dictionary-encoded), the cluster as a plain string; cast both.
        import pyarrow as pa

        filt = ds.field("typology").cast(pa.string()) == typology
    return dset.to_table(columns=columns, filter=filt).to_pandas()


def parquet_columns(uri: str, endpoint: str | None = None) -> list[str]:
    import pyarrow.dataset as ds

    fsys, path = _resolve(uri, endpoint)
    return list(ds.dataset(path, filesystem=fsys, format="parquet").schema.names)


def load_run(report_uri: str, endpoint: str | None = None) -> dict:
    """{"uri", "sha256", "report"}; raises on a missing or unreadable report."""
    raw = read_bytes(report_uri, endpoint)
    return {
        "uri": str(report_uri),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "report": json.loads(raw),
    }


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


class _APResampler:
    """Weighted average precision (sklearn's definition: sum over distinct
    score thresholds of recall gain x precision) of one run, and of its
    bootstrap resamples of customers. Scores are sorted once; a resample
    multiplies each row's weight by how many times its customer was drawn,
    which gives the same AP as the replicated rows."""

    def __init__(self, y, score, w, groups):
        import numpy as np
        import pandas as pd

        order = np.argsort(-np.asarray(score, dtype=float), kind="stable")
        s = np.asarray(score, dtype=float)[order]
        self.y = np.asarray(y, dtype=float)[order]
        self.w = np.asarray(w, dtype=float)[order]
        codes, uniq = pd.factorize(np.asarray(groups)[order])
        self.codes = codes
        self.n_groups = len(uniq)
        # Last row of each block of tied scores: the curve's thresholds (the
        # appended NaN makes the final row an end).
        self.ends = np.flatnonzero(np.diff(s, append=np.nan) != 0)

    def ap(self, mult=None) -> float | None:
        import numpy as np

        w = self.w if mult is None else self.w * mult[self.codes]
        tp = np.cumsum(w * self.y)[self.ends]
        ps = np.cumsum(w)[self.ends]
        if not len(tp) or tp[-1] <= 0:
            return None
        precision = np.divide(tp, ps, out=np.ones_like(tp), where=ps > 0)
        recall = tp / tp[-1]
        return float(np.sum(np.diff(recall, prepend=0) * precision))

    def draw(self, rng):
        import numpy as np

        return np.bincount(
            rng.integers(0, self.n_groups, self.n_groups), minlength=self.n_groups
        ).astype(float)


def ap_difference_ci(small: dict, large: dict, prereg: dict) -> dict:
    """Point AP of each run, and the percentile CI of AP_large - AP_small
    over independent customer resamples of each run (power block of the
    pre-registration; the RNG is seeded with cv.seed, the gate's own seed).
    ``small``/``large`` hold arrays y, score, w, groups."""
    import numpy as np

    pw = prereg["power"]
    a = _APResampler(**small)
    b = _APResampler(**large)
    ap_s, ap_l = a.ap(), b.ap()
    rng = np.random.default_rng(prereg["cv"]["seed"])
    diffs = []
    n_degenerate = 0
    for _ in range(pw["bootstrap_iterations"]):
        ds_, dl = a.ap(a.draw(rng)), b.ap(b.draw(rng))
        if ds_ is None or dl is None:
            n_degenerate += 1
            continue
        diffs.append(dl - ds_)
    tail = (1 - pw["ci_level"]) / 2
    ok = bool(diffs) and n_degenerate == 0
    lo: float | None = float(np.quantile(diffs, tail)) if ok else None
    hi: float | None = float(np.quantile(diffs, 1 - tail)) if ok else None
    return {
        "ap_small": ap_s,
        "ap_large": ap_l,
        "ap_diff": None if ap_s is None or ap_l is None else ap_l - ap_s,
        "diff_ci": [lo, hi],
        "diff_ci_width": None if lo is None or hi is None else hi - lo,
        "n_resamples": len(diffs),
        # A resample without a positive has no AP; any such draw voids the CI
        # (fail closed) rather than biasing it by being dropped.
        "n_degenerate_resamples": n_degenerate,
    }


def weighted_ks(a, wa, b, wb) -> float:
    """Two-sample KS statistic sup |F_a - F_b| with per-row weights. NaN is
    mapped below every finite value, so it is compared as a point mass."""
    import numpy as np

    def prep(x, w):
        x = np.asarray(x, dtype=float)
        x = np.where(np.isnan(x), -np.inf, x)
        o = np.argsort(x, kind="stable")
        cw = np.cumsum(np.asarray(w, dtype=float)[o])
        return x[o], np.concatenate([[0], cw / cw[-1]])

    xa, ca = prep(a, wa)
    xb, cb = prep(b, wb)
    grid = np.unique(np.concatenate([xa, xb]))
    fa = ca[np.searchsorted(xa, grid, side="right")]
    fb = cb[np.searchsorted(xb, grid, side="right")]
    return float(np.max(np.abs(fa - fb)))


# ---------------------------------------------------------------------------
# The check
# ---------------------------------------------------------------------------


def _output_uri(run: dict, name: str) -> str:
    entry = (run["report"].get("model_outputs") or {}).get(name)
    if not isinstance(entry, dict) or not entry.get("path"):
        raise ValueError(f"{run['uri']}: report lists no model_outputs.{name} path")
    return str(entry["path"])


def _provenance(run: dict, prereg: dict, sha: str) -> tuple[dict, list[str]]:
    """Facts D8 needs from one report, plus the reasons it cannot be used."""
    rep = run["report"]
    prov = rep.get("provenance") or {}
    scale = prov.get("corpus_scale") or {}
    seed_check = prov.get("corpus_seed_check") or {}
    facts = {
        "report": run["uri"],
        "report_sha256": run["sha256"],
        "verdict": rep.get("verdict"),
        "prereg_sha256": rep.get("prereg_sha256"),
        "corpus_seed": prov.get("corpus_seed"),
        "corpus_seed_matched_share": seed_check.get("matched_share"),
        "corpus_role": rep.get("corpus_role"),
        "n_entities": scale.get("n_entities"),
        "scale": scale.get("scale"),
        "unit": rep.get("unit"),
        "adapter": prov.get("adapter"),
        "git_sha": prov.get("git_sha"),
        "libraries": rep.get("libraries"),
    }
    why = []
    if rep.get("verdict") != "ok":
        why.append(f"report verdict {rep.get('verdict')!r}, not 'ok'")
    if rep.get("prereg_sha256") != sha:
        why.append("report was scored under a different pre-registration file (sha256)")
    if prov.get("corpus_seed") in (None, ""):
        why.append("report records no corpus_seed")
    if seed_check.get("matched_share") != 1:
        why.append("corpus seed not verified against the manifest (matched_share != 1)")
    if scale.get("n_entities") is None:
        why.append("report records no corpus_scale")
    if rep.get("features") != list(prereg["features"]):
        why.append("report feature list differs from the pre-registration's")
    return facts, why


def _typology_arrays(sub) -> dict:
    return {
        "y": sub["label"].to_numpy(dtype=int),
        "score": sub["score"].to_numpy(dtype=float),
        "w": sub["weight"].to_numpy(dtype=float),
        "groups": sub["group"].to_numpy(),
    }


def _weights_ok(w) -> bool:
    import numpy as np

    return bool(len(w)) and bool(np.all(np.isfinite(w))) and bool(np.all(w > 0))


def evaluate_d8(
    small_report: str,
    large_report: str,
    *,
    prereg_path: str | None = None,
    endpoint: str | None = None,
) -> dict[str, Any]:
    """Run D8 on two gate reports and return the verdict dict. Never raises
    for bad inputs: an unreadable input gives verdict "error"."""
    try:
        return _evaluate(small_report, large_report, prereg_path, endpoint)
    except Exception as e:  # noqa: BLE001 -- fail closed with the reason
        return {
            "gate": GATE_NAME,
            "verdict": "error",
            "pass": False,
            "errors": [f"{type(e).__name__}: {e}"],
            "inputs": {"small": str(small_report), "large": str(large_report)},
        }


def _evaluate(small_report, large_report, prereg_path, endpoint) -> dict[str, Any]:
    prereg, sha = load_preregistration(prereg_path)
    si = prereg["scale_invariance"]
    pw = prereg["power"]
    corpora = prereg["corpora"]
    typologies = list(prereg["behavioural_subset"])
    features = list(prereg["features"])
    out: dict[str, Any] = {
        "gate": GATE_NAME,
        "objective": "AML-GOALS 5a D8",
        "prereg_version": prereg.get("version"),
        "prereg_sha256": sha,
        "tolerances": {
            "ap_diff_abs_max": si["ap_diff_abs_max"],
            "ks_stat_max": si["ks_stat_max"],
            "ci_level": pw["ci_level"],
            "bootstrap_iterations": pw["bootstrap_iterations"],
            "min_positives": pw["min_positives"],
            "bootstrap_seed": prereg["cv"]["seed"],
            "source": "aml_preregistration.json: scale_invariance.{ap_diff_abs_max,"
            "ks_stat_max}, power.{ci_level,bootstrap_iterations,min_positives}, cv.seed",
        },
        "method": si["method"],
        "multiple_comparisons": "intersection-union: every component must pass, so the "
        "false-invariance error is bounded by the largest single-component error",
        "errors": [],
    }
    errors: list[str] = out["errors"]
    runs = {"small": load_run(small_report, endpoint), "large": load_run(large_report, endpoint)}
    facts = {}
    for role, run in runs.items():
        facts[role], why = _provenance(run, prereg, sha)
        errors += [f"{role}: {w}" for w in why]
    out["inputs"] = facts
    s, lg = facts["small"], facts["large"]
    gate_n = round(corpora["entities_per_scale_unit"] * corpora["gate_scale"])
    checks: dict[str, Any] = {
        "same_seed": s["corpus_seed"] not in (None, "")
        and str(s["corpus_seed"]) == str(lg["corpus_seed"]),
        "small_at_gate_scale": s["n_entities"] == gate_n,
        "large_above_gate_scale": lg["n_entities"] is not None and lg["n_entities"] > gate_n,
        "same_unit": s["unit"] is not None and s["unit"] == lg["unit"],
        "same_libraries": all(
            (s["libraries"] or {}).get(k) is not None
            and (s["libraries"] or {}).get(k) == (lg["libraries"] or {}).get(k)
            for k in COMPARED_LIBRARIES
        ),
    }
    for name, ok in checks.items():
        if not ok:
            errors.append(f"provenance check {name} failed")

    # ---- AP equivalence per behavioural typology --------------------------
    score_cols = ["group", "label", "score", "weight"]
    scores_uri = {role: _output_uri(run, "oof_scores") for role, run in runs.items()}
    per_t: dict[str, Any] = {}
    bound = si["ap_diff_abs_max"]
    for t in typologies:
        r: dict[str, Any] = {"typology": t}
        why = []
        arrays = {}
        for role, run in runs.items():
            rt = (run["report"].get("typologies") or {}).get(t)
            if rt is None:
                why.append(f"{role}: typology missing from the report")
                continue
            arr = _typology_arrays(
                read_parquet_columns(scores_uri[role], score_cols, endpoint, typology=t)
            )
            n, n_pos = len(arr["y"]), int(arr["y"].sum())
            r[role] = {
                "status": rt.get("status"),
                "n_scored": n,
                "n_positives": n_pos,
                "report_ap": rt.get("ap"),
                "prevalence": (float((arr["w"] * arr["y"]).sum() / arr["w"].sum()) if n else None),
            }
            if rt.get("status") != "ok":
                why.append(f"{role}: report status {rt.get('status')!r}")
            if n == 0:
                why.append(f"{role}: no rows in oof_scores")
            if n != rt.get("n_scored") or n_pos != rt.get("n_positives"):
                why.append(f"{role}: oof_scores counts disagree with the report")
            if n_pos < pw["min_positives"]:
                why.append(f"{role}: underpowered ({n_pos} < min_positives)")
            if not _weights_ok(arr["w"]):
                why.append(f"{role}: weights missing, non-finite or non-positive")
            arrays[role] = arr
        if not why:
            r.update(ap_difference_ci(arrays["small"], arrays["large"], prereg))
            lo, hi = r["diff_ci"]
            if lo is None:
                why.append("bootstrap CI undefined (a resample had no positive)")
            r["within_bounds"] = lo is not None and -bound <= lo and hi <= bound
        r["reasons"] = why
        r["pass"] = not why and bool(r.get("within_bounds"))
        per_t[t] = r
    out["typologies"] = per_t

    # ---- Per-feature distribution match -----------------------------------
    unit_uri = {role: _output_uri(run, "unit_features") for role, run in runs.items()}
    weights = {}
    units_ok = True
    for role, run in runs.items():
        cols = parquet_columns(unit_uri[role], endpoint)
        missing = [c for c in [*features, "weight"] if c not in cols]
        if missing:
            raise ValueError(f"{role}: unit_features lacks columns {missing}")
        weights[role] = read_parquet_columns(unit_uri[role], ["weight"], endpoint)[
            "weight"
        ].to_numpy(dtype=float)
        if len(weights[role]) != run["report"].get("n_scored_units"):
            errors.append(f"{role}: unit_features rows disagree with report n_scored_units")
            units_ok = False
        if not _weights_ok(weights[role]):
            errors.append(f"{role}: unit_features weights missing, non-finite or non-positive")
            units_ok = False
    per_f: dict[str, Any] = {}
    if units_ok:
        for f in features:
            col = {
                role: read_parquet_columns(unit_uri[role], [f], endpoint)[f].to_numpy(dtype=float)
                for role in runs
            }
            ks = weighted_ks(col["small"], weights["small"], col["large"], weights["large"])
            per_f[f] = {"ks": ks, "pass": ks < si["ks_stat_max"]}
    out["features"] = per_f

    ap_pass = bool(per_t) and all(r["pass"] for r in per_t.values())
    ks_pass = len(per_f) == len(features) and all(r["pass"] for r in per_f.values())
    out["checks"] = {
        **checks,
        "ap_equivalence_all_behavioural": ap_pass,
        "ks_all_features": ks_pass,
    }
    out["pass"] = bool(not errors and all(out["checks"].values()))
    out["verdict"] = "pass" if out["pass"] else "fail"
    return out


def summary_lines(v: dict) -> list[str]:
    lines = [f"D8 scale invariance: verdict {v.get('verdict')} pass={v.get('pass')}"]
    for e in v.get("errors") or []:
        lines.append(f"  error: {e}")
    for t, r in (v.get("typologies") or {}).items():
        lo, hi = r.get("diff_ci") or [None, None]
        lines.append(
            f"  {t}: AP small={r.get('ap_small')} large={r.get('ap_large')} "
            f"diff CI=[{lo}, {hi}] pass={r.get('pass')} {'; '.join(r.get('reasons') or [])}"
        )
    bad = [f for f, r in (v.get("features") or {}).items() if not r["pass"]]
    if v.get("features"):
        worst = max(v["features"].items(), key=lambda kv: kv[1]["ks"])
        lines.append(
            f"  KS: worst {worst[0]}={worst[1]['ks']:.4f}; failing: {', '.join(bad) or 'none'}"
        )
    return lines
