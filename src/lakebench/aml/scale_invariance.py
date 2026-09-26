"""AML D8: scale invariance (AML-GOALS 5a D8, section 9 #45; pre-registration
3.6.0 ``scale_invariance``).

D8 asks whether the generator's difficulty moves with scale. It is a
registered decision rule with simulated operating characteristics
(scripts/aml_d8_power_sim.py), not an equivalence test: a pass means no shift
above the registered logit tolerance was seen with a difference CI inside the
registered width, and shifts smaller than ``smallest_detectable_shift`` are
expected to pass. Every verdict reports the shift its own SEs could detect.

Inputs are fidelity-gate reports (``--out`` JSON of scripts/aml_gate.py, or a
cluster ``aml_gate_report.json``), local paths or ``s3://`` / ``s3a://`` URIs:

- s2: one run per registered scale-2 seed (corpora.calibration_seed plus
  corpora.calibration_replicate_seeds), each at corpora.gate_scale;
- s10: the scale_invariance.n_shards shards of one
  scale_invariance.large_scale corpus on the calibration seed, each scored
  alone with the s2 code (scripts/aml_gate.py --d8-shard), features on the
  whole corpus, shards cut by planted-instance component.

Each report names its persisted ``oof_scores`` (AP and its customer bootstrap
are recomputed from them) and ``unit_features`` (the per-feature KS).

The rule, per behavioural typology (every constant from the
pre-registration, R7):

- per run: AP, logit(AP) and the customer-bootstrap sd of logit(AP);
- per scale: estimate = mean of the run logits; SE = max(sqrt(sum of the
  runs' bootstrap variances) / n, between-run sd / sqrt(n)); CI = estimate
  +/- t(n - 1) x SE at (1 + power.ci_level) / 2, back-transformed;
- gated when either scale's CI touches [band.ap_min, band.ap_max]: pass when
  |logit_s10 - logit_s2| <= logit_diff_abs_max, the Welch-Satterthwaite CI of
  the difference is no wider than logit_diff_ci_width_max (wider is a miss,
  underpowered) and there is no confident verdict flip (point AP in band at
  one scale and out at the other with both CIs clear of every band edge);
- otherwise (both CIs wholly outside the band) pass when both scales are on
  the same side; the AP ratio and its CI are reported, not gated;
- every run needs power.min_positives positives; fewer is a miss.

Per registered feature, the weighted two-sample KS statistic between the
pooled s2 runs and the pooled s10 shards must stay below ks_stat_max.

Provenance, all of it required: every report ran (verdict ok, not
diagnostic) under the packaged pre-registration with the same unit, label
role, feature list, feature and gate code, generator model versions,
libraries, adapter and generator image (a digest-pinned datagen image
reference, the same in every run: generator output is bit-reproducible only
within one build environment, so host-built corpora are refused), with
corpus_fully_keyed, corpus_seed_verified and
registered_label_role true and no sampled negatives (a driver sample cap that
bound is refused); every report is distinct; persisted outputs match the
fingerprints each report recorded; s2 covers exactly the registered seeds at
the gate scale; the s10 shards come from one corpus and one shard plan, every
index once, no instance spanning shards and no customer in two shards.

The claim is the intersection of every component (Berger 1982), so no
multiplicity correction is needed for the false-invariance error; each extra
component is another chance to fail an invariant corpus, which the power
simulation accounts for.

R7: every tolerance comes from aml_preregistration.json; a unit test fails if
this module carries a numeric literal other than 0, 1 or 2. Any missing
input, typology, column or provenance mismatch fails closed (verdict "fail"
or "error", never "pass").
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from lakebench.aml.fidelity_gate import (
    PREREG_FILENAME,
    SCORES_FINGERPRINT_COLUMNS,
    fingerprint,
    load_preregistration,
)

GATE_NAME = "aml-d8-scale-invariance"
#: Numerical packages that must agree between the runs (A6: like with like).
#: Python itself is recorded but not compared: the cluster driver runs 3.10.
COMPARED_LIBRARIES = ("numpy", "scipy", "pandas", "sklearn", "joblib", "threadpoolctl")
S3_SCHEMES = ("s3://", "s3a://")
#: D8 confirms the freeze on the calibration corpus and its replicates.
REQUIRED_CORPUS_ROLE = "calibration"
#: Entry-point checks the gate runners record in passes; D8 needs each
#: present and true in every run.
REQUIRED_PASSES = ("corpus_fully_keyed", "corpus_seed_verified", "registered_label_role")
#: Checked only when present: the local runner records it, the cluster does
#: not (the driver installs the pins).
PASSES_IF_PRESENT = ("library_versions_match",)
#: Facts that must be equal (and present) in every run.
SAME_IN_ALL = (
    "unit",
    "adapter",
    "label_role",
    "aml_features_sha256",
    "gate_code_sha256",
    "model_versions",
    "generator_image",
)
#: A digest-pinned image reference: generator output is bit-reproducible only
#: within one build environment (platform libm, integrate 9ae4043), so every
#: D8 leg must come from the same pinned datagen image, never a host build.
IMAGE_DIGEST_SUFFIX = "@sha256:"


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


def logit(p: float) -> float:
    import numpy as np

    return float(np.log(p / (1 - p)))


def expit(x: float) -> float:
    import numpy as np

    return float(1 / (1 + np.exp(-x)))


def run_logit_stats(arrays: dict, iterations: int, rng) -> dict:
    """One run's AP, logit(AP) and the customer-bootstrap sd of logit(AP)
    over ``iterations`` resamples. A resample with no positive, or whose AP
    is 0 or 1 (logit undefined), voids the sd (fail closed) instead of being
    dropped, which would bias it."""
    import numpy as np

    r = _APResampler(**arrays)
    ap = r.ap()
    draws = []
    degenerate = 0
    for _ in range(iterations):
        a = r.ap(r.draw(rng))
        if a is None or not 0 < a < 1:
            degenerate += 1
            continue
        draws.append(logit(a))
    ok = ap is not None and 0 < ap < 1 and degenerate == 0 and len(draws) > 1
    return {
        "ap": ap,
        "logit": logit(ap) if ap is not None and 0 < ap < 1 else None,
        "boot_sd_logit": float(np.std(draws, ddof=1)) if ok else None,
        "n_resamples": len(draws),
        "n_degenerate_resamples": degenerate,
    }


def scale_estimate(runs: list[dict], ci_level: float) -> dict:
    """Logit-mean over ``runs`` (each with "logit" and "boot_sd_logit"), its
    SE = max(bootstrap SE of the mean, between-run sd / sqrt(n)) and the
    t(n - 1) CI, back-transformed to AP."""
    import numpy as np
    from scipy.stats import t as student_t

    lg = np.array([r["logit"] for r in runs], dtype=float)
    sd = np.array([r["boot_sd_logit"] for r in runs], dtype=float)
    n = len(lg)
    mean = float(lg.mean())
    se_boot = float(np.sqrt(np.sum(sd**2)) / n)
    se_between = float(np.std(lg, ddof=1) / np.sqrt(n))
    se = max(se_boot, se_between)
    df = n - 1
    crit = float(student_t.ppf((1 + ci_level) / 2, df))
    return {
        "n_runs": n,
        "logit_mean": mean,
        "ap": expit(mean),
        "se_bootstrap": se_boot,
        "se_between": se_between,
        "se": se,
        "se_source": "between" if se_between > se_boot else "bootstrap",
        "df": df,
        "t_crit": crit,
        "logit_ci": [mean - crit * se, mean + crit * se],
        "ap_ci": [expit(mean - crit * se), expit(mean + crit * se)],
        "run_logits": [float(x) for x in lg],
    }


def typology_rule(s2: dict, s10: dict, prereg: dict) -> dict:
    """The registered D8 rule on two scale_estimate() results (pure, so the
    known-answer tests drive it directly)."""
    import numpy as np
    from scipy.stats import norm
    from scipy.stats import t as student_t

    si = prereg["scale_invariance"]
    lo, hi = prereg["band"]["ap_min"], prereg["band"]["ap_max"]
    q = (1 + prereg["power"]["ci_level"]) / 2

    def touches(ci):
        return ci[1] >= lo and ci[0] <= hi

    def side(ci):
        return "below" if ci[1] < lo else "above" if ci[0] > hi else "in"

    def clear(ci):
        return ci[1] < lo or ci[0] > hi or (ci[0] > lo and ci[1] < hi)

    def in_band(ap):
        return lo <= ap <= hi

    diff = s10["logit_mean"] - s2["logit_mean"]
    var = s2["se"] ** 2 + s10["se"] ** 2
    df = (
        var**2 / ((s2["se"] ** 2) ** 2 / s2["df"] + (s10["se"] ** 2) ** 2 / s10["df"])
        if var > 0
        else None
    )
    half = float(student_t.ppf(q, df) * np.sqrt(var)) if df else 0.0
    out: dict[str, Any] = {
        "ap_s2": s2["ap"],
        "ap_s10": s10["ap"],
        "ap_ci_s2": s2["ap_ci"],
        "ap_ci_s10": s10["ap_ci"],
        "logit_diff": diff,
        "logit_diff_ci": [diff - half, diff + half],
        "logit_diff_ci_width": 2 * half,
        "welch_df": df,
    }
    reasons: list[str] = []
    if touches(s2["ap_ci"]) or touches(s10["ap_ci"]):
        out["gated"] = True
        flip = (
            in_band(s2["ap"]) != in_band(s10["ap"]) and clear(s2["ap_ci"]) and clear(s10["ap_ci"])
        )
        out["confident_flip"] = bool(flip)
        out["within_logit_tolerance"] = abs(diff) <= si["logit_diff_abs_max"]
        out["powered"] = 2 * half <= si["logit_diff_ci_width_max"]
        if not out["within_logit_tolerance"]:
            reasons.append(f"|logit_s10 - logit_s2| = {abs(diff):.3f} > logit_diff_abs_max")
        if not out["powered"]:
            reasons.append(
                f"difference CI {2 * half:.3f} wider than logit_diff_ci_width_max: "
                "underpowered (a miss, not a pass)"
            )
        if flip:
            reasons.append("confident verdict flip across the band edge")
        # The shift this run's SEs detect with probability power_target
        # (one-sided normal approximation): reported, never gated.
        sds = si["smallest_detectable_shift"]
        z = float(norm.ppf(sds["power_target"]))
        det = si["logit_diff_abs_max"] + z * float(np.sqrt(var))
        out["detectable_logit_shift"] = det
        out["detectable_ap_shift"] = [
            expit(s2["logit_mean"] - det) - s2["ap"],
            expit(s2["logit_mean"] + det) - s2["ap"],
        ]
        out["detectable_power"] = sds["power_target"]
    else:
        out["gated"] = False
        out["side_s2"], out["side_s10"] = side(s2["ap_ci"]), side(s10["ap_ci"])
        if out["side_s2"] != out["side_s10"]:
            reasons.append(f"out of band on opposite sides ({out['side_s2']} vs {out['side_s10']})")
        # Reported, not gated: AP ratio s10/s2 with a delta-method CI on the
        # log ratio (d log AP = (1 - AP) d logit AP).
        a2, a10 = s2["ap"], s10["ap"]
        v = ((1 - a2) * s2["se"]) ** 2 + ((1 - a10) * s10["se"]) ** 2
        h = float(student_t.ppf(q, df) * np.sqrt(v)) if df else 0.0
        out["ap_ratio"] = a10 / a2
        out["ap_ratio_ci"] = [float(a10 / a2 * np.exp(-h)), float(a10 / a2 * np.exp(h))]
    out["reasons"] = reasons
    out["pass"] = not reasons
    return out


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
        "corpus": prov.get("corpus") or prov.get("manifest"),
        "n_entities": scale.get("n_entities"),
        "unit": rep.get("unit"),
        "adapter": prov.get("adapter"),
        "git_sha": prov.get("git_sha"),
        "libraries": rep.get("libraries"),
        "label_role": prov.get("label_role"),
        "aml_features_sha256": prov.get("aml_features_sha256"),
        "gate_code_sha256": rep.get("gate_code_sha256"),
        "model_versions": prov.get("model_versions"),
        "generator_image": prov.get("generator_image"),
        "sampling": prov.get("sampling"),
        "diagnostic": prov.get("diagnostic"),
        "d8_shard": prov.get("d8_shard"),
        "n_scored_customers": rep.get("n_scored_customers"),
        "passes": rep.get("passes"),
    }
    why = []
    passes = rep.get("passes") or {}
    for name in REQUIRED_PASSES:
        if passes.get(name) is not True:
            why.append(f"report passes.{name} is not true")
    for name in PASSES_IF_PRESENT:
        if name in passes and passes[name] is not True:
            why.append(f"report passes.{name} is not true")
    if prov.get("label_role") != (prereg.get("unit_of_scoring") or {}).get("label_role"):
        why.append("report label_role is not the registered one")
    if rep.get("corpus_role") != REQUIRED_CORPUS_ROLE:
        why.append(f"corpus_role {rep.get('corpus_role')!r}, not {REQUIRED_CORPUS_ROLE!r}")
    for name in SAME_IN_ALL:
        if not facts[name]:
            why.append(f"report records no {name}")
    img = str(prov.get("generator_image") or "")
    digest = img.rpartition(IMAGE_DIGEST_SUFFIX)[2]
    if (
        IMAGE_DIGEST_SUFFIX not in img
        or len(digest) != len(hashlib.sha256().hexdigest())
        or any(c not in "0123456789abcdef" for c in digest)
    ):
        why.append(
            f"generator_image {img or None!r} is not a digest-pinned image: D8 legs must be "
            "generated by the pinned datagen image, not a host build"
        )
    if rep.get("verdict") != "ok":
        why.append(f"report verdict {rep.get('verdict')!r}, not 'ok'")
    if prov.get("diagnostic"):
        why.append("report is a diagnostic run")
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
    for unit, smp in (prov.get("sampling") or {}).items():
        frac = (smp or {}).get("negative_fraction")
        if frac is None or frac != 1:
            why.append(
                f"sampling.{unit}.negative_fraction {frac!r}: a driver sample cap bound; D8 "
                "runs are scored uncapped"
            )
    return facts, why


def _fingerprints(run: dict, name: str) -> dict:
    entry = (run["report"].get("model_outputs") or {}).get(name) or {}
    fp = entry.get("fingerprint")
    return fp if isinstance(fp, dict) else {}


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


def _packaged_prereg_path() -> Path:
    """The tracked pre-registration inside the package; D8 refuses to certify
    under any other file (an override could loosen a tolerance)."""
    import lakebench.aml.fidelity_gate as fg

    return Path(fg.__file__).resolve().parent.parent / "spark" / "data" / "aml" / PREREG_FILENAME


def evaluate_d8(
    s2_reports: list[str],
    s10_reports: list[str],
    *,
    prereg_path: str | None = None,
    endpoint: str | None = None,
    jobs: int | None = None,
) -> dict[str, Any]:
    """Run D8 on the s2 reports (one per registered scale-2 seed) and the s10
    shard reports; return the verdict dict. Never raises for bad inputs: an
    unreadable input gives verdict "error"."""
    try:
        return _evaluate(list(s2_reports), list(s10_reports), prereg_path, endpoint, jobs)
    except Exception as e:  # noqa: BLE001 -- fail closed with the reason
        return {
            "gate": GATE_NAME,
            "verdict": "error",
            "pass": False,
            "errors": [f"{type(e).__name__}: {e}"],
            "inputs": {"s2": [str(r) for r in s2_reports], "s10": [str(r) for r in s10_reports]},
        }


def _cross_checks(facts: dict, prereg: dict, sha: str, packaged_sha: str | None) -> dict:
    """Checks across runs: identity, the registered seed set and scales, and
    the shard plan."""
    si = prereg["scale_invariance"]
    corpora = prereg["corpora"]
    eps = corpora["entities_per_scale_unit"]
    s2, s10 = facts["s2"], facts["s10"]
    every = s2 + s10
    want_s2 = sorted(
        [int(corpora["calibration_seed"])] + list(corpora["calibration_replicate_seeds"])
    )

    def seed_of(f):
        try:
            return int(f["corpus_seed"])
        except (TypeError, ValueError):
            return None

    def same(name):
        vals = [f[name] for f in every]
        return bool(vals) and all(v for v in vals) and all(v == vals[0] for v in vals)

    shards = [f["d8_shard"] for f in s10]
    shard_ok = all(isinstance(d, dict) for d in shards)
    per_shard = [d.get("customers_per_shard") for d in shards] if shard_ok else []
    checks: dict[str, Any] = {
        "packaged_preregistration": sha == packaged_sha,
        "nonempty_registration": bool(prereg["behavioural_subset"]) and bool(prereg["features"]),
        **{f"same_{name}": same(name) for name in SAME_IN_ALL},
        "same_libraries": all(
            (f["libraries"] or {}).get(k) is not None
            and (f["libraries"] or {}).get(k) == (every[0]["libraries"] or {}).get(k)
            for f in every
            for k in COMPARED_LIBRARIES
        ),
        "distinct_reports": len({f["report_sha256"] for f in every}) == len(every),
        "s2_registered_seeds": sorted(s for s in map(seed_of, s2) if s is not None) == want_s2
        and len(s2) == len(want_s2),
        "s2_at_gate_scale": bool(s2)
        and all(f["n_entities"] == round(eps * corpora["gate_scale"]) for f in s2),
        "s2_not_shards": all(f["d8_shard"] is None for f in s2),
        "s10_shard_count": len(s10) == si["n_shards"],
        "s10_calibration_seed": bool(s10)
        and all(seed_of(f) == int(corpora["calibration_seed"]) for f in s10),
        "s10_at_large_scale": bool(s10)
        and all(f["n_entities"] == round(eps * si["large_scale"]) for f in s10),
        "s10_one_corpus": bool(s10)
        and all(f["corpus"] and f["corpus"] == s10[0]["corpus"] for f in s10),
        "s10_shard_plan_recorded": shard_ok and bool(s10),
        "s10_every_index_once": shard_ok
        and sorted(d.get("index") for d in shards) == list(range(si["n_shards"])),
        "s10_registered_plan": shard_ok
        and all(
            d.get("salt") == si["shard_salt"] and d.get("n_shards") == si["n_shards"]
            for d in shards
        ),
        "s10_one_plan": shard_ok
        and bool(shards)
        and all(
            d.get("plan_fingerprint")
            and d.get("plan_fingerprint") == shards[0].get("plan_fingerprint")
            for d in shards
        )
        and all(p == per_shard[0] for p in per_shard),
        "s10_no_spanning_instance": shard_ok
        and all(d.get("spanning_instances") == 0 for d in shards),
        "s10_scored_within_shard": shard_ok
        and all(
            isinstance(d.get("customers_per_shard"), list)
            and isinstance(d.get("index"), int)
            and 0 <= d["index"] < len(d["customers_per_shard"])
            and f["n_scored_customers"] is not None
            and f["n_scored_customers"] <= d["customers_per_shard"][d["index"]]
            for f, d in zip(s10, shards, strict=True)
        ),
    }
    return checks


def _evaluate(s2_reports, s10_reports, prereg_path, endpoint, jobs) -> dict[str, Any]:
    from concurrent.futures import ThreadPoolExecutor

    import numpy as np
    import pandas as pd

    prereg, sha = load_preregistration(prereg_path)
    packaged = _packaged_prereg_path()
    packaged_sha = hashlib.sha256(packaged.read_bytes()).hexdigest() if packaged.is_file() else None
    si = prereg["scale_invariance"]
    pw = prereg["power"]
    typologies = list(prereg["behavioural_subset"])
    features = list(prereg["features"])
    out: dict[str, Any] = {
        "gate": GATE_NAME,
        "objective": "AML-GOALS 5a D8 (section 9 #45)",
        "prereg_version": prereg.get("version"),
        "prereg_sha256": sha,
        "prereg_packaged_sha256": packaged_sha,
        "prereg_override": {
            "path": str(prereg_path) if prereg_path else None,
            "env": os.environ.get("LB_AML_PREREG_PATH"),
        },
        "tolerances": {
            "logit_diff_abs_max": si["logit_diff_abs_max"],
            "logit_diff_ci_width_max": si["logit_diff_ci_width_max"],
            "ks_stat_max": si["ks_stat_max"],
            "band": [prereg["band"]["ap_min"], prereg["band"]["ap_max"]],
            "ci_level": pw["ci_level"],
            "bootstrap_iterations": pw["bootstrap_iterations"],
            "min_positives": pw["min_positives"],
            "n_shards": si["n_shards"],
            "bootstrap_seed": prereg["cv"]["seed"],
            "source": "aml_preregistration.json: scale_invariance, band, power, cv.seed",
        },
        "rule": si["rule"],
        "not_an_equivalence_test": si["not_an_equivalence_test"],
        "smallest_detectable_shift_registered": si["smallest_detectable_shift"],
        "errors": [],
    }
    errors: list[str] = out["errors"]
    runs = {
        "s2": [load_run(r, endpoint) for r in s2_reports],
        "s10": [load_run(r, endpoint) for r in s10_reports],
    }
    facts: dict[str, list] = {"s2": [], "s10": []}
    for scale, rs in runs.items():
        for i, run in enumerate(rs):
            f, why = _provenance(run, prereg, sha)
            facts[scale].append(f)
            errors += [f"{scale}[{i}] {run['uri']}: {w}" for w in why]
    out["inputs"] = facts
    checks = _cross_checks(facts, prereg, sha, packaged_sha)
    for name, ok in checks.items():
        if not ok:
            errors.append(f"provenance check {name} failed")
    all_runs = [(scale, i, run) for scale, rs in runs.items() for i, run in enumerate(rs)]
    if not runs["s2"] or not runs["s10"]:
        raise ValueError("D8 needs s2 and s10 reports")

    # ---- Disjoint shard customers ------------------------------------------
    unit_uri = {(s, i): _output_uri(run, "unit_features") for s, i, run in all_runs}
    shard_groups = [
        pd.unique(read_parquet_columns(unit_uri[("s10", i)], ["group"], endpoint)["group"])
        for i in range(len(runs["s10"]))
    ]
    n_distinct = len(pd.unique(np.concatenate(shard_groups))) if shard_groups else 0
    checks["s10_disjoint_customers"] = n_distinct == sum(len(g) for g in shard_groups)
    if not checks["s10_disjoint_customers"]:
        errors.append("provenance check s10_disjoint_customers failed: a customer is in two shards")

    # ---- Per behavioural typology -------------------------------------------
    score_cols = list(SCORES_FINGERPRINT_COLUMNS)
    scores_uri = {(s, i): _output_uri(run, "oof_scores") for s, i, run in all_runs}
    workers = jobs or min(len(all_runs), os.cpu_count() or 1)
    per_t: dict[str, Any] = {}
    for ti, t in enumerate(typologies):

        def one(item, ti=ti, t=t):
            scale, i, run = item
            why = []
            rt = (run["report"].get("typologies") or {}).get(t)
            if rt is None:
                return {"reasons": [f"{scale}[{i}]: typology missing from the report"]}
            sub = read_parquet_columns(scores_uri[(scale, i)], score_cols, endpoint, typology=t)
            arr = _typology_arrays(sub)
            want_fp = _fingerprints(run, "oof_scores").get(t)
            if want_fp is None or fingerprint(sub, SCORES_FINGERPRINT_COLUMNS) != want_fp:
                why.append(f"{scale}[{i}]: oof_scores rows do not match the report's fingerprint")
            n, n_pos = len(arr["y"]), int(arr["y"].sum())
            if rt.get("status") != "ok":
                why.append(f"{scale}[{i}]: report status {rt.get('status')!r}")
            if n == 0 or n != rt.get("n_scored") or n_pos != rt.get("n_positives"):
                why.append(f"{scale}[{i}]: oof_scores counts disagree with the report")
            if n_pos < pw["min_positives"]:
                why.append(f"{scale}[{i}]: underpowered ({n_pos} < min_positives), a miss")
            if not _weights_ok(arr["w"]):
                why.append(f"{scale}[{i}]: weights missing, non-finite or non-positive")
            res: dict[str, Any] = {"n_scored": n, "n_positives": n_pos, "report_ap": rt.get("ap")}
            if not why:
                scale_ix = list(runs).index(scale)
                rng = np.random.default_rng(
                    np.random.SeedSequence([prereg["cv"]["seed"], scale_ix, i, ti])
                )
                res.update(run_logit_stats(arr, pw["bootstrap_iterations"], rng))
                if res["boot_sd_logit"] is None:
                    why.append(f"{scale}[{i}]: bootstrap undefined (a resample had AP 0 or 1)")
            res["reasons"] = why
            return res

        with ThreadPoolExecutor(max_workers=workers) as ex:
            results = list(ex.map(one, all_runs))
        r: dict[str, Any] = {"typology": t, "runs": {"s2": [], "s10": []}}
        why = []
        for (scale, _, _), res in zip(all_runs, results, strict=True):
            r["runs"][scale].append(res)
            why += res["reasons"]
        if not why:
            est = {s: scale_estimate(r["runs"][s], pw["ci_level"]) for s in ("s2", "s10")}
            r["s2"], r["s10"] = est["s2"], est["s10"]
            r.update(typology_rule(est["s2"], est["s10"], prereg))
            why += r["reasons"]
        r["reasons"] = why
        r["pass"] = not why
        per_t[t] = r
    out["typologies"] = per_t

    # ---- Per-feature distribution match (pooled s2 vs pooled s10) ----------
    weights: dict = {}
    units_ok = True
    for s, i, run in all_runs:
        cols = parquet_columns(unit_uri[(s, i)], endpoint)
        missing = [c for c in [*features, "weight"] if c not in cols]
        if missing:
            raise ValueError(f"{s}[{i}]: unit_features lacks columns {missing}")
        w = read_parquet_columns(unit_uri[(s, i)], ["weight"], endpoint)["weight"].to_numpy(
            dtype=float
        )
        weights[(s, i)] = w
        if len(w) != run["report"].get("n_scored_units"):
            errors.append(f"{s}[{i}]: unit_features rows disagree with report n_scored_units")
            units_ok = False
        if not _weights_ok(w):
            errors.append(f"{s}[{i}]: unit_features weights missing, non-finite or non-positive")
            units_ok = False
        if _fingerprints(run, "unit_features").get("weight") != fingerprint(
            pd.DataFrame({"weight": w}), ["weight"]
        ):
            errors.append(f"{s}[{i}]: unit_features weight does not match the report's fingerprint")
    per_f: dict[str, Any] = {}
    if units_ok:
        for f in features:
            col: dict = {}
            fp_ok = True
            for s, i, run in all_runs:
                col[(s, i)] = read_parquet_columns(unit_uri[(s, i)], [f], endpoint)[f].to_numpy(
                    dtype=float
                )
                if _fingerprints(run, "unit_features").get(f) != fingerprint(
                    pd.DataFrame({f: col[(s, i)]}), [f]
                ):
                    fp_ok = False

            def pooled(scale, col=col):
                keys = [k for k in col if k[0] == scale]
                return (
                    np.concatenate([col[k] for k in keys]),
                    np.concatenate([weights[k] for k in keys]),
                )

            (a, wa), (b, wb) = pooled("s2"), pooled("s10")
            ks = weighted_ks(a, wa, b, wb)
            per_f[f] = {
                "ks": ks,
                "fingerprint_match": fp_ok,
                "pass": fp_ok and ks < si["ks_stat_max"],
            }
    out["features"] = per_f

    out["l2_sensitivity"] = _l2_secondary(runs, typologies, prereg)
    ap_pass = bool(per_t) and all(r["pass"] for r in per_t.values())
    ks_pass = len(per_f) == len(features) and all(r["pass"] for r in per_f.values())
    out["checks"] = {**checks, "typologies_all_behavioural": ap_pass, "ks_all_features": ks_pass}
    out["pass"] = bool(not errors and all(out["checks"].values()))
    out["verdict"] = "pass" if out["pass"] else "fail"
    return out


def _l2_secondary(runs: dict, typologies: list, prereg: dict) -> dict:
    """Ungated: per l2 value and typology, logit-mean(s10) - logit-mean(s2)
    of the refitted APs each run recorded (scripts/aml_gate.py
    --l2-sensitivity). Never enters the verdict."""
    import numpy as np

    values = [float(v) for v in prereg["scale_invariance"]["l2_sensitivity"]["values"]]
    out: dict[str, Any] = {"gated": False, "values": values}
    blocks = {
        s: [(run["report"].get("l2_sensitivity") or {}) for run in rs] for s, rs in runs.items()
    }
    if not all(b.get("values") == values for bs in blocks.values() for b in bs):
        out["available"] = False
        out["note"] = "not every run was scored with --l2-sensitivity at the registered values"
        return out
    out["available"] = True
    table: dict[str, Any] = {}
    for t in typologies:
        row = {}
        for v in values:
            lg: dict = {}
            for s, bs in blocks.items():
                aps = [((b.get("typologies") or {}).get(t) or {}).get(str(v)) for b in bs]
                lg[s] = (
                    float(np.mean([logit(float(a)) for a in aps if a is not None]))
                    if all(a is not None and 0 < a < 1 for a in aps)
                    else None
                )
            row[str(v)] = {
                "logit_mean_s2": lg["s2"],
                "logit_mean_s10": lg["s10"],
                "logit_diff": None
                if lg["s2"] is None or lg["s10"] is None
                else lg["s10"] - lg["s2"],
            }
        table[t] = row
    out["typologies"] = table
    return out


def summary_lines(v: dict) -> list[str]:
    lines = [f"D8 scale invariance: verdict {v.get('verdict')} pass={v.get('pass')}"]
    for e in v.get("errors") or []:
        lines.append(f"  error: {e}")

    def f3(x):
        return "None" if x is None else f"{x:.3f}"

    for t, r in (v.get("typologies") or {}).items():
        if "ap_s2" not in r:
            lines.append(f"  {t}: pass={r.get('pass')} {'; '.join(r.get('reasons') or [])}")
            continue
        ci2, ci10 = r["ap_ci_s2"], r["ap_ci_s10"]
        head = (
            f"  {t}: AP s2={f3(r['ap_s2'])} [{f3(ci2[0])}, {f3(ci2[1])}] "
            f"s10={f3(r['ap_s10'])} [{f3(ci10[0])}, {f3(ci10[1])}] "
            f"dlogit={f3(r['logit_diff'])} CI width={f3(r['logit_diff_ci_width'])}"
        )
        if r.get("gated"):
            lo, hi = r["detectable_ap_shift"]
            head += (
                f" detectable shift (power {r['detectable_power']}): logit "
                f"{f3(r['detectable_logit_shift'])}, AP {f3(lo)}/+{f3(hi)}"
            )
        else:
            head += f" out of band {r['side_s2']}/{r['side_s10']}, ratio {f3(r['ap_ratio'])}"
        lines.append(f"{head} pass={r['pass']} {'; '.join(r.get('reasons') or [])}")
    feats = v.get("features") or {}
    if feats:
        bad = [f for f, r in feats.items() if not r["pass"]]
        worst = max(feats.items(), key=lambda kv: kv[1]["ks"])
        lines.append(
            f"  KS: worst {worst[0]}={worst[1]['ks']:.4f}; failing: {', '.join(bad) or 'none'}"
        )
    lines.append(
        "  D8 is a registered decision rule, not an equivalence test: shifts below the "
        "detectable shift are expected to pass."
    )
    return lines
