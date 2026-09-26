"""Level-2 predictions from the calibration runs (AML-GOALS section 9 #46, D-8).

Level 2 is published as "calibrated difficulty replicates out of sample":
before any registered look, the per-typology evaluation AP is predicted from
the scale-2 calibration runs (corpora.calibration_seed plus
corpora.calibration_replicate_seeds, all from the pinned datagen image) and
committed to aml_level2_predictions.json. The method (pre-registration
level2.predictions):

- per run: logit(AP) and its customer-bootstrap sd, recomputed from the
  run's persisted oof_scores exactly as D8 does (scale_invariance);
- s_run = max(between-run sd of logit AP, root-mean-square bootstrap sd):
  the spread of one new corpus's logit AP around the calibration mean;
- predicted AP = expit(mean logit); prediction interval = expit(mean +/-
  t(n - 1) x s_run x sqrt(1 + 1/n)) at (1 + level2.predictions.pi_level) / 2,
  the interval for one further run from the same generator at the same scale.

The evaluation corpus is that further run. The robustness corpus is
perturbed by design, so its comparison with the same interval is reported,
never read as a replication failure of the generator.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from lakebench.aml import scale_invariance as si
from lakebench.aml.fidelity_gate import (
    SCORES_FINGERPRINT_COLUMNS,
    fingerprint,
    load_preregistration,
)


def compute_predictions(
    s2_reports: list[str], *, prereg_path: str | None = None, endpoint: str | None = None
) -> dict[str, Any]:
    """The predictions block for aml_level2_predictions.json. Raises on any
    provenance defect: predictions are committed only from clean runs."""
    import numpy as np
    from scipy.stats import t as student_t

    prereg, sha = load_preregistration(prereg_path)
    packaged = si._packaged_prereg_path()
    if hashlib.sha256(packaged.read_bytes()).hexdigest() != sha:
        raise ValueError("predictions are computed under the packaged pre-registration only")
    corpora = prereg["corpora"]
    method = prereg["level2"]["predictions"]
    runs = [si.load_run(r, endpoint) for r in s2_reports]
    facts = []
    for run in runs:
        f, why = si._provenance(run, prereg, sha)
        if why:
            raise ValueError(f"{run['uri']}: {'; '.join(why)}")
        facts.append(f)
    want = sorted([int(corpora["calibration_seed"]), *corpora["calibration_replicate_seeds"]])
    if sorted(int(f["corpus_seed"]) for f in facts) != want:
        raise ValueError(f"the calibration runs must be exactly seeds {want}")
    eps = corpora["entities_per_scale_unit"]
    for name in (*si.SAME_IN_ALL, "n_entities"):
        if len({repr(f[name]) for f in facts}) != 1:
            raise ValueError(f"the calibration runs differ in {name}")
    if facts[0]["n_entities"] != round(eps * corpora["gate_scale"]):
        raise ValueError("the calibration runs are not at corpora.gate_scale")
    if len({f["report_sha256"] for f in facts}) != len(facts):
        raise ValueError("a calibration report is given twice")

    level = method["pi_level"]
    q = (1 + level) / 2
    iterations = prereg["power"]["bootstrap_iterations"]
    per: dict[str, Any] = {}
    for ti, t in enumerate(prereg["behavioural_subset"]):
        stats = []
        for i, run in enumerate(runs):
            rt = (run["report"].get("typologies") or {}).get(t) or {}
            uri = si._output_uri(run, "oof_scores")
            sub = si.read_parquet_columns(
                uri, list(SCORES_FINGERPRINT_COLUMNS), endpoint, typology=t
            )
            if fingerprint(sub, SCORES_FINGERPRINT_COLUMNS) != si._fingerprints(
                run, "oof_scores"
            ).get(t):
                raise ValueError(f"{run['uri']}: oof_scores for {t} do not match the fingerprint")
            if rt.get("status") != "ok":
                raise ValueError(f"{run['uri']}: {t} status {rt.get('status')!r}")
            rng = np.random.default_rng(np.random.SeedSequence([prereg["cv"]["seed"], 0, i, ti]))
            st = si.run_logit_stats(si._typology_arrays(sub), iterations, rng)
            if st["boot_sd_logit"] is None:
                raise ValueError(f"{run['uri']}: {t} bootstrap undefined")
            stats.append(st)
        lg = np.array([s["logit"] for s in stats])
        sd = np.array([s["boot_sd_logit"] for s in stats])
        n = len(lg)
        s_run = max(float(np.std(lg, ddof=1)), float(np.sqrt(np.mean(sd**2))))
        half = float(student_t.ppf(q, n - 1)) * s_run * float(np.sqrt(1 + 1 / n))
        m = float(lg.mean())
        per[t] = {
            "predicted_ap": si.expit(m),
            "pi": [si.expit(m - half), si.expit(m + half)],
            "logit_mean": m,
            "s_run": s_run,
            "n_runs": n,
            "run_aps": [s["ap"] for s in stats],
        }
    return {
        "method": method["method"],
        "pi_level": level,
        "prereg_version": prereg.get("version"),
        "prereg_sha256": sha,
        "generator_image": facts[0]["generator_image"],
        "model_versions": facts[0]["model_versions"],
        "inputs": [
            {"report": f["report"], "sha256": f["report_sha256"], "seed": int(f["corpus_seed"])}
            for f in facts
        ],
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "typologies": per,
    }
