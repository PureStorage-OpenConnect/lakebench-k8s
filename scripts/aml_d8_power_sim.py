#!/usr/bin/env python3
"""Power of the registered D8 rule (AML-GOALS 5a D8, section 9 #45).

Simulates the rule src/lakebench/aml/scale_invariance.py implements, with the
constants read from aml_preregistration.json (scale_invariance block, band,
power.ci_level, corpora.calibration_replicate_seeds), so what this prints is
the power of the registered rule, not of a sketch of it. The pre-registration
records the sha256 of this script's stdout (scale_invariance.power_sim); a
change to the rule or to this script shows up as a different hash.

Model. Per typology, each scale-2 run (one per s2 seed) and each scale-10
shard is an independent draw of logit(AP) = logit(true AP) + k x s x N(0, 1),
where s is the customer-bootstrap sd of logit(AP) measured on the v6-43
calibration run (seed 43, scale 2, full refit; analysis in
lb-scratch/d8stats, boot.py) and k inflates it for refit noise the bootstrap
does not see (halfA/halfB replicate chi-square suggests k ~ 1.3). A shard is
s2-sized, so it has the same s. Each run's bootstrap sd is taken as s (1000
resamples estimate it to about 2%).

The rule, per behavioural typology, exactly as implemented:

- per scale: mean of the run logits; SE = max(s / sqrt(n), between-run sd /
  sqrt(n)); CI = mean +/- t(n - 1) x SE, back-transformed;
- gated when either scale's CI touches [band.ap_min, band.ap_max]: pass when
  |mean10 - mean2| <= logit_diff_abs_max, the Welch-Satterthwaite CI of the
  difference is no wider than logit_diff_ci_width_max, and there is no
  confident verdict flip;
- otherwise both scales must lie on the same side of the band.

Output is deterministic for a given numpy (the pinned REFERENCE_PY_DEPS one)
and scipy. Run: python3.11 scripts/aml_d8_power_sim.py | sha256sum
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
from scipy.stats import t as student_t

ROOT = Path(__file__).resolve().parents[1]
PREREG = ROOT / "src/lakebench/spark/data/aml/aml_preregistration.json"

N_SIM = 20000
RNG_SEED = 20260926
#: (AP, customer-bootstrap sd of logit AP) per typology at scale 2, v6-43 full
#: refit (lb-scratch/d8stats/out/boot__d0v2__*.npz and boot__v6full__*.npz).
BASE = {
    "gather_scatter": (0.7235, 0.134),
    "rapid_layering": (0.0049, 0.071),
    "stack": (0.0020, 0.150),
    "dormant_reactivation": (0.7339, 0.104),
    "micro_structuring": (0.5865, 0.110),
    "corridor_high_risk": (0.7349, 0.086),
}
#: Scenario: {typology: (true AP at s2, true AP at s10)}; others invariant.
SCENARIOS = {
    "S0 invariant": {},
    "S1a micro_structuring +0.10 (0.587->0.687)": {"micro_structuring": (0.5865, 0.6865)},
    "S1b gather_scatter -0.10 (0.724->0.624)": {"gather_scatter": (0.7235, 0.6235)},
    "S1c corridor_high_risk -0.07 (0.735->0.665)": {"corridor_high_risk": (0.7349, 0.6649)},
    "S2a dormant_reactivation 0.76->0.84 (edge)": {"dormant_reactivation": (0.76, 0.84)},
    "S2b corridor_high_risk 0.34->0.26 (edge)": {"corridor_high_risk": (0.34, 0.26)},
    "S2c dormant_reactivation 0.78->0.82 (small edge)": {"dormant_reactivation": (0.78, 0.82)},
    "S3 rapid_layering x2 (0.0049->0.0098)": {"rapid_layering": (0.0049, 0.0098)},
    "S0b invariant near edge (dormant_reactivation 0.79)": {"dormant_reactivation": (0.79, 0.79)},
}
K_VALUES = (1.0, 1.3)
POWER_TARGETS = (0.80, 0.90)


def logit(p):
    return np.log(p / (1 - p))


def expit(x):
    return 1 / (1 + np.exp(-x))


class Rule:
    def __init__(self, prereg: dict, n_s2: int | None = None):
        si = prereg["scale_invariance"]
        self.lo, self.hi = prereg["band"]["ap_min"], prereg["band"]["ap_max"]
        self.q = (1 + prereg["power"]["ci_level"]) / 2
        self.dmax = si["logit_diff_abs_max"]
        self.wmax = si["logit_diff_ci_width_max"]
        self.n10 = si["n_shards"]
        self.n2 = n_s2 or 1 + len(prereg["corpora"]["calibration_replicate_seeds"])

    def scale(self, rng, ap, s, k, n):
        runs = logit(ap) + k * s * rng.standard_normal((N_SIM, n))
        mean = runs.mean(axis=1)
        se = np.maximum(s / np.sqrt(n), runs.std(axis=1, ddof=1) / np.sqrt(n))
        return mean, se, n - 1

    def apply(self, rng, a2, a10, s, k):
        """(pass, shift_detected) arrays for one typology."""
        m2, se2, d2 = self.scale(rng, a2, s, k, self.n2)
        m10, se10, d10 = self.scale(rng, a10, s, k, self.n10)
        c2, c10 = student_t.ppf(self.q, d2), student_t.ppf(self.q, d10)
        ci2 = (expit(m2 - c2 * se2), expit(m2 + c2 * se2))
        ci10 = (expit(m10 - c10 * se10), expit(m10 + c10 * se10))

        def touches(ci):
            return (ci[1] >= self.lo) & (ci[0] <= self.hi)

        def side(ci):
            return np.where(ci[1] < self.lo, -1, np.where(ci[0] > self.hi, 1, 0))

        def clear(ci):
            return (ci[1] < self.lo) | (ci[0] > self.hi) | ((ci[0] > self.lo) & (ci[1] < self.hi))

        def inband(p):
            return (p >= self.lo) & (p <= self.hi)

        gated = touches(ci2) | touches(ci10)
        diff = m10 - m2
        var = se2**2 + se10**2
        df = var**2 / (se2**4 / d2 + se10**4 / d10)
        width = 2 * student_t.ppf(self.q, df) * np.sqrt(var)
        flip = (inband(expit(m2)) != inband(expit(m10))) & clear(ci2) & clear(ci10)
        shifted = (np.abs(diff) > self.dmax) | flip
        ok_gated = ~shifted & (width <= self.wmax)
        ok = np.where(gated, ok_gated, side(ci2) == side(ci10))
        detected = np.where(gated, shifted, side(ci2) != side(ci10))
        return ok, detected


def pass_probability(rule, rng, scenario, k):
    ok = np.ones(N_SIM, dtype=bool)
    for typ, (ap, s) in BASE.items():
        a2, a10 = scenario.get(typ, (ap, ap))
        ok &= rule.apply(rng, a2, a10, s, k)[0]
    return float(ok.mean())


def smallest_detectable_shift(rule, rng, typ, k, target, sign):
    """Smallest AP shift (0.01 steps, from the typology's calibration AP) that
    this typology's own check calls a shift with probability >= target."""
    ap, s = BASE[typ]
    for step in range(1, 41):
        a10 = ap + sign * step / 100
        if not 0 < a10 < 1:
            return None
        if rule.apply(rng, ap, a10, s, k)[1].mean() >= target:
            return round(step / 100, 2)
    return None


def main() -> int:
    prereg = json.loads(PREREG.read_text())
    rng = np.random.default_rng(RNG_SEED)
    registered = Rule(prereg)
    print(f"prereg {prereg['version']}: s2 runs {registered.n2}, s10 shards {registered.n10}")
    print(
        f"rule: |dlogit| <= {registered.dmax}, Welch CI width <= {registered.wmax}, "
        f"band [{registered.lo}, {registered.hi}], CI level {prereg['power']['ci_level']}"
    )
    designs = [("registered", registered), ("3 s2 runs (not registered)", Rule(prereg, n_s2=3))]
    for name, rule in designs:
        for k in K_VALUES:
            print(f"\n{name}, k={k}: P(D8 passes)")
            for sname, scen in SCENARIOS.items():
                print(f"  {sname:52s} {pass_probability(rule, rng, scen, k):.2f}")
    print("\nsmallest detectable AP shift (registered rule; own check calls a shift)")
    for typ, (ap, _) in BASE.items():
        if not registered.lo <= ap <= registered.hi:
            continue
        for k in K_VALUES:
            cells = []
            for target in POWER_TARGETS:
                down = smallest_detectable_shift(registered, rng, typ, k, target, -1)
                up = smallest_detectable_shift(registered, rng, typ, k, target, 1)
                cells.append(f"power {target:.2f}: -{down} / +{up}")
            print(f"  {typ:22s} AP {ap:.3f} k={k}: " + "; ".join(cells))
    return 0


if __name__ == "__main__":
    sys.exit(main())
