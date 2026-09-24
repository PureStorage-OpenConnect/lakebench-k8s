"""Threshold-cliff check for a generated financial corpus (AML-GOALS R2, LB-138).

R2: the datagen is never tuned to a rule. The behavioural test is that no
generated distribution shows a density cliff at a rule threshold. For every
threshold t this compares the row count in [t(1-w), t) with the count in
[t, t(1+w)], and the counts in the two 2% bins either side of t.

    python3.11 datagen_rs/tools/threshold_cliff.py <dir containing bronze/pacs008 and manifest/>

Exit status 1 when any threshold FAILs, 2 when none fails but some check is
INSUFFICIENT (INCONCLUSIVE), 0 on a full PASS. Needs duckdb.

Thresholds are read from the rule source, never copied (ast.literal_eval of
the constants and of the rule functions' keyword defaults), so the check
cannot drift from the rules:

- W8 amount (``w8_dormant_reactivation(amount_threshold_usd=...)``) on the
  USD amount, over all rows, all planted rows, and dormancy burst rows;
- W8 gap (``dormant_days``) on per-originator inter-send gaps, over all
  gaps and the gap that ends in a dormancy burst;
- W2 band edges per currency (``_STRUCTURING_THRESHOLDS``: the threshold t
  and the band floor 0.9 t that ``_suspicious_amount_expr`` uses) on the
  native amount;
- any USD amount threshold in the W7 rule (keyword default or a literal
  ``>= N`` in its body), and W6's ``>= N`` USD literal.

USD amounts use ``silver_build_financial._FX_TO_USD``, the reference rates
silver applies. w (window_rel) and the window ratio limit
(max_density_ratio) come from the pre-registration JSON (threshold_cliff).

Exemption (R2): a declared definitional typology is exempt on its defining
attribute only. micro_structuring's defining attribute is its amount (the
structuring band), so its rows are left out of the amount checks (W2 band
edges and the USD amount thresholds it sits under, W6 and W7's $10,000) and
kept in the gap checks.

The statistic and its limits, declared before any run:

1. Window ratio r = n[t, t(1+w)] / n[t(1-w), t). FAIL when max(r, 1/r)
   exceeds max_density_ratio (2.0) and the excess is significant.
2. Adjacent-bin ratio a = n[t, 1.02 t) / n[0.98 t, t). FAIL when max(a, 1/a)
   exceeds ADJACENT_MAX (1.5) and the excess is significant.

Gaps: the dormancy population (the gap ending at each instance's earliest
burst row) is compared day-of-week matched, i.e. both ratios are divided by
the baseline gaps' ratios over the same bins, so the calendar's weekly
ripple cancels. The baseline gap population itself is EXEMPT and printed
with its reason (AML-GOALS section 9 #28). A threshold the tool cannot find
FAILs unless NO_THRESHOLD lists it ("rule has no such threshold").

Why these limits: the baseline amount is log-normal (sigma 1.4 in log USD).
Its log density has slope -(1 + (ln x - mu) / sigma^2) in ln x, so within
3 sigma of the median |slope| <= 3.1. The two window centres are 0.10 apart
in ln x and the two adjacent bins 0.02 apart, so a smooth log-normal gives
at most exp(0.31) = 1.37 and exp(0.062) = 1.06. The limits 2.0 and 1.5 sit
well above that and well below what a floor or a band edge gives (a floor
at t makes the lower count near zero, an unbounded ratio). Gaps are not
log-normal, and they are not smooth either: the day-of-week calendar gives
per-account gaps a weekly ripple of about 1.3x, and 13 weeks (91 days) sits
next to W8's 90. The same limits apply, so a gap FAIL must be read against
the day histogram (printed below the table) before it is called rule
tuning.

Rows exactly equal to t are left out of both windows and reported as the
atom at t. The generator snaps 15% of amounts to round numbers (people pay
round sums), and every threshold here is a round number, so the baseline has
a real point mass at t, and the snap grid coarsens at decade boundaries, so
that mass is large at $10,000. It does not gate for all rows. A planted
population, though, must not concentrate on t more than baseline rows do:

3. Atom share s = atom / (window rows + atom). For a planted population,
   FAIL when s_planted > max_density_ratio x s_baseline, with at least
   MIN_ATOM planted rows at t and the difference significant (two-proportion
   z > 3). For USD thresholds both sides are USD-currency rows, since only
   those can land exactly on a USD value. This is what catches a typology pinned to exactly t, which the
   windows alone would read as empty.

"Significant" means |ln ratio| exceeds 3 standard errors, with the Poisson
approximation se = sqrt(1/n1 + 1/n2) (0.5 continuity correction for a zero
count). A significant excess FAILs even in a thin window; a PASS needs
MIN_COUNT rows on each side, and a large but noisy ratio is INSUFFICIENT.
INSUFFICIENT is not a pass: it says the corpus is too small to test that
threshold, so run at a larger scale for a claim.
"""

from __future__ import annotations

import ast
import json
import math
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RULES = ROOT / "src/lakebench/spark/scripts/detection_rules.py"
SILVER = ROOT / "src/lakebench/spark/scripts/silver_build_financial.py"
PREREG = ROOT / "src/lakebench/spark/data/aml/aml_preregistration.json"

ADJACENT_BIN = 0.02
ADJACENT_MAX = 1.5
MIN_COUNT = 30
MIN_ATOM = 10
Z_SIGNIFICANT = 3.0
EXEMPT_W2 = ("micro_structuring",)

# Rules the tool expects no threshold of a given kind in. A missing threshold
# FAILs unless it is listed here with the reason.
NO_THRESHOLD = {
    "W7 amount": "rule has no such threshold: W7 filters on cross_border and the "
    "beneficiary's FATF-listed country, with no amount filter",
}

# Baseline gap populations exempt from the gap check, with the recorded reason.
GAP_EXEMPT_REASON = (
    "EXEMPT (calendar realism, AML-GOALS section 9 #28): per-account gaps carry the "
    "day-of-week calendar's weekly ripple, and 13 weeks = 91 days sits next to W8's 90"
)


def _module_constant(tree: ast.Module, name: str):
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else []
        if isinstance(node, ast.AnnAssign):
            targets = [node.target]
        for tgt in targets:
            if isinstance(tgt, ast.Name) and tgt.id == name:
                return ast.literal_eval(node.value)
    raise KeyError(name)


def _function(tree: ast.Module, name: str) -> ast.FunctionDef:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise KeyError(name)


def _defaults(fn: ast.FunctionDef) -> dict:
    """Keyword defaults of a function, literal values only."""
    args = fn.args.args[len(fn.args.args) - len(fn.args.defaults) :]
    out = {}
    for a, d in zip(args, fn.args.defaults, strict=True):
        try:
            out[a.arg] = ast.literal_eval(d)
        except ValueError:
            pass
    for a, d in zip(fn.args.kwonlyargs, fn.args.kw_defaults, strict=True):
        if d is not None:
            try:
                out[a.arg] = ast.literal_eval(d)
            except ValueError:
                pass
    return out


def _ge_literals(fn: ast.FunctionDef) -> list[float]:
    """Numbers N in ``>= N`` inside string literals of a function body (the
    rules build some filters as SQL strings)."""
    found = []
    for node in ast.walk(fn):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            found += [float(x) for x in re.findall(r">=\s*([0-9][0-9_]*(?:\.[0-9]+)?)", node.value)]
    return found


def load_thresholds(rules_src: str, silver_src: str, prereg: dict) -> dict:
    """Every rule threshold the check covers, read from the sources."""
    rules = ast.parse(rules_src)
    w8 = _defaults(_function(rules, "w8_dormant_reactivation"))
    w7_fn = _function(rules, "w7_cross_border_high_risk")
    w7 = [
        float(v)
        for k, v in _defaults(w7_fn).items()
        if isinstance(v, (int, float)) and ("amount" in k or "threshold" in k)
    ] + _ge_literals(w7_fn)
    w6 = _ge_literals(_function(rules, "w6_pep_counterparty"))
    if not w6:
        raise ValueError("no '>= N' USD threshold found in w6_pep_counterparty")
    band = [
        n.value.right.value
        for n in ast.walk(_function(rules, "_suspicious_amount_expr"))
        if isinstance(n, ast.Assign)
        and getattr(n.targets[0], "id", "") == "floor"
        and isinstance(n.value, ast.BinOp)
        and isinstance(n.value.op, ast.Mult)
        and isinstance(n.value.right, ast.Constant)
    ]
    if len(band) != 1:
        raise ValueError("could not read the W2 band floor factor from _suspicious_amount_expr")
    cliff = prereg["threshold_cliff"]
    return {
        "w8_amount_usd": float(w8["amount_threshold_usd"]),
        "w8_gap_days": float(w8["dormant_days"]),
        "w2": {k: float(v) for k, v in _module_constant(rules, "_STRUCTURING_THRESHOLDS").items()},
        "w7_amount_usd": sorted(set(w7)),
        "w6_amount_usd": sorted(set(w6)),
        "w2_floor_factor": float(band[0]),
        "fx": _module_constant(ast.parse(silver_src), "_FX_TO_USD"),
        "window_rel": float(cliff["window_rel"]),
        "max_ratio": float(cliff["max_density_ratio"]),
    }


def _ratio_verdict(
    n_lo: int, n_hi: int, limit: float, base: tuple[int, int] | None = None
) -> tuple[str, float]:
    """FAIL on a significant excess over the limit even in a thin window (266
    rows below t against 11 above is a cliff whatever MIN_COUNT says); PASS
    needs MIN_COUNT rows on each side. Zero counts get a 0.5 continuity
    correction for the test only."""
    if n_lo + n_hi == 0:
        return "INSUFFICIENT", float("nan")
    a, b = max(n_lo, 0.5), max(n_hi, 0.5)
    r = b / a
    var = 1 / a + 1 / b
    if base is not None:
        # Day-of-week matched: divide by the baseline's ratio over the same
        # bins, so the calendar's own ripple cancels.
        bl, bh = max(base[0], 0.5), max(base[1], 0.5)
        r = r / (bh / bl)
        var += 1 / bl + 1 / bh
    lr = abs(math.log(r))
    se = math.sqrt(var)
    if lr > math.log(limit) and lr / se > Z_SIGNIFICANT:
        return "FAIL", r
    if min(n_lo, n_hi) < MIN_COUNT:
        return "INSUFFICIENT", r
    return ("PASS" if lr <= math.log(limit) else "INSUFFICIENT"), r


def cliff(count, t: float, w: float, max_ratio: float, base_count=None) -> dict:
    """Verdict for one threshold. ``count(lo, hi)`` returns rows in [lo, hi);
    ``count(t, t, exact=True)`` returns rows equal to t.

    The upper window is closed at t(1+w) in the goal's wording; the half-open
    form differs by the rows exactly on that edge, which is immaterial.
    """
    atom = count(t, t, exact=True)
    n_lo, n_hi = count(t * (1 - w), t), count(t, t * (1 + w)) - atom
    b_lo, b_hi = count(t * (1 - ADJACENT_BIN), t), count(t, t * (1 + ADJACENT_BIN)) - atom
    bw = ba = None
    if base_count is not None:
        b_atom = base_count(t, t, exact=True)
        bw = (base_count(t * (1 - w), t), base_count(t, t * (1 + w)) - b_atom)
        ba = (
            base_count(t * (1 - ADJACENT_BIN), t),
            base_count(t, t * (1 + ADJACENT_BIN)) - b_atom,
        )
    v1, r1 = _ratio_verdict(n_lo, n_hi, max_ratio, bw)
    v2, r2 = _ratio_verdict(b_lo, b_hi, ADJACENT_MAX, ba)
    order = {"FAIL": 2, "INSUFFICIENT": 1, "PASS": 0}
    verdict = max((v1, v2), key=order.get)
    return {
        "verdict": verdict,
        "atom": atom,
        "window": (n_lo, n_hi, r1, v1),
        "adjacent": (b_lo, b_hi, r2, v2),
    }


def atom_verdict(atom_p: int, n_p: int, atom_b: int, n_b: int, max_ratio: float) -> str:
    """Statistic 3: does a planted population sit on t more than baseline?
    n_p and n_b are window rows plus the atom.

    Too few planted rows at t to see a pin is INSUFFICIENT, never PASS, unless
    the baseline share predicts at least MIN_ATOM there (then a small atom is
    evidence against a pin, a PASS)."""
    if atom_p < MIN_ATOM:
        expected = atom_b / n_b * n_p if n_b else 0.0
        return "PASS" if expected >= MIN_ATOM else "INSUFFICIENT"
    if n_b == 0:
        return "FAIL"  # planted rows sit on t where baseline has nothing at all
    sp, sb = atom_p / n_p, atom_b / n_b
    if sp <= max_ratio * sb:
        return "PASS"
    pool = (atom_p + atom_b) / (n_p + n_b)
    se = math.sqrt(pool * (1 - pool) * (1 / n_p + 1 / n_b)) or 1e-12
    return "FAIL" if (sp - sb) / se > Z_SIGNIFICANT else "INSUFFICIENT"


def main(root: str) -> int:
    import duckdb

    base = Path(root)
    pacs = next(base.rglob("bronze/pacs008"))
    # Every cycle's manifest (manifest.parquet, manifest-c001.parquet, ...).
    manifest = next(base.rglob("manifest"))
    manifest = manifest / "manifest*.parquet"
    th = load_thresholds(RULES.read_text(), SILVER.read_text(), json.loads(PREREG.read_text()))
    fx_case = " ".join(f"WHEN '{k}' THEN {v}" for k, v in th["fx"].items())
    c = duckdb.connect()
    c.sql(f"""
        CREATE TABLE planted AS
          SELECT typology_id, typology_type, u.uetr, u.pos
          FROM '{manifest}', UNNEST(participant_uetrs) WITH ORDINALITY AS u(uetr, pos);
        CREATE TABLE r AS
          SELECT b.uetr, b.dbtr_acct.iban AS orig, b.cre_dt_tm AS ts,
                 b.intr_bk_sttlm_ccy AS ccy,
                 CAST(b.intr_bk_sttlm_amt AS DOUBLE) AS amt,
                 CAST(b.intr_bk_sttlm_amt AS DOUBLE)
                   * CASE b.intr_bk_sttlm_ccy {fx_case} ELSE 1.0 END AS usd,
                 p.typology_type AS typ, p.pos, p.typology_id AS tid
          FROM '{pacs}/*.parquet' b LEFT JOIN planted p USING (uetr);
        CREATE TABLE g AS
          SELECT typ, pos, tid, ts,
                 date_diff('second', LAG(ts) OVER (PARTITION BY orig ORDER BY ts, uetr), ts)
                   / 86400.0 AS gap_days
          FROM r;
    """)
    w, rmax = th["window_rel"], th["max_ratio"]
    results: list[tuple[str, str, dict]] = []

    def counter(table: str, col: str, where: str):
        def count(lo: float, hi: float, exact: bool = False) -> int:
            # Amounts are cents; compare exact values at cent resolution so a
            # float product never misses an atom.
            if exact:
                rng = f"round({col}, 2) = round({lo!r}, 2)"
            else:
                rng = f"{col} >= {lo!r} AND {col} < {hi!r}"
            (n,) = c.sql(f"SELECT COUNT(*) FROM {table} WHERE ({where}) AND {rng}").fetchone()
            return int(n)

        return count

    def run(
        label: str,
        pop: str,
        table: str,
        col: str,
        where: str,
        t: float,
        base: str | None = None,
        norm: str | None = None,
        exempt_reason: str | None = None,
    ) -> None:
        """``base``: planted-vs-baseline atom check. ``norm``: divide the
        ratios by this population's ratios over the same bins (day-of-week
        matched gaps). ``exempt_reason``: report, do not gate."""
        nc = counter(table, col, norm) if norm is not None else None
        res = cliff(counter(table, col, where), t, w, rmax, nc)
        res["atom_verdict"] = "-"
        if exempt_reason is not None:
            res["verdict"] = "EXEMPT"
            res["reason"] = exempt_reason
        if base is not None:
            cb = counter(table, col, base)
            n_b = cb(t * (1 - w), t * (1 + w))
            n_p = res["window"][0] + res["window"][1] + res["atom"]
            av = atom_verdict(res["atom"], n_p, cb(t, t, exact=True), n_b, rmax)
            res["atom_verdict"] = av
            order = {"FAIL": 2, "INSUFFICIENT": 1, "PASS": 0}
            res["verdict"] = max((res["verdict"], av), key=order.get)
        results.append((label, pop, res))

    t8 = th["w8_amount_usd"]
    # Dormancy burst rows are every row after the out-of-window anchor (pos 1).
    burst = "typ = 'dormant_reactivation' AND pos > 1"
    run(f"W8 amount ${t8:,.0f}", "all rows", "r", "usd", "TRUE", t8)
    # Only USD-native rows can land exactly on a USD threshold (snapping is in
    # the payment currency), so the planted-vs-baseline atom comparison uses
    # USD rows on both sides; a different currency mix would otherwise move it.
    usd_base = "typ IS NULL AND ccy = 'USD'"
    exempt = ", ".join(f"'{x}'" for x in EXEMPT_W2)
    planted_usd = f"typ IS NOT NULL AND typ NOT IN ({exempt}) AND ccy = 'USD'"
    for pop, where in (("planted $", planted_usd), ("bursts", burst)):
        run(f"W8 amount ${t8:,.0f}", pop, "r", "usd", where, t8, base=usd_base)
    tg = th["w8_gap_days"]
    base_gaps = "gap_days IS NOT NULL AND typ IS NULL"
    run(
        f"W8 gap {tg:g} d",
        "all gaps",
        "g",
        "gap_days",
        "gap_days IS NOT NULL",
        tg,
        exempt_reason=GAP_EXEMPT_REASON,
    )
    # The dormancy length is the gap that ends at the instance's earliest
    # burst row (arg_min of ts over pos > 1), compared with baseline gaps over
    # the same bins so the weekly ripple cancels.
    first_burst = (
        "(tid, ts) IN (SELECT tid, min(ts) FROM g "
        "WHERE typ = 'dormant_reactivation' AND pos > 1 GROUP BY tid)"
    )
    run(f"W8 gap {tg:g} d", "dormancy", "g", "gap_days", first_burst, tg, norm=base_gaps)
    ff = th["w2_floor_factor"]
    for ccy, t in th["w2"].items():
        where = f"ccy = '{ccy}' AND (typ IS NULL OR typ NOT IN ({exempt}))"
        planted = f"ccy = '{ccy}' AND typ IS NOT NULL AND typ NOT IN ({exempt})"
        base_c = f"ccy = '{ccy}' AND typ IS NULL"
        for name, edge in (("threshold", t), ("band floor", ff * t)):
            run(f"W2 {ccy} {name} {edge:,.0f}", "all rows", "r", "amt", where, edge)
            run(f"W2 {ccy} {name} {edge:,.0f}", "planted", "r", "amt", planted, edge, base=base_c)
    missing: list[str] = []
    for label, key in (("W7", "w7_amount_usd"), ("W6", "w6_amount_usd")):
        if not th[key]:
            reason = NO_THRESHOLD.get(f"{label} amount")
            if reason:
                print(f"{label} amount: {reason}")
            else:
                print(f"FAIL {label} amount: no threshold found in detection_rules.py")
                missing.append(label)
        for t in th[key]:
            run(f"{label} amount ${t:,.0f}", "all rows", "r", "usd", "TRUE", t)
            run(
                f"{label} amount ${t:,.0f}",
                "planted $",
                "r",
                "usd",
                planted_usd,
                t,
                base=usd_base,
            )

    print(
        f"window +/-{w:.0%} limit {rmax:g}; adjacent {ADJACENT_BIN:.0%} bins limit {ADJACENT_MAX:g}"
    )
    print(
        f"{'threshold':32s} {'population':10s} {'window lo/hi ratio':>26s} {'adjacent lo/hi ratio':>24s} {'atom':>6s} {'atom v.':>12s}  verdict"
    )
    fails = inconclusive = 0
    for label, pop, res in results:
        wl, wh, wr, _ = res["window"]
        al, ah, ar, _ = res["adjacent"]
        print(
            f"{label:32s} {pop:10s} {wl:>9}/{wh:<9} {wr:6.2f} {al:>8}/{ah:<8} {ar:6.2f} {res['atom']:>6} {res['atom_verdict']:>12s}  {res['verdict']}"
        )
        if "reason" in res:
            print(f"    {res['reason']}")
        fails += res["verdict"] == "FAIL"
        inconclusive += res["verdict"] == "INSUFFICIENT"
    fails += len(missing)
    lo, hi = int(tg * (1 - w)), int(math.ceil(tg * (1 + w)))
    hist = c.sql(f"""
        SELECT floor(gap_days)::INT AS d, COUNT(*) FROM g
        WHERE gap_days >= {lo} AND gap_days < {hi} GROUP BY 1 ORDER BY 1
    """).fetchall()
    print(f"\nper-originator gaps by day, {lo}-{hi - 1}: " + " ".join(f"{d}:{n}" for d, n in hist))
    if fails:
        print(f"\nFAIL: {fails} thresholds with a cliff ({inconclusive} inconclusive)")
        return 1
    if inconclusive:
        # INSUFFICIENT is not a pass: the corpus is too small to test them.
        print(f"\nINCONCLUSIVE: no cliff found, but {inconclusive} checks lack the rows to test")
        return 2
    print("\nPASS: no threshold shows a cliff")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
