"""Calendar leakage report for a generated financial corpus.

Planted (typology) rows must follow the same calendar as baseline rows, or
the calendar itself identifies them. For every typology this compares the
planted share of rows per weekday and per day of month with the baseline
share (a likelihood ratio, LR; 1.0 is no signal), counts planted rows in
(country, date) cells that hold no baseline rows at all (public holidays,
for example), checks leg order, and checks that the manifest's injection
window contains the rows it describes.

    python3.11 tools/calendar_leakage.py <dir containing pacs008/ and manifest/>

Exit status 1 when any gate fails. Needs duckdb and scipy. The row-level
LR tables are printed for inspection; the calendar gate is the
instance-level test, because rows of one instance cluster on a few days.

Power, stated plainly: the per-typology tests have roughly one observation
per instance, so at scale 0.5 (50 to 900 instances per typology) they only
catch large per-typology leaks; a single typology at LR 1.5 with 50
instances can pass. The pooled tests (about 4,000 instances) are the ones
with real power, and they separated the old generator (p 1e-5 to 1e-9) from
the new one. Run at a larger scale, or several seeds, for per-typology
claims. Window containment is close to definitional now that the manifest
window is the rows' own span; it guards against the manifest drifting from
the rows again.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
from scipy import stats

LR_LIMIT = 1.3  # a cell whose planted share is 1.3x the baseline share is a signal
MIN_PLANTED = 40  # ignore cells with fewer planted rows than this (noise)
P_LIMIT = 0.01  # family-wise false-alarm rate for the per-typology calendar tests


MIN_EXPECTED = 5.0  # chi-square validity: every tested bin needs this many expected


def chi2_merged(base: dict, cells: dict, n: float):
    """Chi-square of observed ``cells`` against ``n * base`` shares, merging
    the sparsest bins into one until every expected count is >= MIN_EXPECTED.

    Returns (chi2, dof) or None when fewer than two bins remain (the test is
    not meaningful at this sample size). Without merging, 24 hour bins or 31
    day bins at a few dozen instances have expected counts below 1, where
    the chi-square approximation is invalid and fails falsely.
    """
    keys = sorted((k for k in base if base[k] > 0), key=lambda k: base[k])
    bins: list[tuple[float, float]] = []  # (expected, observed)
    acc_e = acc_o = 0.0
    for k in keys:
        acc_e += n * base[k]
        acc_o += cells.get(k, 0.0)
        if acc_e >= MIN_EXPECTED:
            bins.append((acc_e, acc_o))
            acc_e = acc_o = 0.0
    if acc_e > 0:
        if bins:
            e, o = bins.pop()
            bins.append((e + acc_e, o + acc_o))
        else:
            return None
    if len(bins) < 2:
        return None
    return sum((o - e) ** 2 / e for e, o in bins), len(bins) - 1


def main(root: str) -> int:
    base = Path(root)
    pacs = next(base.rglob("bronze/pacs008"))
    manifest = next(base.rglob("manifest/manifest.parquet"))
    c = duckdb.connect()
    c.sql(f"""
        CREATE VIEW m AS SELECT * FROM '{manifest}';
        CREATE TABLE planted AS
          SELECT typology_id, typology_type, u.uetr, u.pos, injection_ts_start, injection_ts_end
          FROM m, UNNEST(participant_uetrs) WITH ORDINALITY AS u(uetr, pos);
        CREATE TABLE b AS
          SELECT uetr, cre_dt_tm AS ts, dbtr.ctry_of_res AS cc
          FROM '{pacs}/*.parquet';
        CREATE TABLE r AS
          SELECT b.*, p.typology_id, p.typology_type, p.pos,
                 p.injection_ts_start AS ws, p.injection_ts_end AS we
          FROM b LEFT JOIN planted p USING (uetr);
    """)
    failures: list[str] = []

    def lr_table(expr: str, label: str) -> None:
        rows = c.sql(f"""
            WITH base AS (
              SELECT {expr} AS k, COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER () AS s
              FROM r WHERE typology_id IS NULL GROUP BY 1),
            pl AS (
              SELECT typology_type, {expr} AS k, COUNT(*) AS n,
                     COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER (PARTITION BY typology_type) AS s
              FROM r WHERE typology_id IS NOT NULL GROUP BY 1, 2)
            SELECT pl.typology_type, pl.k, pl.n, pl.s / base.s AS lr
            FROM pl JOIN base USING (k)
            WHERE pl.n >= {MIN_PLANTED} AND pl.s / base.s > {LR_LIMIT}
            ORDER BY lr DESC
        """).fetchall()
        print(f"\n{label}: cells with LR > {LR_LIMIT} (min {MIN_PLANTED} planted rows)")
        for t, k, n, lr in rows:
            print(f"  {t:24s} {k!s:>4}  n={n:<6} LR={lr:.2f}")
        if not rows:
            print("  none")

    def instance_test(expr: str, label: str) -> None:
        """Chi-square per typology with each INSTANCE as one observation.

        An instance's rows share a few days, so row counts overstate the
        evidence: 50 rows on one day of the month can be two instances.
        Each instance contributes weight 1, spread over its rows' cells.
        """
        base = dict(
            c.sql(f"""
                SELECT {expr}, COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER ()
                FROM r WHERE typology_id IS NULL GROUP BY 1
            """).fetchall()
        )
        obs = c.sql(f"""
            SELECT typology_type, k, SUM(w) FROM (
              SELECT typology_type, {expr} AS k,
                     1.0 / COUNT(*) OVER (PARTITION BY typology_id) AS w
              FROM r WHERE typology_id IS NOT NULL)
            GROUP BY 1, 2
        """).fetchall()
        by_t: dict[str, dict] = {}
        for t, k, w in obs:
            by_t.setdefault(t, {})[k] = w
        alpha = P_LIMIT / max(1, len(by_t))
        print(f"\n{label}: instance-level chi-square (Bonferroni alpha {alpha:.1e})")
        flagged = False
        for t, cells in sorted(by_t.items()):
            n = sum(cells.values())
            res = chi2_merged(base, cells, n)
            if res is None:
                continue
            chi2, dof = res
            p = stats.chi2.sf(chi2, dof)
            if p < alpha:
                flagged = True
                print(f"  {t:24s} instances={n:.0f} chi2={chi2:.1f} p={p:.1e}")
                failures.append(f"{label} {t} p={p:.1e}")
        if not flagged:
            print("  none")

    def pooled_gate(expr: str, label: str) -> None:
        """All typologies pooled, each instance one observation (chi-square),
        with the worst cell's LR printed for scale."""
        base = dict(
            c.sql(f"""
                SELECT {expr}, COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER ()
                FROM r WHERE typology_id IS NULL GROUP BY 1
            """).fetchall()
        )
        cells = dict(
            c.sql(f"""
                SELECT k, SUM(w) FROM (
                  SELECT {expr} AS k, 1.0 / COUNT(*) OVER (PARTITION BY typology_id) AS w
                  FROM r WHERE typology_id IS NOT NULL)
                GROUP BY 1
            """).fetchall()
        )
        n = sum(cells.values())
        res = chi2_merged(base, cells, n)
        if res is None:
            print(f"\n{label}: too few instances to test")
            return
        chi2, dof = res
        p = stats.chi2.sf(chi2, dof)
        lrs = {k: cells.get(k, 0.0) / (n * sh) for k, sh in base.items() if sh > 0}
        k_worst = max(lrs, key=lambda k: abs(lrs[k] - 1))
        print(
            f"\n{label}, all typologies pooled ({n:.0f} instances): chi2={chi2:.1f} "
            f"p={p:.1e}; worst cell {k_worst} LR={lrs[k_worst]:.2f}"
        )
        if p < P_LIMIT:
            failures.append(
                f"{label} pooled p={p:.1e} (worst cell {k_worst} LR={lrs[k_worst]:.2f})"
            )

    pooled_gate("dayofweek(ts)", "weekday")
    pooled_gate("day(ts)", "day of month")
    pooled_gate("hour(ts)", "hour of day")
    lr_table("dayofweek(ts)", "weekday (rows, informational)")
    lr_table("day(ts)", "day of month (rows, informational)")
    instance_test("dayofweek(ts)", "weekday")
    instance_test("day(ts)", "day of month")
    instance_test("hour(ts)", "hour of day")

    (orphans,) = c.sql("""
        WITH cells AS (
          SELECT cc, CAST(ts AS DATE) AS d,
                 COUNT(*) FILTER (WHERE typology_id IS NULL) AS nb,
                 COUNT(*) FILTER (WHERE typology_id IS NOT NULL) AS np
          FROM r GROUP BY 1, 2)
        SELECT COALESCE(SUM(np), 0) FROM cells WHERE nb = 0 AND np > 0
    """).fetchone()
    print(f"\nplanted rows in (country, date) cells with no baseline rows: {orphans}")
    if orphans:
        failures.append(f"{orphans} planted rows on baseline-empty calendar cells")

    (inverted,) = c.sql("""
        SELECT COUNT(*) FROM (
          SELECT typology_id,
                 MAX(ts) FILTER (WHERE pos = 1) AS t_in,
                 MAX(ts) FILTER (WHERE pos = 2) AS t_out
          FROM r WHERE typology_type = 'rapid_layering' GROUP BY 1)
        WHERE t_in > t_out
    """).fetchone()
    print(f"rapid_layering instances forwarding before receiving: {inverted}")
    if inverted:
        failures.append(f"{inverted} rapid_layering legs out of order")

    print("\nshare of planted rows outside the manifest injection window:")
    for t, share in c.sql("""
        SELECT typology_type,
               AVG(CASE WHEN ts < ws OR ts > we THEN 1.0 ELSE 0.0 END) AS share
        FROM r
        WHERE typology_id IS NOT NULL
          AND NOT (typology_type = 'dormant_reactivation' AND pos = 1)  -- anchor, by design
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall():
        print(f"  {t:24s} {share:.1%}")
        if share > 0:
            failures.append(f"{t}: {share:.1%} of rows outside the injection window")

    mon, n = c.sql("""
        SELECT AVG(CASE WHEN dayofweek(ts) = 1 THEN 1.0 ELSE 0.0 END), COUNT(*)
        FROM r WHERE typology_type = 'dormant_reactivation' AND pos = 1
    """).fetchone()
    (base_mon,) = c.sql("""
        SELECT AVG(CASE WHEN dayofweek(ts) = 1 THEN 1.0 ELSE 0.0 END)
        FROM r WHERE typology_id IS NULL
    """).fetchone()
    lr = mon / base_mon if base_mon else 0
    print(f"\ndormancy anchors on Monday: {mon:.1%} of {n} (baseline {base_mon:.1%}, LR {lr:.2f})")
    if n >= MIN_PLANTED and lr > LR_LIMIT:
        failures.append(f"dormancy anchor Monday LR={lr:.2f}")

    print(f"\n{'FAIL' if failures else 'PASS'}: {len(failures)} gate failures")
    for f in failures:
        print(f"  - {f}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
