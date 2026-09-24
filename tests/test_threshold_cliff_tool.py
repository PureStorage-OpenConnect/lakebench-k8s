"""The R2 threshold-cliff check (datagen_rs/tools/threshold_cliff.py, LB-138)
must read every threshold from the rule source and must tell a smooth
distribution from one pinned to a threshold."""

from __future__ import annotations

import importlib.util
import json
import math
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "datagen_rs/tools/threshold_cliff.py"


def _tool():
    spec = importlib.util.spec_from_file_location("threshold_cliff", TOOL)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


def _thresholds():
    t = _tool()
    return t.load_thresholds(
        t.RULES.read_text(), t.SILVER.read_text(), json.loads(t.PREREG.read_text())
    )


def test_thresholds_come_from_the_rule_source():
    th = _thresholds()
    import ast

    rules = ast.parse((ROOT / "src/lakebench/spark/scripts/detection_rules.py").read_text())
    band = next(
        ast.literal_eval(n.value)
        for n in rules.body
        if isinstance(n, ast.Assign)
        and getattr(n.targets[0], "id", "") == "_STRUCTURING_THRESHOLDS"
    )
    assert th["w2"] == {k: float(v) for k, v in band.items()}
    assert th["w8_amount_usd"] == 5000.0 and th["w8_gap_days"] == 90.0
    assert 10000.0 in th["w6_amount_usd"]
    assert th["fx"]["USD"] == 1.0 and th["window_rel"] == 0.1 and th["max_ratio"] == 2.0


def test_thresholds_follow_a_changed_rule():
    t = _tool()
    src = t.RULES.read_text().replace(
        "amount_threshold_usd: float = 5000.0", "amount_threshold_usd: float = 7500.0"
    )
    th = t.load_thresholds(src, t.SILVER.read_text(), json.loads(t.PREREG.read_text()))
    assert th["w8_amount_usd"] == 7500.0


def _lognormal_count(n, mu, sigma, floor=None, atom_at=None, atom_n=0):
    """Expected counts of a log-normal sample, optionally floored at `floor`
    (mass below moved just above it) or with a point mass."""

    def cdf(x):
        return 0.5 * (1 + math.erf((math.log(x) - mu) / (sigma * math.sqrt(2)))) if x > 0 else 0.0

    def count(lo, hi, exact=False):
        if exact:
            return atom_n if atom_at is not None and lo == atom_at else 0
        if floor is not None:
            lo, hi = max(lo, floor), max(hi, floor)
            extra = n * cdf(floor) if lo <= floor < hi else 0.0
        else:
            extra = 0.0
        a = atom_n if atom_at is not None and lo <= atom_at < hi else 0
        return int(round(n * (cdf(hi) - cdf(lo)) + extra)) + a

    return count


def test_smooth_lognormal_passes_at_every_threshold():
    t = _tool()
    for thr in (5000.0, 9000.0, 10000.0, 50000.0):
        res = t.cliff(_lognormal_count(10_000_000, math.log(5000), 1.4), thr, 0.1, 2.0)
        assert res["verdict"] == "PASS", (thr, res)


def test_a_floor_at_the_threshold_fails():
    t = _tool()
    res = t.cliff(_lognormal_count(1_000_000, math.log(3000), 1.4, floor=5200.0), 5000.0, 0.1, 2.0)
    assert res["verdict"] == "FAIL"
    # A floor right at t leaves nothing below it.
    res = t.cliff(_lognormal_count(1_000_000, math.log(3000), 1.4, floor=5000.0), 5000.0, 0.1, 2.0)
    assert res["verdict"] == "FAIL"


def test_a_round_number_atom_at_the_threshold_does_not_gate():
    t = _tool()
    count = _lognormal_count(1_000_000, math.log(5000), 1.4, atom_at=10000.0, atom_n=50_000)
    res = t.cliff(count, 10000.0, 0.1, 2.0)
    assert res["verdict"] == "PASS" and res["atom"] == 50_000


def test_small_counts_are_insufficient_not_pass():
    t = _tool()
    res = t.cliff(_lognormal_count(2_000, math.log(5000), 1.4), 90_000.0, 0.1, 2.0)
    assert res["verdict"] == "INSUFFICIENT"


def test_end_to_end_on_a_tiny_corpus(tmp_path, capsys):
    duckdb = pytest.importorskip("duckdb")
    t = _tool()
    pacs = tmp_path / "bronze/pacs008"
    pacs.mkdir(parents=True)
    (tmp_path / "manifest").mkdir()
    c = duckdb.connect()
    # 400k USD payments, log-normal amounts, but every amount in [4500, 5000)
    # pushed to 5000.01: a floor just over W8's threshold.
    c.sql(f"""
        COPY (
          SELECT 'u' || i AS uetr,
                 {{'iban': 'IB' || (i % 5000)}} AS dbtr_acct,
                 TIMESTAMP '2021-01-01' + to_seconds((i * 397) % 150000000) AS cre_dt_tm,
                 'USD' AS intr_bk_sttlm_ccy,
                 CASE WHEN a >= 4500 AND a < 5000 THEN 5000.01 ELSE a END AS intr_bk_sttlm_amt
          FROM (SELECT i, round(exp(ln(5000) + 1.4 * sqrt(-2 * ln(random())) * cos(2 * pi() * random())), 2) AS a
                FROM range(400000) r(i))
        ) TO '{pacs}/part-000000.parquet' (FORMAT parquet)
    """)
    c.sql(f"""
        COPY (SELECT 'X_0' AS typology_id, 'fan_in' AS typology_type,
                     ['u1', 'u2'] AS participant_uetrs)
        TO '{tmp_path}/manifest/manifest.parquet' (FORMAT parquet)
    """)
    assert t.main(str(tmp_path)) == 1
    out = capsys.readouterr().out
    w8 = next(ln for ln in out.splitlines() if ln.startswith("W8 amount") and "all rows" in ln)
    assert w8.endswith("FAIL")
    usd_floor = next(ln for ln in out.splitlines() if "W2 USD band floor" in ln)
    assert usd_floor.endswith("PASS")
