"""Silver's reference USD rates must match the generator's (LB-137).

The generator expresses amounts in each account's own currency using
`datagen_rs::amounts::fx_to_usd`; silver converts them back with
`silver_build_financial._FX_TO_USD`. If the two drift, `txn_amount_usd` is
wrong for every non-USD row and every USD-threshold rule moves with it.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _rust_rates() -> dict[str, float]:
    src = (ROOT / "datagen_rs/src/amounts.rs").read_text()
    body = src[src.index("pub fn fx_to_usd") :]
    body = body[: body.index("\n}\n")]
    return {k: float(v) for k, v in re.findall(r'"([A-Z]{3})"\s*=>\s*([0-9.]+)', body)}


def _python_rates() -> dict[str, float]:
    tree = ast.parse((ROOT / "src/lakebench/spark/scripts/silver_build_financial.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_FX_TO_USD" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("_FX_TO_USD not found in silver_build_financial.py")


def test_fx_tables_match():
    rust, py = _rust_rates(), _python_rates()
    assert len(rust) >= 15
    assert rust == py


def test_silver_does_not_use_xchg_rate_as_usd_rate():
    src = (ROOT / "src/lakebench/spark/scripts/silver_build_financial.py").read_text()
    assert 'col("intr_bk_sttlm_amt") * coalesce(col("xchg_rate")' not in src
