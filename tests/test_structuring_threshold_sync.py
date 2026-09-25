"""The generator plants structured amounts under amounts::reporting_threshold
and W2 scores them against detection_rules._STRUCTURING_THRESHOLDS: the two
tables must agree currency by currency, or structured rows in a currency fall
outside (or wholly inside) the rule's band without any test noticing."""

from __future__ import annotations

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AMOUNTS_RS = ROOT / "datagen_rs/src/amounts.rs"
RULES_PY = ROOT / "src/lakebench/spark/scripts/detection_rules.py"


def _rust_thresholds() -> dict[str, float]:
    src = AMOUNTS_RS.read_text()
    body = src[src.index("pub fn reporting_threshold") :]
    body = body[: body.index("\n}\n")]
    out: dict[str, float] = {}
    for arm, val in re.findall(r"^\s*((?:\"[A-Z]{3}\"\s*\|?\s*)+)=>\s*([\d_\.]+)", body, re.M):
        for ccy in re.findall(r'"([A-Z]{3})"', arm):
            out[ccy] = float(val.replace("_", ""))
    return out


def _python_thresholds() -> dict[str, float]:
    for node in ast.parse(RULES_PY.read_text()).body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == "_STRUCTURING_THRESHOLDS" for t in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("_STRUCTURING_THRESHOLDS not found")


def test_generator_and_rule_thresholds_agree():
    rust, py = _rust_thresholds(), _python_thresholds()
    assert len(rust) >= 15
    assert rust == py
