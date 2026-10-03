"""AML-5: the reason-code vocabulary (spark/scripts/aml_reason_codes.py).
Loaded as a plain module: it has no pyspark import at its top."""

from __future__ import annotations

import ast
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "src" / "lakebench" / "spark" / "scripts"


def _rules() -> list[str]:
    tree = ast.parse((SCRIPTS / "detection_rules.py").read_text())
    node = next(
        n
        for n in tree.body
        if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", None) == "_RULE_DISPATCH"
    )
    return [k.value for k in node.value.keys]  # type: ignore[attr-defined]


def test_every_rule_has_a_base_code_first(load_script):
    rc = load_script("aml_reason_codes")
    assert set(rc.BASE_CODE) == set(_rules())
    for rule, codes in rc.REASON_CODES.items():
        assert codes[0] == rc.BASE_CODE[rule], rule
        assert len(codes) == len(set(codes)), rule
        prefix = rule.split("_")[0] + "_"
        assert all(c.startswith(prefix) for c in codes), (rule, codes)


def test_conditional_keys_are_rules_or_their_projections(load_script):
    rc = load_script("aml_reason_codes")
    for key in rc.CONDITIONAL_CODES:
        assert key.split(":")[0] in rc.BASE_CODE, key
    assert set(rc.BASE_CODE) <= set(rc.CONDITIONAL_CODES)


def test_vocabulary_digest_is_stable_and_moves_with_the_vocabulary(load_script):
    rc = load_script("aml_reason_codes")
    d = rc.vocabulary_digest()
    assert d == rc.vocabulary_digest() and len(d) == 16
    rc.BASE_CODE["W8_dormant_reactivation"] = "W8_OTHER"
    assert rc.vocabulary_digest() != d


def test_no_pyspark_at_module_top():
    tree = ast.parse((SCRIPTS / "aml_reason_codes.py").read_text())
    top = [n for n in tree.body if isinstance(n, (ast.Import, ast.ImportFrom))]
    assert all(
        not (getattr(n, "module", "") or "").startswith("pyspark")
        and all(not a.name.startswith("pyspark") for a in n.names)
        for n in top
    )
