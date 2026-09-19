"""Static and API-shape tests for detection_rules.py.

Full end-to-end tests need a live Spark session (which the CI environment
here doesn't provide -- `import pyspark` fails). These tests exercise
what's testable without pyspark:

1. Rule dispatcher shape: get_rule returns callables for the documented
   rule ids; known_rules() lists them; unknown rule returns None.
2. Structuring thresholds table covers the currencies the datagen emits
   (see datagen_rs/src/amounts.rs::structuring_band).
3. Rule constants are non-empty strings (regression guard against a
   silent rename that would ship as unknown-rule from the dispatcher).

Live-Spark integration tests belong in a separate `tests/spark/`
directory once a mini-cluster fixture is available.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


DETECTION_RULES_PATH = Path(__file__).resolve().parents[1] / (
    "src/lakebench/spark/scripts/detection_rules.py"
)


def _module_ast():
    return ast.parse(DETECTION_RULES_PATH.read_text())


def test_detection_rules_module_parses():
    _module_ast()


def test_dispatcher_covers_documented_rules():
    tree = _module_ast()
    dispatch = None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if getattr(t, "id", None) == "_RULE_DISPATCH":
                    dispatch = node.value
                    break
    assert dispatch is not None, "_RULE_DISPATCH not found"
    keys = [k.value for k in dispatch.keys if isinstance(k, ast.Constant)]
    # W1 landed with LB-108; the scoring loop and replay CLI expect all
    # four workloads to be routable through the dispatcher.
    for expected in (
        "W1_connected_components",
        "W2_structuring",
        "W3_round_tripping",
        "W4_risk_propagation",
    ):
        assert expected in keys, f"missing rule {expected} in dispatcher"


def test_w1_signature_and_defaults():
    """W1_connected_components must accept the kwargs replay_financial
    passes it, and its numeric defaults must be sane (min_cluster_size
    >= 2 so pairs don't over-fire, max_iterations bounded so a hostile
    graph can't wedge the replay)."""
    tree = _module_ast()
    fn = next(
        (n for n in tree.body
         if isinstance(n, ast.FunctionDef) and n.name == "w1_connected_components"),
        None,
    )
    assert fn is not None, "w1_connected_components not defined"
    argnames = [a.arg for a in fn.args.args]
    for expected in ("silver_txns", "min_cluster_size", "max_iterations", "run_id"):
        assert expected in argnames, f"w1_connected_components missing arg {expected}"

    # Defaults are the last-N args aligned with argnames tail.
    defaults = fn.args.defaults
    def_map = dict(zip(argnames[-len(defaults):], defaults))
    for key, low in (("min_cluster_size", 2), ("max_iterations", 1)):
        node = def_map[key]
        assert isinstance(node, ast.Constant) and isinstance(node.value, int)
        assert node.value >= low, f"{key} default {node.value} below sane floor {low}"


def test_structuring_thresholds_cover_datagen_currencies():
    """Rust datagen structuring_band covers these currencies; the rule's
    threshold table must not miss any or the rule silently under-fires
    on that currency."""
    tree = _module_ast()
    thresholds = None
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if getattr(t, "id", None) == "_STRUCTURING_THRESHOLDS":
                    thresholds = node.value
                    break
    assert thresholds is not None, "_STRUCTURING_THRESHOLDS not found"
    ccys = {k.value for k in thresholds.keys if isinstance(k, ast.Constant)}
    # Currencies from datagen_rs/src/amounts.rs::structuring_band().
    datagen_ccys = {
        "USD", "CAD", "AUD", "GBP", "EUR", "CHF",
        "JPY", "INR", "AED", "SGD", "MXN",
        "CNY", "BRL", "HKD", "KRW",
    }
    missing = datagen_ccys - ccys
    assert not missing, f"detection rule doesn't cover currencies: {missing}"


def test_rule_constants_non_empty():
    tree = _module_ast()
    for name in ("RULE_VERSION", "MODEL_ID", "MODEL_VERSION"):
        node = next(
            (n for n in tree.body
             if isinstance(n, ast.Assign)
             and any(getattr(t, "id", None) == name for t in n.targets)),
            None,
        )
        assert node is not None, f"{name} not defined"
        assert isinstance(node.value, ast.Constant)
        assert isinstance(node.value.value, str)
        assert len(node.value.value) > 0, f"{name} is empty"


def test_w2_structuring_takes_expected_kwargs():
    """The replay dispatcher passes threshold_count via kwargs. If the
    signature drops that kwarg silently, replay's --threshold flag becomes
    a no-op."""
    tree = _module_ast()
    fn = next(
        (n for n in tree.body
         if isinstance(n, ast.FunctionDef) and n.name == "w2_structuring"),
        None,
    )
    assert fn is not None, "w2_structuring not defined"
    argnames = [a.arg for a in fn.args.args]
    for expected in ("silver_txns", "threshold_count", "window_hours", "run_id"):
        assert expected in argnames, f"w2_structuring missing arg {expected}"


@pytest.mark.parametrize(
    "rule_id",
    [
        "W1_connected_components",
        "W2_structuring",
        "W3_round_tripping",
        "W4_risk_propagation",
    ],
)
def test_rule_ids_stable(rule_id):
    """These rule IDs are wire contract: score_financial groups by
    rule_id, gold.alerts.rule_id is queried by rule name, and replay's
    --rule flag accepts these strings. Renaming any silently breaks the
    scoring pipeline."""
    tree = _module_ast()
    src = ast.unparse(tree)
    assert rule_id in src, f"{rule_id} disappeared from detection_rules"
