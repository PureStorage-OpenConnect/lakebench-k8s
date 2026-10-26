"""Static tests for the AML detection rules and the files around them.

The rules run inside Spark drivers, so most of these read the driver scripts
as source or AST instead of importing them: the packaged AML reference data,
the rule dispatch and typology maps against their orchestrator-side copies,
which rules each gold script schedules and the gold.alerts column order. Also the
scripts ConfigMaps shipping the reference JSON and the Spark Thrift memory
the autosizer gives a financial workload.
"""

from __future__ import annotations

import ast
from pathlib import Path

DETECTION_RULES_PATH = Path(__file__).resolve().parents[1] / (
    "src/lakebench/spark/scripts/detection_rules.py"
)
# The pure rule metadata (RULE_TARGET_TYPOLOGY, CUSTOMER_SCOPED_RULES,
# HIGH_PRIORITY_CUTOFFS) lives here now and ships flat to the driver;
# detection_rules re-exports it.
RULE_META_PATH = Path(__file__).resolve().parents[1] / "src/lakebench/aml/rule_meta.py"


def _module_ast():
    return ast.parse(DETECTION_RULES_PATH.read_text())


def test_aml_reference_shapes():
    """Reference tables' entries must carry the columns the rule joins on."""
    import json

    ref_dir = Path(__file__).resolve().parents[1] / "src/lakebench/spark/data/aml"
    hrj = json.loads((ref_dir / "high_risk_jurisdictions.json").read_text())
    corr = json.loads((ref_dir / "synthetic_corridors.json").read_text())
    for doc in (hrj, corr):
        assert doc["entries"] and "license" in doc
    for row in hrj["entries"]:
        assert "country_code" in row and len(row["country_code"]) == 2
        assert row["risk_tier"] in ("grey", "black")

    assert corr["source"].startswith("SYNTHETIC")
    for row in corr["entries"]:
        assert row["risk_tier"] == "synthetic_corridor"
    # The sanctions and PEP lists are per-corpus generator output
    # (bronze/watchlist.parquet), not packaged files.
    assert not (ref_dir / "sanctions_list.json").exists()
    assert not (ref_dir / "pep_list.json").exists()


def test_customer_scope_lists_agree_across_the_package_boundary():
    """detection_rules (driver side), tm_operations and the config default
    must name the same counterparty scenarios, and every rule is either
    customer-scoped or declared. Drift here fails noncustomer_alerts_declared
    on every live run, or lets a customer-only rule's non-customer alerts
    pass as declared."""
    from lakebench.config.schema import TmOperationsConfig

    def _lit(path, name):
        tree = ast.parse(Path(path).read_text())
        for n in tree.body:
            if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == name:
                v = n.value
                if isinstance(v, ast.Call):  # frozenset({...})
                    v = v.args[0]
                return set(ast.literal_eval(v))
        raise AssertionError(f"{name} not found in {path}")

    scripts = DETECTION_RULES_PATH.parent
    counterparty = _lit(DETECTION_RULES_PATH, "COUNTERPARTY_SCENARIOS")
    scoped = _lit(RULE_META_PATH, "CUSTOMER_SCOPED_RULES")
    dispatch = set(_lit(RULE_META_PATH, "RULE_TARGET_TYPOLOGY"))
    assert counterparty | scoped == dispatch and not counterparty & scoped
    assert _lit(scripts / "tm_operations.py", "DEFAULT_COUNTERPARTY_SCENARIOS") == counterparty
    assert set(TmOperationsConfig().counterparty_scenarios) == counterparty


def test_deploy_scripts_configmap_ships_aml_json():
    """gold_finalize invokes W7 which needs the
    high_risk_jurisdictions.json reference file. That file lives under
    src/lakebench/spark/data/aml/ and is NOT under spark/scripts/, so
    the pre-fix deploy_scripts_configmap did not ship it. Without it,
    W7 previously crashed inside the driver.
    """
    from lakebench.config import LakebenchConfig
    from lakebench.modules.pipeline_engines.spark.scripts_maps import build_script_configmaps

    cfg = LakebenchConfig(name="t")
    shipped = {k for cm in build_script_configmaps(cfg, "ns") for k in cm["data"]}
    assert "high_risk_jurisdictions.json" in shipped, (
        "the scripts ConfigMaps must ship the AML reference JSONs alongside "
        "the pipeline scripts; W7 crashes in the driver otherwise"
    )


# The financial spark-thrift autosizer memory bump is now unreachable:
# financial scores through Trino only, so a financial spark-thrift config is
# refused at load (test_config.test_aml_without_a_scoring_query_engine_is_refused).
# The test that exercised that bump is removed with the path it covered.


# ---------------------------------------------------------------------------
# Rule->typology map, detection_status, run_id-scoped
# projection, non-null guards, reason normalization.
# ---------------------------------------------------------------------------


def _module_const_dict(tree, name):
    """Extract a module-level ``name = {...}`` literal dict from an AST."""
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            getattr(t, "id", None) == name for t in node.targets
        ):
            return {
                k.value: (v.value if isinstance(v, ast.Constant) else None)
                for k, v in zip(node.value.keys, node.value.values, strict=True)
            }
    return None


def test_rule_target_typology_matches_aml_queries():
    """The driver-side RULE_TARGET_TYPOLOGY must stay in lock-step with the
    orchestrator-side RULE_TARGETS -- they live on opposite sides of the
    package boundary and score_financial relies on them agreeing."""
    from lakebench.benchmark.aml_queries import RULE_TARGETS

    tree = ast.parse(RULE_META_PATH.read_text())
    driver_map = _module_const_dict(tree, "RULE_TARGET_TYPOLOGY")
    assert driver_map is not None, "RULE_TARGET_TYPOLOGY must be defined"
    assert driver_map == RULE_TARGETS, (
        "RULE_TARGET_TYPOLOGY (aml.rule_meta) and RULE_TARGETS (aml_queries) "
        f"drifted: {driver_map} vs {RULE_TARGETS}"
    )


def test_score_financial_scopes_alerts_by_run_id():
    """score_financial must scope its gold.alerts read to the
    current run (via detection_status.run_id) so stale prior-run alerts from
    a skipped rule on a reused catalog don't inflate total_alerts/fp_rate."""
    p = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/score_financial.py"
    body = p.read_text()
    assert "current_run_id" in body
    assert 'col("run_id") == lit(current_run_id)' in body


def test_detected_ts_follows_evidence_in_alert_columns():
    """detected_ts must come after evidence in ALERT_COLUMNS: the gold.alerts
    DDL and the empty-alerts schema are built from it, and gold.alerts is
    written positionally (INSERT ... SELECT *)."""
    names = [c[0] for c in _alert_columns()]
    assert names.index("evidence") < names.index("detected_ts")


def _alert_columns():
    tree = _module_ast()
    node = next(
        n
        for n in tree.body
        if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", None) == "ALERT_COLUMNS"
    )
    return ast.literal_eval(node.value)


def test_every_targeted_rule_is_scheduled_or_declared_skipped():
    """A rule with a planted target must run in batch, and in continuous
    either run or be recorded as skipped, or its typology reads 0 alerts.
    Every batch rule must be in _RULE_DISPATCH, or ``get_rule`` returns None
    and the detection loop silently skips it."""
    import re as _re

    scripts = DETECTION_RULES_PATH.parent

    def _tuple(path, name):
        tree = ast.parse(Path(path).read_text())
        for n in tree.body:
            if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == name:
                return set(ast.literal_eval(n.value))
        raise AssertionError(name)

    src = RULE_META_PATH.read_text()
    block = _re.search(r"RULE_TARGET_TYPOLOGY = \{(.*?)\n\}", src, _re.S).group(1)
    targeted = set(_re.findall(r'"(W\d+_\w+)": "', block))
    batch = _tuple(scripts / "gold_finalize_financial.py", "DEFAULT_DETECTION_RULES")
    cont = _tuple(scripts / "gold_refresh_financial.py", "CONTINUOUS_RULES") | _tuple(
        scripts / "gold_refresh_financial.py", "CONTINUOUS_SKIPPED_RULES"
    )
    assert {"W5_sanctions_match", "W6_pep_counterparty"} <= targeted
    assert targeted <= batch, targeted - batch
    dispatch = _module_const_dict(_module_ast(), "_RULE_DISPATCH")
    assert batch <= set(dispatch), batch - set(dispatch)
    assert targeted <= cont, targeted - cont
