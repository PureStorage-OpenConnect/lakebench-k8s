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


def test_aml_reference_files_exist_and_parse():
    """The packaged AML reference JSON files must exist, be valid JSON,
    and carry the entries array the rules expect."""
    import json

    ref_dir = Path(__file__).resolve().parents[1] / "src/lakebench/spark/data/aml"
    for filename in (
        "high_risk_jurisdictions.json",
        "synthetic_corridors.json",
    ):
        p = ref_dir / filename
        assert p.exists(), f"missing AML reference {p}"
        payload = json.loads(p.read_text())
        assert "entries" in payload
        assert isinstance(payload["entries"], list)
        assert len(payload["entries"]) > 0
        assert "source" in payload
        assert "license" in payload


def test_aml_reference_shapes():
    """Reference tables' entries must carry the columns the rule joins on."""
    import json

    ref_dir = Path(__file__).resolve().parents[1] / "src/lakebench/spark/data/aml"
    hrj = json.loads((ref_dir / "high_risk_jurisdictions.json").read_text())["entries"]
    for row in hrj:
        assert "country_code" in row and len(row["country_code"]) == 2
        assert row["risk_tier"] in ("grey", "black")

    corr = json.loads((ref_dir / "synthetic_corridors.json").read_text())
    assert corr["source"].startswith("SYNTHETIC")
    for row in corr["entries"]:
        assert row["risk_tier"] == "synthetic_corridor"
    # The sanctions and PEP lists are per-corpus generator output
    # (bronze/watchlist.parquet), not packaged files.
    assert not (ref_dir / "sanctions_list.json").exists()
    assert not (ref_dir / "pep_list.json").exists()


def test_fatf_list_is_dated_and_sourced():
    import json

    ref_dir = Path(__file__).resolve().parents[1] / "src/lakebench/spark/data/aml"
    hrj = json.loads((ref_dir / "high_risk_jurisdictions.json").read_text())
    assert hrj["as_of"] == "2026-06-19" and "fatf-gafi.org" in hrj["source"]
    tiers = {}
    for e in hrj["entries"]:
        tiers.setdefault(e["risk_tier"], set()).add(e["country_code"])
    assert tiers["black"] == {"IR", "KP", "MM"}
    assert len(tiers["grey"]) == 22 and {"BA", "IQ"} <= tiers["grey"]
    assert not {"DZ", "NA", "AE"} & tiers["grey"]


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
    scoped = _lit(DETECTION_RULES_PATH, "CUSTOMER_SCOPED_RULES")
    dispatch = set(_lit(DETECTION_RULES_PATH, "RULE_TARGET_TYPOLOGY"))
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


@pytest.mark.parametrize(
    ("node_gib", "expected"),
    [
        # No cluster capacity (offline autosizing): the AML default 24g
        # (16g was on the edge; three-iter S1 crashed thrift mid-query).
        (None, "24g"),
        # Under the 36 GiB allocatable threshold the target is
        # min(20, node - 8) GiB, leaving ~8 GiB for Spark overhead, kubelet
        # and co-scheduled pods, so a small node never gets a Pending pod.
        (16, "8g"),
        (24, "16g"),
        # At 36 GiB the cap branch is skipped: the full 24g.
        (36, "24g"),
    ],
)
def test_autosizer_sizes_spark_thrift_memory_for_financial(node_gib, expected):
    """Spark-thrift default 4g OOMs on every AML benchmark query at scale 1.
    The autosizer must set the resolved config's ``spark_thrift.memory`` to
    24g when ``workload.schema=financial`` and the user did not override,
    capped to fit the cluster's largest allocatable node.

    The autosizer runs against a real financial config (not a source grep),
    so a refactor that keeps the strings but breaks the mutation is caught.
    """
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s.client import ClusterCapacity

    cfg = LakebenchConfig.model_validate(
        {
            "name": "aml-autosizer-test",
            "recipe": "polaris-iceberg-spark-thrift",
            "platform": {
                "storage": {
                    "s3": {
                        "endpoint": "http://example:80",
                        "access_key": "x",
                        "secret_key": "y",
                        "buckets": {
                            "bronze": "aml-autosizer-test-bronze",
                            "silver": "aml-autosizer-test-silver",
                            "gold": "aml-autosizer-test-gold",
                        },
                    },
                },
            },
            "architecture": {
                "workload": {"schema": "financial", "datagen": {"scale": 1}},
                "query_engine": {"type": "spark-thrift"},
            },
        }
    )
    cap = (
        None
        if node_gib is None
        else ClusterCapacity(
            total_cpu_millicores=8000,
            total_memory_bytes=node_gib * 1024**3,
            node_count=1,
            largest_node_cpu_millicores=8000,
            largest_node_memory_bytes=node_gib * 1024**3,
        )
    )
    resolve_auto_sizing(cfg, cluster_capacity=cap)
    resolved = cfg.architecture.query_engine.spark_thrift.memory
    assert resolved == expected, f"expected {expected} on a {node_gib} GiB node, got {resolved!r}"


def test_gold_finalize_detection_rules_subset_of_dispatcher():
    """Every rule id in DEFAULT_DETECTION_RULES must
    exist in detection_rules._RULE_DISPATCH, otherwise ``get_rule``
    returns None and the loop silently skips.
    """
    gf_path = (
        Path(__file__).resolve().parents[1]
        / "src/lakebench/spark/scripts/gold_finalize_financial.py"
    )
    gf_tree = ast.parse(gf_path.read_text())
    tuple_node = None
    for node in gf_tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if getattr(t, "id", None) == "DEFAULT_DETECTION_RULES":
                    tuple_node = node.value
                    break
    assert isinstance(tuple_node, ast.Tuple), "DEFAULT_DETECTION_RULES must be a tuple literal"
    tuple_ids = {e.value for e in tuple_node.elts if isinstance(e, ast.Constant)}

    dr_tree = _module_ast()
    dispatch = None
    for node in dr_tree.body:
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if getattr(t, "id", None) == "_RULE_DISPATCH":
                    dispatch = node.value
                    break
    assert dispatch is not None
    dispatch_keys = {k.value for k in dispatch.keys if isinstance(k, ast.Constant)}
    missing = tuple_ids - dispatch_keys
    assert not missing, (
        f"DEFAULT_DETECTION_RULES references rules missing from _RULE_DISPATCH: "
        f"{missing}; the detection loop would silently skip these at runtime"
    )


def test_w7_country_dedup_is_deterministic():
    """Adversarial-review finding C: ``dropDuplicates(['entity_id'])``
    is shuffle-order-dependent when an entity has multiple country
    values. Re-runs of gold_finalize against the same silver produce
    different alert counts, breaking the "reproducible per rule"
    contract. Fix must use a deterministic aggregation.
    """
    tree = _module_ast()
    fn = next(
        (
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "w7_cross_border_high_risk"
        ),
        None,
    )
    assert fn is not None
    body = ast.unparse(fn)
    assert "dropDuplicates(" not in body, (
        "w7 must not use dropDuplicates for entity-country collapse -- "
        "shuffle-order dependent, violates run-to-run reproducibility"
    )
    assert "groupBy" in body and "bene_country" in body, (
        "w7 must collapse silver.entities to one country per entity_id via "
        "a deterministic aggregation"
    )


# ---------------------------------------------------------------------------
# W1 vertex-cap skip is a distinct third outcome, not a zero.
# ---------------------------------------------------------------------------

GOLD_FINALIZE_PATH = Path(__file__).resolve().parents[1] / (
    "src/lakebench/spark/scripts/gold_finalize_financial.py"
)


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

    tree = _module_ast()
    driver_map = _module_const_dict(tree, "RULE_TARGET_TYPOLOGY")
    assert driver_map is not None, "RULE_TARGET_TYPOLOGY must be defined"
    assert driver_map == RULE_TARGETS, (
        "RULE_TARGET_TYPOLOGY (detection_rules) and RULE_TARGETS (aml_queries) "
        f"drifted: {driver_map} vs {RULE_TARGETS}"
    )


def test_projection_is_run_scoped_and_non_null_guarded():
    """_project_derived_gold must (a) filter to this run's alerts so a skip
    can't re-emit stale prior-run clusters as fresh, and (b) guarantee the
    NOT NULL derived columns receive non-null input regardless of
    storeAssignmentPolicy."""
    body = GOLD_FINALIZE_PATH.read_text()
    assert 'col("run_id") == lit(run_id)' in body, "projection must be run-scoped"
    assert 'col("related_entity_ids").isNotNull()' in body
    assert 'col("alert_score").isNotNull()' in body


def test_score_financial_scopes_alerts_by_run_id():
    """score_financial must scope its gold.alerts read to the
    current run (via detection_status.run_id) so stale prior-run alerts from
    a skipped rule on a reused catalog don't inflate total_alerts/fp_rate."""
    p = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/score_financial.py"
    body = p.read_text()
    assert "current_run_id" in body
    assert 'col("run_id") == lit(current_run_id)' in body


def test_detected_ts_in_empty_schema_and_all_ddls():
    """detected_ts must be present in ALERT_COLUMNS after evidence
    (the empty-alerts schema and gold_finalize's DDL are built from it), in
    the other gold.alerts DDL sites, with the reused-catalog ALTER guard, or
    a positional INSERT ... SELECT * misaligns."""
    root = Path(__file__).resolve().parents[1]
    names = [c[0] for c in _alert_columns()]
    assert names.index("evidence") < names.index("detected_ts")
    det = (root / "src/lakebench/spark/scripts/detection_rules.py").read_text()
    assert "for name, ddl_type, nullable in ALERT_COLUMNS" in det  # _empty_alerts_df

    ddl = (root / "src/lakebench/deploy/financial_ddl.py").read_text()
    assert "detected_ts" in ddl

    # Reused-catalog upgrade: one helper adds missing trailing columns
    # (through ensure_column_with_retry's live-schema check, not the invalid
    # `ADD COLUMN IF NOT EXISTS`) and refuses a table out of order.
    for script in (
        "gold_finalize_financial.py",
        "gold_refresh_financial.py",
    ):
        text = (root / "src/lakebench/spark/scripts" / script).read_text()
        assert "ensure_alert_columns(spark," in text, script
        assert "ALERT_COLUMNS)" in text, script


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
    either run or be recorded as skipped, or its typology reads 0 alerts
    (review finding: W5/W6 were targeted but never invoked)."""
    import re as _re

    scripts = DETECTION_RULES_PATH.parent

    def _tuple(path, name):
        tree = ast.parse(Path(path).read_text())
        for n in tree.body:
            if isinstance(n, ast.Assign) and getattr(n.targets[0], "id", "") == name:
                return set(ast.literal_eval(n.value))
        raise AssertionError(name)

    src = DETECTION_RULES_PATH.read_text()
    block = _re.search(r"RULE_TARGET_TYPOLOGY = \{(.*?)\n\}", src, _re.S).group(1)
    targeted = set(_re.findall(r'"(W\d+_\w+)": "', block))
    batch = _tuple(scripts / "gold_finalize_financial.py", "DEFAULT_DETECTION_RULES")
    cont = _tuple(scripts / "gold_refresh_financial.py", "CONTINUOUS_RULES") | _tuple(
        scripts / "gold_refresh_financial.py", "CONTINUOUS_SKIPPED_RULES"
    )
    assert {"W5_sanctions_match", "W6_pep_counterparty"} <= targeted
    assert targeted <= batch, targeted - batch
    assert targeted <= cont, targeted - cont
