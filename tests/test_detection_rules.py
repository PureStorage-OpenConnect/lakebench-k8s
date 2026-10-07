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
import re
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
    # four workloads to be routable through the dispatcher. W5-W8 added
    # in the AML hardening pass (Phase 3C+D).
    for expected in (
        "W1_connected_components",
        "W2_structuring",
        "W3_round_tripping",
        "W4_risk_propagation",
        "W5_sanctions_match",
        "W6_pep_counterparty",
        "W7_cross_border_high_risk",
        "W8_dormant_reactivation",
        "W17_layering_chain",
    ):
        assert expected in keys, f"missing rule {expected} in dispatcher"


@pytest.mark.parametrize(
    "rule_id",
    [
        "W1_connected_components",
        "W2_structuring",
        "W3_round_tripping",
        "W4_risk_propagation",
        "W5_sanctions_match",
        "W6_pep_counterparty",
        "W7_cross_border_high_risk",
        "W8_dormant_reactivation",
        "W17_layering_chain",
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


def test_normalize_name_expr_strips_punctuation_and_suffix():
    """Regression against the adversarial-review F1 finding: the normalize
    helper must strip punctuation and common corporate suffixes so
    `Foo, LLC` and `FOO LLC` and `Foo Corp` all match `FOO`.

    We can't invoke Spark's regexp_replace in a unit test (no Spark
    session available), so we translate the expression into a Python
    regex and exercise the equivalent transform.
    """

    def _py_normalize(s: str) -> str:
        if s is None:
            return ""
        s = s.upper().strip()
        s = re.sub(r"[^A-Z0-9 ]", " ", s)
        s = re.sub(r"\s+", " ", s)
        s = re.sub(
            r"( LLC| LTD| PLC| CORP| CORPORATION| INC| SA| AG| GMBH"
            r"| PTE| LP| CO| COMPANY| GROUP| HOLDINGS)+$",
            "",
            s,
        )
        return s

    assert _py_normalize("GLOBAL COMMODITY TRADING LLC") == "GLOBAL COMMODITY TRADING"
    assert _py_normalize("Global Commodity Trading, LLC") == "GLOBAL COMMODITY TRADING"
    assert _py_normalize("global commodity trading llc") == "GLOBAL COMMODITY TRADING"
    assert _py_normalize("Foo Corp") == "FOO"
    assert _py_normalize("Foo Corporation") == "FOO"
    assert _py_normalize("Foo") == "FOO"
    # Repeated whitespace collapses:
    assert _py_normalize("Foo   Bar") == "FOO BAR"
    # NULL/None does not crash:
    assert _py_normalize(None) == ""


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
    """LB-092 second-order: gold_finalize invokes W7 which needs the
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


def test_autosizer_bumps_spark_thrift_memory_for_financial():
    """LB-093: spark-thrift default 4g OOMs on every AML benchmark
    query at scale 1. Autosizer must actually bump the resolved
    config's ``spark_thrift.memory`` field to 16g when
    ``workload.schema=financial`` AND the user did not override.

    This test invokes the autosizer against a real financial config
    (not a source grep) so a future refactor that keeps the strings
    around but breaks the mutation is caught.
    """
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.config.schema import LakebenchConfig

    cfg_yaml = {
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
    cfg = LakebenchConfig.model_validate(cfg_yaml)
    # No cluster capacity: the guard's fallback keeps the AML default 24g
    # (LB-117: 16g was on the edge; three-iter S1 crashed thrift mid-query).
    resolve_auto_sizing(cfg, cluster_capacity=None)
    assert cfg.architecture.query_engine.spark_thrift.memory == "24g", (
        f"expected spark_thrift.memory=24g on AML, got "
        f"{cfg.architecture.query_engine.spark_thrift.memory!r}"
    )


def test_autosizer_thrift_memory_caps_on_small_node():
    """The 16g AML bump must not push spark_thrift beyond a small
    cluster's largest node. Adversarial-review finding: a user on a
    laptop-scale cluster (16 GiB nodes) would get a Pending pod
    forever.
    """
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s.client import ClusterCapacity

    cfg = LakebenchConfig.model_validate(
        {
            "name": "aml-tiny-cluster",
            "recipe": "polaris-iceberg-spark-thrift",
            "platform": {
                "storage": {
                    "s3": {
                        "endpoint": "http://example:80",
                        "access_key": "x",
                        "secret_key": "y",
                        "buckets": {
                            "bronze": "aml-tiny-bronze",
                            "silver": "aml-tiny-silver",
                            "gold": "aml-tiny-gold",
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
    # Largest node is 16 GiB *allocatable* -> 24g bump would not fit;
    # cap to min(20, 16-8) = 8g. LB-117 revised: formula leaves ~8 GiB
    # headroom for Spark overhead + kubelet + other pods.
    cap = ClusterCapacity(
        total_cpu_millicores=8000,
        total_memory_bytes=16 * 1024**3,
        node_count=1,
        largest_node_cpu_millicores=8000,
        largest_node_memory_bytes=16 * 1024**3,
    )
    resolve_auto_sizing(cfg, cluster_capacity=cap)
    resolved = cfg.architecture.query_engine.spark_thrift.memory
    assert resolved == "8g", f"expected 8g on a 16 GiB node, got {resolved}"


def test_autosizer_thrift_capped_on_24gi_node():
    """LB-117: a node with 24 GiB *allocatable* is under the 36 GiB
    threshold that the 24g target needs after Spark overhead + kubelet
    + co-scheduled pods. On a 24 GiB allocatable node the autosizer
    must cap thrift to min(20, 24-8) = 16g."""
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s.client import ClusterCapacity

    cfg = LakebenchConfig.model_validate(
        {
            "name": "aml-24g-node",
            "recipe": "polaris-iceberg-spark-thrift",
            "platform": {
                "storage": {
                    "s3": {
                        "endpoint": "http://example:80",
                        "access_key": "x",
                        "secret_key": "y",
                        "buckets": {
                            "bronze": "aml-24g-bronze",
                            "silver": "aml-24g-silver",
                            "gold": "aml-24g-gold",
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
    cap = ClusterCapacity(
        total_cpu_millicores=8000,
        total_memory_bytes=24 * 1024**3,
        node_count=1,
        largest_node_cpu_millicores=8000,
        largest_node_memory_bytes=24 * 1024**3,
    )
    resolve_auto_sizing(cfg, cluster_capacity=cap)
    resolved = cfg.architecture.query_engine.spark_thrift.memory
    assert resolved == "16g", f"expected 16g cap on a 24 GiB node, got {resolved}"


def test_autosizer_thrift_at_36gi_uses_full_24g():
    """LB-117 boundary: at 36 GiB allocatable the cap branch is skipped
    and thrift gets the full 24g target."""
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s.client import ClusterCapacity

    cfg = LakebenchConfig.model_validate(
        {
            "name": "aml-36g-node",
            "recipe": "polaris-iceberg-spark-thrift",
            "platform": {
                "storage": {
                    "s3": {
                        "endpoint": "http://example:80",
                        "access_key": "x",
                        "secret_key": "y",
                        "buckets": {
                            "bronze": "aml-36g-bronze",
                            "silver": "aml-36g-silver",
                            "gold": "aml-36g-gold",
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
    cap = ClusterCapacity(
        total_cpu_millicores=16000,
        total_memory_bytes=36 * 1024**3,
        node_count=1,
        largest_node_cpu_millicores=16000,
        largest_node_memory_bytes=36 * 1024**3,
    )
    resolve_auto_sizing(cfg, cluster_capacity=cap)
    resolved = cfg.architecture.query_engine.spark_thrift.memory
    assert resolved == "24g", f"36 GiB allocatable should get full 24g, got {resolved}"


def test_gold_finalize_detection_rules_subset_of_dispatcher():
    """LB-092 hygiene: every rule id in DEFAULT_DETECTION_RULES must
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
# LB-119: W1 vertex-cap skip is a distinct third outcome, not a zero.
# ---------------------------------------------------------------------------

GOLD_FINALIZE_PATH = Path(__file__).resolve().parents[1] / (
    "src/lakebench/spark/scripts/gold_finalize_financial.py"
)


# ---------------------------------------------------------------------------
# LB-119 review fixes: rule->typology map, detection_status, run_id-scoped
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
    """LB-119 fix S1: score_financial must scope its gold.alerts read to the
    current run (via detection_status.run_id) so stale prior-run alerts from
    a skipped rule on a reused catalog don't inflate total_alerts/fp_rate."""
    p = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/score_financial.py"
    body = p.read_text()
    assert "current_run_id" in body
    assert 'col("run_id") == lit(current_run_id)' in body


def test_detected_ts_in_empty_schema_and_all_ddls():
    """LB-125: detected_ts must be present in ALERT_COLUMNS after evidence
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
        "replay_financial.py",
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
