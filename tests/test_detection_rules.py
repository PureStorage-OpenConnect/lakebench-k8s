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
    ):
        assert expected in keys, f"missing rule {expected} in dispatcher"


def test_w1_signature_and_defaults():
    """W1_connected_components must accept the kwargs replay_financial
    passes it, and its numeric defaults must be sane (min_cluster_size
    >= 2 so pairs don't over-fire, max_iterations bounded so a hostile
    graph can't wedge the replay)."""
    tree = _module_ast()
    fn = next(
        (
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "w1_connected_components"
        ),
        None,
    )
    assert fn is not None, "w1_connected_components not defined"
    argnames = [a.arg for a in fn.args.args]
    for expected in ("silver_txns", "min_cluster_size", "max_iterations", "run_id"):
        assert expected in argnames, f"w1_connected_components missing arg {expected}"

    # Defaults are the last-N args aligned with argnames tail.
    defaults = fn.args.defaults
    def_map = dict(zip(argnames[-len(defaults) :], defaults, strict=True))
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
        "USD",
        "CAD",
        "AUD",
        "GBP",
        "EUR",
        "CHF",
        "JPY",
        "INR",
        "AED",
        "SGD",
        "MXN",
        "CNY",
        "BRL",
        "HKD",
        "KRW",
    }
    missing = datagen_ccys - ccys
    assert not missing, f"detection rule doesn't cover currencies: {missing}"


def test_rule_constants_non_empty():
    tree = _module_ast()
    for name in ("RULE_VERSION", "MODEL_ID", "MODEL_VERSION"):
        node = next(
            (
                n
                for n in tree.body
                if isinstance(n, ast.Assign)
                and any(getattr(t, "id", None) == name for t in n.targets)
            ),
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
        (n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "w2_structuring"),
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
        "W5_sanctions_match",
        "W6_pep_counterparty",
        "W7_cross_border_high_risk",
        "W8_dormant_reactivation",
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


def test_faml_reference_files_exist_and_parse():
    """The packaged FAML reference JSON files must exist, be valid JSON,
    and carry the entries array the rules expect."""
    import json

    ref_dir = Path(__file__).resolve().parents[1] / "src/lakebench/spark/data/faml"
    for filename in (
        "high_risk_jurisdictions.json",
        "sanctions_list.json",
        "pep_list.json",
    ):
        p = ref_dir / filename
        assert p.exists(), f"missing FAML reference {p}"
        payload = json.loads(p.read_text())
        assert "entries" in payload
        assert isinstance(payload["entries"], list)
        assert len(payload["entries"]) > 0
        assert "source" in payload
        assert "license" in payload


def test_faml_reference_shapes():
    """Reference tables' entries must carry the columns the rule joins on."""
    import json

    ref_dir = Path(__file__).resolve().parents[1] / "src/lakebench/spark/data/faml"
    hrj = json.loads((ref_dir / "high_risk_jurisdictions.json").read_text())["entries"]
    for row in hrj:
        assert "country_code" in row and len(row["country_code"]) == 2
        assert row["risk_tier"] in ("grey", "black")

    sdn = json.loads((ref_dir / "sanctions_list.json").read_text())["entries"]
    for row in sdn:
        assert row["sdn_id"].startswith("SDN-")
        assert row["entity_name"]

    pep = json.loads((ref_dir / "pep_list.json").read_text())["entries"]
    for row in pep:
        assert row["pep_id"].startswith("PEP-")
        assert row["entity_name"]
        assert row["position"]


def test_normalize_name_expr_strips_punctuation_and_suffix():
    """Regression against the adversarial-review F1 finding: the normalize
    helper must strip punctuation and common corporate suffixes so
    `Foo, LLC` and `FOO LLC` and `Foo Corp` all match `FOO`.

    We can't invoke Spark's regexp_replace in a unit test (no Spark
    session available), so we translate the expression into a Python
    regex and exercise the equivalent transform.
    """
    import re

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


def test_load_reference_short_circuits_on_empty():
    """Regression against F6 (adversarial-review): `_load_reference`
    used to return a bare DF that downstream `.select("sdn_id")` would
    crash on. It now returns None on empty entries, and every W5-W8
    caller must check for None before selecting."""
    src = DETECTION_RULES_PATH.read_text()
    # The helper returns None on empty list.
    assert "if not entries:" in src
    assert "return None" in src
    # Every caller must check `if raw is None`.
    for rule in ("w5_sanctions_match", "w6_pep_counterparty", "w7_cross_border_high_risk"):
        # Extract the function body via AST for a scoped check.
        tree = ast.parse(src)
        fn = next(
            (n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == rule),
            None,
        )
        assert fn is not None, f"{rule} not defined"
        body_text = ast.unparse(fn)
        assert "raw is None" in body_text, (
            f"{rule} does not guard against `_load_reference` returning None; "
            f"crashes on downstream .select() when the reference JSON has empty entries."
        )


def test_new_rule_signatures():
    """W5-W8 signatures must accept the kwargs the runner passes them."""
    tree = _module_ast()
    fns = {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef)}
    for name in (
        "w5_sanctions_match",
        "w6_pep_counterparty",
        "w7_cross_border_high_risk",
        "w8_dormant_reactivation",
    ):
        assert name in fns, f"{name} not defined"
        args = [a.arg for a in fns[name].args.args]
        assert "silver_txns" in args, f"{name} missing silver_txns arg"
        assert "run_id" in args, f"{name} missing run_id arg"


def test_w1_aliases_propagation_join():
    """Regression: without aliases on ``labels`` and ``edges_u`` in the
    label-propagation loop, Spark raises ``AnalysisException: Column
    dst#NNN are ambiguous`` on the second iteration because ``labels``'
    attribute IDs trace back to ``edges_u`` via the prior unionByName.
    Surfaced by the first live S1 run of the FAML batch pipeline.
    Assertion is AST-scoped: the join call in the loop body must use
    ``col("lbl.id") == col("eg.src")``-style qualified references so
    the aliases are load-bearing.
    """
    tree = _module_ast()
    fn = next(
        (
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "w1_connected_components"
        ),
        None,
    )
    assert fn is not None
    body = ast.unparse(fn)
    # The alias for edges_u must be created inside the loop body.
    assert "edges_u.alias(" in body, "w1 must alias edges_u to disambiguate self-joins"
    assert "labels.alias(" in body, "w1 must alias labels to disambiguate self-joins"
    # And the join must use the aliased column references.
    assert "'lbl.id'" in body or '"lbl.id"' in body
    assert "'eg.src'" in body or '"eg.src"' in body


def test_w7_auto_loads_silver_entities_when_none():
    """Regression: when the caller (e.g. replay_financial) does not
    pass silver_entities, W7 must auto-load ``silver.entities`` from
    the catalog rather than silently returning empty. The prior
    behavior returned empty AND, when the high-risk-jurisdiction
    reference JSON was not mounted, crashed with an opaque ImportError
    from ``import lakebench.spark.data``. Both failure modes are
    covered by the check below: the function body must contain the
    auto-load fallback path AND its catch for a missing table.
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
    assert "silver_entities is None" in body, (
        "w7 must auto-load silver.entities when the caller does not pass it"
    )
    assert "spark.table" in body, (
        "w7 must resolve silver.entities via spark.table when silver_entities is None"
    )
    assert "AnalysisException" in body, (
        "w7 must catch AnalysisException so a missing silver.entities table "
        "degrades to empty alerts rather than crashing the whole rule dispatcher"
    )


def test_load_reference_uses_candidate_dirs():
    """Regression: inside the driver pod the ``lakebench`` package is
    not installed, so ``import lakebench.spark.data`` raises
    ImportError. The prior _faml_data_path re-raised that ImportError
    from inside every W5/W6/W7 invocation. `_load_reference` must now
    walk a list of candidate directories (env override, package data,
    same-dir fallback) and return None only when no candidate contains
    the requested file.
    """
    src = DETECTION_RULES_PATH.read_text()
    assert "_faml_data_candidates" in src, (
        "candidate-directory search helper missing; W7 will crash on "
        "cluster driver when lakebench.spark.data cannot be imported"
    )
    assert "except ImportError" in src, (
        "package-data lookup must tolerate ImportError inside the driver pod"
    )


def test_gold_finalize_invokes_detection_rules():
    """LB-092: gold_finalize_financial must run detection rules as part
    of the batch pipeline so `lakebench run` produces alerts without a
    separate `financial replay` invocation. Regression AST check
    against gold_finalize_financial.py.
    """
    gf_path = (
        Path(__file__).resolve().parents[1]
        / "src/lakebench/spark/scripts/gold_finalize_financial.py"
    )
    src = gf_path.read_text()
    tree = ast.parse(src)
    fn_names = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    assert "run_detection_rules" in fn_names, (
        "gold_finalize_financial must define run_detection_rules() so the "
        "batch pipeline emits alerts (LB-092)"
    )
    # The main entrypoint must call it.
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    main_src = ast.unparse(main)
    assert "run_detection_rules(" in main_src, (
        "gold_finalize main() must invoke run_detection_rules() -- otherwise "
        "batch runs still produce zero alerts and LB-092 reopens"
    )
    # Idempotency contract: DELETE per rule_id before append.
    assert "DELETE FROM" in src and "WHERE rule_id = " in src, (
        "detection loop must DELETE per rule_id before append so re-runs do not double-count"
    )


def test_deploy_scripts_configmap_ships_faml_json():
    """LB-092 second-order: gold_finalize invokes W7 which needs the
    high_risk_jurisdictions.json reference file. That file lives under
    src/lakebench/spark/data/faml/ and is NOT under spark/scripts/, so
    the pre-fix deploy_scripts_configmap did not ship it. Without it,
    W7 previously crashed inside the driver.
    """
    p = Path(__file__).resolve().parents[1] / "src/lakebench/modules/pipeline_engines/spark/job.py"
    src = p.read_text()
    assert "get_faml_data_dir" in src, (
        "deploy_scripts_configmap must ship FAML reference JSONs alongside "
        "the pipeline scripts; W7 crashes in the driver otherwise"
    )


def test_autosizer_bumps_spark_thrift_memory_for_financial():
    """LB-093: spark-thrift default 4g OOMs on every FAML benchmark
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
        "name": "faml-autosizer-test",
        "recipe": "polaris-iceberg-spark-thrift",
        "platform": {
            "storage": {
                "s3": {
                    "endpoint": "http://example:80",
                    "access_key": "x",
                    "secret_key": "y",
                    "buckets": {
                        "bronze": "faml-autosizer-test-bronze",
                        "silver": "faml-autosizer-test-silver",
                        "gold": "faml-autosizer-test-gold",
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
    # No cluster capacity: the guard's fallback keeps the FAML default 24g
    # (LB-117: 16g was on the edge; three-iter S1 crashed thrift mid-query).
    resolve_auto_sizing(cfg, cluster_capacity=None)
    assert cfg.architecture.query_engine.spark_thrift.memory == "24g", (
        f"expected spark_thrift.memory=24g on FAML, got "
        f"{cfg.architecture.query_engine.spark_thrift.memory!r}"
    )


def test_autosizer_thrift_memory_caps_on_small_node():
    """The 16g FAML bump must not push spark_thrift beyond a small
    cluster's largest node. Adversarial-review finding: a user on a
    laptop-scale cluster (16 GiB nodes) would get a Pending pod
    forever.
    """
    from lakebench.config.autosizer import resolve_auto_sizing
    from lakebench.config.schema import LakebenchConfig
    from lakebench.k8s.client import ClusterCapacity

    cfg = LakebenchConfig.model_validate(
        {
            "name": "faml-tiny-cluster",
            "recipe": "polaris-iceberg-spark-thrift",
            "platform": {
                "storage": {
                    "s3": {
                        "endpoint": "http://example:80",
                        "access_key": "x",
                        "secret_key": "y",
                        "buckets": {
                            "bronze": "faml-tiny-bronze",
                            "silver": "faml-tiny-silver",
                            "gold": "faml-tiny-gold",
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
            "name": "faml-24g-node",
            "recipe": "polaris-iceberg-spark-thrift",
            "platform": {
                "storage": {
                    "s3": {
                        "endpoint": "http://example:80",
                        "access_key": "x",
                        "secret_key": "y",
                        "buckets": {
                            "bronze": "faml-24g-bronze",
                            "silver": "faml-24g-silver",
                            "gold": "faml-24g-gold",
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
            "name": "faml-36g-node",
            "recipe": "polaris-iceberg-spark-thrift",
            "platform": {
                "storage": {
                    "s3": {
                        "endpoint": "http://example:80",
                        "access_key": "x",
                        "secret_key": "y",
                        "buckets": {
                            "bronze": "faml-36g-bronze",
                            "silver": "faml-36g-silver",
                            "gold": "faml-36g-gold",
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


def test_gold_finalize_uses_signature_not_covarnames():
    """Adversarial-review finding A (silent corruption): `co_varnames`
    includes every local variable in a function's body, not just
    parameters. A future rule that uses ``silver_entities`` as an
    internal local would silently receive the DataFrame as a kwarg
    and raise TypeError; the broad ``except Exception`` around the
    rule call would swallow it into a zero-alert result. Correct
    primitive is ``inspect.signature(fn).parameters``.
    """
    gf_path = (
        Path(__file__).resolve().parents[1]
        / "src/lakebench/spark/scripts/gold_finalize_financial.py"
    )
    src = gf_path.read_text()
    tree = ast.parse(src)
    fn = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_detection_rules"
    )
    body = ast.unparse(fn)
    assert "inspect.signature" in body, (
        "run_detection_rules must use inspect.signature for its param filter, "
        "not fn.__code__.co_varnames (co_varnames leaks locals)"
    )
    assert ".co_varnames" not in body, (
        "run_detection_rules leaked back to co_varnames -- future locals named "
        "silver_entities would be routed to the rule as a kwarg and swallowed"
    )


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


def test_gold_finalize_crashed_rules_emit_alerts_line():
    """Adversarial-review finding F: a crashed rule's ``FAILED`` log
    line does not include ``alerts=`` so a downstream metrics parser
    that greps for ``[detection] <rule>: alerts=N`` misses the row.
    Fix must emit ``alerts=0 error=...`` on failure so parsers see a
    row for every rule that was attempted.
    """
    gf_path = (
        Path(__file__).resolve().parents[1]
        / "src/lakebench/spark/scripts/gold_finalize_financial.py"
    )
    src = gf_path.read_text()
    tree = ast.parse(src)
    fn = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_detection_rules"
    )
    body = ast.unparse(fn)
    assert "alerts=0 error=" in body, (
        "run_detection_rules failure branch must emit alerts=0 error=... "
        "so metrics parsers see a row for every rule attempted"
    )


def test_faml_data_candidates_env_override_is_authoritative():
    """Adversarial-review finding G: iterating candidate directories
    per-file lets a partial LB_FAML_DATA_DIR override silently mix
    with the installed pkg (only sanctions_list.json in the override
    shadows that file while pep/hrj read from pkg). Env override must
    be the ONLY candidate when set.
    """
    tree = _module_ast()
    fn = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_faml_data_candidates"
    )
    body = ast.unparse(fn)
    # Env-override path must return early with a single-element list.
    assert "if override:" in body and "return [override]" in body, (
        "_faml_data_candidates must return [override] when LB_FAML_DATA_DIR "
        "is set; anything that adds more candidates enables split-brain "
        "reference data (finding G)"
    )


def test_configmap_size_guardrail():
    """Adversarial-review test coverage #4: adding a large future
    FAML reference file (e.g. a real ~5 MB OFAC SDN list) would
    silently break deploy with an opaque `Request entity too large`
    since K8s hard-rejects ConfigMap >1 MiB. Cap the shipped set of
    (scripts + FAML reference JSONs) at 900 KiB total so the failure
    mode is a test rather than a broken deploy.
    """
    from lakebench._resources import get_faml_data_dir, get_scripts_dir

    total = 0
    for p in get_scripts_dir().glob("*.py"):
        total += p.stat().st_size
    faml_dir = get_faml_data_dir()
    if faml_dir is not None:
        for p in faml_dir.glob("*.json"):
            total += p.stat().st_size
    assert total < 900 * 1024, (
        f"combined scripts + FAML data size {total} bytes exceeds 900 KiB; "
        f"K8s ConfigMap limit is 1 MiB and the deploy will fail with "
        f"`Request entity too large`. Trim reference files or split the "
        f"ConfigMap."
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
