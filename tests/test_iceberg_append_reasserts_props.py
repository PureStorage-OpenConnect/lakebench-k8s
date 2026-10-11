"""Iceberg silver append re-asserts TBLPROPERTIES on cycles 1+.

An Iceberg table's `writeTo(...).append()` does not carry TBLPROPERTIES, so a
table that lost a property would keep writing under whichever properties
currently exist. The silver build issues an ALTER TABLE ... SET TBLPROPERTIES
before each append, honouring the operator's distribution mode and fanout.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_SILVER_BUILD = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/silver_build.py"
TABLE = "ice.silver.customer_interactions_enriched"
KEYS = {
    "write.format.default",
    "write.parquet.compression-codec",
    "write.metadata.delete-after-commit.enabled",
    "write.metadata.previous-versions-max",
    "write.target-file-size-bytes",
    "write.distribution-mode",
}


@pytest.fixture
def silver_build(monkeypatch, load_script):
    """The re-assert helper and its property tuple from silver_build.py.

    The script runs the whole pipeline when imported, so only the two
    definitions are executed, against the real constants of ``common``.
    """
    for mod in ("pyspark", "pyspark.sql", "pyspark.sql.functions"):
        monkeypatch.setitem(sys.modules, mod, MagicMock())
    common = load_script("common")
    wanted = {"_SILVER_ICEBERG_STATIC_PROPS", "reassert_silver_iceberg_props"}
    nodes = []
    for node in ast.parse(_SILVER_BUILD.read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            nodes.append(node)
        elif isinstance(node, ast.AnnAssign) and getattr(node.target, "id", None) in wanted:
            nodes.append(node)
    assert len(nodes) == len(wanted)
    ns = {
        "METADATA_DELETE_AFTER_COMMIT": common.METADATA_DELETE_AFTER_COMMIT,
        "METADATA_PREVIOUS_VERSIONS_MAX": common.METADATA_PREVIOUS_VERSIONS_MAX,
    }
    exec(compile(ast.fix_missing_locations(ast.Module(nodes, [])), str(_SILVER_BUILD), "exec"), ns)
    return ns


def _mock_spark_with_conf(dist_mode, fanout):
    spark = MagicMock()
    spark.conf.get.side_effect = lambda key, default=None: {
        "spark.lb.silver.distribution_mode": dist_mode,
        "spark.lb.silver.fanout_enabled": fanout,
    }.get(key, default)
    return spark


@pytest.mark.parametrize(
    ("dist_mode", "fanout"),
    [
        ("hash", "false"),
        # The operator's escape hatch must survive the cycle-1+ re-assert rather
        # than revert to hash.
        ("none", "false"),
        ("hash", "true"),
    ],
    ids=["defaults", "distribution-none", "fanout"],
)
def test_reassert_sets_the_full_property_set_from_the_conf(silver_build, dist_mode, fanout):
    spark = _mock_spark_with_conf(dist_mode, fanout)
    silver_build["reassert_silver_iceberg_props"](spark, TABLE)
    sql = spark.sql.call_args.args[0]
    assert sql.startswith(f"ALTER TABLE {TABLE} SET TBLPROPERTIES")
    props = dict(re.findall(r"'([^']+)' = '([^']*)'", sql))
    expected = {
        **dict(silver_build["_SILVER_ICEBERG_STATIC_PROPS"]),
        "write.distribution-mode": dist_mode,
    }
    if fanout == "true":
        expected["write.spark.fanout.enabled"] = "true"
    assert props == expected
    assert KEYS <= set(props)
