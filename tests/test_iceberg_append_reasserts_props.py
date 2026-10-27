"""Iceberg silver table properties honour the operator's distribution mode
and fanout on every write.

An Iceberg table's `writeTo(...).append()` does not carry TBLPROPERTIES, so a
table that lost a property would keep writing under whichever properties
currently exist. The silver build issues an ALTER TABLE ... SET TBLPROPERTIES
before each append, and both strategies' full builds create the table with
the same properties.
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


_WANTED = {
    "_SILVER_ICEBERG_STATIC_PROPS",
    "silver_iceberg_props",
    "reassert_silver_iceberg_props",
    "write_silver",
    "silver_simple",
    "silver_streaming",
}


@pytest.fixture
def silver_build(monkeypatch, load_script):
    """The table-property helpers and both strategies from silver_build.py.

    The script runs the whole pipeline when imported, so only these
    definitions are executed, against the real constants of ``common`` and
    pass-through stand-ins for the transforms.
    """
    for mod in ("pyspark", "pyspark.sql", "pyspark.sql.functions"):
        monkeypatch.setitem(sys.modules, mod, MagicMock())
    common = load_script("common")
    wanted = _WANTED
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
        "log": lambda _msg: None,
        "_COUNTED": {},
        "resolve_data_clock": lambda _df, strict: None,
        "tag_batch": lambda df, _cycle, _appending: df,
        "apply_silver_transformations_anchored": lambda df, _anchor: df,
        "batch_id_last": lambda df: df,
        "drop_if_columns_move": lambda *_a: None,
        "ensure_column": lambda *_a: None,
        "rows_added_by_last_commit": lambda _spark, _tbl: 5,
    }
    exec(compile(ast.fix_missing_locations(ast.Module(nodes, [])), str(_SILVER_BUILD), "exec"), ns)
    return ns


def _mock_spark_with_conf(dist_mode, fanout):
    spark = MagicMock()
    conf = {
        "spark.lb.silver.distribution_mode": dist_mode,
        "spark.lb.silver.fanout_enabled": fanout,
    }
    spark.conf.get.side_effect = lambda key, default=None: (
        default if conf.get(key) is None else conf[key]
    )
    return spark


@pytest.mark.parametrize(
    ("dist_mode", "fanout"),
    [
        # The operator's escape hatch must survive the cycle-1+ re-assert rather
        # than revert to hash.
        ("none", "false"),
        ("hash", "true"),
    ],
    ids=["distribution-none", "fanout"],
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


class _Writer:
    """The DataFrameWriterV2 calls a full build makes, recorded."""

    def __init__(self):
        self.props: dict[str, str] = {}
        self.created = False

    def partitionedBy(self, *_cols):  # noqa: N802 -- pyspark name
        return self

    def tableProperty(self, key, value):  # noqa: N802 -- pyspark name
        self.props[key] = value
        return self

    def createOrReplace(self):  # noqa: N802 -- pyspark name
        self.created = True


@pytest.mark.parametrize("strategy", ["silver_simple", "silver_streaming"])
@pytest.mark.parametrize(
    ("dist_mode", "fanout", "expected_mode"),
    [(None, None, "hash"), ("none", "true", "none")],
    ids=["default", "distribution-none-fanout"],
)
def test_full_build_creates_the_table_with_the_conf_properties(
    silver_build, strategy, dist_mode, fanout, expected_mode
):
    """Each strategy's full build sets the distribution mode and fanout the
    conf names, the same set a later append re-asserts."""
    spark = _mock_spark_with_conf(dist_mode, fanout)
    df = spark.read.parquet.return_value
    df.count.return_value = 5
    df.writeTo.return_value = writer = _Writer()
    args = (spark, "s3a://lb-bronze/", TABLE)
    if strategy == "silver_streaming":
        args += (200.0,)
    silver_build[strategy](*args)
    assert writer.created
    assert writer.props == dict(silver_build["silver_iceberg_props"](spark))
    assert writer.props["write.distribution-mode"] == expected_mode
    assert ("write.spark.fanout.enabled" in writer.props) == (fanout == "true")
