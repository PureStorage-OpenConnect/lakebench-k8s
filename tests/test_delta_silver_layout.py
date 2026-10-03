"""Delta silver file layout (2026-09-27 sweep).

hive-delta-spark-thrift could not finish Q3/Q6 inside 300 s at scale 1:
silver was written with partitionBy(interaction_date) and no clustering, so
every write task put a file into almost every day (tasks x 366 files, about
984 scan tasks on the 2-core Thrift server). Iceberg silver clusters with
write.distribution-mode=hash. These tests pin the Delta equivalent.
"""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


@pytest.fixture()
def common(load_script):
    return load_script("common")


class TestClusterByPartition:
    def test_rebalances_by_the_partition_column(self, common):
        spark, df = MagicMock(), MagicMock()
        out = common.cluster_by_partition(spark, df, ["interaction_date"])
        view = df.createOrReplaceTempView.call_args.args[0]
        sql = spark.sql.call_args.args[0]
        assert "/*+ REBALANCE(interaction_date) */" in sql
        assert sql.endswith(f"FROM {view}")
        assert out is spark.sql.return_value

    @pytest.mark.parametrize("mode", ["none", "NONE", " none "])
    def test_none_is_the_escape_hatch(self, common, mode):
        spark, df = MagicMock(), MagicMock()
        assert common.cluster_by_partition(spark, df, ["interaction_date"], mode) is df
        spark.sql.assert_not_called()

    def test_unpartitioned_write_is_untouched(self, common):
        spark, df = MagicMock(), MagicMock()
        assert common.cluster_by_partition(spark, df, None) is df
        assert common.cluster_by_partition(spark, df, []) is df
        spark.sql.assert_not_called()

    def test_views_do_not_collide(self, common):
        spark = MagicMock()
        a, b = MagicMock(), MagicMock()
        common.cluster_by_partition(spark, a, ["d"])
        common.cluster_by_partition(spark, b, ["d"])
        assert (
            a.createOrReplaceTempView.call_args.args[0]
            != b.createOrReplaceTempView.call_args.args[0]
        )

    def test_refuses_an_expression(self, common):
        with pytest.raises(ValueError):
            common.cluster_by_partition(MagicMock(), MagicMock(), ["d) */ * FROM x --"])


class TestFilesAddedByLastCommit:
    def test_reads_num_files(self, common):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = [{"operationMetrics": {"numFiles": "366"}}]
        assert common.files_added_by_last_commit(spark, "spark_catalog.silver.t") == 366

    def test_unknown_is_none(self, common):
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("no history")
        assert common.files_added_by_last_commit(spark, "t") is None
        spark.sql.side_effect = None
        spark.sql.return_value.collect.return_value = [{"operationMetrics": {}}]
        assert common.files_added_by_last_commit(spark, "t") is None


def _func(tree: ast.Module, name: str) -> ast.FunctionDef:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"{name} not found")


@pytest.mark.parametrize("fn", ["silver_simple", "silver_streaming"])
def test_every_delta_silver_write_is_clustered(fn):
    """Each write_delta_table call in the batch silver build writes a frame
    that went through cluster_silver first (append and overwrite paths)."""
    tree = ast.parse((SCRIPTS / "silver_build_delta.py").read_text())
    body = _func(tree, fn)
    clustered_at = [
        n.lineno
        for n in ast.walk(body)
        if isinstance(n, ast.Assign)
        and isinstance(n.value, ast.Call)
        and getattr(n.value.func, "id", None) == "cluster_silver"
        and any(getattr(t, "id", None) == "silver_df" for t in n.targets)
    ]
    writes = [
        n
        for n in ast.walk(body)
        if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "write_delta_table"
    ]
    assert clustered_at and writes
    for w in writes:
        assert getattr(w.args[1], "id", None) == "silver_df"
        assert min(clustered_at) < w.lineno


def test_cluster_silver_honours_the_iceberg_override():
    tree = ast.parse((SCRIPTS / "silver_build_delta.py").read_text())
    src = ast.unparse(_func(tree, "cluster_silver"))
    assert "spark.lb.silver.distribution_mode" in src
    assert "cluster_by_partition" in src and "interaction_date" in src
