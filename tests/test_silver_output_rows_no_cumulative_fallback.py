"""A4 (silver-plan): output_rows fallback returns None, never a cumulative count.

Before A4 the ``rows_added_by_last_commit`` helper in silver_build.py and
silver_build_delta.py returned ``spark.table(silver_tbl).count()`` when the
snapshot / DESCRIBE HISTORY metric was missing. In incremental mode that is
the whole table (every cycle so far), which masked a zero-write cycle -- the
exact silent-corruption surface the LB-044 gate exists to catch. A4 changes
the fallback to return ``None`` and refuses the run rather than publishing
a misleading ``output_rows`` value.

The silver_build.py and silver_build_delta.py modules run their main pipeline
at module top level (no ``if __name__`` guard, so ``spark-submit`` runs them
directly), and import ``pyspark`` at the top. That means we cannot ``import``
them in a unit test without a Spark environment; instead we extract
``rows_added_by_last_commit`` from the source with ``ast`` and exec it in a
minimal namespace. Behaviour of the whole main is covered in tests/spark.
"""

from __future__ import annotations

import ast
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts"


def _extract_function(path: Path, name: str):
    """Extract a top-level function from a source file without importing it."""
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            module = ast.Module(body=[node], type_ignores=[])
            namespace: dict = {"log": lambda _msg: None}
            exec(compile(module, str(path), "exec"), namespace)
            return namespace[name]
    raise AssertionError(f"{name} not found in {path}")


_ICEBERG_FN = _extract_function(_SCRIPTS / "silver_build.py", "rows_added_by_last_commit")
_DELTA_FN = _extract_function(_SCRIPTS / "silver_build_delta.py", "rows_added_by_last_commit")


def _spark_that_never_full_counts():
    """A fake Spark whose .table().count() would raise, so any fallback to
    the cumulative count is caught by AssertionError rather than silently
    returning a plausible number."""
    spark = MagicMock()

    def _table(_name):
        raise AssertionError(
            "rows_added_by_last_commit fell back to spark.table().count(); A4 requires None instead"
        )

    spark.table = _table
    return spark


def _sql_raises(spark, msg):
    spark.sql.side_effect = RuntimeError(msg)


def _rows(rows):
    def setup(spark, _msg):
        spark.sql.return_value.collect.return_value = rows

    return setup


@pytest.mark.parametrize(
    ("fn", "table", "setup", "msg"),
    [
        ("iceberg", "ice.silver.customer", _sql_raises, "snapshot metadata unavailable"),
        ("iceberg", "ice.silver.customer", _rows([{"n": None}]), None),
        ("delta", "spark_catalog.silver.customer", _sql_raises, "DESCRIBE HISTORY unavailable"),
        ("delta", "spark_catalog.silver.customer", _rows([{"operationMetrics": {}}]), None),
    ],
)
def test_unknown_rows_added_is_none_never_a_full_count(fn, table, setup, msg):
    """A failed or empty snapshot/history read is unknown (None), never a
    spark.table().count() of the cumulative table."""
    spark = _spark_that_never_full_counts()
    setup(spark, msg)
    assert {"iceberg": _ICEBERG_FN, "delta": _DELTA_FN}[fn](spark, table) is None
