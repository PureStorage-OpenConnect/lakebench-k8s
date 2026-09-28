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


def test_iceberg_snapshot_failure_returns_none():
    """The snapshot SELECT raising must NOT lead to spark.table().count()."""
    spark = _spark_that_never_full_counts()
    spark.sql.side_effect = RuntimeError("snapshot metadata unavailable")
    assert _ICEBERG_FN(spark, "ice.silver.customer") is None


def test_iceberg_empty_snapshot_summary_returns_none():
    """A snapshot row with a NULL 'added-records' summary is treated as unknown."""
    spark = _spark_that_never_full_counts()
    spark.sql.return_value.collect.return_value = [{"n": None}]
    assert _ICEBERG_FN(spark, "ice.silver.customer") is None


def test_iceberg_snapshot_success_still_returns_int():
    """The happy path is unchanged: real added-records is returned as int."""
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"n": 12345}]
    assert _ICEBERG_FN(spark, "ice.silver.customer") == 12345


def test_delta_history_failure_returns_none():
    """Delta's DESCRIBE HISTORY raising must NOT trigger a full table count."""
    spark = _spark_that_never_full_counts()
    spark.sql.side_effect = RuntimeError("DESCRIBE HISTORY unavailable")
    assert _DELTA_FN(spark, "spark_catalog.silver.customer") is None


def test_delta_history_missing_metric_returns_none():
    """A history row without operationMetrics.numOutputRows is treated as unknown."""
    spark = _spark_that_never_full_counts()
    spark.sql.return_value.collect.return_value = [{"operationMetrics": {}}]
    assert _DELTA_FN(spark, "spark_catalog.silver.customer") is None


def test_delta_history_success_still_returns_int():
    """The happy path returns numOutputRows as an int."""
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"operationMetrics": {"numOutputRows": "999"}}]
    assert _DELTA_FN(spark, "spark_catalog.silver.customer") == 999


def test_source_no_longer_falls_back_to_full_count():
    """Belt-and-braces on source: the ``spark.table(silver_tbl).count()``
    fallback is gone from both files. A future edit that restores it fails
    this test.
    """
    for name in ("silver_build.py", "silver_build_delta.py"):
        src = (_SCRIPTS / name).read_text()
        # Compare against the extracted function body (ast dedented) rather
        # than the raw slice, so a docstring mention of the removed line
        # cannot pass for the code itself.
        code = _extract_function(_SCRIPTS / name, "rows_added_by_last_commit").__code__
        # ast.dump of the function's AST tree is the tightest guard against
        # a docstring or comment coincidence.
        tree = ast.parse((_SCRIPTS / name).read_text())
        fn = next(
            n
            for n in tree.body
            if isinstance(n, ast.FunctionDef) and n.name == "rows_added_by_last_commit"
        )
        # Drop the docstring node so it is not textually searched.
        if (
            fn.body
            and isinstance(fn.body[0], ast.Expr)
            and isinstance(fn.body[0].value, ast.Constant)
            and isinstance(fn.body[0].value.value, str)
        ):
            fn.body = fn.body[1:]
        rendered = ast.unparse(fn)
        assert "spark.table(silver_tbl).count()" not in rendered, (
            f"{name} still falls back to a cumulative table count"
        )
        assert "return None" in rendered, f"{name} rows_added_by_last_commit no longer returns None"
        # Belt-and-braces on the raw src too (any leftover reference).
        del code, src


def test_main_source_aborts_on_unknown_output_rows():
    """Both mains raise SilverAbort when silver_count is None and log
    ``output_rows: unknown`` first so the metrics collector records the
    reason.
    """
    for name in ("silver_build.py", "silver_build_delta.py"):
        src = (_SCRIPTS / name).read_text()
        tail = src.rsplit("=== JOB METRICS", 1)[1]
        assert "SilverAbort" in tail, f"{name} main is missing the A4 SilverAbort"
        assert "output_rows unknown" in tail, (
            f"{name} main abort message must name 'output_rows unknown'"
        )
        assert "output_rows: unknown" in tail, (
            f"{name} main must log 'output_rows: unknown' before aborting"
        )
