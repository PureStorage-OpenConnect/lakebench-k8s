"""Iceberg c360 silver-build: the table-existence check fails closed, and a
rebuild at a later cycle refuses when it cannot tag rows by cycle.

``silver_build.py`` runs a Spark job at import, so the two functions are
lifted out of its source and run with stand-ins. The executed scenarios are
in tests/spark/test_silver_build_rebuild_spark.py.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from types import SimpleNamespace

import pytest

_SCRIPT = Path(__file__).resolve().parents[1] / "src/lakebench/spark/scripts/silver_build.py"


class _Abort(RuntimeError):
    pass


def _lift(names, **ns):
    tree = ast.parse(_SCRIPT.read_text())
    keep = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in names]
    assert {n.name for n in keep} == set(names)
    ns.setdefault("re", re)
    ns.setdefault("SilverAbort", _Abort)
    exec(compile(ast.Module(keep, []), str(_SCRIPT), "exec"), ns)  # noqa: S102
    return ns


def _spark_raising(exc):
    def table(name):
        raise exc

    return SimpleNamespace(table=table)


def test_table_exists_raises_on_an_error_that_is_not_not_found(load_script):
    """A metastore or metadata read failure is not "no table": counting it as
    missing sent a populated table down the full-rebuild path."""
    common = load_script("common")
    fn = _lift(["_table_exists"], table_exists=common.table_exists)["_table_exists"]
    with pytest.raises(RuntimeError, match="metastore timed out"):
        fn(_spark_raising(RuntimeError("metastore timed out")), "ice.silver.t")


class _Col:
    """Enough of a Column for tag_batch's expression to build."""

    def __eq__(self, other):
        return self

    def __hash__(self):
        return 0

    def cast(self, _type):
        return self

    def otherwise(self, _v):
        return self


def _tag_batch():
    c = _Col()
    return _lift(
        ["tag_batch"],
        col=lambda _n: c,
        lit=lambda _v: c,
        regexp_extract=lambda *_a: c,
        when=lambda *_a: c,
    )["tag_batch"]


class _Df:
    def __init__(self, files):
        self._files = files
        self.tagged = False

    def inputFiles(self):  # noqa: N802 -- pyspark name
        return self._files

    def withColumn(self, name, _expr):  # noqa: N802 -- pyspark name
        assert name == "_batch_id"
        self.tagged = True
        return self


BASE = "s3a://lb-bronze/customer/interactions/"


def test_later_cycle_rebuild_without_its_own_files_refuses():
    """Every row would be tagged 0; a retry would then append the cycle twice."""
    df = _Df([BASE + "part-000000.parquet", BASE + "part-c001-000000.parquet"])
    with pytest.raises(_Abort, match="no bronze file is named part-c002-"):
        _tag_batch()(df, 2, False)


def test_cycle_one_is_not_satisfied_by_cycle_ten():
    df = _Df([BASE + "part-000000.parquet", BASE + "part-c010-000000.parquet"])
    with pytest.raises(_Abort):
        _tag_batch()(df, 1, False)
