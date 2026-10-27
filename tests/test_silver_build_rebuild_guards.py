"""Iceberg c360 silver-build: a rebuild at a later cycle refuses when it
cannot tag rows by cycle.

``silver_build.py`` runs a Spark job at import, so the function is lifted
out of its source and run with stand-ins. The executed scenarios are
in tests/spark/test_silver_build_rebuild_spark.py.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

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


@pytest.mark.parametrize(
    ("files", "cycle", "match"),
    [
        # a later-cycle rebuild with none of its own files: every row would be
        # tagged 0 and a retry would then append the cycle twice
        (
            ["part-000000.parquet", "part-c001-000000.parquet"],
            2,
            "no bronze file is named part-c002-",
        ),
        # cycle 1 is not satisfied by cycle 10's files
        (["part-000000.parquet", "part-c010-000000.parquet"], 1, ""),
    ],
)
def test_rebuild_refuses_when_it_cannot_tag_its_cycle(files, cycle, match):
    df = _Df([BASE + f for f in files])
    with pytest.raises(_Abort, match=match or None):
        _tag_batch()(df, cycle, False)
