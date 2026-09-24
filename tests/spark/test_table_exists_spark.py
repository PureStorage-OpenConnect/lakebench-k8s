"""Executed tests for common.table_exists (2026-09-24 audit).

The Delta scripts used DeltaTable.isDeltaTable(spark, "catalog.schema.table"),
whose argument is a file path, so it was always False: Delta continuous
silver never started, bronze was overwritten on driver restart, and batch
reruns appended a second copy of silver.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"))


@pytest.fixture(scope="module")
def spark():
    import os

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def test_existing_and_missing_tables(spark):
    from common import table_exists

    spark.range(3).createOrReplaceTempView("present_t")
    assert table_exists(spark, "present_t") is True
    assert table_exists(spark, "definitely_missing_t") is False
    assert table_exists(spark, "no_such_schema.missing_t") is False


def test_non_not_found_errors_are_raised(spark):
    """A failure that is not "not found" must not read as missing: callers
    create-and-overwrite on False."""
    from common import table_exists

    class Boom:
        def table(self, _name):
            raise RuntimeError("catalog unreachable: connection reset")

    with pytest.raises(RuntimeError):
        table_exists(Boom(), "x.y.z")
