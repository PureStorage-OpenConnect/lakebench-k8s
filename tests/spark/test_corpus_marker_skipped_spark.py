"""Executed: no bronze reader sees the corpus markers (CD-18).

The corpus series marker ``series.json`` (and, from CD-7, the generator's
per-node markers) live in ``<datagen prefix>/_corpus/``, beside the part
files every Customer 360 stage reads. Spark's file index skips a path whose
name starts with ``_`` (measured on 4.0.1: it also leaves out the files of a
subdirectory that is not a ``key=value`` partition), so the single-cycle read
of the whole prefix, the cycle globs (``common.c360_bronze_path`` and
``c360_bronze_run_path``) and the continuous streaming source read the same
rows with the marker as without it.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

pytest.importorskip("pyspark")


def _common():
    path = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts/common.py"
    spec = importlib.util.spec_from_file_location("lb_common_marker_test", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def bronze(spark_session, tmp_path_factory):
    """customer/interactions/ with a cycle-0 file, a cycle-1 file and the
    corpus markers directory (a series marker and a node marker)."""
    root = tmp_path_factory.mktemp("bronze")
    prefix = root / "customer" / "interactions"
    prefix.mkdir(parents=True)
    for name, ids in (("part-000000.parquet", range(5)), ("part-c001-000000.parquet", range(5, 8))):
        df = spark_session.createDataFrame([(i, f"c{i}") for i in ids], "id long, customer string")
        out = root / f"_stage_{name}"
        df.coalesce(1).write.parquet(str(out))
        (next(out.glob("part-*.parquet"))).rename(prefix / name)
    markers = prefix / "_corpus"
    markers.mkdir()
    (markers / "series.json").write_text(json.dumps({"format": 1, "cycles_complete": [0, 1]}))
    (markers / "c000-node-0000.json").write_text(json.dumps({"format": 1}))
    return root


def test_whole_prefix_read_skips_the_markers(spark_session, bronze):
    df = spark_session.read.parquet(str(bronze / "customer" / "interactions") + "/")
    assert df.count() == 8
    assert sorted(df.columns) == ["customer", "id"]


@pytest.mark.parametrize(("cycle", "rows"), [("0", 5), ("1", 8)])
def test_cycle_globs_skip_the_markers(spark_session, bronze, monkeypatch, cycle, rows):
    common = _common()
    monkeypatch.setenv("LB_BRONZE_CYCLE", cycle)
    uri = f"file://{bronze}/"
    assert spark_session.read.parquet(common.c360_bronze_run_path(uri)).count() == rows
    silver = common.c360_bronze_path(uri, appending=cycle != "0")
    assert spark_session.read.parquet(silver).count() == (5 if cycle == "0" else 3)


def test_streaming_source_skips_the_markers(spark_session, bronze, tmp_path):
    stream = (
        spark_session.readStream.schema("id long, customer string")
        .parquet(str(bronze / "customer" / "interactions") + "/")
        .writeStream.format("memory")
        .queryName("marker_skip")
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .trigger(availableNow=True)
        .start()
    )
    stream.awaitTermination(120)
    assert spark_session.sql("SELECT count(*) FROM marker_skip").first()[0] == 8
