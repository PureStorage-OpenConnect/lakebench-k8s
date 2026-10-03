"""Executed: the datagen per-node marker (<prefix>/_corpus/*.json) is never
read as data. Spark's file index skips directories starting with `_` when it
lists a path, so a parquet read of the datagen prefix, a part-* glob, a
recursive read and a file-source stream over it see the parquet rows only.
Skipped without pyspark."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("pyspark")


def _prefix(tmp_path, spark):
    # One flat parquet file named as the generator names them, beside the
    # marker directory, as on the bucket.
    import shutil

    staged = tmp_path / "staged"
    spark.createDataFrame([(i, f"r{i}") for i in range(5)], "id long, v string").coalesce(
        1
    ).write.parquet(str(staged))
    root = tmp_path / "bronze" / "customer" / "interactions"
    root.mkdir(parents=True)
    (part,) = staged.glob("part-*.parquet")
    shutil.copy(part, root / "part-000000.parquet")
    marker = root / "_corpus" / "c000-node-0000.json"
    marker.parent.mkdir(parents=True)
    marker.write_text(json.dumps({"format": 1, "files_written": 1, "corpus_args": {}}))
    return root


def test_marker_skipped_by_glob(spark_session, tmp_path):
    # The readers' shapes: the prefix directory (bronze-verify, bronze-ingest
    # schema inference, single-cycle silver), a part-* name glob
    # (multi-cycle silver), and a recursive listing. (A bare `<prefix>/*`
    # glob would hand Spark the _corpus directory as a root path; no reader
    # uses one.)
    root = _prefix(tmp_path, spark_session)
    assert spark_session.read.parquet(str(root)).count() == 5
    assert spark_session.read.parquet(str(root / "part-*")).count() == 5
    df = spark_session.read.option("recursiveFileLookup", "true").parquet(str(root))
    assert df.count() == 5
    assert all("_corpus" not in f for f in df.inputFiles())


def test_marker_skipped_by_a_file_stream(spark_session, tmp_path):
    root = _prefix(tmp_path, spark_session)
    schema = "id long, v string"
    q = (
        spark_session.readStream.schema(schema)
        .option("recursiveFileLookup", "true")
        .parquet(str(root))
        .writeStream.format("memory")
        .queryName("marker_stream")
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination(120)
    assert spark_session.sql("select count(*) from marker_stream").collect()[0][0] == 5
