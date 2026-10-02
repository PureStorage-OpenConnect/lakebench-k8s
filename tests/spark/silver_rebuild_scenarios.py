"""Rebuild scenarios for the c360 batch silver builds, run in fresh JVMs.

Not collected by pytest (no ``test_`` prefix).
``test_silver_build_rebuild_spark`` runs it with ``spark_subprocess``.

Each silver job is the real ``silver_build.py`` (Iceberg, a hadoop catalog
under the work directory) or ``silver_build_delta.py`` (Delta, a Derby Hive
metastore), one process per job as on the cluster; the helpers are
``delta_silver_epoch_scenarios``'s.

- A later cycle that finds no silver table rebuilds it. It must read only
  this run's files (cycle 0 and cycles 1..k), not every file under the
  prefix, and an operator retry of it must leave the rebuild in place.
- A populated table whose row probe fails is not rebuilt without
  --force-rebuild.

Usage: python silver_rebuild_scenarios.py <jars> <work_dir>  (jars: comma-separated)
Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import sys
from pathlib import Path

from delta_silver_epoch_scenarios import (
    _clear_bronze,
    _fresh,
    _table_dir,
    run,
    silver_cycles,
    silver_job,
    stage_bronze,
)

# Run 9's cycle 5 stands for a file an earlier, longer run left in the bucket.
LEFTOVER = (9, 5)


def _drop_data_files(table_dir):
    """Delete every data file of a table and keep its metadata or log, so a
    read of the table fails while the catalog still lists it."""
    n = 0
    for f in glob.glob(f"{table_dir}/**/*.parquet", recursive=True):
        if "/_delta_log/" in f or "/metadata/" in f:
            continue
        os.remove(f)
        n += 1
    return n


def later_cycle_rebuild(spark, jars, root, fmt, strategy):
    work = _fresh(root, f"rebuild-{fmt}-{strategy}")
    log = []
    first = run(spark, jars, work, 0, [0, 0], strategy=strategy, log=log, fmt=fmt)
    # The table goes between cycles; the bucket keeps every bronze file.
    if fmt == "delta":
        shutil.rmtree(os.path.join(work, "metastore_db"))
    shutil.rmtree(_table_dir(work, fmt))
    stage_bronze(spark, work, *LEFTOVER)
    stage_bronze(spark, work, 0, 2)
    rebuild_rc = silver_job(jars, work, 2, 0, strategy=strategy, log=log, fmt=fmt)
    held_after_rebuild = silver_cycles(spark, work, fmt)
    retry_rc = silver_job(jars, work, 2, 0, strategy=strategy, log=log, fmt=fmt)
    held_after_retry = silver_cycles(spark, work, fmt)
    # The next cycle appends after the rebuild as after any cycle.
    stage_bronze(spark, work, 0, 3)
    next_rc = silver_job(jars, work, 3, 0, strategy=strategy, log=log, fmt=fmt)
    return {
        "rcs": first,
        "rebuild_rc": rebuild_rc,
        "held_after_rebuild": held_after_rebuild,
        "retry_rc": retry_rc,
        "held_after_retry": held_after_retry,
        "next_rc": next_rc,
        "held_after_next": silver_cycles(spark, work, fmt),
        "log": log,
    }


def failed_probe(spark, jars, root, fmt):
    work = _fresh(root, f"probe-{fmt}")
    log = []
    first = run(spark, jars, work, 0, [0], log=log, fmt=fmt)
    dropped = _drop_data_files(_table_dir(work, fmt))
    _clear_bronze(work)
    stage_bronze(spark, work, 1, 0)
    rc = silver_job(jars, work, 0, 0, log=log, fmt=fmt)
    refused = "cannot tell whether" in log[-1]["tail"]
    # --force-rebuild still rebuilds it.
    forced_rc = silver_job(jars, work, 0, 0, force=True, log=log, fmt=fmt)
    return {
        "rcs": first,
        "dropped": dropped,
        "rc": rc,
        "refused": refused,
        "forced_rc": forced_rc,
        "held_after_forced": silver_cycles(spark, work, fmt),
        "log": log,
    }


def unreadable_iceberg_metadata(spark, jars, root):
    """The table's current metadata file cannot be read: the existence check
    itself fails, which must not count as "no table"."""
    work = _fresh(root, "metadata-iceberg")
    log = []
    first = run(spark, jars, work, 0, [0], log=log, fmt="iceberg")
    meta = sorted(glob.glob(f"{_table_dir(work, 'iceberg')}/metadata/*.metadata.json"))
    Path(meta[-1]).write_text("{")
    _clear_bronze(work)
    stage_bronze(spark, work, 1, 0)
    rc = silver_job(jars, work, 0, 0, log=log, fmt="iceberg")
    after = sorted(glob.glob(f"{_table_dir(work, 'iceberg')}/metadata/*.metadata.json"))
    return {
        "rcs": first,
        "rc": rc,
        "metadata_before": len(meta),
        "metadata_after": len(after),
        "log": log,
    }


def cleaned_delta(spark, jars, root):
    """`lakebench clean silver` empties the bucket and keeps the catalog
    entry; the next run builds silver afresh, without --force-rebuild."""
    work = _fresh(root, "cleaned-delta")
    log = []
    first = run(spark, jars, work, 0, [0, 0], log=log, fmt="delta")
    shutil.rmtree(_table_dir(work, "delta"))
    _clear_bronze(work)
    second = run(spark, jars, work, 1, [0, 0], log=log, fmt="delta")
    return {"rcs": [first, second], "held": silver_cycles(spark, work, "delta"), "log": log}


def logless_delta_with_files(spark, jars, root):
    """The Delta log is gone but data files remain: refused, files kept."""
    work = _fresh(root, "logless-delta")
    log = []
    first = run(spark, jars, work, 0, [0], log=log, fmt="delta")
    table = _table_dir(work, "delta")
    shutil.rmtree(f"{table}/_delta_log")
    files = sorted(glob.glob(f"{table}/**/*.parquet", recursive=True))
    _clear_bronze(work)
    stage_bronze(spark, work, 1, 0)
    rc = silver_job(jars, work, 0, 0, log=log, fmt="delta")
    after = sorted(glob.glob(f"{table}/**/*.parquet", recursive=True))
    return {
        "rcs": first,
        "rc": rc,
        "refused": "still holds files" in log[-1]["tail"],
        "files_kept": bool(files) and after == files,
        "log": log,
    }


def main():
    from pyspark.sql import SparkSession

    jars, root = sys.argv[1], sys.argv[2]
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", f"file://{root}/reader-wh")
        .getOrCreate()
    )
    out = {
        "iceberg_simple": later_cycle_rebuild(spark, jars, root, "iceberg", "simple"),
        "iceberg_streaming": later_cycle_rebuild(spark, jars, root, "iceberg", "streaming"),
        "delta_simple": later_cycle_rebuild(spark, jars, root, "delta", "simple"),
        "probe_iceberg": failed_probe(spark, jars, root, "iceberg"),
        "probe_delta": failed_probe(spark, jars, root, "delta"),
        "metadata_iceberg": unreadable_iceberg_metadata(spark, jars, root),
        "cleaned_delta": cleaned_delta(spark, jars, root),
        "logless_delta": logless_delta_with_files(spark, jars, root),
    }
    spark.stop()
    print(json.dumps(out))


if __name__ == "__main__":
    main()
