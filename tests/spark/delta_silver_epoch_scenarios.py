"""Rebuild-epoch scenarios for the Delta c360 silver build, run in fresh JVMs.

Not collected by pytest (no ``test_`` prefix).
``test_silver_build_delta_epoch_spark`` runs it with ``spark_subprocess``, which
puts the Spark scripts and tests/spark on its PYTHONPATH.

Every silver job is the real ``silver_build_delta.py``, started as its own
process the way the Spark Operator starts one driver per cycle. The jobs
share a Derby-backed Hive metastore and a warehouse under the work directory,
so a catalog entry and a Delta log outlive a job exactly as they outlive a
driver on the cluster. Deleting the Derby directory stands in for a destroy
that drops the metastore and keeps the bucket.

A run is cycles 0..n-1 of a multi-cycle batch: cycle 0 is the full build,
later cycles append with ``LB_SILVER_INCREMENTAL=true``. Each cycle's
bronze rows carry event ids no other cycle or run uses, so the scenario can
report which cycles silver holds, not only how many rows.

Usage: python delta_silver_epoch_scenarios.py <jars> <work_dir>  (jars: comma-separated)
Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from c360_stream_scenarios import bronze_df

_SCRIPTS = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"

TABLE = "spark_catalog.silver.customer_interactions_enriched"
ROWS_PER_CYCLE = 10  # bronze rows; every 10th is filtered, so 9 reach silver
SILVER_PER_CYCLE = 9


ICEBERG_CATALOG = "ice"


def _submit_args(jars, work, fmt="delta"):
    if fmt == "iceberg":
        confs = {
            "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
            f"spark.sql.catalog.{ICEBERG_CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{ICEBERG_CATALOG}.type": "hadoop",
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse": f"file://{work}/ice-wh",
        }
        parts = ["--master", "local[2]", "--jars", jars]
        for k, v in confs.items():
            parts += ["--conf", f"{k}={v}"]
        return " ".join(parts) + " pyspark-shell"
    confs = {
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.catalogImplementation": "hive",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.warehouse.dir": f"file://{work}/warehouse",
        "spark.hadoop.javax.jdo.option.ConnectionURL": (
            f"jdbc:derby:;databaseName={work}/metastore_db;create=true"
        ),
        "spark.driver.extraJavaOptions": f"-Dderby.system.home={work}",
        # A checkpoint every 2 commits, so the transaction ids are also read
        # back from checkpoints, as on a long-lived table (default 10).
        "spark.databricks.delta.properties.defaults.checkpointInterval": "2",
    }
    parts = ["--master", "local[2]", "--jars", jars]
    for k, v in confs.items():
        parts += ["--conf", f"{k}={v}"]
    return " ".join(parts) + " pyspark-shell"


def stage_bronze(spark, work, run, cycle):
    """Write one cycle's bronze file under the name datagen_rs gives it."""
    base = Path(work) / "bronze" / "customer" / "interactions"
    base.mkdir(parents=True, exist_ok=True)
    name = f"part-{run:06d}.parquet" if cycle == 0 else f"part-c{cycle:03d}-{run:06d}.parquet"
    tmp = Path(work) / "bronze-tmp"
    start = 100_000 * (run + 1) + 1_000 * cycle
    bronze_df(spark, ROWS_PER_CYCLE, start=start).coalesce(1).write.mode("overwrite").parquet(
        str(tmp)
    )
    (part,) = tmp.glob("part-*.parquet")
    shutil.move(str(part), str(base / name))
    shutil.rmtree(tmp)


def silver_job(jars, work, cycle, epoch, force=False, strategy="simple", log=None, fmt="delta"):
    """Run silver_build_delta.py (or silver_build.py for Iceberg) as one
    driver; return its exit code."""
    ckpts_before = checkpoints(work)
    env = dict(os.environ)
    env.update(
        {
            "PYSPARK_SUBMIT_ARGS": _submit_args(jars, work, fmt),
            "LB_ICEBERG_CATALOG": ICEBERG_CATALOG if fmt == "iceberg" else "spark_catalog",
            "LB_BRONZE_URI": f"file://{work}/bronze/",
            "LB_SILVER_URI": f"file://{work}/silver/",
            "LB_DATA_CLOCK": "2031-01-01",
            "LB_SILVER_SIZE_GB": "0.001",
            "LB_SILVER_STRATEGY": strategy,
            "LB_BRONZE_CYCLE": str(cycle),
            "LB_REBUILD_EPOCH": str(epoch),
            "LB_FORCE_REBUILD": "1" if force else "0",
            "LB_SILVER_INCREMENTAL": "true" if cycle > 0 else "false",
        }
    )
    proc = subprocess.run(
        [
            sys.executable,
            str(_SCRIPTS / ("silver_build.py" if fmt == "iceberg" else "silver_build_delta.py")),
        ],
        cwd=work,
        env=env,
        capture_output=True,
        text=True,
        timeout=600,
    )
    if log is not None:
        log.append(
            {
                "checkpoints_before": ckpts_before,
                "cycle": cycle,
                "epoch": epoch,
                "force": force,
                "rc": proc.returncode,
                "tail": (proc.stdout + proc.stderr)[-3000:],
                # silver_build.py's drop decision, wherever it fell in the output.
                "full_rebuild": [ln for ln in proc.stdout.splitlines() if "Full rebuild:" in ln],
            }
        )
    return proc.returncode


def _table_dir(work, fmt="delta"):
    if fmt == "iceberg":
        return f"{work}/ice-wh/silver/customer_interactions_enriched"
    return f"{work}/warehouse/silver.db/customer_interactions_enriched"


def checkpoints(work):
    return len(glob.glob(f"{_table_dir(work)}/_delta_log/*.checkpoint*.parquet"))


def silver_cycles(spark, work, fmt="delta"):
    """Which (run, cycle) pairs silver holds, with the row count of each."""
    path = _table_dir(work, fmt)
    if not os.path.isdir(path):
        return {}  # no table at all (a job failed before writing one)
    if fmt == "iceberg":
        rows = spark.read.format("iceberg").load(path).select("id").collect()
    else:
        rows = spark.read.format("delta").load(path).select("id").collect()
    held = {}
    for r in rows:
        key = f"{r['id'] // 100_000 - 1}:{(r['id'] % 100_000) // 1_000}"
        held[key] = held.get(key, 0) + 1
    return dict(sorted(held.items()))


def run(
    spark,
    jars,
    work,
    run_no,
    epochs,
    force_cycle0=False,
    strategy="simple",
    log=None,
    fmt="delta",
):
    """Cycles 0..n-1 of one `lakebench run`; ``epochs`` is LB_REBUILD_EPOCH per cycle."""
    rcs = []
    for cycle, epoch in enumerate(epochs):
        stage_bronze(spark, work, run_no, cycle)
        rcs.append(
            silver_job(
                jars,
                work,
                cycle,
                epoch,
                force=force_cycle0 and cycle == 0,
                strategy=strategy,
                log=log,
                fmt=fmt,
            )
        )
        if rcs[-1] != 0:
            break
    return rcs


def _fresh(root, name):
    work = os.path.join(root, name)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    return work


def _clear_bronze(work):
    shutil.rmtree(Path(work) / "bronze", ignore_errors=True)


def main():
    from pyspark.sql import SparkSession

    jars, root = sys.argv[1], sys.argv[2]
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", f"file://{root}/reader-wh")
        .getOrCreate()
    )
    out = {}

    # 1. The epoch counter goes back to 0 while the catalog entry and the
    #    log survive (silver-state lost, or job.py's read falling back to 0),
    #    and the next run is a --force-rebuild. Run 0 committed epoch 0 at
    #    cycle 1, so run 1's cycle 1 reuses that (appId, version) key.
    work = _fresh(root, "epoch-reset")
    log = []
    first = run(spark, jars, work, 0, [0, 0], log=log)
    _clear_bronze(work)
    second = run(spark, jars, work, 1, [0, 0], force_cycle0=True, log=log)
    out["epoch_reset"] = {
        "rcs": [first, second],
        "held": silver_cycles(spark, work),
        "log": log,
    }

    # 2. One later cycle reads a stale epoch (the ConfigMap read in job.py
    #    falls back to 0) while cycles 0 and 2 read the bumped epoch 1.
    #    STREAMING strategy, so both write functions are covered.
    work = _fresh(root, "stale-cycle")
    log = []
    first = run(spark, jars, work, 0, [0, 0, 0], strategy="streaming", log=log)
    _clear_bronze(work)
    second = run(spark, jars, work, 1, [1, 0, 1], force_cycle0=True, strategy="streaming", log=log)
    held_after_run = silver_cycles(spark, work)
    # An operator retry of the last cycle (same env): Delta must skip it.
    retry_rc = silver_job(jars, work, 2, 1, strategy="streaming", log=log)
    out["stale_cycle"] = {
        "rcs": [first, second],
        "held": held_after_run,
        "retry_rc": retry_rc,
        "held_after_retry": silver_cycles(spark, work),
        "log": log,
    }

    # 3. The BUGS-row staging: the catalog entry goes (metastore dropped,
    #    bucket kept) and the next deployment starts again at epoch 0.
    work = _fresh(root, "catalog-lost")
    log = []
    first = run(spark, jars, work, 0, [0, 0], log=log)
    shutil.rmtree(os.path.join(work, "metastore_db"))
    _clear_bronze(work)
    second = run(spark, jars, work, 1, [0, 0], log=log)
    refused = "already holds a Delta log" in log[-1]["tail"]
    held_after_refusal = silver_cycles(spark, work)
    # The refusal's remedy: delete the table directory. Cycle 1 then finds
    # no table and builds it from the whole prefix (cycles 0 and 1 of run
    # 1), a create carrying the key (0, 1); its operator retry is skipped.
    shutil.rmtree(_table_dir(work))
    stage_bronze(spark, work, 1, 1)
    rebuild_rc = silver_job(jars, work, 1, 0, log=log)
    held_after_rebuild = silver_cycles(spark, work)
    retry_rc = silver_job(jars, work, 1, 0, log=log)
    out["catalog_lost"] = {
        "rcs": [first, second],
        "refused_orphan_log": refused,
        "held": held_after_refusal,
        "rebuild_rc": rebuild_rc,
        "held_after_rebuild": held_after_rebuild,
        "retry_rc": retry_rc,
        "held_after_retry": silver_cycles(spark, work),
        "log": log,
    }

    spark.stop()
    print(json.dumps(out))


if __name__ == "__main__":
    main()
