"""N incremental cycles equal one rebuild (C36-2), run in fresh JVMs.

Not collected by pytest (no ``test_`` prefix).
``test_c360_multicycle_equivalence_spark`` runs it with ``spark_subprocess``.

Three cycles of bronze with the names datagen_rs gives them
(``part-000000.parquet``, ``part-c001-000000.parquet``,
``part-c002-000000.parquet``), each a disjoint, chronological slice of one
window from the generator model (``c360_generator_model``, fixed seeds).
Each job is the real script started as its own driver, as on the cluster:

- incremental: for each cycle, ``silver_build`` then ``gold_finalize`` (or the
  ``_delta`` scripts) with ``LB_BRONZE_CYCLE`` and, from cycle 1,
  ``LB_SILVER_INCREMENTAL``/``LB_GOLD_INCREMENTAL`` as the CLI sets them;
- rebuild: one ``silver_build`` and one ``gold_finalize`` over every file
  (``LB_BRONZE_CYCLE`` unset, the single-cycle read of the whole prefix);
- changed: the rebuild again with one cycle-1 purchase's
  ``transaction_amount`` changed, which the fingerprints must tell apart.

Every job takes ``LB_DATA_CLOCK`` = the exclusive end of the series window,
as ``job.py`` gives every cycle of a multi-cycle Customer 360 run.

The fingerprint is sha256 of the sorted rows over the business columns
(``silver_processing_timestamp``, ``_batch_id`` and ``interaction_payload``
left out): order-independent and exact.

Usage: python c360_multicycle_equivalence_scenarios.py <jars> <work_dir>
Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

from c360_generator_model import generate
from c360_stream_scenarios import BRONZE_DDL
from delta_silver_epoch_scenarios import ICEBERG_CATALOG, _submit_args
from table_fingerprint import table_fingerprint

_SCRIPTS = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"

#: The series window and its three cycle windows (config.c360_run.cycle_windows
#: over 2024-01-01..2024-04-01 with three cycles).
SERIES_START = date(2024, 1, 1)
CYCLE_DAYS = (30, 30, 31)
DATA_CLOCK = "2024-04-01"
ROWS_PER_CYCLE = 3_000
CUSTOMERS = 400


def cycle_rows(cycle: int) -> list[dict]:
    start = SERIES_START + timedelta(days=sum(CYCLE_DAYS[:cycle]))
    rows = generate(ROWS_PER_CYCLE, CUSTOMERS, start, CYCLE_DAYS[cycle], seed=101 + cycle)
    # Each cycle draws its own ids, as each datagen cycle does.
    for r in rows:
        r["id"] += 1_000_000 * cycle
        r["row_id"] += 1_000_000 * cycle
        r["event_id"] = f"c{cycle}-{r['event_id']}"
        r["session_id"] = f"c{cycle}-{r['session_id']}"
    return rows


def stage_bronze(spark, work: str, cycle: int, rows: list[dict]) -> None:
    base = Path(work) / "bronze" / "customer" / "interactions"
    base.mkdir(parents=True, exist_ok=True)
    name = "part-000000.parquet" if cycle == 0 else f"part-c{cycle:03d}-000000.parquet"
    cols = [c.split()[0] for c in BRONZE_DDL.split(", ")]
    tmp = Path(work) / "bronze-tmp"
    df = spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], BRONZE_DDL)
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    (part,) = tmp.glob("part-*.parquet")
    shutil.move(str(part), str(base / name))
    shutil.rmtree(tmp)


def job(jars: str, work: str, fmt: str, script: str, cycle: int | None, log: list) -> int:
    """One driver of *script* (``silver_build`` or ``gold_finalize``);
    *cycle* None is the single-cycle rebuild of the whole prefix."""
    name = f"{script}{'_delta' if fmt == 'delta' else ''}.py"
    env = dict(os.environ)
    env.pop("LB_BRONZE_CYCLE", None)
    env.update(
        {
            "PYSPARK_SUBMIT_ARGS": _submit_args(jars, work, fmt),
            "LB_ICEBERG_CATALOG": ICEBERG_CATALOG if fmt == "iceberg" else "spark_catalog",
            "LB_BRONZE_URI": f"file://{work}/bronze/",
            "LB_SILVER_URI": f"file://{work}/silver/",
            "LB_GOLD_URI": f"file://{work}/gold/",
            "LB_DATA_CLOCK": DATA_CLOCK,
            "LB_SILVER_SIZE_GB": "0.001",
            "LB_SILVER_STRATEGY": "simple",
            "LB_REBUILD_EPOCH": "0",
            "LB_FORCE_REBUILD": "0",
            "LB_SILVER_INCREMENTAL": "true" if cycle else "false",
            "LB_GOLD_INCREMENTAL": "true" if cycle else "false",
        }
    )
    if cycle is not None:
        env["LB_BRONZE_CYCLE"] = str(cycle)
    proc = subprocess.run(
        [sys.executable, str(_SCRIPTS / name)],
        cwd=work,
        env=env,
        capture_output=True,
        text=True,
        timeout=900,
    )
    log.append(
        {
            "job": name,
            "cycle": cycle,
            "rc": proc.returncode,
            "tail": (proc.stdout + proc.stderr)[-3000:],
        }
    )
    return proc.returncode


def _table(spark, work: str, fmt: str, layer: str):
    leaf = "customer_interactions_enriched" if layer == "silver" else "customer_executive_dashboard"
    if fmt == "iceberg":
        return spark.read.format("iceberg").load(f"{work}/ice-wh/{layer}/{leaf}")
    logs = glob.glob(f"{work}/**/{leaf}/_delta_log", recursive=True)
    if len(logs) != 1:
        raise RuntimeError(f"{len(logs)} Delta tables named {leaf} under {work}")
    return spark.read.format("delta").load(os.path.dirname(logs[0]))


def fingerprint(spark, work: str, fmt: str, layer: str) -> dict:
    return table_fingerprint(_table(spark, work, fmt, layer))


def _fresh(root: str, name: str) -> str:
    work = os.path.join(root, name)
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    return work


def changed(rows: list[dict]) -> list[dict]:
    """The rows with the first purchase's amount changed by one cent."""
    out = [dict(r) for r in rows]
    hit = next(r for r in out if r["interaction_type"] == "purchase")
    hit["transaction_amount"] = round(hit["transaction_amount"] + 0.01, 2)
    return out


def scenario(spark, jars: str, root: str, fmt: str) -> dict:
    cycles = [cycle_rows(c) for c in range(len(CYCLE_DAYS))]
    log: list = []

    inc = _fresh(root, f"{fmt}-incremental")
    rcs = []
    for c, rows in enumerate(cycles):
        stage_bronze(spark, inc, c, rows)
        rcs.append(job(jars, inc, fmt, "silver_build", c, log))
        rcs.append(job(jars, inc, fmt, "gold_finalize", c, log))
        if any(rcs):
            return {"rcs": rcs, "log": log}

    def rebuild(name: str, data: list[list[dict]]) -> tuple[str, list[int]]:
        work = _fresh(root, f"{fmt}-{name}")
        for c, rows in enumerate(data):
            stage_bronze(spark, work, c, rows)
        return work, [
            job(jars, work, fmt, "silver_build", None, log),
            job(jars, work, fmt, "gold_finalize", None, log),
        ]

    full, full_rcs = rebuild("rebuild", cycles)
    alt, alt_rcs = rebuild("changed", [cycles[0], changed(cycles[1]), cycles[2]])
    out = {"rcs": rcs + full_rcs + alt_rcs, "log": log}
    if any(out["rcs"]):
        return out
    for layer in ("silver", "gold"):
        out[layer] = {
            "incremental": fingerprint(spark, inc, fmt, layer),
            "rebuild": fingerprint(spark, full, fmt, layer),
            "changed": fingerprint(spark, alt, fmt, layer),
        }
    out["log"] = [{k: e[k] for k in ("job", "cycle", "rc")} for e in log]
    return out


def main():
    from pyspark.sql import SparkSession

    jars, root = sys.argv[1], sys.argv[2]
    spark = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", f"file://{root}/reader-wh")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    out = {fmt: scenario(spark, jars, root, fmt) for fmt in ("iceberg", "delta")}
    spark.stop()
    print(json.dumps(out))


if __name__ == "__main__":
    main()
