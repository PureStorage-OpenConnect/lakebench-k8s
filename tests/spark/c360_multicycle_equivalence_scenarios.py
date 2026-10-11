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
  ``transaction_amount`` changed by one cent, which the comparisons must
  tell apart.

Every job takes ``LB_DATA_CLOCK`` = the exclusive end of the series window,
as ``job.py`` gives every cycle of a multi-cycle Customer 360 run.

Silver is compared by ``table_fingerprint`` over its business columns
(``silver_processing_timestamp`` left out): order-independent and exact.
Gold goes back to the parent as rows (``table_rows``) for
``c360_gold_compare``: its two averages of a DOUBLE amount can land one
cent apart between two correct builds, so an exact hash of gold is not
stable.

Usage: python c360_multicycle_equivalence_scenarios.py <jars> <work_dir>
Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import glob
import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

from c360_generator_model import generate
from c360_stream_scenarios import BRONZE_DDL
from delta_silver_epoch_scenarios import ICEBERG_CATALOG, fresh, submit_args, write_part
from table_fingerprint import table_fingerprint, table_rows

from lakebench.config.c360_run import cycle_windows

_SCRIPTS = Path(__file__).resolve().parents[2] / "src/lakebench/spark/scripts"

#: The series window, split as a multi-cycle run splits it
#: (``config.c360_run.cycle_windows``); the data clock is its exclusive end,
#: as ``job.py`` gives every cycle's silver job.
WINDOWS = cycle_windows(3, "2024-01-01", "2024-04-01")
DATA_CLOCK = WINDOWS[-1][1]
ROWS_PER_CYCLE = 3_000
CUSTOMERS = 400


def cycle_rows(cycle: int) -> list[dict]:
    lo, hi = (date.fromisoformat(d) for d in WINDOWS[cycle])
    rows = generate(ROWS_PER_CYCLE, CUSTOMERS, lo, (hi - lo).days, seed=101 + cycle)
    # Each cycle draws its own ids, as each datagen cycle does.
    for r in rows:
        r["id"] += 1_000_000 * cycle
        r["row_id"] += 1_000_000 * cycle
        r["event_id"] = f"c{cycle}-{r['event_id']}"
        r["session_id"] = f"c{cycle}-{r['session_id']}"
    return rows


def stage_bronze(spark, work: str, cycle: int, rows: list[dict]) -> None:
    name = "part-000000.parquet" if cycle == 0 else f"part-c{cycle:03d}-000000.parquet"
    cols = [c.split()[0] for c in BRONZE_DDL.split(", ")]
    df = spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], BRONZE_DDL)
    write_part(df, work, name)


def job(jars: str, work: str, fmt: str, script: str, cycle: int | None, log: list) -> int:
    """One driver of *script* (``silver_build`` or ``gold_finalize``);
    *cycle* None is the single-cycle rebuild of the whole prefix."""
    name = f"{script}{'_delta' if fmt == 'delta' else ''}.py"
    env = dict(os.environ)
    env.pop("LB_BRONZE_CYCLE", None)
    env.update(
        {
            "PYSPARK_SUBMIT_ARGS": submit_args(jars, work, fmt),
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
    text = proc.stdout + proc.stderr
    log.append(
        {
            "job": name,
            "cycle": cycle,
            "rc": proc.returncode,
            # Gold is rewritten whole either way, so its incremental path is
            # visible only in the driver output (the tail below is for failures).
            "replaced_from_watermark": "Replacing gold rows from" in text,
            "tail": text[-3000:],
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


def _data_files(table, fmt: str) -> set[str]:
    """The data files a read of *table* scans."""
    if fmt == "iceberg":
        return {r[0] for r in table.select("_file").distinct().collect()}
    return set(table.inputFiles())


def fingerprint(spark, work: str, fmt: str, layer: str) -> dict:
    """Silver's exact fingerprint; gold's rows (``c360_gold_compare``)."""
    table = _table(spark, work, fmt, layer)
    return table_rows(table) if layer == "gold" else table_fingerprint(table)


def changed(rows: list[dict]) -> list[dict]:
    """The rows with the first purchase's amount changed by one cent."""
    out = [dict(r) for r in rows]
    hit = next(r for r in out if r["interaction_type"] == "purchase")
    hit["transaction_amount"] = round(hit["transaction_amount"] + 0.01, 2)
    return out


def scenario(spark, jars: str, root: str, fmt: str) -> dict:
    cycles = [cycle_rows(c) for c in range(len(WINDOWS))]
    log: list = []

    inc = fresh(root, f"{fmt}-incremental")
    rcs = []
    appended: dict[int, bool] = {}
    held: set[str] = set()
    for c, rows in enumerate(cycles):
        stage_bronze(spark, inc, c, rows)
        rcs.append(job(jars, inc, fmt, "silver_build", c, log))
        if not any(rcs):
            # An append keeps every data file silver held and adds more; a
            # full rebuild replaces them.
            files = _data_files(_table(spark, inc, fmt, "silver"), fmt)
            appended[c] = held < files
            held = files
        rcs.append(job(jars, inc, fmt, "gold_finalize", c, log))
        if any(rcs):
            return {"rcs": rcs, "log": log}

    def rebuild(name: str, data: list[list[dict]]) -> tuple[str, list[int]]:
        work = fresh(root, f"{fmt}-{name}")
        for c, rows in enumerate(data):
            stage_bronze(spark, work, c, rows)
        return work, [
            job(jars, work, fmt, "silver_build", None, log),
            job(jars, work, fmt, "gold_finalize", None, log),
        ]

    full, full_rcs = rebuild("rebuild", cycles)
    alt, alt_rcs = rebuild("changed", [cycles[0], changed(cycles[1]), cycles[2]])
    # The incremental run really appended: cycles 1 and 2 of silver and gold
    # took the incremental path (a silent fallback to a full rebuild would
    # make the comparison trivially equal).
    by_job = {(e["job"], e["cycle"]): e for e in log}
    gold_job = f"gold_finalize{'_delta' if fmt == 'delta' else ''}.py"
    markers = {
        str(c): {
            "silver_appended": appended[c],
            "gold_incremental": by_job[(gold_job, c)]["replaced_from_watermark"],
        }
        for c in (1, 2)
    }
    out = {"rcs": rcs + full_rcs + alt_rcs, "log": log, "incremental": markers}
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
