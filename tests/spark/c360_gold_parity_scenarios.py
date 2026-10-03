"""Customer 360 gold, batch against continuous (V16-7 gate), in a fresh JVM.

Not collected by pytest (no ``test_`` prefix).
``test_c360_gold_batch_stream_parity`` runs it with ``spark_subprocess``.

Silver is the product's transform of the generator model's rows
(``gold_repeat_scenarios.silver_df``). Per format:

- batch: the real ``gold_finalize`` (``gold_finalize_delta``) ``main()``
  over the whole silver table;
- continuous: the real ``gold_refresh`` (``gold_refresh_delta``) module,
  loaded with its rate stream replaced by a stub that hands back the
  ``foreachBatch`` handler, which is then called for three ticks while
  silver grows by a third of its dates before each (as the silver stream
  appends);
- changed: one silver purchase amount changed, then ``gold_finalize`` again.

Gold tables are compared by ``table_fingerprint`` (shared with C36-2).

Usage: python c360_gold_parity_scenarios.py <jars> <work_dir>
Prints one JSON object on the last stdout line.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
from typing import Any

import gold_repeat_scenarios as sc
from table_fingerprint import table_fingerprint

CATALOGS = {"iceberg": "ice", "delta": "spark_catalog"}


class _Query:
    isActive = False

    def exception(self) -> None:
        return None

    def stop(self) -> None:
        pass


class _RateStream:
    """``spark.readStream`` for the refresh module: keeps the handler."""

    handler: Any = None

    def format(self, *_a: Any) -> _RateStream:
        return self

    def option(self, *_a: Any, **_k: Any) -> _RateStream:
        return self

    def load(self) -> _RateStream:
        return self

    @property
    def writeStream(self) -> _RateStream:  # noqa: N802 (pyspark name)
        return self

    def foreachBatch(self, fn: Any) -> _RateStream:  # noqa: N802 (pyspark name)
        _RateStream.handler = fn
        return self

    def trigger(self, *_a: Any, **_k: Any) -> _RateStream:
        return self

    def start(self) -> _Query:
        return _Query()


def _write_silver(spark, fmt: str, df, mode: str) -> None:
    tbl = f"{CATALOGS[fmt]}.{sc.SILVER}"
    if fmt == "iceberg":
        if mode == "overwrite":
            df.writeTo(tbl).createOrReplace()
        else:
            df.writeTo(tbl).append()
    else:
        w = df.write.format("delta").mode(mode)
        if mode == "overwrite":
            w = w.option("overwriteSchema", "true")
        w.saveAsTable(tbl)


def _env(work: str, fmt: str, gold_table: str) -> None:
    os.environ.update(
        {
            "LB_ICEBERG_CATALOG": CATALOGS[fmt],
            "LB_GOLD_URI": f"file://{work}/gold-{fmt}/",
            "LB_GOLD_TABLE": gold_table,
            "CHECKPOINT_LOCATION": f"file://{work}/ckpt-{fmt}",
        }
    )
    os.environ.pop("LB_GOLD_INCREMENTAL", None)


def batch_gold(spark, work: str, fmt: str, gold_table: str) -> dict:
    _env(work, fmt, gold_table)
    name = "gold_finalize" if fmt == "iceberg" else "gold_finalize_delta"
    sys.modules.pop(name, None)
    importlib.import_module(name).main()
    return table_fingerprint(spark.table(f"{CATALOGS[fmt]}.{gold_table}"))


def stream_gold(spark, work: str, fmt: str, parts: list, gold_table: str) -> dict:
    """Three refresh ticks, silver growing by one part before each."""
    from pyspark.sql import SparkSession

    _env(work, fmt, gold_table)
    name = "gold_refresh" if fmt == "iceberg" else "gold_refresh_delta"
    SparkSession.readStream = property(lambda self: _RateStream())  # type: ignore[assignment]
    sys.modules.pop(name, None)
    importlib.import_module(name)
    tick = _RateStream.handler
    for i, part in enumerate(parts):
        _write_silver(spark, fmt, part, "overwrite" if i == 0 else "append")
        tick(None, i)
    return table_fingerprint(spark.table(f"{CATALOGS[fmt]}.{gold_table}"))


def scenario(spark, work: str, fmt: str) -> dict:
    from pyspark.sql.functions import col

    silver = sc.silver_df(spark).localCheckpoint()
    days = sorted(r[0] for r in silver.select("interaction_date").distinct().collect())
    cut = [days[len(days) // 3], days[2 * len(days) // 3]]
    parts = [
        silver.filter(col("interaction_date") < cut[0]),
        silver.filter((col("interaction_date") >= cut[0]) & (col("interaction_date") < cut[1])),
        silver.filter(col("interaction_date") >= cut[1]),
    ]
    if fmt == "iceberg":
        spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.silver")
    else:
        spark.sql("CREATE SCHEMA IF NOT EXISTS spark_catalog.silver")
    out = {"stream": stream_gold(spark, work, fmt, parts, "gold.parity_stream")}
    # The batch reads the silver the three ticks built.
    out["batch"] = batch_gold(spark, work, fmt, "gold.parity_batch")
    out["silver_rows"] = spark.table(f"{CATALOGS[fmt]}.{sc.SILVER}").count()
    _write_silver(spark, fmt, sc.changed(silver), "overwrite")
    out["changed"] = batch_gold(spark, work, fmt, "gold.parity_changed")
    return out


def main():
    from pyspark.sql import SparkSession

    jars, work = sys.argv[1], sys.argv[2]
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
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", f"file://{work}/ice-wh")
        .config("spark.sql.warehouse.dir", f"file://{work}/spark-wh")
        .getOrCreate()
    )
    # The scripts end with spark.stop(); this session serves every job.
    SparkSession.stop = lambda self: None  # type: ignore[method-assign]
    out = {fmt: scenario(spark, work, fmt) for fmt in ("iceberg", "delta")}
    print(json.dumps(out, default=str))


if __name__ == "__main__":
    main()
