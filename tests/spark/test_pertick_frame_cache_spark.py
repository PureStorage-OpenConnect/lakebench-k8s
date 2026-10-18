"""The per-tick frame cache does not change detection alerts.

Continuous gold persists the span the incremental rules' read spans cover
once per tick and hands every rule that cached frame, so the overlapping
spans are scanned once instead of once per rule. The alerts a tick writes
must be identical to the same tick with the cache off: the cache holds the
union of the rules' read spans, so each rule reads exactly the rows it would
have read from silver.

This drives ``gold_finalize_financial.run_detection_rules`` for a full tick
then a region tick, once with the cache on and once with
``read_span_union`` forced to None (cache off, the per-rule reads), over the
same silver, and compares the gold.alerts content. The corpus helpers come
from the incremental-detection test module (same scripts on PYTHONPATH).

Runs in a Spark child (``spark_subprocess``) with the Iceberg jar on the
driver classpath at JVM launch.
"""

from __future__ import annotations

import json
import sys

import pytest

pytest.importorskip("pyspark")


@pytest.mark.requires_jars("iceberg")
def test_pertick_frame_cache_keeps_alerts(tmp_path, spark_subprocess, spark_jars):
    res = spark_subprocess(__file__, tmp_path, spark_jars.classpath, timeout=900)
    out = json.loads(res.stdout.strip().splitlines()[-1])
    # The cache path ran (and logged). This line does not exist without the
    # fix, so a revert fails here.
    assert out["cache_log_seen"], out
    # Every incremental rule's read span is inside the cached union.
    assert out["union_covers_reads"], out
    # The alerts the region tick wrote are identical with the cache on and
    # off: same rules, entities, evidence, timestamps.
    assert out["cache_on"] == out["cache_off"], out
    # The tick produced alerts (an empty comparison would be vacuous).
    assert out["alert_count"] > 0, out


def _session(warehouse, jars):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars", jars)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.type", "hadoop")
        .config("spark.sql.catalog.lakehouse.cache-enabled", "false")
        .config("spark.sql.catalog.lakehouse.warehouse", f"file://{warehouse}/wh")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _run(warehouse, jars):
    import detection_rules as dr
    import gold_finalize_financial as gf
    import gold_refresh_financial as gr
    from incremental_detection import INCREMENTAL_RULES, IncrementalDetection
    from test_incremental_detection_spark import (
        _batches,
        _corpus,
        _entities,
        _frame,
        _watchlist,
    )

    spark = _session(warehouse, jars)
    for ns in ("gold", "silver"):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS lakehouse.{ns}")
    for ddl in (gf.DDL_ALERTS, gf.DDL_STATUS):
        spark.sql(ddl)

    watchlist = f"file://{warehouse}/watchlist.parquet"
    _watchlist(spark, watchlist)
    import os

    os.environ["LB_FINANCIAL_WATCHLIST_PATH"] = watchlist

    batches = _batches(_corpus(), "ordered")

    def _write_silver(upto):
        from pyspark.sql.functions import col, months

        frame = _frame(spark, batches, upto)
        # ingest_ts is not read by the rules; add it so the frame matches the
        # silver schema the pin carries. Partition by months(txn_timestamp)
        # so the per-rule span filters prune.
        frame = frame.withColumn("ingest_ts", col("txn_timestamp"))
        spark.sql("DROP TABLE IF EXISTS lakehouse.silver.transactions")
        frame.writeTo("lakehouse.silver.transactions").partitionedBy(
            months("txn_timestamp")
        ).create()
        return spark.table("lakehouse.silver.transactions")

    def _entities_written(tick):
        ents = _entities(spark, tick, late_from=1)
        spark.sql("DROP TABLE IF EXISTS lakehouse.silver.entities")
        ents.writeTo("lakehouse.silver.entities").create()

    rules = tuple(INCREMENTAL_RULES)
    overrides = {r: dict(gr.CONTINUOUS_RULE_OVERRIDES.get(r, {})) for r in rules}
    for r in ("W3_round_tripping", "W17_layering_chain"):
        overrides[r]["max_out_degree"] = 8

    def _tick(inc, upto, seen, run_id):
        txns = _write_silver(upto)
        _entities_written(upto)
        cut, position = gr.tick_position(txns, seen)
        inc.begin_tick(cut, rules)
        gf.run_detection_rules(
            spark,
            txns,
            run_id,
            rules=rules,
            incremental=inc,
            rule_overrides=overrides,
        )
        return position

    def _alerts_content(run_id):
        from pyspark.sql.functions import col

        rows = (
            spark.table("lakehouse.gold.alerts")
            .where(col("run_id") == run_id)
            .select("rule_id", "entity_id", "related_txn_ids", "alert_ts", "alert_score")
            .collect()
        )
        return sorted(
            (
                r["rule_id"],
                r["entity_id"],
                tuple(sorted(r["related_txn_ids"] or ())),
                str(r["alert_ts"]),
                float(r["alert_score"]) if r["alert_score"] is not None else None,
            )
            for r in rows
        )

    logged = []
    real_log = gf.log
    gf.log = lambda m: (logged.append(m), real_log(m))

    union_covers_reads = True
    try:
        # Cache ON: full tick to seed state, then a region tick (the cache
        # path). Capture this run's read-span union against each rule's exact
        # planner read span to prove the union is a superset.
        inc_on = IncrementalDetection(spark, f"file://{warehouse}/state-on")
        seen = _tick(inc_on, 3, None, "run-on")

        # Build the union the second tick will use, and each rule's exact read
        # from its planner, to check containment before the rules commit.
        txns2 = _write_silver(5)
        _entities_written(5)
        cut, position = gr.tick_position(txns2, seen)
        inc_on.begin_tick(cut, rules)

        def _params_of(rid):
            p = dr.rule_params(dr.get_rule(rid), "run-on", spark.table("lakehouse.silver.entities"))
            p.update(overrides.get(rid, {}))
            return p

        union = inc_on.read_span_union(rules, _params_of)
        # Each region rule's exact read span (from its planner) must lie in the
        # union.
        if union is not None:
            from incremental_detection import _RULES, _defaults, merge_spans

            for rid in rules:
                mode, rcut = inc_on._mode_for(rid)
                if mode != "region":
                    continue
                p = _defaults(dr.get_rule(rid), _params_of(rid))
                _a, _s, bounds = _RULES[rid](inc_on, txns2, p, inc_on.state.get(rid), mode, rcut)
                read = bounds.get("read")
                if read is None:
                    continue
                covered = merge_spans(list(union) + list(read))
                if merge_spans(union) != covered:
                    union_covers_reads = False

        cache_log_seen = any("per-tick frame cache:" in m for m in logged)

        gf.run_detection_rules(
            spark, txns2, "run-on", rules=rules, incremental=inc_on, rule_overrides=overrides
        )
        cache_on = _alerts_content("run-on")
        cache_log_seen = cache_log_seen or any("per-tick frame cache:" in m for m in logged)

        # Cache OFF: the same two ticks with read_span_union forced to None.
        real_union = IncrementalDetection.read_span_union
        IncrementalDetection.read_span_union = lambda *a, **k: None
        try:
            inc_off = IncrementalDetection(spark, f"file://{warehouse}/state-off")
            seen = _tick(inc_off, 3, None, "run-off")
            _tick(inc_off, 5, seen, "run-off")
            cache_off = _alerts_content("run-off")
        finally:
            IncrementalDetection.read_span_union = real_union
    finally:
        gf.log = real_log

    out = {
        "cache_log_seen": cache_log_seen,
        "union_covers_reads": union_covers_reads,
        "cache_on": cache_on,
        "cache_off": cache_off,
        "alert_count": len(cache_on),
    }
    print(json.dumps(out, default=str))
    spark.stop()


if __name__ == "__main__":
    _run(sys.argv[1], sys.argv[2])
