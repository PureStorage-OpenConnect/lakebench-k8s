"""Detection rule modules (LB-108).

Provides rule dispatchers for the fraud-aml pipeline. Each rule takes a
silver.transactions DataFrame (already filtered/time-travelled by the
caller) and returns a DataFrame conforming to the gold.alerts schema.

Rules implemented in this file:
- W1_connected_components: undirected connected components over the
  entity graph induced by silver.transactions. Iterative label
  propagation, O(graph-diameter) iterations, no GraphFrames dependency.
- W2_structuring: entities making N>=3 transactions in a rolling window
  where every amount is within 90-100% of the local reporting threshold.
- W3_round_tripping: sequences of transactions where money returns to
  origin within a short window.
- W4_risk_propagation: high-velocity entity-to-entity chains.
- W5_sanctions_match: transactions with a counterparty entity name that
  fuzzy-matches an OFAC SDN entry (Phase 3D).
- W6_pep_counterparty: transactions with a counterparty on the PEP
  (politically exposed persons) list, filtered by co-occurring
  velocity anomaly.
- W7_cross_border_high_risk: cross-border transactions to a FATF grey/
  black list jurisdiction.
- W8_dormant_reactivation: originator account inactive > 90 days then
  a transaction >= $5,000-equivalent.

W5_splink_resolution (probabilistic entity resolution) is a separate
research effort tracked as ENH; it is not a "fix" and is intentionally
out of scope here.

The rule dispatcher (`get_rule`) is called by replay_financial.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    array,
    array_distinct,
    array_sort,
    col,
    collect_list,
    collect_set,
    count,
    current_timestamp,
    explode,
    expr,
    lit,
    map_from_arrays,
    row_number,
    size,
    struct,
    to_timestamp,
    when,
)
from pyspark.sql.functions import (
    max as max_,
)
from pyspark.sql.functions import (
    min as min_,
)
from pyspark.sql.functions import (
    sum as sum_,
)

# Reporting thresholds by currency, mirroring datagen_rs::amounts::structuring_band.
# A txn is "structuring-suspicious" when its amount is >= 90% of the
# reporting threshold in its currency (i.e. sits in the top band under
# reporting) -- this matches the width of the datagen's structuring_band.
_STRUCTURING_THRESHOLDS = {
    "USD": 10_000.0,
    "CAD": 10_000.0,
    "AUD": 10_000.0,
    "GBP": 15_000.0,
    "EUR": 15_000.0,
    "CHF": 15_000.0,
    "JPY": 1_000_000.0,
    "INR": 1_000_000.0,
    "AED": 55_000.0,
    "SGD": 20_000.0,
    "MXN": 100_000.0,
    "CNY": 50_000.0,
    "BRL": 50_000.0,
    "HKD": 75_000.0,
    "KRW": 10_000_000.0,
}

RULE_VERSION = "1.0.0"
MODEL_ID = "lb-rules"
MODEL_VERSION = "1.0.0"

# Driver-side rule -> planted-typology-target map. This MUST stay in lock-step
# with RULE_TARGETS in src/lakebench/benchmark/aml_queries.py (the
# orchestrator-side authority); a consistency test asserts they match, because
# the two live on opposite sides of the package boundary (this module ships to
# the Spark driver under /opt/spark/scripts; aml_queries does not). It exists
# here so score_financial can attribute a rule SKIP to the typologies that rule
# was the sole detector for, and mark their recall "not run" instead of 0%
# (LB-119 review F1). A None target means the rule has no planted typology.
RULE_TARGET_TYPOLOGY = {
    "W1_connected_components": "gather_scatter",
    "W2_structuring": "micro_structuring",
    "W3_round_tripping": "rapid_layering",
    "W4_risk_propagation": "stack",
    "W5_sanctions_match": None,
    "W6_pep_counterparty": None,
    "W7_cross_border_high_risk": "corridor_high_risk",
    "W8_dormant_reactivation": "dormant_reactivation",
}


# W1 declines to report when giant components (above max_cluster_size)
# hold more than this share of all vertices; see w1_connected_components.
GIANT_COMPONENT_SKIP_SHARE = 0.5


class RuleSkipped(Exception):
    """A rule declined to run for a structural reason (not an error).

    Raised when a rule cannot execute against the given silver corpus but
    nothing is broken -- e.g. W1 connected-components refusing above its
    vertex cap. The caller (gold_finalize / replay) must treat this as a
    THIRD outcome, distinct from both "ran and found zero alerts" and
    "raised an unexpected error". Reporting a skip as ``alerts=0`` makes a
    scale-100 W1 skip read as a 0% recall regression on the scorecard
    (LB-119); reporting it as ``error=`` would falsely imply a defect.

    ``reason`` is a short machine-parseable slug (e.g. ``vertex-cap``);
    ``detail`` carries the human-readable specifics for the driver log.
    """

    def __init__(self, reason: str, detail: str = "") -> None:
        # Normalise reason to a non-empty slug: the collector's skip parser
        # matches ``skipped=[A-Za-z0-9_-]+`` and would silently drop a line
        # whose reason is empty or contains spaces (LB-119 review F2).
        # Collapse any run of non-slug chars to a single '-' and fall back
        # to "unknown" so a skip is never lost, whatever a future caller
        # passes.
        import re as _re

        slug = _re.sub(r"[^A-Za-z0-9_-]+", "-", (reason or "").strip()).strip("-")
        self.reason = slug or "unknown"
        self.detail = detail
        super().__init__(f"{self.reason}: {detail}" if detail else self.reason)


def _suspicious_amount_expr():
    """Build a boolean Column expressing "txn amount is in the structuring
    band for its currency". The band per datagen is (9500, 9999) for USD;
    we widen slightly to (9000, 10000) to catch amounts detection would
    flag in the real world (someone rounding down to $9500 exactly, or
    the datagen picking $9500 = the band's floor).
    """
    when_expr = None
    for ccy, thr in _STRUCTURING_THRESHOLDS.items():
        floor = thr * 0.9
        cond = (col("txn_currency") == lit(ccy)) & (col("txn_amount").between(floor, thr))
        when_expr = cond if when_expr is None else (when_expr | cond)
    return when_expr


def w2_structuring(
    silver_txns: DataFrame,
    threshold_count: int = 3,
    window_hours: int = 24,
    run_id: str = "unknown",
) -> DataFrame:
    """Detect entities that made N>=threshold_count structuring-band txns
    within a rolling window_hours-hour window.

    Emits one alert per (entity, window) with related_txn_ids populated
    with the UETRs of the triggering transactions.

    Uses a rolling window via `window()` function (Spark) so overlapping
    24-hour spans are handled naturally. Group by (entity, window,
    currency) so a mixed-currency actor doesn't dilute.

    Args:
        silver_txns: DataFrame of silver.transactions schema.
        threshold_count: minimum count of structuring-band txns in the
            window to trigger an alert. Default 3, matches the datagen
            micro_structuring instance's typical (participants=9,
            rows_per_instance=8) pattern.
        window_hours: rolling window size. Default 24 (typical AML
            interpretation of "same day").
        run_id: opaque id for the detection run, written into every
            alert row.

    Returns:
        DataFrame with the gold.alerts schema.
    """
    from pyspark.sql.functions import window as window_

    suspicious = silver_txns.filter(_suspicious_amount_expr()).select(
        col("uetr"),
        col("originator_id").alias("entity_id"),
        col("beneficiary_id"),
        col("txn_timestamp"),
        col("txn_amount").cast("double").alias("amount"),
        col("txn_currency"),
    )

    windowed = (
        suspicious.groupBy(
            col("entity_id"),
            col("txn_currency"),
            window_(col("txn_timestamp"), f"{window_hours} hours"),
        )
        .agg(
            count(lit(1)).alias("suspicious_count"),
            collect_list("uetr").alias("related_txn_ids"),
            collect_set("beneficiary_id").alias("_related_entity_ids"),
            max_("amount").alias("max_amount"),
            min_("txn_timestamp").alias("first_ts"),
            max_("txn_timestamp").alias("last_ts"),
        )
        .filter(col("suspicious_count") >= threshold_count)
    )

    alerts = windowed.select(
        expr("uuid()").alias("alert_id"),
        lit("W2_structuring").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        col("entity_id"),
        # Deduplicate defensively (Spark collect_list preserves duplicates).
        array_distinct(col("related_txn_ids")).alias("related_txn_ids"),
        # Cast the set -> list of BIGINT for the array<bigint> DDL column.
        expr("cast(_related_entity_ids as array<bigint>)").alias("related_entity_ids"),
        col("last_ts").alias("alert_ts"),
        # Alert score: 0.5 baseline + 0.05 * (count - threshold), capped 0.95.
        (lit(0.5) + (col("suspicious_count") - lit(threshold_count)) * lit(0.05))
        .cast("double")
        .alias("alert_score"),
        when(col("suspicious_count") >= 6, lit("HIGH"))
        .when(col("suspicious_count") >= 4, lit("MED"))
        .otherwise(lit("LOW"))
        .alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("structuring").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Entity ', cast(entity_id as string), ' made ', "
            "cast(suspicious_count as string), ' structuring-band ', txn_currency, "
            "' transactions between ', cast(first_ts as string), ' and ', "
            "cast(last_ts as string))"
        ).alias("narrative"),
        map_from_arrays(
            array(lit("rule"), lit("threshold"), lit("window_hours")),
            array(lit("W2_structuring"), lit(str(threshold_count)), lit(str(window_hours))),
        ).alias("evidence"),
        # LB-125: wall-clock at rule execution. Batch = detection time;
        # continuous = the far end of detected_ts - ingest_ts (freshness /
        # time-to-detect). Appended LAST to match the gold.alerts DDL column
        # order, because gold_finalize writes via positional INSERT ... SELECT *.
        current_timestamp().alias("detected_ts"),
    )
    return alerts


def w3_round_tripping(
    silver_txns: DataFrame,
    window_hours: int = 72,
    run_id: str = "unknown",
) -> DataFrame:
    """Detect 2-hop round-trips: entity A -> entity B -> entity A within
    window_hours. This catches the `cycle` typology (at participants=4
    the intermediate hops produce direct back-and-forth pairs as a side
    effect) and `corridor_high_risk` / `cross_border_cycle` at their
    simplest instances.

    Real N-hop cycle detection requires GraphFrames / iterative BFS which
    isn't wired in this build; the 2-hop approximation is the minimum
    that still fires on the datagen's cycle instances.

    Emits one alert per (A, B) pair with related_txn_ids = UETRs of the
    A->B and B->A transactions inside the window.
    """
    from pyspark.sql.functions import greatest, least, unix_timestamp

    left = silver_txns.select(
        col("uetr").alias("uetr_l"),
        col("originator_id").alias("a"),
        col("beneficiary_id").alias("b"),
        col("txn_amount_usd").alias("amt_l"),
        col("txn_timestamp").alias("ts_l"),
    )
    right = silver_txns.select(
        col("uetr").alias("uetr_r"),
        col("originator_id").alias("b_r"),
        col("beneficiary_id").alias("a_r"),
        col("txn_timestamp").alias("ts_r"),
    )
    # Round-trip: (a -> b) then (b -> a) within +/- window_hours,
    # ts_r >= ts_l (b returns to a AFTER a sends to b). Filter
    # a != b so degenerate self-loops don't count.
    seconds = window_hours * 3600
    pairs = (
        left.join(
            right,
            (col("a") == col("a_r")) & (col("b") == col("b_r")) & (col("a") != col("b")),
            "inner",
        )
        .filter(col("ts_r") >= col("ts_l"))
        .filter((unix_timestamp(col("ts_r")) - unix_timestamp(col("ts_l"))) <= seconds)
    )
    # Aggregate per (a, b): collect all round-trip UETRs and count them.
    # Alerting once per direction (a<b canonicalized) so we don't double-
    # emit for each direction of the same underlying cycle.
    canon = pairs.select(
        least(col("a"), col("b")).alias("e1"),
        greatest(col("a"), col("b")).alias("e2"),
        col("uetr_l"),
        col("uetr_r"),
        col("ts_l"),
        col("ts_r"),
    )
    # roundtrip_count is triangular-inflated by the self-join (for k
    # matching pairs each direction, the join produces up to
    # k*(k+1)/2 rows because every earlier ts_l matches every later
    # ts_r). Dedupe by taking the SIZE OF THE DISTINCT UETR SET / 2:
    # each real round-trip pair contributes exactly 2 UETRs (l, r).
    alerts = (
        canon.groupBy("e1", "e2")
        .agg(
            collect_list(col("uetr_l")).alias("uetrs_l"),
            collect_list(col("uetr_r")).alias("uetrs_r"),
            max_("ts_r").alias("last_ts"),
            min_("ts_l").alias("first_ts"),
        )
        .withColumn(
            "related_txn_ids",
            array_distinct(expr("concat(uetrs_l, uetrs_r)")),
        )
        # 2 UETRs per real round-trip; divide the deduped uetr count.
        .withColumn(
            "roundtrip_count",
            (expr("size(related_txn_ids)") / lit(2)).cast("int"),
        )
        .filter(col("roundtrip_count") >= 1)
    )
    return alerts.select(
        expr("uuid()").alias("alert_id"),
        lit("W3_round_tripping").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        col("e1").alias("entity_id"),
        col("related_txn_ids"),
        array(col("e1"), col("e2")).alias("related_entity_ids"),
        col("last_ts").alias("alert_ts"),
        # Score clamped to [0, 0.95] so aggregate percentiles don't
        # exceed the [0,1] domain analysts expect.
        expr("least(0.95, 0.6 + roundtrip_count * 0.05)").cast("double").alias("alert_score"),
        when(col("roundtrip_count") >= 3, lit("HIGH"))
        .when(col("roundtrip_count") >= 2, lit("MED"))
        .otherwise(lit("LOW"))
        .alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("round_tripping").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Round-trip between entities ', cast(e1 as string), ' and ', "
            "cast(e2 as string), ' -- ', cast(roundtrip_count as string), "
            "' back-and-forth pairs between ', cast(first_ts as string), ' and ', "
            "cast(last_ts as string))"
        ).alias("narrative"),
        map_from_arrays(
            array(lit("rule"), lit("window_hours")),
            array(lit("W3_round_tripping"), lit(str(window_hours))),
        ).alias("evidence"),
        # LB-125: wall-clock at rule execution. Batch = detection time;
        # continuous = the far end of detected_ts - ingest_ts (freshness /
        # time-to-detect). Appended LAST to match the gold.alerts DDL column
        # order, because gold_finalize writes via positional INSERT ... SELECT *.
        current_timestamp().alias("detected_ts"),
    )


def w4_risk_propagation(
    silver_txns: DataFrame,
    velocity_hours: int = 6,
    forward_ratio: float = 0.8,
    run_id: str = "unknown",
) -> DataFrame:
    """Detect rapid pass-through: entity B receives funds from A and
    forwards >= forward_ratio of them to some entity C, all within
    velocity_hours.

    Fires on the `rapid_layering` typology (participants=3, whole chain
    within one civil day per datagen). Approximation: does not require
    C != A (which would require another join step); a self-loop that
    just cycles back also fires, treated as a subset of round-tripping.

    Emits one alert per B (the intermediate entity) with related_txn_ids
    = [incoming_uetr, outgoing_uetr] pair.
    """
    from pyspark.sql.functions import unix_timestamp

    incoming = silver_txns.select(
        col("uetr").alias("uetr_in"),
        col("originator_id").alias("a"),
        col("beneficiary_id").alias("b"),
        col("txn_amount_usd").alias("amt_in"),
        col("txn_timestamp").alias("ts_in"),
    )
    outgoing = silver_txns.select(
        col("uetr").alias("uetr_out"),
        col("originator_id").alias("b2"),
        col("beneficiary_id").alias("c"),
        col("txn_amount_usd").alias("amt_out"),
        col("txn_timestamp").alias("ts_out"),
    )
    seconds = velocity_hours * 3600
    joined = (
        incoming.join(outgoing, col("b") == col("b2"), "inner")
        .filter(col("ts_out") >= col("ts_in"))
        .filter((unix_timestamp(col("ts_out")) - unix_timestamp(col("ts_in"))) <= seconds)
        # amt_in > 0 guards against div-by-zero downstream on the score
        # computation AND filters degenerate matches when txn_amount_usd
        # coerced to 0 (nullable xchg_rate paths). Without it, entities
        # with a zero-value incoming txn matched every outgoing txn,
        # blowing up alert count by cartesian.
        .filter(col("amt_in") > lit(0))
        .filter(col("amt_out") >= col("amt_in") * lit(forward_ratio))
    )
    # Group by B (the entity being alerted on) so the alert count is
    # per-entity, not per (uetr_in, uetr_out) pair. Prior design produced
    # N*M rows for an entity with N incoming + M outgoing matches
    # (10000 rows for a moderate case). Collect all in/out UETRs per B
    # into related_txn_ids and the counterparty ids into
    # related_entity_ids so investigators can trace all txns from a
    # single alert row.
    per_entity = joined.groupBy("b").agg(
        collect_list(col("uetr_in")).alias("uetrs_in"),
        collect_list(col("uetr_out")).alias("uetrs_out"),
        collect_set(col("a")).alias("as_set"),
        collect_set(col("c")).alias("cs_set"),
        count(lit(1)).alias("chain_count"),
        max_("ts_out").alias("last_ts"),
        min_("ts_in").alias("first_ts"),
        max_(col("amt_out") / col("amt_in")).alias("max_forward_ratio"),
    )
    return per_entity.select(
        expr("uuid()").alias("alert_id"),
        lit("W4_risk_propagation").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        col("b").alias("entity_id"),
        array_distinct(expr("concat(uetrs_in, uetrs_out)")).alias("related_txn_ids"),
        # cast(set as array<bigint>) via expr for compat with older Spark
        expr("cast(array_union(as_set, cs_set) as array<bigint>)").alias("related_entity_ids"),
        col("last_ts").alias("alert_ts"),
        # Score clamped to [0, 0.95] so downstream percentile aggregations
        # don't skew from >1 values (see W3 fix). max_forward_ratio -
        # threshold is scaled and offset from 0.7 baseline.
        expr(f"least(0.95, 0.7 + (max_forward_ratio - {forward_ratio}) * 0.15)")
        .cast("double")
        .alias("alert_score"),
        when(col("chain_count") >= 3, lit("HIGH"))
        .when(col("chain_count") >= 2, lit("MED"))
        .otherwise(lit("LOW"))
        .alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("risk_propagation").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Rapid pass-through at entity ', cast(b as string), ': ', "
            "cast(chain_count as string), ' incoming/outgoing chains, first ', "
            "cast(first_ts as string), ' last ', cast(last_ts as string))"
        ).alias("narrative"),
        map_from_arrays(
            array(lit("rule"), lit("velocity_hours"), lit("forward_ratio")),
            array(lit("W4_risk_propagation"), lit(str(velocity_hours)), lit(str(forward_ratio))),
        ).alias("evidence"),
        # LB-125: wall-clock at rule execution. Batch = detection time;
        # continuous = the far end of detected_ts - ingest_ts (freshness /
        # time-to-detect). Appended LAST to match the gold.alerts DDL column
        # order, because gold_finalize writes via positional INSERT ... SELECT *.
        current_timestamp().alias("detected_ts"),
    )


def _truncate_lineage(df: DataFrame) -> DataFrame:
    """Materialise ``df`` and cut its lineage.

    A reliable checkpoint under ``LB_GOLD_URI`` when that is set (cluster
    and local lakebench runs): unlike localCheckpoint it survives losing an
    executor, which would otherwise fail the rule with "checkpoint block not
    found". localCheckpoint only when no gold URI is set (tests). The
    directory is removed by cleanup_w1_checkpoints after the rule loop.
    """
    path = _w1_checkpoint_dir()
    sc = df.sparkSession.sparkContext
    if path:
        if not sc.getCheckpointDir():
            sc.setCheckpointDir(path)
        return df.checkpoint(eager=True)
    return df.localCheckpoint(eager=True)


def _w1_checkpoint_dir() -> str | None:
    """Where W1 writes its reliable checkpoints, or None (localCheckpoint)."""
    import os

    base = os.getenv("LB_GOLD_URI")
    return base.rstrip("/") + "/_checkpoints/w1" if base else None


def cleanup_w1_checkpoints(spark) -> None:
    """Delete W1's checkpoint directory. Call once the alerts are written.

    Nothing else removes it (Spark's cleaner does not delete reliable
    checkpoints by default), and left in place it grew with every run and
    was counted in the measured gold size.
    """
    path = _w1_checkpoint_dir()
    if not path:
        return
    try:
        jvm = spark._jvm  # type: ignore[attr-defined]
        hconf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), hconf)
        p = jvm.org.apache.hadoop.fs.Path(path)
        if fs.exists(p):
            fs.delete(p, True)
    except Exception as e:  # noqa: BLE001
        print(f"[W1] checkpoint cleanup of {path} failed: {e}")


def w1_connected_components(
    silver_txns: DataFrame,
    min_cluster_size: int = 3,
    max_iterations: int = 20,
    max_vertices: int = 5_000_000,
    run_id: str = "unknown",
    max_cluster_size: int = 1000,
    max_txns_per_alert: int = 250_000,
) -> DataFrame:
    """Undirected connected components over the entity graph induced by
    silver.transactions. Emits one alert per component whose vertex count
    reaches `min_cluster_size`.

    Algorithm: iterative label propagation using the classic
    "min-neighbour-id" rule. Each vertex starts with its own id as its
    label; on each iteration a vertex adopts the minimum label seen
    among itself and its neighbours. Converges to the "min id in the
    connected component" label in O(diameter) iterations. Bounded by
    `max_iterations` so a pathologically hostile graph can't hang the
    replay job.

    Implemented directly in Spark SQL rather than through GraphFrames
    because GraphFrames publishes per Spark minor and per Scala minor
    (e.g. `graphframes:graphframes:0.8.4-spark3.5-s_2.12`) and lakebench
    supports Spark 3.5, 4.0, 4.1 across Scala 2.12/2.13 -- a single
    dependency string cannot span that matrix, so wiring GraphFrames
    would silently downgrade in the matrix cells the artifact doesn't
    cover. Iterative label propagation is O(graph-diameter) iterations
    which is a handful for fraud graphs (typology instances are small
    cliques and short chains); slower than a Pregel implementation at
    the top end but with no dependency risk.

    Args:
        silver_txns: DataFrame of silver.transactions schema.
        min_cluster_size: minimum vertex count in a component to emit
            an alert. Default 3 (a pair is a normal transaction; three
            or more entities sharing a component is worth flagging).
        max_iterations: safety cap on label-propagation rounds. Default
            20 (fraud graphs almost always converge in <10). If the
            job hits this cap the emitted alerts still reflect the
            partial labeling; a diagnostic row is logged.
        max_vertices: refuse to run above this vertex count. Iterative
            label propagation shuffles O(edges) per iteration; a
            multi-million-vertex silver at scale >= 100 would produce
            an unhelpful multi-hour replay. Default 5M; operator
            raises it when they have the executor budget.
        run_id: opaque id written into every alert row.
        max_cluster_size: components larger than this are not emitted. On
            the full transaction graph the baseline counterparty rings join
            most entities into one giant component; as an alert it named
            every transaction in the corpus (a single row past Spark's 2 GB
            limit at scale 10) and no investigator could work it. They are
            counted and logged instead; when they hold more than
            GIANT_COMPONENT_SKIP_SHARE of all vertices the rule raises
            RuleSkipped("giant-component") so the scorecard reads "not run".
        max_txns_per_alert: cap on related_txn_ids per alert (earliest
            first); the evidence map records the total and whether the list
            was truncated.

    Returns:
        DataFrame with the gold.alerts schema. Empty when no component
        meets min_cluster_size or when the vertex count exceeds
        max_vertices (a warning is emitted in the latter case).
    """
    spark = silver_txns.sparkSession
    from pyspark.sql.functions import min as _min

    edges = (
        silver_txns.select(
            col("originator_id").alias("src"),
            col("beneficiary_id").alias("dst"),
            col("uetr"),
            col("txn_timestamp"),
        )
        .filter(col("src").isNotNull() & col("dst").isNotNull())
        .filter(col("src") != col("dst"))  # self-loops don't affect components
    )

    # Symmetric edge list: undirected components need both directions.
    edges_u = (
        edges.select(col("src"), col("dst"))
        .unionByName(edges.select(col("dst").alias("src"), col("src").alias("dst")))
        .distinct()
    )

    vertices = (
        edges_u.select(col("src").alias("id"))
        .unionByName(edges_u.select(col("dst").alias("id")))
        .distinct()
    )
    # NB: v_count itself is a full O(edges) distinct-count shuffle over the
    # symmetric edge list, so the cap check is cheaper than running 20
    # label-propagation iterations but is NOT free -- at scale 100 this
    # count alone shuffles billions of rows. It is the minimum work needed
    # to know the vertex count before deciding to run.
    v_count = vertices.count()
    if v_count > max_vertices:
        # Skip loud AND distinguishably. Raising RuleSkipped (rather than
        # returning an empty alerts DF) lets gold_finalize / replay emit a
        # ``skipped=vertex-cap`` log line the metrics collector records as a
        # third state, so a scale-100 W1 skip is never rendered as a 0%
        # recall regression (LB-119). Raise the cap via the
        # ``financial.w1_max_vertices`` config field (env
        # ``LB_FINANCIAL_W1_MAX_VERTICES``), which gold_finalize threads
        # into this parameter -- there is no separate CLI flag.
        raise RuleSkipped(
            "vertex-cap",
            f"vertices={v_count} max={max_vertices} "
            f"(raise financial.w1_max_vertices to run W1 at this scale)",
        )

    if v_count == 0:
        # Empty edge set (or every txn was a self-loop) -- nothing to
        # propagate. Return early with an empty alerts DF so we don't
        # emit a false "hit max_iterations without convergence" warning.
        return _empty_alerts_df(spark, run_id)

    # Checkpointed (eager) rather than cached: it materialises AND cuts the
    # lineage. With cache each iteration's plan embedded the previous one, so
    # the plan (and its string form, built for every query event) grew with
    # every round; the driver ran out of heap building it (reproduced in the
    # executed W1 test) and planning time grew each iteration.
    labels = _truncate_lineage(vertices.withColumn("label", col("id")))

    converged = False
    for _iteration in range(max_iterations):
        # Propagate: each vertex takes min(own label, min(neighbours' labels)).
        # Alias both sides. After the first iteration, ``labels`` is derived
        # from a prior unionByName of ``neighbour_labels`` -- which itself
        # was built from ``edges_u`` -- so ``labels`` and ``edges_u`` share
        # attribute IDs in Catalyst. Without aliases, ``labels["id"]`` and
        # ``edges_u["src"]`` resolve to ambiguous attributes and Spark raises
        # ``AnalysisException: Column dst#NNN are ambiguous`` (surfaced by
        # the first live S1 run of the AML batch pipeline). Aliases give
        # each side its own attribute namespace so column resolution is
        # unambiguous every iteration.
        lbl = labels.alias("lbl")
        eg = edges_u.alias("eg")
        neighbour_labels = lbl.join(eg, col("lbl.id") == col("eg.src")).select(
            col("eg.dst").alias("id"), col("lbl.label").alias("label")
        )
        new_labels = (
            labels.select("id", "label")
            .unionByName(neighbour_labels)
            .groupBy("id")
            .agg(_min("label").alias("label"))
        )
        new_labels = _truncate_lineage(new_labels)
        # Convergence check: per-vertex label deltas, not distinct-label
        # count. A component that still has multiple live labels can
        # keep shuffling vertices between them without changing the
        # distinct set, so distinct().count() reports "converged" while
        # min-propagation is still bubbling. That silently splits a real
        # cluster into two smaller ones and mis-sizes the alert. Correct
        # signal: count vertices whose label strictly decreased between
        # iterations; propagation is stable iff that count is zero.
        changed = (
            new_labels.alias("n")
            .join(labels.alias("p"), col("n.id") == col("p.id"), "inner")
            .filter(col("n.label") < col("p.label"))
            .limit(1)
            .count()
        )
        labels = new_labels
        if changed == 0:
            converged = True
            break

    if not converged:
        print(
            f"[W1] hit max_iterations={max_iterations} without convergence; "
            f"emitting components from partial labeling. Consider raising "
            f"max_iterations if precision matters."
        )

    # Vertex -> component: labels is (id, label). Component = label.
    # Group by component to get its members.
    # Sizes first; only components inside [min, max] are materialised, since
    # collect_set over the giant component is itself an oversized row.
    sizes = labels.groupBy(col("label").alias("component")).agg(
        count(lit(1)).alias("component_size")
    )
    giant = (
        sizes.filter(col("component_size") > max_cluster_size)
        .agg(
            count(lit(1)).alias("n"),
            max_("component_size").alias("largest"),
            sum_("component_size").alias("vertices"),
        )
        .collect()[0]
    )
    if giant["n"]:
        share = (giant["vertices"] or 0) / max(1, v_count)
        detail = (
            f"{giant['n']} component(s) above max_cluster_size={max_cluster_size} "
            f"(largest {giant['largest']} entities, {share:.0%} of vertices)"
        )
        if share > GIANT_COMPONENT_SKIP_SHARE:
            # Most of the graph is one component: planted participants sit
            # inside it, so emitting only the small components would report
            # near-zero recall as if W1 had run and missed. Say "not run".
            raise RuleSkipped(
                "giant-component",
                f"{detail}; connected components over the full transaction "
                "graph cannot isolate typologies",
            )
        print(f"[W1] {detail} not emitted")
    qualifying = sizes.filter(
        (col("component_size") >= min_cluster_size) & (col("component_size") <= max_cluster_size)
    )
    components = (
        labels.join(qualifying, labels["label"] == qualifying["component"], "inner")
        .groupBy("component")
        .agg(
            collect_set(col("id")).alias("entity_ids"),
            max_("component_size").alias("component_size"),
        )
    )

    # For each qualifying component, gather the transactions that
    # touch its members. Tag each edge with the src vertex's component
    # label via a single hash join -- since both endpoints of every
    # edge are in the same connected component by construction, the
    # src's label IS the component. This avoids the previous
    # union-of-two-joins pattern (which doubled shuffle: each edge
    # appeared once under src_hits and once under dst_hits, then
    # collect_list->distinct dedup'd on the reduce side). Single
    # equi-join, each edge counted once, no dedup shuffle.
    edges_tagged = (
        edges.join(
            labels.select(col("id").alias("_src"), col("label").alias("component")),
            edges["src"] == col("_src"),
            "inner",
        )
        .join(qualifying.select("component"), "component", "left_semi")
        .select(col("component"), col("uetr"), col("txn_timestamp"))
    )
    edge_aggs = (
        edges_tagged.groupBy("component")
        .agg(
            array_sort(
                array_distinct(collect_list(struct(col("txn_timestamp"), col("uetr"))))
            ).alias("_txns"),
            min_("txn_timestamp").alias("first_ts"),
            max_("txn_timestamp").alias("last_ts"),
        )
        .withColumn("txn_total", size(col("_txns")))
        .withColumn(
            "related_txn_ids",
            expr(f"transform(slice(_txns, 1, {int(max_txns_per_alert)}), t -> t.uetr)"),
        )
        .drop("_txns")
    )
    # Bring the components metadata back for alert construction. Inner
    # join on component drops any component_size < min_cluster_size
    # component that has no in-cluster edges (self-loop cluster edge
    # case, filtered out earlier).
    with_txns = components.join(edge_aggs, "component", "inner")

    alerts = with_txns.select(
        expr("uuid()").alias("alert_id"),
        lit("W1_connected_components").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        # No single entity owns a cluster alert -- pick the min id
        # deterministically so replay is stable across runs.
        col("component").alias("entity_id"),
        col("related_txn_ids"),
        expr("cast(entity_ids as array<bigint>)").alias("related_entity_ids"),
        col("last_ts").alias("alert_ts"),
        # Larger components ~= higher risk. Bounded 0.5-0.95.
        expr(
            "cast(least(0.95, 0.5 + 0.05 * cast(component_size - "
            + str(min_cluster_size)
            + " as double)) as double)"
        ).alias("alert_score"),
        when(col("component_size") >= 8, lit("HIGH"))
        .when(col("component_size") >= 5, lit("MED"))
        .otherwise(lit("LOW"))
        .alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("cluster").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Connected component of ', cast(component_size as string), "
            "' entities (min id ', cast(component as string), "
            "') active between ', cast(first_ts as string), ' and ', "
            "cast(last_ts as string))"
        ).alias("narrative"),
        map_from_arrays(
            array(
                lit("rule"),
                lit("min_cluster_size"),
                lit("max_iterations"),
                lit("max_vertices"),
                lit("converged"),
                lit("max_cluster_size"),
                lit("txn_total"),
                lit("txns_truncated"),
            ),
            array(
                lit("W1_connected_components"),
                lit(str(min_cluster_size)),
                lit(str(max_iterations)),
                lit(str(max_vertices)),
                lit("true" if converged else "false"),
                lit(str(max_cluster_size)),
                col("txn_total").cast("string"),
                (col("txn_total") > lit(int(max_txns_per_alert))).cast("string"),
            ),
        ).alias("evidence"),
        # LB-125: wall-clock at rule execution. Batch = detection time;
        # continuous = the far end of detected_ts - ingest_ts (freshness /
        # time-to-detect). Appended LAST to match the gold.alerts DDL column
        # order, because gold_finalize writes via positional INSERT ... SELECT *.
        current_timestamp().alias("detected_ts"),
    )
    # ``labels`` is checkpointed, so everything downstream (components,
    # edges_tagged, alerts) reads the materialised labels rather than
    # recomputing the propagation loop.
    return alerts


# ---------------------------------------------------------------------------
# W5-W8: reference-table rules. Load lists from the packaged JSON files and
# broadcast to workers. Reference tables are small (< 100 rows each) so a
# broadcast join is O(N) in silver.transactions size with no shuffle.
# ---------------------------------------------------------------------------


def _aml_data_candidates() -> list[str]:
    """Candidate directories for AML reference JSON files.

    When ``LB_AML_DATA_DIR`` is set it is the ONLY candidate --
    tests and operators use the env var to pin a specific reference
    directory, and falling through to other candidates on a per-file
    miss would silently mix stale + current reference data (a partial
    override that only contains sanctions_list.json would otherwise
    shadow that file while other rules read from the installed pkg
    -- split-brain reference state, no warning).

    Otherwise:

    1. ``lakebench.spark.data.aml`` under an installed lakebench pkg
       (dev environment).
    2. ``aml/`` subdirectory next to this script (cluster fallback
       when the ConfigMap ships JSONs under a subdir mount).
    3. The directory containing this script itself (cluster fallback
       when the ConfigMap ships JSONs as flat top-level keys, which
       is the default since ConfigMap keys cannot contain slashes).
    """
    import os

    override = os.environ.get("LB_AML_DATA_DIR")
    if override:
        return [override]

    candidates: list[str] = []
    try:
        pkg = __import__("lakebench.spark.data", fromlist=["_"])
        pkg_dir = os.path.dirname(os.path.abspath(pkg.__file__))
        candidates.append(os.path.join(pkg_dir, "aml"))
    except ImportError:
        pass
    here = os.path.dirname(os.path.abspath(__file__))
    candidates.append(os.path.join(here, "aml"))
    candidates.append(here)
    return candidates


def _aml_data_path() -> str:
    """First existing AML data directory, or the last candidate as a
    stable string. Kept for tests that patched the singular helper."""
    import os

    for c in _aml_data_candidates():
        if os.path.isdir(c):
            return c
    return _aml_data_candidates()[-1]


def _load_reference(spark, filename: str, schema_ddl: str) -> DataFrame | None:
    """Load one packaged JSON reference file as a Spark DataFrame.

    Returns None when the entries list is empty OR the file is not
    present in any candidate directory. The latter is expected inside
    the cluster driver when the scripts ConfigMap has not shipped the
    AML sidecar files -- callers already treat None as "reference-list
    rule cannot run here" and return the empty alerts DF, which is the
    correct silent degrade for a benchmark whose W5/W6/W7 signal is
    optional.

    When ``LB_AML_DATA_DIR`` is set as the exclusive candidate and
    it does NOT contain ``filename``, we emit a stderr note so a
    partial override does not silently degrade to empty (round-2
    finding: an operator setting the env var to a scratch dir with
    only one file expects the others to work; making the override
    authoritative for correctness reasons is right, but the missing
    file has to be visible).

    `schema_ddl` is a Spark DDL string ("col1 type1, col2 type2, ...")
    used to type the DF. Explicit typing avoids Spark's Python-side
    schema inference, which is slow and infers Long for what should be
    String (e.g. sdn_id "SDN-000001").
    """
    import json
    import os

    path: str | None = None
    for cand in _aml_data_candidates():
        candidate_path = os.path.join(cand, filename)
        if os.path.isfile(candidate_path):
            path = candidate_path
            break
    if path is None:
        override = os.environ.get("LB_AML_DATA_DIR")
        if override:
            print(
                f"[aml] LB_AML_DATA_DIR={override!r} does not contain "
                f"{filename!r}; rule dependent on this reference will "
                "return empty alerts."
            )
        return None
    with open(path) as f:
        payload = json.load(f)
    entries = payload["entries"]
    if not entries:
        return None
    return spark.createDataFrame(entries, schema=schema_ddl)


def _normalize_name_expr(col_name: str) -> str:
    """Aggressive name normalization for sanctions/PEP joining.

    Applied to both sides of the join. Strategy:
    - upper + trim
    - strip punctuation and any non-alphanumeric except space
    - collapse repeated whitespace to a single space
    - drop the common corporate suffixes (LLC, LTD, PLC, CORP, INC,
      SA, AG, GMBH, PTE, LP) so `Foo LLC` matches `FOO`

    This is a heuristic first pass -- not fuzzy match. Real production
    sanctions filters go further (Jaccard 3-gram, phonetic, alias
    graphs). Our contract is documented as "exact match on aggressively
    normalized name" so the benchmark numbers describe THAT rule, not
    fuzzy-match performance.
    """
    return (
        # Order matters: strip punctuation before collapsing spaces so
        # `"FOO, LLC"` -> `"FOO  LLC"` -> `"FOO LLC"` -> `"FOO"` after
        # the corporate-suffix strip.
        "regexp_replace("
        "regexp_replace("
        "regexp_replace("
        "upper(trim(coalesce(" + col_name + ", ''))), "
        "'[^A-Z0-9 ]', ' '"
        "), "
        "'\\\\s+', ' '"
        "), "
        "'( LLC| LTD| PLC| CORP| CORPORATION| INC| SA| AG| GMBH| PTE| LP| CO| COMPANY| GROUP| HOLDINGS)+$', ''"
        ")"
    )


def w5_sanctions_match(
    silver_txns: DataFrame,
    run_id: str = "unknown",
) -> DataFrame:
    """Flag transactions whose beneficiary name matches an SDN entry.

    Exact match on the normalized (uppercased/trimmed/collapsed) name.
    Fuzzy match (Jaccard 3-gram or Levenshtein) is a Phase 2 extension
    tracked as a follow-up. Exact match is the FIRST pass -- it matches
    real production sanctions filters' "hard hit" tier.
    """
    from pyspark.sql.functions import broadcast

    spark = silver_txns.sparkSession
    raw = _load_reference(
        spark,
        "sanctions_list.json",
        "sdn_id string, entity_name string, entity_type string, program string",
    )
    if raw is None:
        return _empty_alerts_df(spark, run_id)
    sdn = raw.select(
        col("sdn_id"),
        expr(_normalize_name_expr("entity_name")).alias("sdn_name_key"),
    )

    hits = silver_txns.select(
        col("uetr"),
        col("originator_id").alias("entity_id"),
        col("beneficiary_id"),
        col("txn_timestamp"),
        col("txn_currency"),
        col("txn_amount").cast("double").alias("amount"),
        col("rptd_beneficiary_name"),
        expr(_normalize_name_expr("rptd_beneficiary_name")).alias("bene_name_key"),
    ).join(broadcast(sdn), col("bene_name_key") == col("sdn_name_key"), "inner")

    alerts = hits.select(
        expr("uuid()").alias("alert_id"),
        lit("W5_sanctions_match").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        col("entity_id"),
        array(col("uetr")).alias("related_txn_ids"),
        expr("array(cast(beneficiary_id as bigint))").alias("related_entity_ids"),
        col("txn_timestamp").alias("alert_ts"),
        lit(0.95).cast("double").alias("alert_score"),
        lit("HIGH").alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("sanctions_match").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Beneficiary ', rptd_beneficiary_name, ' matches SDN entry ', sdn_id, "
            "' on transaction ', uetr)"
        ).alias("narrative"),
        map_from_arrays(
            array(lit("rule"), lit("sdn_id"), lit("match_mode")),
            array(lit("W5_sanctions_match"), col("sdn_id"), lit("exact")),
        ).alias("evidence"),
        # LB-125: wall-clock at rule execution. Batch = detection time;
        # continuous = the far end of detected_ts - ingest_ts (freshness /
        # time-to-detect). Appended LAST to match the gold.alerts DDL column
        # order, because gold_finalize writes via positional INSERT ... SELECT *.
        current_timestamp().alias("detected_ts"),
    )
    return alerts


def w6_pep_counterparty(
    silver_txns: DataFrame,
    run_id: str = "unknown",
) -> DataFrame:
    """Flag transactions with a PEP beneficiary.

    Unlike sanctions matches, PEP counterparties are not intrinsically
    fraudulent -- they trigger enhanced due diligence, not blocking. So
    the alert priority defaults to MED, and we only emit an alert when
    the transaction is over $10,000 USD equivalent (small PEP payments
    are administrative and generate too many false positives).
    """
    from pyspark.sql.functions import broadcast

    spark = silver_txns.sparkSession
    raw = _load_reference(
        spark,
        "pep_list.json",
        "pep_id string, entity_name string, entity_type string, position string",
    )
    if raw is None:
        return _empty_alerts_df(spark, run_id)
    pep = raw.select(
        col("pep_id"),
        expr(_normalize_name_expr("entity_name")).alias("pep_name_key"),
        col("position"),
    )

    # Use txn_amount when txn_amount_usd is NULL (missing FX enrichment).
    # A JPY / EUR PEP payment without the USD conversion would otherwise
    # silently drop out of alerts because `NULL >= 10000` filters to false.
    # Using the raw txn_amount is a conservative fallback: false-positive
    # rate rises slightly on foreign currencies but no PEP hits vanish.
    hits = (
        silver_txns.filter(expr("coalesce(txn_amount_usd, txn_amount) >= 10000"))
        .select(
            col("uetr"),
            col("originator_id").alias("entity_id"),
            col("beneficiary_id"),
            col("txn_timestamp"),
            col("txn_amount").cast("double").alias("amount"),
            col("txn_amount_usd"),
            col("rptd_beneficiary_name"),
            expr(_normalize_name_expr("rptd_beneficiary_name")).alias("bene_name_key"),
        )
        .join(broadcast(pep), col("bene_name_key") == col("pep_name_key"), "inner")
    )

    alerts = hits.select(
        expr("uuid()").alias("alert_id"),
        lit("W6_pep_counterparty").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        col("entity_id"),
        array(col("uetr")).alias("related_txn_ids"),
        expr("array(cast(beneficiary_id as bigint))").alias("related_entity_ids"),
        col("txn_timestamp").alias("alert_ts"),
        lit(0.65).cast("double").alias("alert_score"),
        lit("MED").alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("pep_counterparty").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Beneficiary ', rptd_beneficiary_name, ' is PEP ', pep_id, "
            "' (', position, ') with USD equivalent ', cast(txn_amount_usd as string))"
        ).alias("narrative"),
        map_from_arrays(
            array(lit("rule"), lit("pep_id"), lit("position")),
            array(lit("W6_pep_counterparty"), col("pep_id"), col("position")),
        ).alias("evidence"),
        # LB-125: wall-clock at rule execution. Batch = detection time;
        # continuous = the far end of detected_ts - ingest_ts (freshness /
        # time-to-detect). Appended LAST to match the gold.alerts DDL column
        # order, because gold_finalize writes via positional INSERT ... SELECT *.
        current_timestamp().alias("detected_ts"),
    )
    return alerts


def w7_cross_border_high_risk(
    silver_txns: DataFrame,
    silver_entities: DataFrame | None = None,
    run_id: str = "unknown",
) -> DataFrame:
    """Flag cross-border transactions to a FATF grey/black list jurisdiction.

    Requires silver.entities to be joinable on beneficiary_id to get the
    country. Callers with a live silver in the current catalog can leave
    ``silver_entities`` as None -- the rule will auto-load
    ``LB_ICEBERG_CATALOG.LB_FINANCIAL_SILVER_ENTITIES`` (default
    ``lakehouse.silver.entities``) from the Spark session. If that table
    is not present (e.g. running against a partial silver from a legacy
    schema), the rule returns zero alerts rather than raising.

    Historically this rule crashed inside the driver when the caller did
    NOT pass silver_entities AND the reference JSON was not mounted --
    the ImportError from ``import lakebench.spark.data`` surfaced as a
    hard failure of the whole replay job. Both paths now degrade to an
    empty alerts DF with a stderr note so downstream metrics see a real
    zero, not a stack trace.
    """
    import os

    from pyspark.sql.functions import broadcast
    from pyspark.sql.utils import AnalysisException

    spark = silver_txns.sparkSession
    raw = _load_reference(
        spark,
        "high_risk_jurisdictions.json",
        "country_code string, country_name string, risk_tier string",
    )
    if raw is None:
        print("[W7] high_risk_jurisdictions.json not available; skipping.")
        return _empty_alerts_df(spark, run_id)

    if silver_entities is None:
        catalog = os.environ.get("LB_ICEBERG_CATALOG", "lakehouse")
        entities_table = os.environ.get("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
        try:
            silver_entities = spark.table(f"{catalog}.{entities_table}")
        except AnalysisException as e:
            print(f"[W7] silver.entities not found ({e}); skipping cross-border alerts.")
            return _empty_alerts_df(spark, run_id)
    hrj = raw.select(
        col("country_code"),
        col("risk_tier"),
    )

    # Collapse silver.entities to one country per entity_id
    # deterministically. Prior version used ``.dropDuplicates(["bene_entity_id"])``
    # which keeps whichever row Spark's shuffle put first -- across
    # re-runs against the same silver, two entities with divergent
    # country values (SCD1 update in flight, upstream dedup bug) would
    # produce different W7 alert counts run-to-run. Contract of this
    # rule is reproducibility, so aggregate to the lexicographically
    # smallest country per entity_id: unambiguous, deterministic,
    # tolerant of a rare multi-country row without silently biasing.
    from pyspark.sql.functions import min as _min_agg

    entities_country = (
        silver_entities.filter(col("country").isNotNull())
        .select(col("entity_id").alias("bene_entity_id"), col("country").alias("bene_country"))
        .groupBy("bene_entity_id")
        .agg(_min_agg("bene_country").alias("bene_country"))
    )

    hits = (
        silver_txns.filter(col("cross_border") == True)  # noqa: E712
        # inner join on entities: rows without a resolvable beneficiary
        # country do NOT alert here. That is a documented limitation:
        # cross-border alerts require the entity master to know the
        # counterparty country. Alternative would be to derive country
        # from beneficiary_bank_bic (chars 5-6) as a fallback; deferred
        # until we characterize what fraction of cross-border txns have
        # missing entity enrichment. Left join + inner-on-hrj was
        # misleading because inner-on-hrj drops NULL country too.
        .join(entities_country, col("beneficiary_id") == col("bene_entity_id"), "inner")
        .join(broadcast(hrj), col("bene_country") == col("country_code"), "inner")
        .select(
            col("uetr"),
            col("originator_id").alias("entity_id"),
            col("beneficiary_id"),
            col("txn_timestamp"),
            col("txn_amount").cast("double").alias("amount"),
            col("bene_country"),
            col("risk_tier"),
        )
    )

    alerts = hits.select(
        expr("uuid()").alias("alert_id"),
        lit("W7_cross_border_high_risk").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        col("entity_id"),
        array(col("uetr")).alias("related_txn_ids"),
        expr("array(cast(beneficiary_id as bigint))").alias("related_entity_ids"),
        col("txn_timestamp").alias("alert_ts"),
        when(col("risk_tier") == "black", lit(0.90))
        .otherwise(lit(0.60))
        .cast("double")
        .alias("alert_score"),
        when(col("risk_tier") == "black", lit("HIGH")).otherwise(lit("MED")).alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("cross_border_high_risk").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Cross-border transaction to ', bene_country, ' (FATF ', risk_tier, ' list) "
            "for ', cast(amount as string), ' on ', cast(txn_timestamp as string))"
        ).alias("narrative"),
        map_from_arrays(
            array(lit("rule"), lit("country"), lit("risk_tier")),
            array(lit("W7_cross_border_high_risk"), col("bene_country"), col("risk_tier")),
        ).alias("evidence"),
        # LB-125: wall-clock at rule execution. Batch = detection time;
        # continuous = the far end of detected_ts - ingest_ts (freshness /
        # time-to-detect). Appended LAST to match the gold.alerts DDL column
        # order, because gold_finalize writes via positional INSERT ... SELECT *.
        current_timestamp().alias("detected_ts"),
    )
    return alerts


def w8_dormant_reactivation(
    silver_txns: DataFrame,
    dormant_days: int = 90,
    amount_threshold_usd: float = 5000.0,
    run_id: str = "unknown",
) -> DataFrame:
    """Flag reactivation of dormant originator accounts.

    An originator's transaction is flagged when:
    - The gap to the originator's prior transaction exceeds dormant_days;
    - The current transaction's USD-equivalent amount >= threshold.

    First-ever activity for an originator is NOT flagged (no prior
    baseline). This is the correct semantics -- we cannot say an
    account was dormant if we never observed it before.
    """
    from pyspark.sql.functions import lag, unix_timestamp

    # Secondary sort on uetr so same-microsecond ties are broken
    # deterministically. Without this the LAG output is
    # shuffle-order-dependent and W8's alert count drifts between runs
    # -- fatal for benchmark reproducibility because typology-injected
    # bursts pack multiple txns into the same microsecond by design.
    prior = Window.partitionBy("originator_id").orderBy(col("txn_timestamp"), col("uetr"))
    # Keep txn_amount under its original name so the coalesce below
    # resolves; the aliased `amount` copy is only used in the narrative.
    with_lag = silver_txns.select(
        col("uetr"),
        col("originator_id"),
        col("beneficiary_id"),
        col("txn_timestamp"),
        col("txn_amount"),
        col("txn_amount").cast("double").alias("amount"),
        col("txn_amount_usd"),
        lag("txn_timestamp").over(prior).alias("prev_ts"),
    )

    dormant_seconds = dormant_days * 86400
    # W8 amount check falls back to raw txn_amount when the USD
    # conversion is missing -- symmetric with the W6 NULL-USD guard so
    # a foreign-currency reactivation is not silently ignored.
    hits = with_lag.filter(
        (col("prev_ts").isNotNull())
        & ((unix_timestamp("txn_timestamp") - unix_timestamp("prev_ts")) >= dormant_seconds)
        & (expr("coalesce(txn_amount_usd, txn_amount) >= " + str(amount_threshold_usd)))
    )

    alerts = hits.select(
        expr("uuid()").alias("alert_id"),
        lit("W8_dormant_reactivation").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        col("originator_id").alias("entity_id"),
        array(col("uetr")).alias("related_txn_ids"),
        expr("array(cast(beneficiary_id as bigint))").alias("related_entity_ids"),
        col("txn_timestamp").alias("alert_ts"),
        lit(0.70).cast("double").alias("alert_score"),
        lit("MED").alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("dormant_reactivation").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Originator ', cast(originator_id as string), ' reactivated after ', "
            "cast(round((unix_timestamp(txn_timestamp) - unix_timestamp(prev_ts)) / 86400.0, 0) "
            "as string), ' days with USD ', cast(txn_amount_usd as string))"
        ).alias("narrative"),
        map_from_arrays(
            array(lit("rule"), lit("dormant_days"), lit("amount_threshold_usd")),
            array(
                lit("W8_dormant_reactivation"),
                lit(str(dormant_days)),
                lit(str(amount_threshold_usd)),
            ),
        ).alias("evidence"),
        # LB-125: wall-clock at rule execution. Batch = detection time;
        # continuous = the far end of detected_ts - ingest_ts (freshness /
        # time-to-detect). Appended LAST to match the gold.alerts DDL column
        # order, because gold_finalize writes via positional INSERT ... SELECT *.
        current_timestamp().alias("detected_ts"),
    )
    return alerts


def _empty_alerts_df(spark, run_id: str) -> DataFrame:
    """Zero-row DataFrame with the gold.alerts schema, for the case
    where a rule declines to run (e.g. W1 above max_vertices)."""
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,  # noqa: F401
        DoubleType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("alert_id", StringType(), False),
            StructField("rule_id", StringType(), False),
            StructField("rule_version", StringType(), False),
            StructField("model_id", StringType(), False),
            StructField("model_version", StringType(), False),
            StructField("entity_id", LongType(), True),
            StructField("related_txn_ids", ArrayType(StringType()), True),
            StructField("related_entity_ids", ArrayType(LongType()), True),
            StructField("alert_ts", TimestampType(), True),
            StructField("alert_score", DoubleType(), True),
            StructField("priority", StringType(), True),
            StructField("status", StringType(), True),
            StructField("disposition", StringType(), True),
            StructField("alert_type", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("narrative", StringType(), True),
            StructField("evidence", MapType(StringType(), StringType()), True),
            # LB-125: last column, matching every rule projection + the DDL.
            StructField("detected_ts", TimestampType(), True),
        ]
    )
    return spark.createDataFrame([], schema)


_RULE_DISPATCH = {
    "W1_connected_components": w1_connected_components,
    "W2_structuring": w2_structuring,
    "W3_round_tripping": w3_round_tripping,
    "W4_risk_propagation": w4_risk_propagation,
    "W5_sanctions_match": w5_sanctions_match,
    "W6_pep_counterparty": w6_pep_counterparty,
    "W7_cross_border_high_risk": w7_cross_border_high_risk,
    "W8_dormant_reactivation": w8_dormant_reactivation,
}


def get_rule(rule_id: str):
    """Look up a rule function by id. Returns None if unknown."""
    return _RULE_DISPATCH.get(rule_id)


def known_rules() -> list[str]:
    return list(_RULE_DISPATCH.keys())


# Guard against ruff unused-import warnings for symbols exported for callers.
_ = (row_number, to_timestamp, explode, Window)
