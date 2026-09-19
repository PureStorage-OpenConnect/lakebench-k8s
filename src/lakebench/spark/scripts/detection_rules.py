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

W5_splink_resolution (probabilistic entity resolution) is a separate
research effort tracked as ENH; it is not a "fix" and is intentionally
out of scope here.

The rule dispatcher (`get_rule`) is called by replay_financial.py.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    array_distinct,
    col,
    collect_list,
    collect_set,
    count,
    current_timestamp,
    explode,
    expr,
    lit,
    map_from_arrays,
    max as max_,
    min as min_,
    row_number,
    to_timestamp,
    when,
)
from pyspark.sql import Window


# Reporting thresholds by currency, mirroring datagen_rs::amounts::structuring_band.
# A txn is "structuring-suspicious" when its amount is >= 90% of the
# reporting threshold in its currency (i.e. sits in the top band under
# reporting) -- this matches the width of the datagen's structuring_band.
_STRUCTURING_THRESHOLDS = {
    "USD": 10_000.0, "CAD": 10_000.0, "AUD": 10_000.0,
    "GBP": 15_000.0, "EUR": 15_000.0, "CHF": 15_000.0,
    "JPY": 1_000_000.0, "INR": 1_000_000.0,
    "AED": 55_000.0,
    "SGD": 20_000.0,
    "MXN": 100_000.0,
    "CNY": 50_000.0, "BRL": 50_000.0,
    "HKD": 75_000.0,
    "KRW": 10_000_000.0,
}

RULE_VERSION = "1.0.0"
MODEL_ID = "lb-rules"
MODEL_VERSION = "1.0.0"


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
        cond = (col("txn_currency") == lit(ccy)) & (
            col("txn_amount").between(floor, thr)
        )
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
        suspicious
        .groupBy(
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
        .cast("double").alias("alert_score"),
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
        .filter(
            (unix_timestamp(col("ts_r")) - unix_timestamp(col("ts_l"))) <= seconds
        )
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
        .filter(
            (unix_timestamp(col("ts_out")) - unix_timestamp(col("ts_in"))) <= seconds
        )
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
        expr("cast(array_union(as_set, cs_set) as array<bigint>)").alias(
            "related_entity_ids"
        ),
        col("last_ts").alias("alert_ts"),
        # Score clamped to [0, 0.95] so downstream percentile aggregations
        # don't skew from >1 values (see W3 fix). max_forward_ratio -
        # threshold is scaled and offset from 0.7 baseline.
        expr(
            "least(0.95, 0.7 + (max_forward_ratio - {th}) * 0.15)".format(th=forward_ratio)
        ).cast("double").alias("alert_score"),
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
    )


def w1_connected_components(
    silver_txns: DataFrame,
    min_cluster_size: int = 3,
    max_iterations: int = 20,
    max_vertices: int = 5_000_000,
    run_id: str = "unknown",
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

    Returns:
        DataFrame with the gold.alerts schema. Empty when no component
        meets min_cluster_size or when the vertex count exceeds
        max_vertices (a warning is emitted in the latter case).
    """
    spark = silver_txns.sparkSession
    from pyspark.sql.functions import min as _min

    edges = (
        silver_txns
        .select(
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
    v_count = vertices.count()
    if v_count > max_vertices:
        # Fail loud but return an empty alerts DF so replay_financial
        # can proceed with its usual DELETE-then-append behavior (which
        # will simply clear any prior W1 rows for this rule_id).
        print(
            f"[W1] vertex count {v_count:,} exceeds max_vertices "
            f"{max_vertices:,}; skipping connected-components. Raise "
            f"max_vertices with the CLI --threshold-vertices override."
        )
        return _empty_alerts_df(spark, run_id)

    if v_count == 0:
        # Empty edge set (or every txn was a self-loop) -- nothing to
        # propagate. Return early with an empty alerts DF so we don't
        # emit a false "hit max_iterations without convergence" warning.
        return _empty_alerts_df(spark, run_id)

    labels = vertices.withColumn("label", col("id")).cache()

    converged = False
    for iteration in range(max_iterations):
        # Propagate: each vertex takes min(own label, min(neighbours' labels)).
        neighbour_labels = (
            labels.join(edges_u, labels["id"] == edges_u["src"])
            .select(edges_u["dst"].alias("id"), labels["label"])
        )
        new_labels = (
            labels.select("id", "label")
            .unionByName(neighbour_labels)
            .groupBy("id")
            .agg(_min("label").alias("label"))
            .cache()
        )
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
        labels.unpersist(blocking=False)
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
    components = (
        labels.groupBy(col("label").alias("component"))
        .agg(
            collect_set(col("id")).alias("entity_ids"),
            count(lit(1)).alias("component_size"),
        )
        .filter(col("component_size") >= min_cluster_size)
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
        .select(col("component"), col("uetr"), col("txn_timestamp"))
    )
    edge_aggs = (
        edges_tagged
        .groupBy("component")
        .agg(
            array_distinct(collect_list("uetr")).alias("related_txn_ids"),
            min_("txn_timestamp").alias("first_ts"),
            max_("txn_timestamp").alias("last_ts"),
        )
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
                lit("rule"), lit("min_cluster_size"), lit("max_iterations"),
                lit("max_vertices"),
            ),
            array(
                lit("W1_connected_components"),
                lit(str(min_cluster_size)),
                lit(str(max_iterations)),
                lit(str(max_vertices)),
            ),
        ).alias("evidence"),
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

    schema = StructType([
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
    ])
    return spark.createDataFrame([], schema)


_RULE_DISPATCH = {
    "W1_connected_components": w1_connected_components,
    "W2_structuring": w2_structuring,
    "W3_round_tripping": w3_round_tripping,
    "W4_risk_propagation": w4_risk_propagation,
}


def get_rule(rule_id: str):
    """Look up a rule function by id. Returns None if unknown."""
    return _RULE_DISPATCH.get(rule_id)


def known_rules() -> list[str]:
    return list(_RULE_DISPATCH.keys())


# Guard against ruff unused-import warnings for symbols exported for callers.
_ = (row_number, to_timestamp, explode, Window)
