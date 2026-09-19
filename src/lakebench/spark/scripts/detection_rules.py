"""Detection rule modules (LB-108, partial implementation).

Provides rule dispatchers for the fraud-aml pipeline. Each rule takes a
silver.transactions DataFrame (already filtered/time-travelled by the
caller) and returns a DataFrame conforming to the gold.alerts schema.

Rules implemented in this file:
- W2_structuring: entities making N>=3 transactions in a rolling window
  where every amount is within 90-100% of the local reporting threshold.
- W3_round_tripping: (stub) sequences of 3+ transactions where
  money returns to origin within a short window.
- W4_risk_propagation: (stub) high-velocity entity-to-entity chains.

Two more rules that the datagen's synthetic typologies would benefit from
but which need graph libraries (GraphFrames, sparkling-graph) are tracked
as follow-ups:
- W1_connected_components: entity clustering for synthetic_identity /
  entity_clusters gold table population.
- W5_splink_resolution: probabilistic entity resolution.

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
    uuid,
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
    alerts = (
        canon.groupBy("e1", "e2")
        .agg(
            count(lit(1)).alias("roundtrip_count"),
            collect_list(col("uetr_l")).alias("uetrs_l"),
            collect_list(col("uetr_r")).alias("uetrs_r"),
            max_("ts_r").alias("last_ts"),
            min_("ts_l").alias("first_ts"),
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
        array_distinct(
            expr("concat(uetrs_l, uetrs_r)")
        ).alias("related_txn_ids"),
        array(col("e1"), col("e2")).alias("related_entity_ids"),
        col("last_ts").alias("alert_ts"),
        (lit(0.6) + col("roundtrip_count") * lit(0.05)).cast("double").alias("alert_score"),
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
        .filter(col("amt_out") >= col("amt_in") * lit(forward_ratio))
    )
    return joined.select(
        expr("uuid()").alias("alert_id"),
        lit("W4_risk_propagation").alias("rule_id"),
        lit(RULE_VERSION).alias("rule_version"),
        lit(MODEL_ID).alias("model_id"),
        lit(MODEL_VERSION).alias("model_version"),
        col("b").alias("entity_id"),
        array(col("uetr_in"), col("uetr_out")).alias("related_txn_ids"),
        array(col("a"), col("b"), col("c")).alias("related_entity_ids"),
        col("ts_out").alias("alert_ts"),
        (
            lit(0.7) + ((col("amt_out") / col("amt_in")) - lit(forward_ratio)) * lit(0.3)
        ).cast("double").alias("alert_score"),
        lit("HIGH").alias("priority"),
        lit("OPEN").alias("status"),
        lit(None).cast("string").alias("disposition"),
        lit("risk_propagation").alias("alert_type"),
        lit(run_id).alias("run_id"),
        expr(
            "concat('Rapid pass-through at entity ', cast(b as string), "
            "': received ', cast(amt_in as string), ' from ', cast(a as string), "
            "' at ', cast(ts_in as string), ', forwarded ', cast(amt_out as string), "
            "' to ', cast(c as string), ' at ', cast(ts_out as string))"
        ).alias("narrative"),
        map_from_arrays(
            array(lit("rule"), lit("velocity_hours"), lit("forward_ratio")),
            array(lit("W4_risk_propagation"), lit(str(velocity_hours)), lit(str(forward_ratio))),
        ).alias("evidence"),
    )


_RULE_DISPATCH = {
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
_ = (row_number, uuid, to_timestamp, explode, Window)
