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
- W3_round_tripping: funds that return to their originator through 2-5
  transfers within a bounded window (temporal cycle search).
- W4_risk_propagation: high-velocity entity-to-entity chains.
- W5_sanctions_match: payments whose beneficiary fuzzy-matches an entry on
  the corpus's dated synthetic sanctions list, screened at transaction time
  and rescreened when a list version is published.
- W6_pep_counterparty: payments of $10,000 or more whose beneficiary
  fuzzy-matches an entry on the corpus's synthetic PEP list.
- W7_cross_border_high_risk: cross-border transactions to a FATF grey/
  black list jurisdiction or to one of the generator's synthetic high-risk
  corridor countries (synthetic_corridors.json, not a regulatory list).
- W8_dormant_reactivation: originator account inactive > 90 days then
  a transaction >= $5,000-equivalent.
- W17_layering_chain: open chains of 3+ transfers where each hop forwards
  80-100% of the previous one within 7 days (temporal path search).

W5_splink_resolution (probabilistic entity resolution) is a separate
research effort tracked as ENH; it is not a "fix" and is intentionally
out of scope here.

The rule dispatcher (`get_rule`) is called by replay_financial.py.
"""

from __future__ import annotations

import contextlib

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

# Cash-reporting thresholds by currency (USD 10,000 CTR and the local
# equivalents). Structuring is defined relative to these, so the generator
# plants its structuring amounts against the same figures; that shared input
# is the regulation, not a rule parameter tuned to the data. A txn is
# "structuring-suspicious" when its amount is between 90% of the threshold
# and the threshold. The 90% floor is self-chosen (R2): it is wider than the
# generator's band (95-99.99% for USD) and belongs in the threshold-cliff
# check.
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

#: The gold.alerts columns in table order: (name, DDL type, nullable). The
#: one source for every rule's alert frame (_alert_frame), the empty frame
#: (_empty_alerts_df) and gold_finalize_financial.DDL_ALERTS. The driver
#: writes alerts with a positional INSERT ... SELECT *, so this order is the
#: table's. New columns are appended, never inserted.
ALERT_COLUMNS = (
    ("alert_id", "STRING", False),
    ("rule_id", "STRING", False),
    ("rule_version", "STRING", False),
    ("model_id", "STRING", False),
    ("model_version", "STRING", False),
    ("entity_id", "BIGINT", False),
    ("related_txn_ids", "ARRAY<STRING>", True),
    ("related_entity_ids", "ARRAY<BIGINT>", True),
    ("alert_ts", "TIMESTAMP", False),
    ("alert_score", "DOUBLE", True),
    ("priority", "STRING", True),
    ("status", "STRING", True),
    ("disposition", "STRING", True),
    ("alert_type", "STRING", True),
    ("run_id", "STRING", False),
    ("narrative", "STRING", True),
    ("evidence", "MAP<STRING, STRING>", True),
    ("detected_ts", "TIMESTAMP", True),
)

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
    "W3_round_tripping": "cycle",
    "W4_risk_propagation": "rapid_layering",
    "W5_sanctions_match": "sanctions_match",
    "W6_pep_counterparty": "pep_match",
    "W7_cross_border_high_risk": "corridor_high_risk",
    "W8_dormant_reactivation": "dormant_reactivation",
    "W17_layering_chain": "stack",
}

# Alert subject scope (GOALS P10 stage 0). The reporting FI monitors its own
# customers: a customer-scoped rule raises an alert only when its subject
# (entity_id) is a customer in silver.entities, and non-customers appear only
# as related parties. The graph scenarios run as an advanced-analytics overlay
# across the whole payment network, so their subject can be an account at
# another bank; they are declared as counterparty scenarios instead.
# COUNTERPARTY_SCENARIOS must match tm_operations.DEFAULT_COUNTERPARTY_SCENARIOS
# and the TmOperationsConfig default (a test asserts both), or the TM layer's
# noncustomer_alerts_declared invariant fails on every run. The typology
# generator makes every planted subject a customer (typology::subject_index),
# and each customer-scoped rule's entity_id is that subject, so the scope does
# not change designated recall.
CUSTOMER_SCOPED_RULES = frozenset(
    {
        "W2_structuring",
        "W5_sanctions_match",
        "W6_pep_counterparty",
        "W7_cross_border_high_risk",
        "W8_dormant_reactivation",
    }
)
COUNTERPARTY_SCENARIOS = (
    "W1_connected_components",
    "W3_round_tripping",
    "W4_risk_propagation",
    "W17_layering_chain",
)


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


def _entities_frame(spark, silver_entities: DataFrame | None, rule: str) -> DataFrame:
    """silver.entities as passed, or loaded from the session catalog
    (``LB_ICEBERG_CATALOG`` . ``LB_FINANCIAL_SILVER_ENTITIES``) when None.

    An unreadable table raises RuleSkipped("no-customer-master"): a
    customer-scoped rule cannot tell its population, and "ran, 0 alerts"
    would read as 0% recall.
    """
    import os

    from pyspark.sql.utils import AnalysisException

    if silver_entities is not None:
        return silver_entities
    catalog = os.environ.get("LB_ICEBERG_CATALOG", "lakehouse")
    table = os.environ.get("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
    try:
        return spark.table(f"{catalog}.{table}")
    except AnalysisException as e:
        raise RuleSkipped(
            "no-customer-master", f"{rule}: {catalog}.{table} not readable ({e})"
        ) from None


def _customer_ids(spark, silver_entities: DataFrame | None, rule: str) -> DataFrame:
    """entity_id of every customer of the reporting FI (is_customer true).

    The same test the TM layer applies (tm_operations.build_alert_inputs), so
    an entity missing from silver.entities, or with a NULL flag, is not a
    customer. Skips, rather than returning an empty set, when silver.entities
    has no is_customer column or no customer at all (a corpus from before
    KYC, or a KYC join that matched nothing): every customer-scoped rule
    would otherwise run and report 0 alerts.
    """
    ents = _entities_frame(spark, silver_entities, rule)
    if "is_customer" not in ents.columns:
        raise RuleSkipped("no-kyc", f"{rule}: silver.entities has no is_customer column")
    # No distinct(): the left semi join in _customers_only is duplicate-safe,
    # and a distinct would add an aggregate shuffle of every customer per rule.
    # take(1) stops at the first partition holding a customer.
    cust = ents.filter(col("is_customer") == lit(True)).select("entity_id")
    if not cust.take(1):
        raise RuleSkipped("no-customers", f"{rule}: no entity in silver.entities is a customer")
    return cust


def _customers_only(alerts: DataFrame, customers: DataFrame) -> DataFrame:
    """Alerts whose subject is a customer. A semi join keeps the alert
    columns and their order (gold_finalize inserts positionally)."""
    return alerts.join(customers, "entity_id", "left_semi").select(*alerts.columns)


def _suspicious_amount_expr():
    """Boolean Column: txn amount is in [0.9 x threshold, threshold] for its
    currency (see _STRUCTURING_THRESHOLDS for the provenance of both)."""
    when_expr = None
    for ccy, thr in _STRUCTURING_THRESHOLDS.items():
        floor = thr * 0.9
        cond = (col("txn_currency") == lit(ccy)) & (col("txn_amount").between(floor, thr))
        when_expr = cond if when_expr is None else (when_expr | cond)
    return when_expr


def _alert_frame(
    df: DataFrame,
    *,
    rule_id,
    entity_id,
    related_txn_ids,
    related_entity_ids,
    alert_ts,
    alert_score,
    priority,
    alert_type,
    run_id,
    narrative,
    evidence,
) -> DataFrame:
    """``df`` projected to gold.alerts: every rule returns through here, so
    no rule chooses the column order (the driver's INSERT is positional).

    Each keyword is a Column; ``rule_id`` and ``run_id`` may also be a str.
    Keyword-only, so a column left out is a TypeError, which the driver
    records as the rule's error, never as zero alerts. The helper supplies
    ``alert_id`` (a uuid), the rule and model versions, ``status`` OPEN, a
    NULL ``disposition`` and ``detected_ts`` (wall clock at rule execution:
    detection time in batch, the far end of time to detect in continuous).
    Every value is cast to its ALERT_COLUMNS type and named.
    """
    if isinstance(rule_id, str):
        rule_id = lit(rule_id)
    if isinstance(run_id, str):
        run_id = lit(run_id)
    values = {
        "alert_id": expr("uuid()"),
        "rule_id": rule_id,
        "rule_version": lit(RULE_VERSION),
        "model_id": lit(MODEL_ID),
        "model_version": lit(MODEL_VERSION),
        "entity_id": entity_id,
        "related_txn_ids": related_txn_ids,
        "related_entity_ids": related_entity_ids,
        "alert_ts": alert_ts,
        "alert_score": alert_score,
        "priority": priority,
        "status": lit("OPEN"),
        "disposition": lit(None),
        "alert_type": alert_type,
        "run_id": run_id,
        "narrative": narrative,
        "evidence": evidence,
        "detected_ts": current_timestamp(),
    }
    return df.select(
        *[values[name].cast(ddl_type).alias(name) for name, ddl_type, _ in ALERT_COLUMNS]
    )


def w2_structuring(
    silver_txns: DataFrame,
    threshold_count: int = 3,
    window_hours: int = 24,
    per_beneficiary: bool = True,
    max_txns_per_alert: int = 1000,
    silver_entities: DataFrame | None = None,
    run_id: str = "unknown",
) -> DataFrame:
    """Structuring: N>=threshold_count structuring-band transactions within
    window_hours, aggregated two ways.

    - originator (alert_type ``structuring``): one account making the
      transactions. Grouped by (originator, currency, tumbling window).
    - beneficiary (alert_type ``structuring_beneficiary``): one account
      receiving them from at least threshold_count different senders in
      the window, the classic multiple-depositor (smurfing) scenario. Fewer
      senders is the originator kind's case: with a two-sender minimum, one
      sender structuring three times plus any other in-band credit raised
      both kinds on the same transactions. No currency key: each
      sender pays in its own account currency, so a currency split would
      let smurfs in different currencies evade; each credit is still tested
      against its own currency's band.

      The beneficiary kind uses a sliding window: one candidate window
      [t, t + window_hours) per credit, keeping only windows that are not
      contained in the previous credit's window, then merging overlapping
      qualifying windows into bursts, cut into chunks of one window length
      of anchors: one alert per chunk, spanning at most two windows, with
      alert_ts the chunk's last credit. related_txn_ids is capped at
      max_txns_per_alert (by uetr sort order; the narrative carries the full
      count). Tumbling buckets miss bursts that straddle
      a boundary (eight credits split 2/2/2/2 across four UTC days never
      reach 3 in one bucket).

    Both kinds share the band and count, and both are W2_structuring
    alerts; ``evidence['aggregation']`` names the kind.

    Customer-scoped (CUSTOMER_SCOPED_RULES): an alert is kept only when its
    subject (the originator, or the collecting beneficiary) is a customer.
    Counting is unchanged, so a customer receiving from non-customer senders
    still alerts, and the senders stay in related_entity_ids.

    Thresholds (R2 provenance): threshold_count 3 and the 24 h window are
    self-chosen. FinCEN and FFIEC describe structuring by its intent
    (transactions broken up to stay under the reporting threshold) and give
    no count or window; "3 in-band transactions in a day" is our reading,
    not a cited figure, and belongs in the threshold-cliff check with the
    90% band floor.

    Args:
        silver_txns: DataFrame of silver.transactions schema.
        threshold_count: minimum count of structuring-band txns in the
            window to trigger an alert.
        window_hours: window length.
        per_beneficiary: also emit the beneficiary aggregation.
        silver_entities: silver.entities (is_customer); loaded from the
            session catalog when None.
        run_id: opaque id for the detection run, written into every
            alert row.

    Returns:
        DataFrame with the gold.alerts schema.
    """
    from pyspark.sql.functions import lag
    from pyspark.sql.functions import window as window_

    customers = _customer_ids(silver_txns.sparkSession, silver_entities, "W2")
    suspicious = silver_txns.filter(_suspicious_amount_expr()).select(
        col("uetr"),
        col("originator_id"),
        col("beneficiary_id"),
        col("txn_timestamp"),
        col("txn_currency"),
        _epoch_micros(silver_txns).alias("_t"),
    )
    win = window_(col("txn_timestamp"), f"{window_hours} hours")

    by_orig = (
        suspicious.groupBy(col("originator_id").alias("entity_id"), col("txn_currency"), win)
        .agg(
            count(lit(1)).alias("suspicious_count"),
            collect_list("uetr").alias("related_txn_ids"),
            collect_set("beneficiary_id").alias("_related_entity_ids"),
            min_("txn_timestamp").alias("first_ts"),
            max_("txn_timestamp").alias("last_ts"),
        )
        .filter(col("suspicious_count") >= threshold_count)
        .select(
            "entity_id",
            "suspicious_count",
            "related_txn_ids",
            "_related_entity_ids",
            "first_ts",
            "last_ts",
            lit("structuring").alias("_type"),
            lit("originator").alias("_aggregation"),
            expr(
                "concat('Entity ', cast(entity_id as string), ' made ', "
                "cast(suspicious_count as string), ' structuring-band ', txn_currency, "
                "' transactions between ', cast(first_ts as string), ' and ', "
                "cast(last_ts as string))"
            ).alias("_narrative"),
        )
    )
    windowed = by_orig
    if per_beneficiary:
        window_us = int(window_hours) * 3_600_000_000
        ordered = Window.partitionBy("beneficiary_id").orderBy("_t", "uetr")
        frame = Window.partitionBy("beneficiary_id").orderBy("_t").rangeBetween(0, window_us - 1)
        anchored = (
            suspicious.withColumn("_rn", row_number().over(ordered))
            .select(
                col("beneficiary_id").alias("entity_id"),
                col("_rn"),
                col("uetr"),
                col("_t"),
                count(lit(1)).over(frame).alias("suspicious_count"),
                collect_list("uetr").over(frame).alias("related_txn_ids"),
                collect_set("originator_id").over(frame).alias("_related_entity_ids"),
                col("txn_timestamp").alias("first_ts"),
                max_("txn_timestamp").over(frame).alias("last_ts"),
                max_("_rn").over(frame).alias("_last_rn"),
            )
            # A window is contained in the previous credit's window exactly
            # when both end at the same credit.
            .withColumn(
                "_prev_last",
                lag("_last_rn").over(Window.partitionBy("entity_id").orderBy("_rn")),
            )
            .filter(col("_prev_last").isNull() | (col("_last_rn") > col("_prev_last")))
        )
        qualifying = anchored.filter(
            (col("suspicious_count") >= threshold_count)
            & (size(col("_related_entity_ids")) >= threshold_count)
        ).withColumn("_end_t", expr(f"_t + {window_us - 1}"))
        # Overlapping qualifying windows are one burst: a steady stream of
        # band-sized credits would otherwise raise one alert per credit,
        # each repeating the previous one's transactions.
        by_start = Window.partitionBy("entity_id").orderBy("_t", "_rn")
        bursts = (
            qualifying.withColumn(
                "_prev_end",
                max_("_end_t").over(by_start.rowsBetween(Window.unboundedPreceding, -1)),
            )
            .withColumn(
                "_new", (col("_prev_end").isNull() | (col("_t") > col("_prev_end"))).cast("int")
            )
            .withColumn("_burst", sum_("_new").over(by_start))
            # A burst is cut into window-length chunks of anchors, so a
            # busy account's months-long stream is not one alert whose
            # alert_ts (its end) lies weeks after the credits of interest.
            # Each alert then spans at most two windows.
            .withColumn(
                "_chunk",
                (
                    (col("_t") - min_("_t").over(Window.partitionBy("entity_id", "_burst")))
                    / lit(window_us)
                ).cast("long"),
            )
            .groupBy("entity_id", "_burst", "_chunk")
            .agg(
                array_sort(array_distinct(expr("flatten(collect_list(related_txn_ids))"))).alias(
                    "_all_txns"
                ),
                # Sorted before the cut, so the kept senders are the same on
                # every run.
                expr(
                    f"slice(array_sort(array_distinct(flatten(collect_list("
                    f"_related_entity_ids)))), 1, {int(max_txns_per_alert)})"
                ).alias("_related_entity_ids"),
                min_("first_ts").alias("first_ts"),
                max_("last_ts").alias("last_ts"),
            )
            .withColumn("suspicious_count", size(col("_all_txns")))
            .withColumn("related_txn_ids", expr(f"slice(_all_txns, 1, {int(max_txns_per_alert)})"))
        )
        by_bene = bursts.select(
            "entity_id",
            "suspicious_count",
            "related_txn_ids",
            "_related_entity_ids",
            "first_ts",
            "last_ts",
            lit("structuring_beneficiary").alias("_type"),
            lit("beneficiary").alias("_aggregation"),
            expr(
                "concat('Entity ', cast(entity_id as string), ' received ', "
                "cast(suspicious_count as string), ' structuring-band transactions from ', "
                "cast(size(_related_entity_ids) as string), ' senders between ', "
                "cast(first_ts as string), ' and ', cast(last_ts as string))"
            ).alias("_narrative"),
        )
        windowed = windowed.unionByName(by_bene)

    alerts = _alert_frame(
        windowed,
        rule_id=lit("W2_structuring"),
        entity_id=col("entity_id"),
        # Deduplicate defensively (Spark collect_list preserves duplicates).
        related_txn_ids=array_distinct(col("related_txn_ids")),
        # Cast the set -> list of BIGINT for the array<bigint> DDL column.
        related_entity_ids=expr("cast(_related_entity_ids as array<bigint>)"),
        alert_ts=col("last_ts"),
        # Alert score: 0.5 baseline + 0.05 * (count - threshold), capped 0.95.
        alert_score=expr(
            f"least(0.95, 0.5 + (suspicious_count - {int(threshold_count)}) * 0.05)"
        ).cast("double"),
        priority=when(col("suspicious_count") >= 6, lit("HIGH"))
        .when(col("suspicious_count") >= 4, lit("MED"))
        .otherwise(lit("LOW")),
        alert_type=col("_type"),
        run_id=lit(run_id),
        narrative=col("_narrative"),
        # txn_total is the alert's full count of in-band payments;
        # txns_truncated says the beneficiary kind's cap cut related_txn_ids
        # (the originator kind is never cut).
        evidence=map_from_arrays(
            array(
                lit("rule"),
                lit("threshold"),
                lit("window_hours"),
                lit("aggregation"),
                lit("txn_total"),
                lit("txns_truncated"),
            ),
            array(
                lit("W2_structuring"),
                lit(str(threshold_count)),
                lit(str(window_hours)),
                col("_aggregation"),
                col("suspicious_count").cast("string"),
                (
                    (col("_aggregation") == lit("beneficiary"))
                    & (col("suspicious_count") > lit(int(max_txns_per_alert)))
                ).cast("string"),
            ),
        ),
    )
    return _customers_only(alerts, customers)


# ---------------------------------------------------------------------------
# Temporal path search shared by W3 (cycles) and W17 (open layering chains).
# ---------------------------------------------------------------------------

# Path-search budget. Every row the searches hold on an executor (edges, the
# step frame, the live path levels) counts against it, and each level is
# estimated before it is built, so the search skips with
# RuleSkipped("path-cap") instead of filling the stage's scratch. The budget is
# in rows, from bytes:
#   1. LB_PATH_SEARCH_MAX_ROWS, when set to a positive integer;
#   2. the running job's scratch: spark.executor.instances x the executor
#      scratch PVC sizeLimit x PATH_SEARCH_SCRATCH_SHARE, divided by
#      PATH_SEARCH_BYTES_PER_ROW;
#   3. PATH_SEARCH_MAX_PATHS (gold-finalize at scale 10, 4 x 100 Gi).
# PATH_SEARCH_BYTES_PER_ROW is a footprint, not a row size: peak local scratch
# (cached and checkpointed blocks plus shuffle files, including the sampling
# joins that estimate each level) per held row. Measured with
# spark.local.dir polled every 0.5 s on a scale-0.25 corpus (6.67M
# transfers): W3 peaked at 4.45 GB holding 26.2M rows (170 B per row), W17
# at 5.25 GB holding 20.2M (260 B, including files W3 had not yet released;
# the rules run back to back in one stage). 260 is used. The share leaves
# 30% of scratch for the silver scan and the alert write. Projected at
# scale 10, W3 holds about 1.05B rows at peak against a 1.16B budget on
# gold-finalize (4 x 100 Gi); gold-refresh (2 x 100 Gi) skips it there. A
# row's width grows with depth (more uetrs per path); the flat figure was
# measured over all levels together. Before levels were checkpointed and
# released, a review measured 430 B. These are resource guards, not
# detection thresholds.
PATH_SEARCH_BYTES_PER_ROW = 260
PATH_SEARCH_SCRATCH_SHARE = 0.7
PATH_SEARCH_MAX_PATHS = int(4 * 100 * 2**30 * PATH_SEARCH_SCRATCH_SHARE / PATH_SEARCH_BYTES_PER_ROW)
PATH_SEARCH_MAX_ROWS_ENV = "LB_PATH_SEARCH_MAX_ROWS"
PATH_SEARCH_NO_PVC_BYTES = 20 * 2**30
# Partition count of the extension join (the step frame and each level). At
# the job's shuffle partitions (32 at scale 10) a level of 260M rows put ~8M
# rows (~2 GB) in every task and every cached block. The count is sized so a
# level of up to PATH_SEARCH_LEVEL_EDGE_RATIO x edges rows (the ratio W3 held
# at peak on the scale-0.25 corpus, rounded up) averages
# PATH_SEARCH_ROWS_PER_PARTITION rows per partition, and is never below the
# job's own count. Scale 10 (266.7M transfers) gets 534 partitions. The cap
# keeps a stage near the ~2,000 tasks the driver's status listener handles
# at scale 100 (LB-049); scale 100 reaches it.
PATH_SEARCH_ROWS_PER_PARTITION = 2_000_000
PATH_SEARCH_LEVEL_EDGE_RATIO = 4
PATH_SEARCH_MAX_PARTITIONS = 2048
# Files per written W3 cycles frame: cycles are a sliver of their level.
PATH_SEARCH_RESULT_FILES = 16
# Spill directories of drivers that died before their cleanup are swept at
# the next detection run once their newest file is this old. Well above any
# gold job's run time, so a live replay driver's files are never touched.
PATH_SEARCH_STALE_HOURS = 24
_SCRATCH_SIZE_CONF = (
    "spark.kubernetes.executor.volumes.persistentVolumeClaim.spark-local-dir-1.options.sizeLimit"
)


def _parse_size_bytes(text: str | None) -> int | None:
    """Kubernetes quantity ("100Gi", "500G", "512Mi") in bytes, or None."""
    import re

    m = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGTP]i?)?\s*", text or "")
    if not m:
        return None
    unit = m.group(2) or ""
    power = "KMGTP".index(unit[0]) + 1 if unit else 0
    base = 1024 if unit.endswith("i") else 1000
    return int(float(m.group(1)) * base**power)


def path_search_budget_rows(spark) -> int:
    """Row budget for W3/W17 in the running job (see the table above)."""
    import os

    raw = os.environ.get(PATH_SEARCH_MAX_ROWS_ENV, "").strip()
    if raw:
        try:
            if int(raw) > 0:
                return int(raw)
        except ValueError:
            pass
        print(f"[path-search] ignoring {PATH_SEARCH_MAX_ROWS_ENV}={raw!r}")
    try:
        conf = spark.sparkContext.getConf()
        executors = int(conf.get("spark.executor.instances", "0") or 0)
        scratch = _parse_size_bytes(conf.get(_SCRATCH_SIZE_CONF, None))
    except Exception:  # noqa: BLE001
        executors, scratch = 0, None
    if executors > 0:
        # No scratch PVC (platform.storage.scratch off, e.g. non-Portworx):
        # Spark's local dirs are then an emptyDir with no size limit on the
        # node's ephemeral storage, whose size the job cannot see. 20 Gi per
        # executor is a conservative guess, not a known limit.
        per_executor = scratch or PATH_SEARCH_NO_PVC_BYTES
        return int(executors * per_executor * PATH_SEARCH_SCRATCH_SHARE / PATH_SEARCH_BYTES_PER_ROW)
    return PATH_SEARCH_MAX_PATHS


def path_search_partitions(spark, n_edges: int) -> int:
    """Partitions for the step frame and the extension join (see the table)."""
    base = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
    want = -(-PATH_SEARCH_LEVEL_EDGE_RATIO * max(0, n_edges) // PATH_SEARCH_ROWS_PER_PARTITION)
    return max(base, min(PATH_SEARCH_MAX_PARTITIONS, want))


def _path_spill_root(spark) -> str | None:
    """Where path levels are written, or None (local checkpoints, tests).

    Under ``LB_GOLD_URI`` and the driver's application id, so a replay
    driver of the same deployment never shares or deletes this one's files.
    """
    import os

    base = os.getenv("LB_GOLD_URI")
    if not base:
        return None
    app = spark.sparkContext.applicationId
    return f"{base.rstrip('/')}/_checkpoints/paths/{app}"


def _delete_uri(spark, path: str, who: str) -> None:
    """Recursively delete ``path`` through the Hadoop FileSystem. Best effort."""
    try:
        jvm = spark._jvm  # type: ignore[attr-defined]
        hconf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), hconf)
        p = jvm.org.apache.hadoop.fs.Path(path)
        if fs.exists(p):
            fs.delete(p, True)
    except Exception as e:  # noqa: BLE001
        print(f"[{who}] cleanup of {path} failed: {e}")


def sweep_stale_path_spill(spark, max_age_hours: float = PATH_SEARCH_STALE_HOURS) -> int:
    """Delete other drivers' spill directories whose newest file is older than
    ``max_age_hours``: a driver killed mid-search (OOM, eviction, retry) never
    ran its cleanup, and every retry has a new application id. Returns the
    number of directories removed. Best effort."""
    import time

    root = _path_spill_root(spark)
    if not root:
        return 0
    parent = root.rsplit("/", 1)[0]
    cutoff_ms = (time.time() - max_age_hours * 3600) * 1000
    removed = 0
    try:
        jvm = spark._jvm  # type: ignore[attr-defined]
        hconf = spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(parent), hconf)
        ppath = jvm.org.apache.hadoop.fs.Path(parent)
        if not fs.exists(ppath):
            return 0
        for st in fs.listStatus(ppath):
            p = st.getPath()
            # By name: Hadoop prints file:/// URIs as file:/, so a whole-path
            # comparison would miss this driver's own directory.
            if not st.isDirectory() or p.getName() == root.rstrip("/").rsplit("/", 1)[1]:
                continue
            # Directory times are not reliable on S3; the files' are. Every
            # write first touches the driver's _alive file, so a driver with a
            # write in flight (its part files are invisible on S3 until the
            # upload closes) always shows a fresh file. A directory with no
            # file at all is never deleted: it may be a write just starting.
            try:
                newest, seen = 0, False
                it = fs.listFiles(p, True)
                while it.hasNext():
                    seen = True
                    newest = max(newest, it.next().getModificationTime())
                    if newest >= cutoff_ms:
                        break
                if seen and newest < cutoff_ms:
                    fs.delete(p, True)
                    removed += 1
            except Exception as e:  # noqa: BLE001 -- e.g. removed by its owner meanwhile
                print(f"[path-search] stale spill sweep skipped {p.toString()}: {e}")
    except Exception as e:  # noqa: BLE001
        print(f"[path-search] stale spill sweep under {parent} failed: {e}")
    if removed:
        print(f"[path-search] removed {removed} stale spill directories under {parent}")
    return removed


def cleanup_path_search_spill(spark) -> None:
    """Delete this driver's path-search levels and results. Call once the
    rule's alerts are written (the alerts frame reads the result files)."""
    root = _path_spill_root(spark)
    if root:
        _delete_uri(spark, root, "path-search")


class _PathBudget:
    """Rows one path search holds, against its budget.

    ``admit`` checks the estimate first, so a level that would take the held
    rows over the budget is never built, then materialises the frame
    (``cut``) and checks the real count.

    Materialising drops the frame's lineage, which frees the shuffles behind
    it once unreferenced. With ``LB_GOLD_URI`` set (every cluster run) a frame
    is written as Parquet under _path_spill_root and read back. A local
    checkpoint, used before, kept each block only on the executor that built
    it: at scale 10 gold executors were OOM-killed and the next level failed
    with CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND, since a lost block cannot be
    recomputed. Why the executors exceeded their 40Gi limit is not settled
    from the driver log alone: exit 137 is the container limit, and cached
    blocks at MEMORY_AND_DISK are evicted rather than killed. Writing the
    levels removes the fatal dependency either way and takes them out of
    executor memory and scratch; the writes add S3A upload buffers
    (fast.upload.buffer=bytebuffer, about 4 tasks x 4 blocks x 64 MB per
    executor) to the 8g overhead. Without ``LB_GOLD_URI`` (unit tests) the
    local checkpoint remains.

    The budget still counts a written level as held. That overstates the
    scratch it takes (only its shuffle, when it is the next level's input,
    lands on scratch), so the skip stays where it was; relaxing it needs a
    scratch measurement on the cluster.
    """

    def __init__(self, spark, rule: str, max_rows: int) -> None:
        import uuid

        self.spark = spark
        self.rule = rule
        self.max_rows = max_rows
        self.live = 0
        root = _path_spill_root(spark)
        self.spill = f"{root}/{rule}-{uuid.uuid4().hex[:12]}" if root else None
        self._written: dict[str, str] = {}

    def _skip(self, what: str, rows: int) -> None:
        raise RuleSkipped(
            "path-cap",
            f"{self.rule} path search {what} {rows} rows with {self.live} held "
            f"(budget {self.max_rows}; set {PATH_SEARCH_MAX_ROWS_ENV} when the "
            "stage has the scratch for it)",
        )

    def check(self, estimate: float, label: str) -> None:
        if self.live + estimate > self.max_rows:
            self._skip(f"estimates {label} at", int(estimate))

    def add(self, n: int, label: str, estimate: float | None = None) -> None:
        self.live += n
        est = "n/a" if estimate is None else str(int(estimate))
        print(
            f"[{self.rule}] {label} rows={n} estimate={est} held={self.live} budget={self.max_rows}"
        )
        if self.live > self.max_rows:
            self._skip(f"holds {label} of", n)

    def cut(self, df: DataFrame, label: str, files: int | None = None) -> DataFrame:
        """Materialise ``df`` without its lineage (see the class docstring).
        ``files`` repartitions a small result first, so it is not written as
        one file per join partition."""
        if not self.spill:
            return df.localCheckpoint(eager=True)
        if files:
            df = df.repartition(files)
        self._touch_alive()
        path = f"{self.spill}/{label.replace(' ', '-')}"
        schema = df.schema
        hconf = self.spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
        key = "mapreduce.fileoutputcommitter.algorithm.version"
        prior = hconf.get(key)
        # Algorithm 2 commits each task's files straight into the output
        # directory: one S3 copy per file, made by the tasks in parallel,
        # instead of a second job-level rename of the whole level from the
        # driver. A failed write leaves partial files only here, and they are
        # deleted with the directory.
        hconf.set(key, "2")
        try:
            df.write.mode("overwrite").parquet(path)
        finally:
            if prior is None:
                hconf.unset(key)
            else:
                hconf.set(key, prior)
        self._written[label] = path
        return self.spark.read.schema(schema).parquet(path)

    def _touch_alive(self) -> None:
        """Rewrite this driver's liveness file (see sweep_stale_path_spill)."""
        root = self.spill.rsplit("/", 1)[0]
        try:
            jvm = self.spark._jvm  # type: ignore[attr-defined]
            hconf = self.spark._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
            p = jvm.org.apache.hadoop.fs.Path(f"{root}/_alive")
            jvm.org.apache.hadoop.fs.FileSystem.get(p.toUri(), hconf).create(p, True).close()
        except Exception as e:  # noqa: BLE001
            print(f"[{self.rule}] could not write {root}/_alive: {e}")

    def admit(self, df: DataFrame, estimate: float, label: str):
        """Returns ``(materialised_df, rows)``."""
        self.check(estimate, label)
        cut = self.cut(df, label)
        n = cut.count()
        self.add(n, label, estimate)
        return cut, n

    def release(self, n: int, label: str | None = None) -> None:
        """Forget ``n`` held rows whose frame the caller no longer references,
        and delete that frame's files when it was written (``label``).

        Local checkpoint blocks and shuffle files are removed by Spark's
        ContextCleaner once the driver-side objects are collected; the GC
        request makes that happen now rather than at the next periodic GC.
        """
        self.live -= n
        path = self._written.pop(label, None) if label else None
        if path:
            _delete_uri(self.spark, path, self.rule)
        try:
            self.spark.sparkContext._jvm.System.gc()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    def discard(self) -> None:
        """Delete every file this search wrote (it skipped or failed)."""
        if self.spill:
            _delete_uri(self.spark, self.spill, self.rule)
        self._written.clear()


def _epoch_micros(df: DataFrame):
    """txn_timestamp as epoch microseconds, independent of the session zone.

    TIMESTAMP (silver): unix_micros. TIMESTAMP_NTZ (raw generator Parquet):
    read as UTC wall-clock from calendar fields; a cast or timestampdiff
    would apply the session zone and, outside UTC, shift or collapse wall
    times inside a DST change.
    """
    from pyspark.sql.types import TimestampNTZType

    if isinstance(df.schema["txn_timestamp"].dataType, TimestampNTZType):
        return expr(
            "datediff(to_date(txn_timestamp), DATE'1970-01-01') * 86400000000L"
            " + (hour(txn_timestamp) * 3600L + minute(txn_timestamp) * 60L) * 1000000L"
            " + cast(extract(SECOND FROM txn_timestamp) * 1000000 AS BIGINT)"
        )
    return expr("unix_micros(txn_timestamp)")


def _flow_edges(silver_txns: DataFrame, with_amount: bool = False) -> DataFrame:
    """Directed transfer edges for the path searches.

    ``t`` is epoch microseconds, so two hops inside the same second still
    order. For TIMESTAMP input (silver) it is unix_micros, which does not
    depend on the session time zone. TIMESTAMP_NTZ input (raw generator
    Parquet) is read as UTC wall-clock by arithmetic, not by a cast, since a
    cast would apply the session zone and, outside UTC, shift or collapse
    wall times inside a DST change. With ``with_amount`` only transfers with
    a positive USD amount are kept (amount continuity needs one).
    """
    t = _epoch_micros(silver_txns)
    cols = [
        col("uetr"),
        col("originator_id").alias("src"),
        col("beneficiary_id").alias("dst"),
        t.alias("t"),
        col("txn_timestamp").alias("ts"),
    ]
    if with_amount:
        cols.append(col("txn_amount_usd").cast("double").alias("amt"))
    edges = silver_txns.select(*cols).filter(
        col("src").isNotNull() & col("dst").isNotNull() & (col("src") != col("dst"))
    )
    if with_amount:
        edges = edges.filter(col("amt").isNotNull() & (col("amt") > lit(0)))
    return edges


def _search_frames(
    silver_txns: DataFrame,
    rule: str,
    bucket_us: int,
    max_out_degree: int,
    max_edges: int,
    max_paths: int | None,
    with_amount: bool,
):
    """Edges and the extension (step) frame, both held and budgeted.

    Returns ``(budget, edges, n_edges, step, n_step, parts)``. ``edges`` is
    persisted (the caller unpersists it once level 2 is built); ``step`` is
    persisted for the whole search.

    The step frame holds every non-hub transfer. Hubs are accounts sending
    more than ``max_out_degree`` transfers in any hop-window bucket (payment
    processors); they are excluded as intermediaries so one processor does
    not multiply every path. max_out_degree=200 per week is self-chosen.

    Each transfer appears twice, under bucket ``floor(t / hop)`` and the one
    before it. A path ending at time ``t_last`` in bucket ``b`` can only be
    extended by a transfer in ``(t_last, t_last + hop]``, whose bucket is
    ``b`` or ``b + 1``; joining on ``(node, b)`` therefore finds every valid
    extension exactly once. Without the bucket the join key is the node
    alone, and a busy beneficiary pairs every transfer it received over 60
    months with every one it sent before the time filter runs. The frame is
    hash-partitioned on the join key before it is persisted, so each level's
    join shuffles only the path side.

    W3 and W17 each build their own edges and step frames. Sharing them
    within one detection pass would save one silver scan and one step build
    per pass, but the driver isolates rules (and clears the cache after each
    one), so it is not done.
    """
    from pyspark import StorageLevel

    spark = silver_txns.sparkSession
    budget = _PathBudget(
        spark, rule, path_search_budget_rows(spark) if max_paths is None else max_paths
    )
    # DISK_ONLY: at scale 10 the step frame is ~530M rows. Cached at
    # MEMORY_AND_DISK it filled the executors' storage memory, which the
    # extension joins' sorts then had to evict. Both frames keep their
    # lineage, so a block lost with an executor is recomputed from silver.
    edges = _flow_edges(silver_txns, with_amount).persist(StorageLevel.DISK_ONLY)
    n_edges = edges.count()
    if n_edges > max_edges:
        raise RuleSkipped(
            "edge-cap",
            f"edges={n_edges} max={max_edges} (raise max_edges to run {rule} at this scale)",
        )
    budget.add(n_edges, "edges")

    bucket = (col("t") / lit(bucket_us)).cast("long")
    hubs = (
        edges.groupBy("src", bucket.alias("_w"))
        .agg(count(lit(1)).alias("_n"))
        .filter(col("_n") > max_out_degree)
        .select(col("src").alias("hub"))
        .distinct()
    )
    non_hub = edges.join(hubs, edges["src"] == hubs["hub"], "left_anti")
    renamed = [
        col("uetr").alias("e_uetr"),
        col("src").alias("e_src"),
        col("dst").alias("e_dst"),
        col("t").alias("e_t"),
        col("ts").alias("e_ts"),
    ]
    if with_amount:
        renamed.append(col("amt").alias("e_amt"))
    base = non_hub.select(*renamed)
    e_bucket = (col("e_t") / lit(bucket_us)).cast("long")
    parts = path_search_partitions(spark, n_edges)
    step = (
        base.withColumn("_b", e_bucket)
        .unionByName(base.withColumn("_b", e_bucket - lit(1)))
        .repartition(parts, "e_src", "_b")
    )
    budget.check(2 * n_edges, "step")
    step = step.persist(StorageLevel.DISK_ONLY)
    n_step = step.count()
    budget.add(n_step, "step", 2 * n_edges)
    print(f"[{rule}] extension join partitions={parts}")
    return budget, edges, n_edges, step, n_step, parts


@contextlib.contextmanager
def _copartition_on_key_subset(spark):
    """Let the extension join reuse the step frame's partitioning when the
    join has more keys than it is partitioned on.

    On the last hop both rules keep only extensions that return to the start,
    and the optimizer adds ``e_dst == start`` to the join keys. With
    spark.sql.requireAllClusterKeysForCoPartition at its default (true), the
    step frame's partitioning on (e_src, _b) then no longer counts, and Spark
    reshuffled the whole step frame and the paths at the job's shuffle
    partitions (32 at scale 10) for the last and often largest level. A
    partitioning on a subset of the keys still places every matching pair in
    one partition. Set only while the levels are built; every join runs
    eagerly inside.
    """
    key = "spark.sql.requireAllClusterKeysForCoPartition"
    prior = spark.conf.get(key, None)
    spark.conf.set(key, "false")
    try:
        yield
    finally:
        if prior is None:
            spark.conf.unset(key)
        else:
            spark.conf.set(key, prior)


def _extend_paths(paths: DataFrame, step: DataFrame, bucket_us: int, parts: int) -> DataFrame:
    """Join each path to the transfers that can follow its last hop.

    A hop must start strictly after the previous one. Two transfers with the
    same microsecond timestamp therefore never chain, in either order: the
    data cannot say which came first, and neither rule guesses.

    The paths are hash-partitioned on the join key into the step frame's
    ``parts`` partitions, so the persisted step frame is joined in place and
    each task takes one of ``parts`` slices of the level.
    """
    p_bucket = (col("t_last") / lit(bucket_us)).cast("long")
    return (
        paths.withColumn("_pb", p_bucket)
        .repartition(parts, "end", "_pb")
        .join(step, (col("end") == col("e_src")) & (col("_pb") == col("_b")), "inner")
        .filter(col("e_t") > col("t_last"))
        .filter(col("e_t") <= col("t_last") + lit(bucket_us))
        .drop("_pb", "_b")
    )


def _sampled_size(paths: DataFrame, n_paths: int, extend) -> float:
    """Estimated row count of ``extend(paths)`` from a sample of the paths."""
    if n_paths <= 0:
        return 0.0
    frac = min(1.0, 1_000_000 / n_paths)
    if frac >= 1.0:
        return float(extend(paths).count())
    return extend(paths.sample(False, frac, seed=17)).count() / frac


def _cut_small(
    df: DataFrame, budget: _PathBudget, label: str, files: int | None = None
) -> DataFrame:
    """Materialise a result frame (cycles, complete chains) and drop its
    lineage, so the levels it came from can be released. Its rows stay held
    until the rule ends and count against the budget. Written result files
    stay until cleanup_path_search_spill, after the alerts are written."""
    cut = budget.cut(df, label, files=files)
    budget.add(cut.count(), label)
    return cut


def _w3_levels(budget, edges, n_edges, step, paths, _ext, max_hops):
    """W3's level loop: the union of the cycles found at each level."""
    from pyspark.sql.functions import array_contains, concat

    n_paths = n_edges
    held_prev, prev_label = n_edges, None  # level 1 is the edge frame
    cycles = []
    for _hop in range(2, max_hops + 1):
        # The last level is only read for the transfers that close a cycle.
        final = _hop == max_hops

        def _build(p: DataFrame, final: bool = final) -> DataFrame:
            e = _ext(p)
            return e.filter(col("e_dst") == col("start")) if final else e

        # Estimated from a sample of the paths at every level: a growth
        # factor from earlier levels is wrong in both directions (a review
        # measured a 45x under-estimate and a skip on a level that was empty).
        est = _sampled_size(paths, n_paths, _build)
        level, n = budget.admit(_build(paths), est, f"level {_hop}")
        cycles.append(
            _cut_small(
                level.filter(col("e_dst") == col("start")).select(
                    col("start"),
                    concat(col("uetrs"), array(col("e_uetr"))).alias("uetrs"),
                    col("nodes"),
                    col("t_first"),
                    col("e_ts").alias("ts_last"),
                ),
                budget,
                f"cycles {_hop}",
                # Cycles are a sliver of their level. W17's complete chains are
                # not (at the last hop nearly the whole level), so they keep
                # the join's partitions rather than pay a shuffle into 16.
                files=PATH_SEARCH_RESULT_FILES,
            )
        )
        # The previous level (the edge frame, for level 2) is not read again.
        if _hop == 2:
            edges.unpersist()
        paths = None
        budget.release(held_prev, prev_label)
        held_prev, prev_label = n, f"level {_hop}"
        if final:
            break
        paths = level.filter(~array_contains(col("nodes"), col("e_dst"))).select(
            col("start"),
            col("e_dst").alias("end"),
            col("t_first"),
            col("e_t").alias("t_last"),
            concat(col("uetrs"), array(col("e_uetr"))).alias("uetrs"),
            concat(col("nodes"), array(col("e_dst"))).alias("nodes"),
        )
        n_paths = n
        level = None
    # The last level was only read for its cycles.
    budget.release(held_prev, prev_label)
    step.unpersist()

    found = cycles[0]
    for c in cycles[1:]:
        found = found.unionByName(c)
    return found


def w3_round_tripping(
    silver_txns: DataFrame,
    max_hops: int = 5,
    hop_window_hours: int = 168,
    total_window_days: int = 30,
    max_out_degree: int = 200,
    max_edges: int = 3_000_000_000,
    max_paths: int | None = None,
    run_id: str = "unknown",
) -> DataFrame:
    """Round-tripping: funds that return to their originator through 2 to
    ``max_hops`` transfers. Each hop must start after the previous one and
    within ``hop_window_hours`` of it, and the whole cycle within
    ``total_window_days``. Paths are simple (no entity repeats before the
    return).

    The earlier form only found A -> B -> A, which none of the planted cycle
    typologies contain (cycle and cross_border_cycle route A -> B -> C -> D
    -> A), so it could not detect its designated typology. No amount
    condition is applied. The windows, max_hops and the hub cut are
    self-chosen scenario parameters, not derived from the generator.

    Cost control: paths are extended hop by hop from every transfer, and
    each cycle is found once (starting from its earliest transfer, since hop
    times strictly increase). Entities sending more than ``max_out_degree``
    transfers in any hop window are treated as hubs (payment processors)
    and excluded as intermediaries. The extension join is keyed on (entity,
    hop-window bucket). Each level is checkpointed and the one before it
    released, so at most two levels are held. Above ``max_edges`` transfers
    the rule raises RuleSkipped("edge-cap"); when a level would take the held
    rows over the path-search budget (``max_paths`` rows, default
    path_search_budget_rows) it raises RuleSkipped("path-cap").

    Emits one alert per cycle: entity_id is the originator, related_txn_ids
    the transfers in hop order.
    """

    spark = silver_txns.sparkSession
    if max_hops < 2:
        print(f"[W3] max_hops={max_hops}: a round trip needs at least 2 transfers")
        return _empty_alerts_df(spark, run_id)
    hop_us = hop_window_hours * 3_600_000_000
    total_us = total_window_days * 86_400_000_000
    budget, edges, n_edges, step, _, parts = _search_frames(
        silver_txns, "W3", hop_us, max_out_degree, max_edges, max_paths, with_amount=False
    )

    paths = edges.select(
        col("src").alias("start"),
        col("dst").alias("end"),
        col("t").alias("t_first"),
        col("t").alias("t_last"),
        array(col("uetr")).alias("uetrs"),
        array(col("src"), col("dst")).alias("nodes"),
    )

    def _ext(p: DataFrame) -> DataFrame:
        return _extend_paths(p, step, hop_us, parts).filter(
            col("e_t") <= col("t_first") + lit(total_us)
        )

    try:
        with _copartition_on_key_subset(spark):
            found = _w3_levels(budget, edges, n_edges, step, paths, _ext, max_hops)
    except BaseException:
        budget.discard()
        raise
    alerts = found.withColumn("hops", size(col("uetrs")))
    return _alert_frame(
        alerts,
        rule_id=lit("W3_round_tripping"),
        entity_id=col("start"),
        related_txn_ids=col("uetrs"),
        related_entity_ids=col("nodes"),
        alert_ts=col("ts_last"),
        # Longer cycles are more deliberate. Bounded [0.6, 0.9].
        alert_score=expr("least(0.9, 0.5 + 0.1 * hops)").cast("double"),
        priority=when(col("hops") >= 4, lit("HIGH"))
        .when(col("hops") >= 3, lit("MED"))
        .otherwise(lit("LOW")),
        alert_type=lit("round_tripping"),
        run_id=lit(run_id),
        narrative=expr(
            "concat('Funds returned to entity ', cast(start as string), ' through ', "
            "cast(hops as string), ' transfers ending ', cast(ts_last as string))"
        ),
        evidence=map_from_arrays(
            array(
                lit("rule"),
                lit("hops"),
                lit("hop_window_hours"),
                lit("total_window_days"),
                lit("max_out_degree"),
            ),
            array(
                lit("W3_round_tripping"),
                col("hops").cast("string"),
                lit(str(hop_window_hours)),
                lit(str(total_window_days)),
                lit(str(max_out_degree)),
            ),
        ),
    )


def _w17_levels(budget, edges, n_edges, step, paths, _ext, _advance, min_hops, max_hops):
    """W17's level loop: the union of the complete chains of each level."""
    from pyspark.sql.functions import array_contains

    n_paths = n_edges
    held_prev, prev_label = n_edges, None  # level 1 is the edge frame
    complete = []  # complete chains of >= min_hops transfers, per level
    for hop in range(1, max_hops + 1):
        # ``paths`` holds chains of ``hop`` transfers; ``level`` their
        # one-transfer extensions. At max_hops only the extensions that
        # return to the start are needed (to drop returning chains).
        final = hop == max_hops

        def _build(p: DataFrame, final: bool = final) -> DataFrame:
            e = _ext(p)
            return e.filter(col("e_dst") == col("start")) if final else e

        # Sampled at every level; see w3_round_tripping.
        est = _sampled_size(paths, n_paths, _build)
        level, n = budget.admit(_build(paths), est, f"level {hop + 1}")
        if hop >= min_hops:
            returns = level.filter(col("e_dst") == col("start")).select("uetrs")
            done = paths.join(returns, "uetrs", "left_anti")
            if not final:
                onward = level.filter(~array_contains(col("nodes"), col("e_dst")))
                done = done.join(onward.select("uetrs"), "uetrs", "left_anti")
            complete.append(
                _cut_small(
                    done.select("uetrs", "nodes", "ts_last", "ts_q"), budget, f"complete {hop}"
                )
            )
            # Drop every reference to the previous level before releasing it.
            done = returns = onward = None
        if hop == 1:
            edges.unpersist()
        paths = None
        budget.release(held_prev, prev_label)
        held_prev, prev_label = n, f"level {hop + 1}"
        if final:
            break
        paths = _advance(level)
        n_paths = n
        level = None
    # The last level was only read for the chains that return to the start.
    budget.release(held_prev, prev_label)
    step.unpersist()

    chains = complete[0]
    for c in complete[1:]:
        chains = chains.unionByName(c)
    return chains


def w17_layering_chain(
    silver_txns: DataFrame,
    min_hops: int = 3,
    max_hops: int = 6,
    hop_window_hours: int = 168,
    min_forward_ratio: float = 0.8,
    max_forward_ratio: float = 1.0,
    max_out_degree: int = 200,
    max_edges: int = 3_000_000_000,
    max_paths: int | None = None,
    run_id: str = "unknown",
) -> DataFrame:
    """Layering chain: funds passed along an open chain of at least
    ``min_hops`` transfers A -> B -> C -> D, where each hop starts after the
    previous one and within ``hop_window_hours`` of it, forwards between
    ``min_forward_ratio`` and ``max_forward_ratio`` of the previous hop's USD
    amount, and the chain does not return to its start (that is W3's cycle).

    Thresholds (R2 provenance): min_forward_ratio 0.8, the seven-day hop
    window, min_hops 3 and max_hops 6 are self-chosen scenario parameters (0.8
    is the pass-through figure W4 also uses). None is derived from the
    generator's stack windows, hop gaps or skim range, and they belong in the
    threshold-cliff check. max_forward_ratio 1.0 is a physical bound, not a
    tuning choice: an account cannot pass on more than it received, so the
    cliff check must exempt it.

    Precision decision: on a corpus with amount continuity this rule is
    about 1% precise for stack (118 of 11,381 alerts touch a stack
    transfer on Lane A's 2fb9259 corpus at scale 0.25, and a review found 25
    of 2,191 at scale 0.05).
    That is deliberate and published as is. Tightening min_hops, adding a
    dwell limit or narrowing the ratio toward the generator's stack
    parameters (hop gaps of 6-54 h, a 1-10% skim) would be tuning the rule
    to the data (AML-GOALS R2). The planned structural cut is monitoring
    customer accounts only (P10 stage 0).

    Which chains are reported. Chains are searched from every transfer; a
    chain is complete when no transfer can extend it, or when it reaches
    ``max_hops`` (truncated; a longer run is then reported as overlapping
    windows, one per start). A complete chain whose end can send the funds
    back to its start is dropped. Of the rest:

    - a chain that is a strict suffix of another reported chain is dropped:
      it shows no transfer the longer one does not;
    - chains that differ only in their first transfer are merged into one
      alert. Several unrelated credits into one account can each carry the
      amount onward; they feed one pass-through, not several.

    Attribution: entity_id is the chain's first intermediary, the first
    account seen to receive funds and pass on 80-100% of them within the
    window. The first sender is only a payer: it may be innocent (an
    employer, a customer, a hub), and several payers can feed one chain, so
    alerting on it blames the wrong party. The senders stay in
    related_entity_ids.

    Transfers with equal timestamps never chain (see _extend_paths).

    Cost control is the same as W3: hubs excluded as intermediaries, a join
    keyed on (entity, hop-window bucket), checkpointed levels with at most
    two held, and the ``max_edges`` / path-search budget skips. Amount
    continuity prunes each level to the small share of onward transfers that
    carry the amount.
    """
    from pyspark.sql.functions import array_contains, concat, element_at
    from pyspark.sql.functions import slice as slice_

    spark = silver_txns.sparkSession
    min_hops = max(2, int(min_hops))
    if max_hops < min_hops:
        print(f"[W17] min_hops={min_hops} > max_hops={max_hops}: no chain can qualify")
        return _empty_alerts_df(spark, run_id)

    hop_us = hop_window_hours * 3_600_000_000
    budget, edges, n_edges, step, _, parts = _search_frames(
        silver_txns, "W17", hop_us, max_out_degree, max_edges, max_paths, with_amount=True
    )

    def _ext(p: DataFrame) -> DataFrame:
        e = _extend_paths(p, step, hop_us, parts)
        return e.filter(col("e_amt") >= col("amt_last") * lit(min_forward_ratio)).filter(
            col("e_amt") <= col("amt_last") * lit(max_forward_ratio)
        )

    def _advance(level: DataFrame) -> DataFrame:
        return level.filter(~array_contains(col("nodes"), col("e_dst"))).select(
            col("start"),
            col("e_dst").alias("end"),
            col("e_t").alias("t_last"),
            col("e_amt").alias("amt_last"),
            col("e_ts").alias("ts_last"),
            # When the chain first qualifies: the time of its min_hops-th
            # transfer. Used as alert_ts, the moment the rule could have
            # fired; later onward hops extend the chain but do not delay it.
            when(size(col("uetrs")) + lit(1) == lit(min_hops), col("e_ts"))
            .otherwise(col("ts_q"))
            .alias("ts_q"),
            concat(col("uetrs"), array(col("e_uetr"))).alias("uetrs"),
            concat(col("nodes"), array(col("e_dst"))).alias("nodes"),
        )

    paths = edges.select(
        col("src").alias("start"),
        col("dst").alias("end"),
        col("t").alias("t_last"),
        col("amt").alias("amt_last"),
        col("ts").alias("ts_last"),
        when(lit(False), col("ts")).alias("ts_q"),  # null, typed like ts
        array(col("uetr")).alias("uetrs"),
        array(col("src"), col("dst")).alias("nodes"),
    )
    try:
        with _copartition_on_key_subset(spark):
            chains = _w17_levels(
                budget, edges, n_edges, step, paths, _ext, _advance, min_hops, max_hops
            )
    except BaseException:
        budget.discard()
        raise
    # Every strict suffix (of length >= min_hops) of each complete chain.
    suffixes = (
        "transform(sequence(2, greatest(2, size(uetrs) - {m} + 1)), "
        "i -> slice(uetrs, i, size(uetrs) - i + 1))"
    ).replace("{m}", str(min_hops))
    covered = (
        chains.select(explode(expr(suffixes)).alias("s"))
        .filter(size(col("s")) >= lit(min_hops))
        .select(col("s").alias("uetrs"))
        .distinct()
    )
    kept = chains.join(covered, "uetrs", "left_anti")

    # Merge chains that share everything after their first transfer.
    n_t = size(col("uetrs"))
    merged = (
        kept.select(
            slice_(col("uetrs"), 2, n_t - 1).alias("tail"),
            slice_(col("nodes"), 2, n_t).alias("tail_nodes"),
            element_at(col("uetrs"), 1).alias("first_uetr"),
            element_at(col("nodes"), 1).alias("sender"),
            col("ts_last"),
            col("ts_q"),
        )
        .groupBy("tail", "tail_nodes")
        .agg(
            array_sort(collect_set("first_uetr")).alias("first_uetrs"),
            array_sort(collect_set("sender")).alias("senders"),
            max_("ts_last").alias("ts_last"),
            max_("ts_q").alias("ts_q"),
        )
        .select(
            element_at(col("tail_nodes"), 1).alias("entity"),
            concat(col("first_uetrs"), col("tail")).alias("uetrs"),
            array_distinct(concat(col("senders"), col("tail_nodes"))).alias("nodes"),
            (size(col("tail")) + lit(1)).alias("hops"),
            size(col("first_uetrs")).alias("feeders"),
            col("ts_last"),
            col("ts_q"),
        )
    )
    return _alert_frame(
        merged,
        rule_id=lit("W17_layering_chain"),
        entity_id=col("entity"),
        related_txn_ids=col("uetrs"),
        related_entity_ids=col("nodes"),
        # When the chain first qualified (see _advance), not when its last
        # onward hop happened: scoring bounds alert_ts by the planted window.
        alert_ts=col("ts_q"),
        # Longer chains are more deliberate. Bounded [0.6, 0.9].
        alert_score=expr("least(0.9, 0.3 + 0.1 * hops)").cast("double"),
        priority=when(col("hops") >= 5, lit("HIGH"))
        .when(col("hops") >= 4, lit("MED"))
        .otherwise(lit("LOW")),
        alert_type=lit("layering_chain"),
        run_id=lit(run_id),
        narrative=expr(
            "concat('Entity ', cast(entity as string), ' passed on funds along ', "
            "cast(hops as string), ' transfers (', cast(feeders as string), "
            "' feeding credit(s)) ending ', cast(ts_last as string))"
        ),
        evidence=map_from_arrays(
            array(
                lit("rule"),
                lit("hops"),
                lit("feeders"),
                lit("hop_window_hours"),
                lit("min_forward_ratio"),
                lit("max_forward_ratio"),
                lit("max_out_degree"),
            ),
            array(
                lit("W17_layering_chain"),
                col("hops").cast("string"),
                col("feeders").cast("string"),
                lit(str(hop_window_hours)),
                lit(str(min_forward_ratio)),
                lit(str(max_forward_ratio)),
                lit(str(max_out_degree)),
            ),
        ),
    )


def w4_risk_propagation(
    silver_txns: DataFrame,
    velocity_hours: int = 6,
    forward_ratio: float = 0.8,
    run_id: str = "unknown",
    max_txns_per_alert: int = 1000,
) -> DataFrame:
    """Detect rapid pass-through: entity B receives funds from A and
    forwards >= forward_ratio of them to some entity C, all within
    velocity_hours.

    Fires on the `rapid_layering` typology (participants=3, whole chain
    within one civil day per datagen). Approximation: does not require
    C != A (which would require another join step); a self-loop that
    just cycles back also fires, treated as a subset of round-tripping.

    Emits one alert per B (the intermediate entity). related_txn_ids holds
    the uetrs of every matched incoming and outgoing payment, and
    related_entity_ids every A and C, each sorted and cut to the first
    max_txns_per_alert (an evidence bound, the same value as W2's: without it
    a hub's arrays grow with the corpus). The evidence map carries the full
    counts, txn_total and entity_total, and txns_truncated and
    entities_truncated ('true' when the cap cut the list). Scoring matches
    planted transactions against related_txn_ids, so a truncated hub alert
    can miss planted payments past the cut: recall is reported as bounded by
    this cap when any W4 alert was truncated (score_financial).
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
    # Join on (entity, velocity-window bucket), not the entity alone: keyed
    # on the entity, a busy account paired every credit it received over the
    # corpus with every payment it sent before the time filter ran. An
    # outgoing payment within ``seconds`` after a credit in bucket k lies in
    # bucket k or k + 1, so each outgoing row is offered under both.
    in_b = (unix_timestamp(col("ts_in")) / lit(seconds)).cast("long")
    out_b = (unix_timestamp(col("ts_out")) / lit(seconds)).cast("long")
    incoming = incoming.withColumn("_bi", in_b)
    outgoing = outgoing.withColumn("_bo", out_b).unionByName(
        outgoing.withColumn("_bo", out_b - lit(1))
    )
    joined = (
        incoming.join(outgoing, (col("b") == col("b2")) & (col("_bi") == col("_bo")), "inner")
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
    # Sorted, so the kept prefix is the same on every run whatever order the
    # collect_* produced.
    cap = int(max_txns_per_alert)
    per_entity = per_entity.withColumn(
        "_txns", array_sort(array_distinct(expr("concat(uetrs_in, uetrs_out)")))
    ).withColumn(
        "_entities", array_sort(expr("cast(array_union(as_set, cs_set) as array<bigint>)"))
    )
    return _alert_frame(
        per_entity,
        rule_id=lit("W4_risk_propagation"),
        entity_id=col("b"),
        related_txn_ids=expr(f"slice(_txns, 1, {cap})"),
        related_entity_ids=expr(f"slice(_entities, 1, {cap})"),
        alert_ts=col("last_ts"),
        # Score clamped to [0, 0.95] so downstream percentile aggregations
        # don't skew from >1 values (see W3 fix). max_forward_ratio -
        # threshold is scaled and offset from 0.7 baseline.
        alert_score=expr(f"least(0.95, 0.7 + (max_forward_ratio - {forward_ratio}) * 0.15)").cast(
            "double"
        ),
        priority=when(col("chain_count") >= 3, lit("HIGH"))
        .when(col("chain_count") >= 2, lit("MED"))
        .otherwise(lit("LOW")),
        alert_type=lit("risk_propagation"),
        run_id=lit(run_id),
        narrative=expr(
            "concat('Rapid pass-through at entity ', cast(b as string), ': ', "
            "cast(chain_count as string), ' incoming/outgoing chains, first ', "
            "cast(first_ts as string), ' last ', cast(last_ts as string))"
        ),
        evidence=map_from_arrays(
            array(
                lit("rule"),
                lit("velocity_hours"),
                lit("forward_ratio"),
                lit("txn_total"),
                lit("txns_truncated"),
                lit("entity_total"),
                lit("entities_truncated"),
            ),
            array(
                lit("W4_risk_propagation"),
                lit(str(velocity_hours)),
                lit(str(forward_ratio)),
                size(col("_txns")).cast("string"),
                (size(col("_txns")) > lit(cap)).cast("string"),
                size(col("_entities")).cast("string"),
                (size(col("_entities")) > lit(cap)).cast("string"),
            ),
        ),
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
    was counted in the measured gold size. The W3/W17 path-search files are
    removed too (cleanup_path_search_spill), for callers such as replay that
    do not go through the gold detection loop.
    """
    cleanup_path_search_spill(spark)
    if not _w1_checkpoint_dir():
        return
    # Only this driver's directory: setCheckpointDir writes under a random
    # per-context subdirectory, and another driver of the same deployment
    # (a replay running W1) may be using a sibling right now.
    path = spark.sparkContext.getCheckpointDir()
    if not path:
        return
    _delete_uri(spark, path, "W1")


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

    alerts = _alert_frame(
        with_txns,
        rule_id=lit("W1_connected_components"),
        # No single entity owns a cluster alert -- pick the min id
        # deterministically so replay is stable across runs.
        entity_id=col("component"),
        related_txn_ids=col("related_txn_ids"),
        related_entity_ids=expr("cast(entity_ids as array<bigint>)"),
        alert_ts=col("last_ts"),
        # Larger components ~= higher risk. Bounded 0.5-0.95.
        alert_score=expr(
            "cast(least(0.95, 0.5 + 0.05 * cast(component_size - "
            + str(min_cluster_size)
            + " as double)) as double)"
        ),
        priority=when(col("component_size") >= 8, lit("HIGH"))
        .when(col("component_size") >= 5, lit("MED"))
        .otherwise(lit("LOW")),
        alert_type=lit("cluster"),
        run_id=lit(run_id),
        narrative=expr(
            "concat('Connected component of ', cast(component_size as string), "
            "' entities (min id ', cast(component as string), "
            "') active between ', cast(first_ts as string), ' and ', "
            "cast(last_ts as string))"
        ),
        evidence=map_from_arrays(
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
        ),
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


# ---------------------------------------------------------------------------
# W5/W6: watchlist screening (AML-GOALS #50). The corpus carries its own dated
# watchlist (bronze/watchlist.parquet: a sanctions list in two versions and a
# PEP list); planted payments name listed parties as the payer typed them.
# ---------------------------------------------------------------------------

#: Minimum name similarity for a screening hit: 1 - Levenshtein distance /
#: longer length, over the normalized, token-sorted names. Self-chosen: it
#: admits one edit in a name of seven or more letters and a token-order swap
#: (sorting removes it), and rejects a different middle name in a typical
#: three-token name. Not tuned to the planted variants' recall.
SCREEN_SIMILARITY_MIN = 0.85
#: Most prior payments a rescreen alert lists as related transactions.
RESCREEN_MAX_RELATED = 200


def _watchlist_path() -> str:
    import os

    override = os.environ.get("LB_FINANCIAL_WATCHLIST_PATH")
    if override:
        return override
    uri = os.environ.get("LB_BRONZE_URI", "s3a://lb-bronze/")
    root = os.environ.get("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/").rstrip("/")
    return f"{uri}{root}/bronze/watchlist.parquet"


def _load_watchlist(spark, list_type: str, rule: str) -> DataFrame:
    """The corpus watchlist entries of one list type. A corpus without a
    watchlist (generated before the screening track) or with no entry of
    this type raises RuleSkipped, so the rule reads "not run", never 0%."""
    from pyspark.sql.utils import AnalysisException

    path = _watchlist_path()
    try:
        wl = spark.read.parquet(path)
    except AnalysisException as e:
        raise RuleSkipped("no-watchlist", f"{rule}: {path} not readable ({e})") from None
    wl = wl.filter(col("list_type") == lit(list_type))
    if not wl.take(1):
        raise RuleSkipped("empty-watchlist", f"{rule}: no {list_type} entries in {path}")
    return wl


def _name_tokens_expr(col_name: str) -> str:
    """Sorted, non-empty tokens of the normalized name."""
    return f"array_sort(array_remove(split({_normalize_name_expr(col_name)}, ' '), ''))"


def _block_keys_expr(tokens: str) -> str:
    """Blocking keys: every unordered pair of the tokens' Soundex codes (a
    one-token name blocks on its code). Soundex absorbs most vowel and
    doubled-letter respellings (MOHAMMED and MUHAMMAD share M530), and a
    typo that does change a code changes one token, so a three-token name
    keeps at least one intact pair. Pairs, not single codes, keep the
    candidate set near the namesakes of an entry rather than everyone who
    shares a given name."""
    codes = f"array_sort(array_distinct(transform({tokens}, t -> soundex(t))))"
    return (
        f"case when size({codes}) < 2 then {codes} else "
        f"flatten(transform(sequence(0, size({codes}) - 2), i -> "
        f"transform(sequence(i + 1, size({codes}) - 1), j -> "
        f"concat(element_at({codes}, i + 1), '|', element_at({codes}, j + 1))))) end"
    )


def _entity_country_frame(silver_entities: DataFrame) -> DataFrame:
    """One country per entity_id, deterministically (lexicographic minimum)."""
    from pyspark.sql.functions import min as _min_agg

    return (
        silver_entities.filter(col("country").isNotNull())
        .select(col("entity_id").alias("bene_entity_id"), col("country").alias("bene_country"))
        .groupBy("bene_entity_id")
        .agg(_min_agg("bene_country").alias("bene_country"))
    )


def screen_counterparties(names: DataFrame, watchlist: DataFrame) -> DataFrame:
    """Fuzzy-screen distinct counterparties against watchlist entries.

    ``names``: (rptd_beneficiary_name, bene_country). ``watchlist``: the
    bronze watchlist rows. A counterparty matches an entry when one of the
    entry's names (primary or alias) has similarity >= SCREEN_SIMILARITY_MIN
    to it and the entry's country, when it has one, is the counterparty's
    country (the secondary identifier that separates namesakes abroad).

    Returns (rptd_beneficiary_name, bene_country, list_id, list_version,
    version_published_date, listed_date, similarity, match_mode) with the best
    similarity per (counterparty, entry).
    """
    from pyspark.sql.functions import broadcast, length, levenshtein

    wl_names = watchlist.select(
        "list_id",
        "list_version",
        "version_published_date",
        "listed_date",
        col("country").alias("wl_country"),
        explode(expr("concat(array(name), coalesce(aliases, array()))")).alias("wl_name"),
    )
    wl_keyed = (
        wl_names.withColumn("wl_tokens", expr(_name_tokens_expr("wl_name")))
        .withColumn("wl_key", expr("array_join(wl_tokens, ' ')"))
        .withColumn("block", explode(expr(_block_keys_expr("wl_tokens"))))
        .drop("wl_tokens", "wl_name")
        .distinct()
    )
    cp = (
        names.select("rptd_beneficiary_name", "bene_country")
        .distinct()
        .withColumn("cp_tokens", expr(_name_tokens_expr("rptd_beneficiary_name")))
        .withColumn("cp_key", expr("array_join(cp_tokens, ' ')"))
    )
    cand = (
        cp.withColumn("block", explode(expr(_block_keys_expr("cp_tokens"))))
        .drop("cp_tokens")
        .join(broadcast(wl_keyed), "block", "inner")
        .drop("block")
        .distinct()
    )
    sim = lit(1.0) - levenshtein(col("cp_key"), col("wl_key")) / expr(
        "greatest(length(cp_key), length(wl_key), 1)"
    )
    hits = (
        cand.filter(length(col("cp_key")) > 0)
        .withColumn("similarity", sim.cast("double"))
        .filter(col("similarity") >= lit(SCREEN_SIMILARITY_MIN))
        .filter(col("wl_country").isNull() | (col("wl_country") == col("bene_country")))
    )
    best = Window.partitionBy("rptd_beneficiary_name", "bene_country", "list_id").orderBy(
        col("similarity").desc()
    )
    return (
        hits.withColumn("_rn", row_number().over(best))
        .filter(col("_rn") == 1)
        .select(
            "rptd_beneficiary_name",
            "bene_country",
            "list_id",
            "list_version",
            "version_published_date",
            "listed_date",
            "similarity",
            when(col("similarity") >= lit(1.0), lit("exact"))
            .otherwise(lit("fuzzy"))
            .alias("match_mode"),
        )
    )


def _screen_txns(silver_txns: DataFrame, silver_entities: DataFrame) -> DataFrame:
    """silver_txns with the beneficiary's country, for screening."""
    return silver_txns.join(
        _entity_country_frame(silver_entities),
        col("beneficiary_id") == col("bene_entity_id"),
        "left",
    ).select(
        col("uetr"),
        col("originator_id").alias("entity_id"),
        col("beneficiary_id"),
        col("txn_timestamp"),
        col("txn_amount_usd"),
        col("txn_amount"),
        col("rptd_beneficiary_name"),
        col("bene_country"),
    )


def _screen_alerts(hits: DataFrame, rule_id: str, alert_type: str, run_id: str, score, priority):
    """gold.alerts rows for per-transaction screening hits (one alert per
    transaction, on its best-matching entry)."""
    best = Window.partitionBy("uetr").orderBy(col("similarity").desc(), col("list_id"))
    h = hits.withColumn("_rn", row_number().over(best)).filter(col("_rn") == 1)
    return _alert_frame(
        h,
        rule_id=lit(rule_id),
        entity_id=col("entity_id"),
        related_txn_ids=array(col("uetr")),
        related_entity_ids=expr("array(cast(beneficiary_id as bigint))"),
        alert_ts=col("txn_timestamp"),
        alert_score=score.cast("double"),
        priority=lit(priority) if isinstance(priority, str) else priority,
        alert_type=lit(alert_type),
        run_id=lit(run_id),
        narrative=expr(
            "concat('Beneficiary ', coalesce(rptd_beneficiary_name, ''), ' matches watchlist "
            "entry ', list_id, ' (', match_mode, ', similarity ', "
            "cast(round(similarity, 3) as string), ') on transaction ', uetr)"
        ),
        evidence=map_from_arrays(
            array(
                lit("rule"),
                lit("list_id"),
                lit("list_version"),
                lit("match_mode"),
                lit("similarity"),
            ),
            array(
                lit(rule_id),
                col("list_id"),
                col("list_version").cast("string"),
                col("match_mode"),
                expr("cast(round(similarity, 4) as string)"),
            ),
        ),
    )


def w5_sanctions_match(
    silver_txns: DataFrame,
    silver_entities: DataFrame | None = None,
    run_id: str = "unknown",
) -> DataFrame:
    """Sanctions screen: payments to a party on the corpus sanctions list.

    Customer-scoped: the subject is the originator, and only a customer
    originator alerts (the beneficiary is the listed party).

    Two passes, both a fuzzy name screen (screen_counterparties) against the
    dated list:

    - Transaction screen: each payment is screened against the entries
      listed on or before its date.
    - Rescreen: when a list version is published, the counterparty base is
      screened against the entries it adds; payments made to a newly listed
      party before its listing raise one alert per (customer, counterparty,
      entry), dated at the publication, listing those prior payments.

    Bounded by design (the mission excludes production screening
    completeness): no phonetic keys, no date-of-birth or identifier
    matching, the country as the only secondary identifier.
    """
    spark = silver_txns.sparkSession
    silver_entities = _entities_frame(spark, silver_entities, "W5")
    customers = _customer_ids(spark, silver_entities, "W5")
    wl = _load_watchlist(spark, "sanctions", "W5")
    txns = _screen_txns(silver_txns, silver_entities)
    matches = screen_counterparties(txns, wl)
    from pyspark.sql.functions import broadcast

    hits = txns.join(broadcast(matches), ["rptd_beneficiary_name", "bene_country"], "inner")
    listed_ts = expr("cast(listed_date as timestamp)")
    at_txn = hits.filter(col("txn_timestamp") >= listed_ts)
    txn_alerts = _screen_alerts(
        at_txn,
        "W5_sanctions_match",
        "sanctions_match",
        run_id,
        lit(0.5) + lit(0.45) * col("similarity"),
        "HIGH",
    )
    pre = hits.filter((col("txn_timestamp") < listed_ts) & (col("list_version") > lit(1)))
    grouped = pre.groupBy("entity_id", "beneficiary_id", "list_id").agg(
        collect_list(struct(col("txn_timestamp"), col("uetr"))).alias("_txns"),
        max_(col("similarity")).alias("similarity"),
        min_(col("version_published_date")).alias("version_published_date"),
        min_(col("list_version")).alias("list_version"),
        max_(col("rptd_beneficiary_name")).alias("rptd_beneficiary_name"),
    )
    rescreen = _alert_frame(
        grouped,
        rule_id=lit("W5_sanctions_match"),
        entity_id=col("entity_id"),
        related_txn_ids=expr(
            f"slice(transform(array_sort(_txns), x -> x.uetr), 1, {RESCREEN_MAX_RELATED})"
        ),
        related_entity_ids=expr("array(cast(beneficiary_id as bigint))"),
        alert_ts=expr("cast(version_published_date as timestamp)"),
        alert_score=(lit(0.5) + lit(0.45) * col("similarity")).cast("double"),
        priority=lit("HIGH"),
        alert_type=lit("sanctions_rescreen"),
        run_id=lit(run_id),
        narrative=expr(
            "concat('Rescreen on list version ', cast(list_version as string), ': prior "
            "counterparty ', coalesce(rptd_beneficiary_name, ''), ' matches new entry ', "
            "list_id, ' (', cast(size(_txns) as string), ' prior payments)')"
        ),
        evidence=map_from_arrays(
            array(
                lit("rule"),
                lit("list_id"),
                lit("list_version"),
                lit("match_mode"),
                lit("txn_total"),
                lit("txns_truncated"),
            ),
            array(
                lit("W5_sanctions_match"),
                col("list_id"),
                col("list_version").cast("string"),
                lit("rescreen"),
                size(col("_txns")).cast("string"),
                (size(col("_txns")) > lit(RESCREEN_MAX_RELATED)).cast("string"),
            ),
        ),
    )
    return _customers_only(txn_alerts.unionByName(rescreen), customers)


#: W6 screens every payment; this USD equivalent splits triage priority
#: (MED at or above, LOW below). Small PEP payments are mostly
#: administrative, and PEP exposure feeds enhanced due diligence rather than
#: blocking, so the line orders the queue instead of dropping hits: an
#: amount floor would make W6 recall a property of the amount distribution
#: rather than of the screen.
PEP_MIN_USD = 10_000.0


def w6_pep_counterparty(
    silver_txns: DataFrame,
    silver_entities: DataFrame | None = None,
    run_id: str = "unknown",
) -> DataFrame:
    """PEP screen: payments to a party on the corpus PEP list, by the same
    fuzzy screen as W5 (transaction screen only).

    Customer-scoped: the subject is the originator, and only a customer
    originator alerts. PEP counterparties are not intrinsically suspicious,
    so priority is MED at PEP_MIN_USD or more and LOW below. The USD amount
    falls back to txn_amount when the FX enrichment is missing.
    """
    spark = silver_txns.sparkSession
    silver_entities = _entities_frame(spark, silver_entities, "W6")
    customers = _customer_ids(spark, silver_entities, "W6")
    wl = _load_watchlist(spark, "pep", "W6")
    txns = _screen_txns(silver_txns, silver_entities)
    matches = screen_counterparties(txns, wl)
    from pyspark.sql.functions import broadcast

    hits = txns.join(broadcast(matches), ["rptd_beneficiary_name", "bene_country"], "inner").filter(
        col("txn_timestamp") >= expr("cast(listed_date as timestamp)")
    )
    alerts = _screen_alerts(
        hits,
        "W6_pep_counterparty",
        "pep_counterparty",
        run_id,
        lit(0.3) + lit(0.4) * col("similarity"),
        when(expr(f"coalesce(txn_amount_usd, txn_amount) >= {PEP_MIN_USD}"), lit("MED")).otherwise(
            lit("LOW")
        ),
    )
    return _customers_only(alerts, customers)


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
    is not present, or carries no customer (a pre-KYC corpus), the rule
    raises RuleSkipped so the scorecard reads "not run" rather than 0%.

    Customer-scoped: the subject is the originator, and only a customer
    originator alerts. The beneficiary's country comes from every entity,
    customer or not (the high-risk counterparty is usually not one).

    Historically this rule crashed inside the driver when the caller did
    NOT pass silver_entities AND the reference JSON was not mounted --
    the ImportError from ``import lakebench.spark.data`` surfaced as a
    hard failure of the whole replay job. A missing reference JSON now
    degrades to an empty alerts DF with a stderr note.
    """
    from pyspark.sql.functions import broadcast

    spark = silver_txns.sparkSession
    silver_entities = _entities_frame(spark, silver_entities, "W7")
    customers = _customer_ids(spark, silver_entities, "W7")
    raw = _load_reference(
        spark,
        "high_risk_jurisdictions.json",
        "country_code string, country_name string, risk_tier string",
    )
    if raw is None:
        print("[W7] high_risk_jurisdictions.json not available; skipping.")
        return _empty_alerts_df(spark, run_id)
    # The FATF list names none of the generator's home countries (June 2026),
    # so on its own W7 could never fire on this corpus. The synthetic
    # corridor list is the set corridor_high_risk plants against, labelled
    # synthetic in its file and as risk_tier "synthetic_corridor" in evidence.
    corridors = _load_reference(
        spark,
        "synthetic_corridors.json",
        "country_code string, country_name string, risk_tier string",
    )
    if corridors is not None:
        raw = raw.unionByName(corridors)

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

    alerts = _alert_frame(
        hits,
        rule_id=lit("W7_cross_border_high_risk"),
        entity_id=col("entity_id"),
        related_txn_ids=array(col("uetr")),
        related_entity_ids=expr("array(cast(beneficiary_id as bigint))"),
        alert_ts=col("txn_timestamp"),
        alert_score=when(col("risk_tier") == "black", lit(0.90))
        .otherwise(lit(0.60))
        .cast("double"),
        priority=when(col("risk_tier") == "black", lit("HIGH")).otherwise(lit("MED")),
        alert_type=lit("cross_border_high_risk"),
        run_id=lit(run_id),
        narrative=expr(
            "concat('Cross-border transaction to ', bene_country, ' (', "
            "case when risk_tier = 'synthetic_corridor' then 'synthetic high-risk corridor' "
            "else concat('FATF ', risk_tier, ' list') end, ') for ', cast(amount as string), "
            "' on ', cast(txn_timestamp as string))"
        ),
        evidence=map_from_arrays(
            array(lit("rule"), lit("country"), lit("risk_tier")),
            array(lit("W7_cross_border_high_risk"), col("bene_country"), col("risk_tier")),
        ),
    )
    return _customers_only(alerts, customers)


def w8_dormant_reactivation(
    silver_txns: DataFrame,
    dormant_days: int = 90,
    amount_threshold_usd: float = 5000.0,
    silver_entities: DataFrame | None = None,
    run_id: str = "unknown",
) -> DataFrame:
    """Flag reactivation of dormant originator accounts.

    An originator's transaction is flagged when:
    - The gap to the originator's prior transaction exceeds dormant_days;
    - The current transaction's USD-equivalent amount >= threshold.

    First-ever activity for an originator is NOT flagged (no prior
    baseline). This is the correct semantics -- we cannot say an
    account was dormant if we never observed it before.

    Customer-scoped: only a customer originator alerts. The gap is measured
    over the originator's own sends, so the scope changes no gap.
    """
    from pyspark.sql.functions import lag, unix_timestamp

    customers = _customer_ids(silver_txns.sparkSession, silver_entities, "W8")

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

    alerts = _alert_frame(
        hits,
        rule_id=lit("W8_dormant_reactivation"),
        entity_id=col("originator_id"),
        related_txn_ids=array(col("uetr")),
        related_entity_ids=expr("array(cast(beneficiary_id as bigint))"),
        alert_ts=col("txn_timestamp"),
        alert_score=lit(0.70).cast("double"),
        priority=lit("MED"),
        alert_type=lit("dormant_reactivation"),
        run_id=lit(run_id),
        narrative=expr(
            "concat('Originator ', cast(originator_id as string), ' reactivated after ', "
            "cast(round((unix_timestamp(txn_timestamp) - unix_timestamp(prev_ts)) / 86400.0, 0) "
            "as string), ' days with USD ', cast(txn_amount_usd as string))"
        ),
        evidence=map_from_arrays(
            array(lit("rule"), lit("dormant_days"), lit("amount_threshold_usd")),
            array(
                lit("W8_dormant_reactivation"),
                lit(str(dormant_days)),
                lit(str(amount_threshold_usd)),
            ),
        ),
    )
    return _customers_only(alerts, customers)


def _empty_alerts_df(spark, run_id: str) -> DataFrame:
    """Zero-row DataFrame with the gold.alerts schema (ALERT_COLUMNS), for
    the case where a rule declines to run (e.g. W1 above max_vertices)."""
    from pyspark.sql.types import StructField, StructType, _parse_datatype_string

    schema = StructType(
        [
            StructField(name, _parse_datatype_string(ddl_type), nullable)
            for name, ddl_type, nullable in ALERT_COLUMNS
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
    "W17_layering_chain": w17_layering_chain,
}


def get_rule(rule_id: str):
    """Look up a rule function by id. Returns None if unknown."""
    return _RULE_DISPATCH.get(rule_id)


def known_rules() -> list[str]:
    return list(_RULE_DISPATCH.keys())


# Guard against ruff unused-import warnings for symbols exported for callers.
_ = (row_number, to_timestamp, explode, Window)
