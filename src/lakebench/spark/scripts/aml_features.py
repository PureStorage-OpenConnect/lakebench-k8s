"""Per-entity AML features for the pre-registered fidelity gate (AML-GOALS D5, D9, A6).

One feature definition, written once, fed by two adapters:

- ``silver_frames`` reads the lakehouse (silver.transactions, silver.entities,
  silver.accounts). This is the cluster half of the gate (D9).
- ``bronze_frames`` reads a raw datagen corpus (pacs.008 parquet plus the party
  and account masters). This is the local harness (R7's tracked gate).

A6 compares the two: if silver's entity keying merged or split planted
accounts, the silver AP moves away from the bronze AP. For that comparison to
mean anything the adapters must be genuinely different inputs, so each builds
its own normalised transaction frame and its own entity-attribute frame, and
only ``entity_features`` is shared.

Normalised transaction frame (one row per payment):
    uetr, orig_key, bene_key, ts, amount (native currency), currency,
    amount_usd, orig_country, bene_country

Entity-attribute frame (one row per entity key):
    key, is_customer, home_country, customer_type, crr_tier

``entity_features`` returns one row per entity that appears in a payment, with
the pre-registered feature columns plus ``is_customer`` and ``n_sends`` (not
features: the gate filters on the first and D2 selects its cohort on the
second).

Feature definitions. "Payments" means every payment the entity is a party to,
either side; a self-payment counts once. "Sends" means payments it originates.

- txn_count, active_days: payments, distinct UTC dates of payments.
- gap_mean_days, gap_max_days, gap_cv, max_gap_over_mean_gap: over the gaps
  between consecutive SENDS. Sends, not all payments, because the account's
  own activity is what the generator's persona and dormancy shape (a dormant
  account still receives), and W8 scans sends. NULL with fewer than two sends.
- amount_mean_usd, amount_max_usd, amount_cv: over payments, in USD.
- n_counterparties: distinct counterparties over payments.
- frac_cross_border: share of payments whose two parties' countries differ.
- frac_round_amount: share of payments whose native amount is a whole
  multiple of ROUND_UNIT.
- max_burst_24h: most payments in any [t, t + 24 h] window.
- frac_in_structuring_band: share of payments whose native amount lies in
  W2's band, [STRUCTURING_BAND_FLOOR x threshold, threshold] for the payment's
  currency (thresholds from detection_rules).
- home_country_high_risk: 1 if the entity's home country is in
  HIGH_RISK_COUNTRIES, else 0.
- frac_high_risk_corridor: share of payments whose counterparty's country is
  in HIGH_RISK_COUNTRIES.
- frac_overnight: share of payments with UTC hour < OVERNIGHT_END_HOUR.
- frac_weekend: share of payments on a Saturday or Sunday.
- hour_of_day_entropy: Shannon entropy (bits) of the payments' hour-of-day
  histogram.
- customer_type: 1 business, 0 person, NULL for non-customers.
- crr_tier: 0 low, 1 medium, 2 high, NULL for non-customers.

The constants below are feature definitions, not gate thresholds; every gate
threshold lives in aml_preregistration.json (R7).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    broadcast,
    coalesce,
    col,
    countDistinct,
    create_map,
    dayofweek,
    explode,
    hour,
    lag,
    lit,
    stddev_pop,
    to_date,
    trim,
    when,
)
from pyspark.sql.functions import count as count_
from pyspark.sql.functions import log2 as log2_
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import mean as mean_
from pyspark.sql.functions import min as min_
from pyspark.sql.functions import sum as sum_

#: The generator's high-risk corridor pool (datagen_rs/src/typology.rs
#: HIGH_RISK_CC; a drift test keeps the two in step). corridor_high_risk draws
#: both participants from residents of these countries, so this is the
#: attribute the generator conditions that label on (AML-GOALS 5a: the feature
#: set must cover every generative attribute). It is NOT W7's FATF list.
HIGH_RISK_COUNTRIES = ("AE", "CN", "SG", "HK", "MX", "IN")

#: W2's band floor as a fraction of the reporting threshold
#: (detection_rules._suspicious_amount_expr).
STRUCTURING_BAND_FLOOR = 0.9

#: A "round" amount is a whole multiple of this, in the payment's currency.
ROUND_UNIT = 100

#: Payments before this UTC hour count as overnight.
OVERNIGHT_END_HOUR = 6

_SECONDS_PER_DAY = 86_400
_BURST_WINDOW_SECONDS = _SECONDS_PER_DAY

_CRR_TIER_CODE = {"low": 0.0, "medium": 1.0, "high": 2.0}
_CUSTOMER_TYPE_CODE = {"person": 0.0, "business": 1.0}

#: Columns every adapter's transaction frame must carry.
TXN_COLUMNS = (
    "uetr",
    "orig_key",
    "bene_key",
    "ts",
    "amount",
    "currency",
    "amount_usd",
    "orig_country",
    "bene_country",
)
#: Columns every adapter's entity-attribute frame must carry.
ENTITY_COLUMNS = ("key", "is_customer", "home_country", "customer_type", "crr_tier")

#: Feature columns entity_features produces, in a fixed order. The gate checks
#: this set against the pre-registration's features list.
FEATURE_COLUMNS = (
    "txn_count",
    "active_days",
    "gap_mean_days",
    "gap_max_days",
    "gap_cv",
    "amount_mean_usd",
    "amount_max_usd",
    "amount_cv",
    "n_counterparties",
    "frac_cross_border",
    "frac_round_amount",
    "max_burst_24h",
    "frac_in_structuring_band",
    "home_country_high_risk",
    "frac_high_risk_corridor",
    "max_gap_over_mean_gap",
    "frac_overnight",
    "frac_weekend",
    "hour_of_day_entropy",
    "customer_type",
    "crr_tier",
)


def _code_map(mapping):
    pairs = []
    for k, v in mapping.items():
        pairs += [lit(k), lit(v)]
    return create_map(*pairs)


def _in_structuring_band(amount_col, ccy_col):
    from detection_rules import _STRUCTURING_THRESHOLDS

    expr = lit(False)
    for ccy, thr in _STRUCTURING_THRESHOLDS.items():
        expr = expr | (
            (ccy_col == lit(ccy)) & amount_col.between(thr * STRUCTURING_BAND_FLOOR, thr)
        )
    return expr


def _check_columns(df: DataFrame, needed, what: str) -> None:
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"{what} frame is missing columns {missing}")


def entity_features(txns: DataFrame, entities: DataFrame) -> DataFrame:
    """One row per entity key with FEATURE_COLUMNS, is_customer and n_sends."""
    _check_columns(txns, TXN_COLUMNS, "transaction")
    _check_columns(entities, ENTITY_COLUMNS, "entity")
    hr = [lit(c) for c in HIGH_RISK_COUNTRIES]

    base = txns.select(*TXN_COLUMNS).withColumn(
        "_band", _in_structuring_band(col("amount"), col("currency"))
    )
    common = [
        col("uetr"),
        col("ts"),
        col("amount"),
        col("amount_usd"),
        col("_band"),
        coalesce(col("orig_country") != col("bene_country"), lit(False)).alias("_xb"),
    ]
    out_side = base.select(
        col("orig_key").alias("key"),
        col("bene_key").alias("cp"),
        col("bene_country").alias("cp_country"),
        lit(True).alias("is_send"),
        *common,
    )
    # A self-payment appears once, on its send side.
    in_side = base.filter(col("orig_key") != col("bene_key")).select(
        col("bene_key").alias("key"),
        col("orig_key").alias("cp"),
        col("orig_country").alias("cp_country"),
        lit(False).alias("is_send"),
        *common,
    )
    sides = out_side.unionByName(in_side).withColumn("_hour", hour(col("ts")))

    def frac(cond):
        return mean_(when(cond, lit(1.0)).otherwise(lit(0.0)))

    agg = sides.groupBy("key").agg(
        count_(lit(1)).cast("double").alias("txn_count"),
        countDistinct(to_date(col("ts"))).cast("double").alias("active_days"),
        mean_("amount_usd").alias("amount_mean_usd"),
        max_("amount_usd").alias("amount_max_usd"),
        (stddev_pop("amount_usd") / mean_("amount_usd")).alias("amount_cv"),
        countDistinct("cp").cast("double").alias("n_counterparties"),
        frac(col("_xb")).alias("frac_cross_border"),
        frac((col("amount") % lit(ROUND_UNIT)) == lit(0)).alias("frac_round_amount"),
        frac(col("_band")).alias("frac_in_structuring_band"),
        frac(coalesce(col("cp_country").isin(*hr), lit(False))).alias("frac_high_risk_corridor"),
        frac(col("_hour") < lit(OVERNIGHT_END_HOUR)).alias("frac_overnight"),
        frac(dayofweek(col("ts")).isin(1, 7)).alias("frac_weekend"),
    )

    # Hour-of-day entropy in bits.
    per_hour = sides.groupBy("key", "_hour").agg(count_(lit(1)).alias("_n"))
    tot = per_hour.groupBy("key").agg(sum_("_n").alias("_tot"))
    entropy = (
        per_hour.join(tot, "key")
        .withColumn("_p", col("_n") / col("_tot"))
        .groupBy("key")
        .agg((-sum_(col("_p") * log2_(col("_p")))).alias("hour_of_day_entropy"))
    )

    # Most payments in any [t, t + 24 h] window, anchored at each payment.
    secs = col("ts").cast("long")
    w_burst = (
        Window.partitionBy("key")
        .orderBy(secs)
        .rangeBetween(Window.currentRow, _BURST_WINDOW_SECONDS)
    )
    burst = (
        sides.select("key", "ts")
        .withColumn("_b", count_(lit(1)).over(w_burst))
        .groupBy("key")
        .agg(max_("_b").cast("double").alias("max_burst_24h"))
    )

    # Gaps between consecutive sends, in days.
    w_gap = Window.partitionBy("key").orderBy(col("ts"), col("uetr"))
    sends = sides.filter(col("is_send")).select("key", "ts", "uetr")
    gaps = sends.withColumn(
        "_gap",
        (col("ts").cast("double") - lag(col("ts").cast("double")).over(w_gap))
        / lit(float(_SECONDS_PER_DAY)),
    )
    gap_agg = gaps.groupBy("key").agg(
        count_(lit(1)).cast("double").alias("n_sends"),
        mean_("_gap").alias("gap_mean_days"),
        max_("_gap").alias("gap_max_days"),
        (stddev_pop("_gap") / mean_("_gap")).alias("gap_cv"),
        (max_("_gap") / mean_("_gap")).alias("max_gap_over_mean_gap"),
    )

    ent = entities.select(
        col("key"),
        col("is_customer").cast("boolean").alias("is_customer"),
        when(col("home_country").isNull(), lit(None).cast("double"))
        .otherwise(when(col("home_country").isin(*hr), lit(1.0)).otherwise(lit(0.0)))
        .alias("home_country_high_risk"),
        _code_map(_CUSTOMER_TYPE_CODE)[col("customer_type")].alias("customer_type"),
        _code_map(_CRR_TIER_CODE)[col("crr_tier")].alias("crr_tier"),
    )

    out = (
        agg.join(entropy, "key", "left")
        .join(burst, "key", "left")
        .join(gap_agg, "key", "left")
        .join(ent, "key", "left")
        .withColumn("n_sends", coalesce(col("n_sends"), lit(0.0)))
        .withColumn("is_customer", coalesce(col("is_customer"), lit(False)))
    )
    return out.select("key", *FEATURE_COLUMNS, "is_customer", "n_sends")


# ---------------------------------------------------------------------------
# Labels, density, timing mixture (adapter-agnostic given an id map)
# ---------------------------------------------------------------------------


def labels_from_participants(manifest: DataFrame, id_map: DataFrame) -> DataFrame:
    """(key, typology_type) for every participant of every manifest instance.

    ``id_map`` maps the datagen entity id (``dg_id``) to the adapter's entity
    key. The gate keeps only customers, so a non-customer participant simply
    never scores.
    """
    parts = manifest.select(
        col("typology_type"), explode(col("participant_entity_ids")).alias("dg_id")
    )
    return parts.join(id_map, "dg_id", "inner").select("key", "typology_type").distinct()


def labels_from_uetrs(manifest: DataFrame, txns: DataFrame) -> DataFrame:
    """(key, typology_type) for both parties of every planted payment.

    A cross-check on labels_from_participants: the two agree when every
    participant of an instance is a party to one of its planted rows.
    """
    planted = manifest.select(
        col("typology_type"), explode(col("participant_uetrs")).alias("uetr")
    ).distinct()
    hit = txns.select("uetr", "orig_key", "bene_key").join(planted, "uetr", "inner")
    return (
        hit.select(col("orig_key").alias("key"), "typology_type")
        .unionByName(hit.select(col("bene_key").alias("key"), "typology_type"))
        .distinct()
    )


def label_agreement(by_id: DataFrame, by_uetr: DataFrame) -> dict:
    """Per-typology overlap of the two label routes."""
    a = by_id.withColumn("_a", lit(1))
    b = by_uetr.withColumn("_b", lit(1))
    j = a.join(b, ["key", "typology_type"], "full_outer")
    rows = (
        j.groupBy("typology_type")
        .agg(
            sum_(coalesce(col("_a"), lit(0))).alias("by_participant_id"),
            sum_(coalesce(col("_b"), lit(0))).alias("by_uetr"),
            sum_(
                when(col("_a").isNotNull() & col("_b").isNotNull(), lit(1)).otherwise(lit(0))
            ).alias("both"),
        )
        .collect()
    )
    return {
        r["typology_type"]: {
            "by_participant_id": int(r["by_participant_id"]),
            "by_uetr": int(r["by_uetr"]),
            "both": int(r["both"]),
        }
        for r in rows
    }


def typology_density(txns: DataFrame, manifest: DataFrame) -> dict:
    """D11 inputs: total rows, planted rows, planted rows per typology."""
    planted = manifest.select(
        col("typology_type"), explode(col("participant_uetrs")).alias("uetr")
    ).dropDuplicates(["uetr"])
    total = txns.count()
    hit = txns.select("uetr").join(planted, "uetr", "inner")
    per = {
        r["typology_type"]: int(r["n"])
        for r in hit.groupBy("typology_type").count().withColumnRenamed("count", "n").collect()
    }
    return {"total_rows": int(total), "planted_rows": int(sum(per.values())), "per_typology": per}


def timing_mixture_counts(
    features: DataFrame, *, cohort_min_sends: int, low_cv_edge: float, high_cv_edge: float
) -> dict:
    """D2 inputs over every entity (the generator's population, not only
    customers) with at least cohort_min_sends sends."""
    cohort = features.filter((col("n_sends") >= lit(cohort_min_sends)) & col("gap_cv").isNotNull())
    r = cohort.agg(
        count_(lit(1)).alias("n"),
        sum_(when(col("gap_cv") < lit(low_cv_edge), lit(1)).otherwise(lit(0))).alias("below"),
        sum_(when(col("gap_cv") > lit(high_cv_edge), lit(1)).otherwise(lit(0))).alias("above"),
    ).collect()[0]
    return {
        "n_cohort": int(r["n"] or 0),
        "n_below_low": int(r["below"] or 0),
        "n_above_high": int(r["above"] or 0),
    }


def manifest_provenance(manifest: DataFrame) -> dict:
    """Generator MODEL_VERSION(s) and instance count carried by the manifest."""
    versions = sorted(
        r["model_version"]
        for r in manifest.select("model_version").distinct().collect()
        if r["model_version"] is not None
    )
    return {"model_versions": versions, "n_instances": int(manifest.count())}


# ---------------------------------------------------------------------------
# Adapters
# ---------------------------------------------------------------------------


def _account_id_map(account: DataFrame, iban_to_key: DataFrame) -> DataFrame:
    """dg_id -> key through the account master's IBANs.

    The party master's LEI is NULL for persons (typology participants are all
    drawn from persons), so the datagen id cannot be joined to a key by LEI.
    Every payment names its parties' IBANs, and the account master names each
    IBAN's holder, so IBAN is the one join that covers everyone.
    """
    acct = account.select(col("iban"), col("holder_entity_id").cast("long").alias("dg_id"))
    return acct.join(iban_to_key, "iban", "inner").groupBy("dg_id").agg(min_("key").alias("key"))


def silver_frames(
    spark,
    *,
    catalog: str,
    txns_table: str,
    entities_table: str,
    accounts_table: str,
    account_path: str,
):
    """(txns, entities, id_map) from the lakehouse silver tables.

    Countries come from silver.entities (silver.transactions does not store
    them); the id map goes datagen id -> IBAN (account master) -> silver
    holder_entity_id (silver.accounts).
    """
    t = spark.table(f"{catalog}.{txns_table}")
    e = spark.table(f"{catalog}.{entities_table}")
    a = spark.table(f"{catalog}.{accounts_table}")
    ent = e.select(
        col("entity_id").alias("key"),
        col("is_customer"),
        col("country").alias("home_country"),
        col("customer_type"),
        col("crr_tier"),
    )
    ctry = broadcast(ent.select("key", "home_country"))
    txns = (
        t.select(
            col("uetr"),
            col("originator_id").alias("orig_key"),
            col("beneficiary_id").alias("bene_key"),
            col("txn_timestamp").alias("ts"),
            col("txn_amount").cast("double").alias("amount"),
            col("txn_currency").alias("currency"),
            col("txn_amount_usd").cast("double").alias("amount_usd"),
        )
        .join(
            ctry.withColumnRenamed("key", "orig_key").withColumnRenamed(
                "home_country", "orig_country"
            ),
            "orig_key",
            "left",
        )
        .join(
            ctry.withColumnRenamed("key", "bene_key").withColumnRenamed(
                "home_country", "bene_country"
            ),
            "bene_key",
            "left",
        )
    )
    iban_to_key = a.select(col("iban"), col("holder_entity_id").alias("key")).filter(
        col("iban").isNotNull()
    )
    id_map = _account_id_map(spark.read.parquet(account_path), iban_to_key)
    return txns, ent, id_map


def bronze_frames(spark, *, pacs_path: str, party_path: str, account_path: str):
    """(txns, entities, id_map) from a raw datagen corpus.

    Entity key is the payment's LEI (datagen stamps one on every party of
    every payment). Attributes come from the party master, not from silver.
    """
    from silver_build_financial import _usd_rate

    p = spark.read.parquet(pacs_path)
    txns = p.select(
        col("uetr"),
        trim(col("dbtr.id.lei")).alias("orig_key"),
        trim(col("cdtr.id.lei")).alias("bene_key"),
        # Bronze carries TIMESTAMP_NTZ; silver stores TIMESTAMP. Cast so both
        # adapters hand entity_features the same type (session time zone UTC).
        col("cre_dt_tm").cast("timestamp").alias("ts"),
        col("intr_bk_sttlm_amt").cast("double").alias("amount"),
        col("intr_bk_sttlm_ccy").alias("currency"),
        (col("intr_bk_sttlm_amt").cast("double") * _usd_rate(col("intr_bk_sttlm_ccy"))).alias(
            "amount_usd"
        ),
        col("dbtr.ctry_of_res").alias("orig_country"),
        col("cdtr.ctry_of_res").alias("bene_country"),
    )
    iban_to_key = (
        p.select(col("dbtr_acct.iban").alias("iban"), trim(col("dbtr.id.lei")).alias("key"))
        .unionByName(
            p.select(col("cdtr_acct.iban").alias("iban"), trim(col("cdtr.id.lei")).alias("key"))
        )
        .filter(col("iban").isNotNull() & col("key").isNotNull())
        .distinct()
    )
    id_map = _account_id_map(spark.read.parquet(account_path), iban_to_key)
    party = spark.read.parquet(party_path).select(
        col("entity_id").cast("long").alias("dg_id"),
        col("is_customer"),
        col("country").alias("home_country"),
        col("customer_type"),
        col("crr_tier"),
    )
    ent = party.join(id_map, "dg_id", "inner").drop("dg_id")
    return txns, ent, id_map


# ---------------------------------------------------------------------------
# Hand-off to the pure-Python gate
# ---------------------------------------------------------------------------


def gate_frame(features: DataFrame, labels: DataFrame, typologies):
    """Customers only, one 0/1 label column per typology (``label:<name>``),
    pulled to pandas for lakebench.aml.gate."""
    cust = features.filter(col("is_customer"))
    lab = labels.filter(col("typology_type").isin(*list(typologies)))
    wide = lab.groupBy("key").pivot("typology_type", list(typologies)).agg(count_(lit(1)))
    out = cust.join(wide, "key", "left")
    for t in typologies:
        out = out.withColumn(
            f"label:{t}", (coalesce(col(f"`{t}`"), lit(0)) > lit(0)).cast("int")
        ).drop(t)
    return out.drop("key").toPandas()
