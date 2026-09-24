"""Score (Financial, reference) -- leakage gate + optional GBT model.

Closes the standing rule "distribution checks do not prove semantics
-- must run reference detector + leakage check" that came out of a
prior review pass and was reaffirmed by the AML audit in
``dev-artifacts/roleplay/COLLATED.md``.

Two independent things this script does:

1. **Leakage gate** -- compares baseline (log-normal) transaction
   density against typology density inside each currency-specific
   structuring band. If baseline density is < 10 % of typology
   density in a band, the band effectively IS the label: a rule
   that filters on the band, or a model that sees the raw amount,
   scores recall by construction rather than by detector skill.
   Emits ``leakage_report.parquet`` with a row per (currency, band)
   and a top-level pass/fail.

2. **Reference detector** (best-effort) -- pulls a labelled
   per-entity-per-day feature frame down to the driver, trains a
   scikit-learn Gradient Boosted Classifier on features that
   EXCLUDE the amount-band membership, and evaluates per-typology
   recall/precision on a held-out split. Emits
   ``reference_metrics.parquet``. Verdict is ``no_sklearn`` when
   scikit-learn isn't on the driver image; the leakage gate still
   ran.

Both outputs land alongside ``recall.parquet`` (from
``score_financial.py``) under the run's output prefix. Downstream
aggregation joins them so a report shows rule F1 next to the
reference F1 -- when they diverge sharply, the rule's advantage
is coming from label knowledge the rule shouldn't have, and the
band-leakage row will name which currency to fix in the datagen.

Not this script: full feature engineering. The feature set here is
deliberately small (log_amount, txn_count, unique_counterparties,
mean_hour, std_hour, high_risk_country) so the model result is a
"can any signal be learned" check, not a competition submission. A
future PR can add richer graph/velocity features once the
plumbing is proven under UAT.
"""

from __future__ import annotations

import argparse
import json

from common import env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    hour,
    lit,
    stddev,
    to_date,
    when,
)
from pyspark.sql.functions import (
    count as count_,
)
from pyspark.sql.functions import (
    log as spark_log,
)
from pyspark.sql.functions import (
    max as max_,
)
from pyspark.sql.functions import (
    mean as mean_,
)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TXNS", "silver.transactions")

# Structuring thresholds mirror
# `datagen_rs/src/amounts.rs::structuring_band` and the detector
# filter in ``detection_rules._STRUCTURING_THRESHOLDS``. Kept as a
# single dict so the leakage gate is aligned with what the datagen
# actually writes and what W2 actually reads. A currency whose band
# drifts here without a matching update in the datagen or in the
# detector is exactly the class of divergence the gate is written
# to catch, so if the check contradicts the code, fix the code, not
# this list.
_STRUCTURING_BANDS = {
    "USD": (9_500.0, 9_999.0),
    "CAD": (9_500.0, 9_999.0),
    "AUD": (9_500.0, 9_999.0),
    "GBP": (14_700.0, 14_995.0),
    "EUR": (14_700.0, 14_995.0),
    "CHF": (14_700.0, 14_995.0),
    "JPY": (990_000.0, 999_999.0),
    "INR": (990_000.0, 999_999.0),
    "AED": (54_500.0, 54_999.0),
    "SGD": (19_500.0, 19_999.0),
    "MXN": (99_000.0, 99_999.0),
    "CNY": (49_500.0, 49_999.0),
    "BRL": (49_500.0, 49_999.0),
    "HKD": (74_500.0, 74_999.0),
    "KRW": (9_900_000.0, 9_999_999.0),
}

# High-risk countries used by the ``corridor_high_risk`` typology
# and ``detection_rules._HIGH_RISK_CC``. Deliberately NOT used by
# this script's feature set: any binary "is beneficiary in this set"
# feature would be an exact-label proxy for corridor_high_risk (P1
# from PR-A adversarial review). Retained here so a future PR that
# hardens the datagen (probabilistic country overlays) can bring
# country features back with a clean audit trail.


def _compute_leakage_bands(spark: SparkSession, silver_txns_name: str, manifest) -> list[dict]:
    """Aggregate baseline vs typology counts per (currency, band).

    A transaction is "typology" if its UETR appears in any manifest
    row's ``participant_uetrs``. Otherwise it's baseline. The count
    of interest is *rows with txn_amount inside the band* on each
    side.
    """
    silver = spark.table(f"{CATALOG}.{silver_txns_name}")

    # Typology UETR set from manifest.
    typology_uetrs = manifest.select(explode(col("participant_uetrs")).alias("uetr")).distinct()

    # Tag each silver txn as typology (T) or baseline (B).
    tagged = silver.select(
        col("uetr"),
        col("txn_currency").alias("currency"),
        col("txn_amount").cast("double").alias("amount"),
    ).join(
        typology_uetrs.withColumnRenamed("uetr", "t_uetr"), col("uetr") == col("t_uetr"), how="left"
    )
    tagged = tagged.withColumn(
        "is_typology",
        when(col("t_uetr").isNotNull(), lit(1)).otherwise(lit(0)),
    ).drop("t_uetr")

    rows: list[dict] = []
    for currency, (band_lo, band_hi) in _STRUCTURING_BANDS.items():
        in_band = tagged.filter(
            (col("currency") == lit(currency))
            & (col("amount") >= lit(band_lo))
            & (col("amount") <= lit(band_hi))
        )
        counts = in_band.groupBy("is_typology").agg(count_("*").alias("n")).collect()
        baseline_n = 0
        typology_n = 0
        for r in counts:
            if r["is_typology"] == 1:
                typology_n = int(r["n"])
            else:
                baseline_n = int(r["n"])
        rows.append(
            {
                "currency": currency,
                "band_lo": band_lo,
                "band_hi": band_hi,
                "baseline_count": baseline_n,
                "typology_count": typology_n,
            }
        )
    return rows


def _build_reference_feature_frame(spark: SparkSession, silver_txns_name: str, manifest):
    """Assemble a per-entity-per-day feature DataFrame plus labels.

    Feature columns:

    - ``log_amount_mean``, ``log_amount_std`` over the entity-day
    - ``mean_hour``, ``std_hour`` over the entity-day

    ``amount_pct_of_ceiling`` was removed: it was log_amount_mean divided
    by a constant, so it added no information and was not the relative
    signal it was described as.

    Excluded on purpose, and NOT added to LEAKY_FEATURES because
    they are legitimate features in a real AML system that only
    become label proxies against THIS datagen's deterministic
    typology shapes:

    - ``txn_count`` and ``unique_counterparties``: AML typologies
      plant known participant counts and rows-per-instance (see
      ``datagen_rs/src/typology.rs::SPECS``), so these cardinality
      features fingerprint the typology by construction. Adding
      them back is a datagen-side fix (probabilistic instance
      shapes overlapping baseline), not a scorer-side fix.
    - ``high_risk_country_ratio``: the datagen's ``corridor_high_risk``
      typology draws participants exclusively from the SAME
      HIGH_RISK_CC set the detector uses, so this ratio is 1.0 for
      every corridor_high_risk row and near-baseline for everything
      else. It IS the label for that typology.

    Trade-off recorded in ``dev-artifacts/roleplay/COLLATED.md``:
    dropping these features means the reference detector cannot
    recover ``corridor_high_risk`` or the cardinality-planted
    typologies at all -- their per-typology recall will read as 0.
    That is the honest answer: nothing in the ~4 remaining features
    encodes those typologies' signal, so the model has nothing to
    learn from. A follow-up PR that adds a probabilistic country
    overlay to the datagen would let those features come back.

    Label: typology_type of any matching manifest row for that entity,
    or ``"baseline"``.
    """
    silver = spark.table(f"{CATALOG}.{silver_txns_name}")

    # (uetr, typology_type) pairs. First match wins if a UETR is in
    # multiple manifest rows -- realistic worst case is rare and the
    # model doesn't need a strict multi-label view.
    typology_map = (
        manifest.select("typology_type", explode(col("participant_uetrs")).alias("uetr"))
        .dropna(subset=["uetr"])
        .dropDuplicates(["uetr"])
    )

    labelled = silver.join(typology_map, on="uetr", how="left").withColumn(
        "label",
        when(col("typology_type").isNull(), lit("baseline")).otherwise(col("typology_type")),
    )

    # Aggregate to entity-day. Bucket key is ``to_date(txn_timestamp)``,
    # NOT ``dayofmonth`` -- the latter collapses Jan 15 + Feb 15
    # into a single ``day=15`` groupBy key, mashing unrelated
    # transactions and destroying the signal the model tries to
    # learn (P1 finding from PR-A adversarial review). AML datagen
    # spans multi-week windows by design (CLAUDE.md gotcha 17).
    # Group by entity-day ONLY. Grouping by the label as well split a planted
    # entity-day into a typology-only row and a baseline-only row, so the
    # features described the planted transactions in isolation: the model
    # was scored on groups the label itself had formed (self-scoring). A
    # real detector sees the account's day as a whole; the day is positive
    # when any of its transactions is planted.
    per_entity_day = (
        labelled.withColumn("day", to_date(col("txn_timestamp")))
        .groupBy("originator_id", "day")
        .agg(
            mean_(spark_log("txn_amount")).alias("log_amount_mean"),
            stddev(spark_log("txn_amount")).alias("log_amount_std"),
            mean_(hour("txn_timestamp")).alias("mean_hour"),
            stddev(hour("txn_timestamp")).alias("std_hour"),
            max_(col("typology_type")).alias("_typ"),
        )
        .withColumn("label", when(col("_typ").isNull(), lit("baseline")).otherwise(col("_typ")))
        .drop("_typ")
    )

    return per_entity_day


def _cap_and_pull(features_df, cap_rows: int):
    """Pull labelled features into a pandas DataFrame with a size cap.

    The cap protects the driver: at scale 100 the entity-day frame
    can be O(10^7) rows, which is bigger than a 32 GB driver can
    hold. Strategy: KEEP EVERY TYPOLOGY ROW, then fill the remaining
    budget with a random sample of baseline rows. Typologies are
    ~0.1 % of rows per REQ-G-03, so 200K cap * 0.1 % = 200 typology
    rows if we sampled uniformly -- spread across 10+ typology_types
    that is 20 per class average and rare typologies (e.g. random,
    corridor_high_risk) fall below the ``min_positive_per_class``
    threshold and ``INSUFFICIENT_LABELS`` becomes the default
    verdict at production scale. Keeping all typology rows fixes
    that (P2 fix from PR-A adversarial review).

    Also caches the feature frame before the size probes so the
    Spark plan runs once, not three times (P2 same review).
    """
    # Cache: three actions coming (count, filter+count, filter+sample+toPandas).
    features_df = features_df.cache()
    total = features_df.count()
    if total <= cap_rows:
        pdf = features_df.withColumn("sample_weight", lit(1.0)).toPandas()
        features_df.unpersist()
        return pdf

    typology_df = features_df.filter(col("label") != lit("baseline"))
    baseline_df = features_df.filter(col("label") == lit("baseline"))
    typology_count = typology_df.count()

    # If typology rows alone exceed the cap, we're in an unusual
    # situation (very small dataset with mostly typologies) -- fall
    # back to a proportional sample, still keeping all typologies.
    if typology_count >= cap_rows:
        pdf = typology_df.withColumn("sample_weight", lit(1.0)).toPandas()
        features_df.unpersist()
        return pdf

    baseline_budget = max(0, cap_rows - typology_count)
    total_baseline = total - typology_count
    if total_baseline == 0:
        # No baseline rows at all -- just pull typology.
        pdf = typology_df.withColumn("sample_weight", lit(1.0)).toPandas()
    else:
        baseline_frac = min(1.0, float(baseline_budget) / float(total_baseline))
        baseline_sample = baseline_df.sample(withReplacement=False, fraction=baseline_frac, seed=0)
        # Each kept baseline row stands for 1 / fraction real ones, so the
        # evaluation counts false positives at the true prevalence.
        pdf = (
            typology_df.withColumn("sample_weight", lit(1.0))
            .union(baseline_sample.withColumn("sample_weight", lit(1.0 / baseline_frac)))
            .toPandas()
        )

    features_df.unpersist()
    return pdf


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Leakage gate + reference-detector metrics for AML. "
            "See docs/design/namespace-isolation.md for context on why."
        )
    )
    parser.add_argument("--manifest", required=True, help="S3 URI to manifest.parquet")
    parser.add_argument(
        "--output-prefix",
        required=True,
        help="S3 URI prefix. Writes {prefix}/leakage_report.parquet and "
        "{prefix}/reference_metrics.parquet.",
    )
    parser.add_argument(
        "--leakage-threshold",
        type=float,
        default=0.10,
        help="Minimum baseline/typology ratio inside a structuring band "
        "for that band to pass the leakage gate. Default 0.10.",
    )
    parser.add_argument(
        "--driver-sample-cap",
        type=int,
        default=200_000,
        help="Maximum labelled feature rows to pull into the driver for "
        "sklearn training. Sampled per-label so rare typologies stay in.",
    )
    parser.add_argument(
        "--silver-txns",
        default=SILVER_TXNS,
        help="silver.transactions table name (default: env LB_FINANCIAL_SILVER_TXNS).",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("lb-score-financial-reference").getOrCreate()
    log("=" * 60)
    log("AML reference detector + leakage gate")
    log(f"Manifest:        {args.manifest}")
    log(f"Output prefix:   {args.output_prefix}")
    log(f"Silver table:    {CATALOG}.{args.silver_txns}")
    log(f"Threshold:       {args.leakage_threshold}")
    log("=" * 60)

    manifest = spark.read.parquet(args.manifest)
    manifest_n = manifest.count()
    if manifest_n == 0:
        raise SystemExit(
            f"Manifest {args.manifest} empty; run datagen first. Without "
            "ground truth the reference detector cannot label rows."
        )
    log(f"Manifest instances: {manifest_n:,}")

    # ---------- Leakage gate ----------
    from reference_score import compute_leakage_gate

    band_rows = _compute_leakage_bands(spark, args.silver_txns, manifest)
    leakage_report = compute_leakage_gate(band_rows, threshold_ratio=args.leakage_threshold)
    log(f"Leakage gate: overall_pass={leakage_report.overall_pass}")
    for row in leakage_report.rows:
        log(
            f"  {row.currency} [{row.band_lo:.0f},{row.band_hi:.0f}] "
            f"baseline={row.baseline_count} typology={row.typology_count} "
            f"ratio={row.ratio_baseline_over_typology:.3f} verdict={row.verdict.value}"
        )

    leakage_df = spark.createDataFrame(leakage_report.as_dicts())
    leakage_out = f"{args.output_prefix.rstrip('/')}/leakage_report.parquet"
    leakage_df.write.mode("overwrite").parquet(leakage_out)
    log(f"Wrote {leakage_out}")

    # ---------- Reference detector ----------
    from reference_score import (
        LEAKY_FEATURES,
        ReferenceModelVerdict,
        train_reference_gbt,
    )

    # Build labelled features. If we cannot even build them (silver
    # missing columns, empty), record NO_SKLEARN-shaped verdict with
    # a note and continue -- the leakage gate has still shipped.
    try:
        features_df = _build_reference_feature_frame(spark, args.silver_txns, manifest)
    except Exception as e:  # noqa: BLE001
        log(f"WARN: feature frame build failed: {e}; skipping reference model.")
        _write_reference_stub(
            spark, args.output_prefix, verdict="no_sklearn", note=f"feature build failed: {e}"
        )
        spark.stop()
        return

    pdf = _cap_and_pull(features_df, cap_rows=args.driver_sample_cap)
    log(f"Reference feature rows pulled to driver: {len(pdf):,}")

    if pdf.empty:
        _write_reference_stub(
            spark, args.output_prefix, verdict="no_sklearn", note="empty feature frame"
        )
        spark.stop()
        return

    labels = pdf["label"]
    # Feature set is deliberately narrow -- see the docstring on
    # _build_reference_feature_frame for why txn_count,
    # unique_counterparties, and any country feature are excluded.
    feature_cols = [
        "log_amount_mean",
        "log_amount_std",
        "mean_hour",
        "std_hour",
    ]
    features = pdf[feature_cols].fillna(0.0)

    # Runtime guard, not assert -- assert becomes a no-op under
    # python -O and the P3 finding from PR-A adversarial review
    # correctly flagged this as fragile even though Spark drivers
    # don't run -O today.
    leaks = set(features.columns) & LEAKY_FEATURES
    if leaks:
        raise RuntimeError(
            f"BUG: feature frame contains leaky columns {leaks!r}. "
            "This should have been caught by train_reference_gbt's own "
            "leak check -- if you reached here, the LEAKY_FEATURES set "
            "in reference_score.py (packaged flat on the driver) is out of sync with what this Spark "
            "script builds."
        )

    report = train_reference_gbt(
        features,
        labels,
        groups=pdf["originator_id"],
        sample_weight=pdf["sample_weight"],
    )
    log(f"Reference model verdict: {report.verdict.value}")
    log(
        f"Overall: precision={report.overall_precision:.3f} "
        f"recall={report.overall_recall:.3f} f1={report.overall_f1:.3f} "
        f"n_train={report.n_train} n_test={report.n_test}"
    )
    for r in report.per_typology:
        log(
            f"  {r.typology_type}: support={r.support} "
            f"precision={r.precision:.3f} recall={r.recall:.3f} f1={r.f1:.3f}"
        )

    # Serialise: header row + per-typology rows so a single parquet
    # can be joined against rule recall.
    header = {
        "row_kind": "aggregate",
        "typology_type": None,
        "verdict": report.verdict.value,
        "precision": report.overall_precision,
        "recall": report.overall_recall,
        "f1": report.overall_f1,
        "support": report.n_test,
        "note": report.note,
        "feature_names_json": json.dumps(list(report.feature_names)),
        "excluded_features_json": json.dumps(list(report.excluded_features)),
    }
    detail_rows = [
        {
            "row_kind": "typology",
            "typology_type": r.typology_type,
            "verdict": report.verdict.value,
            "precision": r.precision,
            "recall": r.recall,
            "f1": r.f1,
            "support": r.support,
            "note": None,
            "feature_names_json": None,
            "excluded_features_json": None,
        }
        for r in report.per_typology
    ]
    combined = spark.createDataFrame([header] + detail_rows)
    ref_out = f"{args.output_prefix.rstrip('/')}/reference_metrics.parquet"
    combined.write.mode("overwrite").parquet(ref_out)
    log(f"Wrote {ref_out}")

    if report.verdict is ReferenceModelVerdict.NO_SKLEARN:
        log(
            "Reference detector SKIPPED (scikit-learn not installed on driver). "
            "The leakage gate still ran. Install scikit-learn on the Spark "
            "image (e.g. add `scikit-learn>=1.3` to the driver deps) to "
            "enable per-typology model recall."
        )

    spark.stop()


def _write_reference_stub(spark, output_prefix: str, *, verdict: str, note: str) -> None:
    """Emit a placeholder reference_metrics.parquet so downstream
    aggregations can join without a "file not found" error."""
    row = {
        "row_kind": "aggregate",
        "typology_type": None,
        "verdict": verdict,
        "precision": 0.0,
        "recall": 0.0,
        "f1": 0.0,
        "support": 0,
        "note": note,
        "feature_names_json": None,
        "excluded_features_json": None,
    }
    df = spark.createDataFrame([row])
    out = f"{output_prefix.rstrip('/')}/reference_metrics.parquet"
    df.write.mode("overwrite").parquet(out)
    log(f"Wrote stub {out} (verdict={verdict})")


if __name__ == "__main__":
    main()
