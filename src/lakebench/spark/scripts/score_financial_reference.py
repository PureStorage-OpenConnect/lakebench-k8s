"""Score (Financial, reference) -- leakage gate + the pre-registered AML fidelity gate.

The cluster half of the AML fidelity gate (AML-GOALS D9, and A6 against the
local harness scripts/aml_gate.py). Two independent things this script does:

1. **Band leakage gate** -- compares baseline transaction density against
   typology density inside each currency-specific structuring band. If
   baseline density is < 10 % of typology density in a band, the band
   effectively IS the label. Emits ``leakage_report.parquet`` with a row per
   (currency, band).

2. **Fidelity gate** -- builds the pre-registered per-entity features from
   silver (``aml_features.silver_frames`` + ``entity_features``, the same
   feature code the local harness runs over bronze), labels each customer by
   manifest participation, and runs ``gate.evaluate_gate``: out-of-fold AP per
   in-scope typology with a bootstrap CI and n_positives, the D5 single and
   pair shortcuts against the leakage caps, the definitional check, the D7
   band and K-of-N summary, D2 timing mixture and D11 density. Every constant
   comes from ``aml_preregistration.json`` (packaged flat next to this script).
   Emits ``aml_gate_report.json`` (the full report) and
   ``reference_metrics.parquet`` (one aggregate row plus one row per typology,
   the table ``aggregate_reference_vs_rule.sql`` joins against rule recall).

scikit-learn is not on the Spark image; job.py installs it per job
(REFERENCE_PY_DEPS). Without it the report carries verdict ``no_sklearn`` and
the band leakage gate still runs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from common import env, log
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, when
from pyspark.sql.functions import count as count_

# numpy/pandas/scikit-learn (imported inside functions) are installed per job into this directory by an
# init container (job.py REFERENCE_PY_DEPS); the Spark image has none of them.
_PYDEPS = os.environ.get("LB_PYDEPS_DIR", "/opt/lb-pydeps")
if os.path.isdir(_PYDEPS) and _PYDEPS not in sys.path:
    sys.path.insert(0, _PYDEPS)

CATALOG = env("LB_ICEBERG_CATALOG", "lakehouse")
SILVER_TXNS = env("LB_FINANCIAL_SILVER_TXNS", "silver.transactions")
SILVER_ENTITIES = env("LB_FINANCIAL_SILVER_ENTITIES", "silver.entities")
SILVER_ACCOUNTS = env("LB_FINANCIAL_SILVER_ACCOUNTS", "silver.accounts")
_BRONZE_URI = env("LB_BRONZE_URI", "s3a://lb-bronze/")
_BRONZE_ROOT = env("LB_FINANCIAL_BRONZE_PREFIX", "pacs008/").rstrip("/")
ACCOUNT_PATH = env(
    "LB_FINANCIAL_ACCOUNT_PATH", f"{_BRONZE_URI}{_BRONZE_ROOT}/bronze/account.parquet"
)

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

#: reference_metrics.parquet columns (aggregate_reference_vs_rule.sql reads
#: row_kind, typology_type, verdict, precision, recall, f1).
_METRIC_SCHEMA = (
    "row_kind string, typology_type string, verdict string, precision double, "
    "recall double, f1 double, support long, r_precision double, ap double, ap_ci_lo double, "
    "ap_ci_hi double, n_positives long, in_band boolean, leakage_pass boolean, "
    "note string, feature_names_json string, excluded_features_json string"
)


def _compute_leakage_bands(spark: SparkSession, silver_txns_name: str, manifest) -> list[dict]:
    """Aggregate baseline vs typology counts per (currency, band).

    A transaction is "typology" if its UETR appears in any manifest
    row's ``participant_uetrs``. Otherwise it's baseline. The count
    of interest is *rows with txn_amount inside the band* on each
    side.
    """
    silver = spark.table(f"{CATALOG}.{silver_txns_name}")

    typology_uetrs = manifest.select(explode(col("participant_uetrs")).alias("uetr")).distinct()

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


def _cap_customers(features, labels, typologies, cap_rows: int):
    """Keep every labelled customer and a seeded sample of the rest when the
    customer count exceeds ``cap_rows`` (driver memory). Each kept negative
    carries weight 1 / fraction, which the gate uses in fitting and in AP, so
    AP reflects the true prevalence."""
    import aml_features as af

    cust = features.filter(col("is_customer")).cache()
    n = cust.count()
    if n <= cap_rows:
        pdf = af.gate_frame(cust, labels, typologies)
        pdf["weight"] = 1.0
        return pdf, n, 1.0
    pos_keys = labels.filter(col("typology_type").isin(*typologies)).select("key").distinct()
    pos = cust.join(pos_keys, "key", "left_semi")
    neg = cust.join(pos_keys, "key", "left_anti")
    n_pos = pos.count()
    frac = min(1.0, max(0.0, (cap_rows - n_pos) / max(1, n - n_pos)))
    kept = pos.withColumn("weight", lit(1.0)).unionByName(
        neg.sample(withReplacement=False, fraction=frac, seed=0).withColumn(
            "weight", lit(1.0 / frac) if frac > 0 else lit(0.0)
        )
    )
    pdf = af.gate_frame(kept, labels, typologies)
    cust.unpersist()
    return pdf, n, frac


def _write_text(spark, uri: str, text: str) -> None:
    """Write one file at ``uri`` through the Hadoop FileSystem API."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    out = fs.create(path, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()


def _metric_rows(report: dict) -> list[dict]:
    feats = json.dumps(report.get("features", []))
    header = {
        "row_kind": "aggregate",
        "typology_type": None,
        "verdict": report.get("verdict"),
        "precision": None,
        "recall": None,
        "f1": None,
        "support": report.get("n_scored_customers"),
        "r_precision": None,
        "ap": None,
        "ap_ci_lo": None,
        "ap_ci_hi": None,
        "n_positives": None,
        "in_band": None,
        "leakage_pass": (report.get("level2") or {}).get("all_pass_leakage"),
        "note": (
            "AML fidelity gate (prereg v{}). precision/recall/f1 are NULL: the "
            "gate scores customers by AP, which is not comparable to rule "
            "instance recall; r_precision is the customer-level cut that alerts "
            "on as many customers as there are positives".format(report.get("prereg_version"))
        ),
        "feature_names_json": feats,
        "excluded_features_json": json.dumps(["is_customer"]),
    }
    rows = [header]
    for t, r in (report.get("typologies") or {}).items():
        lo, hi = r.get("ap_ci") or [None, None]
        rows.append(
            {
                "row_kind": "typology",
                "typology_type": t,
                "verdict": r.get("status"),
                "precision": None,
                "recall": None,
                "f1": None,
                "support": r.get("n_positives"),
                "r_precision": r.get("r_precision"),
                "ap": r.get("ap"),
                "ap_ci_lo": lo,
                "ap_ci_hi": hi,
                "n_positives": r.get("n_positives"),
                "in_band": r.get("in_band"),
                "leakage_pass": r.get("leakage_pass"),
                "note": r.get("kind"),
                "feature_names_json": None,
                "excluded_features_json": None,
            }
        )
    return rows


def _write_metrics(spark, output_prefix: str, rows: list[dict]) -> None:
    df = spark.createDataFrame(rows, _METRIC_SCHEMA)
    out = f"{output_prefix.rstrip('/')}/reference_metrics.parquet"
    df.write.mode("overwrite").parquet(out)
    log(f"Wrote {out}")


def run_fidelity_gate(
    spark, manifest, *, cap_rows: int, provenance: dict, silver_txns: str | None = None
) -> dict:
    """Build silver features and evaluate the gate. Returns the report dict."""
    import aml_features as af
    from fidelity_gate import evaluate_gate, in_scope_typologies, load_preregistration

    prereg, sha = load_preregistration()
    typologies = in_scope_typologies(prereg)
    txns, ents, id_map = af.silver_frames(
        spark,
        catalog=CATALOG,
        txns_table=silver_txns or SILVER_TXNS,
        entities_table=SILVER_ENTITIES,
        accounts_table=SILVER_ACCOUNTS,
        account_path=ACCOUNT_PATH,
    )
    txns = txns.cache()
    features = af.entity_features(txns, ents).cache()
    role = prereg["unit_of_scoring"]["label_role"]
    labels = af.labels_for_role(spark, manifest, id_map, role).cache()
    tm = prereg["timing_mixture"]
    timing = af.timing_mixture_counts(
        features,
        cohort_min_sends=tm["cohort_min_sends"],
        low_cv_edge=tm["low_cv_edge"],
        high_cv_edge=tm["high_cv_edge"],
    )
    density = af.typology_density(txns, manifest)
    agreement = af.label_agreement(
        af.labels_from_participants(manifest, id_map).join(
            features.filter(col("is_customer")).select("key"), "key", "left_semi"
        ),
        af.labels_from_uetrs(manifest, txns).join(
            features.filter(col("is_customer")).select("key"), "key", "left_semi"
        ),
    )
    pdf, n_customers, frac = _cap_customers(features, labels, typologies, cap_rows)
    log(f"Customers: {n_customers:,}; pulled to driver: {len(pdf):,} (negative fraction {frac})")
    report = evaluate_gate(
        pdf,
        prereg,
        prereg_sha256=sha,
        timing_counts=timing,
        density_counts=density,
        provenance={
            **provenance,
            **af.manifest_provenance(manifest),
            "adapter": "silver",
            "label_role": role,
            "aml_features_sha256": af.source_sha256(),
            "n_customers": n_customers,
            "negative_sample_fraction": frac,
            "label_route_agreement_customers": agreement,
        },
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Leakage gate + AML fidelity gate (silver).")
    parser.add_argument("--manifest", required=True, help="S3 URI to manifest.parquet")
    parser.add_argument(
        "--output-prefix",
        required=True,
        help="S3 URI prefix. Writes leakage_report.parquet, reference_metrics.parquet "
        "and aml_gate_report.json under it.",
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
        default=1_000_000,
        help="Maximum customers pulled to the driver. Above it every labelled "
        "customer is kept and the rest are sampled with inverse-fraction weights.",
    )
    parser.add_argument(
        "--silver-txns",
        default=SILVER_TXNS,
        help="silver.transactions table name (default: env LB_FINANCIAL_SILVER_TXNS).",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("lb-score-financial-reference").getOrCreate()
    # hour/dayofweek/to_date features follow the session zone; the other
    # financial scripts and the local harness all run in UTC (A6).
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    prefix = args.output_prefix.rstrip("/")
    log("=" * 60)
    log("AML band leakage gate + fidelity gate")
    log(f"Manifest:        {args.manifest}")
    log(f"Output prefix:   {prefix}")
    log(f"Silver table:    {CATALOG}.{args.silver_txns}")
    log("=" * 60)

    import aml_features as af

    manifest_src = af.manifest_glob(args.manifest)
    manifest = spark.read.parquet(manifest_src)
    manifest_n = manifest.count()
    if manifest_n == 0:
        raise SystemExit(
            f"Manifest {args.manifest} empty; run datagen first. Without "
            "ground truth the reference detector cannot label rows."
        )
    log(f"Manifest instances: {manifest_n:,} ({manifest_src})")

    # ---------- Band leakage gate ----------
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
    leakage_out = f"{prefix}/leakage_report.parquet"
    spark.createDataFrame(leakage_report.as_dicts()).write.mode("overwrite").parquet(leakage_out)
    log(f"Wrote {leakage_out}")

    # ---------- Fidelity gate ----------
    provenance = {
        "git_sha": os.environ.get("LB_GIT_SHA", "unknown"),
        "corpus_seed": os.environ.get("LB_DATAGEN_SEED"),
        "manifest": manifest_src,
        "silver_txns": f"{CATALOG}.{args.silver_txns}",
    }
    try:
        report = run_fidelity_gate(
            spark,
            manifest,
            cap_rows=args.driver_sample_cap,
            provenance=provenance,
            silver_txns=args.silver_txns,
        )
    except Exception as e:  # noqa: BLE001 -- the band gate has shipped; record why this did not
        log(f"WARN: fidelity gate failed: {e}")
        report = {"gate": "aml-fidelity", "verdict": "error", "note": str(e)}
    report["band_leakage_overall_pass"] = leakage_report.overall_pass

    try:
        from fidelity_gate import summary_lines

        for line in summary_lines(report):
            log(line)
    except Exception:  # noqa: BLE001 -- logging only
        pass
    text = json.dumps(report, indent=2, default=str)
    report_out = f"{prefix}/aml_gate_report.json"
    _write_text(spark, report_out, text)
    log(f"Wrote {report_out}")
    _write_metrics(spark, prefix, _metric_rows(report))
    if report.get("verdict") == "no_sklearn":
        log(
            "Fidelity gate SKIPPED (scikit-learn not importable on the driver). "
            "The band leakage gate still ran."
        )
    spark.stop()
    if report.get("verdict") == "error":
        # Outputs are written, but a crashed gate must not read as a pass (R6).
        raise SystemExit("fidelity gate failed; see aml_gate_report.json")


if __name__ == "__main__":
    main()
