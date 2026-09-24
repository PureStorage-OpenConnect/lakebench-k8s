#!/usr/bin/env python3
"""Local AML fidelity gate over a raw datagen corpus (AML-GOALS R7, A6).

Runs the bronze adapter of ``aml_features`` over a local corpus in a local
Spark session, then ``lakebench.aml.fidelity_gate.evaluate_gate``, and writes
the same JSON report the cluster job (score_financial_reference.py, silver
adapter) writes. Every gate constant comes from aml_preregistration.json.

The corpus directory is what the generator writes under ``--prefix`` with
DG_LOCAL_DIR set: ``bronze/pacs008/part-*.parquet``, ``bronze/party.parquet``,
``bronze/account.parquet`` and ``manifest/manifest.parquet``.

Usage (needs pyspark, a JDK, numpy, pandas and scikit-learn):

    DG_LOCAL_DIR=/scratch/c datagen_rs/target/release/generate \\
        --bucket bronze --prefix pacs008/ --seed 42 --scale 1
    python scripts/aml_gate.py /scratch/c/bronze/pacs008 --seed 42 --out gate.json

``--seed`` is recorded as provenance only; the corpus does not carry its
top-level seed.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src/lakebench/spark/scripts"))


def _git_sha() -> str:
    try:
        sha = subprocess.run(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "-C", str(ROOT), "status", "--porcelain", "--untracked-files=no"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        return sha + ("-dirty" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("corpus", type=Path, help="corpus root (contains bronze/ and manifest/)")
    ap.add_argument("--seed", type=int, default=None, help="corpus seed (provenance only)")
    ap.add_argument("--out", type=Path, default=None, help="write the JSON report here")
    ap.add_argument("--prereg", type=Path, default=None, help="pre-registration JSON path")
    ap.add_argument("--driver-memory", default="12g")
    ap.add_argument("--master", default="local[*]")
    args = ap.parse_args(argv)

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    import aml_features as af
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    from lakebench.aml.fidelity_gate import (
        evaluate_gate,
        in_scope_typologies,
        load_preregistration,
        summary_lines,
    )

    prereg, sha = load_preregistration(args.prereg)
    typologies = in_scope_typologies(prereg)
    corpus = args.corpus.resolve()
    spark = (
        SparkSession.builder.master(args.master)
        .appName("lb-aml-gate-local")
        .config("spark.driver.memory", args.driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    t0 = time.time()
    try:
        manifest = spark.read.parquet(str(corpus / "manifest/manifest.parquet"))
        txns, ents, id_map = af.bronze_frames(
            spark,
            pacs_path=str(corpus / "bronze/pacs008"),
            party_path=str(corpus / "bronze/party.parquet"),
            account_path=str(corpus / "bronze/account.parquet"),
        )
        txns = txns.cache()
        features = af.entity_features(txns, ents).cache()
        labels = af.labels_from_participants(manifest, id_map).cache()
        tm = prereg["timing_mixture"]
        timing = af.timing_mixture_counts(
            features,
            cohort_min_sends=tm["cohort_min_sends"],
            low_cv_edge=tm["low_cv_edge"],
            high_cv_edge=tm["high_cv_edge"],
        )
        density = af.typology_density(txns, manifest)
        cust_keys = features.filter(col("is_customer")).select("key")
        agreement = af.label_agreement(
            labels.join(cust_keys, "key", "left_semi"),
            af.labels_from_uetrs(manifest, txns).join(cust_keys, "key", "left_semi"),
        )
        pdf = af.gate_frame(features, labels, typologies)
        prov = {
            "adapter": "bronze",
            "corpus": str(corpus),
            "corpus_seed": args.seed,
            "git_sha": _git_sha(),
            **af.manifest_provenance(manifest),
            "n_entities": int(features.count()),
            "label_route_agreement_customers": agreement,
        }
        t_spark = time.time() - t0
    finally:
        spark.stop()

    report = evaluate_gate(
        pdf,
        prereg,
        prereg_sha256=sha,
        timing_counts=timing,
        density_counts=density,
        provenance={**prov, "spark_seconds": round(t_spark, 1)},
    )
    report["provenance"]["total_seconds"] = round(time.time() - t0, 1)
    for line in summary_lines(report):
        print(line)
    text = json.dumps(report, indent=2, default=str)
    if args.out:
        args.out.write_text(text + "\n")
        print(f"wrote {args.out}")
    else:
        print(text)
    return 0 if report.get("verdict") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
