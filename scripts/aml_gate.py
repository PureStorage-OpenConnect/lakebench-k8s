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

``--seed`` is checked against the manifest's instance seeds and recorded.

Library versions must match the cluster's (A6 compares like with like): the
runner refuses to score when numpy, scipy, pandas, scikit-learn, joblib or
threadpoolctl differ from REFERENCE_PY_DEPS in
src/lakebench/modules/pipeline_engines/spark/job.py, unless
``--allow-version-mismatch`` is given, in which case the mismatch is recorded
and ``passes.library_versions_match`` fails. Build a matching environment
with (Python 3.11; the cluster driver runs 3.10, which the report records):

    python3.11 -m venv /scratch/aml-gate-venv
    /scratch/aml-gate-venv/bin/pip install pyspark==4.0.1 pyarrow \
        $(python3.11 scripts/aml_gate.py --print-pinned-deps)

and point JAVA_HOME at a JDK 17.
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


JOB_PY = ROOT / "src/lakebench/modules/pipeline_engines/spark/job.py"
#: Pinned packages whose version must match; the rest of REFERENCE_PY_DEPS are
#: pure-Python helpers that do not change a number.
_CHECKED = {"numpy", "scipy", "pandas", "scikit-learn", "joblib", "threadpoolctl"}
_MODULE_OF = {"scikit-learn": "sklearn"}


def pinned_deps() -> dict[str, str]:
    """REFERENCE_PY_DEPS from job.py, read from source (importing job.py
    would pull the whole CLI dependency tree into the Spark venv)."""
    import ast

    tree = ast.parse(JOB_PY.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "REFERENCE_PY_DEPS" for t in node.targets
        ):
            pins = [e.value for e in node.value.elts]
            return dict(p.split("==", 1) for p in pins)
    raise RuntimeError(f"REFERENCE_PY_DEPS not found in {JOB_PY}")


def version_mismatches(installed: dict) -> dict[str, dict]:
    """{package: {pinned, installed}} for every checked package that differs."""
    out = {}
    for pkg, want in pinned_deps().items():
        if pkg not in _CHECKED:
            continue
        have = installed.get(_MODULE_OF.get(pkg, pkg))
        if have != want:
            out[pkg] = {"pinned": want, "installed": have}
    return out


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
    if argv is None and "--print-pinned-deps" in sys.argv[1:]:
        print(" ".join(f"{k}=={v}" for k, v in pinned_deps().items()))
        return 0
    ap.add_argument("corpus", type=Path, help="corpus root (contains bronze/ and manifest/)")
    ap.add_argument("--seed", type=int, default=None, help="corpus seed (provenance only)")
    ap.add_argument("--out", type=Path, default=None, help="write the JSON report here")
    ap.add_argument("--prereg", type=Path, default=None, help="pre-registration JSON path")
    ap.add_argument("--driver-memory", default="12g")
    ap.add_argument("--master", default="local[*]")
    ap.add_argument(
        "--label-role",
        choices=["participant", "subject"],
        default=None,
        help="override the pre-registered unit_of_scoring.label_role (diagnostic only; "
        "the report records the override)",
    )
    ap.add_argument(
        "--allow-version-mismatch",
        action="store_true",
        help="score even when library versions differ from the cluster pins (recorded, fails "
        "passes.library_versions_match)",
    )
    ap.add_argument(
        "--diagnostic",
        action="store_true",
        help="allow a corpus whose scale is not corpora.gate_scale; the report is marked "
        "diagnostic and passes.corpus_at_gate_scale fails",
    )
    ap.add_argument(
        "--counts-only",
        action="store_true",
        help="build units and labels and report counts only: no model, no AP (smoke tests "
        "on a unit that must not be looked at before its first registered gate run)",
    )
    ap.add_argument(
        "--require-pass",
        action="store_true",
        help="exit 2 unless every gate passes (passes.all); default exit 0 when the gate ran",
    )
    args = ap.parse_args(argv)

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    import aml_features as af
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col

    from lakebench.aml.fidelity_gate import (
        add_pass,
        evaluate_gate,
        in_scope_typologies,
        library_versions,
        lifetime_prereg,
        load_preregistration,
        summary_lines,
        write_model_outputs,
    )

    mismatch = version_mismatches(library_versions())
    if mismatch:
        print(f"LIBRARY VERSION MISMATCH vs REFERENCE_PY_DEPS: {mismatch}", file=sys.stderr)
        if not args.allow_version_mismatch:
            print("refusing to score; see the docstring for a pinned env", file=sys.stderr)
            return 1
    prereg, sha = load_preregistration(args.prereg)
    typologies = in_scope_typologies(prereg)
    corpus = args.corpus.resolve()
    spark = (
        SparkSession.builder.master(args.master)
        .appName("lb-aml-gate-local")
        .config("spark.driver.memory", args.driver_memory)
        # The monthly unit's frame is collected whole: at scale 2 its Arrow
        # batches pass 1 GiB (Spark's default cap), so the cap follows the
        # driver's memory instead.
        .config("spark.driver.maxResultSize", args.driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        # Arrow for toPandas: the monthly unit pulls millions of units, which
        # the row-by-row path converts far more slowly. Measured at scale 0.1
        # (seed 7777): identical primary and lifetime frames, values and
        # dtypes, with build_gate_inputs at 27 s against 107 s.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    t0 = time.time()
    try:
        manifest_src = af.manifest_glob(str(corpus / "manifest/manifest.parquet"))
        manifest = af.read_manifest(spark, manifest_src)
        af.check_manifest(manifest)
        seed_check = af.corpus_seed_check(manifest, args.seed)
        scale_info = af.corpus_scale(
            spark,
            str(corpus / "bronze/account.parquet"),
            prereg["corpora"]["entities_per_scale_unit"],
        )
        at_scale = af.at_gate_scale(scale_info, prereg)
        if not at_scale and not args.diagnostic:
            print(
                f"corpus scale {scale_info['scale']} is not the gate scale "
                f"{prereg['corpora']['gate_scale']}; refusing (use --diagnostic)",
                file=sys.stderr,
            )
            return 1
        txns, ents, id_map = af.bronze_frames(
            spark,
            pacs_path=str(corpus / "bronze/pacs008"),
            party_path=str(corpus / "bronze/party.parquet"),
            account_path=str(corpus / "bronze/account.parquet"),
        )
        txns = txns.cache()
        registered_role = prereg["unit_of_scoring"]["label_role"]
        role = args.label_role or registered_role
        inputs = af.build_gate_inputs(
            spark,
            prereg,
            txns=txns,
            ents=ents,
            id_map=id_map,
            manifest=manifest,
            role=role,
            typologies=typologies,
        )
        features = inputs["lifetime_features"]
        by_participant = af.labels_from_participants(manifest, id_map)
        tm = prereg["timing_mixture"]
        timing = af.timing_mixture_counts(
            features,
            cohort_min_sends=tm["cohort_min_sends"],
            low_cv_edge=tm["low_cv_edge"],
            high_cv_edge=tm["high_cv_edge"],
        )
        density = af.typology_density(txns, manifest)
        unkeyed = af.unkeyed_rows(txns)
        dup_ibans = af.duplicate_ibans(spark, str(corpus / "bronze/account.parquet"))
        cust_keys = features.filter(col("is_customer")).select("key")
        agreement = af.label_agreement(
            by_participant.join(cust_keys, "key", "left_semi"),
            af.labels_from_uetrs(manifest, txns).join(cust_keys, "key", "left_semi"),
        )
        prov = {
            "adapter": "bronze",
            "manifest": manifest_src,
            "label_role": role,
            "label_role_overridden": role != registered_role,
            "library_version_mismatch": mismatch,
            "unkeyed_rows": unkeyed,
            "duplicate_ibans": dup_ibans,
            "corpus_seed_check": seed_check,
            "corpus_scale": scale_info,
            "diagnostic": bool(args.diagnostic),
            "aml_features_sha256": af.source_sha256(),
            "corpus": str(corpus),
            "corpus_seed": args.seed,
            "git_sha": _git_sha(),
            **af.manifest_provenance(manifest),
            "n_entities": int(features.count()),
            "n_payments": int(txns.count()),
            "label_route_agreement_customers": agreement,
        }
        t_spark = time.time() - t0
    finally:
        spark.stop()

    report = evaluate_gate(
        inputs["primary"],
        prereg,
        prereg_sha256=sha,
        timing_counts=timing,
        density_counts=density,
        provenance={**prov, "spark_seconds": round(t_spark, 1)},
        score=not args.counts_only,
        collect_outputs=args.out is not None and not args.counts_only,
    )
    outputs = report.pop("_model_outputs", None)
    if outputs is not None:
        base = str(args.out.with_suffix(""))
        # Written before the report, so a failure here (a full disk) must be
        # recorded rather than lose the gate numbers.
        try:
            paths = write_model_outputs(outputs, base)
            report["model_outputs"] = {
                name: {"path": p, "bytes": os.path.getsize(p)} for name, p in paths.items()
            }
            report["model_outputs"]["oof_scores"]["rows"] = int(len(outputs["scores"]))
        except Exception as e:  # noqa: BLE001
            report["model_outputs"] = {"error": str(e)}
    report["unit_detail"] = inputs["unit"]
    if "secondary_lifetime_error" in inputs:
        report["secondary_lifetime"] = {
            "gated": False,
            "verdict": "error",
            "note": inputs["secondary_lifetime_error"],
        }
    if "secondary_lifetime" in inputs:
        # Ungated: the lifetime unit kept for comparison, never in passes; its
        # failure must not lose the gated report.
        try:
            sec = evaluate_gate(
                inputs["secondary_lifetime"], lifetime_prereg(prereg), score=not args.counts_only
            )
        except Exception as e:  # noqa: BLE001
            sec = {"verdict": "error", "note": str(e)}
        report["secondary_lifetime"] = {
            "gated": False,
            **{
                k: sec.get(k)
                for k in (
                    "unit",
                    "verdict",
                    "note",
                    "n_scored_customers",
                    "n_scored_units",
                    "typologies",
                )
            },
        }
    report["provenance"]["total_seconds"] = round(time.time() - t0, 1)
    seed_ok = seed_check["matched_share"] == 1
    if args.seed is not None and not seed_ok:
        report["corpus_role"] = "unverified"
    if report.get("verdict") == "ok":
        n_unres = af.unresolved_subjects(inputs["unit"])
        add_pass(report, "corpus_fully_keyed", unkeyed == 0 and dup_ibans == 0 and n_unres == 0)
        add_pass(report, "library_versions_match", not mismatch)
        add_pass(report, "corpus_at_gate_scale", at_scale)
        if args.seed is not None:
            add_pass(report, "corpus_seed_verified", seed_ok)
        # A diagnostic run under another label role is never a pass.
        add_pass(report, "registered_label_role", role == registered_role)
    for line in summary_lines(report):
        print(line)
    text = json.dumps(report, indent=2, default=str)
    if args.out:
        args.out.write_text(text + "\n")
        print(f"wrote {args.out}")
    else:
        print(text)
    if args.counts_only:
        return 0 if report.get("verdict") == "counts_only" else 1
    if report.get("verdict") != "ok":
        return 1
    if args.require_pass and not report["passes"]["all"]:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
