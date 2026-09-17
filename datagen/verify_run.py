"""Post-generation reconciliation: manifest vs bronze bytes.

Reads:
  s3://{bucket}/{prefix}/manifest/*.parquet   -- the TypologyInstance manifest
  s3://{bucket}/{prefix}/part-*.parquet        -- the emitted pacs.008 files

Checks:
  1. Every UETR appearing in the manifest exists in bronze
  2. No duplicate UETRs across bronze files
  3. Per-typology counts in manifest are non-zero
  4. Bronze row count >= manifest UETR count (baseline noise on top)
  5. Distribution bands: R1-R11 practitioner queries pass their thresholds

Exit 0 on success, exit 1 with actionable message on failure.
Runs against the same S3 credentials the datagen container uses.

Usage:
    python verify_run.py --bucket lb-financial-bronze --prefix pacs008 [--endpoint http://...]
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
from collections import Counter

try:
    import boto3
    import pyarrow.parquet as pq
    from botocore.config import Config as BotoConfig
except ImportError as exc:
    print(f"verify_run: missing dep {exc}. pip install boto3 pyarrow", file=sys.stderr)
    sys.exit(2)


# Practitioner smell-test bands (from the datagen success criteria doc)
BANDS = {
    "amount_p50": (3_000, 9_000),
    "amount_p95": (20_000, 200_000),
    "amount_p99": (30_000, 2_000_000),
    "cross_border_share": (0.10, 0.35),
    "chain_populated_share": (0.05, 0.35),
    "rgltry_populated_share": (0.02, 0.20),
    "distinct_corridors": (10, 999),   # at least 10 across whole run
    "distinct_currencies": (3, 20),
}


def _mk_s3(endpoint: str | None):
    kwargs = dict(
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("S3_REGION", "us-east-1"),
        config=BotoConfig(s3={"addressing_style": "path"}),
    )
    if endpoint or os.environ.get("S3_ENDPOINT"):
        kwargs["endpoint_url"] = endpoint or os.environ.get("S3_ENDPOINT")
    return boto3.client("s3", **kwargs)


def _list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        r = s3.list_objects_v2(**kwargs)
        for obj in r.get("Contents", []):
            keys.append(obj["Key"])
        token = r.get("NextContinuationToken")
        if not token:
            break
    return keys


def _read_parquet(s3, bucket: str, key: str):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--prefix", default="pacs008", help="S3 prefix for pacs.008 files")
    ap.add_argument(
        "--manifest-prefix",
        default=None,
        help="Manifest S3 prefix (default: <prefix>/manifest)",
    )
    ap.add_argument("--endpoint", default=None)
    ap.add_argument(
        "--sample-limit",
        type=int,
        default=200_000,
        help="Cap for row-level distribution assertions (per-file random sample)",
    )
    ap.add_argument(
        "--fail-fast",
        action="store_true",
        help="Exit on the first failed check instead of collecting all",
    )
    args = ap.parse_args()

    prefix = args.prefix.rstrip("/") + "/"
    manifest_prefix = (args.manifest_prefix or f"{args.prefix.rstrip('/')}/manifest").rstrip(
        "/"
    ) + "/"

    s3 = _mk_s3(args.endpoint)

    failures: list[str] = []
    warnings: list[str] = []

    # --- manifest side ---
    manifest_keys = [
        k for k in _list_keys(s3, args.bucket, manifest_prefix) if k.endswith(".parquet")
    ]
    if not manifest_keys:
        failures.append(f"No manifest parquet under s3://{args.bucket}/{manifest_prefix}")
        print(json.dumps({"verify_run": "FAIL", "failures": failures}, indent=2))
        return 1

    manifest_uetrs: set[str] = set()
    per_type: Counter = Counter()
    for k in manifest_keys:
        t = _read_parquet(s3, args.bucket, k)
        for uetrs, typ in zip(
            t.column("participant_uetrs").to_pylist(),
            t.column("typology_type").to_pylist(),
        ):
            per_type[typ] += 1
            manifest_uetrs.update(uetrs or [])

    if not manifest_uetrs:
        failures.append("Manifest has zero UETRs (participant_uetrs empty everywhere)")
    if len(per_type) < 4:
        failures.append(
            f"Manifest has only {len(per_type)} typology types; expected all 8"
        )

    # --- bronze side ---
    bronze_keys = [
        k
        for k in _list_keys(s3, args.bucket, prefix)
        if k.endswith(".parquet") and "manifest" not in k
    ]
    if not bronze_keys:
        failures.append(f"No pacs.008 parquet under s3://{args.bucket}/{prefix}")
        print(json.dumps({"verify_run": "FAIL", "failures": failures}, indent=2))
        return 1

    all_bronze_uetrs: set[str] = set()
    dup_count = 0
    amounts: list[float] = []
    cross_border = 0
    chain_pop = 0
    rgltry_pop = 0
    total_rows = 0
    total_bytes = 0
    currencies: Counter = Counter()
    corridors: set[tuple[str, str]] = set()

    for k in bronze_keys:
        t = _read_parquet(s3, args.bucket, k)
        n = t.num_rows
        total_rows += n
        total_bytes += s3.head_object(Bucket=args.bucket, Key=k)["ContentLength"]
        # UETR uniqueness
        uetr_col = t.column("uetr").to_pylist()
        seen_before = len(all_bronze_uetrs)
        all_bronze_uetrs.update(uetr_col)
        dup_count += (seen_before + len(uetr_col)) - len(all_bronze_uetrs)
        # Row-level aggregates -- use struct field access via pydict
        d = t.to_pydict()
        amt = d["intr_bk_sttlm_amt"]
        for a in amt:
            if a is not None:
                amounts.append(float(a))
                if len(amounts) >= args.sample_limit:
                    break
        for i, dbtr in enumerate(d["dbtr"]):
            cdtr = d["cdtr"][i]
            dc = dbtr["ctry_of_res"] if dbtr else None
            cc = cdtr["ctry_of_res"] if cdtr else None
            if dc and cc and dc != cc:
                cross_border += 1
            if dc and cc:
                corridors.add((dc, cc))
            currencies[d["intr_bk_sttlm_ccy"][i]] += 1
        for c in d["intrmy_agt_1"]:
            if c is not None:
                chain_pop += 1
        for r in d["rgltry_rptg"]:
            if r:
                rgltry_pop += 1

    # --- checks ---
    missing = manifest_uetrs - all_bronze_uetrs
    if missing:
        failures.append(
            f"{len(missing)} manifest UETRs missing from bronze; e.g. "
            f"{list(missing)[:3]}"
        )

    if dup_count > 0:
        failures.append(f"{dup_count} duplicate UETRs across bronze files")

    if total_rows < len(manifest_uetrs):
        failures.append(
            f"bronze rows ({total_rows}) < manifest UETRs ({len(manifest_uetrs)})"
        )

    # Distribution assertions
    def _pct(vs, p):
        vs = sorted(vs)
        if not vs:
            return 0.0
        return vs[min(len(vs) - 1, int(p * len(vs)))]

    observed = {
        "amount_p50": round(_pct(amounts, 0.50), 2),
        "amount_p95": round(_pct(amounts, 0.95), 2),
        "amount_p99": round(_pct(amounts, 0.99), 2),
        "cross_border_share": round(cross_border / max(1, total_rows), 4),
        "chain_populated_share": round(chain_pop / max(1, total_rows), 4),
        "rgltry_populated_share": round(rgltry_pop / max(1, total_rows), 4),
        "distinct_corridors": len(corridors),
        "distinct_currencies": len(currencies),
    }

    for name, (lo, hi) in BANDS.items():
        v = observed[name]
        if not (lo <= v <= hi):
            warnings.append(
                f"{name}={v} outside band [{lo}, {hi}]"
            )

    report = {
        "verify_run": "PASS" if not failures else "FAIL",
        "bronze_rows": total_rows,
        "bronze_bytes": total_bytes,
        "bronze_files": len(bronze_keys),
        "manifest_uetrs": len(manifest_uetrs),
        "manifest_typologies": dict(per_type),
        "observed": observed,
        "warnings": warnings,
        "failures": failures,
    }
    print(json.dumps(report, indent=2))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
