#!/usr/bin/env python3
"""AML D8 scale-invariance verdict from two fidelity-gate reports.

Compares the gate-scale run (small) against a larger-scale run of the same
seed (large): per behavioural typology, the bootstrap CI of AP_large -
AP_small must lie inside the pre-registered bound, and per feature the KS
statistic must be under the pre-registered cap. See
src/lakebench/aml/scale_invariance.py for the method; every tolerance comes
from aml_preregistration.json (R7).

Inputs are gate reports as local paths or s3:// / s3a:// URIs: the local
runner's ``--out`` JSON (scripts/aml_gate.py) and the cluster job's
``<output-prefix>/aml_gate_report.json``. Each report must list its
``oof_scores`` and ``unit_features`` model outputs. For S3, credentials come
from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY and the endpoint from
``--s3-endpoint`` or AWS_ENDPOINT_URL.

    python3.11 scripts/aml_d8.py --small /scratch/s2/gate.json \\
        --large s3a://lb-gold/aml/aml_gate_report.json \\
        --s3-endpoint http://10.21.227.93:80 --out d8.json

Exit status: 0 D8 passes, 2 D8 ran and fails, 1 error (an input could not be
read; the verdict JSON is still written and says why).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--small", required=True, help="gate report at corpora.gate_scale")
    ap.add_argument("--large", required=True, help="gate report at the larger scale, same seed")
    ap.add_argument("--out", type=Path, default=None, help="write the verdict JSON here")
    ap.add_argument("--prereg", default=None, help="pre-registration JSON path")
    ap.add_argument("--s3-endpoint", default=None, help="S3 endpoint URL for s3:// inputs")
    args = ap.parse_args(argv)

    from lakebench.aml.scale_invariance import evaluate_d8, summary_lines

    verdict = evaluate_d8(
        args.small, args.large, prereg_path=args.prereg, endpoint=args.s3_endpoint
    )
    for line in summary_lines(verdict):
        print(line)
    text = json.dumps(verdict, indent=2, default=str)
    if args.out:
        args.out.write_text(text + "\n")
        print(f"wrote {args.out}")
    else:
        print(text)
    if verdict.get("verdict") == "pass" and verdict.get("pass") is True:
        return 0
    if verdict.get("verdict") == "fail":
        return 2
    return 1


if __name__ == "__main__":
    sys.exit(main())
