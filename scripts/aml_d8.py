#!/usr/bin/env python3
"""AML D8 scale-invariance verdict from the s2 runs and the s10 shard runs.

D8 (pre-registration 3.6.0 scale_invariance, AML-GOALS section 9 #45) is a
registered decision rule, not an equivalence test. Per behavioural typology
it compares the logit-mean AP of the scale-2 runs (one per registered s2
seed: corpora.calibration_seed plus corpora.calibration_replicate_seeds)
with the logit-mean AP of the scale-10 shards (scale_invariance.n_shards
shards of one scale-10 calibration corpus, each scored with
``scripts/aml_gate.py --d8-shard INDEX``), and per feature the KS statistic
of the pooled runs. See src/lakebench/aml/scale_invariance.py for the rule;
every tolerance comes from aml_preregistration.json (R7).

Inputs are gate reports as local paths or s3:// / s3a:// URIs. Each must list
its ``oof_scores`` and ``unit_features`` model outputs. For S3, credentials
come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY and the endpoint from
``--s3-endpoint`` or AWS_ENDPOINT_URL.

    python3.11 scripts/aml_d8.py \\
        --s2 s2-43/gate.json s2-123456832/gate.json s2-246913621/gate.json \\
             s2-370370410/gate.json s2-493827199/gate.json \\
        --s10 s10-43-shard0/gate.json s10-43-shard1/gate.json s10-43-shard2/gate.json \\
              s10-43-shard3/gate.json s10-43-shard4/gate.json \\
        --out d8.json

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
    ap.add_argument("--s2", nargs="+", required=True, help="one gate report per s2 seed")
    ap.add_argument("--s10", nargs="+", required=True, help="one gate report per s10 shard")
    ap.add_argument("--out", type=Path, default=None, help="write the verdict JSON here")
    ap.add_argument("--prereg", default=None, help="pre-registration JSON path")
    ap.add_argument("--s3-endpoint", default=None, help="S3 endpoint URL for s3:// inputs")
    ap.add_argument("--jobs", type=int, default=None, help="runs bootstrapped in parallel")
    args = ap.parse_args(argv)

    from lakebench.aml.scale_invariance import evaluate_d8, summary_lines

    verdict = evaluate_d8(
        args.s2, args.s10, prereg_path=args.prereg, endpoint=args.s3_endpoint, jobs=args.jobs
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
