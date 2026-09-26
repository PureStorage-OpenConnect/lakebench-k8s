#!/usr/bin/env python3
"""Record a cluster registered look, then print its verdict (AML-GOALS R3).

The cluster reference job (score_financial_reference.py) cannot write the
tracked look record, so for a declared registered evaluation or robustness
run it writes aml_gate_report.json and withholds the verdict. This script
reads that report (local path or s3:// / s3a:// URI), records its sha256 and
seed in src/lakebench/spark/data/aml/aml_registered_looks.json, and only then
prints the verdict. If the record cannot be written, nothing is printed but
the reason. Commit the record afterwards.

    python3.11 scripts/aml_record_look.py s3a://lb-gold/aml/aml_gate_report.json \\
        --s3-endpoint http://10.21.227.93:80

Exit status: 0 recorded, 1 refused or not recorded.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("report", help="aml_gate_report.json of the registered look")
    ap.add_argument("--s3-endpoint", default=None, help="S3 endpoint URL for s3:// inputs")
    args = ap.parse_args(argv)

    from lakebench.aml.fidelity_gate import summary_lines
    from lakebench.aml.scale_invariance import read_bytes
    from lakebench.config.datagen_seed import PROTECTED_ROLES, complete_look

    try:
        raw = read_bytes(args.report, args.s3_endpoint)
        report = json.loads(raw)
        prov = report.get("provenance") or {}
        role = prov.get("declared_corpus_role")
        seed = prov.get("corpus_seed")
        if role not in PROTECTED_ROLES:
            raise ValueError(f"the report declares no registered look (role {role!r})")
        if seed in (None, ""):
            raise ValueError("the report records no corpus_seed")
        # #46 D-8: the look must have run on the committed predictions, and
        # they must not have changed since.
        from lakebench.config.datagen_seed import load_predictions

        if prov.get("level2_predictions_sha256") != load_predictions()[1]:
            raise ValueError("the Level-2 predictions differ from the ones the look recorded")
        digest = hashlib.sha256(raw).hexdigest()
        entry = complete_look(
            role,
            int(seed),
            digest,
            str(args.report),
            {"git_sha": prov.get("git_sha"), "prereg_sha256": report.get("prereg_sha256")},
            claim_if_missing=True,
        )
    except Exception as e:  # noqa: BLE001 -- fail closed: no record, no verdict
        print(f"not recorded, verdict withheld: {e}", file=sys.stderr)
        return 1
    print(f"recorded look {entry['role']} seed {entry['seed']} report sha256 {digest}")
    from lakebench.config.datagen_seed import load_predictions, replication

    report["level2_replication"] = replication(
        report.get("typologies") or {}, load_predictions()[0]
    )
    for line in summary_lines(report):
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
