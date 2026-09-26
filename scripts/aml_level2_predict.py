#!/usr/bin/env python3
"""Commit the Level-2 predictions from the calibration runs (AML-GOALS #46, D-8).

Reads the scale-2 calibration gate reports (one per registered s2 seed, from
the pinned datagen image), computes per-typology predicted evaluation AP and
prediction interval (src/lakebench/aml/predictions.py; method in the
pre-registration's level2.predictions) and writes them into the tracked
src/lakebench/spark/data/aml/aml_level2_predictions.json. Refuses to
overwrite committed predictions. Commit the file before any registered look.

    python3.11 scripts/aml_level2_predict.py s2-43/gate.json s2-123456832/gate.json \\
        s2-246913621/gate.json s2-370370410/gate.json s2-493827199/gate.json

Exit status: 0 written, 1 refused.
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
    ap.add_argument("reports", nargs="+", help="calibration gate reports (local or s3://)")
    ap.add_argument("--s3-endpoint", default=None)
    ap.add_argument("--record", type=Path, default=None, help="predictions file (default: tracked)")
    args = ap.parse_args(argv)

    from lakebench.aml.predictions import compute_predictions
    from lakebench.config.datagen_seed import _write_atomic, predictions_path

    path = args.record or predictions_path()
    doc = json.loads(path.read_text())
    if doc.get("predictions") is not None:
        print(f"refusing: {path} already holds committed predictions", file=sys.stderr)
        return 1
    try:
        doc["predictions"] = compute_predictions(args.reports, endpoint=args.s3_endpoint)
    except Exception as e:  # noqa: BLE001 -- nothing is written
        print(f"refusing: {e}", file=sys.stderr)
        return 1
    _write_atomic(path, doc)
    for t, r in doc["predictions"]["typologies"].items():
        print(f"{t}: predicted AP {r['predicted_ap']:.3f} PI [{r['pi'][0]:.3f}, {r['pi'][1]:.3f}]")
    print(f"wrote {path}; commit it before any registered look")
    return 0


if __name__ == "__main__":
    sys.exit(main())
