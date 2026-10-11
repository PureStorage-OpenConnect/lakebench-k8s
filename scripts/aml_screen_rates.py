#!/usr/bin/env python3
"""W5/W6 screening: non-planted alerts per customer, by scale.

The screening rules W5 (sanctions) and W6 (PEP counterparty) match names
against a watchlist that grows with the population; the generator grows its
name pools with the population too, so their non-planted alerts per customer
should stay flat across scale. This script measures that from stored run
records only (never a bucket or a cluster) and writes
the published figure ``docs/benchmarks/data/aml_screening_rates.json``.

For each AML batch record given it reads
``financial_scoring.nonplanted_alerts_by_rule[W5, W6]`` and
``financial_scoring.customer_count`` and reports the per-customer rate, with
the record's run id, corpus role, scale, generator digest, workload version
and the rule's evidence-capped alert count (a Lakebench cap: a cut planted
alert can read non-planted, so a non-zero count makes the figure an upper
bound). Per (seed role, rule) it reports the ratio scale-10 / scale-1 of
the per-customer rates, computed from the raw counts and marked when an
evidence cap cut either side. Every figure is one run (n=1).

Refused, by run id and reason, never read as zero:

- a record from a protected corpus (``look_guard.protected_record_reason``,
  fail closed): the evaluation and robustness seeds stay out;
- a record whose verdict is not PASSED (``metrics.verdict.verdict_of``);
- a record that is not an AML batch run, that lacks the scorer's keys
  (recorded before the scorer counted them) or holds a non-count, or in
  which W5 or W6 did not run (the scorer fills 0 for a rule that did not);
- a record with no observed generator digest, or an observed scale or seed
  that is missing or differs from the declared one;
- a record at a scale other than 1 or 10;
- a record whose seed is neither 43 nor the calibration seed;
- two records for one (seed role, scale), a missing one of the runs
  (seed 43 at scale 1 and 10, and the calibration seed at both when it is
  not 43: the pre-registration's calibration seed may be 43 itself, and
  then seed 43's two runs are the whole set), and a scale-1 and scale-10
  pair that differ in generator or workload version.

The output holds no seed: rows name a seed role (``seed-43`` or
``calibration``). Exit 0 when the file was written (or, with ``--check``,
matches), 1 when it differs under ``--check``, 2 when a record was refused
or the scale-1 counts are too few to publish a ratio (fewer than
``MIN_S1_ALERTS`` non-planted W5 plus W6 alerts for a seed role).

Usage::

    python scripts/aml_screen_rates.py RUN_DIR_OR_METRICS_JSON ... \\
        [--out docs/benchmarks/data/aml_screening_rates.json] [--check]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

#: The screening rules the figure covers.
RULES = ("W5_sanctions_match", "W6_pep_counterparty")
#: Below this many non-planted W5 plus W6 alerts at scale 1 the ratio is too
#: noisy to publish (another calibration seed is added first).
MIN_S1_ALERTS = 50
#: The public development seed.
DEV_SEED = 43
#: The scales the figure compares.
SCALES = (1.0, 10.0)
DEFAULT_OUT = ROOT / "docs" / "benchmarks" / "data" / "aml_screening_rates.json"


class Refused(Exception):
    """A record the figure must not use; the message names why (no seed)."""


def _load(path: Path) -> dict[str, Any]:
    p = path / "metrics.json" if path.is_dir() else path
    try:
        data = json.loads(p.read_text())
    except (OSError, ValueError) as e:
        raise Refused(f"{p}: unreadable ({type(e).__name__})") from None
    if not isinstance(data, dict):
        raise Refused(f"{p}: not a run record")
    return data


def seed_role(record: dict[str, Any]) -> str:
    """``seed-43`` or ``calibration``; Refused for any other seed. The seed
    value is compared in-process and never returned or printed."""
    from lakebench.config import datagen_seed

    corpus = (record.get("experiment") or {}).get("corpus") or {}
    seed = corpus.get("seed")
    if isinstance(seed, bool) or not isinstance(seed, int):
        raise Refused("the record names no integer corpus seed")
    if seed == DEV_SEED:
        return "seed-43"
    try:
        calibration = datagen_seed.calibration_seed()
    except Exception as e:  # noqa: BLE001 -- unreadable pre-registration
        raise Refused(f"the calibration seed cannot be read ({type(e).__name__})") from None
    if seed == calibration:
        return "calibration"
    raise Refused("the corpus seed is neither 43 nor the calibration seed")


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _count(value: Any) -> int | None:
    """A non-negative integer count, or None when the value is not one."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def record_row(record: dict[str, Any]) -> dict[str, Any]:
    """The figure's row for one record; Refused with the reason when the
    record must not be used."""
    from lakebench.aml.look_guard import protected_record_reason
    from lakebench.metrics.verdict import verdict_of

    run = str(record.get("run_id") or "?")

    def refuse(why: str) -> Refused:
        return Refused(f"run {run}: {why}")

    why = protected_record_reason(record, require_identity=True, fail_closed=True)
    if why is not None:
        raise refuse(f"a protected AML corpus ({why})")
    exp = _mapping(record.get("experiment"))
    workload = _mapping(exp.get("workload"))
    if workload.get("name") != "financial" or exp.get("mode") != "batch":
        raise refuse("not an AML batch run")
    status = verdict_of(record).get("status")
    if status != "PASSED":
        raise refuse(f"verdict {status}, not PASSED")
    scoring = _mapping(record.get("financial_scoring"))
    counts = scoring.get("nonplanted_alerts_by_rule")
    if not isinstance(counts, dict) or not all(r in counts for r in RULES):
        raise refuse("financial_scoring.nonplanted_alerts_by_rule lacks W5 or W6")
    nonplanted = {r: _count(counts[r]) for r in RULES}
    if None in nonplanted.values():
        raise refuse("financial_scoring.nonplanted_alerts_by_rule holds a non-count for W5 or W6")
    customers = _count(scoring.get("customer_count"))
    if not customers:
        raise refuse("financial_scoring.customer_count is missing or not positive")
    capped_by = scoring.get("evidence_capped_alerts_by_rule")
    capped = {r: _count(_mapping(capped_by).get(r, 0)) for r in RULES}
    if not isinstance(capped_by, dict) or None in capped.values():
        raise refuse("financial_scoring.evidence_capped_alerts_by_rule is missing or malformed")
    # The scorer fills 0 for every rule with a target, run or not: a zero is
    # a count only when the rule ran.
    rules = scoring.get("rules")
    ran = {
        r.get("rule_id")
        for r in (rules if isinstance(rules, list) else [])
        if isinstance(r, dict) and r.get("status") == "ran"
    }
    not_ran = [r for r in RULES if r not in ran]
    if not_ran:
        raise refuse(f"{', '.join(not_ran)} did not run (financial_scoring.rules)")
    corpus = _mapping(exp.get("corpus"))
    datagen = _mapping(corpus.get("datagen"))
    scale = corpus.get("scale")
    if isinstance(scale, bool) or not isinstance(scale, (int, float)) or scale <= 0:
        raise refuse("experiment.corpus.scale is missing")
    if float(scale) not in SCALES:
        raise refuse(f"scale {scale:g} is not one the figure uses (1 or 10)")
    # The generator, scale and seed must be observed from the datagen pods
    # of the namespace's latest generate, not declared by a config over
    # older data.
    if datagen.get("observed") is not True or not datagen.get("digest"):
        raise refuse("no observed generator digest for the corpus")
    if datagen.get("scale") != scale:
        raise refuse("the observed datagen scale differs from experiment.corpus.scale")
    if datagen.get("seed") is None or datagen.get("seed") != corpus.get("seed"):
        raise refuse("the observed datagen seed is missing or differs from the declared seed")
    version = workload.get("version")
    if not version:
        raise refuse("experiment.workload.version is missing")
    try:
        role = seed_role(record)
    except Refused as e:
        raise refuse(str(e)) from None
    return {
        "seed_role": role,
        "scale": float(scale),
        "run_id": run,
        "corpus_role": corpus.get("corpus_role"),
        "generator": datagen["digest"],
        "workload_version": version,
        "customer_count": customers,
        "nonplanted_alerts": nonplanted,
        "evidence_capped": capped,
    }


def required_runs() -> tuple[tuple[str, float], ...]:
    """The runs the figure needs: seed 43 at both scales, and the calibration
    seed at both scales when it is another seed (when the pre-registration's
    calibration seed is 43, the two are one corpus and seed 43's runs stand
    for both). Raises Refused when the calibration seed cannot be read."""
    from lakebench.config import datagen_seed

    try:
        same = datagen_seed.calibration_seed() == DEV_SEED
    except Exception as e:  # noqa: BLE001 -- unreadable pre-registration
        raise Refused(f"the calibration seed cannot be read ({type(e).__name__})") from None
    roles = ("seed-43",) if same else ("seed-43", "calibration")
    return tuple((role, scale) for role in roles for scale in SCALES)


def build(records: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    """The published document and the problems that stop it being written."""
    problems: list[str] = []
    rows: dict[tuple[str, float], dict[str, Any]] = {}
    for record in records:
        try:
            row = record_row(record)
        except Refused as e:
            problems.append(str(e))
            continue
        key = (row["seed_role"], row["scale"])
        if key in rows:
            problems.append(
                f"runs {rows[key]['run_id']} and {row['run_id']}: two records for "
                f"{key[0]} at scale {key[1]:g}"
            )
            continue
        rows[key] = row
    try:
        required = required_runs()
    except Refused as e:
        problems.append(str(e))
        required = ()
    for role, scale in required:
        if (role, scale) not in rows:
            problems.append(f"{role}: no usable record at scale {scale:g}")
    figures = []
    for (role, _scale), row in sorted(rows.items()):
        for rule in RULES:
            n = row["nonplanted_alerts"][rule]
            figures.append(
                {
                    "seed_role": role,
                    "scale": row["scale"],
                    "rule": rule,
                    "nonplanted_alerts": n,
                    "customer_count": row["customer_count"],
                    "per_customer": round(n / row["customer_count"], 9),
                    # Lakebench-imposed evidence cap: when it cut any of the
                    # rule's alerts, the non-planted count is an upper bound.
                    "evidence_capped_alerts": row["evidence_capped"][rule],
                    "run_id": row["run_id"],
                    "corpus_role": row["corpus_role"],
                    "generator": row["generator"],
                    "workload_version": row["workload_version"],
                    "n": 1,
                }
            )
    ratios = []
    for role in sorted({role for role, _ in rows}):
        a, b = rows.get((role, 1.0)), rows.get((role, 10.0))
        if a is not None:
            total = sum(a["nonplanted_alerts"].values())
            if total < MIN_S1_ALERTS:
                problems.append(
                    f"{role}: {total} non-planted W5 plus W6 alerts at scale 1, under "
                    f"{MIN_S1_ALERTS}: the ratio is too noisy to publish"
                )
        if a is None or b is None:
            continue
        for field in ("generator", "workload_version"):
            if a[field] != b[field]:
                problems.append(
                    f"runs {a['run_id']} and {b['run_id']}: {role} at scale 1 and 10 differ "
                    f"in {field}, so the ratio would not isolate scale"
                )
        for rule in RULES:
            n1, n10 = a["nonplanted_alerts"][rule], b["nonplanted_alerts"][rule]
            ratio = None
            if n1:
                ratio = round((n10 / b["customer_count"]) / (n1 / a["customer_count"]), 3)
            capped = a["evidence_capped"][rule] + b["evidence_capped"][rule]
            ratios.append(
                {
                    "seed_role": role,
                    "rule": rule,
                    "s10_over_s1": ratio,
                    "undefined_reason": None if n1 else "no non-planted alert at scale 1",
                    "bounded_by_evidence_cap": capped > 0,
                    "runs": [a["run_id"], b["run_id"]],
                    "n": 1,
                }
            )
    doc = {
        "schema": 1,
        "generated_by": "scripts/aml_screen_rates.py",
        "what": (
            "W5/W6 non-planted alerts per customer (silver.entities is_customer) by scale, "
            "from stored AML batch run records; n=1 per figure. evidence_capped_alerts "
            "counts alerts a Lakebench evidence cap cut; when it is above 0 the "
            "non-planted count is an upper bound, and a ratio with either side cut is "
            "marked bounded_by_evidence_cap"
        ),
        "rules": list(RULES),
        "figures": figures,
        "ratios": ratios,
    }
    return doc, problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("records", nargs="+", type=Path, help="run directories or metrics.json files")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--check", action="store_true", help="compare with --out, write nothing")
    args = ap.parse_args(argv)
    records: list[dict[str, Any]] = []
    problems: list[str] = []
    for path in args.records:
        try:
            records.append(_load(path))
        except Refused as e:
            problems.append(str(e))
    doc, more = build(records)
    problems += more
    for p in problems:
        print(f"refused: {p}", file=sys.stderr)
    if problems:
        return 2
    text = json.dumps(doc, indent=2, sort_keys=True) + "\n"
    if args.check:
        current = args.out.read_text() if args.out.is_file() else ""
        if current != text:
            print(f"{args.out} differs from the records", file=sys.stderr)
            return 1
        return 0
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text)
    print(f"wrote {args.out}: {len(doc['figures'])} figures, {len(doc['ratios'])} ratios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
