"""The published benchmark specs name what the code measures and checks.

Code to doc: every metric the registry tags with a workload, every query id
of the workload's query set and every correctness check id the workload
evaluates appears, in backticks, in that workload's spec under
``docs/benchmarks/``. A lane that adds a metric, query or check without a
spec line fails here.

Seed scan: no spec carries an integer whose salted hash is a held-out AML
seed's. The scan compares sha256 hashes against
``spark/data/aml/heldout_hashes.json`` and never loads a seed value; it
skips until that file exists.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
from pathlib import Path

import pytest

from lakebench.benchmark.queries import get_benchmark_queries
from lakebench.config.schema import WorkloadSchema
from lakebench.metrics import c360_correctness, metric_registry

ROOT = Path(__file__).resolve().parents[1]
SPECS = ROOT / "docs" / "benchmarks"
C360_SPEC = SPECS / "C360.md"
C360_CHECKS = ROOT / "src" / "lakebench" / "metrics" / "c360_correctness.py"
HELDOUT_HASHES = ROOT / "src" / "lakebench" / "spark" / "data" / "aml" / "heldout_hashes.json"

#: The functions in c360_correctness.py that build one check from a literal id.
_CHECK_BUILDERS = ("_check", "_mean_bound", "_share_bound")


def backticked(text: str) -> set[str]:
    """Every backticked token of *text*."""
    return set(re.findall(r"`([^`\n]+)`", text))


def registry_metrics(workload: str, metrics: dict | None = None) -> set[str]:
    """Metric ids whose registry entries include *workload*."""
    metrics = metric_registry.METRICS if metrics is None else metrics
    return {k for k, entries in metrics.items() if any(workload in e.workloads for e in entries)}


def c360_check_ids(source: str) -> set[str]:
    """Check ids c360_correctness builds: the string literal first argument of
    each check builder, plus the ids ``benchmark_checks`` returns for the
    workload's query set (built from the query names, so the AST walk alone
    cannot see them)."""
    ids = set()
    for node in ast.walk(ast.parse(source)):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in _CHECK_BUILDERS
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            ids.add(node.args[0].value)
    queries = [
        {"name": q.name, "success": True, "rows_returned": 1}
        for q in get_benchmark_queries(WorkloadSchema.CUSTOMER360)
    ]
    ids |= {c["id"] for c in c360_correctness.benchmark_checks(queries, None, {})}
    return ids


def missing_from(spec_text: str, names: set[str]) -> list[str]:
    return sorted(names - backticked(spec_text))


def test_every_registry_metric_in_spec():
    missing = missing_from(C360_SPEC.read_text(), registry_metrics("customer360"))
    assert not missing, f"docs/benchmarks/C360.md names no registry metric: {missing}"


def test_every_query_id_in_spec():
    names = {q.name for q in get_benchmark_queries(WorkloadSchema.CUSTOMER360)}
    assert len(names) == 8
    missing = missing_from(C360_SPEC.read_text(), names)
    assert not missing, f"docs/benchmarks/C360.md names no query: {missing}"


def test_every_c360_check_id_in_spec():
    ids = c360_check_ids(C360_CHECKS.read_text())
    assert len(ids) >= 34, sorted(ids)
    missing = missing_from(C360_SPEC.read_text(), ids)
    assert not missing, f"docs/benchmarks/C360.md names no check: {missing}"


def test_planted_registry_metric_without_a_spec_line_fails():
    planted = dict(metric_registry.METRICS)
    entry = next(iter(planted["time_to_value_seconds"]))
    planted["planted_unnamed_metric"] = (
        metric_registry.MetricMeta(
            "planted_unnamed_metric",
            entry.unit,
            entry.direction,
            entry.band,
            entry.modes,
            entry.workloads,
        ),
    )
    names = registry_metrics("customer360", planted)
    assert missing_from(C360_SPEC.read_text(), names) == ["planted_unnamed_metric"]


def test_planted_check_and_query_without_a_spec_line_fail():
    source = C360_CHECKS.read_text() + '\n\n_check("planted_unnamed_check", "invariant", True)\n'
    assert missing_from(C360_SPEC.read_text(), c360_check_ids(source)) == ["planted_unnamed_check"]
    assert missing_from("`Q1_full_aggregation_scan`", {"Q1_full_aggregation_scan", "Q8_x"}) == [
        "Q8_x"
    ]


# -- held-out seed scan ---------------------------------------------------------


def seed_hash(salt_hex: str, value: int) -> str:
    """The held-out hash form: sha256(bytes.fromhex(salt) + b':' + decimal)."""
    return hashlib.sha256(bytes.fromhex(salt_hex) + b":" + str(value).encode()).hexdigest()


def candidate_integers(text: str, min_digits: int = 5, max_digits: int = 19) -> set[int]:
    """Every integer a page could spell a seed with: each run of digits (with
    comma, underscore or space grouping removed) and every window of
    *min_digits* to *max_digits* digits inside it. Values under *min_digits*
    digits are not scanned."""
    out: set[int] = set()
    for m in re.finditer(r"\d(?:[\d,_ ]*\d)?", text):
        digits = re.sub(r"[,_ ]", "", m.group(0))
        for size in range(min_digits, min(max_digits, len(digits)) + 1):
            for i in range(len(digits) - size + 1):
                out.add(int(digits[i : i + size]))
    return out


def heldout_hits(text: str, hashes: dict, min_digits: int = 5) -> int:
    """How many candidate integers of *text* hash to a held-out role hash."""
    salt = hashes["salt"]
    targets = {h for role in hashes.get("roles", {}).values() for h in role}
    if not targets:
        return 0
    return sum(seed_hash(salt, v) in targets for v in candidate_integers(text, min_digits))


def test_seed_scan_finds_a_planted_value():
    # A synthetic hash file built from public values (seed 43, the local AML
    # seed, and 1,234,567), never from a held-out seed.
    salt = "00" * 32
    hashes = {"salt": salt, "roles": {"evaluation": [seed_hash(salt, 43)], "robustness": []}}
    assert heldout_hits("Use seed 43 locally.", hashes, min_digits=1) == 1
    assert heldout_hits("Use seed 44 locally.", hashes, min_digits=1) == 0
    hashes["roles"]["robustness"] = [seed_hash(salt, 1234567)]
    assert heldout_hits("a corpus of 1,234,567 rows", hashes) == 1
    assert heldout_hits("run-20261234567-abc", hashes) == 1  # a window of a longer run
    assert heldout_hits("no numbers here", hashes) == 0


@pytest.mark.skipif(not HELDOUT_HASHES.is_file(), reason="held-out hash file not in this tree yet")
def test_specs_carry_no_heldout_seed():
    hashes = json.loads(HELDOUT_HASHES.read_text())
    hits = {
        str(p.relative_to(ROOT)): n
        for p in sorted(SPECS.glob("*.md"))
        if (n := heldout_hits(p.read_text(), hashes))
    }
    # Counts only: never print a value.
    assert not hits, f"held-out seed hash matches (counts per file): {hits}"
