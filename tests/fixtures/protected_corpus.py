"""Test helpers for the protected AML corpus guard (SAF-5).

TEST VALUES ONLY: the held-out record is ``tests/fixtures/heldout_test.json``
(salt and seeds in ``heldout_test_seeds.py``), never an AML pre-registration
value. ``use_heldout`` builds the record without the compiled floor, so these
tests behave the same whether or not the owner's floor is initialised, and
replaces the pre-registration's ``corpora`` block with a synthetic one whose
registered looks are open (so a protected config loads and the per-verb guard,
not the load-time validator, is what refuses it).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from lakebench.config import datagen_seed as ds
from tests.fixtures import heldout_test_seeds as ts

EV = ts.TEST_EVALUATION_SEED
RB = ts.TEST_ROBUSTNESS_SEED
SPENT = ts.TEST_SPENT_SEED
CALIBRATION = 43

#: The synthetic pre-registration ``corpora`` block (test values).
CORPORA = {
    "calibration_seed": CALIBRATION,
    "spent_seeds": [42],
    "registered_looks_open": True,
    "robustness_perturbation": {
        "median_amount_multiplier": 1.5,
        "persona_sd_multiplier": 1.25,
        "dormancy_range_multiplier": 1.2,
    },
}


def fixture_heldout() -> ds.HeldOut:
    """The fixture file as a HeldOut, its own roles standing in for the
    compiled floor (no floor read)."""
    doc = json.loads(ts.FIXTURE.read_text())
    roles = {r: frozenset(doc["roles"][r]) for r in ds.PROTECTED_ROLES}
    return ds.HeldOut(
        salt=doc["salt"],
        roles=roles,
        spent=frozenset(int(x) for x in doc["spent"]),
        absence_check=doc["absence_check"],
        floor_salt=doc["salt"],
        floor=roles,
    )


def use_heldout(monkeypatch, *, looks_open: bool = True, looks: list | None = None) -> ds.HeldOut:
    """Point datagen_seed at the fixture record and the synthetic corpora."""
    held = fixture_heldout()
    corpora = {**CORPORA, "registered_looks_open": looks_open}
    recorded = list(looks or [])
    monkeypatch.setattr(ds, "_heldout", lambda: held)
    # No host ledger leaks in: a test that needs one sets its own path.
    monkeypatch.setenv("LB_AML_CORPORA_LEDGER", "/nonexistent/lakebench/aml_corpora.jsonl")
    monkeypatch.setattr(ds, "load_looks", lambda path=None: list(recorded))
    monkeypatch.setattr(
        ds,
        "prereg_spent_seeds",
        lambda: frozenset(corpora["spent_seeds"]) | {int(e["seed"]) for e in recorded},
    )
    monkeypatch.setattr(
        ds,
        "_corpora",
        lambda: {
            **corpora,
            "spent_seeds": sorted(set(corpora["spent_seeds"]) | {int(e["seed"]) for e in recorded}),
        },
    )
    return held


def financial_config(
    path: Path,
    *,
    seed: int | None = None,
    role: str | None = None,
    perturbation: bool = False,
    name: str = "lbtest-aml",
) -> Path:
    """A minimal financial config at ``path``."""
    lines = [f"name: {name}", "workload:", "  schema: financial", "  datagen:"]
    if seed is not None:
        lines.append(f"    seed: {seed}")
    if role is not None:
        lines.append(f"    corpus_role: {role}")
    if perturbation:
        lines.append("    robustness_perturbation: true")
    if len(lines) == 4:
        lines.append("    scale: 1")
    path.write_text("\n".join(lines) + "\n")
    return path


_INT = re.compile(r"\d+")


def seed_tokens(text: str, seeds=(EV, RB)) -> list[str]:
    """Every integer token in ``text`` equal to one of ``seeds``, and every
    digit run that contains one."""
    wanted = {str(s) for s in seeds}
    return [t for t in _INT.findall(text) if any(w in t for w in wanted)]
