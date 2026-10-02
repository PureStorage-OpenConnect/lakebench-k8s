"""AM-22 edits the refusal texts of the pinned ``aml_seed_error`` (and its
helper ``_match_label``) and nothing else: over a synthetic matrix the edited
function refuses exactly where the parent's did (``tests/fixtures/
aml_seed_error_v0.py``), raises exactly where it raised, and never prints a
seed. TEST VALUES ONLY (the held-out record is the test fixture)."""

from __future__ import annotations

import ast
import hashlib
import itertools
import re
from pathlib import Path

import pytest

from lakebench.config import datagen_seed as ds
from tests.fixtures import aml_seed_error_v0 as v0
from tests.fixtures import protected_corpus as pc

HELD = pc.fixture_heldout()
SEEDS = [None, pc.CALIBRATION, pc.EV, pc.RB, 42, pc.SPENT, 7777, 2**62 + 5]
ROLES = [None, "calibration", "evaluation", "robustness", "bogus"]
MATCHED = [
    (),
    ("evaluation",),
    ("robustness",),
    ("spent",),
    ("spent", "evaluation"),
    (pc.EV,),
    (pc.RB,),
    (pc.CALIBRATION,),
    (42,),
    (7777,),
    ("bogus-role",),
]
COUNTS_ONLY = [False, True]
CLAIM = [None, True, False]


def _corpora(looks_open, spent=(42,), calibration=pc.CALIBRATION):
    out = {"spent_seeds": list(spent), "registered_looks_open": looks_open}
    if calibration is not None:
        out["calibration_seed"] = calibration
    return out


CORPORA = [
    _corpora(True),
    _corpora(False),
    _corpora("true"),  # a string never opens the looks
    _corpora(True, spent=(42, pc.EV)),  # a held-out seed that is also spent
]


def _outcome(fn, *args, **kw):
    try:
        return ("ok", fn(*args, **kw))
    except Exception as e:  # noqa: BLE001 -- compared, not swallowed
        return ("raise", type(e).__name__)


def _matrix():
    yield from itertools.product(CORPORA, SEEDS, ROLES, MATCHED, COUNTS_ONLY, CLAIM)


def test_the_baseline_copy_is_the_parent_function():
    """The copy is pinned: an edit to it fails here, not silently."""
    src = Path(v0.__file__).read_text()
    tree = ast.parse(src)
    fns = [
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name in ("_match_label", "aml_seed_error")
    ]
    assert [f.name for f in fns] == ["_match_label", "aml_seed_error"]
    body = ast.Module(body=fns, type_ignores=[])
    assert hashlib.sha256(ast.dump(body).encode()).hexdigest() == v0.BASELINE_AST_SHA256


def test_aml_seed_error_verdicts_unchanged():
    """Refuse (non-None), allow (None) and raise exactly where the parent did."""
    n = flips = 0
    for corpora, seed, role, matched, counts_only, claim in _matrix():
        args = (corpora, seed, role, matched, counts_only, claim)
        old = _outcome(v0.aml_seed_error, *args, heldout=HELD)
        new = _outcome(ds.aml_seed_error, *args, heldout=HELD)
        n += 1
        if old[0] != new[0] or (old[0] == "ok" and (old[1] is None) != (new[1] is None)):
            flips += 1
            pytest.fail(f"verdict changed for {args!r}: {old!r} -> {new!r}")
        if old[0] == "raise":
            assert old[1] == new[1], args
    assert n > 10_000 and flips == 0


def test_missing_calibration_seed_still_refuses():
    """Accepted difference: the parent raised KeyError formatting the spent
    branch without corpora.calibration_seed; the edited text names the key
    instead. Both refuse."""
    c = _corpora(True, calibration=None)
    assert _outcome(v0.aml_seed_error, c, 42, heldout=HELD)[0] == "raise"
    assert ds.aml_seed_error(c, 42, heldout=HELD) is not None


_INT = re.compile(r"\d+")


def test_no_refusal_prints_a_seed():
    """Every edited message: no integer token equal to any seed of the matrix
    (held-out, spent, calibration or other)."""
    seeds = {str(s) for s in SEEDS if s is not None}
    for corpora, seed, role, matched, counts_only, claim in _matrix():
        out = _outcome(
            ds.aml_seed_error, corpora, seed, role, matched, counts_only, claim, heldout=HELD
        )
        if out[0] == "ok" and out[1]:
            assert not (set(_INT.findall(out[1])) & seeds), (seed, role, matched, out[1])
