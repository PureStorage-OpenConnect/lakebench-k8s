"""Test-only held-out seeds for ``tests/fixtures/heldout_test.json``.

TEST VALUES. These seeds were drawn for the test fixture and are not, and
never were, AML pre-registration seeds. They let the held-out guard be
exercised end to end without any real held-out value in a test.
"""

from __future__ import annotations

from pathlib import Path

from lakebench.config import datagen_seed as ds

#: Registered as ``evaluation`` in the fixture (test value).
TEST_EVALUATION_SEED = 8763195430032412900
#: Registered as ``robustness`` in the fixture (test value).
TEST_ROBUSTNESS_SEED = 4695594915748112205
#: In the fixture's ``spent`` list beside 42 (test value).
TEST_SPENT_SEED = 5783979690583702767

FIXTURE = Path(__file__).with_name("heldout_test.json")


def load() -> ds.HeldOut:
    """The fixture file plus the compiled production floor."""
    return ds.load_heldout(FIXTURE)


def use_fixture(monkeypatch) -> ds.HeldOut:
    """Point the module's held-out record at the fixture for one test."""
    held = load()
    monkeypatch.setattr(ds, "_heldout", lambda: held)
    return held


#: (typology name, tid) pairs the rows below are spread over.
_TYPOLOGIES = (("GATHER_SCATTER", 0), ("STACK", 3), ("MICRO_STRUCTURING", 7))


def manifest_rows(
    seed: int, n: int, start: int = 0, typologies=_TYPOLOGIES
) -> list[tuple[str, int]]:
    """``n`` (typology_id, instance seed) manifest rows derived from ``seed``
    exactly as datagen_rs::typology::schedule_ex does, signed like the
    manifest's Int64 column."""
    rows = []
    for k in range(start, start + n):
        name, tid = typologies[k % len(typologies)]
        j = k // len(typologies)
        inner = ds._splitmix64((0xF100 + tid * ds.TID_SEED_STRIDE + j) & ds._MASK64)
        iseed = ds._signed64(ds._splitmix64((seed & ds._MASK64) ^ inner))
        rows.append((f"{name}_{tid}_{j:07d}", iseed))
    return rows


def screening_rows(seed: int, n: int) -> list[tuple[str, int]]:
    """``n`` screening (sanctions and PEP) manifest rows derived from ``seed``
    as datagen_rs::screening does, signed like the manifest's Int64 column."""
    rows = []
    for i in range(n):
        k, r = 3 * i + 1, i % ds._SCREEN_MAX_REL
        inner = ds._splitmix64(ds._SCREEN_BASE + 4 * k + r)
        iseed = ds._signed64(ds._splitmix64((seed & ds._MASK64) ^ ds.SCREEN_SALT ^ inner))
        name = "SANCTIONS_MATCH" if i % 2 else "PEP_MATCH"
        rows.append((f"{name}_{i + 1:07d}", iseed))
    return rows
