"""How a run record names a corpus seed (SPEC release success 5).

A record, the datagen fleet record and the HTML report keep a corpus seed in
plaintext unless the AML seed guard says it must not be shown
(``datagen_seed.seed_is_protected``: a held-out seed, a spent one, one with
a recorded look) or cannot tell. 42 (the Customer 360 default) and the AML
calibration seed are public and always plaintext. A protected seed is
written as the recorded form the readers already know
(``look_guard.recorded_seed_role``, ``comparability.seeds_equal``)::

    {"seed_ref": <salted hash>, "role": <held-out role or None>}

and a seed that cannot be checked (the held-out record cannot be read) is
withheld, ``{"seed_ref": None}`` (fail closed).

Every other seed stays plaintext, so the record still names its seed
(invariant 5), compares with records written before this rule, and passes
the fail-closed readers that require a seed they can check. Hashing an
unprotected seed would hide nothing: the salt is public, a small seed is
recovered from its hash at once, and the seed is in the datagen Job's args
anyway. A value that is already a recorded form passes through unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

#: The Customer 360 default seed (``datagen_seed.NON_AML_DEFAULT_SEED``).
_C360_DEFAULT = 42
#: The AML calibration seed when the pre-registration cannot be read.
_CALIBRATION_FALLBACK = 43


def public_seeds() -> frozenset[int]:
    """The development seeds a record may show: 42 and the AML calibration
    seed (43 unless the pre-registration says otherwise)."""
    try:
        from lakebench.config.datagen_seed import calibration_seed

        cal = int(calibration_seed())
    except Exception:  # noqa: BLE001 -- unreadable: the published value
        cal = _CALIBRATION_FALLBACK
    return frozenset({_C360_DEFAULT, cal})


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("+-").isdigit():
        return int(value.strip())
    return None


def recorded_seed(value: Any) -> Any:
    """*value* as a record keeps it (see the module docstring). None stays
    None; a recorded form (a mapping) is returned unchanged; anything that
    is not a seed number is withheld."""
    if value is None or isinstance(value, Mapping):
        return value
    seed = _as_int(value)
    if seed is None:
        return {"seed_ref": None}
    if seed in public_seeds():
        return seed
    try:
        from lakebench.config.datagen_seed import (
            _heldout,
            heldout_role,
            seed_hash,
            seed_is_protected,
        )

        held = _heldout()
        if not seed_is_protected(seed, held):
            return seed
        return {"seed_ref": seed_hash(held.salt, seed), "role": heldout_role(seed, held)}
    except Exception:  # noqa: BLE001 -- fail closed: a seed that cannot be checked is not shown
        return {"seed_ref": None}


def seed_label(value: Any) -> str:
    """A recorded seed for display: the number, a held-out role and a short
    hash, a short hash, or "withheld"."""
    if value is None:
        return "not recorded"
    if isinstance(value, Mapping):
        ref = value.get("seed_ref")
        if not ref:
            return "withheld"
        role = value.get("role")
        short = str(ref)[:12]
        return f"{role} seed (ref {short})" if role else f"ref {short}"
    form = recorded_seed(value)
    if isinstance(form, Mapping):
        return seed_label(form)
    return str(form)
