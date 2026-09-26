"""The top-level datagen seed a deployment generates with, and the AML seed guard.

The seed names the corpus: the AML reference job reports it (LB_DATAGEN_SEED)
and the AML pre-registration assigns seeds to roles (calibration, evaluation,
robustness) and retires seeds that have been looked at (AML-GOALS R3, section 9
#38). A cluster run used to hard-code 42 in three places, which is the spent D0
seed, so every cluster corpus silently reused it.

Resolution:

- ``datagen.seed`` set in the config: that seed.
- unset, financial schema: the pre-registration's calibration seed. Tuning
  runs are what an unconfigured deployment is for.
- unset, any other schema: 42, the seed those corpora have always used, so
  their data does not change.

Guard (financial schema, and the local gate ``scripts/aml_gate.py``):

- a seed in ``corpora.spent_seeds`` is always refused;
- the evaluation and robustness seeds are refused unless the run declares
  that role explicitly (``datagen.corpus_role`` in config, ``--registered``
  on the gate). Each is generated and scored once, after the freeze, as the
  registered gate run for its role; anything else would be a look that burns
  the seed (R3);
- a declared role must match its registered seed, so a role cannot be
  attached to another seed to make a tuning run look registered.

Only the standard library is imported, so config validation stays cheap.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

#: Seed for schemas the AML pre-registration does not govern (Customer 360,
#: IoT). Unchanged from the old hard-coded value so their corpora stay the same.
NON_AML_DEFAULT_SEED = 42

#: Roles whose corpus may only be generated or scored as the registered run.
PROTECTED_ROLES = ("evaluation", "robustness")
ROLES = ("calibration", *PROTECTED_ROLES)

_PREREG_PATH = (
    Path(__file__).resolve().parent.parent / "spark" / "data" / "aml" / "aml_preregistration.json"
)


@lru_cache(maxsize=1)
def _corpora() -> dict:
    with open(_PREREG_PATH, encoding="utf-8") as f:
        return json.load(f)["corpora"]


def spent_seeds() -> frozenset[int]:
    """Seeds the AML pre-registration has retired (looked at, or voided)."""
    return frozenset(int(s) for s in _corpora().get("spent_seeds", []))


def calibration_seed() -> int:
    return int(_corpora()["calibration_seed"])


def role_seed(role: str) -> int:
    return int(_corpora()[f"{role}_seed"])


def protected_seeds() -> dict[int, str]:
    """{seed: role} for the evaluation and robustness seeds."""
    return {role_seed(r): r for r in PROTECTED_ROLES}


def check_aml_seed(seed: int | None, corpus_role: str | None = None) -> None:
    """Raise ValueError unless ``seed`` may be used with ``corpus_role``."""
    if corpus_role is not None and corpus_role not in ROLES:
        raise ValueError(f"corpus_role must be one of {ROLES}, got {corpus_role!r}")
    if seed is not None and seed in spent_seeds():
        raise ValueError(
            f"seed {seed} is listed as spent in the AML pre-registration "
            "(corpora.spent_seeds): a corpus from it has already been looked at or "
            "voided and must not be regenerated or scored. Use the calibration seed "
            f"({calibration_seed()}) or another unregistered seed."
        )
    if corpus_role is not None:
        want = role_seed(corpus_role)
        if seed != want:
            raise ValueError(
                f"corpus_role {corpus_role!r} is registered for seed {want}, not {seed}"
            )
        return
    role = protected_seeds().get(seed) if seed is not None else None
    if role is not None:
        raise ValueError(
            f"seed {seed} is the pre-registered {role} seed. It is generated and "
            f"scored once, as the registered {role} gate run after the datagen "
            f"freeze: declare corpus_role: {role} (config) or --registered {role} "
            "(scripts/aml_gate.py) for that run. Any other use burns the seed."
        )


def check_seed(seed: int, schema: str, corpus_role: str | None = None) -> None:
    """``check_aml_seed`` for the financial schema; other schemas are free."""
    if schema == "financial":
        check_aml_seed(seed, corpus_role)
    elif corpus_role is not None:
        raise ValueError("corpus_role applies to the financial schema only")


def resolve_seed(seed: int | None, schema: str, corpus_role: str | None = None) -> int:
    """The seed a deployment generates with (see the module docstring)."""
    if seed is None:
        if corpus_role is not None and schema == "financial":
            seed = role_seed(corpus_role)
        else:
            seed = calibration_seed() if schema == "financial" else NON_AML_DEFAULT_SEED
    check_seed(seed, schema, corpus_role)
    return seed


def config_seed(cfg) -> int:
    """``resolve_seed`` for a LakebenchConfig."""
    workload = cfg.architecture.workload
    dg = workload.datagen
    return resolve_seed(dg.seed, workload.schema_type.value, getattr(dg, "corpus_role", None))
