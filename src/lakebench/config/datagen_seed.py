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
  attached to another seed to make a tuning run look registered;
- a registered evaluation or robustness run is refused until the
  pre-registration sets ``corpora.registered_looks_open`` (done with the
  datagen freeze); after the look the seed is appended to
  ``corpora.spent_seeds``, which refuses any second look.

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


# The robustness look scores seed 90000042 with corpora.robustness_perturbation
# applied by datagen. Until datagen implements it (programme step 2b, lane T2)
# a registered robustness run is refused: it would spend the seed on an
# unperturbed corpus. T2 sets this True in the same change as the flag.
ROBUSTNESS_PERTURBATION_IMPLEMENTED = False


def spent_from(corpora: dict) -> frozenset[int]:
    """``corpora.spent_seeds``, strictly: a missing key or anything but a list
    of integers raises, so a damaged pre-registration fails closed instead of
    reading as "nothing is spent"."""
    raw = corpora["spent_seeds"]
    if not isinstance(raw, list) or not all(
        isinstance(x, int) and not isinstance(x, bool) for x in raw
    ):
        raise ValueError(f"corpora.spent_seeds must be a list of integers, got {raw!r}")
    return frozenset(raw)


def looks_open(corpora: dict) -> bool:
    """``corpora.registered_looks_open`` is open only when it is literally
    true; a string such as "false" or a missing key keeps looks closed."""
    return corpora.get("registered_looks_open") is True


def spent_seeds() -> frozenset[int]:
    """Seeds the AML pre-registration has retired (looked at, or voided)."""
    return spent_from(_corpora())


def calibration_seed() -> int:
    return int(_corpora()["calibration_seed"])


def role_seed(role: str) -> int:
    return int(_corpora()[f"{role}_seed"])


def protected_seeds() -> dict[int, str]:
    """{seed: role} for the evaluation and robustness seeds."""
    return {role_seed(r): r for r in PROTECTED_ROLES}


def aml_seed_error(
    corpora: dict,
    seed: int | None,
    corpus_role: str | None = None,
    matched: list[int] | tuple[int, ...] = (),
    counts_only: bool = False,
    claim_verified: bool | None = None,
) -> str | None:
    """Why ``seed`` may not be generated or scored, or None.

    ``corpora`` is the pre-registration's ``corpora`` block (passed in so the
    flat copy on the Spark driver can use it). ``matched`` lists guarded seeds
    whose instance seeds a corpus's manifest reproduces: the corpus's real seed
    whatever ``seed`` claims. ``claim_verified`` is whether the manifest was
    checked to come wholly from ``seed`` (None: no corpus yet, as at
    generation time); a registered run on a corpus that is not verified is
    refused, so the one look is never spent on the wrong corpus.
    ``counts_only`` is a units-and-labels smoke run
    that computes no AP; it is not a look, so it may touch a protected corpus
    but never a spent one.
    """
    if corpus_role is not None and corpus_role not in ROLES:
        return f"corpus_role must be one of {ROLES}, got {corpus_role!r}"
    for actual in matched:
        if seed is not None and int(actual) != int(seed):
            return f"the corpus was generated with seed {actual}, not the claimed {seed}"
    eff = int(matched[0]) if matched else seed
    spent = spent_from(corpora)
    if eff is not None and eff in spent:
        return (
            f"seed {eff} is listed as spent in the AML pre-registration "
            "(corpora.spent_seeds): a corpus from it has already been looked at or "
            "voided and must not be regenerated or scored. Use the calibration seed "
            f"({corpora['calibration_seed']}) or another unregistered seed."
        )
    protected = {int(corpora[f"{r}_seed"]): r for r in PROTECTED_ROLES}
    if corpus_role is not None:
        if counts_only and corpus_role in PROTECTED_ROLES:
            return "a counts-only run is not a look: do not declare a registered role for it"
        want = int(corpora[f"{corpus_role}_seed"])
        if eff != want:
            return f"corpus_role {corpus_role!r} is registered for seed {want}, not {eff}"
        if corpus_role in PROTECTED_ROLES and claim_verified is False:
            return (
                f"the corpus is not verified as seed {want}'s: its manifest does not "
                "wholly come from that seed, so the registered look would be spent on "
                "another corpus"
            )
        if corpus_role in PROTECTED_ROLES and not looks_open(corpora):
            return (
                f"registered {corpus_role} runs are closed: the pre-registration's "
                "corpora.registered_looks_open is false. It is set true with the datagen "
                "freeze, and the seed is appended to corpora.spent_seeds after its look."
            )
        if corpus_role == "robustness" and not ROBUSTNESS_PERTURBATION_IMPLEMENTED:
            return (
                "registered robustness runs are refused until datagen applies "
                "corpora.robustness_perturbation (programme step 2b): scoring seed "
                f"{want} unperturbed would spend it on the wrong corpus"
            )
        return None
    role = protected.get(eff) if eff is not None else None
    if role is not None and not counts_only:
        return (
            f"seed {eff} is the pre-registered {role} seed. It is generated and "
            f"scored once, as the registered {role} gate run after the datagen "
            f"freeze: declare corpus_role: {role} (config) or --registered {role} "
            "(scripts/aml_gate.py) for that run. Any other use burns the seed."
        )
    return None


def check_aml_seed(seed: int | None, corpus_role: str | None = None) -> None:
    """Raise ValueError unless ``seed`` may be used with ``corpus_role``."""
    err = aml_seed_error(_corpora(), seed, corpus_role)
    if err:
        raise ValueError(err)


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
