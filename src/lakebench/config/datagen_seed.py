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
# applied by datagen (datagen_rs/src/robustness.rs, --robustness-perturbation;
# lane T2). A registered robustness run needs datagen.robustness_perturbation
# (``perturbation_error``), and the Rust driver refuses the robustness seed
# without the flag, so the seed is never spent on an unperturbed corpus.
ROBUSTNESS_PERTURBATION_IMPLEMENTED = True


#: Manifest stamp written by the generator on every row of a perturbed corpus
#: (injection_parameters map keys; datagen_rs/src/robustness.rs MANIFEST_*).
MANIFEST_STAMP_KEY = "robustness_perturbation"
#: prereg corpora.robustness_perturbation key -> manifest key.
MANIFEST_MULTIPLIER_KEYS = {
    "median_amount_multiplier": "robustness_median_amount_multiplier",
    "persona_sd_multiplier": "robustness_persona_sd_multiplier",
    "dormancy_range_multiplier": "robustness_dormancy_range_multiplier",
}
MANIFEST_KEYS = (MANIFEST_STAMP_KEY, *MANIFEST_MULTIPLIER_KEYS.values())


def summarise_stamp(groups) -> dict:
    """Summarise the manifest stamp from ``groups``: an iterable of
    (values, count) where ``values`` maps each of MANIFEST_KEYS to the row's
    injection_parameters value (None when absent) and ``count`` is how many
    manifest rows share them. Returns n_instances, n_stamped (rows whose stamp
    is exactly "true") and, per multiplier key, the sorted distinct values on
    stamped rows."""
    n = stamped = 0
    mult: dict[str, set] = {k: set() for k in MANIFEST_MULTIPLIER_KEYS.values()}
    for values, count in groups:
        n += int(count)
        if values.get(MANIFEST_STAMP_KEY) == "true":
            stamped += int(count)
            for k in mult:
                mult[k].add(values.get(k))
    return {
        "n_instances": n,
        "n_stamped": stamped,
        "multipliers": {k: sorted(v, key=str) for k, v in mult.items()},
    }


def perturbation_stamp_error(
    corpora: dict, corpus_role: str | None, stamp: dict, declared: bool | None = None
) -> str | None:
    """Why a corpus with manifest ``stamp`` (``summarise_stamp``) may not be
    scored as ``corpus_role``, or None.

    The stamp is read from the corpus, not the deployment config: an image
    without the perturbation writes no stamp, so a registered robustness look
    fails closed on its corpus instead of trusting the config. The robustness
    role needs every row stamped with the pre-registered multipliers; a
    calibration or evaluation role refuses any stamp; a mixed manifest is
    always refused. ``declared`` is the deployment's
    datagen.robustness_perturbation when known (None locally): a corpus that
    disagrees with it is refused, so the report never records a perturbation
    the corpus does not have.
    """
    n, k = int(stamp["n_instances"]), int(stamp["n_stamped"])
    if 0 < k < n:
        return (
            f"the manifest is mixed: {k} of {n} instances carry the robustness "
            "stamp (manifests from different runs in one prefix?)"
        )
    stamped = n > 0 and k == n
    if corpus_role == "robustness":
        if not stamped:
            return (
                "the registered robustness look needs a perturbed corpus, and this "
                "manifest carries no robustness stamp (generated without "
                "--robustness-perturbation, or by an image that predates it)"
            )
        want = corpora["robustness_perturbation"]
        for prereg_key, mkey in MANIFEST_MULTIPLIER_KEYS.items():
            vals = stamp["multipliers"].get(mkey) or []
            try:
                ok = len(vals) == 1 and float(vals[0]) == float(want[prereg_key])
            except (TypeError, ValueError):
                ok = False
            if not ok:
                return (
                    f"manifest {mkey} is {vals}, not the pre-registered "
                    f"{prereg_key} {want[prereg_key]}"
                )
        return None
    if stamped and corpus_role in ("calibration", "evaluation"):
        return f"the {corpus_role} corpus is never perturbed, and this manifest carries the robustness stamp"
    if declared is not None and bool(declared) != stamped:
        return (
            f"the deployment declares robustness_perturbation={bool(declared)} but the "
            f"corpus manifest says {stamped} (generated by another image or run?)"
        )
    return None


def perturbation_error(schema: str, corpus_role: str | None, perturbation: bool) -> str | None:
    """Why ``datagen.robustness_perturbation`` may not take this value, or None.

    The perturbation belongs to the financial schema. The registered
    robustness run must have it (its corpus is the perturbed one by
    definition, AML-GOALS R3(b)); a declared calibration or evaluation run must
    not. Without a declared role it is free on any seed the seed guard allows,
    so a perturbed dev corpus can be generated for testing.
    """
    if perturbation and schema != "financial":
        return "datagen.robustness_perturbation applies to the financial schema only"
    if corpus_role == "robustness" and not perturbation:
        return (
            "corpus_role 'robustness' needs datagen.robustness_perturbation: true "
            "(corpora.robustness_perturbation); the registered robustness corpus is "
            "the perturbed one"
        )
    if perturbation and corpus_role in ("calibration", "evaluation"):
        return f"the {corpus_role} corpus is never perturbed: unset datagen.robustness_perturbation"
    return None


def check_perturbation(schema: str, corpus_role: str | None, perturbation: bool) -> None:
    """Raise ValueError when ``perturbation_error`` refuses the combination."""
    err = perturbation_error(schema, corpus_role, perturbation)
    if err:
        raise ValueError(err)


def config_perturbation(cfg) -> bool:
    """``datagen.robustness_perturbation`` for a LakebenchConfig, checked."""
    workload = cfg.architecture.workload
    dg = workload.datagen
    on = bool(getattr(dg, "robustness_perturbation", False))
    check_perturbation(workload.schema_type.value, getattr(dg, "corpus_role", None), on)
    return on


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
