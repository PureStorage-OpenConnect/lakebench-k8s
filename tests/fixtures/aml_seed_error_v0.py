"""A verbatim copy of ``aml_seed_error`` and ``_match_label`` as they were
at AM-22's parent commit (``PARENT``), before AM-22 made their refusal texts
seed-free. ``tests/test_aml_seed_error_verdicts.py`` runs both over a
synthetic matrix: the edited function must refuse exactly where this one did.
Do not edit the two functions: ``BASELINE_AST_SHA256`` pins them.
"""

# ruff: noqa: E501
from __future__ import annotations

from lakebench.config.datagen_seed import (  # noqa: F401 -- names the copy reads
    PROTECTED_ROLES,
    ROBUSTNESS_PERTURBATION_IMPLEMENTED,
    ROLES,
    VERDICT_ROLES,
    HeldOut,
    _heldout,
    heldout_role,
    looks_open,
    spent_from,
)

#: AM-22's parent: the CD-3+4 commits on integrate dfc8afa7 (local stack).
PARENT = "64f1294a0fb3bbe6f556724498ed93017183fbf2"
#: sha256 of ast.dump of the two functions below.
BASELINE_AST_SHA256 = "83f00d6e0f3822b2bc3dc5f42f6f873d3d52289881ffc2615a6279bb87c4070f"


def _match_label(seed: int | None, role: str | None) -> str:
    """How a refusal names a seed: a spent or unregistered seed by value
    (public), a held-out seed only by its role."""
    if role in PROTECTED_ROLES:
        return f"the registered {role} seed"
    if seed is None:
        return "a spent seed" if role == "spent" else "another seed"
    return f"seed {seed}"


def aml_seed_error(
    corpora: dict,
    seed: int | None,
    corpus_role: str | None = None,
    matched: list | tuple = (),
    counts_only: bool = False,
    claim_verified: bool | None = None,
    *,
    heldout: HeldOut | None = None,
) -> str | None:
    """Why ``seed`` may not be generated or scored, or None.

    ``corpora`` is the pre-registration's ``corpora`` block (passed in so the
    flat copy on the Spark driver can use it); its spent_seeds and the hash
    file's spent list are both spent. ``matched`` describes the guarded
    seeds a corpus's manifest comes from, whatever ``seed`` claims: either
    seeds, or the roles ``corpus_verdict`` recovered ("evaluation",
    "robustness", "spent"; strictest first). ``claim_verified`` is whether the
    manifest was checked to come wholly from ``seed`` (None: no corpus yet, as
    at generation time); a registered run on a corpus that is not verified is
    refused, so the one look is never spent on the wrong corpus.
    ``counts_only`` is a units-and-labels smoke run that computes no AP; it is
    not a look, so it may touch a protected corpus but never a spent one.
    Held-out seeds are checked against heldout_hashes.json (``heldout``, or
    the packaged file when None), and no message prints one.
    """
    if corpus_role is not None and corpus_role not in ROLES:
        return f"corpus_role must be one of {ROLES}, got {corpus_role!r}"
    h = heldout if heldout is not None else _heldout()
    spent = spent_from(corpora) | h.spent

    def role_of(s: int) -> str | None:
        return "spent" if s in spent else heldout_role(s, h)

    found: list[tuple[int | None, str | None]] = []
    for m in matched:
        if isinstance(m, str):
            if m not in VERDICT_ROLES:
                raise ValueError(f"matched role must be one of {VERDICT_ROLES}, got {m!r}")
            found.append((None, m))
        else:
            found.append((int(m), role_of(int(m))))
    if seed is not None:
        for s, r in found:
            if s is not None and s != int(seed):
                return f"the corpus was generated with {_match_label(s, r)}, not the claimed seed"
            if s is None and claim_verified is not True:
                return (
                    f"the corpus holds instances from {_match_label(None, r)}, not only the "
                    "claimed seed"
                )
    if found:
        s0, eff_role = found[0]
        eff = s0 if s0 is not None else (int(seed) if seed is not None else None)
    else:
        eff = int(seed) if seed is not None else None
        eff_role = role_of(eff) if eff is not None else None
    # Spent wins over every other role: a held-out seed that is also spent (its
    # look is recorded, or it was voided) is refused even as its registered run.
    if any(r == "spent" for _, r in found) or (eff is not None and eff in spent):
        eff_role = "spent"
    if eff_role == "spent":
        # A spent seed is public, unless it is also held out (voided before its
        # look): then it is named by role only.
        held_role = heldout_role(eff, h) if eff is not None else None
        label = "the corpus seed" if eff is None else _match_label(eff, held_role)
        return (
            f"{label} is listed as spent "
            "(the AML pre-registration's corpora.spent_seeds, a recorded look, or "
            "heldout_hashes.json): a corpus from it has already been looked at or voided and "
            "must not be regenerated or scored. Use the calibration seed "
            f"({corpora['calibration_seed']}) or another unregistered seed."
        )
    if corpus_role is not None:
        if counts_only and corpus_role in PROTECTED_ROLES:
            return "a counts-only run is not a look: do not declare a registered role for it"
        if corpus_role in PROTECTED_ROLES:
            if eff_role != corpus_role:
                return (
                    f"corpus_role {corpus_role!r} is registered for another seed: the given "
                    f"seed is not the registered {corpus_role} seed (heldout_hashes.json)"
                )
        else:
            want = int(corpora["calibration_seed"])
            if eff != want:
                return (
                    f"corpus_role 'calibration' is registered for seed {want}, not "
                    f"{_match_label(eff, eff_role)}"
                )
        if corpus_role in PROTECTED_ROLES and claim_verified is False:
            return (
                f"the corpus is not verified as the registered {corpus_role} seed's: its "
                "manifest does not wholly come from that seed, so the registered look would "
                "be spent on another corpus"
            )
        if corpus_role in PROTECTED_ROLES and not looks_open(corpora):
            return (
                f"registered {corpus_role} runs are closed: the pre-registration's "
                "corpora.registered_looks_open is false. It is set true with the datagen "
                "freeze, and the seed is spent after its look."
            )
        if corpus_role == "robustness" and not ROBUSTNESS_PERTURBATION_IMPLEMENTED:
            return (
                "registered robustness runs are refused until datagen applies "
                "corpora.robustness_perturbation: scoring the robustness seed unperturbed "
                "would spend it on the wrong corpus"
            )
        return None
    if eff_role in PROTECTED_ROLES and not counts_only:
        return (
            f"the seed (given, or recovered from the corpus) is the pre-registered {eff_role} "
            f"seed. It is generated and scored once, as the registered {eff_role} gate run "
            f"after the datagen freeze: declare corpus_role: {eff_role} (config) or "
            f"--registered {eff_role} (scripts/aml_gate.py) for that run. Any other use "
            "burns the seed."
        )
    return None
