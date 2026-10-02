"""The protected AML corpus guard.

The evaluation and robustness corpora are scored once each, as the
registered look, by ``scripts/aml_gate.py --registered``, which records the
look before it fits anything. Every other command that reads or scores data
refuses them before its first cluster call, so a look can never be spent
silently. One guard serves every caller:

- ``protected_corpus_reason(cfg)``: a config that declares a protected
  ``corpus_role`` or whose ``datagen.seed`` hashes to a held-out seed
  (``heldout_hashes.json`` and the compiled floor);
- ``protected_record_reason(record)``: a stored run record whose corpus is a
  protected one, for ``compare``, the release gate and the held-out audit;
- ``manifest_protected_reason(rows)``: a corpus manifest, over every row
  (``config/datagen_seed.py``, which also ships flat to the Spark driver);
- ``refuse_if_protected(cfg, verb)``: the refusal itself, exit 2 on the
  ``run.protected_corpus`` path.

Every reason names a role or a kind, never a seed.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from lakebench.config import datagen_seed as ds
from lakebench.config.datagen_seed import PROTECTED_ROLES, manifest_protected_reason
from lakebench.exit_codes import UsageError

__all__ = [
    "PATH",
    "PROTECTED_ROLES",
    "manifest_protected_reason",
    "protected_corpus_reason",
    "protected_record_reason",
    "refuse_if_protected",
    "refuse_protected_records",
]

#: The exit path every protected-corpus refusal takes (exit 2).
PATH = "run.protected_corpus"

_NEXT = (
    "Registered looks run only through `scripts/aml_gate.py --registered`, and their "
    "corpus is generated only with `lakebench generate --registered-corpus`. Use the "
    "calibration seed or another unregistered seed for everything else."
)
_HEX64 = re.compile(r"[0-9a-f]{64}")


def _held_out(seed: int) -> str | None:
    """``heldout_role`` of an integer seed; raises when the held-out record
    cannot be read."""
    return ds.heldout_role(int(seed))


def protected_corpus_reason(cfg: Any) -> str | None:
    """Why ``cfg`` names a protected AML corpus, or None.

    A declared ``corpus_role`` of evaluation or robustness, or a
    ``datagen.seed`` that hashes to a held-out seed, whatever the role says.
    An unset seed resolves to the calibration seed (financial) or 42, neither
    protected, so ``config_seed`` (which can raise) is never called. For the
    financial schema a held-out record that cannot be read refuses (fail
    closed); for other schemas the seed check is skipped then, since their
    corpora are not AML data."""
    workload = cfg.architecture.workload
    dg = workload.datagen
    role = getattr(dg, "corpus_role", None)
    if role in PROTECTED_ROLES:
        return f"corpus_role {role}"
    seed = getattr(dg, "seed", None)
    if seed is None or isinstance(seed, bool):
        return None
    financial = workload.schema_type.value == "financial"
    try:
        held = _held_out(seed)
    except Exception as e:  # noqa: BLE001 -- unreadable: refuse for AML
        if financial:
            return (
                f"the held-out record cannot be read ({type(e).__name__}), so the seed is unchecked"
            )
        return None
    if held is not None:
        return f"datagen.seed is the registered {held} seed"
    return None


def _hash_role(ref: str) -> str | None:
    """The role whose hash list holds ``ref`` (a recorded ``seed_ref``),
    under the hash file's salt, and the floor's when it is the same salt."""
    h = ds._heldout()
    for r in PROTECTED_ROLES:
        if ref in h.roles.get(r, ()) or (h.floor_salt == h.salt and ref in h.floor.get(r, ())):
            return r
    return None


def _financial(record: Mapping[str, Any], corpus: Mapping[str, Any]) -> bool:
    return corpus.get("schema") == "financial" or record.get("financial_scoring") is not None


def protected_record_reason(record: Any, *, require_identity: bool = True) -> str | None:
    """Why a stored run record (``metrics.json``) is from a protected AML
    corpus, or None.

    Refused: a protected ``experiment.corpus.corpus_role``; a recorded seed
    that hashes to a held-out seed (an integer, a digit string, or the
    ``{seed_ref, role}`` form, whose role or hash is checked and whose
    withheld ``seed_ref`` refuses); a financial record whose held-out check
    cannot run because the record of held-out hashes cannot be read. With
    ``require_identity`` (the release gate and the audit), a financial record
    with no corpus block or no seed is refused as unidentified; ``compare``
    passes False and shows such a record as not established instead."""
    if not isinstance(record, Mapping):
        return "the record cannot be read"
    exp = record.get("experiment")
    corpus = exp.get("corpus") if isinstance(exp, Mapping) else None
    corpus = corpus if isinstance(corpus, Mapping) else {}
    financial = _financial(record, corpus)
    role = corpus.get("corpus_role")
    if role in PROTECTED_ROLES:
        return f"corpus_role {role}"
    seed = corpus.get("seed")
    try:
        if isinstance(seed, Mapping):
            r = seed.get("role")
            if r in PROTECTED_ROLES:
                return f"its seed is recorded as the registered {r} seed"
            ref = seed.get("seed_ref")
            if ref is None:
                return "its seed is withheld"
            if isinstance(ref, str) and _HEX64.fullmatch(ref):
                held = _hash_role(ref)
                if held is not None:
                    return f"its seed is the registered {held} seed"
            elif isinstance(ref, int | str) and str(ref).strip().isdigit():
                held = _held_out(int(str(ref).strip()))
                if held is not None:
                    return f"its seed is the registered {held} seed"
            return None
        if isinstance(seed, str) and _HEX64.fullmatch(seed):
            held = _hash_role(seed)
            if held is not None:
                return f"its seed is the registered {held} seed"
            return None
        if isinstance(seed, int) and not isinstance(seed, bool):
            held = _held_out(seed)
        elif isinstance(seed, str) and seed.strip().lstrip("-").isdigit():
            held = _held_out(int(seed.strip()))
        else:
            held = None
            if financial and require_identity:
                return "unidentified: a financial record with no corpus seed"
        if held is not None:
            return f"its seed is the registered {held} seed"
    except Exception as e:  # noqa: BLE001 -- unreadable held-out record
        if financial:
            return f"the held-out record cannot be read ({type(e).__name__})"
    return None


def refuse_if_protected(cfg: Any, verb: str) -> None:
    """Raise UsageError (exit 2, ``run.protected_corpus``) when ``cfg``
    names a protected AML corpus. Called right after ``load_config``, before
    any client is built."""
    reason = protected_corpus_reason(cfg)
    if reason is not None:
        raise UsageError(
            f"Refused: `{verb}` never runs on a protected AML corpus ({reason}).",
            next=_NEXT,
            path=PATH,
        )


def refuse_protected_records(
    records: Iterable[tuple[str, Any]], verb: str, *, require_identity: bool = False
) -> None:
    """Raise UsageError (exit 2) for the first ``(run_id, record)`` whose
    record is from a protected AML corpus."""
    for run_id, record in records:
        reason = protected_record_reason(record, require_identity=require_identity)
        if reason is not None:
            raise UsageError(
                f"Refused: `{verb}` never reads a run on a protected AML corpus "
                f"(run {run_id}: {reason}).",
                next=_NEXT,
                path=PATH,
            )
