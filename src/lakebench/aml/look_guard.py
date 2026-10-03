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

import json
import re
from collections.abc import Iterable, Mapping
from decimal import Decimal, InvalidOperation
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
_INT_TEXT = re.compile(r"[+-]?[0-9][0-9_]*")


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
    if financial:
        return registered_corpus_at(cfg)
    return None


def registered_corpus_at(cfg: Any) -> str | None:
    """Why ``cfg``'s bronze datagen prefix is where a registered corpus was
    generated on this host (the corpus ledger, ``generate
    --registered-corpus``), or None. A development config pointed at that
    bucket would read the registered corpus: bronze-verify, silver, gold and
    the TM operations would run on it before the scorer's manifest check. A
    ledger that cannot be read refuses (fail closed)."""
    path = ds.corpora_ledger_path()
    if not path.is_file():
        return None
    try:
        from lakebench.deploy.datagen import bronze_datagen_prefix

        here = (
            f"s3://{cfg.platform.storage.s3.buckets.bronze}/"
            f"{bronze_datagen_prefix(cfg).rstrip('/')}/"
        )
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            entry = json.loads(line)
            if entry.get("kind") == "registered_corpus" and entry.get("bronze_uri") == here:
                return (
                    f"its bronze prefix {here} holds a registered {entry.get('role')} corpus "
                    f"(corpus ledger {path}, attempt {entry.get('attempt')}); use another bronze "
                    "bucket for development"
                )
    except Exception as e:  # noqa: BLE001 -- unreadable ledger: refuse
        return f"the corpus ledger {path} cannot be read ({type(e).__name__})"
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


#: What ``recorded_seed_role`` returns for a ``{seed_ref: None}`` form.
WITHHELD = "withheld"


def recorded_seed_role(value: Any) -> str | None:
    """The held-out role a recorded seed names, ``WITHHELD`` for a withheld
    ``{seed_ref: None}`` form, or None. Reads an integer (or an integer-valued
    float), decimal text (with ``+``, ``-``, ``_`` or a ``.0`` tail), a salted
    hash in either case, a ``{seed_ref, role}`` mapping and a list of any of
    these. Raises when the held-out record cannot be read."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Mapping):
        r = value.get("role")
        if r in PROTECTED_ROLES:
            return str(r)
        if "seed_ref" in value and value.get("seed_ref") is None:
            return WITHHELD
        return recorded_seed_role(value.get("seed_ref"))
    if isinstance(value, list | tuple):
        roles = [recorded_seed_role(v) for v in value]
        return next((r for r in roles if r is not None), None)
    if isinstance(value, int):
        return _held_out(value)
    if isinstance(value, float):
        return _held_out(int(value)) if value.is_integer() else None
    if isinstance(value, str):
        text = value.strip()
        if _HEX64.fullmatch(text.lower()):
            return _hash_role(text.lower())
        if _INT_TEXT.fullmatch(text):
            return _held_out(int(text.replace("_", "")))
        try:
            d = Decimal(text)
        except InvalidOperation:
            return None
        if d.is_finite() and d == d.to_integral_value():
            return _held_out(int(d))
    return None


def protected_record_reason(
    record: Any, *, require_identity: bool = True, fail_closed: bool = True
) -> str | None:
    """Why a stored run record (``metrics.json``) is from a protected AML
    corpus, or None.

    Refused: a protected ``experiment.corpus.corpus_role``; a recorded seed
    that names a held-out seed in any form ``recorded_seed_role`` reads, or
    is withheld. With ``fail_closed`` (the default), a financial record whose
    held-out check cannot run because the held-out record cannot be read is
    refused; ``compare``, which spends nothing and hides every integer seed
    then, passes False. With ``require_identity`` (the release gate and the
    audit), a financial record with no corpus block or no seed is refused as
    unidentified; ``compare`` passes False and shows such a record as not
    established instead."""
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
    if seed is None or isinstance(seed, bool):
        if financial and require_identity:
            return "unidentified: a financial record with no corpus seed"
        return None
    try:
        held = recorded_seed_role(seed)
    except Exception as e:  # noqa: BLE001 -- unreadable held-out record
        if financial and fail_closed:
            return f"the held-out record cannot be read ({type(e).__name__})"
        return None
    if held == WITHHELD:
        return "its seed is withheld"
    if held is not None:
        return f"its seed is the registered {held} seed"
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
    records: Iterable[tuple[str, Any]],
    verb: str,
    *,
    require_identity: bool = False,
    fail_closed: bool = False,
) -> None:
    """Raise UsageError (exit 2) for the first ``(run_id, record)`` whose
    record is from a protected AML corpus. The defaults are compare's: a
    record is refused only when it is shown to be protected."""
    for run_id, record in records:
        reason = protected_record_reason(
            record, require_identity=require_identity, fail_closed=fail_closed
        )
        if reason is not None:
            raise UsageError(
                f"Refused: `{verb}` never reads a run on a protected AML corpus "
                f"(run {run_id}: {reason}).",
                next=_NEXT,
                path=PATH,
            )
