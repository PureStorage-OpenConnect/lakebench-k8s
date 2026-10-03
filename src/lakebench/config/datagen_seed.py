"""The top-level datagen seed a deployment generates with, and the AML seed guard.

The seed names the corpus: the AML reference job reports it (LB_DATAGEN_SEED)
and the AML pre-registration assigns seeds to roles (calibration, evaluation,
robustness) and retires seeds that have been looked at. A cluster run used
to hard-code 42 in three places, which is the spent D0 seed, so every cluster
corpus silently reused it.

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
  attached to another seed to make a tuning run look registered. The
  evaluation and robustness seeds are known only as salted hashes in
  ``heldout_hashes.json`` (next to the pre-registration), so a registered
  run names its seed in ``datagen.seed`` and the guard checks its hash;
- a registered evaluation or robustness run is refused until the
  pre-registration sets ``corpora.registered_looks_open`` (done with the
  datagen freeze). The look records itself in ``aml_registered_looks.json``
  (next to the pre-registration): its seed when it starts, its report's
  sha256 before any verdict is printed. Every recorded seed is spent, so a
  second look is refused without editing the pre-registration. A held-out
  seed retired without a look (it became public, or a void needs a fresh
  seed) is recorded there as burned (``burn_seed``), and the owner appends
  a newly drawn seed's hash to its role in ``heldout_hashes.json``.

Only the standard library is imported, so config validation stays cheap.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

#: Seed for schemas the AML pre-registration does not govern (Customer 360,
#: IoT). Unchanged from the old hard-coded value so their corpora stay the same.
NON_AML_DEFAULT_SEED = 42

#: Roles whose corpus may only be generated or scored as the registered run.
PROTECTED_ROLES = ("evaluation", "robustness")
ROLES = ("calibration", *PROTECTED_ROLES)

_PREREG_PATH = (
    Path(__file__).resolve().parent.parent / "spark" / "data" / "aml" / "aml_preregistration.json"
)


#: The tracked record of registered looks, next to the
#: pre-registration in the package and flat next to this module on the
#: Spark driver (every AML data JSON is mounted there).
LOOKS_FILENAME = "aml_registered_looks.json"


def looks_path() -> Path:
    """The look record; raises FileNotFoundError when it is missing, so a
    lost record fails closed instead of reading as "nothing looked at"."""
    here = Path(__file__).resolve().parent
    # The packaged path first; the flat path only on the Spark driver, where
    # the package is absent (a stray copy next to this module never shadows).
    for p in (_PREREG_PATH.parent / LOOKS_FILENAME, here / LOOKS_FILENAME):
        if p.is_file():
            return p
    raise FileNotFoundError(f"{LOOKS_FILENAME} not found next to {__file__} or {_PREREG_PATH}")


def load_looks(path: str | os.PathLike | None = None) -> list[dict]:
    """The recorded looks, strictly: anything but {"looks": [entries with an
    integer seed and a role]} raises."""
    p = Path(path) if path is not None else looks_path()
    with open(p, encoding="utf-8") as f:
        doc = json.load(f)
    looks = doc.get("looks") if isinstance(doc, dict) else None
    if not isinstance(looks, list):
        raise ValueError(f"{p}: 'looks' must be a list")
    for e in looks:
        seed = e.get("seed") if isinstance(e, dict) else None
        if not isinstance(seed, int) or isinstance(seed, bool) or e.get("role") not in ROLES:
            raise ValueError(
                f"{p}: malformed look entry (keys {sorted(e) if isinstance(e, dict) else type(e).__name__})"
            )
    return looks


def recorded_seeds(path: str | os.PathLike | None = None) -> frozenset[int]:
    """Seeds with any recorded look (started or complete): all spent."""
    return frozenset(int(e["seed"]) for e in load_looks(path))


def with_recorded_looks(corpora: dict, path: str | os.PathLike | None = None) -> dict:
    """``corpora`` with every recorded look's seed added to spent_seeds."""
    out = dict(corpora)
    out["spent_seeds"] = sorted(spent_from(corpora) | recorded_seeds(path))
    return out


@lru_cache(maxsize=1)
def _corpora() -> dict:
    with open(_PREREG_PATH, encoding="utf-8") as f:
        return with_recorded_looks(json.load(f)["corpora"])


def _write_atomic(path: Path, doc: dict) -> None:
    """Write ``doc`` to ``path`` so a crash leaves the old file or the new
    one, and the new one is on disk (file and directory fsynced) on return."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(doc, indent=2) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _locked_update(path: str | os.PathLike | None, update) -> dict:
    """Apply ``update(doc) -> entry`` to the record under an exclusive lock,
    write it atomically, re-read it and return the entry. Raises on any
    failure (the caller must not print a verdict then)."""
    import fcntl

    p = Path(path) if path is not None else looks_path()
    with open(p.with_name(f".{p.name}.lock"), "a") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        load_looks(p)
        with open(p, encoding="utf-8") as f:
            doc = json.load(f)
        entry = update(doc)
        _write_atomic(p, doc)
        if entry not in load_looks(p):
            raise OSError(f"{p}: the look entry did not read back")
    _corpora.cache_clear()
    return entry


def _utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def claim_look(
    role: str, seed: int, meta: dict | None = None, path: str | os.PathLike | None = None
) -> dict:
    """Record that a registered ``role`` look on ``seed`` has started, before
    any model is fitted: from here the seed is spent whatever happens next.
    Raises when the seed already has a look."""
    if role not in PROTECTED_ROLES:
        raise ValueError(f"only {PROTECTED_ROLES} looks are recorded, not {role!r}")

    def update(doc):
        if any(int(e["seed"]) == int(seed) for e in doc["looks"]):
            raise ValueError(f"the {role} seed already has a recorded look")
        entry = {
            "role": role,
            "seed": int(seed),
            "state": "started",
            "started_utc": _utc(),
            **(meta or {}),
        }
        doc["looks"].append(entry)
        return entry

    return _locked_update(path, update)


def complete_look(
    role: str,
    seed: int,
    report_sha256: str,
    report_path: str,
    meta: dict | None = None,
    path: str | os.PathLike | None = None,
) -> dict:
    """Record the finished look's report sha256 (the report must already be
    written) on the started entry for ``seed``. A seed with no started entry,
    or whose look is already complete, is refused: the record never changes a
    recorded hash."""
    if role not in PROTECTED_ROLES:
        raise ValueError(f"only {PROTECTED_ROLES} looks are recorded, not {role!r}")

    def update(doc):
        mine = [e for e in doc["looks"] if int(e["seed"]) == int(seed)]
        if len(mine) > 1 or (mine and mine[0].get("state") != "started"):
            raise ValueError(f"the {role} look is already complete or recorded twice")
        if mine and mine[0]["role"] != role:
            raise ValueError(f"the seed was claimed as {mine[0]['role']!r}, not {role!r}")
        if not mine:
            raise ValueError(f"the {role} seed has no started look to complete")
        entry = mine[0]
        entry.update(
            state="complete",
            completed_utc=_utc(),
            report_sha256=report_sha256,
            report_path=str(report_path),
            **(meta or {}),
        )
        return entry

    return _locked_update(path, update)


def burn_seed(
    role: str,
    seed: int,
    reason: str,
    meta: dict | None = None,
    path: str | os.PathLike | None = None,
    heldout: HeldOut | None = None,
) -> dict:
    """Record that a held-out ``role`` seed is retired without a completed
    look (burned: it became public, or a void retires a look that started).
    The entry has state ``burned`` and the owner's ``reason``; from here the
    seed is spent, so ``claim_look`` and ``complete_look`` refuse it and
    heldout_hashes.json may list it in ``spent``
    (``heldout_history_problems``). The seed is written in plaintext, which
    is why only a seed that is public or given up is burned. A seed whose
    only entries are ``started`` looks for the same role (a void) gets the
    burn beside them; any other entry, an empty reason, or a seed that is
    not registered for ``role`` (``heldout``, or the packaged hash file and
    floor) raises."""
    if role not in PROTECTED_ROLES:
        raise ValueError(f"only {PROTECTED_ROLES} seeds are burned, not {role!r}")
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("a burn needs a reason (the owner decision that retires the seed)")
    if heldout_role(seed, heldout) != role:
        raise ValueError(
            f"the seed is not a registered {role} seed; only a held-out seed is burned"
        )

    def update(doc):
        mine = [e for e in doc["looks"] if int(e["seed"]) == int(seed)]
        if any(e.get("state") != "started" or e.get("role") != role for e in mine):
            raise ValueError(f"the {role} seed already has a completed, burned or other-role entry")
        entry = {
            "role": role,
            "seed": int(seed),
            "state": "burned",
            "burned_utc": _utc(),
            "reason": reason.strip(),
            **(meta or {}),
        }
        doc["looks"].append(entry)
        return entry

    return _locked_update(path, update)


# The robustness look scores the registered robustness seed with
# corpora.robustness_perturbation applied by datagen (datagen_rs/src/robustness.rs,
# --robustness-perturbation). A registered robustness run needs
# datagen.robustness_perturbation (``perturbation_error``), and the Rust driver refuses the robustness seed
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
    definition); a declared calibration or evaluation run must
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
        # Types only, never the values: a held-out seed appended here by
        # mistake must not be echoed into a driver log or a SystemExit.
        got = type(raw).__name__ if not isinstance(raw, list) else "a list holding a non-integer"
        raise ValueError(f"corpora.spent_seeds must be a list of integers, got {got}")
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


# ---------------------------------------------------------------------------
# Held-out seeds as salted hashes
# ---------------------------------------------------------------------------
#
# The evaluation and robustness seeds are never stored in plaintext here: the
# guard holds a salted SHA-256 per held-out seed, read at run time from
# heldout_hashes.json (next to the pre-registration, flat next to this module
# on the Spark driver, or LB_HELDOUT_HASHES on the datagen pod). The file may
# only be appended to: heldout_history_problems is the rule, which the frozen
# guard (QR-10) calls for every commit once it lands. Every registered hash is
# also compiled in below (_HELDOUT_FLOOR; the owner's redraw appends to both),
# checked under its own salt, so a stripped or re-salted file still protects
# those seeds; tests/test_heldout.py holds the two equal. The Rust floor
# (datagen_rs/src/heldout.rs) keeps the first hashes until the next datagen
# image lifts the rest. The salt is public: a hash hides only a seed drawn uniformly
# from 63 bits; an 8-digit seed is recovered from its hash in seconds.

HELDOUT_FILENAME = "heldout_hashes.json"
HELDOUT_FORMAT = 1
HELDOUT_ALGORITHM = "sha256(bytes.fromhex(salt) + b':' + decimal(seed))"
ABSENCE_MODES = ("report", "enforce")
_HELDOUT_KEYS = frozenset({"format", "algorithm", "salt", "roles", "spent", "absence_check"})
_HEX64 = re.compile(r"[0-9a-f]{64}")

# BEGIN HELDOUT FLOOR (written once, by the owner, when the file was created)
_HELDOUT_FLOOR: dict = {
    "salt": "267910981b7370a7aaed163588284507373136289bfeec908df42ae15e1d75aa",
    "roles": {
        "evaluation": ("206064919eb86d06940ba8f4a66510605707e6cfe99bb2564f47dc859c2cba06",),
        "robustness": ("9d730778dff8ae49bc2eb428a83016de00a9f227e6c0a9c43f84043fe2869562",),
    },
}
# END HELDOUT FLOOR


def seed_hash(salt: str, seed: int) -> str:
    """The salted hash of one seed: sha256(salt bytes + b':' + decimal seed)."""
    if isinstance(seed, bool):
        raise TypeError("a seed is an integer, not a bool")
    return hashlib.sha256(bytes.fromhex(salt) + b":" + str(int(seed)).encode("ascii")).hexdigest()


@dataclass(frozen=True)
class HeldOut:
    """The held-out record: the file's role hashes under its salt, its spent
    seeds, the absence-check mode, and the compiled floor under its salt."""

    salt: str
    roles: Mapping[str, frozenset[str]]
    spent: frozenset[int]
    absence_check: str
    floor_salt: str
    floor: Mapping[str, frozenset[str]]

    def __repr__(self) -> str:  # hashes and spent seeds are public, but keep logs short
        n = {r: len(h) for r, h in self.roles.items()}
        return f"HeldOut(roles={n}, spent={len(self.spent)}, absence_check={self.absence_check!r})"


def heldout_path() -> Path:
    """The hash file: the packaged path, then the flat path next to this
    module (the Spark driver), then ``LB_HELDOUT_HASHES``. Missing raises
    FileNotFoundError, so a lost file fails closed."""
    here = Path(__file__).resolve().parent
    for p in (_PREREG_PATH.parent / HELDOUT_FILENAME, here / HELDOUT_FILENAME):
        if p.is_file():
            return p
    env = os.environ.get("LB_HELDOUT_HASHES")
    if env and Path(env).is_file():
        return Path(env)
    raise FileNotFoundError(
        f"{HELDOUT_FILENAME} not found next to {__file__}, {_PREREG_PATH} or LB_HELDOUT_HASHES"
    )


def _heldout_doc_problems(doc) -> list[str]:
    """Structural problems of a hash-file document; messages never print a value."""
    if not isinstance(doc, dict):
        return ["the document is not an object"]
    out = []
    extra = sorted(k for k in doc if k not in _HELDOUT_KEYS and not str(k).startswith("_"))
    if extra:
        out.append(f"unknown keys {extra}")
    if type(doc.get("format")) is not int or doc.get("format") != HELDOUT_FORMAT:
        out.append(f"format must be {HELDOUT_FORMAT}")
    if doc.get("algorithm") != HELDOUT_ALGORITHM:
        out.append("algorithm is not the supported one")
    if not (isinstance(doc.get("salt"), str) and _HEX64.fullmatch(doc["salt"])):
        out.append("salt must be 64 lowercase hex characters")
    roles = doc.get("roles")
    if not isinstance(roles, dict):
        out.append("roles must be an object")
    else:
        for r in roles:
            if r not in PROTECTED_ROLES:
                out.append(f"role {r!r} is not one of {PROTECTED_ROLES}")
        for r in PROTECTED_ROLES:
            hs = roles.get(r)
            if not isinstance(hs, list):
                out.append(f"roles.{r} must be a list")
                continue
            for i, h in enumerate(hs):
                if not (isinstance(h, str) and _HEX64.fullmatch(h)):
                    out.append(f"roles.{r}[{i}] is not a 64-character lowercase hex hash")
        # A hash in two places would give its seed whichever role is checked
        # first, so each hash appears once across every role.
        seen: dict[str, str] = {}
        for r in PROTECTED_ROLES:
            hs = roles.get(r)
            for i, h in enumerate(hs if isinstance(hs, list) else []):
                if not isinstance(h, str):
                    continue
                if h in seen:
                    out.append(f"roles.{r}[{i}] repeats a hash already in roles.{seen[h]}")
                seen.setdefault(h, f"{r}[{i}]")
    spent = doc.get("spent")
    if not isinstance(spent, list) or not all(
        isinstance(x, int) and not isinstance(x, bool) for x in spent
    ):
        out.append("spent must be a list of integers")
    else:
        # A seed is a non-negative i64. Anything else, such as seed - 2**64,
        # hashes to no role, so it would pass the spent rule while publishing
        # a held-out seed in a trivially reversible form.
        for i, x in enumerate(spent):
            if not 0 <= x < 1 << 63:
                out.append(f"spent[{i}] is outside 0..2^63-1")
    if doc.get("absence_check") not in ABSENCE_MODES:
        out.append(f"absence_check must be one of {ABSENCE_MODES}")
    return out


def _floor() -> tuple[str, dict[str, frozenset[str]]]:
    salt = _HELDOUT_FLOOR.get("salt")
    roles = _HELDOUT_FLOOR.get("roles") or {}
    ok = isinstance(salt, str) and bool(_HEX64.fullmatch(salt))
    out = {}
    for r in PROTECTED_ROLES:
        hs = tuple(roles.get(r) or ())
        ok = ok and bool(hs) and all(isinstance(h, str) and _HEX64.fullmatch(h) for h in hs)
        out[r] = frozenset(hs)
    if not ok:
        raise ValueError(
            "the compiled held-out floor (_HELDOUT_FLOOR) is not initialised: every protected "
            "role needs a hash under a 64-hex salt; refusing to check seeds without it"
        )
    return str(salt), out


def load_heldout(path: str | os.PathLike | None = None) -> HeldOut:
    """The hash file plus the compiled floor, strictly: an unknown format or
    algorithm, a bad salt or hash, a role outside PROTECTED_ROLES, or an
    uninitialised floor raises (fail closed)."""
    p = Path(path) if path is not None else heldout_path()
    with open(p, encoding="utf-8") as f:
        doc = json.load(f)
    problems = _heldout_doc_problems(doc)
    if problems:
        raise ValueError(f"{p}: malformed held-out hash file: {'; '.join(problems)}")
    floor_salt, floor = _floor()
    if doc["salt"] == floor_salt:
        for r in PROTECTED_ROLES:
            for other in PROTECTED_ROLES:
                if other != r and set(doc["roles"][r]) & floor[other]:
                    raise ValueError(
                        f"{p}: roles.{r} holds a hash the compiled floor registers as {other}"
                    )
    return HeldOut(
        salt=doc["salt"],
        roles={r: frozenset(doc["roles"][r]) for r in PROTECTED_ROLES},
        spent=frozenset(int(x) for x in doc["spent"]),
        absence_check=doc["absence_check"],
        floor_salt=floor_salt,
        floor=floor,
    )


@lru_cache(maxsize=1)
def _heldout() -> HeldOut:
    return load_heldout()


def heldout_role(seed, heldout: HeldOut | None = None) -> str | None:
    """``evaluation`` or ``robustness`` when ``seed`` is a held-out seed (its
    hash is in the file under the file's salt, or in the compiled floor under
    the floor's salt), else None. A seed the file and the floor give
    different roles raises ValueError: a re-salted file must not move a floor
    seed to another role."""
    if seed is None:
        return None
    h = heldout if heldout is not None else _heldout()
    fh = seed_hash(h.salt, seed)
    gh = fh if h.floor_salt == h.salt else seed_hash(h.floor_salt, seed)
    in_file = {r for r in PROTECTED_ROLES if fh in h.roles.get(r, ())}
    in_floor = {r for r in PROTECTED_ROLES if gh in h.floor.get(r, ())}
    if len(in_file | in_floor) > 1:
        raise ValueError(
            "heldout_hashes.json and the compiled floor give one seed different roles "
            f"({sorted(in_file | in_floor)}); refusing to check seeds against it"
        )
    return next(iter(in_file | in_floor), None)


def is_spent(seed, heldout: HeldOut | None = None) -> bool:
    """Whether ``seed`` is in the hash file's spent list. Callers that hold
    the pre-registration also check ``corpora.spent_seeds`` and the recorded
    looks (aml_seed_error and corpus_verdict do)."""
    if seed is None:
        return False
    h = heldout if heldout is not None else _heldout()
    return int(seed) in h.spent


def seed_is_protected(value, heldout: HeldOut | None = None) -> bool:
    """Whether an integer seed must not be shown: held out, spent (the hash
    file, the pre-registration) or with a recorded look. A record that cannot
    be read hides every seed (True). A value that is not an integer is not a
    seed (False)."""
    if isinstance(value, bool):
        return False
    try:
        v = int(value)
    except (TypeError, ValueError):
        return False
    try:
        h = heldout if heldout is not None else _heldout()
        if heldout_role(v, h) is not None or v in h.spent:
            return True
        return v in spent_seeds() or v in recorded_seeds()
    except Exception:  # noqa: BLE001 -- unreadable: hide it
        return True


def seed_ref(schema: str, seed: int, heldout: HeldOut | None = None) -> str:
    """How a marker names a corpus seed: the plaintext decimal seed outside
    the financial schema, the salted hash under the file's salt for it, so a
    marker for a held-out corpus never carries the seed."""
    if schema != "financial":
        return str(int(seed))
    h = heldout if heldout is not None else _heldout()
    return seed_hash(h.salt, seed)


# Seed recovery. The generator derives each instance seed as
# splitmix64(seed ^ splitmix64(0xF100 + tid * TID_SEED_STRIDE + j))
# (datagen_rs::typology::schedule_ex). splitmix64 is a bijection on 64-bit
# words, so every manifest row gives back the corpus seed exactly.

TID_SEED_STRIDE = 100_000_000  # datagen_rs::typology::TID_SEED_STRIDE
_GAMMA = 0x9E3779B97F4A7C15
_M1 = 0xBF58476D1CE4E5B9
_M2 = 0x94D049BB133111EB
_MASK64 = (1 << 64) - 1
_M1_INV = pow(_M1, -1, 1 << 64)
_M2_INV = pow(_M2, -1, 1 << 64)


def _splitmix64(x: int) -> int:
    """datagen_rs::hash::splitmix64."""
    z = (x + _GAMMA) & _MASK64
    z = ((z ^ (z >> 30)) * _M1) & _MASK64
    z = ((z ^ (z >> 27)) * _M2) & _MASK64
    return z ^ (z >> 31)


def _unxorshift(y: int, s: int) -> int:
    x = y
    for _ in range(64 // s + 1):
        x = y ^ (x >> s)
    return x


def unsplitmix64(y: int) -> int:
    """The inverse of ``_splitmix64`` on 64-bit words."""
    z = _unxorshift(y & _MASK64, 31)
    z = (z * _M2_INV) & _MASK64
    z = _unxorshift(z, 27)
    z = (z * _M1_INV) & _MASK64
    z = _unxorshift(z, 30)
    return (z - _GAMMA) & _MASK64


def _signed64(x: int) -> int:
    x &= _MASK64
    return x - (1 << 64) if x >= 1 << 63 else x


class CorpusSeedError(ValueError):
    """A manifest the corpus seed cannot be recovered from. Every caller
    treats it as a refusal, never as a pass."""


#: Screening instances (datagen_rs::screening) derive their seed as
#: splitmix64(seed ^ SCREEN_SALT ^ splitmix64(0x51_0000 + 4 * k + r)) for
#: party k and relationship account r < MAX_REL, and their typology_id carries
#: a running count, not k. So a screening row cannot give back its seed alone,
#: but it can be checked against a candidate seed exactly: two inversions give
#: back 0x51_0000 + 4 * k + r, which is a small number only for the right seed.
SCREEN_SALT = 0x5C4E_E400_0000_0001  # datagen_rs::screening::SCREEN_SALT
_SCREEN_BASE = 0x51_0000
_SCREEN_MAX_REL = 2  # datagen_rs::screening::MAX_REL
_SCREEN_MAX_PARTIES = 1 << 32
_SCREEN_ID = re.compile(r"(?:SANCTIONS|PEP)_MATCH_[0-9]+")
#: Distinct corpus seeds a manifest may recover before it is refused as not
#: following the derivation.
MAX_CORPUS_SEEDS = 8


def _screen_row_from(iseed: int, seed: int) -> bool:
    """Whether a screening instance seed comes from corpus ``seed``."""
    inner = unsplitmix64(unsplitmix64(iseed & _MASK64) ^ (seed & _MASK64) ^ SCREEN_SALT)
    off = (inner - _SCREEN_BASE) & _MASK64
    return off < 4 * _SCREEN_MAX_PARTIES and off % 4 < _SCREEN_MAX_REL


def recover_corpus_seeds(rows: Iterable[tuple[str, int]]) -> set[int]:
    """Distinct corpus seeds behind (typology_id, instance_seed) rows.

    Consumes an iterator. A typology row (``<name>_<tid>_<j>``) gives back its
    corpus seed exactly; a screening row (``SANCTIONS_MATCH_<n>``,
    ``PEP_MATCH_<n>``) must then come from one of those seeds. Memory is
    O(distinct corpus seeds + screening rows), not O(rows). Raises
    CorpusSeedError on a null seed, an id of neither shape, a screening row no
    recovered seed explains, no typology row, or zero rows. The message counts
    the bad rows and never prints a seed."""
    seeds: set[int] = set()
    screening: set[int] = set()
    n = bad = 0
    for typology_id, iseed in rows:
        n += 1
        if typology_id is None or iseed is None or isinstance(iseed, bool):
            bad += 1
            continue
        try:
            if _SCREEN_ID.fullmatch(str(typology_id)):
                screening.add(int(iseed) & _MASK64)
                continue
            _, tid, j = str(typology_id).rsplit("_", 2)
            inner = _splitmix64((0xF100 + int(tid) * TID_SEED_STRIDE + int(j)) & _MASK64)
            seeds.add(_signed64(unsplitmix64(int(iseed) & _MASK64) ^ inner))
        except (TypeError, ValueError):
            bad += 1
    if n == 0:
        raise CorpusSeedError("the manifest has no rows, so no corpus seed can be recovered")
    if bad:
        raise CorpusSeedError(
            f"{bad} of {n} manifest rows have a null seed or a typology_id that is neither "
            "<name>_<tid>_<j> nor a screening id, so the corpus seed cannot be recovered"
        )
    if not seeds:
        raise CorpusSeedError(
            f"none of the {n} manifest rows is a typology row, so the corpus seed cannot be "
            "recovered"
        )
    if len(seeds) > MAX_CORPUS_SEEDS:
        # One corpus has one seed; a few stale prefixes add a few more. Many
        # means the ids no longer follow the generator's derivation.
        raise CorpusSeedError(
            f"the manifest rows recover {len(seeds)} distinct corpus seeds (more than "
            f"{MAX_CORPUS_SEEDS}): the instance seeds do not follow the generator's derivation"
        )
    unexplained = sum(1 for i in screening if not any(_screen_row_from(i, s) for s in seeds))
    if unexplained:
        raise CorpusSeedError(
            f"{unexplained} of {len(screening)} screening rows come from no seed the typology "
            "rows recover (a manifest from another corpus in the prefix?)"
        )
    return seeds


#: corpus_verdict roles, strictest first: a spent seed is refused for every
#: use, a held-out one only outside its registered run.
VERDICT_ROLES = ("spent", "evaluation", "robustness")


@dataclass(frozen=True)
class CorpusVerdict:
    """What a corpus is, without its seeds: ``refused`` when any recovered
    seed is held out or spent, the strictest such role, how many distinct
    seeds the manifest holds, and whether they are exactly the claimed one
    (None when nothing was claimed)."""

    verdict: Literal["ok", "refused"]
    role: Literal["spent", "evaluation", "robustness"] | None
    seeds_found: int
    matches_claim: bool | None


def corpus_verdict(
    rows: Iterable[tuple[str, int]],
    *,
    claimed: int | None = None,
    heldout: HeldOut | None = None,
    spent: Iterable[int] | None = None,
) -> CorpusVerdict:
    """recover_corpus_seeds over every row, then heldout_role and the spent
    check on each recovered seed. ``spent`` adds seeds the caller knows are
    spent (the pre-registration's spent_seeds and recorded looks); None reads
    them from the packaged pre-registration. ``heldout`` None loads the
    packaged file plus the compiled floor (fail closed if unreadable)."""
    h = heldout if heldout is not None else _heldout()
    extra = frozenset(int(s) for s in spent) if spent is not None else spent_seeds()
    seeds = recover_corpus_seeds(rows)
    found = set()
    for s in seeds:
        r = heldout_role(s, h)
        if r is not None:
            found.add(r)
        if s in h.spent or s in extra:
            found.add("spent")
    role = next((r for r in VERDICT_ROLES if r in found), None)
    matches = None if claimed is None else seeds == {_signed64(int(claimed))}
    return CorpusVerdict(
        verdict="refused" if found else "ok",
        role=role,  # type: ignore[arg-type]
        seeds_found=len(seeds),
        matches_claim=matches,
    )


def heldout_history_problems(old: dict | None, new: dict, looks: dict | None) -> list[str]:
    """Why ``new`` may not follow ``old`` as heldout_hashes.json, or [].

    Allowed between two commits: a hash appended to a role's list, a seed
    appended to ``spent``, and ``absence_check`` moving from report to
    enforce. Everything else is refused. A spent append whose hash (under
    the file's salt or the floor's) is a role entry is allowed only when
    ``looks`` (aml_registered_looks.json in the same commit) records that
    seed for that role as a look with a report_sha256 or as burned with a
    reason (``burn_seed``); otherwise the append would publish a live look
    seed. ``old`` None is the file's creation. Messages name the role and
    the list index, never a value."""
    out = [f"new file: {p}" for p in _heldout_doc_problems(new)]
    if out:
        return out
    if old is not None:
        if _heldout_doc_problems(old):
            return ["the previous file is malformed; the history cannot be checked"]
        for k in sorted((set(old) | set(new)) - {"roles", "spent", "absence_check"}):
            if old.get(k) != new.get(k):
                out.append(f"{k} changed (only appends and report -> enforce are allowed)")
        for r in sorted(set(new["roles"]) - set(old["roles"])):
            out.append(f"new role {r!r}")
        floor_hashes = {fh: fr for fr, fs in _floor()[1].items() for fh in fs}
        for r, now in new["roles"].items():
            for i, h in enumerate(
                now[len(old["roles"].get(r, [])) :], len(old["roles"].get(r, []))
            ):
                if h in floor_hashes and floor_hashes[h] != r:
                    out.append(f"roles.{r}[{i}] append is the floor's {floor_hashes[h]} hash")
        for r, hs in old["roles"].items():
            now = new["roles"].get(r)
            if now is None:
                out.append(f"roles.{r} removed")
                continue
            for i, h in enumerate(hs):
                if i >= len(now):
                    out.append(f"roles.{r}[{i}] removed")
                elif now[i] != h:
                    out.append(f"roles.{r}[{i}] edited")
        for i, s in enumerate(old["spent"]):
            if i >= len(new["spent"]):
                out.append(f"spent[{i}] removed")
            elif new["spent"][i] != s:
                out.append(f"spent[{i}] edited")
        if old["absence_check"] == "enforce" and new["absence_check"] != "enforce":
            out.append("absence_check moved from enforce back to report")
    start = len(old["spent"]) if old is not None else 0
    floor_salt, floor = _floor()
    recorded = {
        (e.get("role"), e.get("seed"))
        for e in ((looks or {}).get("looks") or [])
        if isinstance(e, dict)
        and (
            (isinstance(e.get("report_sha256"), str) and bool(e["report_sha256"]))
            or (
                e.get("state") == "burned"
                and isinstance(e.get("reason"), str)
                and bool(e["reason"].strip())
            )
        )
    }
    for i, s in enumerate(new["spent"][start:], start):
        fh, gh = seed_hash(new["salt"], s), seed_hash(floor_salt, s)
        for r in PROTECTED_ROLES:
            if fh in new["roles"].get(r, ()) or gh in floor.get(r, ()):
                if (r, s) not in recorded:
                    out.append(
                        f"spent[{i}] append matches the held-out {r} seed and no look or burn "
                        f"for {r} is recorded"
                    )
    return out


# Integer tokens: every maximal run of decimal digits (underscores allowed
# between digits) wherever it sits, so a seed bounded by letters or
# punctuation (seed_123_s1, run123, 123ms) is found, plus every 0x hex
# literal. Two more shapes are scanned: every window of 6 to 19 digits inside
# a longer digit run (a seed embedded in a timestamp, a longer number, or
# written as decimal digits after "0x"), and thousands groups separated by
# commas or spaces (12,345,678). The hash compare makes a false hit
# impossible, so no boundary is needed. Not scanned: bare hex without 0x,
# octal, binary, and floats or scientific notation.
_TOKEN = re.compile(r"0[xX][0-9a-fA-F_]+|[0-9](?:[0-9_]*[0-9])?")
_DIGITS = re.compile(r"[0-9]{7,}")
_GROUPED = re.compile(r"(?<![0-9])[0-9]{1,3}(?:[, ][0-9]{3})+(?![0-9])")
_WINDOW_MIN, _WINDOW_MAX = 6, 19


def absence_problems(
    texts: Mapping[str, str],
    heldout: HeldOut | None = None,
    exclude: Iterable[int] | None = None,
) -> list[str]:
    """Every ``texts`` key holding an integer token that hashes to a held-out
    seed. Each decimal or hex token (underscores allowed), each 6-19 digit
    window of a longer digit run and each comma- or space-grouped number
    (the shapes listed at ``_TOKEN``) in 1..2^63 is hashed
    under the file's salt and the floor's and compared with the role hashes;
    the value is never kept or printed. Spent seeds are public and never
    reported: ``exclude`` lists them; None means the recorded looks, the
    pre-registration's spent_seeds and the hash file's spent list."""
    h = heldout if heldout is not None else _heldout()
    if exclude is not None:
        skip = frozenset(int(s) for s in exclude)
    else:
        skip = recorded_seeds() | spent_seeds() | h.spent
    out = []
    for key, text in texts.items():
        hit: set[str] = set()
        values = set()
        for m in _TOKEN.finditer(text):
            raw = m.group(0)
            if raw[:2] in ("0x", "0X"):
                cands = [raw[2:].replace("_", "")]
                base = 16
            else:
                # Every contiguous span of underscore-separated groups: the
                # whole run (1_000_000), each part, and a grouped seed followed
                # by _<cycle> (1_234_567_2).
                parts = raw.split("_")
                if len(parts) > 32:
                    cands = [raw.replace("_", ""), *parts]
                else:
                    cands = [
                        "".join(parts[i:j])
                        for i in range(len(parts))
                        for j in range(i + 1, len(parts) + 1)
                    ]
                base = 10
            for c in cands:
                try:
                    v = int(c, base)
                except ValueError:
                    continue
                if 1 <= v < 1 << 63:
                    values.add(v)
        for m in _DIGITS.finditer(text):
            run = m.group(0)
            for w in range(_WINDOW_MIN, min(_WINDOW_MAX, len(run)) + 1):
                for i in range(len(run) - w + 1):
                    v = int(run[i : i + w])
                    if 1 <= v < 1 << 63:
                        values.add(v)
        for m in _GROUPED.finditer(text):
            v = int(m.group(0).replace(",", "").replace(" ", ""))
            if 1 <= v < 1 << 63:
                values.add(v)
        for v in values - skip:
            r = heldout_role(v, h)
            if r is not None:
                hit.add(r)
        for r in PROTECTED_ROLES:
            if r in hit:
                out.append(f"{key}: an integer token hashes to a held-out {r} seed")
    return out


def _match_label(seed: int | None, role: str | None) -> str:
    """How a refusal names a seed: never by value, a held-out seed by its
    role, a spent one as spent. ``seed`` is kept for the signature only."""
    if role in PROTECTED_ROLES:
        return f"the registered {role} seed"
    return "a spent seed" if role == "spent" else "another seed"


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
        if eff is None:
            label = "the corpus seed"
        elif held_role is not None:
            label = _match_label(eff, held_role)
        else:
            label = "the configured seed"
        return (
            f"{label} is listed as spent "
            "(the AML pre-registration's corpora.spent_seeds, a recorded look, or "
            "heldout_hashes.json): a corpus from it has already been looked at or voided and "
            "must not be regenerated or scored. Use the calibration seed "
            "(corpora.calibration_seed) or another unregistered seed."
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
                    "corpus_role 'calibration' does not match the configured seed (the "
                    "role names the pre-registration's corpora.calibration_seed)"
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
            f"this seed (given, or recovered from the corpus) is the registered {eff_role} "
            "seed: its corpus is generated only with `lakebench generate "
            "--registered-corpus` and scored only by `scripts/aml_gate.py --registered "
            f"{eff_role}`, once, after the datagen freeze. Any other use burns the seed."
        )
    return None


class ProtectedCorpusError(ValueError):
    """The AML protocol refuses this seed or role: spent, held out outside its
    registered run, a role that does not match the seed, or a registered run
    while looks are closed. The message never names a seed. ``load_config``
    turns it into ``ConfigProtectedCorpusError`` (exit 2,
    ``run.protected_corpus``)."""


def check_aml_seed(seed: int | None, corpus_role: str | None = None) -> None:
    """Raise ProtectedCorpusError unless ``seed`` may be used with
    ``corpus_role``. A record the guard cannot read (the pre-registration,
    the look record, the hash file) raises a plain ValueError: the load still
    fails closed, as a config error rather than an unhandled exception."""
    try:
        err = aml_seed_error(_corpora(), seed, corpus_role)
    except OSError as e:
        raise ValueError(
            f"the AML seed guard cannot read its records ({type(e).__name__}: "
            f"{getattr(e, 'filename', None) or e}); refusing"
        ) from None
    if err:
        raise ProtectedCorpusError(err)


def check_seed(seed: int, schema: str, corpus_role: str | None = None) -> None:
    """``check_aml_seed`` for the financial schema; other schemas are free."""
    if schema == "financial":
        check_aml_seed(seed, corpus_role)
    elif corpus_role is not None:
        raise ValueError("corpus_role applies to the financial schema only")


def resolve_seed(seed: int | None, schema: str, corpus_role: str | None = None) -> int:
    """The seed a deployment generates with (see the module docstring)."""
    if seed is None:
        if corpus_role in PROTECTED_ROLES and schema == "financial":
            raise ProtectedCorpusError(
                "a registered look names its seed in datagen.seed; it is checked against "
                "heldout_hashes.json"
            )
        seed = calibration_seed() if schema == "financial" else NON_AML_DEFAULT_SEED
    check_seed(seed, schema, corpus_role)
    return seed


def config_seed(cfg) -> int:
    """``resolve_seed`` for a LakebenchConfig."""
    workload = cfg.architecture.workload
    dg = workload.datagen
    return resolve_seed(dg.seed, workload.schema_type.value, getattr(dg, "corpus_role", None))


# ---------------------------------------------------------------------------
# Protected corpora: the manifest check, redaction, and the local ledgers
# ---------------------------------------------------------------------------


def _verdict_label(role: str | None) -> str:
    if role in PROTECTED_ROLES:
        return f"the registered {role} seed"
    return "a spent seed" if role == "spent" else "a guarded seed"


def prereg_spent_seeds() -> frozenset[int]:
    """The pre-registration's ``corpora.spent_seeds`` plus every recorded
    look, read from the packaged pre-registration or, on the Spark driver,
    the flat copy next to this module. Raises when neither can be read."""
    here = Path(__file__).resolve().parent
    for p in (_PREREG_PATH, here / _PREREG_PATH.name):
        if p.is_file():
            with open(p, encoding="utf-8") as f:
                return spent_from(with_recorded_looks(json.load(f)["corpora"]))
    raise FileNotFoundError(f"{_PREREG_PATH.name} not found next to {__file__} or {_PREREG_PATH}")


def manifest_protected_reason(
    rows: Iterable[tuple[str, int]],
    *,
    heldout: HeldOut | None = None,
    spent: Iterable[int] | None = None,
) -> str | None:
    """Why a corpus whose manifest holds ``rows`` (typology_id, instance
    seed; every row, never a sample) may not be scored outside a registered
    look, or None. ``corpus_verdict`` over every row: a recovered seed that
    is held out or spent refuses. A manifest the corpus seed cannot be
    recovered from, and a held-out record that cannot be read, refuse too
    (fail closed). The reason names a role, never a seed."""
    try:
        known = spent if spent is not None else prereg_spent_seeds()
        verdict = corpus_verdict(rows, heldout=heldout, spent=known)
    except CorpusSeedError as e:
        return f"the corpus seed cannot be recovered from the manifest ({e})"
    except (OSError, ValueError) as e:  # their messages name files and roles, never a seed
        return (
            f"the held-out record cannot be read ({type(e).__name__}: {e}), so the corpus "
            "is unchecked"
        )
    except Exception as e:  # noqa: BLE001 -- unreadable held-out record: refuse
        return (
            f"the held-out record cannot be read ({type(e).__name__}), so the corpus is unchecked"
        )
    if verdict.verdict == "refused":
        return f"the corpus manifest comes from {_verdict_label(verdict.role)}"
    return None


def looks_ledger_path() -> Path:
    """The out-of-tree, append-only record of look claims, so a reverted or
    stashed aml_registered_looks.json cannot un-spend a seed on this host
    (``LB_AML_LOOKS_LEDGER`` overrides ``~/.lakebench/aml_looks.jsonl``)."""
    return Path(
        os.environ.get("LB_AML_LOOKS_LEDGER") or Path.home() / ".lakebench" / "aml_looks.jsonl"
    )


def _append_jsonl(path: Path, entry: dict) -> None:
    """Append one JSON line to ``path`` under an exclusive lock, fsync the
    file (and the directory when the file is new), and read the line back.
    Raises on any failure: the caller must not go on as if it was written."""
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(entry, sort_keys=True)
    new = not path.exists()
    with open(path, "a", encoding="utf-8") as f:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())
    if new:
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    with open(path, encoding="utf-8") as f:
        if line not in (ln.rstrip("\n") for ln in f):
            raise OSError(f"{path}: the appended entry did not read back")


def append_looks_ledger(entry: dict) -> None:
    """Append a look claim to ``looks_ledger_path()`` (raises on failure)."""
    _append_jsonl(looks_ledger_path(), entry)


def seed_ever_recorded(seed: int) -> str | None:
    """Why ``seed`` already has a look anywhere this host can see, or None:
    the out-of-tree look ledger, or any commit on any branch (stashes and
    unpushed branches included) that added it to the tracked look record.
    A ledger line that cannot be read raises ValueError and a git history
    that cannot be searched raises OSError: callers refuse then. Held-out
    seeds are named by role, others as "the seed"; never by value."""
    led = looks_ledger_path()
    if led.is_file():
        for i, line in enumerate(led.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                got = int(json.loads(line)["seed"])
            except (ValueError, KeyError, TypeError):
                raise ValueError(f"{led} line {i} is not a look entry") from None
            if got == int(seed):
                role = heldout_role(seed)
                label = f"the registered {role} seed" if role else "the seed"
                return f"{label} is in the look ledger {led}"
    rec = looks_path()
    commit = _look_commit(rec, int(seed))
    if commit:
        role = heldout_role(seed)
        label = f"the registered {role} seed" if role else "the seed"
        return f"{label} was recorded in {rec.name} by commit {commit}"
    return None


def _look_commit(rec: Path, seed: int) -> str | None:
    """The first commit whose copy of the look record ``rec`` lists
    ``seed``, or None: every branch and tag, every reflog entry (older
    stashes, amended and rebased commits) and merge commits against each
    parent. Each copy is parsed; one that is not JSON (a conflict left in it)
    is searched as text. The seed never appears in a command line (``git log
    -S`` would put it in argv, readable by every process on the host).
    Raises OSError when git cannot list or read the history, or the
    repository is shallow (its history would be cut short)."""
    import subprocess

    def git(*args: str) -> str:
        out = subprocess.run(
            ["git", "-C", str(rec.parent), *args], capture_output=True, text=True, check=False
        )
        if out.returncode != 0:
            raise OSError(f"git {args[0]} over {rec.name} failed (exit {out.returncode})")
        return out.stdout

    if git("rev-parse", "--is-shallow-repository").strip() != "false":
        raise OSError(f"the repository holding {rec.name} is shallow; its look history is cut")
    shas = git(
        "log", "--all", "--reflog", "-m", "--format=%H", "--diff-filter=ACMR", "--", rec.name
    ).split()
    as_text = re.compile(r'"seed"\s*:\s*"?' + str(int(seed)) + r"(?![0-9])")
    for sha in dict.fromkeys(shas):
        raw = git("show", f"{sha}:./{rec.name}")
        try:
            doc = json.loads(raw)
        except ValueError:
            if as_text.search(raw):
                return sha
            continue
        looks = doc.get("looks") if isinstance(doc, dict) else None
        for e in looks if isinstance(looks, list) else []:
            v = e.get("seed") if isinstance(e, dict) else None
            if isinstance(v, bool):
                continue
            if (isinstance(v, int) and v == seed) or (
                isinstance(v, str) and v.strip().isdigit() and int(v) == seed
            ):
                return sha
    return None


#: The local ledger of registered-corpus generations (``generate
#: --registered-corpus``). Separate from the look record, because any entry
#: there spends the seed, and generation is not a look.
CORPORA_LEDGER_ENV = "LB_AML_CORPORA_LEDGER"


def corpora_ledger_path() -> Path:
    """``LB_AML_CORPORA_LEDGER`` or ``~/.lakebench/aml_corpora.jsonl``."""
    return Path(
        os.environ.get(CORPORA_LEDGER_ENV) or Path.home() / ".lakebench" / "aml_corpora.jsonl"
    )


def append_corpus_ledger(entry: dict) -> None:
    """Append a registered-corpus entry to ``corpora_ledger_path()``,
    fsynced and read back (raises on failure). The entry names the seed by
    its salted hash only; a plaintext integer under any key is refused."""
    for k, v in entry.items():
        if isinstance(v, int) and not isinstance(v, bool):
            raise ValueError(f"corpus ledger entry key {k!r} is an integer; seeds go in as hashes")
    _append_jsonl(corpora_ledger_path(), entry)


def corpus_file(relpath: str) -> bool:
    """Whether a file under the datagen prefix is corpus data: every file
    Spark's Parquet reader would read, whatever its extension (a ``.parquet``
    left half-renamed by a sync is read too), so every path with no ``_`` or
    ``.`` segment (markers, checksum and temporary files are left out)."""
    parts = [p for p in relpath.split("/") if p]
    return bool(parts) and not any(p.startswith(("_", ".")) for p in parts)


def corpus_fingerprint(
    files: Iterable[tuple[str, int]], manifest_sha256: Mapping[str, str]
) -> dict:
    """What a corpus is: every data file's path (relative to the datagen
    prefix) and size, and the sha256 of each manifest file. Data files are
    matched by path and size, not hashed (hashing a gate-scale corpus would
    mean reading all of it back from S3). ``generate --registered-corpus`` records it from S3 when
    the generation finishes; ``registered_corpus_problem`` recomputes it over
    the corpus a registered look scores."""
    items = sorted((str(p), int(n)) for p, n in files if corpus_file(str(p)))
    listing = json.dumps(items, separators=(",", ":")).encode("utf-8")
    return {
        "format": 1,
        "files": len(items),
        "bytes": sum(n for _, n in items),
        "listing_sha256": hashlib.sha256(listing).hexdigest(),
        "manifest_sha256": {k: manifest_sha256[k] for k in sorted(manifest_sha256)},
    }


def local_corpus_fingerprint(root: str | os.PathLike) -> dict:
    """``corpus_fingerprint`` of a corpus directory (the local copy of the
    datagen prefix: ``bronze/...`` and ``manifest/manifest*.parquet``)."""
    base = Path(root)
    files, manifests = [], {}
    for path in sorted(base.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(base).as_posix()
        if not corpus_file(rel):
            continue
        files.append((rel, path.stat().st_size))
        if rel.startswith("manifest/"):
            manifests[rel] = hashlib.sha256(path.read_bytes()).hexdigest()
    return corpus_fingerprint(files, manifests)


def _image_digest(image: str | None) -> str | None:
    """The ``sha256:...`` part of an image reference or a kubelet imageID."""
    text = str(image or "")
    return text.rsplit("@", 1)[-1] if "@sha256:" in text else None


def registered_corpus_problem(
    role: str, seed: int, generator_image: str | None, corpus_dir: str | os.PathLike
) -> str | None:
    """Why a registered ``role`` look may not score the corpus at
    ``corpus_dir`` (owner, 10-03), or None. The corpus ledger
    (``corpora_ledger_path()``) must hold a ``generated`` entry of
    ``generate --registered-corpus`` for this role and seed (by its salted
    hash) whose corpus fingerprint equals the directory's, so the look
    scores exactly the bytes that generation wrote; every datagen pod of it
    must have run one image, the ``--generator-image`` digest it was pinned
    to; and no other attempt on the same bronze prefix may have had a
    datagen Job running while it ran. An unreadable ledger refuses. The
    ledger is per host: attempts made on another host are not seen. Names
    the role, never the seed."""
    path = corpora_ledger_path()
    if not path.is_file():
        return f"no generation of the registered {role} corpus is recorded ({path} is absent)"
    want = seed_hash(_heldout().salt, seed)
    entries: list[dict] = []
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            e = json.loads(line)
        except ValueError:
            raise ValueError(f"{path} line {n} is not JSON") from None
        if not isinstance(e, dict):
            raise ValueError(f"{path} line {n} is not an entry")
        entries.append(e)
    mine = [
        (i, e)
        for i, e in enumerate(entries)
        if e.get("kind") == "registered_corpus"
        and e.get("seed_hash") == want
        and e.get("role") == role
    ]
    if not mine:
        return (
            f"no generation of the registered {role} corpus is recorded in {path} (run "
            "`lakebench generate --registered-corpus` first)"
        )
    local = local_corpus_fingerprint(corpus_dir)
    done = [(i, e) for i, e in mine if e.get("state") == "generated"]
    if done and all(e.get("corpus_fingerprint") is None for _, e in done):
        return (
            f"the recorded generation of the registered {role} corpus has no corpus fingerprint "
            f"({done[-1][1].get('fingerprint_error')}); regenerate it"
        )
    match = [(i, e) for i, e in done if e.get("corpus_fingerprint") == local]
    if not match:
        return (
            f"the corpus at {corpus_dir} is not the one a recorded generation of the registered "
            f"{role} corpus wrote ({len(done)} generated entr{'y' if len(done) == 1 else 'ies'} "
            "in the ledger; its files or manifest differ)"
        )
    end, gen = match[-1]
    attempt = gen.get("attempt")
    # The look names the image the generation was pinned to, and every pod
    # ran one image (the kubelet may report a per-platform digest for a
    # multi-arch pin, so the pods are compared with each other).
    want_digest = _image_digest(generator_image)
    ids = gen.get("image_ids")
    pod_digests = {_image_digest(x) for x in ids} if isinstance(ids, list) else set()
    if (
        want_digest is None
        or want_digest != _image_digest(gen.get("image"))
        or len(pod_digests) != 1
        or None in pod_digests
    ):
        return (
            f"the recorded generation (attempt {attempt}) was not pinned to the --generator-image "
            "digest, or its datagen pods did not all run one image digest"
        )
    starts = [i for i, e in mine if e.get("attempt") == attempt and e.get("state") == "attempted"]
    if not starts:
        return f"the recorded generation (attempt {attempt}) has no attempted entry"
    start = starts[0]
    # Another attempt on this bronze prefix that submitted a datagen Job
    # before this generation ended and had not finished before it began may
    # have written into this corpus.
    for j, e in enumerate(entries[:end]):
        other = e.get("attempt")
        if (
            e.get("kind") != "registered_corpus"
            or other == attempt
            or e.get("state") != "submitting"
            or e.get("bronze_uri") != gen.get("bronze_uri")
        ):
            continue
        # Finished means generated. A failure after submit leaves its Job and
        # pods behind (OOM, crash loop, timeout), so it may still be writing.
        closed = next(
            (
                k
                for k in range(j + 1, len(entries))
                if entries[k].get("attempt") == other and entries[k].get("state") == "generated"
            ),
            None,
        )
        if closed is None or closed > start:
            return (
                f"another attempt ({other}) had a datagen Job in the same bronze prefix that may "
                f"have been writing while attempt {attempt} ran; generate the registered corpus "
                "again into an empty bucket"
            )
    return None


# ---------------------------------------------------------------------------
# Level-2 predictions
# ---------------------------------------------------------------------------

#: Tracked record of the per-typology predicted evaluation AP and prediction
#: interval, committed from the calibration runs before any registered look
#: (scripts/aml_level2_predict.py). A registered look refuses to start
#: without it and hashes it into its report and look record.
PREDICTIONS_FILENAME = "aml_level2_predictions.json"


def predictions_path() -> Path:
    here = Path(__file__).resolve().parent
    for p in (_PREREG_PATH.parent / PREDICTIONS_FILENAME, here / PREDICTIONS_FILENAME):
        if p.is_file():
            return p
    raise FileNotFoundError(
        f"{PREDICTIONS_FILENAME} not found next to {__file__} or {_PREREG_PATH}"
    )


def load_predictions(path: str | os.PathLike | None = None, typologies=None) -> tuple[dict, str]:
    """(predictions block, sha256 of the file bytes). Raises unless the file
    holds committed predictions: a prereg sha256, a generator image and, per
    typology (every one of ``typologies`` when given), a predicted AP in (0, 1)
    inside its prediction interval."""
    import hashlib

    p = Path(path) if path is not None else predictions_path()
    raw = p.read_bytes()
    doc = json.loads(raw)
    pred = doc.get("predictions") if isinstance(doc, dict) else None
    if not isinstance(pred, dict):
        raise ValueError(f"{p}: no committed predictions (run scripts/aml_level2_predict.py)")
    for key in ("prereg_sha256", "generator_image"):
        if not isinstance(pred.get(key), str) or not pred[key]:
            raise ValueError(f"{p}: predictions.{key} missing")
    per = pred.get("typologies")
    if not isinstance(per, dict) or not per:
        raise ValueError(f"{p}: predictions.typologies missing")
    for t in typologies or []:
        if t not in per:
            raise ValueError(f"{p}: no prediction for typology {t!r}")
    for t, r in per.items():
        try:
            ap, (lo, hi) = float(r["predicted_ap"]), r["pi"]
            ok = 0 < float(lo) <= ap <= float(hi) < 1
        except (KeyError, TypeError, ValueError):
            ok = False
        if not ok:
            raise ValueError(f"{p}: malformed prediction for {t!r}: {r!r}")
    return pred, hashlib.sha256(raw).hexdigest()


def replication(typology_reports: dict, pred: dict) -> dict:
    """Observed AP against each committed prediction interval (reported
    beside the verdict; the Level-2 framing is "calibrated difficulty
    replicates out of sample")."""
    out = {}
    for t, r in pred["typologies"].items():
        obs = (typology_reports.get(t) or {}).get("ap")
        lo, hi = r["pi"]
        out[t] = {
            "predicted_ap": r["predicted_ap"],
            "pi": [lo, hi],
            "observed_ap": obs,
            "inside_pi": None if obs is None else bool(lo <= obs <= hi),
        }
    return {"gated": False, "pi_level": pred.get("pi_level"), "typologies": out}
