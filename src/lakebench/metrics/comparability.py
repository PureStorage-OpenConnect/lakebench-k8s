"""Identity groups: which recorded keys say what a run was.

Every metrics.json key that decides whether two runs compare is classified
here into one group:

* **Workload**: what work was done (workload, its version, the generator
  model version, the workload parameters, the mode; exp2 adds the query set
  id);
* **Corpus**: what data it was done on (corpus id, seed, corpus role, scale,
  cycles above 1, and on exp1 the generator image; the generator digest is
  compared only when both sides have one);
* **Architecture**: the composition and the access paths between its
  components (recipe, the four components with their versions, the query
  access path, the dependency pinset, user-set executor overrides and Spark
  conf);
* **System**: the cluster and object store it ran on (the ``cluster`` or
  ``local`` string, and the system fingerprint when one was observed);
* **Conditions**: how it was executed (effective maintenance and the
  compaction operation, maintenance settings, benchmark iterations and mode,
  the Lakebench limits that bound, in-stream rounds);
* **Observational**: recorded, never compared (allocatable capacity,
  co-tenant load, timestamps, names).

Stored records are classified at read time from the block as stored: the v1
identity dict and its digest never change (``experiment._identity_v1`` is
frozen), and the keys a v1.6 record cannot carry are derived from what it
does carry (``classify``). Keys that exist only when a user set something
come from one table, ``OPTIONAL_IDENTITY_KEYS``, and are emitted only when
their value is not the default, so a default config's identity has none of
them, and default runs keep their identity.

The ladder that turns two classified sides into one verdict is
``pair_verdict``.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

WORKLOAD = "workload"
CORPUS = "corpus"
ARCHITECTURE = "architecture"
SYSTEM = "system"
CONDITIONS = "conditions"
OBSERVATIONAL = "observational"

GROUPS = (WORKLOAD, CORPUS, ARCHITECTURE, SYSTEM, CONDITIONS, OBSERVATIONAL)

#: Record generations. A legacy record has no experiment block.
LEGACY, EXP1, EXP2 = "legacy", "exp1", "exp2"

#: The value a None corpus role reads as, so None never meets None as an
#: accidental match. Every exp1 record stores None; exp2 always writes
#: a role string.
UNDECLARED_ROLE = "undeclared"

#: The pinset value of a v1.7 record that did not record one: unequal to
#: every pinset and to absence.
PINSET_NOT_RECORDED = "not_recorded"

#: Keys compared only when both sides carry a value (a digest is evidence;
#: an unresolved digest on one side is not a difference).
BOTH_SIDES_ONLY = frozenset({"generator digest"})

#: The Conditions group, in display order. ``benchmark rounds`` is present
#: on continuous records only; ``investigator sessions`` is an optional key
#: (``OPTIONAL_IDENTITY_KEYS``) and not listed here, so default identities
#: do not gain it.
CONDITION_KEYS = (
    "effective maintenance",
    "compaction operation",
    "maintenance settings",
    "benchmark iterations",
    "benchmark mode",
    "Lakebench limits that bound",
    "benchmark rounds",
)

#: Conditions that are also outcomes of the run: compare reports a
#: difference (not like-for-like); the perf gate and reproduce do not refuse
#: on it.
OUTCOME_CONDITION_KEYS = frozenset({"benchmark rounds", "investigator sessions"})


# ---------------------------------------------------------------------------
# Optional identity keys: present only when non-default
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OptionalKey:
    """One ``OPTIONAL_IDENTITY_KEYS`` row. *extractor* reads the value from
    an experiment block and, when given, its whole metrics.json record;
    *owner* names the feature that writes the key."""

    group: str
    default: Any
    extractor: Callable[[Mapping[str, Any], Mapping[str, Any] | None], Any]
    owner: str


def _cycles(exp: Mapping[str, Any], record: Mapping[str, Any] | None) -> int:
    """Corpus cycles: the per-cycle list of the record (``cycles``, appended
    by ``cli/_run.py`` per cycle) when it has more than one entry, else the
    block's own ``corpus.cycles`` when an integer, else 1."""
    listed = (record or {}).get("cycles")
    if isinstance(listed, list) and len(listed) > 1:
        return len(listed)
    stored = (exp.get("corpus") or {}).get("cycles")
    if isinstance(stored, int) and not isinstance(stored, bool) and stored > 1:
        return stored
    return 1


def _executor_overrides(exp: Mapping[str, Any], record: Mapping[str, Any] | None) -> dict:
    """User-set executor counts: the block's ``architecture.
    spark_executor_overrides`` (when user-set overrides are recorded), else the non-null entries of the
    record's ``config_snapshot.spark.executor_overrides``, else the
    ``override`` of each ``limits.executors`` entry."""
    stored = (exp.get("architecture") or {}).get("spark_executor_overrides")
    if isinstance(stored, Mapping):
        return {str(k): v for k, v in sorted(stored.items()) if v is not None}
    snap = ((record or {}).get("config_snapshot") or {}).get("spark") or {}
    overrides = snap.get("executor_overrides")
    if isinstance(overrides, Mapping):
        return {str(k): v for k, v in sorted(overrides.items()) if v is not None}
    out = {}
    for entry in (exp.get("limits") or {}).get("executors") or []:
        if isinstance(entry, Mapping) and entry.get("override") is not None:
            out[str(entry.get("job_type"))] = entry["override"]
    return dict(sorted(out.items()))


def _spark_conf(exp: Mapping[str, Any], record: Mapping[str, Any] | None) -> dict:
    """User Spark conf keys and hash, as the block records them."""
    stored = (exp.get("architecture") or {}).get("spark_conf_user")
    return dict(stored) if isinstance(stored, Mapping) else {}


def _investigator_sessions(exp: Mapping[str, Any], record: Mapping[str, Any] | None) -> Any:
    """The investigator sessions that ran, written only when
    ``architecture.benchmark.investigator_sessions`` is set."""
    inv = exp.get("investigators")
    return inv.get("run") if isinstance(inv, Mapping) else None


#: Only-when-non-default identity keys. Other chapters add rows here and
#: never edit ``experiment._identity_v1``. The v1.8 rows (``evaluation
#: profile`` and ``ml loop``) are not built in v1.7.
OPTIONAL_IDENTITY_KEYS: dict[str, OptionalKey] = {
    "cycles": OptionalKey(CORPUS, 1, _cycles, "multi-cycle generate"),
    "spark executor overrides": OptionalKey(
        ARCHITECTURE, {}, _executor_overrides, "user executor overrides"
    ),
    "spark conf": OptionalKey(ARCHITECTURE, {}, _spark_conf, "user Spark conf"),
    "investigator sessions": OptionalKey(
        CONDITIONS, None, _investigator_sessions, "AML investigators under load"
    ),
}


def optional_keys(exp: Mapping[str, Any], record: Mapping[str, Any] | None = None) -> dict:
    """``{key: value}`` of the optional identity keys whose value is not the
    default for *exp*. A value that cannot be read is ``"unreadable: ..."``,
    which equals nothing a run sets, never the default (reading it as the
    default would hide a difference)."""
    out: dict[str, Any] = {}
    for name, row in OPTIONAL_IDENTITY_KEYS.items():
        try:
            value = row.extractor(exp, record)
        except Exception as exc:  # noqa: BLE001 -- recorded, never the default
            value = f"unreadable: {type(exc).__name__}"
        if value != row.default:
            out[name] = value
    return out


# ---------------------------------------------------------------------------
# Read-time derivations
# ---------------------------------------------------------------------------

_VERSION = re.compile(r"^\s*v?(\d+)\.(\d+)")


def lakebench_minor(exp: Mapping[str, Any]) -> tuple[tuple[int, int] | None, str | None]:
    """The ``major.minor`` of the Lakebench that wrote *exp*
    (``lakebench.lakebench_version``, leading integers: ``1.7.0.dev3`` is
    1.7), and a note when the string is present but unreadable."""
    raw = (exp.get("lakebench") or {}).get("lakebench_version")
    if raw is None or raw == "":
        return None, None
    m = _VERSION.match(str(raw))
    if not m:
        return None, "lakebench version unreadable"
    return (int(m.group(1)), int(m.group(2))), None


def written_by_v17(
    exp: Mapping[str, Any], record: Mapping[str, Any] | None = None
) -> tuple[bool, list[str]]:
    """Whether *exp* was written by Lakebench 1.7 or later, and notes.

    Never decided by the schema string alone: an exp1 block can be written
    by 1.6 or by 1.7. A block is 1.7 when its ``lakebench_version`` parses
    as 1.7 or later, or when it carries what only 1.7 writes: identity v2,
    ``v2_unavailable``, or a run-start ``identity_version`` 2 in the
    record's snapshot. The second rule covers 1.7 development builds, whose
    package version still reads 1.6."""
    minor, note = lakebench_minor(exp)
    notes = [note] if note else []
    if minor is not None and minor >= (1, 7):
        return True, notes
    inputs = ((record or {}).get("config_snapshot") or {}).get("experiment_inputs") or {}
    v17 = (
        block_generation(exp) == EXP2
        or "v2_unavailable" in exp
        or (isinstance(inputs, Mapping) and inputs.get("identity_version") == 2)
    )
    return v17, notes


def dependency_pinset(
    exp: Mapping[str, Any], record: Mapping[str, Any] | None = None
) -> tuple[bool, Any, list[str]]:
    """``(present, value, notes)`` of the ``dependency pinset`` key.

    A record written by 1.7 or later (``written_by_v17``) carries the key,
    valued ``provenance.deps.pinset_sha256`` (the block's ``lakebench``
    copy of provenance first, which is what the identity hashes, else the
    record's), or ``"not_recorded"`` when that is absent. Older records do
    not carry it."""
    v17, notes = written_by_v17(exp, record)
    if not v17:
        return False, None, notes
    for prov in (exp.get("lakebench"), (record or {}).get("provenance")):
        deps = (prov or {}).get("deps") if isinstance(prov, Mapping) else None
        value = (deps or {}).get("pinset_sha256") if isinstance(deps, Mapping) else None
        if value and value != PINSET_NOT_RECORDED:
            return True, str(value), notes
    return True, PINSET_NOT_RECORDED, notes


def _compaction_ran(effective: Mapping[str, Any]) -> bool:
    ident = str(effective.get("id") or "")
    tail = ident.split(":", 1)[1] if ":" in ident else ident
    return any(
        tok == "compaction=ran" or tok.startswith("compaction=ran(") for tok in tail.split(",")
    )


def compaction_operation(exp: Mapping[str, Any]) -> str | None:
    """The compaction operation a run's maintenance executed, or
    None when no compaction ran.

    Read from ``effective_maintenance.detail.operations.compaction`` when
    the block records it; otherwise derived from the query engine
    and table format when the stored effective-maintenance id says
    ``compaction=ran``. The engine that ran the maintenance is the query
    engine (``cli/_sustained.py`` and ``deploy/iceberg.py`` build the
    statements per engine): Trino runs ``optimize`` with a 128MB file size
    threshold, Spark Thrift runs Iceberg ``rewrite_data_files`` with its
    defaults. The maintenance policy marks Delta compaction not supported,
    so the Delta labels apply only to a record that says it ran. The label
    is an inference from the composition for records that do not name the
    operation."""
    em = exp.get("effective_maintenance") or {}
    if not isinstance(em, Mapping):
        return None
    detail = ((em.get("detail") or {}).get("operations") or {}).get("compaction")
    if isinstance(detail, Mapping) and detail.get("operation"):
        params = detail.get("params") or {}
        if isinstance(params, Mapping) and params:
            return f"{detail['operation']}:" + ",".join(f"{v}" for _, v in sorted(params.items()))
        return str(detail["operation"])
    if not _compaction_ran(em):
        return None
    arch = exp.get("architecture") or {}
    engine = str(((arch.get("query_engine") or {}).get("type")) or "")
    fmt = str(((arch.get("table_format") or {}).get("type")) or "")
    if fmt == "iceberg" and engine == "trino":
        return "trino_optimize:128MB"
    if fmt == "iceberg" and engine == "spark-thrift":
        return "iceberg_rewrite_data_files"
    if fmt == "delta" and engine == "trino":
        return "trino_optimize"
    if fmt == "delta" and engine == "spark-thrift":
        return "delta_optimize"
    return f"unknown ({engine or 'no engine'}, {fmt or 'no format'})"


def access_paths(exp: Mapping[str, Any]) -> dict[str, Any]:
    """How each engine reaches the tables: ``architecture.access_paths``
    when stored (exp2), else built from the v1.6 ``query_access_path``. The
    pipeline engine (Spark) always writes through the catalog."""
    arch = exp.get("architecture") or {}
    stored = arch.get("access_paths")
    if isinstance(stored, Mapping):
        return dict(stored)
    return {"query": arch.get("query_access_path")}


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def generation(record: Mapping[str, Any] | None) -> str:
    """``legacy``, ``exp1`` or ``exp2`` of a metrics.json
    dict. An unknown schema string reads as exp1 only when it is ``exp1``;
    anything else that is not ``exp2`` is legacy (never comparable)."""
    exp = (record or {}).get("experiment")
    if not isinstance(exp, Mapping) or not exp.get("schema"):
        return LEGACY
    schema = exp.get("schema")
    if schema == EXP2 and exp.get("identity_version") == 2:
        return EXP2
    if schema == EXP1:
        return EXP1
    return LEGACY


def block_generation(exp: Mapping[str, Any]) -> str:
    """The generation of an experiment block alone (a planned block with no
    schema reads as exp1)."""
    if exp.get("schema") == EXP2 and exp.get("identity_version") == 2:
        return EXP2
    return EXP1


@dataclass
class Classified:
    """One run's keys by group, as a reader compares them."""

    run_id: str | None
    generation: str
    groups: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: ``experiment.system_identity`` when one was observed.
    system_identity: Mapping[str, Any] | None = None
    notes: list[str] = field(default_factory=list)

    def keys(self, group: str) -> dict[str, Any]:
        return self.groups.get(group, {})


def _role(value: Any) -> Any:
    return UNDECLARED_ROLE if value is None else value


def classify(exp: Mapping[str, Any], record: Mapping[str, Any] | None = None) -> Classified:
    """*exp* (an experiment block, as stored) classified into the identity
    groups. *record* is its whole metrics.json dict when available (some
    exp1 derivations read the per-cycle list, the config snapshot and
    provenance); a planned block (``planned_experiment``) has none."""
    gen = block_generation(exp)
    w = exp.get("workload") or {}
    c = exp.get("corpus") or {}
    dg = c.get("datagen") or {}
    arch = exp.get("architecture") or {}
    limits = exp.get("limits") or {}
    out = Classified(run_id=(record or {}).get("run_id"), generation=gen)
    optional = optional_keys(exp, record)

    workload = {
        "workload": w.get("name"),
        "workload version": w.get("version"),
        "generator model version": w.get("generator_model_version"),
        "workload parameters": w.get("parameters_id"),
        "mode": exp.get("mode"),
    }
    if gen == EXP2:
        workload["query set id"] = (exp.get("results") or {}).get("query_set_id")

    corpus: dict[str, Any] = {}
    if gen == EXP2:
        corpus["corpus id v2"] = c.get("id_v2")
    else:
        corpus["corpus id"] = c.get("id")
        corpus["generator image"] = c.get("generator_image")
    corpus["seed"] = c.get("seed")
    corpus["corpus role"] = _role(c.get("corpus_role"))
    corpus["scale"] = c.get("scale")
    corpus["generator digest"] = dg.get("digest")

    architecture: dict[str, Any] = {
        "recipe": arch.get("recipe"),
        "catalog": arch.get("catalog"),
        "table format": arch.get("table_format"),
        "pipeline engine": arch.get("pipeline_engine"),
        "query engine": arch.get("query_engine"),
    }
    if gen == EXP2:
        architecture["access paths"] = access_paths(exp)
        observed = (exp.get("lakebench") or {}).get("images_observed")
        if observed:
            architecture["observed image digests"] = observed
    else:
        architecture["query access path"] = arch.get("query_access_path")
    has_pinset, pinset, notes = dependency_pinset(exp, record)
    out.notes += notes
    if has_pinset:
        architecture["dependency pinset"] = pinset

    sysid = exp.get("system_identity")
    out.system_identity = sysid if isinstance(sysid, Mapping) and sysid.get("parts") else None
    system = {
        "system": exp.get("system"),
        "system fingerprint": (out.system_identity or {}).get("fingerprint"),
    }

    values = {
        "effective maintenance": (exp.get("effective_maintenance") or {}).get("id"),
        "compaction operation": compaction_operation(exp),
        "maintenance settings": exp.get("maintenance_settings"),
        "benchmark iterations": limits.get("benchmark_iterations"),
        "benchmark mode": limits.get("benchmark_mode"),
        "Lakebench limits that bound": list(limits.get("bound_kinds") or []),
        "benchmark rounds": limits.get("benchmark_rounds"),
    }
    conditions = {
        k: values[k]
        for k in CONDITION_KEYS
        if k != "benchmark rounds" or exp.get("mode") == "sustained"
    }

    groups = {
        WORKLOAD: workload,
        CORPUS: corpus,
        ARCHITECTURE: architecture,
        SYSTEM: system,
        CONDITIONS: conditions,
    }
    for name, value in optional.items():
        groups[OPTIONAL_IDENTITY_KEYS[name].group][name] = value
    groups[OBSERVATIONAL] = {
        "observed": exp.get("observed"),
        "deployment": (record or {}).get("deployment_name"),
        "start time": (record or {}).get("start_time"),
    }
    out.groups = groups
    return out


# ---------------------------------------------------------------------------
# Required keys and group differences
# ---------------------------------------------------------------------------

#: Workload and Corpus keys that are never legitimately None, per
#: generation; ``financial`` adds the generator model version (Customer 360
#: has none). Corpus role is not required: a None role reads "undeclared".
REQUIRED_KEYS: dict[str, tuple[str, ...]] = {
    "all": ("workload", "workload version", "workload parameters", "mode", "seed", "scale"),
    EXP1: ("corpus id",),
    EXP2: ("corpus id v2", "query set id"),
    "financial": ("generator model version",),
}


def missing_required(c: Classified, *, results: bool = True) -> list[str]:
    """The ``REQUIRED_KEYS`` that are None on *c* (ladder step 0). The query
    set id is required only of a run whose results were checked
    (*results*): a run with no benchmark has none, and reads NOT
    ESTABLISHED at step 4, not identity incomplete."""
    keys = list(REQUIRED_KEYS["all"]) + list(REQUIRED_KEYS.get(c.generation, ()))
    if not results:
        keys = [k for k in keys if k != "query set id"]
    if c.keys(WORKLOAD).get("workload") == "financial":
        keys += list(REQUIRED_KEYS["financial"])
    values = {**c.keys(WORKLOAD), **c.keys(CORPUS)}
    return [k for k in keys if values.get(k) is None]


@dataclass(frozen=True)
class Difference:
    group: str
    key: str
    a: Any
    b: Any

    def __str__(self) -> str:
        return f"{self.key} differs ({self.a!r} vs {self.b!r})"


def seeds_equal(a: Any, b: Any) -> bool | None:
    """Whether two recorded seeds name one seed; None when it cannot be told.

    Plain integers compare exactly. A recorded form ``{seed_ref, role}``
    (a protected seed) compares by ``seed_ref`` through ``datagen_seed.seeds_equal``
    when this tree has it; a form whose ``seed_ref`` is None (withheld) is
    never equal to anything."""
    from lakebench.config import datagen_seed

    rule = getattr(datagen_seed, "seeds_equal", None)
    if rule is not None:
        return rule(a, b)
    if a is None or b is None:
        return None
    if isinstance(a, Mapping) or isinstance(b, Mapping):
        ra = a.get("seed_ref") if isinstance(a, Mapping) else None
        rb = b.get("seed_ref") if isinstance(b, Mapping) else None
        if ra is None or rb is None:
            return None
        return bool(ra == rb)
    if isinstance(a, bool) or isinstance(b, bool):
        return None
    return bool(a == b)


def diff_group(a: Classified, b: Classified, group: str, *, skip: Any = ()) -> list[Difference]:
    """Key-by-key differences of one group (System is compared by
    ``system_relation``, not here). A key present on one side only differs;
    ``BOTH_SIDES_ONLY`` keys are compared only when both have a value."""
    ka, kb = a.keys(group), b.keys(group)
    out: list[Difference] = []
    for key in dict.fromkeys([*ka, *kb]):
        if key in skip:
            continue
        va, vb = ka.get(key), kb.get(key)
        if key in BOTH_SIDES_ONLY and (va is None or vb is None):
            continue
        if key == "seed":
            if seeds_equal(va, vb) is True:
                continue
            out.append(Difference(group, key, va, vb))
            continue
        if (key in ka) != (key in kb) or va != vb:
            out.append(Difference(group, key, va, vb))
    return out


def system_relation(a: Classified, b: Classified) -> tuple[str, str | None]:
    """``("same" | "different" | "unknown", note)`` for the System group.

    Different system strings (cluster against local) are different. With
    no fingerprint on either side (every exp1 record from before 1.7) the
    systems are assumed the same, with a note. With both,
    ``system_identity.same_system`` decides (equal over the parts both
    observed, the API server CA among them), and observations that differ
    on a common part are different. ``unknown``, with the reason, is
    everything in between: a fingerprint on one side only (a 1.6 record
    against a 1.7 one), observations that agree on every common part but
    not on the CA, or no common part (two local runs). The ladder reads
    ``unknown`` as the same system with the note, and never as a repeat."""
    from lakebench.metrics.system_identity import common_fingerprints, same_system

    sa, sb = a.keys(SYSTEM).get("system"), b.keys(SYSTEM).get("system")
    if sa != sb:
        return "different", f"system {sa!r} vs {sb!r}"
    ia, ib = a.system_identity, b.system_identity
    if ia is None and ib is None:
        return "same", "system identity not recorded; assumed the same"
    if ia is None or ib is None:
        missing = a.run_id if ia is None else b.run_id
        return "unknown", f"system identity not established: not recorded on {missing}"
    if same_system(ia, ib):
        return "same", None
    fa, fb, keys = common_fingerprints(ia, ib)
    if fa != fb:
        return (
            "different",
            f"system fingerprint differs ({ia.get('fingerprint')} vs {ib.get('fingerprint')})",
        )
    if not keys:
        return "unknown", "system identity not established: no part observed on both sides"
    return "unknown", (
        "system identity not established: the observations agree on "
        + (", ".join(keys) or "no part")
        + " but not on the API server CA"
    )


# ---------------------------------------------------------------------------
# The ladder: two sides of stored records, one verdict
# ---------------------------------------------------------------------------

NOT_COMPARABLE = "NOT COMPARABLE"
NOT_ESTABLISHED = "NOT ESTABLISHED"
CONFOUNDED = "CONFOUNDED"
NOT_LIKE_FOR_LIKE = "NOT LIKE-FOR-LIKE"
LIKE_FOR_LIKE = "LIKE-FOR-LIKE"

#: The code each verdict exits with (TUD 7.6).
VERDICT_CODES = {
    NOT_COMPARABLE: 10,
    NOT_ESTABLISHED: 11,
    NOT_LIKE_FOR_LIKE: 12,
    CONFOUNDED: 13,
    LIKE_FOR_LIKE: 0,
}

#: Record verdict statuses a side may not contain (ladder step 1).
_EXCLUDED_STATUSES = ("FAILED", "INTERRUPTED", "VOID")


@dataclass
class PairVerdict:
    """What ``pair_verdict`` decided, and why."""

    verdict: str
    step: str
    reasons: list[str] = field(default_factory=list)
    #: Between-side differences by group (filled from step 6 on, and for
    #: the Workload and Corpus groups at step 3).
    differences: dict[str, list[Difference]] = field(default_factory=dict)
    #: ``same``, ``different`` or ``unknown`` (``system_relation``); None
    #: when the ladder stopped before the System group was read.
    system: str | None = None
    #: For LIKE-FOR-LIKE: architecture differential, system differential,
    #: repeat, or "system not established".
    attribution: str | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def code(self) -> int:
        return VERDICT_CODES[self.verdict]

    @property
    def comparable(self) -> bool:
        return self.verdict in (CONFOUNDED, NOT_LIKE_FOR_LIKE, LIKE_FOR_LIKE)

    def keys(self, group: str) -> list[str]:
        return [d.key for d in self.differences.get(group, [])]

    def to_dict(self) -> dict[str, Any]:
        return {
            "verdict": self.verdict,
            "code": self.code,
            "step": self.step,
            "comparable": self.comparable,
            "reasons": list(self.reasons),
            "differences": {
                g: [{"key": d.key, "a": d.a, "b": d.b} for d in ds]
                for g, ds in self.differences.items()
                if ds
            },
            "system": self.system,
            "attribution": self.attribution,
            "notes": list(self.notes),
        }


def _rid(record: Mapping[str, Any]) -> str:
    return str(record.get("run_id") or "unknown run")


def _seed_withheld(value: Any) -> bool:
    return isinstance(value, Mapping) and value.get("seed_ref") is None


def _first_reason(record: Mapping[str, Any]) -> str | None:
    reasons = (record.get("verdict") or {}).get("reasons") or []
    return str(reasons[0]) if reasons else None


def _results_differences(
    ea: Mapping[str, Any], eb: Mapping[str, Any], la: str, lb: str
) -> list[str]:
    """Step 5: a different query set or a per-query result mismatch (the
    alert-set fingerprint joins here once it is recorded)."""
    from lakebench.metrics.experiment import fingerprint_differences

    qa = (ea.get("results") or {}).get("query_set_id")
    qb = (eb.get("results") or {}).get("query_set_id")
    out = [f"benchmark query sets differ ({qa} vs {qb})"] if qa != qb else []
    return out + fingerprint_differences(ea, eb, la, lb)


def _member_passed(record: Mapping[str, Any]) -> tuple[bool, str | None]:
    """Whether a record is a passed run, and the status it reads. The
    strictest of the stored verdict (or ``success`` for a record without
    one) and, once the tree has it, the verdict recomputed from the record
    (``metrics.verdict.verdict_from_record``); a reader never promotes."""
    from lakebench.metrics import verdict as verdict_mod

    status = verdict_mod.verdict_status(record)
    ok = status not in _EXCLUDED_STATUSES and verdict_mod.passed(record)
    recompute = getattr(verdict_mod, "verdict_from_record", None)
    if ok and recompute is not None:
        again = recompute(record)
        again_status = getattr(again, "status", None) or (
            again.get("status") if isinstance(again, Mapping) else None
        )
        if again_status != "PASSED":
            return False, f"recomputed {again_status}"
    return ok, status


def _digests(members: list[Classified]) -> set[Any]:
    return {c.keys(CORPUS).get("generator digest") for c in members} - {None}


def pair_verdict(
    side_a: list[Mapping[str, Any]],
    side_b: list[Mapping[str, Any]],
    label_a: str = "A",
    label_b: str = "B",
) -> PairVerdict:
    """The comparison verdict of two sides of metrics.json dicts, each side
    one or more runs of one experiment (the comparison ladder, first match
    wins):

    0. members of different identity versions, or a required Workload or
       Corpus key None (or a withheld seed) on any member: NOT COMPARABLE;
    1. a side with a legacy, FAILED, INTERRUPTED or void member, or no
       member: NOT COMPARABLE;
    2. a side whose members differ in a Workload, Corpus, Architecture,
       System or Conditions key, in their generator digests, or in
       results: NOT COMPARABLE, the side is not one experiment;
    3. Workload or Corpus differs between the sides (generator digests over
       every member), or a member has corpus problems: NOT COMPARABLE;
    4. results not established on a member: NOT ESTABLISHED;
    5. different query sets or results: NOT COMPARABLE;
    6. Architecture and System both differ: CONFOUNDED;
    7. Conditions differ: NOT LIKE-FOR-LIKE;
    7a. only the dependency pinset differs in Architecture: NOT
       LIKE-FOR-LIKE, same composition on different dependency sets;
    8. LIKE-FOR-LIKE, attributed to the architecture, the system, a
       repeat, or "system not established".

    A System that cannot be shown to be one or two systems (``unknown`` in
    ``system_relation``) reads as the same system with its note; it is never
    a repeat.
    """
    from lakebench.metrics.experiment import corpus_problems, results_established

    sides = ((label_a, list(side_a)), (label_b, list(side_b)))
    classified: dict[str, list[Classified]] = {label_a: [], label_b: []}

    # Step 0: identity versions and required keys, over members with a block.
    gens: dict[str, set[str]] = {label_a: set(), label_b: set()}
    incomplete: list[str] = []
    for label, members in sides:
        for rec in members:
            if generation(rec) == LEGACY:
                continue
            exp = rec["experiment"]
            c = classify(exp, rec)
            classified[label].append(c)
            gens[label].add(c.generation)
            checked = results_established(exp) is True
            for key in missing_required(c, results=checked):
                incomplete.append(f"identity incomplete: {key} not recorded on {_rid(rec)}")
            if _seed_withheld(c.keys(CORPUS).get("seed")):
                incomplete.append(f"identity incomplete: seed withheld on {_rid(rec)}")
    for label, _members in sides:
        if len(gens[label]) > 1:
            return PairVerdict(
                NOT_COMPARABLE, "0", [f"side {label} mixes identity v1 and v2 records"]
            )
    ga, gb = gens[label_a], gens[label_b]
    if ga and gb and ga != gb:
        va, vb = (1 if g == {EXP1} else 2 for g in (ga, gb))
        return PairVerdict(
            NOT_COMPARABLE,
            "0",
            [f"{label_a} was recorded with identity v{va} and {label_b} with v{vb}"],
        )
    if incomplete:
        return PairVerdict(NOT_COMPARABLE, "0", incomplete)

    # Step 1: every member a passed run with a block.
    failed: list[str] = []
    for label, members in sides:
        if not members:
            failed.append(f"side {label} has no run")
        for rec in members:
            if generation(rec) == LEGACY:
                failed.append(f"{label} run {_rid(rec)} predates the experiment block; re-run it")
                continue
            ok, status = _member_passed(rec)
            if not ok:
                why = _first_reason(rec)
                failed.append(
                    f"{label} run {_rid(rec)} did not pass"
                    + (f" ({why})" if why else f" ({status or 'success false'})")
                    + "; fix it and re-run"
                )
    if failed:
        return PairVerdict(NOT_COMPARABLE, "1", failed)

    notes: list[str] = []
    for label, _members in sides:
        for c in classified[label]:
            notes += [n for n in c.notes if n not in notes]

    # Step 2: each side is one experiment.
    for label, members in sides:
        first, first_rec = classified[label][0], members[0]
        for c, rec in zip(classified[label][1:], members[1:], strict=True):
            within: list[str] = []
            for group in (WORKLOAD, CORPUS, ARCHITECTURE, CONDITIONS):
                within += [str(d) for d in diff_group(first, c, group)]
            within_rel, within_why = system_relation(first, c)
            if within_rel == "different":
                within.append(f"system: {within_why}")
            elif within_why and within_why not in notes:
                notes.append(within_why)
            within += _results_differences(
                first_rec["experiment"], rec["experiment"], _rid(first_rec), _rid(rec)
            )
            if within:
                return PairVerdict(
                    NOT_COMPARABLE,
                    "2",
                    [f"side {label} is not one experiment ({_rid(first_rec)} vs {_rid(rec)}):"]
                    + within,
                    notes=notes,
                )
        digests = _digests(classified[label])
        if len(digests) > 1:
            return PairVerdict(
                NOT_COMPARABLE,
                "2",
                [
                    f"side {label} is not one experiment:",
                    f"generator digest differs ({', '.join(sorted(map(str, digests)))})",
                ],
                notes=notes,
            )

    ca, cb = classified[label_a][0], classified[label_b][0]
    ea = side_a[0]["experiment"]

    # Step 3: one workload on one corpus.
    identity_diffs = {g: diff_group(ca, cb, g) for g in (WORKLOAD, CORPUS)}
    digests = _digests(classified[label_a] + classified[label_b])
    if len(digests) > 1 and not any(d.key == "generator digest" for d in identity_diffs[CORPUS]):
        da, db = _digests(classified[label_a]), _digests(classified[label_b])
        identity_diffs[CORPUS].append(
            Difference(CORPUS, "generator digest", sorted(map(str, da)), sorted(map(str, db)))
        )
    problems = [
        f"{label}: {p}"
        for label, members in sides
        for rec in members
        for p in corpus_problems(rec["experiment"])
    ]
    if any(identity_diffs.values()) or problems:
        return PairVerdict(
            NOT_COMPARABLE,
            "3",
            [str(d) for ds in identity_diffs.values() for d in ds] + problems,
            differences=identity_diffs,
            notes=notes,
        )

    # Step 4: results checked on every member.
    unestablished = [
        f"{label} run {_rid(rec)}: {established}"
        for label, members in sides
        for rec in members
        if (established := results_established(rec["experiment"])) is not True
    ]
    if unestablished:
        return PairVerdict(NOT_ESTABLISHED, "4", unestablished, notes=notes)

    # Step 5: the same results, every member of B against A's first (step 2
    # already holds each side's members to its own first).
    different = []
    for rec in side_b:
        lb = label_b if len(side_b) == 1 else f"{label_b} run {_rid(rec)}"
        different += _results_differences(ea, rec["experiment"], label_a, lb)
    if different:
        return PairVerdict(NOT_COMPARABLE, "5", different, notes=notes)

    arch = diff_group(ca, cb, ARCHITECTURE)
    cond = diff_group(ca, cb, CONDITIONS)
    rel, rel_note = system_relation(ca, cb)
    why = rel_note or ""
    if rel_note and rel_note not in notes:
        notes.append(rel_note)
    pinsets = [c.keys(ARCHITECTURE).get("dependency pinset") for c in (ca, cb)]
    if all(p in (None, PINSET_NOT_RECORDED) for p in pinsets):
        notes.append("dependency set not recorded")
    diffs = {ARCHITECTURE: arch, CONDITIONS: cond}

    def verdict(name: str, step: str, reasons: list[str], attribution: str | None = None):
        return PairVerdict(
            name, step, reasons, diffs, system=rel, attribution=attribution, notes=notes
        )

    # Step 6: architecture and system both differ.
    if arch and rel == "different":
        return verdict(
            CONFOUNDED,
            "6",
            [f"architecture and system both differ ({', '.join(d.key for d in arch)}; {why})"],
        )
    # Step 7: conditions differ.
    if cond:
        return verdict(NOT_LIKE_FOR_LIKE, "7", [str(d) for d in cond])
    # Step 7a: same composition, different dependency sets.
    if [d.key for d in arch] == ["dependency pinset"]:
        return verdict(
            NOT_LIKE_FOR_LIKE,
            "7a",
            ["same composition, different dependency sets (dependency pinset differs)"],
        )
    # Step 8.
    if arch:
        attribution = "architecture differential"
    elif rel == "different":
        attribution = "system differential"
    elif rel == "unknown":
        attribution = "system not established"
    else:
        attribution = "repeat"
    return verdict(LIKE_FOR_LIKE, "8", [], attribution)
