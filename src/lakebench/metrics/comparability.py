"""OD-2 identity groups: which recorded keys say what a run was (EVD-7).

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
  ``local`` string, and the ER-8 fingerprint when one was observed);
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
them (RR-2).

The ladder that turns two classified sides into one verdict is
``pair_verdict`` (ER-10b).
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

#: Generations (ch03 section 0.1). A legacy record has no experiment block.
LEGACY, EXP1, EXP2 = "legacy", "exp1", "exp2"

#: The value a None corpus role reads as, so None never meets None as an
#: accidental match (S1). Every exp1 record stores None; exp2 always writes
#: a role string.
UNDECLARED_ROLE = "undeclared"

#: The pinset value of a v1.7 record that did not record one: unequal to
#: every pinset and to absence.
PINSET_NOT_RECORDED = "not_recorded"

#: Keys compared only when both sides carry a value (a digest is evidence;
#: an unresolved digest on one side is not a difference).
BOTH_SIDES_ONLY = frozenset({"generator digest"})

#: Conditions that are also outcomes of the run: compare reports a
#: difference (not like-for-like), the perf gate and reproduce do not refuse
#: on it, and members of one side may differ in it.
OUTCOME_CONDITION_KEYS = frozenset({"benchmark rounds", "investigator sessions"})


# ---------------------------------------------------------------------------
# Optional identity keys: present only when non-default
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OptionalKey:
    """One ``OPTIONAL_IDENTITY_KEYS`` row. *extractor* reads the value from
    an experiment block and, when given, its whole metrics.json record."""

    group: str
    default: Any
    extractor: Callable[[Mapping[str, Any], Mapping[str, Any] | None], Any]
    owner_wi: str


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
    spark_executor_overrides`` (CC-12), else the non-null entries of the
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
    """User Spark conf keys and hash (CC-13), as the block records them."""
    stored = (exp.get("architecture") or {}).get("spark_conf_user")
    return dict(stored) if isinstance(stored, Mapping) else {}


def _investigator_sessions(exp: Mapping[str, Any], record: Mapping[str, Any] | None) -> Any:
    """The investigator sessions that ran (AM-13), written only when
    ``architecture.benchmark.investigator_sessions`` is set."""
    inv = exp.get("investigators")
    return inv.get("run") if isinstance(inv, Mapping) else None


#: Only-when-non-default identity keys. Other chapters add rows here and
#: never edit ``experiment._identity_v1``. The v1.8 rows (``evaluation
#: profile``, CC-21; ``ml loop``, ML-14) are not built in v1.7.
OPTIONAL_IDENTITY_KEYS: dict[str, OptionalKey] = {
    "cycles": OptionalKey(CORPUS, 1, _cycles, "CD-18"),
    "spark executor overrides": OptionalKey(ARCHITECTURE, {}, _executor_overrides, "CC-12"),
    "spark conf": OptionalKey(ARCHITECTURE, {}, _spark_conf, "CC-13"),
    "investigator sessions": OptionalKey(CONDITIONS, None, _investigator_sessions, "AM-13"),
}


def optional_keys(exp: Mapping[str, Any], record: Mapping[str, Any] | None = None) -> dict:
    """``{key: value}`` of the optional identity keys whose value is not the
    default for *exp*. A reader bug is a missing key, never a crash."""
    out: dict[str, Any] = {}
    for name, row in OPTIONAL_IDENTITY_KEYS.items():
        try:
            value = row.extractor(exp, record)
        except Exception:  # noqa: BLE001 -- unreadable reads as the default
            continue
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


def dependency_pinset(
    exp: Mapping[str, Any], record: Mapping[str, Any] | None = None
) -> tuple[bool, Any, list[str]]:
    """``(present, value, notes)`` of the ``dependency pinset`` key.

    Keyed on the writer's ``lakebench_version``, never on the schema string:
    a record written by 1.7 or later carries the key, valued
    ``provenance.deps.pinset_sha256`` (the record's, else the block's
    ``lakebench`` copy of provenance), or ``"not_recorded"`` when that is
    absent. Below 1.7, or with no parseable version, the key is absent."""
    minor, note = lakebench_minor(exp)
    notes = [note] if note else []
    if minor is None or minor < (1, 7):
        return False, None, notes
    for prov in ((record or {}).get("provenance"), exp.get("lakebench")):
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
    """The compaction operation a run's maintenance executed (LB-212), or
    None when no compaction ran.

    Read from ``effective_maintenance.detail.operations.compaction`` when
    the block records it (ER-7); otherwise derived from the query engine
    and table format when the stored effective-maintenance id says
    ``compaction=ran``. The engine that ran the maintenance is the query
    engine (``cli/_sustained.py`` and ``deploy/iceberg.py`` build the
    statements per engine): Trino runs ``optimize`` with a 128MB file size
    threshold, Spark Thrift runs Iceberg ``rewrite_data_files`` with its
    defaults. Delta compaction never runs (``maintenance_policy``)."""
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
    """``legacy``, ``exp1`` or ``exp2`` (ch03 section 0.1) of a metrics.json
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
    #: ``experiment.system_identity`` when one was observed (ER-8).
    system_identity: Mapping[str, Any] | None = None
    notes: list[str] = field(default_factory=list)

    def keys(self, group: str) -> dict[str, Any]:
        return self.groups.get(group, {})


def _role(value: Any) -> Any:
    return UNDECLARED_ROLE if value is None else value


def classify(exp: Mapping[str, Any], record: Mapping[str, Any] | None = None) -> Classified:
    """*exp* (an experiment block, as stored) classified into the OD-2
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

    conditions: dict[str, Any] = {
        "effective maintenance": (exp.get("effective_maintenance") or {}).get("id"),
        "compaction operation": compaction_operation(exp),
        "maintenance settings": exp.get("maintenance_settings"),
        "benchmark iterations": limits.get("benchmark_iterations"),
        "benchmark mode": limits.get("benchmark_mode"),
        "Lakebench limits that bound": list(limits.get("bound_kinds") or []),
    }
    if exp.get("mode") == "sustained":
        conditions["benchmark rounds"] = limits.get("benchmark_rounds")

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


def missing_required(c: Classified) -> list[str]:
    """The ``REQUIRED_KEYS`` that are None on *c* (ladder step 0)."""
    keys = list(REQUIRED_KEYS["all"]) + list(REQUIRED_KEYS.get(c.generation, ()))
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
    (LB-229) compares by ``seed_ref`` through ``datagen_seed.seeds_equal``
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
    systems are assumed the same, with a note. A fingerprint on one side
    only is different. With both, ``system_identity.same_system`` decides
    (equal over the parts both observed, the API server CA among them);
    two observations that agree on every common part but cannot show they
    are one cluster are ``unknown``."""
    from lakebench.metrics.system_identity import common_fingerprints, same_system

    sa, sb = a.keys(SYSTEM).get("system"), b.keys(SYSTEM).get("system")
    if sa != sb:
        return "different", f"system {sa!r} vs {sb!r}"
    ia, ib = a.system_identity, b.system_identity
    if ia is None and ib is None:
        return "same", "system identity not recorded; assumed the same"
    if ia is None or ib is None:
        missing = a.run_id if ia is None else b.run_id
        return "different", f"system identity recorded on one side only (not on {missing})"
    if same_system(ia, ib):
        return "same", None
    fa, fb, keys = common_fingerprints(ia, ib)
    if fa != fb:
        return (
            "different",
            f"system fingerprint differs ({ia.get('fingerprint')} vs {ib.get('fingerprint')})",
        )
    if sa == "local":
        return "same", "local runs: system compared on " + (", ".join(keys) or "no part")
    return "unknown", (
        "system identity not established: the observations agree on "
        + (", ".join(keys) or "no part")
        + " but not on the API server CA"
    )
