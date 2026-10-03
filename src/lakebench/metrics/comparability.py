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

#: The Architecture key valued with the image digests a run's pods ran
#: (exp2 only); compared by ``observed_images_equal``.
OBSERVED_IMAGES_KEY = "observed image digests"

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
#: difference (not like-for-like), between the sides and inside one side
#: (``is_outcome_difference``); the perf gate and reproduce do not refuse
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


#: config_snapshot.spark.executor_overrides keys a run of each mode applies
#: (job.EXECUTOR_OVERRIDE_FIELDS; standard library only here).
_OVERRIDE_KEYS_BY_MODE = {
    "batch": {"bronze-verify": "bronze", "silver-build": "silver", "gold-finalize": "gold"},
    "continuous": {
        "bronze-ingest": "bronze_ingest",
        "silver-stream": "silver_stream",
        "gold-refresh": "gold_refresh",
    },
}


def _executor_overrides(exp: Mapping[str, Any], record: Mapping[str, Any] | None) -> dict:
    """User-set executor counts the run applied, keyed as in
    ``config_snapshot.spark.executor_overrides``: the block's
    ``architecture.spark_executor_overrides`` (a v1.7 block: absent means
    none), else, for an exp1 block, the non-null entries of the record's
    snapshot for the run's mode, else the ``override`` of each
    ``limits.executors`` entry. One key space and
    one mode filter, so a v1.6 record and a v1.7 record of the same config
    read the same."""
    stored = (exp.get("architecture") or {}).get("spark_executor_overrides")
    if isinstance(stored, Mapping):
        return {str(k): v for k, v in sorted(stored.items()) if v is not None}
    if block_generation(exp) == EXP2:
        # A v1.7 block records the overrides the run applied; absent means
        # none (a local run, or overrides for the other mode).
        return {}
    # The block's mode is the mode the run used (experiment_inputs' run_mode).
    mode = "continuous" if exp.get("mode") in ("continuous", "sustained") else "batch"
    keys = _OVERRIDE_KEYS_BY_MODE[mode]
    snapshot = (record or {}).get("config_snapshot") or {}
    if snapshot.get("local"):
        # A --local run has no executors: no override applied.
        return {}
    snap = snapshot.get("spark") or {}
    overrides = snap.get("executor_overrides")
    if isinstance(overrides, Mapping):
        wanted = set(keys.values())
        return {str(k): v for k, v in sorted(overrides.items()) if v is not None and k in wanted}
    out = {}
    for entry in (exp.get("limits") or {}).get("executors") or []:
        if isinstance(entry, Mapping) and entry.get("override") is not None:
            key = keys.get(str(entry.get("job_type")))
            if key:
                out[key] = entry["override"]
    return dict(sorted(out.items()))


def _driver_overrides(exp: Mapping[str, Any], record: Mapping[str, Any] | None) -> dict:
    """The global driver overrides the run applied: the block's
    ``architecture.spark_driver_overrides`` (v1.7; a v1.6 record did not
    record them)."""
    stored = (exp.get("architecture") or {}).get("spark_driver_overrides")
    return dict(sorted(stored.items())) if isinstance(stored, Mapping) else {}


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
    "spark driver overrides": OptionalKey(
        ARCHITECTURE, {}, _driver_overrides, "user driver overrides"
    ),
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
        from lakebench.metrics.maintenance_policy import operation_label

        return operation_label(detail)
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


def _benchmark_rounds(limits: Mapping[str, Any], record: Mapping[str, Any] | None) -> Any:
    """In-stream rounds behind a continuous QpH median: the block's
    ``limits.benchmark_rounds``, or, for a record written before the block
    stored it, the rounds of the record's ``pipeline_benchmark`` with a
    positive QpH (the count ``build_experiment`` stores). Without either it
    is None. A read-time derivation: ``_identity_v1`` and its digest do not
    change."""
    stored = limits.get("benchmark_rounds")
    if stored is not None:
        return stored
    pb = (record or {}).get("pipeline_benchmark")
    if not isinstance(pb, Mapping):
        return None
    # A record with no in-stream round saves no rounds list: 0, as stored.
    rounds = pb.get("benchmark_rounds") or []
    if not isinstance(rounds, list):
        return None

    def ran(r: Any) -> bool:
        try:
            return isinstance(r, Mapping) and float(r.get("qph") or 0) > 0
        except (TypeError, ValueError):
            return False

    return sum(1 for r in rounds if ran(r))


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
    #: Written by 1.7 or later (``written_by_v17``): such a record without a
    #: system identity failed to sample it, and is never assumed to share
    #: a system.
    v17: bool = False
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
            architecture[OBSERVED_IMAGES_KEY] = observed
    else:
        architecture["query access path"] = arch.get("query_access_path")
    has_pinset, pinset, notes = dependency_pinset(exp, record)
    out.notes += notes
    out.v17 = written_by_v17(exp, record)[0]
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
        "benchmark rounds": _benchmark_rounds(limits, record),
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


def _digest(image_id: Any) -> str:
    """The ``sha256:...`` part of a pod imageID; a registry mirror can
    report the same image under another repository name."""
    text = str(image_id)
    return text.rsplit("@", 1)[-1] if "@" in text else text


def observed_images_equal(a: Any, b: Any) -> bool | None:
    """Whether two runs' observed image digests agree, over the roles both
    observed. None when there is nothing to compare: a side observed no
    digest (``"not_observed"``), or the two share no role. Which roles were
    seen depends on timing (a short stage's executors may never be read),
    so a role seen on one side only is not a difference."""
    if not isinstance(a, Mapping) or not isinstance(b, Mapping):
        return None
    common = set(a) & set(b)
    if not common:
        return None
    return all(_digest(a[r]) == _digest(b[r]) for r in common)


def observed_images_note(a: Any, b: Any) -> str | None:
    """A note when the observed image digests of a pair were compared on
    part of the roles, or not at all; None when every role was compared or
    neither side carries the key (every record from before 1.7)."""
    if a is None and b is None:
        return None
    if not isinstance(a, Mapping) or not isinstance(b, Mapping):
        return "observed image digests not compared: a side observed none"
    one_sided = sorted(set(a) ^ set(b))
    if not set(a) & set(b):
        return "observed image digests not compared: the runs observed no role in common"
    if one_sided:
        return (
            "observed image digests compared on "
            + ", ".join(sorted(set(a) & set(b)))
            + "; not observed on both sides: "
            + ", ".join(one_sided)
        )
    return None


def is_outcome_difference(d: Difference) -> bool:
    """Whether *d*, found between two runs of ONE side, is an outcome of
    the runs rather than a sign that they are different experiments (owner
    decision 10-03 (a)). An outcome key (``OUTCOME_CONDITION_KEYS``: the
    in-stream round count, investigator sessions) is decided by the run's
    own speed, so a side whose repeats differ in it is still one
    experiment; the pair is NOT LIKE-FOR-LIKE, as when the sides differ in
    it, not NOT COMPARABLE. Every other key, and the query results, still
    make the side not one experiment. So does an outcome value that is not
    a count on both runs (missing, unreadable, a list or a mapping, which
    may be a setting rather than an outcome), and 0 against more than 0:
    with no in-stream round the QpH is the post-stream benchmark, another
    estimator (the perf gate refuses the same switch,
    ``experiment.stored_identity_refusals``), and no investigator session
    against some is a run without the load against one with it."""
    if d.group != CONDITIONS or d.key not in OUTCOME_CONDITION_KEYS:
        return False
    if not all(isinstance(v, int) and not isinstance(v, bool) for v in (d.a, d.b)):
        return False
    return (d.a > 0) == (d.b > 0)


def _all_skipped_policy(effective_id: Any) -> str | None:
    """The maintenance policy (without ``+skipped``) of an
    effective-maintenance id in which every operation was
    ``skipped_by_user``, else None."""
    from lakebench.metrics.maintenance_policy import SKIPPED_BY_USER, SKIPPED_SUFFIX

    if not isinstance(effective_id, str) or ":" not in effective_id:
        return None
    policy, _, ops = effective_id.partition(":")
    classes = [tok.partition("=")[2] for tok in ops.split(",")]
    if not policy or not classes or any(c != SKIPPED_BY_USER for c in classes):
        return None
    return policy.removesuffix(SKIPPED_SUFFIX)


def _both_skipped(ka: Mapping[str, Any], kb: Mapping[str, Any]) -> bool:
    pa = _all_skipped_policy(ka.get("effective maintenance"))
    return pa is not None and pa == _all_skipped_policy(kb.get("effective maintenance"))


def maintenance_equal(a: Any, b: Any) -> bool:
    """Whether two effective-maintenance ids are the same maintenance
    (owner decision 10-03 (b)): the same id, or both runs skipped every
    operation by the user's choice (``--skip-maintenance``, or
    ``pre_benchmark_maintenance`` off) under one maintenance policy. No
    maintenance ran on either side, so an Iceberg run and a Delta run, whose
    ids name different operations, are equal here. ``not_supported`` (the
    composition cannot run it) is not a skip and stays a difference. When
    it holds through the skip case, ``maintenance settings`` (how
    aggressive the maintenance would have been) is not compared either."""
    if a == b:
        return True
    pa = _all_skipped_policy(a)
    return pa is not None and pa == _all_skipped_policy(b)


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
        if key == OBSERVED_IMAGES_KEY:
            if observed_images_equal(va, vb) is False:
                out.append(Difference(group, key, va, vb))
            continue
        if key == "effective maintenance" and key in ka and key in kb and maintenance_equal(va, vb):
            continue
        if key == "maintenance settings" and _both_skipped(ka, kb):
            # How aggressive maintenance would have been, when none ran on
            # either side (maintenance_equal's skip case), changes nothing.
            continue
        if (key in ka) != (key in kb) or va != vb:
            out.append(Difference(group, key, va, vb))
    return out


UNOBSERVED = "unobserved"

#: How strongly a relation says "not one system", for folding several.
_RELATION_ORDER = {"same": 0, "unknown": 1, UNOBSERVED: 2, "different": 3}


def system_relation(a: Classified, b: Classified) -> tuple[str, str | None]:
    """``(relation, note)`` for the System group, the relation one of
    ``same``, ``different``, ``unobserved`` or ``unknown``.

    Different system strings (cluster against local) are different. With
    no fingerprint on either side (every exp1 record from before 1.7) the
    systems are assumed the same, with a note. With both,
    ``system_identity.same_system`` decides (equal over the parts both
    observed, the API server CA among them), and observations that differ
    on a common part are different. ``unobserved`` is a pair nothing could
    be compared for: a fingerprint on one side only, a 1.7 record that did
    not sample its system, or cluster observations with no part in common;
    the ladder treats it as different, but never names a system
    differential on it. ``unknown``, with the reason, is the case in
    between: cluster observations that agree on every common part but not
    on the CA, or two local runs (which record no part). The ladder reads
    ``unknown`` as the same system with the note, and never as a repeat."""
    from lakebench.metrics.system_identity import common_fingerprints, same_system

    sa, sb = a.keys(SYSTEM).get("system"), b.keys(SYSTEM).get("system")
    if sa != sb:
        return "different", f"system {sa!r} vs {sb!r}"
    ia, ib = a.system_identity, b.system_identity
    if ia is None and ib is None:
        unsampled = [c.run_id or "?" for c in (a, b) if c.v17]
        if unsampled:
            # A 1.7 run without one failed to sample it: nothing says where
            # it ran.
            return UNOBSERVED, "system identity not sampled on " + ", ".join(unsampled)
        return "same", "system identity not recorded; assumed the same"
    if ia is None or ib is None:
        missing = a.run_id if ia is None else b.run_id
        return UNOBSERVED, f"system identity recorded on one side only (not on {missing})"
    if same_system(ia, ib):
        return "same", None
    fa, fb, keys = common_fingerprints(ia, ib)
    if fa != fb:
        return (
            "different",
            f"system fingerprint differs ({ia.get('fingerprint')} vs {ib.get('fingerprint')})",
        )
    if not keys:
        if sa == "local":
            return "unknown", "system identity not established: local runs record no part"
        return UNOBSERVED, "system identity not comparable: no part observed on both sides"
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


@dataclass(frozen=True)
class Cause:
    """The first thing that decided a verdict, as data (the hint builder
    reads this, never the prose ``reasons``). *kind* is one of
    ``newer_schema``, ``generation``, ``mixed_generation``, ``required_key``,
    ``seed_withheld``, ``legacy``, ``failed``, ``no_run``, ``side_not_one``,
    ``identity``, ``corpus_problem``, ``not_established``, ``results``,
    ``confounded``, ``condition``, ``side_outcome`` (an outcome key differs
    inside one side), ``pinset`` or ``none``."""

    kind: str
    side: str | None = None
    run: str | None = None
    other_run: str | None = None
    group: str | None = None
    key: str | None = None
    a: Any = None
    b: Any = None
    detail: str | None = None


@dataclass
class PairVerdict:
    """What ``pair_verdict`` decided, and why."""

    verdict: str
    step: str
    reasons: list[str] = field(default_factory=list)
    #: Between-side differences by group (filled from step 6 on, and for
    #: the Workload and Corpus groups at step 3).
    differences: dict[str, list[Difference]] = field(default_factory=dict)
    #: ``same``, ``different``, ``unobserved`` or ``unknown``
    #: (``system_relation``); None
    #: when the ladder stopped before the System group was read.
    system: str | None = None
    #: For LIKE-FOR-LIKE: architecture differential, system differential,
    #: repeat, or "system not established".
    attribution: str | None = None
    notes: list[str] = field(default_factory=list)
    cause: Cause = field(default_factory=lambda: Cause("none"))

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
            "cause": {k: v for k, v in self.cause.__dict__.items() if v is not None},
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

    kind = record.get("record_kind") or "run"
    if kind != "run":
        # A `lakebench benchmark` record re-measures another run's query
        # stage; it is not a run, whatever its verdict.
        return False, f"a {kind} record of run {record.get('parent_run_id') or 'unknown'}"
    status = verdict_mod.verdict_status(record)
    ok = status not in _EXCLUDED_STATUSES and verdict_mod.stored_passed(record)
    recompute = getattr(verdict_mod, "verdict_from_record", None)
    if ok and recompute is not None:
        again = recompute(record)
        again_status = getattr(again, "status", None) or (
            again.get("status") if isinstance(again, Mapping) else None
        )
        if again_status != "PASSED":
            reasons = getattr(again, "reasons", None) or (
                again.get("reasons") if isinstance(again, Mapping) else None
            )
            why = next((str(r) for r in reasons or [] if not str(r).startswith("Gate '")), None)
            return False, f"recomputed {again_status}" + (f": {why}" if why else "")
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
       System or Conditions key other than an outcome key
       (``is_outcome_difference``), in their generator digests, or in
       results: NOT COMPARABLE, the side is not one experiment;
    3. Workload or Corpus differs between the sides (generator digests over
       every member), or a member has corpus problems: NOT COMPARABLE;
    4. results not established on a member: NOT ESTABLISHED;
    5. different query sets or results: NOT COMPARABLE;
    6. Architecture and System both differ: CONFOUNDED;
    7. Conditions differ between the sides (effective maintenance compared
       by ``maintenance_equal``), or an outcome key differs inside a side:
       NOT LIKE-FOR-LIKE;
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
    first_incomplete: Cause | None = None
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
                first_incomplete = first_incomplete or Cause(
                    "required_key", side=label, run=_rid(rec), key=key
                )
            if _seed_withheld(c.keys(CORPUS).get("seed")):
                incomplete.append(f"identity incomplete: seed withheld on {_rid(rec)}")
                first_incomplete = first_incomplete or Cause(
                    "seed_withheld", side=label, run=_rid(rec), key="seed"
                )
    for label, _members in sides:
        if len(gens[label]) > 1:
            return PairVerdict(
                NOT_COMPARABLE,
                "0",
                [f"side {label} mixes identity v1 and v2 records"],
                cause=Cause("mixed_generation", side=label),
            )
    ga, gb = gens[label_a], gens[label_b]
    if ga and gb and ga != gb:
        va, vb = (1 if g == {EXP1} else 2 for g in (ga, gb))
        return PairVerdict(
            NOT_COMPARABLE,
            "0",
            [f"{label_a} was recorded with identity v{va} and {label_b} with v{vb}"],
            cause=Cause("generation", a=va, b=vb),
        )
    if incomplete:
        return PairVerdict(NOT_COMPARABLE, "0", incomplete, cause=first_incomplete or Cause("none"))

    # Step 1: every member a passed run with a block.
    failed: list[str] = []
    first_failed: Cause | None = None
    for label, members in sides:
        if not members:
            failed.append(f"side {label} has no run")
            first_failed = first_failed or Cause("no_run", side=label)
        for rec in members:
            if generation(rec) == LEGACY:
                failed.append(f"{label} run {_rid(rec)} predates the experiment block; re-run it")
                exp_block = rec.get("experiment")
                schema = exp_block.get("schema") if isinstance(exp_block, Mapping) else None
                # A block with a schema this release cannot read is newer, not
                # older: the hint says upgrade, not re-run.
                first_failed = first_failed or Cause(
                    "newer_schema" if schema else "legacy",
                    side=label,
                    run=_rid(rec),
                    detail=str(schema) if schema else None,
                )
                continue
            ok, status = _member_passed(rec)
            if not ok:
                why = _first_reason(rec)
                detail = why if why else (status or "success false")
                failed.append(f"{label} run {_rid(rec)} did not pass ({detail}); fix it and re-run")
                first_failed = first_failed or Cause(
                    "failed", side=label, run=_rid(rec), detail=detail
                )
    if failed:
        return PairVerdict(NOT_COMPARABLE, "1", failed, cause=first_failed or Cause("none"))

    notes: list[str] = []
    for label, _members in sides:
        for c in classified[label]:
            notes += [n for n in c.notes if n not in notes]

    # Step 2: each side is one experiment. The System is checked over every
    # pair of members (same_system is not transitive); the rest against the
    # first member, whose keys equality makes transitive.
    for label, members in sides:
        cs = classified[label]
        for i, ci in enumerate(cs):
            for cj, rj in zip(cs[i + 1 :], members[i + 1 :], strict=True):
                pair_rel, pair_why = system_relation(ci, cj)
                if pair_rel in ("different", UNOBSERVED):
                    return PairVerdict(
                        NOT_COMPARABLE,
                        "2",
                        [
                            f"side {label} is not one experiment "
                            f"({_rid(members[i])} vs {_rid(rj)}):",
                            f"system: {pair_why}",
                        ],
                        notes=notes,
                        cause=Cause(
                            "side_not_one",
                            side=label,
                            run=_rid(members[i]),
                            other_run=_rid(rj),
                            group=SYSTEM,
                            key="system",
                            detail=pair_why,
                        ),
                    )
                if pair_why and pair_why not in notes:
                    notes.append(pair_why)
    # Outcome-key differences inside a side, by side and member pair
    # (is_outcome_difference): one experiment, not like-for-like (step 7).
    side_outcomes: list[tuple[str, str, str, Difference]] = []
    for label, members in sides:
        first, first_rec = classified[label][0], members[0]
        for c, rec in zip(classified[label][1:], members[1:], strict=True):
            within: list[str] = []
            first_diff: Difference | None = None
            for group in (WORKLOAD, CORPUS, ARCHITECTURE, CONDITIONS):
                ds = diff_group(first, c, group)
                side_outcomes += [
                    (label, _rid(first_rec), _rid(rec), d) for d in ds if is_outcome_difference(d)
                ]
                ds = [d for d in ds if not is_outcome_difference(d)]
                first_diff = first_diff or (ds[0] if ds else None)
                within += [str(d) for d in ds]
            res = _results_differences(
                first_rec["experiment"], rec["experiment"], _rid(first_rec), _rid(rec)
            )
            within += res
            if within:
                return PairVerdict(
                    NOT_COMPARABLE,
                    "2",
                    [f"side {label} is not one experiment ({_rid(first_rec)} vs {_rid(rec)}):"]
                    + within,
                    notes=notes,
                    cause=Cause(
                        "side_not_one",
                        side=label,
                        run=_rid(first_rec),
                        other_run=_rid(rec),
                        group=first_diff.group if first_diff else "results",
                        key=first_diff.key if first_diff else "results",
                        a=first_diff.a if first_diff else None,
                        b=first_diff.b if first_diff else None,
                        detail=None if first_diff else res[0],
                    ),
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
                cause=Cause(
                    "side_not_one",
                    side=label,
                    group=CORPUS,
                    key="generator digest",
                    detail=", ".join(sorted(map(str, digests))),
                ),
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
    problem_of = [
        (label, _rid(rec), p)
        for label, members in sides
        for rec in members
        for p in corpus_problems(rec["experiment"])
    ]
    problems = [f"{label}: {p}" for label, _r, p in problem_of]
    if any(identity_diffs.values()) or problems:
        first_id = next((ds[0] for ds in identity_diffs.values() if ds), None)
        if first_id is not None:
            cause = Cause(
                "identity", group=first_id.group, key=first_id.key, a=first_id.a, b=first_id.b
            )
        else:
            label, run, problem = problem_of[0]
            cause = Cause("corpus_problem", side=label, run=run, detail=problem)
        return PairVerdict(
            NOT_COMPARABLE,
            "3",
            [str(d) for ds in identity_diffs.values() for d in ds] + problems,
            differences=identity_diffs,
            notes=notes,
            cause=cause,
        )

    # Step 4: results checked on every member.
    open_items = [
        (label, _rid(rec), established)
        for label, members in sides
        for rec in members
        if (established := results_established(rec["experiment"])) is not True
    ]
    if open_items:
        label, run, why_not = open_items[0]
        return PairVerdict(
            NOT_ESTABLISHED,
            "4",
            [f"{lb} run {r}: {w}" for lb, r, w in open_items],
            notes=notes,
            cause=Cause("not_established", side=label, run=run, detail=str(why_not)),
        )

    # Step 5: the same results, every member of B against A's first (step 2
    # already holds each side's members to its own first).
    different = []
    for rec in side_b:
        lb = label_b if len(side_b) == 1 else f"{label_b} run {_rid(rec)}"
        different += _results_differences(ea, rec["experiment"], label_a, lb)
    if different:
        return PairVerdict(
            NOT_COMPARABLE,
            "5",
            different,
            notes=notes,
            cause=Cause("results", detail=different[0]),
        )

    arch = diff_group(ca, cb, ARCHITECTURE)
    cond = diff_group(ca, cb, CONDITIONS)
    # The System relation of the pair is the weakest link over every member
    # of A against every member of B (same_system is not transitive), and
    # no stronger than "unknown" when a side's members were only unknown.
    rel, rel_note = "same", None
    for a_c in classified[label_a]:
        for b_c in classified[label_b]:
            r, n = system_relation(a_c, b_c)
            if _RELATION_ORDER[r] > _RELATION_ORDER[rel]:
                rel, rel_note = r, n
            if n and n not in notes and r == "same":
                notes.append(n)
    if rel == "same" and any("not established" in n for n in notes):
        rel = "unknown"
    why = rel_note or ""
    if rel_note and rel_note not in notes:
        notes.append(rel_note)
    pinsets = [c.keys(ARCHITECTURE).get("dependency pinset") for c in (ca, cb)]
    if all(p in (None, PINSET_NOT_RECORDED) for p in pinsets):
        notes.append("dependency set not recorded")
    images_note = observed_images_note(
        ca.keys(ARCHITECTURE).get(OBSERVED_IMAGES_KEY),
        cb.keys(ARCHITECTURE).get(OBSERVED_IMAGES_KEY),
    )
    if images_note and images_note not in notes:
        notes.append(images_note)
    diffs = {ARCHITECTURE: arch, CONDITIONS: cond}

    def verdict(
        name: str,
        step: str,
        reasons: list[str],
        attribution: str | None = None,
        cause: Cause | None = None,
    ):
        return PairVerdict(
            name,
            step,
            reasons,
            diffs,
            system=rel,
            attribution=attribution,
            notes=notes,
            cause=cause or Cause("none"),
        )

    inside = [
        f"side {lb}: {d.key} differs inside the side ({r} {d.a!r} vs {o} {d.b!r})"
        for lb, r, o, d in side_outcomes
    ]
    # Step 6: architecture and system both differ.
    if arch and rel in ("different", UNOBSERVED):
        return verdict(
            CONFOUNDED,
            "6",
            [f"architecture and system both differ ({', '.join(d.key for d in arch)}; {why})"]
            + inside,
            cause=Cause(
                "confounded",
                group=ARCHITECTURE,
                key=arch[0].key,
                a=arch[0].a,
                b=arch[0].b,
                detail=why,
            ),
        )
    # Step 7: conditions differ, between the sides or, in an outcome key,
    # inside one.
    if cond or side_outcomes:
        if cond:
            cause = Cause("condition", group=CONDITIONS, key=cond[0].key, a=cond[0].a, b=cond[0].b)
        else:
            lb, r, o, d = side_outcomes[0]
            cause = Cause(
                "side_outcome",
                side=lb,
                run=r,
                other_run=o,
                group=CONDITIONS,
                key=d.key,
                a=d.a,
                b=d.b,
            )
        return verdict(NOT_LIKE_FOR_LIKE, "7", [str(d) for d in cond] + inside, cause=cause)
    # Step 7a: same composition, different dependency sets.
    if [d.key for d in arch] == ["dependency pinset"]:
        unrecorded = [
            c.run_id or "?"
            for c in (ca, cb)
            if c.keys(ARCHITECTURE).get("dependency pinset") in (None, PINSET_NOT_RECORDED)
        ]
        reason = (
            "same composition, different dependency sets (dependency pinset differs)"
            if not unrecorded
            else "same composition; the dependency set is not recorded on " + ", ".join(unrecorded)
        )
        return verdict(
            NOT_LIKE_FOR_LIKE,
            "7a",
            [reason],
            cause=Cause("pinset", key="dependency pinset", a=pinsets[0], b=pinsets[1]),
        )
    # Step 8.
    if arch:
        attribution = "architecture differential"
    elif rel == "different":
        attribution = "system differential"
    elif rel in ("unknown", UNOBSERVED):
        attribution = "system not established"
    else:
        attribution = "repeat"
    return verdict(LIKE_FOR_LIKE, "8", [], attribution)
