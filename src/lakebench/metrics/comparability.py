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
them, and default runs keep their identity."""

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
# Group differences
# ---------------------------------------------------------------------------


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
    """Key-by-key differences of one group. A key present on one side only differs;
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
