"""Support states for workload x architecture x mode (DESIGN.md section 6.5).

Support is judged in four layers:

1. Architecture-valid: the component tuple is in ``_SUPPORTED_COMBINATIONS``.
2. Workload-compatible: ``WORKLOAD_TABLE_FORMATS`` lists the table format.
3. Mode-compatible: ``WORKLOAD_MODES`` lists the pipeline mode.
4. Validated: the release validation record
   (``validated_combinations.yaml`` beside this module) lists the workload x
   recipe x mode with the run ids that validated it on the release tree.

Passing all four is ``supported``; passing 1 to 3 only is ``unverified``;
failing any of 1 to 3 is ``unsupported`` and refused before a run. This module
is the one place that computes the state: the metrics ``experiment`` block,
``lakebench config show``, ``lakebench config recipes``, the report and
the docs tables all read it from here.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUPPORTED = "supported"
UNVERIFIED = "unverified"
UNSUPPORTED = "unsupported"

MODES: tuple[str, ...] = ("batch", "continuous")

#: The release validation record. Package data: shipped in the wheel.
VALIDATION_RECORD = Path(__file__).with_name("validated_combinations.yaml")

#: SPEC section 11: (workload, mode, recipe, scale) of every release row.
RELEASE_MATRIX: tuple[tuple[str, str, str, float], ...] = (
    ("customer360", "batch", "hive-iceberg-spark-trino", 1),
    ("customer360", "batch", "polaris-iceberg-spark-trino", 1),
    ("customer360", "batch", "hive-delta-spark-trino", 1),
    ("customer360", "batch", "hive-delta-spark-thrift", 1),
    ("customer360", "batch", "hive-iceberg-spark-thrift", 1),
    ("customer360", "batch", "polaris-iceberg-spark-thrift", 1),
    ("customer360", "batch", "hive-iceberg-spark-duckdb", 1),
    ("customer360", "batch", "polaris-iceberg-spark-duckdb", 1),
    ("customer360", "batch", "hive-iceberg-spark-none", 1),
    ("customer360", "continuous", "hive-iceberg-spark-trino", 1),
    ("customer360", "continuous", "hive-delta-spark-trino", 1),
    ("financial", "batch", "hive-iceberg-spark-trino", 1),
    ("financial", "batch", "polaris-iceberg-spark-trino", 1),
    ("financial", "continuous", "hive-iceberg-spark-trino", 1),
    ("financial", "continuous", "polaris-iceberg-spark-trino", 1),
    ("financial", "batch", "hive-iceberg-spark-trino", 10),
)

#: SPEC section 11's Spark minor and table format version per row, by
#: (workload, mode, recipe). The support record is keyed by them
#: (config/support.py), so a row's runs must use these versions.
_ICEBERG, _SPARK41, _SPARK40 = "1.11.0", "4.1", "4.0"
RELEASE_MATRIX_VERSIONS: dict[tuple[str, str, str], tuple[str, str]] = {
    ("customer360", "batch", "hive-iceberg-spark-trino"): (_SPARK41, _ICEBERG),
    ("customer360", "batch", "polaris-iceberg-spark-trino"): (_SPARK40, _ICEBERG),
    ("customer360", "batch", "hive-delta-spark-trino"): (_SPARK41, "4.1.0"),
    ("customer360", "batch", "hive-delta-spark-thrift"): (_SPARK40, "4.0.0"),
    ("customer360", "batch", "hive-iceberg-spark-thrift"): (_SPARK41, _ICEBERG),
    ("customer360", "batch", "polaris-iceberg-spark-thrift"): (_SPARK40, _ICEBERG),
    ("customer360", "batch", "hive-iceberg-spark-duckdb"): (_SPARK41, _ICEBERG),
    ("customer360", "batch", "polaris-iceberg-spark-duckdb"): (_SPARK40, _ICEBERG),
    ("customer360", "batch", "hive-iceberg-spark-none"): (_SPARK41, _ICEBERG),
    ("customer360", "continuous", "hive-iceberg-spark-trino"): (_SPARK41, _ICEBERG),
    ("customer360", "continuous", "hive-delta-spark-trino"): (_SPARK41, "4.1.0"),
    ("financial", "batch", "hive-iceberg-spark-trino"): (_SPARK41, _ICEBERG),
    ("financial", "batch", "polaris-iceberg-spark-trino"): (_SPARK40, _ICEBERG),
    ("financial", "continuous", "hive-iceberg-spark-trino"): (_SPARK41, _ICEBERG),
    ("financial", "continuous", "polaris-iceberg-spark-trino"): (_SPARK40, _ICEBERG),
}

_RECORD_NAME = "lakebench/config/validated_combinations.yaml"

WORKLOAD_LABELS: dict[str, str] = {
    "customer360": "Customer 360",
    "financial": "AML (financial)",
}

#: What a mode changes about a workload's meaning on every recipe. Shown
#: beside the support state so an unverified or supported run is not read as
#: the other mode's work. The AML continuous rule lists mirror
#: CONTINUOUS_RULES and CONTINUOUS_SKIPPED_RULES in
#: spark/scripts/gold_refresh_financial.py (a test holds them equal).
AML_CONTINUOUS_RULES: tuple[str, ...] = (
    "W4_risk_propagation",
    "W2_structuring",
    "W17_layering_chain",
    "W3_round_tripping",
)
AML_CONTINUOUS_SKIPPED_RULES: tuple[str, ...] = (
    "W5_sanctions_match",
    "W6_pep_counterparty",
    "W1_connected_components",
    "W7_cross_border_high_risk",
    "W8_dormant_reactivation",
)


def _rule_ids(rules: tuple[str, ...]) -> str:
    ids = sorted((r.split("_", 1)[0] for r in rules), key=lambda w: int(w[1:]))
    return ", ".join(ids)


MODE_NOTES: dict[tuple[str, str], str] = {
    ("financial", "continuous"): (
        f"AML continuous runs detection rules {_rule_ids(AML_CONTINUOUS_RULES)} each tick "
        f"and records {_rule_ids(AML_CONTINUOUS_SKIPPED_RULES)} as not run. Its results depend on when detection ran relative to "
        "arrival, so no end-of-run result check is recorded."
    ),
}


#: Datagen scale bands per workload (owner decision 2026-09-29): every datagen
#: pod stays within DATAGEN_POD_MEMORY_CAP_GIB at 8 threads. ``supported_max``
#: is the largest scale measured on the cluster within the cap (with the
#: autosizer headroom); ``ceiling`` is the largest scale the fitted memory
#: model keeps within the cap, bounded by twice the largest measured scale.
#: Above ``supported_max`` a run is unverified; above ``ceiling`` it is
#: refused. Measurement basis: config/autosizer.py (datagen memory model).
DATAGEN_POD_MEMORY_CAP_GIB = 16
DATAGEN_SCALE_BANDS: dict[str, tuple[float, float]] = {
    "financial": (300.0, 800.0),
    "customer360": (300.0, 600.0),
}


def datagen_scale_problem(workload: str | None, scale: float | None) -> tuple[str, str] | None:
    """``(state, basis)`` when *scale* is outside the workload's supported
    datagen band, else None. UNSUPPORTED above the ceiling, UNVERIFIED between
    the supported maximum and the ceiling."""
    band = DATAGEN_SCALE_BANDS.get(str(workload))
    if band is None or scale is None:
        return None
    supported_max, ceiling = band
    label = WORKLOAD_LABELS.get(str(workload), str(workload))
    if scale > ceiling:
        return (
            UNSUPPORTED,
            f"{label} scale {scale:g} is above the datagen ceiling of {ceiling:g}: a datagen "
            f"pod would exceed {DATAGEN_POD_MEMORY_CAP_GIB} GiB. Use a scale of {ceiling:g} or less.",
        )
    if scale > supported_max:
        return (
            UNVERIFIED,
            f"{label} scale {scale:g} is above the largest scale measured within "
            f"{DATAGEN_POD_MEMORY_CAP_GIB} GiB per datagen pod ({supported_max:g}); the memory "
            f"model predicts it fits up to {ceiling:g}.",
        )
    return None


class ValidationRecordError(ValueError):
    """The release validation record is malformed or lists a combination
    that is not valid for its workload and mode."""


#: (workload, recipe, mode, Spark minor, table format version): a validation
#: entry stands for one set of component versions, not for every Spark and
#: format version a recipe can run on.
ValidationKey = tuple[str, str, str, str, str]

_ENTRY_KEYS = ("workload", "recipe", "mode", "spark", "table_format_version", "tree", "runs")
_SPARK_MINOR = re.compile(r"^\d+\.\d+$")
_SHA40 = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class Validation:
    workload: str
    recipe: str
    mode: str
    spark: str
    table_format_version: str
    tree: str
    runs: tuple[str, ...]

    @property
    def key(self) -> ValidationKey:
        return (self.workload, self.recipe, self.mode, self.spark, self.table_format_version)


def canonical_mode(mode: object) -> str:
    """``batch`` or ``continuous``; ``sustained`` is the legacy spelling."""
    value = getattr(mode, "value", mode)
    value = str(value) if value is not None else "batch"
    return "continuous" if value in ("continuous", "sustained") else value


def workloads() -> tuple[str, ...]:
    from lakebench.config.schema import WORKLOAD_TABLE_FORMATS

    return tuple(WORKLOAD_TABLE_FORMATS)


def recipe_names() -> list[str]:
    """Every recipe name, without the ``default`` alias."""
    from lakebench.config.recipes import RECIPES

    return sorted(n for n in RECIPES if n != "default")


def components_of(recipe: str) -> tuple[str, str, str, str] | None:
    """(catalog, table_format, pipeline_engine, query_engine) of a recipe."""
    from lakebench.config.recipes import RECIPES

    r = RECIPES.get(recipe)
    if r is None:
        return None
    arch = r.get("architecture") or {}
    return (
        str((arch.get("catalog") or {}).get("type")),
        str((arch.get("table_format") or {}).get("type")),
        str(arch.get("pipeline_engine") or "spark"),
        str((arch.get("query_engine") or {}).get("type")),
    )


def recipe_for(
    catalog: str, table_format: str, pipeline_engine: str, query_engine: str
) -> str | None:
    """The recipe name for a component tuple, or None when no recipe has it."""
    want = (catalog, table_format, pipeline_engine, query_engine)
    for name in recipe_names():
        if components_of(name) == want:
            return name
    return None


def compatibility_problem(
    workload: str,
    catalog: str,
    table_format: str,
    pipeline_engine: str,
    query_engine: str,
    mode: str,
) -> str:
    """Why layers 1 to 3 refuse this combination, or '' when they pass."""
    from lakebench.config.schema import (
        _SUPPORTED_COMBINATIONS,
        WORKLOAD_TABLE_FORMATS,
        explain_combination,
        workload_compatibility_problem,
    )

    combo = (catalog, table_format, pipeline_engine, query_engine)
    if combo not in _SUPPORTED_COMBINATIONS:
        why = explain_combination(*combo)
        return (
            f"catalog={catalog}, table_format={table_format}, engine={pipeline_engine}, "
            f"query_engine={query_engine} is not a supported architecture"
            + (f": {why}" if why else ".")
        )
    if workload not in WORKLOAD_TABLE_FORMATS:
        return f"workload {workload!r} declares no supported architecture."
    return workload_compatibility_problem(workload, table_format, canonical_mode(mode))


# ---------------------------------------------------------------------------
# Component versions: the Spark minor and table format version of a key
# ---------------------------------------------------------------------------


#: The image repository release validation runs use, after any registry
#: prefix (a mirror of it is the same image name), and the tag suffixes the
#: job builder strips (job._parse_spark_major_minor).
SPARK_REPOSITORY = "apache/spark"
_SPARK_TAG = re.compile(
    r"^(\d+)\.(\d+)\.\d+(?:-python3|-java21|-java17|-java11|-scala2\.12|-scala2\.13)*$"
)


def spark_minor(image: object) -> str | None:
    """``"4.1"`` for ``apache/spark:4.1.1-python3``: the Spark minor of an
    ``apache/spark`` image (under any registry prefix) whose tag is a plain
    release version with the job builder's suffixes. None for anything
    else (another repository, a custom tag such as
    ``4.1.1-python3-patched``, any digest reference, which the job builder
    cannot read a version from either): its Spark build is not the one
    validated, so a run on it is never stamped supported."""
    if not isinstance(image, str) or not image or "@" in image:
        return None
    repo, sep, tag = image.rpartition(":")
    if not sep or "/" in tag:
        return None
    if not (repo == SPARK_REPOSITORY or repo.endswith("/" + SPARK_REPOSITORY)):
        return None
    m = _SPARK_TAG.match(tag)
    return f"{int(m.group(1))}.{int(m.group(2))}" if m else None


def format_version_problem(spark: str, table_format: str, version: str) -> str:
    """Why *version* of *table_format* cannot run on Spark minor *spark*
    (the job builder's compatibility table), or ''."""
    from lakebench.modules.pipeline_engines.spark.job import resolve_format_version

    try:
        resolve_format_version(f"apache/spark:{spark}.0", table_format, version)
    except ValueError as e:
        return str(e)
    return ""


def resolved_format_version(cfg: Any) -> str | None:
    """The table format version a config's Spark jobs use
    (``job.resolve_format_version`` over the config's Spark image and
    requested version), or None when it does not resolve. The run record's
    ``experiment.architecture.table_format.version`` is this value."""
    arch = cfg.architecture
    table_format = arch.table_format.type.value
    try:
        from lakebench.modules.pipeline_engines.spark.job import resolve_format_version

        requested = (
            arch.table_format.iceberg.version
            if table_format == "iceberg"
            else arch.table_format.delta.version
        )
        return resolve_format_version(cfg.images.spark, table_format, requested) or None
    except Exception:  # noqa: BLE001 -- recorded as unknown, never raised
        return None


def config_versions(cfg: Any) -> tuple[str | None, str | None]:
    """(Spark minor, table format version) a loaded config runs: the same
    reading ``record_versions`` makes of the run record, which holds the
    config's Spark image and ``resolved_format_version``."""
    return spark_minor(cfg.images.spark), resolved_format_version(cfg)


def record_versions(record: Mapping[str, Any]) -> tuple[str | None, str | None]:
    """(Spark minor, table format version) a run record says it ran:
    ``experiment.architecture.pipeline_engine.image`` and
    ``.table_format.version``."""
    from lakebench.metrics.experiment import experiment_of

    exp = experiment_of(dict(record))
    arch = (exp or {}).get("architecture") if isinstance(exp, Mapping) else None
    if not isinstance(arch, Mapping):
        return None, None
    engine = arch.get("pipeline_engine")
    fmt = arch.get("table_format")
    image = engine.get("image") if isinstance(engine, Mapping) else None
    version = fmt.get("version") if isinstance(fmt, Mapping) else None
    return spark_minor(image), version if isinstance(version, str) and version else None


def validation_key_of(record: Mapping[str, Any]) -> ValidationKey | None:
    """The validation key a run record belongs to, or None when the record
    does not name every part of it."""
    from lakebench.metrics.experiment import experiment_of

    exp = experiment_of(dict(record))
    if not isinstance(exp, Mapping):
        return None
    workload = (exp.get("workload") or {}).get("name")
    recipe = (exp.get("architecture") or {}).get("recipe")
    spark, version = record_versions(record)
    if not (isinstance(workload, str) and isinstance(recipe, str) and spark and version):
        return None
    return (workload, recipe, canonical_mode(exp.get("mode")), spark, version)


def versions_label(spark: str, table_format: str, version: str) -> str:
    """``Spark 4.1, Iceberg 1.11.0``."""
    return f"Spark {spark}, {_FORMAT_LABELS.get(table_format, table_format)} {version}"


def matrix_versions_problem(
    workload: str, mode: str, recipe: str, spark: str, version: str
) -> str | None:
    """Why (workload, mode, recipe) at Spark *spark* and format *version*
    cannot be a validation entry, or None: it must be a release-matrix row
    (``RELEASE_MATRIX_VERSIONS``, SPEC section 11) at that
    row's versions. A valid tuple outside the matrix is published
    unverified, so an entry for it is refused rather than stamped."""
    fmt = (components_of(recipe) or ("", ""))[1]
    want = RELEASE_MATRIX_VERSIONS.get((workload, canonical_mode(mode), recipe))
    if want is None:
        return f"{workload} {recipe} {canonical_mode(mode)} is not a release-matrix row"
    if (spark, version) != want:
        return (
            f"the release matrix runs {workload} {recipe} {canonical_mode(mode)} on "
            f"{versions_label(want[0], fmt, want[1])}, not {versions_label(spark, fmt, version)}"
        )
    return None


def _as_str(entry: Mapping[str, Any], key: str, where: str) -> str:
    value = entry.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationRecordError(f"{where}: '{key}' must be a non-empty string")
    return value.strip()


def load_validation_record(path: Path | None = None) -> dict[ValidationKey, Validation]:
    """Parse the release validation record, keyed by (workload, recipe,
    mode, Spark minor, table format version).

    Refuses (ValidationRecordError) a record that lists anything layers 1 to
    3 refuse, an unknown recipe, a row without its Spark minor or table
    format version, a version pair the job builder cannot run, a row that is
    not a release-matrix row at that row's versions, a tree that is not a
    40-hex commit, an entry with no run ids, or the same key twice.
    A record that lists an unsupported combination would otherwise stamp it
    "supported".
    """
    import yaml

    p = path or VALIDATION_RECORD
    try:
        data = yaml.safe_load(p.read_text()) or {}
    except (OSError, yaml.YAMLError) as e:
        raise ValidationRecordError(f"cannot read {p.name}: {e}") from e
    if not isinstance(data, Mapping) or set(data) - {"validated"}:
        raise ValidationRecordError(f"{p.name}: the only top-level key is 'validated'")
    entries = data.get("validated") or []
    if not isinstance(entries, list):
        raise ValidationRecordError(f"{p.name}: 'validated' must be a list")
    out: dict[ValidationKey, Validation] = {}
    for i, entry in enumerate(entries):
        where = f"{p.name} entry {i + 1}"
        if not isinstance(entry, Mapping):
            raise ValidationRecordError(f"{where}: must be a mapping")
        unknown = set(entry) - set(_ENTRY_KEYS)
        if unknown:
            raise ValidationRecordError(f"{where}: unknown keys {sorted(unknown)}")
        if "spark" not in entry or "table_format_version" not in entry:
            raise ValidationRecordError(
                f"{where}: rows need spark and table_format_version since 1.7"
            )
        workload = _as_str(entry, "workload", where)
        recipe = _as_str(entry, "recipe", where)
        mode = _as_str(entry, "mode", where)
        spark = _as_str(entry, "spark", where)
        version = _as_str(entry, "table_format_version", where)
        tree = _as_str(entry, "tree", where)
        if mode not in MODES:
            raise ValidationRecordError(f"{where}: mode must be one of {', '.join(MODES)}")
        comps = components_of(recipe) if recipe != "default" else None
        if comps is None:
            raise ValidationRecordError(
                f"{where}: {recipe!r} is not a recipe name (the 'default' alias is not accepted)"
            )
        problem = compatibility_problem(workload, *comps, mode)
        if problem:
            raise ValidationRecordError(
                f"{where}: {workload} x {recipe} x {mode} is refused: {problem}"
            )
        if not _SPARK_MINOR.match(spark):
            raise ValidationRecordError(f"{where}: spark must be a Spark minor such as 4.1")
        version_problem = format_version_problem(
            spark, comps[1], version
        ) or matrix_versions_problem(workload, mode, recipe, spark, version)
        if version_problem:
            raise ValidationRecordError(f"{where}: {version_problem}")
        if not _SHA40.match(tree):
            raise ValidationRecordError(f"{where}: tree must be the 40-hex freeze commit")
        runs = entry.get("runs")
        if (
            not isinstance(runs, list)
            or not runs
            or not all(isinstance(r, str) and r.strip() for r in runs)
        ):
            raise ValidationRecordError(f"{where}: 'runs' must list at least one run id")
        v = Validation(workload, recipe, mode, spark, version, tree, tuple(r.strip() for r in runs))
        if v.key in out:
            raise ValidationRecordError(
                f"{where}: {workload} x {recipe} x {mode} on "
                f"{versions_label(spark, comps[1], version)} is listed twice"
            )
        out[v.key] = v
    return out


def validations_for(
    record: Mapping[ValidationKey, Validation], workload: str, recipe: str, mode: str
) -> list[Validation]:
    """Every entry of one workload x recipe x mode, whatever its versions,
    sorted by version pair."""
    return sorted(
        (v for k, v in record.items() if k[:3] == (workload, recipe, mode)),
        key=lambda v: (v.spark, v.table_format_version),
    )


def local_problem(workload: str | None, mode: object) -> str:
    """Why ``--local`` cannot run this workload x mode, or ''. Local mode's
    job map, datagen arguments and benchmark tables are Customer 360 batch
    only; anything else would run Customer 360 batch work under its label."""
    wl, m = str(workload), canonical_mode(mode)
    if wl != "customer360":
        return (
            f"Local mode runs the Customer 360 workload only, config requests {wl!r}. "
            "Run it on a cluster instead."
        )
    if m != "batch":
        return f"Local mode runs batch mode only, this run asks for {m}."
    return ""


def support_state(
    workload: str | None,
    catalog: str | None,
    table_format: str | None,
    pipeline_engine: str | None,
    query_engine: str | None,
    mode: object,
    *,
    system: str = "cluster",
    record: Mapping[ValidationKey, Validation] | None = None,
    provenance: Mapping[str, Any] | None = None,
    scale: float | None = None,
    spark: str | None = None,
    table_format_version: str | None = None,
) -> dict[str, Any]:
    """The DESIGN 6.5 support state of one workload x architecture x mode
    at one Spark minor (*spark*) and table format version.

    Returns ``{"state", "basis", ...}``. ``supported`` only when the release
    validation record lists the combination at these versions with run ids
    and lakebench is not running from a modified tree (*provenance*
    ``git_dirty``); without both versions nothing is supported, and an
    unreadable record never promotes anything. With *scale*, a cluster run
    outside the workload's datagen scale band is refused above the ceiling
    and at most unverified above the supported maximum
    (``DATAGEN_SCALE_BANDS``).
    """
    out = _support_state(
        workload,
        catalog,
        table_format,
        pipeline_engine,
        query_engine,
        mode,
        system=system,
        record=record,
        provenance=provenance,
        versions=(spark, table_format_version),
    )
    band = None if system == "local" else datagen_scale_problem(workload, scale)
    if band is None or out["state"] == UNSUPPORTED:
        return out
    state, basis = band
    out["scale_note"] = basis
    if state == UNSUPPORTED or out["state"] == SUPPORTED:
        for k in ("validation_runs", "validation_tree"):
            out.pop(k, None)
        out.update(state=state, basis=basis)
    return out


def _support_state(
    workload: str | None,
    catalog: str | None,
    table_format: str | None,
    pipeline_engine: str | None,
    query_engine: str | None,
    mode: object,
    *,
    system: str,
    record: Mapping[ValidationKey, Validation] | None,
    provenance: Mapping[str, Any] | None,
    versions: tuple[str | None, str | None],
) -> dict[str, Any]:
    wl, m = str(workload), canonical_mode(mode)
    comps = (str(catalog), str(table_format), str(pipeline_engine), str(query_engine))
    out: dict[str, Any] = {"workload": wl, "mode": m}
    note = MODE_NOTES.get((wl, m))
    if note:
        out["mode_note"] = note
    if system == "local":
        # --local swaps in a hadoop catalog and DuckDB whatever the config
        # names, and runs Customer 360 batch only; release validation covers
        # cluster runs only.
        problem = local_problem(wl, m)
        if problem:
            out.update(state=UNSUPPORTED, basis=problem)
        else:
            out.update(
                state=UNVERIFIED,
                basis="local mode: release validation covers cluster runs only",
            )
        return out
    problem = compatibility_problem(wl, *comps, m)
    if problem:
        out.update(state=UNSUPPORTED, basis=problem)
        return out
    recipe = recipe_for(*comps)
    out["recipe"] = recipe
    if record is None:
        try:
            record = load_validation_record()
        except ValidationRecordError as e:
            out.update(state=UNVERIFIED, basis=f"release validation record unreadable ({e})")
            return out
    spark, version = versions
    v = record.get((wl, str(recipe), m, str(spark), str(version))) if spark and version else None
    listed = validations_for(record, wl, str(recipe), m)
    if v is None and listed:
        validated = "; ".join(
            versions_label(e.spark, comps[1], e.table_format_version) for e in listed
        )
        ran = (
            versions_label(spark, comps[1], version)
            if spark and version
            else "a Spark minor or table format version that is not known"
        )
        out.update(
            state=UNVERIFIED,
            basis=f"validated on {validated} only; this run uses {ran}",
        )
        return out
    if v is not None and v.runs:
        # A checkout whose status is unknown (git status failed or timed out)
        # is not proven clean; a wheel install has no sha and no status.
        if provenance and (
            provenance.get("git_dirty")
            or (provenance.get("git_sha") and provenance.get("git_dirty") is None)
        ):
            out.update(
                state=UNVERIFIED,
                basis=(
                    "listed as validated, but this lakebench ran from a modified tree or one "
                    "whose status is unknown "
                    f"(commit {provenance.get('git_sha') or 'unknown'})"
                ),
            )
            return out
        out.update(
            state=SUPPORTED,
            basis=(
                f"validated on {versions_label(v.spark, comps[1], v.table_format_version)}, "
                f"release tree {v.tree[:12]}, by {', '.join(v.runs)}"
            ),
            validation_runs=list(v.runs),
            validation_tree=v.tree,
        )
        return withdraw_if_code_changed(out, provenance)
    out.update(
        state=UNVERIFIED,
        basis=f"valid for this workload and mode; no validation run is listed in {_RECORD_NAME}",
    )
    return out


def withdraw_if_code_changed(
    support: Mapping[str, Any], provenance: Mapping[str, Any] | None
) -> dict[str, Any]:
    """*support*, made unverified when the run's end sample saw other
    lakebench code than its start (``provenance.end_sample``,
    metrics/provenance.py): the release validation then says nothing about
    the code that produced part of the run. Never promotes."""
    out = dict(support)
    end = (provenance or {}).get("end_sample")
    if (
        out.get("state") != SUPPORTED
        or not isinstance(end, Mapping)
        or not end.get("code_changed_during_run")
    ):
        return out
    for k in ("validation_runs", "validation_tree"):
        out.pop(k, None)
    out.update(
        state=UNVERIFIED,
        basis=(
            "listed as validated, but the lakebench code changed during the run "
            f"(commit {(provenance or {}).get('git_sha') or 'unknown'} at start, "
            f"{end.get('git_sha') or 'unknown'} at end)"
        ),
    )
    return out


def support_state_for_config(
    cfg: Any,
    mode: object = None,
    *,
    system: str = "cluster",
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Support state of a loaded config. *mode* overrides the config's mode
    (``run --continuous`` does not write the mode back to the config)."""
    arch = cfg.architecture
    spark, version = config_versions(cfg)
    return support_state(
        arch.workload.schema_type.value,
        arch.catalog.type.value,
        arch.table_format.type.value,
        arch.pipeline_engine.value,
        arch.query_engine.type.value,
        mode if mode is not None else arch.pipeline.mode,
        system=system,
        provenance=provenance,
        scale=float(arch.workload.datagen.get_effective_scale()),
        spark=spark,
        table_format_version=version,
    )


def _matrix_cell(
    record: Mapping[ValidationKey, Validation], recipe: str, workload: str, mode: str
) -> dict[str, Any]:
    """One cell of the support matrix. A cell is not a run, so it has no
    versions of its own: it is ``supported`` when the record lists the
    recipe x workload x mode at any version pair, and lists the pairs
    (``versions``); a run at another pair is unverified (``support_state``).
    An unverified cell says why in ``basis``."""
    comps = components_of(recipe)
    assert comps is not None
    s = support_state(workload, *comps, mode, record=record)
    row: dict[str, Any] = {"recipe": recipe, **s}
    if s["state"] == UNSUPPORTED:
        return row
    listed = validations_for(record, workload, recipe, mode)
    if listed:
        row.update(
            state=SUPPORTED,
            versions=[(v.spark, v.table_format_version) for v in listed],
            basis="validated on "
            + "; ".join(
                f"{versions_label(v.spark, comps[1], v.table_format_version)} "
                f"(release tree {v.tree[:12]}, {', '.join(v.runs)})"
                for v in listed
            )
            + "; any other Spark minor or table format version is unverified",
        )
        return row
    in_matrix = any(r[:3] == (workload, mode, recipe) for r in RELEASE_MATRIX)
    row["basis"] = (
        "in this release's validation matrix; no validation run is listed yet"
        if in_matrix
        else NOT_IN_MATRIX
    )
    return row


#: Why a valid cell outside the release matrix is unverified.
NOT_IN_MATRIX = "not in this release's validation matrix"


def support_matrix(
    record: Mapping[ValidationKey, Validation] | None = None,
) -> list[dict[str, Any]]:
    """Every recipe x workload x mode with its computed state
    (``_matrix_cell``). A record that does not load leaves every row
    unverified with the error as its basis (the same as a run's stamp),
    rather than failing the caller."""
    rows = []
    if record is None:
        try:
            record = load_validation_record()
        except ValidationRecordError as e:
            error = f"release validation record unreadable ({e})"
            for row in support_matrix({}):
                if row["state"] == UNVERIFIED:
                    row["basis"] = error
                rows.append(row)
            return rows
    for recipe in recipe_names():
        for wl in workloads():
            for m in MODES:
                rows.append(_matrix_cell(record, recipe, wl, m))
    return rows


# ---------------------------------------------------------------------------
# Docs tables (generated; tests hold the docs equal to these)
# ---------------------------------------------------------------------------

_BEGIN = "<!-- BEGIN GENERATED: {name} -->"
_REGEN = (
    "<!-- Generated from the code by `PYTHONPATH=src python3.11 -m lakebench.config.support .`; "
    "do not edit by hand. -->"
)
_END = "<!-- END GENERATED: {name} -->"

_CATALOG_LABELS = {"hive": "Hive", "polaris": "Polaris", "unity": "Unity"}
_FORMAT_LABELS = {"iceberg": "Iceberg", "delta": "Delta"}
_ENGINE_LABELS = {"spark": "Spark"}
_QUERY_LABELS = {
    "trino": "Trino",
    "spark-thrift": "Spark Thrift",
    "duckdb": "DuckDB",
    "none": "None",
}


#: The general version rule under the support table (SPEC 13 item 8).
VERSION_NOTE = (
    "A supported cell names the Spark minor and table format version its validation "
    "runs used; the same cell on any other Spark minor or format version is unverified. "
    "Spark 3.5 is unverified and gets no v1.7 features."
)


def _cell_text(row: Mapping[str, Any], fmt: str, notes: dict[str, int]) -> str:
    if row["state"] == SUPPORTED:
        pairs = "; ".join(versions_label(s, fmt, v) for s, v in row.get("versions") or [])
        return f"supported ({pairs})" if pairs else SUPPORTED
    if row["state"] == UNVERIFIED:
        n = notes.setdefault(str(row["basis"]), len(notes) + 1)
        return f"unverified [{n}]"
    return str(row["state"])


def render_support_table(record: Mapping[ValidationKey, Validation] | None = None) -> str:
    """Markdown: one row per recipe, one column per workload x mode. A
    supported cell lists its validated version pairs; an unverified cell
    carries a note number, and each distinct reason is written once under
    the table."""
    rows = support_matrix(record)
    by_cell = {(r["recipe"], r["workload"], r["mode"]): r for r in rows}
    cols = [(wl, m) for wl in workloads() for m in MODES]
    head = "| Recipe | " + " | ".join(f"{WORKLOAD_LABELS[wl]} {m}" for wl, m in cols) + " |"
    sep = "|---|" + "---|" * len(cols)
    lines = [head, sep]
    notes: dict[str, int] = {}
    for recipe in recipe_names():
        fmt = (components_of(recipe) or ("", ""))[1]
        cells = [_cell_text(by_cell[(recipe, wl, m)], fmt, notes) for wl, m in cols]
        lines.append(f"| `{recipe}` | " + " | ".join(cells) + " |")
    lines.append("")
    for basis, n in notes.items():
        lines.append(f"- [{n}] unverified: {basis}.")
    refused: dict[tuple[str, str], list[str]] = {}
    for r in rows:
        if r["state"] == UNSUPPORTED:
            recipes = refused.setdefault((r["workload"], r["basis"]), [])
            if r["recipe"] not in recipes:
                recipes.append(r["recipe"])
    for (wl, basis), recipes in sorted(refused.items()):
        names = ", ".join(f"`{n}`" for n in recipes)
        lines.append(
            f"- **unsupported**, refused at config load: {WORKLOAD_LABELS[wl]} on {names}. {basis}"
        )
    lines.append(
        "- Any catalog, table format and query engine combination that is not a recipe "
        "above is refused at config load for every workload."
    )
    lines.append(f"- {VERSION_NOTE}")
    for (wl, m), note in sorted(MODE_NOTES.items()):
        lines.append(f"- {WORKLOAD_LABELS[wl]} {m}: {note}")
    return "\n".join(lines)


def render_recipe_table() -> str:
    """Markdown: every recipe with its four components."""
    lines = [
        "| Recipe | Catalog | Table Format | Pipeline Engine | Query Engine |",
        "|---|---|---|---|---|",
    ]
    for recipe in recipe_names():
        c, t, e, q = components_of(recipe) or ("?", "?", "?", "?")
        lines.append(
            f"| `{recipe}` | {_CATALOG_LABELS.get(c, c)} | {_FORMAT_LABELS.get(t, t)} | "
            f"{_ENGINE_LABELS.get(e, e)} | {_QUERY_LABELS.get(q, q)} |"
        )
    return "\n".join(lines)


GENERATED_BLOCKS: dict[str, Callable[[], str]] = {
    "support-states": render_support_table,
    "recipe-components": render_recipe_table,
}

#: docs file (relative to the repo root) -> generated blocks it carries.
DOCS_WITH_BLOCKS: dict[str, tuple[str, ...]] = {
    "docs/compatibility-matrix.md": ("recipe-components", "support-states"),
    "docs/recipes.md": ("support-states",),
    "docs/architecture.md": ("recipe-components",),
    "docs/supported-components.md": ("recipe-components",),
    "README.md": ("support-states",),
}


def expected_block(name: str) -> str:
    return "\n".join(
        [
            _BEGIN.format(name=name),
            _REGEN,
            "",
            GENERATED_BLOCKS[name](),
            "",
            _END.format(name=name),
        ]
    )


def block_in(text: str, name: str) -> str | None:
    """The generated block *name* as it appears in *text*, or None."""
    begin, end = _BEGIN.format(name=name), _END.format(name=name)
    i = text.find(begin)
    j = text.find(end, i + 1) if i >= 0 else -1
    if i < 0 or j < 0:
        return None
    return text[i : j + len(end)]


def regenerate_docs(root: Path) -> list[str]:
    """Rewrite every generated block under *root*; return the files changed."""
    changed = []
    for rel, names in DOCS_WITH_BLOCKS.items():
        p = root / rel
        text = p.read_text()
        new = text
        for name in names:
            cur = block_in(new, name)
            if cur is None:
                raise ValueError(f"{rel}: no generated block {name!r} to update")
            new = new.replace(cur, expected_block(name))
        if new != text:
            p.write_text(new)
            changed.append(rel)
    return changed
