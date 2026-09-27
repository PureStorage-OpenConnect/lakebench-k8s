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
``lakebench config show``, ``lakebench config recipes``, the report, compare
and the docs tables all read it from here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SUPPORTED = "supported"
UNVERIFIED = "unverified"
UNSUPPORTED = "unsupported"

MODES: tuple[str, ...] = ("batch", "continuous")

#: The release validation record. Package data: shipped in the wheel.
VALIDATION_RECORD = Path(__file__).with_name("validated_combinations.yaml")
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


class ValidationRecordError(ValueError):
    """The release validation record is malformed or lists a combination
    that is not valid for its workload and mode."""


@dataclass(frozen=True)
class Validation:
    workload: str
    recipe: str
    mode: str
    tree: str
    runs: tuple[str, ...]


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


def _as_str(entry: Mapping[str, Any], key: str, where: str) -> str:
    value = entry.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationRecordError(f"{where}: '{key}' must be a non-empty string")
    return value.strip()


def load_validation_record(path: Path | None = None) -> dict[tuple[str, str, str], Validation]:
    """Parse the release validation record, keyed by (workload, recipe, mode).

    Refuses (ValidationRecordError) a record that lists anything layers 1 to
    3 refuse, an unknown recipe, an entry with no run ids, or the same
    combination twice. A record that lists an unsupported combination would
    otherwise stamp it "supported".
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
    out: dict[tuple[str, str, str], Validation] = {}
    for i, entry in enumerate(entries):
        where = f"{p.name} entry {i + 1}"
        if not isinstance(entry, Mapping):
            raise ValidationRecordError(f"{where}: must be a mapping")
        unknown = set(entry) - {"workload", "recipe", "mode", "tree", "runs"}
        if unknown:
            raise ValidationRecordError(f"{where}: unknown keys {sorted(unknown)}")
        workload = _as_str(entry, "workload", where)
        recipe = _as_str(entry, "recipe", where)
        mode = _as_str(entry, "mode", where)
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
        runs = entry.get("runs")
        if (
            not isinstance(runs, list)
            or not runs
            or not all(isinstance(r, str) and r.strip() for r in runs)
        ):
            raise ValidationRecordError(f"{where}: 'runs' must list at least one run id")
        key = (workload, recipe, mode)
        if key in out:
            raise ValidationRecordError(f"{where}: {workload} x {recipe} x {mode} is listed twice")
        out[key] = Validation(workload, recipe, mode, tree, tuple(r.strip() for r in runs))
    return out


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
    record: Mapping[tuple[str, str, str], Validation] | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The DESIGN 6.5 support state of one workload x architecture x mode.

    Returns ``{"state", "basis", ...}``. ``supported`` only when the release
    validation record lists the combination with run ids and lakebench is not
    running from a modified tree (*provenance* ``git_dirty``); an unreadable
    record never promotes anything.
    """
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
    v = record.get((wl, str(recipe), m))
    if v is not None and v.runs:
        if provenance and provenance.get("git_dirty"):
            out.update(
                state=UNVERIFIED,
                basis=(
                    "listed as validated, but this lakebench ran from a modified tree "
                    f"({provenance.get('git_sha') or 'unknown commit'} with local changes)"
                ),
            )
            return out
        out.update(
            state=SUPPORTED,
            basis=f"validated on release tree {v.tree} by {', '.join(v.runs)}",
            validation_runs=list(v.runs),
            validation_tree=v.tree,
        )
        return out
    out.update(
        state=UNVERIFIED,
        basis=f"valid for this workload and mode; no validation run is listed in {_RECORD_NAME}",
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
    return support_state(
        arch.workload.schema_type.value,
        arch.catalog.type.value,
        arch.table_format.type.value,
        arch.pipeline_engine.value,
        arch.query_engine.type.value,
        mode if mode is not None else arch.pipeline.mode,
        system=system,
        provenance=provenance,
    )


def support_matrix(
    record: Mapping[tuple[str, str, str], Validation] | None = None,
) -> list[dict[str, Any]]:
    """Every recipe x workload x mode with its computed state. A record that
    does not load leaves every row unverified with the error as its basis
    (the same as a run's stamp), rather than failing the caller."""
    rows = []
    if record is None:
        try:
            record = load_validation_record()
        except ValidationRecordError as e:
            record = {}
            error = f"release validation record unreadable ({e})"
            for row in support_matrix(record):
                if row["state"] == UNVERIFIED:
                    row["basis"] = error
                rows.append(row)
            return rows
    for recipe in recipe_names():
        comps = components_of(recipe)
        assert comps is not None
        for wl in workloads():
            for m in MODES:
                s = support_state(wl, *comps, m, record=record)
                rows.append({"recipe": recipe, **s})
    return rows


# ---------------------------------------------------------------------------
# Docs tables (generated; tests hold the docs equal to these)
# ---------------------------------------------------------------------------

_BEGIN = "<!-- BEGIN GENERATED: {name} -->"
_REGEN = (
    "<!-- Generated from the code by `python3.11 -m lakebench.config.support .`; "
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


def render_support_table(record: Mapping[tuple[str, str, str], Validation] | None = None) -> str:
    """Markdown: one row per recipe, one column per workload x mode."""
    rows = support_matrix(record)
    cells = {(r["recipe"], r["workload"], r["mode"]): r["state"] for r in rows}
    cols = [(wl, m) for wl in workloads() for m in MODES]
    head = "| Recipe | " + " | ".join(f"{WORKLOAD_LABELS[wl]} {m}" for wl, m in cols) + " |"
    sep = "|---|" + "---|" * len(cols)
    lines = [head, sep]
    for recipe in recipe_names():
        lines.append(
            f"| `{recipe}` | " + " | ".join(cells[(recipe, wl, m)] for wl, m in cols) + " |"
        )
    lines.append("")
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


GENERATED_BLOCKS = {
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


if __name__ == "__main__":  # python -m lakebench.config.support <repo root>
    import sys

    for f in regenerate_docs(Path(sys.argv[1] if len(sys.argv) > 1 else ".")):
        print(f"updated {f}")
