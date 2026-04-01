---
name: snapshot
description: "Generate exhaustive codebase snapshot for release scoping. Use when starting a new dev cycle."
disable-model-invocation: true
argument-hint: "[version, e.g. 1.4]"
---

# Codebase Snapshot Generator

Generate `dev-artifacts/SNAPSHOT-v$ARGUMENTS.md` -- an exhaustive, mechanical
audit of the entire codebase. This document is the user's ONLY window into the
code when they design the release spec in Claude Desktop (no file access there).

**2,000+ lines is expected. Err on too much detail, never too little.**
**Copy actual code verbatim -- never say "see file X".**

## Required Sections

Work through each section below. Use `wc -l` for all LOC counts. Copy actual
code for signatures, class definitions, dicts, and templates.

### 1. File Inventory with LOC

Run `wc -l` on every `.py` file in `src/lakebench/`. Group by package. Include
`tests/` file list with LOC. No estimates anywhere.

Format:
```
src/lakebench/cli/__init__.py    4950
src/lakebench/cli/_sustained.py  1279
...
```

### 2. Public API Surface per Module

For each package under `src/lakebench/`, list every exported function and class
with their **full signatures** (parameters, types, return types). Include
docstrings if they exist. If a module has `__all__`, use that. Otherwise list
everything not prefixed with `_`.

### 3. Config Schema (Full -- Verbatim)

Copy every Pydantic model in `config/schema.py` with all fields, types,
defaults, validators, and `model_validator` methods. Copy the actual class
definitions -- the user needs to see field names, Optional types, default
values, and validation logic to design config changes.

Also include `config/loader.py` public functions and the env var substitution
logic.

### 4. Recipe and Compatibility Matrix (Verbatim)

Copy the actual contents of these data structures:
- `_SUPPORTED_COMBINATIONS` tuple list
- `_FORMAT_VERSION_COMPAT` dict
- `_ICEBERG_RUNTIME_SUFFIX` dict
- `_JOB_PROFILES` dict
- `_SCALE_FACTORS` or equivalent
- Any other lookup tables that govern runtime behavior

### 5. Template Inventory

Every `.yaml.j2` template in `templates/` with:
- Which deployer renders it
- All Jinja2 variables used
- Full content for templates under 50 lines
- Structure summary + variable list for larger templates

### 6. CLI Command Tree

Every Typer command and subcommand with full parameter signatures (flags,
types, defaults, help text). Include the command grouping structure.

### 7. Key Internal Data Structures (Verbatim)

Copy full class/dataclass definitions for:
- `StageMetrics`
- `PipelineBenchmark`
- `DeploymentResult`
- `QueryExecutorResult`
- Protocol definitions (`CatalogModule`, `QueryEngineModule`, `PipelineEngineModule`, `TableFormatModule`)
- Any other dataclasses/protocols used at module boundaries

### 8. Deploy and Destroy Ordering

The exact sequence from `engine.py:deploy_all()` and `deploy/destroy.py`.
Component-by-component with conditional logic.

### 9. Open Bugs

Full table from `dev-artifacts/BUGS.md` -- open, deferred, and upstream
sections.

### 10. Carried-Forward Items

From the previous release's lessons learned review
(`dev-artifacts/v*.M-lessons-learned.md`).

### 11. Test Inventory

Test count (run `pytest --co -q` to count), deselected count, test file list
with LOC. Note test infrastructure patterns (fixtures, conftest, parametrize).

### 12. Constraints and Gotchas

The full "Things That Will Bite You" list from CLAUDE.md.

### 13. Dependency Versions

Python version, key packages with pinned versions from `pyproject.toml`.

## Output

Write the complete snapshot to `dev-artifacts/SNAPSHOT-v$ARGUMENTS.md`.
Tell the user when done so they can take it to Claude Desktop for spec writing.
