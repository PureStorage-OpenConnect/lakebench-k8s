#!/usr/bin/env python3.11
"""Row-hash parity of the AML silver tables between a batch deployment and a
drained continuous deployment of the same corpus.

Usage::

    python3.11 scripts/release/silver_parity.py BATCH_CFG CONTINUOUS_CFG \\
        --batch-record PATH --continuous-record PATH [--json OUT]

Refusals (exit 2), from the two run records and configs:

* each record must belong to its config (``deployment_name``) and the two
  deployments must differ;
* both must be successful AML (financial) runs of the same corpus: seed,
  scale, generator image, time range, dirty-data ratio, role and
  perturbation all equal;
* the batch record must be a batch run; the continuous record must show a
  drained corpus (``pipeline_benchmark.corpus_drained`` and
  ``continuous.drain.state`` ``drained``) and no ``continuous.gate_problems``;
* the continuous run must have generated with one datagen pod: the
  continuous statement path numbers entries and running balances in arrival
  order, which equals the batch order only when bronze arrives in order.

For each silver table the tool runs, on both deployments, ``lakebench query
CFG --json --sql ...`` through Trino: the row count, and an order-insensitive
``checksum`` of one hash per row, ``xxhash64`` over the row's business
columns joined as text (so a value moved between rows changes the result).
Business columns come from the silver DDL constants of
``src/lakebench/deploy/financial_ddl.py``, parsed from the file, minus the
batch-version sentinels (``SENTINELS``) and the columns that differ by mode
by design (``PER_MODE``). ``silver_counterparty_edges`` is compared through
its consumer view, one row per (source, target) with first/last times and
summed amounts and counts, because continuous mode appends one edge row per
micro-batch. The entity-profile accumulators that continuous mode merges
incrementally (``MERGED``) are compared per entity within a relative and
absolute tolerance of 1e-9, as the Spark-tier parity test does; every other
profile column is hashed exactly. ``silver_batch_versions`` is checked for
rows on both sides, and its counts are reported, not compared: batch writes
one version, continuous one per micro-batch. Every compared table must hold
rows on both sides.

Exit 0 when every table is equal, 1 when any differs, 2 on a refusal or
usage error, 4 when a query fails. ``query`` writes no record.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import os
import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

TREE = Path(__file__).resolve().parents[2]
DDL_FILE = TREE / "src" / "lakebench" / "deploy" / "financial_ddl.py"

#: The silver tables compared, by their key in FINANCIAL_TABLE_DDLS (which is
#: also the config's table-name field).
SILVER_KEYS = (
    "silver",
    "silver_entities",
    "silver_accounts",
    "silver_account_statements",
    "silver_counterparty_edges",
    "silver_entity_profiles",
)
VERSIONS_KEY = "silver_batch_versions"
EDGES_KEY = "silver_counterparty_edges"
PROFILES_KEY = "silver_entity_profiles"
#: Per-run sentinels of the batch-version protocol: which write produced a
#: row, never what the row says.
SENTINELS = frozenset({"_batch_id", "_stream_id", "ingest_ts", "committed_at"})
#: Columns whose value depends on the mode by design, with the reason.
PER_MODE = {
    "profile_updated_ts": (
        "batch stamps the data-clock date; continuous the latest transaction time it merged"
    ),
}
#: Entity-profile columns continuous mode accumulates by merging (Welford and
#: derived means); compared per entity within TOL.
MERGED = ("passthrough_ratio", "avg_gap_days", "stddev_amount_usd", "avg_amount_usd", "_m2")
TOL = 1e-9
#: The edges consumer view: grouping keys, then each other column's aggregate.
EDGE_KEYS = ("source_entity_id", "target_entity_id")
EDGE_AGG = {
    "first_seen_ts": "min",
    "last_seen_ts": "max",
    "cumulative_amount_usd": "sum",
    "txn_count": "sum",
}
QUERY_TIMEOUT_S = 1500
NULL = "\\N"

EXIT_OK, EXIT_DIFFERENT, EXIT_REFUSED, EXIT_QUERY = 0, 1, 2, 4


class Refused(Exception):
    """Printed, exit 2."""


class QueryFailed(Exception):
    """Printed, exit 4."""


# -- business columns from the DDL --------------------------------------------


def _string_value(node: ast.AST) -> str | None:
    """A plain string constant, or one followed by ``.strip()``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "strip"
        and not node.args
    ):
        return _string_value(node.func.value)
    return None


def ddl_strings(path: Path = DDL_FILE) -> dict[str, str]:
    """{FINANCIAL_TABLE_DDLS key: DDL text}, by parsing *path*."""
    tree = ast.parse(path.read_text())
    consts: dict[str, str] = {}
    registry: dict[str, str] = {}
    for node in tree.body:
        target, value = None, None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign):
            target, value = node.target, node.value
        if not isinstance(target, ast.Name) or value is None:
            continue
        text = _string_value(value)
        if text is not None:
            consts[target.id] = text
        elif target.id == "FINANCIAL_TABLE_DDLS" and isinstance(value, ast.Dict):
            for k, v in zip(value.keys, value.values, strict=True):
                if isinstance(k, ast.Constant) and isinstance(v, ast.Name):
                    registry[str(k.value)] = v.id
    missing = [k for k in (*SILVER_KEYS, VERSIONS_KEY) if registry.get(k) not in consts]
    if missing:
        raise Refused(f"{path}: no DDL constant for {', '.join(missing)}")
    return {k: consts[name] for k, name in registry.items() if name in consts}


def _split_top_level(body: str) -> list[str]:
    """Split a column list on commas outside <...> and (...)."""
    parts, depth, cur = [], 0, []
    for ch in body:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur))
    return parts


def ddl_columns(ddl: str) -> list[tuple[str, str]]:
    """[(column, type)] of a CREATE TABLE statement, in order."""
    text = "\n".join(line.split("--", 1)[0] for line in ddl.splitlines())
    start = text.index("(")
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
            if depth == 0:
                body = text[start + 1 : i]
                break
    else:
        raise Refused("unbalanced CREATE TABLE")
    cols = []
    for part in _split_top_level(body):
        part = part.strip()
        if not part:
            continue
        name, _, rest = part.partition(" ")
        ctype = re.sub(r"\s+NOT\s+NULL\s*$", "", rest.strip(), flags=re.IGNORECASE).strip()
        cols.append((name.strip("`"), ctype.upper()))
    return cols


@dataclass(frozen=True)
class TableSpec:
    key: str
    #: (column, type) hashed per row, after the view (if any).
    hashed: tuple[tuple[str, str], ...]
    #: Columns compared per entity within TOL (entity profiles only).
    merged: tuple[str, ...]
    excluded: tuple[str, ...]


def table_specs(path: Path = DDL_FILE) -> list[TableSpec]:
    ddls = ddl_strings(path)
    specs = []
    for key in SILVER_KEYS:
        hashed, merged, excluded = [], [], []
        cols = ddl_columns(ddls[key])
        names = {n for n, _ in cols}
        if key == EDGES_KEY:
            business = names - SENTINELS
            unknown = business - set(EDGE_KEYS) - set(EDGE_AGG)
            if unknown or not set(EDGE_KEYS) <= business:
                raise Refused(
                    f"the edges view does not know columns {sorted(unknown)}: update EDGE_AGG"
                )
        if key == PROFILES_KEY and not set(MERGED) <= names:
            raise Refused(f"entity profiles lost merged columns {sorted(set(MERGED) - names)}")
        for name, ctype in cols:
            if name in SENTINELS or name in PER_MODE:
                excluded.append(name)
            elif key == PROFILES_KEY and name in MERGED:
                merged.append(name)
            else:
                hashed.append((name, ctype))
        specs.append(TableSpec(key, tuple(hashed), tuple(merged), tuple(excluded)))
    return specs


# -- SQL -----------------------------------------------------------------------


def text_of(col: str, ctype: str) -> str:
    """A Trino expression giving *col* as text, NULL as a marker."""
    if ctype.startswith("ARRAY"):
        inner = f"array_join({col}, chr(31), '{NULL}')"
    elif ctype.startswith("STRUCT") or ctype.startswith("MAP"):
        inner = f"json_format(CAST({col} AS JSON))"
    else:
        inner = f"CAST({col} AS VARCHAR)"
    return f"coalesce({inner}, '{NULL}')"


def source_of(table: str, spec: TableSpec) -> str:
    """The table, or for edges the consumer view over it."""
    if spec.key != EDGES_KEY:
        return table
    keys = ", ".join(EDGE_KEYS)
    aggs = ", ".join(f"{fn}({c}) AS {c}" for c, fn in EDGE_AGG.items())
    return f"(SELECT {keys}, {aggs} FROM {table} GROUP BY {keys}) v"


def row_hash(spec: TableSpec) -> str:
    parts = ", ".join(text_of(c, t) for c, t in spec.hashed)
    return f"xxhash64(to_utf8(concat_ws(chr(30), {parts})))"


def table_sql(table: str, spec: TableSpec) -> str:
    return f"SELECT count(*), to_hex(checksum({row_hash(spec)})) FROM {source_of(table, spec)}"


def column_sql(table: str, spec: TableSpec) -> str:
    """One order-insensitive checksum per column, to name what differs."""
    cols = ", ".join(
        f"to_hex(checksum(xxhash64(to_utf8({text_of(c, t)}))))" for c, t in spec.hashed
    )
    return f"SELECT {cols} FROM {source_of(table, spec)}"


def merged_sql(table: str, spec: TableSpec) -> str:
    return f"SELECT entity_id, {', '.join(spec.merged)} FROM {table}"


# -- queries -------------------------------------------------------------------

#: (config, sql) -> every result row, as strings (None for SQL NULL).
Querier = Callable[[Path, str], list[list[str | None]]]


def lakebench_query(config: Path, sql: str) -> list[list[str | None]]:
    """The rows of ``lakebench query CONFIG --json --sql SQL`` (this tree's
    lakebench); QueryFailed when it fails."""
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "PYTHONHOME")}
    env["PYTHONPATH"] = str(TREE / "src")
    argv = [sys.executable, "-m", "lakebench", "query", str(config), "--json"]
    argv += ["--timeout", str(QUERY_TIMEOUT_S), "--sql", sql]
    try:
        out = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            env=env,
            cwd=str(config.parent),
            check=False,
            timeout=QUERY_TIMEOUT_S + 300,
        )
    except subprocess.TimeoutExpired as e:
        raise QueryFailed(f"query on {config} did not return in time") from e
    if out.returncode != 0:
        raise QueryFailed(f"query on {config} exited {out.returncode}: {out.stderr.strip()[-400:]}")
    start = out.stdout.find("{")
    try:
        doc, _ = json.JSONDecoder().raw_decode(out.stdout[start:] if start >= 0 else "")
    except ValueError as e:
        raise QueryFailed(f"query on {config} printed no JSON document") from e
    rows = (doc.get("data") or {}).get("rows")
    if not isinstance(rows, list):
        raise QueryFailed(f"query on {config} returned no rows list")
    return [[None if c in (None, "", "NULL") else str(c) for c in row] for row in rows]


def _one(query: Querier, config: Path, sql: str, width: int) -> list[str | None]:
    rows = query(config, sql)
    if len(rows) != 1 or len(rows[0]) < width:
        raise QueryFailed(f"query on {config} returned {len(rows)} rows, not one of {width} cells")
    return rows[0]


# -- records -------------------------------------------------------------------


def _load(path: Path) -> dict[str, Any]:
    p = path / "metrics.json" if path.is_dir() else path
    try:
        data = json.loads(p.read_text())
    except (OSError, ValueError) as e:
        raise Refused(f"cannot read the record {p}: {e}") from e
    if not isinstance(data, dict):
        raise Refused(f"{p} is not a run record")
    return data


#: The corpus fields two runs must share to have generated the same corpus
#: (the corpus id itself differs between batch and continuous generation).
CORPUS_FIELDS = (
    "seed",
    "scale",
    "generator_image",
    "timestamp_start",
    "timestamp_end",
    "dirty_data_ratio",
    "corpus_role",
    "robustness_perturbation",
)


def _corpus(record: dict[str, Any]) -> dict[str, Any]:
    corpus = ((record.get("experiment") or {}).get("corpus")) or {}
    return {k: corpus.get(k) for k in CORPUS_FIELDS}


def record_problems(
    batch: dict[str, Any], cont: dict[str, Any], names: tuple[str, str] | None = None
) -> list[str]:
    """Why the two records cannot be compared (empty when they can)."""
    problems = []
    for side, rec in (("batch", batch), ("continuous", cont)):
        if ((rec.get("experiment") or {}).get("workload") or {}).get("name") != "financial":
            problems.append(f"the {side} record is not an AML (financial) run")
        if rec.get("success") is not True:
            problems.append(f"the {side} run did not succeed")
    if names is not None:
        for side, rec, name in (("batch", batch, names[0]), ("continuous", cont, names[1])):
            if rec.get("deployment_name") != name:
                problems.append(
                    f"the {side} record is of {rec.get('deployment_name')!r}, not {name!r}"
                )
        if names[0] == names[1]:
            problems.append("both configs name the same deployment")
    cb, cc = _corpus(batch), _corpus(cont)
    if cb != cc or cb["seed"] is None or cb["scale"] is None:
        diff = sorted(k for k in CORPUS_FIELDS if cb[k] != cc[k])
        problems.append(f"the records are not of one corpus (differ in {diff or 'seed/scale'})")
    bmode = (batch.get("pipeline_benchmark") or {}).get("pipeline_mode")
    cmode = (cont.get("pipeline_benchmark") or {}).get("pipeline_mode")
    if bmode != "batch":
        problems.append(f"the batch record's mode is {bmode}")
    if cmode not in ("sustained", "continuous"):
        problems.append(f"the continuous record's mode is {cmode}")
    if (cont.get("pipeline_benchmark") or {}).get("corpus_drained") is not True:
        problems.append("the continuous record does not show a drained corpus (corpus_drained)")
    cblock = cont.get("continuous") or {}
    drain = cblock.get("drain") or {}
    if drain.get("state") != "drained":
        problems.append(f"the continuous drain state is {drain.get('state')!r}, not 'drained'")
    if cblock.get("gate_problems"):
        problems.append(f"the continuous run has gate problems: {cblock['gate_problems']}")
    pods = ((cont.get("config_snapshot") or {}).get("datagen") or {}).get("parallelism")
    if pods != 1:
        problems.append(
            f"the continuous run generated with {pods} datagen pods; statement entry numbers "
            "and running balances match batch only with one (workload.datagen.parallelism: 1)"
        )
    return problems


# -- comparison ----------------------------------------------------------------


@dataclass
class TableResult:
    table: str
    batch: list[str | None]
    continuous: list[str | None]
    equal: bool
    notes: list[str] = field(default_factory=list)


def _close(a: str | None, b: str | None, zero_null: bool = False) -> bool:
    if zero_null:
        a, b = a or "0", b or "0"
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=TOL, abs_tol=TOL)


def _merged_notes(query: Querier, bcfg: Path, ccfg: Path, table: str, spec: TableSpec) -> list[str]:
    sql = merged_sql(table, spec)
    brows = {r[0]: r[1:] for r in query(bcfg, sql)}
    crows = {r[0]: r[1:] for r in query(ccfg, sql)}
    notes = []
    if set(brows) != set(crows):
        notes.append(f"entities differ: {len(set(brows) ^ set(crows))} only on one side")
    bad = 0
    for eid in set(brows) & set(crows):
        for i, col in enumerate(spec.merged):
            if not _close(brows[eid][i], crows[eid][i], zero_null=(col == "_m2")):
                bad += 1
                if bad <= 5:
                    notes.append(f"entity {eid} {col}: {brows[eid][i]} vs {crows[eid][i]}")
    if bad:
        notes.append(f"{bad} merged values outside {TOL}")
    return notes


def compare(
    batch_cfg: Path,
    cont_cfg: Path,
    tables: dict[str, str],
    specs: Sequence[TableSpec],
    query: Querier,
) -> tuple[list[TableResult], dict[str, tuple[str | None, str | None]]]:
    """Per-table results, and the batch-versions row counts (not compared)."""
    results = []
    for spec in specs:
        table = tables[spec.key]
        sql = table_sql(table, spec)
        b, c = _one(query, batch_cfg, sql, 2), _one(query, cont_cfg, sql, 2)
        notes = []
        empty = [side for side, row in (("batch", b), ("continuous", c)) if row[0] in (None, "0")]
        if empty:
            notes.append(f"no rows on {' and '.join(empty)}")
        equal = not empty and b[:2] == c[:2]
        if b[0] != c[0]:
            notes.append(f"rows {b[0]} vs {c[0]}")
        if b[1] != c[1] and not empty:
            csql = column_sql(table, spec)
            width = len(spec.hashed)
            bc, cc = _one(query, batch_cfg, csql, width), _one(query, cont_cfg, csql, width)
            cols = [col for (col, _t), x, y in zip(spec.hashed, bc, cc, strict=False) if x != y]
            notes.append(
                "columns differing: " + (", ".join(cols) or "none alone (rows recombined)")
            )
        if spec.merged and not empty:
            merged = _merged_notes(query, batch_cfg, cont_cfg, table, spec)
            if merged:
                equal = False
                notes += merged
        results.append(TableResult(table, b, c, equal, notes))
    vt = tables[VERSIONS_KEY]
    vb = _one(query, batch_cfg, f"SELECT count(*) FROM {vt}", 1)[0]
    vc = _one(query, cont_cfg, f"SELECT count(*) FROM {vt}", 1)[0]
    if vb in (None, "0") or vc in (None, "0"):
        results.append(TableResult(vt, [vb], [vc], False, ["no batch versions on one side"]))
    return results, {vt: (vb, vc)}


def config_tables(config: Path) -> tuple[str, dict[str, str]]:
    """(deployment name, {table key: catalog.namespace.table}) of a config."""
    if str(TREE / "src") not in sys.path:
        sys.path.insert(0, str(TREE / "src"))
    from lakebench.config import load_config
    from lakebench.config._load_context import LoadPurpose

    try:
        cfg = load_config(config, purpose=LoadPurpose.READ, print_notes=False)
    except Exception as e:  # noqa: BLE001 -- a config that does not load refuses
        raise Refused(f"{config} does not load: {e}") from e
    catalog = cfg.architecture.query_engine.trino.catalog_name
    names = cfg.architecture.tables
    return cfg.name, {k: f"{catalog}.{getattr(names, k)}" for k in (*SILVER_KEYS, VERSIONS_KEY)}


def main(argv: Sequence[str] | None = None, query: Querier = lakebench_query) -> int:
    p = argparse.ArgumentParser(prog="silver_parity.py", description=__doc__.split("\n\n")[0])
    p.add_argument("batch_config", type=Path)
    p.add_argument("continuous_config", type=Path)
    p.add_argument("--batch-record", type=Path, required=True)
    p.add_argument("--continuous-record", type=Path, required=True)
    p.add_argument("--json", type=Path, help="also write the results as JSON here")
    args = p.parse_args(argv)
    try:
        specs = table_specs()
        bname, tables = config_tables(args.batch_config)
        cname, ctables = config_tables(args.continuous_config)
        if ctables != tables:
            raise Refused("the two configs name different silver tables")
        problems = record_problems(
            _load(args.batch_record), _load(args.continuous_record), (bname, cname)
        )
        if problems:
            raise Refused("; ".join(problems))
    except Refused as e:
        print(f"refused: {e}", file=sys.stderr)
        return EXIT_REFUSED
    try:
        results, versions = compare(args.batch_config, args.continuous_config, tables, specs, query)
    except (QueryFailed, ValueError, IndexError) as e:
        print(f"query failed: {e}", file=sys.stderr)
        return EXIT_QUERY
    for r in results:
        verdict = "equal" if r.equal else "DIFFERENT"
        detail = f" ({'; '.join(r.notes)})" if r.notes else ""
        print(f"{r.table}: {verdict}, rows {r.batch[0]} / {r.continuous[0]}{detail}")
    for t, (b, c) in versions.items():
        print(f"{t}: row count {b} (batch) / {c} (continuous), not compared")
    excluded = sorted({c for s in specs for c in s.excluded})
    print(f"excluded columns: {', '.join(excluded)}")
    if args.json:
        args.json.write_text(
            json.dumps(
                {
                    "tables": [r.__dict__ for r in results],
                    "batch_versions": versions,
                    "excluded": excluded,
                },
                indent=2,
            )
        )
    return EXIT_OK if all(r.equal for r in results) else EXIT_DIFFERENT


if __name__ == "__main__":
    sys.exit(main())
