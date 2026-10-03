#!/usr/bin/env python3.11
"""Row-hash parity of the AML silver tables between a batch deployment and a
drained continuous deployment of the same corpus.

Usage::

    python3.11 scripts/release/silver_parity.py BATCH_CFG CONTINUOUS_CFG \\
        --batch-record PATH --continuous-record PATH [--json OUT]

Both configs are AML (financial) deployments that generated the same corpus
(same seed and scale, read from their run records). The continuous record
must show a drained corpus: ``pipeline_benchmark.corpus_drained`` true (all
generated rows reached bronze and silver committed all of them) and
``continuous.drain.state`` ``drained``; otherwise the tool refuses (exit 2).

For each of the six silver tables the tool runs, on both deployments,
``lakebench query CFG --json --sql "SELECT count(*), to_hex(checksum(ROW(...)))
..."`` through Trino (``checksum`` is order-insensitive). The columns are the
table's business columns, read from the silver DDL constants of
``src/lakebench/deploy/financial_ddl.py`` by parsing the file (no list is kept
here), minus:

* the per-run sentinels of the batch-version protocol (``SENTINELS``);
* ``PER_MODE``: columns whose value depends on the mode by design;
* the DOUBLE columns of ``silver_entity_profiles``, which the continuous
  path accumulates by merging (Welford) and so differ in the last bits: each
  is compared as ``sum(col)`` within a relative tolerance of 1e-9, the
  tolerance the Spark-tier parity tests use. Other DOUBLE columns are hashed.

``silver_batch_versions`` is reported by row count only: batch writes one
version, continuous one per committed micro-batch, so the counts differ by
design and are not compared. When a table differs, a second query
checksums each column alone and the differing columns are named.

Exit 0 when every compared table is equal, 1 when any differs, 2 on a
refusal or usage error, 4 when a query fails. ``query`` writes no record.
"""

from __future__ import annotations

import argparse
import ast
import json
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
#: Per-run sentinels of the batch-version protocol: which write produced a
#: row, never what the row says.
SENTINELS = frozenset({"_batch_id", "_stream_id", "ingest_ts", "committed_at"})
#: Columns whose value depends on the mode by design, with the reason.
PER_MODE = {
    "profile_updated_ts": (
        "batch stamps the data-clock date; continuous the latest transaction time it merged"
    ),
}
REL_TOL = 1e-9
#: Tables whose DOUBLE columns are merged incrementally in continuous mode.
MERGED_TABLES = frozenset({"silver_entity_profiles"})

EXIT_OK, EXIT_DIFFERENT, EXIT_REFUSED, EXIT_QUERY = 0, 1, 2, 4


class Refused(Exception):
    """Printed, exit 2."""


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
    hashed: tuple[str, ...]
    summed: tuple[str, ...]
    excluded: tuple[str, ...]


def table_specs(path: Path = DDL_FILE) -> list[TableSpec]:
    ddls = ddl_strings(path)
    specs = []
    for key in SILVER_KEYS:
        hashed, summed, excluded = [], [], []
        for name, ctype in ddl_columns(ddls[key]):
            if name in SENTINELS or name in PER_MODE:
                excluded.append(name)
            elif ctype == "DOUBLE" and key in MERGED_TABLES:
                summed.append(name)
            else:
                hashed.append(name)
        specs.append(TableSpec(key, tuple(hashed), tuple(summed), tuple(excluded)))
    return specs


# -- queries -------------------------------------------------------------------


def table_sql(table: str, spec: TableSpec) -> str:
    sums = "".join(f", sum({c})" for c in spec.summed)
    row = ", ".join(spec.hashed)
    return f"SELECT count(*), to_hex(checksum(ROW({row}))){sums} FROM {table}"


def column_sql(table: str, spec: TableSpec) -> str:
    cols = ", ".join(f"to_hex(checksum({c}))" for c in spec.hashed)
    return f"SELECT {cols} FROM {table}"


Querier = Callable[[Path, str], list[str]]


def lakebench_query(config: Path, sql: str) -> list[str]:
    """The first row of ``lakebench query CONFIG --json --sql SQL`` (this
    tree's lakebench), as strings; RuntimeError when it fails."""
    env = {k: v for k, v in os.environ.items() if k not in ("PYTHONPATH", "PYTHONHOME")}
    env["PYTHONPATH"] = str(TREE / "src")
    out = subprocess.run(
        [sys.executable, "-m", "lakebench", "query", str(config), "--json", "--sql", sql],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(config.parent),
        check=False,
        timeout=1800,
    )
    if out.returncode != 0:
        raise RuntimeError(
            f"query on {config} exited {out.returncode}: {out.stderr.strip()[-400:]}"
        )
    start = out.stdout.find("{")
    doc, _ = json.JSONDecoder().raw_decode(out.stdout[start:])
    rows = (doc.get("data") or {}).get("rows") or []
    if not rows:
        raise RuntimeError(f"query on {config} returned no row")
    return [str(c) for c in rows[0]]


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


def _corpus(record: dict[str, Any]) -> tuple[Any, Any, Any]:
    corpus = ((record.get("experiment") or {}).get("corpus")) or {}
    workload = ((record.get("experiment") or {}).get("workload") or {}).get("name")
    return workload, corpus.get("seed"), corpus.get("scale")


def record_problems(batch: dict[str, Any], cont: dict[str, Any]) -> list[str]:
    """Why the two records cannot be compared (empty when they can)."""
    problems = []
    if _corpus(batch)[0] != "financial" or _corpus(cont)[0] != "financial":
        problems.append("both records must be AML (financial) runs")
    if _corpus(batch)[1:] != _corpus(cont)[1:] or None in _corpus(batch)[1:]:
        problems.append(
            f"different or unknown corpora: batch seed/scale {_corpus(batch)[1:]}, "
            f"continuous {_corpus(cont)[1:]}"
        )
    bmode = (batch.get("pipeline_benchmark") or {}).get("pipeline_mode")
    cmode = (cont.get("pipeline_benchmark") or {}).get("pipeline_mode")
    if bmode != "batch":
        problems.append(f"the batch record's mode is {bmode}")
    if cmode not in ("sustained", "continuous"):
        problems.append(f"the continuous record's mode is {cmode}")
    if (cont.get("pipeline_benchmark") or {}).get("corpus_drained") is not True:
        problems.append("the continuous record does not show a drained corpus (corpus_drained)")
    drain = ((cont.get("continuous") or {}).get("drain")) or {}
    if drain.get("state") != "drained":
        problems.append(f"the continuous drain state is {drain.get('state')!r}, not 'drained'")
    return problems


# -- comparison ----------------------------------------------------------------


@dataclass
class TableResult:
    table: str
    batch: list[str]
    continuous: list[str]
    equal: bool
    notes: list[str] = field(default_factory=list)


def _close(a: str, b: str) -> bool:
    if a == b:
        return True
    try:
        x, y = float(a), float(b)
    except ValueError:
        return False
    return abs(x - y) <= REL_TOL * max(abs(x), abs(y))


def compare(
    batch_cfg: Path,
    cont_cfg: Path,
    tables: dict[str, str],
    specs: Sequence[TableSpec],
    query: Querier,
) -> tuple[list[TableResult], dict[str, tuple[str, str]]]:
    """Per-table results, and the batch-versions row counts (not compared)."""
    results = []
    for spec in specs:
        table = tables[spec.key]
        sql = table_sql(table, spec)
        b, c = query(batch_cfg, sql), query(cont_cfg, sql)
        notes = []
        equal = b[:2] == c[:2]
        for i, col in enumerate(spec.summed, start=2):
            if not _close(b[i], c[i]):
                equal = False
                notes.append(f"sum({col}) {b[i]} vs {c[i]}")
        if b[0] != c[0]:
            notes.append(f"rows {b[0]} vs {c[0]}")
        if b[1] != c[1]:
            csql = column_sql(table, spec)
            bc, cc = query(batch_cfg, csql), query(cont_cfg, csql)
            notes.append(
                "columns differing: "
                + ", ".join(col for col, x, y in zip(spec.hashed, bc, cc, strict=True) if x != y)
            )
        results.append(TableResult(table, b, c, equal, notes))
    vt = tables[VERSIONS_KEY]
    versions = (
        query(batch_cfg, f"SELECT count(*) FROM {vt}")[0],
        query(cont_cfg, f"SELECT count(*) FROM {vt}")[0],
    )
    return results, {vt: versions}


def config_tables(config: Path) -> dict[str, str]:
    """{table key: catalog.namespace.table} from the config (this tree)."""
    sys.path.insert(0, str(TREE / "src"))
    from lakebench.config import load_config
    from lakebench.config._load_context import LoadPurpose

    cfg = load_config(config, purpose=LoadPurpose.READ, print_notes=False)
    catalog = cfg.architecture.query_engine.trino.catalog_name
    names = cfg.architecture.tables
    return {k: f"{catalog}.{getattr(names, k)}" for k in (*SILVER_KEYS, VERSIONS_KEY)}


def main(argv: Sequence[str] | None = None, query: Querier = lakebench_query) -> int:
    p = argparse.ArgumentParser(prog="silver_parity.py", description=__doc__.split("\n\n")[0])
    p.add_argument("batch_config", type=Path)
    p.add_argument("continuous_config", type=Path)
    p.add_argument("--batch-record", type=Path, required=True)
    p.add_argument("--continuous-record", type=Path, required=True)
    p.add_argument("--json", type=Path, help="also write the results as JSON here")
    args = p.parse_args(argv)
    try:
        problems = record_problems(_load(args.batch_record), _load(args.continuous_record))
        if problems:
            raise Refused("; ".join(problems))
        specs = table_specs()
        tables = config_tables(args.batch_config)
        if config_tables(args.continuous_config) != tables:
            raise Refused("the two configs name different silver tables")
    except Refused as e:
        print(f"refused: {e}", file=sys.stderr)
        return EXIT_REFUSED
    try:
        results, versions = compare(args.batch_config, args.continuous_config, tables, specs, query)
    except RuntimeError as e:
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
