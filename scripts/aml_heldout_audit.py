#!/usr/bin/env python3
"""Held-out audit: was any protected AML corpus scored on this host?

Read-only. Searches what this host and its own deployments hold for a run on
the evaluation or robustness corpus (a protected role, or a seed that hashes
to a held-out seed), and lists every place it searched:

1. Records: every ``metrics.json`` under ``--runs-dir``, through
   ``lakebench.aml.look_guard.protected_record_reason`` (fail closed; a
   financial record with no corpus seed is listed as unidentified).
2. Journals: every ``session-*.jsonl`` under ``--journal-dir``. The config
   each session names is read raw (no validation) and checked by the same
   rule as ``generate`` and ``run``; a config that is gone is listed as
   unavailable. Every integer token in the journal is also hashed against
   the held-out seeds (``datagen_seed.absence_problems``).
3. Buckets: for each deployments-ledger row (``--ledger``) whose config
   exists, only that config's own bronze and gold buckets are listed: the
   bronze manifests (every row's ``(typology_id, seed)``, through
   ``datagen_seed.manifest_protected_reason``) and the gold
   ``scoring/*/recall.json`` files. Every S3 call goes through a client that
   refuses any bucket outside that row's config, so no foreign bucket is
   ever listed or read.
4. Local ledgers: the look record, the look ledger and the corpus ledger,
   counted by role and state.

Output: ``--out audit.json`` (``paths_searched``, ``skipped`` with reasons,
``protected_scored_runs``, ``findings``, counts) and a text summary. No seed
and no key is ever printed: reasons name roles and kinds only, and S3
credentials come from each config's ``${VAR}`` substitution and are never
logged. Exit 0 when nothing protected was found and every check ran, 1 when
a protected scored run or corpus was found, 2 when some check could not run.

Usage::

    python scripts/aml_heldout_audit.py \\
        --runs-dir lakebench-output/runs --journal-dir lakebench-output/journal \\
        --ledger ../lakebench-k8s/dev-artifacts/EVIDENCE-v1.7.md --out audit.json
"""

from __future__ import annotations

import argparse
import io
import json
import re
import sys
from collections import Counter
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from lakebench.aml import look_guard  # noqa: E402
from lakebench.config import datagen_seed as ds  # noqa: E402

#: The financial bronze datagen prefix (deploy.datagen.bronze_datagen_prefix).
FINANCIAL_PREFIX = "pacs008"


class ForeignBucket(AssertionError):
    """An S3 call named a bucket outside the ledger row's config."""


class Audit:
    """What the audit searched, skipped and found. (A plain class: the
    script is also exec'd as a module that is not in sys.modules.)"""

    def __init__(self) -> None:
        self.runs_dirs: list[str] = []
        self.journal_dirs: list[str] = []
        self.ledgers: list[str] = []
        self.bucket_prefixes: list[str] = []
        self.skipped: list[dict[str, str]] = []
        self.protected_scored_runs: list[dict[str, str]] = []
        self.findings: list[dict[str, str]] = []
        self.counts: Counter = Counter()
        self.incomplete = False

    def skip(self, what: str, reason: str, *, incomplete: bool = False) -> None:
        self.skipped.append({"what": what, "reason": reason})
        self.incomplete = self.incomplete or incomplete

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": 1,
            "paths_searched": {
                "runs_dirs": self.runs_dirs,
                "journal_dirs": self.journal_dirs,
                "ledgers": self.ledgers,
                "bucket_prefixes": self.bucket_prefixes,
            },
            "skipped": self.skipped,
            "protected_scored_runs": self.protected_scored_runs,
            "findings": self.findings,
            "counts": dict(sorted(self.counts.items())),
            "complete": not self.incomplete,
        }


def reason_kind(reason: str) -> str:
    """A short kind for a guard reason (the reasons name roles, never seeds)."""
    text = reason.lower()
    for kind, needle in (
        ("unidentified", "unidentified"),
        ("unreadable", "cannot be read"),
        ("unrecoverable", "cannot be recovered"),
        ("withheld", "withheld"),
        ("role", "corpus_role"),
        ("manifest", "manifest comes from"),
        ("seed", "seed"),
    ):
        if needle in text:
            return kind
    return "other"


# -- 1. records ---------------------------------------------------------------


def audit_records(audit: Audit, runs_dirs: Iterable[Path]) -> None:
    for d in runs_dirs:
        audit.runs_dirs.append(str(d))
        if not d.is_dir():
            audit.skip(str(d), "runs directory does not exist")
            continue
        for path in sorted(d.glob("**/metrics.json")):
            audit.counts["records"] += 1
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as e:
                audit.findings.append(
                    {"record": str(path), "kind": "unreadable", "reason": type(e).__name__}
                )
                audit.incomplete = True
                continue
            reason = look_guard.protected_record_reason(record)
            if reason is None:
                continue
            kind = reason_kind(reason)
            run_id = str(record.get("run_id") or path.parent.name)
            if kind in ("role", "seed", "withheld"):
                audit.protected_scored_runs.append(
                    {"run_id": run_id, "record": str(path), "reason_kind": kind}
                )
            else:
                # Unidentified or unreadable: it cannot be ruled out.
                audit.findings.append({"record": str(path), "kind": kind, "reason": reason})
                audit.incomplete = True


# -- 2. journals --------------------------------------------------------------


def _raw_config(path: Path) -> dict:
    """A config file read raw: no ``${VAR}`` substitution (the corpus fields
    never need one, and the audit runs without the deployment's keys), the
    flat v2 fields placed as the loader places them, nothing validated."""
    import yaml

    from lakebench.config.loader import _apply_flat_fields

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("the config is not a mapping")
    return _apply_flat_fields(data)


def raw_corpus_candidates(data: dict) -> list[tuple[str | None, Any, Any]]:
    """Every (schema, seed, corpus_role) the loader could make of ``data``:
    a top-level ``workload`` and the deprecated ``architecture.workload``
    are merged both ways round (the loader refuses a conflict, so either
    order may be the one a run used)."""
    from lakebench.config.schema import _merge_workload_blocks, _normalise_workload_block

    arch = data.get("architecture") if isinstance(data.get("architecture"), dict) else {}
    top = _normalise_workload_block(data.get("workload"))
    nested = _normalise_workload_block(arch.get("workload"))
    blocks = []
    for a, b in ((top, nested), (nested, top)):
        merged = _merge_workload_blocks(a, b, "workload", []) if a and b else (a or b)
        blocks.append(merged if isinstance(merged, dict) else {})
    out = []
    for w in blocks:
        dg = w.get("datagen") if isinstance(w.get("datagen"), dict) else {}
        fields = (w.get("schema"), dg.get("seed"), dg.get("corpus_role"))
        if fields not in out:
            out.append(fields)
    return out


def raw_corpus_fields(path: Path) -> tuple[str | None, Any, Any]:
    """The first of ``raw_corpus_candidates`` for a config file."""
    return raw_corpus_candidates(_raw_config(path))[0]


def raw_config_reason(data: dict) -> str | None:
    """``look_guard``'s config rule over a raw config: a protected role or
    held-out seed in any reading of the workload block, or (financial) a
    bronze prefix in this host's corpus ledger."""
    for schema, seed, role in raw_corpus_candidates(data):
        reason = look_guard.corpus_fields_reason(schema, seed, role)
        if reason is not None:
            return reason
        if schema == "financial":
            s3 = ((data.get("platform") or {}).get("storage") or {}).get("s3") or {}
            bronze = (s3.get("buckets") or {}).get("bronze") or f"{data.get('name')}-bronze"
            reason = look_guard.registered_prefix_reason(bronze, FINANCIAL_PREFIX)
            if reason is not None:
                return reason
    return None


def _session_config(path: Path) -> tuple[str | None, str | None]:
    """(session id, config path) from a journal's session.start event."""
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("event_type") == "session.start":
                details = event.get("details") or {}
                return event.get("session_id"), details.get("config_file")
    return None, None


def audit_journals(audit: Audit, journal_dirs: Iterable[Path]) -> None:
    for d in journal_dirs:
        audit.journal_dirs.append(str(d))
        if not d.is_dir():
            audit.skip(str(d), "journal directory does not exist")
            continue
        for path in sorted(d.glob("session-*.jsonl")):
            audit.counts["journals"] += 1
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
                session, config = _session_config(path)
            except OSError as e:
                audit.skip(str(path), f"journal unreadable ({type(e).__name__})", incomplete=True)
                continue
            try:
                hits = ds.absence_problems({path.name: text})
            except Exception as e:  # noqa: BLE001 -- the record cannot be read
                audit.skip(
                    str(path), f"held-out tokens unchecked ({type(e).__name__})", incomplete=True
                )
                hits = []
            for hit in hits:
                audit.findings.append({"journal": str(path), "kind": "seed_token", "reason": hit})
            if not config:
                audit.skip(f"session {session}", "the journal names no config")
                continue
            cfg_path = Path(config)
            if not cfg_path.is_absolute():
                # The journal stores the path as typed: relative to the
                # directory lakebench-output/ sits in.
                cfg_path = d.parent.parent / cfg_path
            if not cfg_path.is_file():
                audit.skip(f"session {session}", f"config unavailable ({config})")
                continue
            try:
                reason = raw_config_reason(_raw_config(cfg_path))
            except Exception as e:  # noqa: BLE001 -- a config that does not parse
                audit.skip(
                    f"session {session}", f"config unreadable ({type(e).__name__})", incomplete=True
                )
                continue
            if reason is not None:
                audit.findings.append(
                    {
                        "journal": str(path),
                        "session": str(session),
                        "kind": "protected_config",
                        "reason": reason,
                    }
                )


# -- 3. ledger buckets --------------------------------------------------------


class LedgerRow:
    """One deployments-ledger row: namespace, config path, where it was read."""

    def __init__(self, namespace: str, config: str, source: str) -> None:
        self.namespace, self.config, self.source = namespace, config, source


_TABLE_ROW = re.compile(r"^\|(.+)\|\s*$")
_INLINE = re.compile(r"namespace:\s*([^,\s]+)[^,]*,\s*config:\s*([^,\s]+)", re.IGNORECASE)
_BULLET = re.compile(r"^\s*(?:[-*]\s*)?(namespace|config)\s*:\s*(\S+)", re.IGNORECASE)


def parse_ledger(path: Path) -> tuple[list[LedgerRow], list[str]]:
    """Deployment rows of a deployments ledger (EVIDENCE-v1.x.md), and the
    ledger sections nothing could be parsed from. Three shapes: the table
    with ``Namespace`` and ``Config path`` columns, inline ``namespace: X,
    config: Y`` bullets, and ``- Namespace: X`` / ``- Config: Y`` bullet
    pairs (the dash optional), in any section, since ledger rows were also
    written under other headings. A section whose heading mentions the
    ledger and yields no row is returned as unparsed."""
    rows: list[LedgerRow] = []
    unparsed: list[str] = []
    heading: str | None = None
    in_ledger = False
    found_here = 0
    cols: list[str] | None = None
    pending: dict[str, str] = {}

    def close_section() -> None:
        if in_ledger and heading is not None and found_here == 0:
            unparsed.append(heading)

    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("#"):
            close_section()
            heading = line.lstrip("#").strip()
            in_ledger = "ledger" in heading.lower()
            found_here, cols, pending = 0, None, {}
            continue
        m = _TABLE_ROW.match(line)
        if m:
            cells = [c.strip() for c in m.group(1).split("|")]
            if cols is None:
                cols = [c.lower() for c in cells]
                continue
            if all(set(c) <= set("-: ") for c in cells):
                continue
            if "namespace" in cols and "config path" in cols:
                row = dict(zip(cols, cells, strict=False))
                rows.append(LedgerRow(row["namespace"], row["config path"], f"{path}: {heading}"))
                found_here += 1
            continue
        m = _INLINE.search(line)
        if m:
            rows.append(LedgerRow(m.group(1), m.group(2), f"{path}: {heading}"))
            found_here += 1
            continue
        m = _BULLET.match(line)
        if m:
            pending[m.group(1).lower()] = m.group(2)
            if "namespace" in pending and "config" in pending:
                rows.append(
                    LedgerRow(pending["namespace"], pending["config"], f"{path}: {heading}")
                )
                found_here += 1
                pending = {}
    close_section()
    return rows, unparsed


class ScopedS3:
    """An S3 client limited to one ledger row's buckets: any other bucket
    raises ForeignBucket before a request is made."""

    def __init__(self, client: Any, allowed: Iterable[str]) -> None:
        self._client = client
        self.allowed = frozenset(allowed)

    def _check(self, bucket: str) -> None:
        if bucket not in self.allowed:
            raise ForeignBucket(f"bucket {bucket!r} is not in this ledger row's config")

    def keys(self, bucket: str, prefix: str) -> list[str]:
        self._check(bucket)
        out: list[str] = []
        token: str | None = None
        while True:
            kw: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            page = self._client.list_objects_v2(**kw)
            out += [o["Key"] for o in page.get("Contents") or []]
            if not page.get("IsTruncated"):
                return out
            token = page.get("NextContinuationToken")

    def read(self, bucket: str, key: str) -> bytes:
        self._check(bucket)
        return self._client.get_object(Bucket=bucket, Key=key)["Body"].read()


def default_client_factory(s3: dict[str, Any]) -> Any:
    """A boto3 client for a config's ``platform.storage.s3`` block."""
    from lakebench.s3 import S3Client

    client = S3Client(
        endpoint=str(s3.get("endpoint") or ""),
        access_key=str(s3.get("access_key") or ""),
        secret_key=str(s3.get("secret_key") or ""),
        region=str(s3.get("region") or "us-east-1"),
        path_style=bool(s3.get("path_style", True)),
    ).raw_client
    if client is None:
        raise RuntimeError("the S3 client could not be built from the config")
    return client


def _resolve_config(row: LedgerRow, ledger: Path) -> Path | None:
    p = Path(row.config)
    if p.is_absolute():
        return p if p.is_file() else None
    for base in (ledger.parent.parent, ledger.parent, Path.cwd()):
        if (base / p).is_file():
            return base / p
    return None


def _manifest_rows(blob: bytes) -> list[tuple[Any, Any]]:
    import pyarrow.parquet as pq

    table = pq.read_table(io.BytesIO(blob), columns=["typology_id", "seed"])
    ids, seeds = table.column("typology_id").to_pylist(), table.column("seed").to_pylist()
    return list(zip(ids, seeds, strict=True))


def audit_bucket_row(
    audit: Audit, row: LedgerRow, ledger: Path, client_factory: Callable[[dict], Any]
) -> None:
    cfg_path = _resolve_config(row, ledger)
    what = f"ledger row {row.namespace} ({row.source})"
    if cfg_path is None:
        audit.skip(what, f"config unavailable ({row.config})")
        return
    try:
        from lakebench.config.loader import _apply_flat_fields, load_yaml

        data = _apply_flat_fields(load_yaml(cfg_path))
        schema, _seed, _role = raw_corpus_fields(cfg_path)
    except Exception as e:  # noqa: BLE001 -- unresolved ${VAR}, bad YAML
        audit.skip(what, f"config unreadable ({type(e).__name__})", incomplete=True)
        return
    if schema != "financial":
        audit.skip(what, "not an AML (financial) deployment")
        return
    s3 = ((data.get("platform") or {}).get("storage") or {}).get("s3") or {}
    buckets = s3.get("buckets") or {}
    name = data.get("name") or row.namespace
    bronze = buckets.get("bronze") or f"{name}-bronze"
    gold = buckets.get("gold") or f"{name}-gold"
    try:
        scoped = ScopedS3(client_factory(s3), {bronze, gold})
    except Exception as e:  # noqa: BLE001 -- never log the config's keys
        audit.skip(what, f"S3 client unavailable ({type(e).__name__})", incomplete=True)
        return
    manifest_prefix = f"{FINANCIAL_PREFIX}/manifest/"
    audit.bucket_prefixes += [f"s3://{bronze}/{manifest_prefix}", f"s3://{gold}/scoring/"]
    try:
        manifests = [
            k
            for k in scoped.keys(bronze, manifest_prefix)
            if re.fullmatch(r".*/manifest[^/]*\.parquet", k)
        ]
        recalls = [k for k in scoped.keys(gold, "scoring/") if k.endswith("/recall.json")]
    except ForeignBucket:
        raise
    except Exception as e:  # noqa: BLE001 -- gone, unreachable, refused
        audit.skip(what, f"buckets unreadable ({type(e).__name__})", incomplete=True)
        return
    audit.counts["bucket_rows"] += 1
    audit.counts["recall_files"] += len(recalls)
    if not manifests:
        if recalls:
            # Scoring files outlived the corpus they scored: unchecked.
            audit.findings.append(
                {
                    "bucket_path": f"s3://{gold}/scoring/",
                    "kind": "unverifiable_scoring",
                    "reason": f"{len(recalls)} recall.json file(s) and no bronze manifest",
                }
            )
            audit.incomplete = True
        else:
            audit.skip(what, "no bronze manifest")
        return
    rows: list[tuple[Any, Any]] = []
    try:
        for key in manifests:
            rows += _manifest_rows(scoped.read(bronze, key))
    except ForeignBucket:
        raise
    except ImportError:
        audit.skip(what, "pyarrow is not installed; manifests unchecked", incomplete=True)
        return
    except Exception as e:  # noqa: BLE001
        audit.skip(what, f"manifest unreadable ({type(e).__name__})", incomplete=True)
        return
    audit.counts["manifest_rows"] += len(rows)
    reason = ds.manifest_protected_reason(iter(rows))
    if reason is None:
        return
    kind = reason_kind(reason)
    where = f"s3://{bronze}/{manifest_prefix}"
    if kind == "manifest" and recalls:
        for key in recalls:
            audit.protected_scored_runs.append(
                {"bucket_path": f"s3://{gold}/{key}", "corpus": where, "reason_kind": "manifest"}
            )
    else:
        audit.findings.append({"bucket_path": where, "kind": kind, "reason": reason})
        audit.incomplete = audit.incomplete or kind != "manifest"


def audit_ledgers(
    audit: Audit, ledgers: Iterable[Path], client_factory: Callable[[dict], Any]
) -> None:
    for ledger in ledgers:
        audit.ledgers.append(str(ledger))
        if not ledger.is_file():
            audit.skip(str(ledger), "ledger file does not exist", incomplete=True)
            continue
        rows, unparsed = parse_ledger(ledger)
        for heading in unparsed:
            audit.skip(
                f"{ledger}: {heading}", "ledger section with no parsable row", incomplete=True
            )
        audit.counts["ledger_rows"] += len(rows)
        for row in rows:
            audit_bucket_row(audit, row, ledger, client_factory)


# -- 4. local ledgers ---------------------------------------------------------


def _jsonl(path: Path) -> list[dict]:
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def audit_local_ledgers(audit: Audit) -> None:
    try:
        looks = ds.load_looks()
        audit.counts.update(
            f"look_record.{e.get('role')}.{e.get('state', 'unknown')}" for e in looks
        )
    except Exception as e:  # noqa: BLE001
        audit.skip("look record", f"unreadable ({type(e).__name__})", incomplete=True)
    for label, path in (
        ("look ledger", ds.looks_ledger_path()),
        ("corpus ledger", ds.corpora_ledger_path()),
    ):
        if not path.is_file():
            audit.skip(f"{label} {path}", "absent")
            continue
        try:
            entries = _jsonl(path)
        except (OSError, ValueError) as e:
            audit.skip(f"{label} {path}", f"unreadable ({type(e).__name__})", incomplete=True)
            continue
        key = label.replace(" ", "_")
        audit.counts.update(
            f"{key}.{e.get('role')}.{e.get('state', 'claimed')}"
            for e in entries
            if isinstance(e, dict)
        )


# -- main ---------------------------------------------------------------------


def run_audit(
    runs_dirs: list[Path],
    journal_dirs: list[Path],
    ledgers: list[Path],
    client_factory: Callable[[dict], Any] = default_client_factory,
) -> Audit:
    audit = Audit()
    audit_records(audit, runs_dirs)
    audit_journals(audit, journal_dirs)
    audit_ledgers(audit, ledgers, client_factory)
    audit_local_ledgers(audit)
    return audit


def summary(audit: Audit) -> str:
    doc = audit.to_dict()
    lines = [
        f"held-out audit: {len(audit.protected_scored_runs)} protected scored run(s), "
        f"{len(audit.findings)} other finding(s), {len(audit.skipped)} skipped, "
        f"complete: {doc['complete']}",
        f"  searched {len(audit.runs_dirs)} runs dir(s), {len(audit.journal_dirs)} journal "
        f"dir(s), {len(audit.ledgers)} ledger(s), {len(audit.bucket_prefixes)} bucket prefix(es)",
    ]
    for r in audit.protected_scored_runs:
        lines.append(f"  PROTECTED {r.get('run_id') or r.get('bucket_path')}: {r['reason_kind']}")
    for f in audit.findings:
        where = f.get("record") or f.get("journal") or f.get("bucket_path")
        lines.append(f"  finding {f['kind']}: {where}")
    for k, v in doc["counts"].items():
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--runs-dir", action="append", type=Path, default=[])
    ap.add_argument("--journal-dir", action="append", type=Path, default=[])
    ap.add_argument("--ledger", action="append", type=Path, default=[])
    ap.add_argument("--out", type=Path)
    args = ap.parse_args(argv)
    runs = args.runs_dir or [ROOT / "lakebench-output" / "runs"]
    journals = args.journal_dir or [ROOT / "lakebench-output" / "journal"]
    audit = run_audit(runs, journals, args.ledger, client_factory=default_client_factory)
    if args.out:
        args.out.write_text(json.dumps(audit.to_dict(), indent=2) + "\n", encoding="utf-8")
    print(summary(audit))
    if audit.protected_scored_runs or any(
        f["kind"] in ("protected_config", "seed_token", "manifest") for f in audit.findings
    ):
        return 1
    return 2 if audit.incomplete else 0


if __name__ == "__main__":
    sys.exit(main())
