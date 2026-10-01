"""Identity of the table-maintenance policy a run was measured under.

Maintenance changes what a run measures: post-maintenance QpH, continuous
freshness and throughput (maintenance statements compete with the streams),
and total_s3_objects. Two runs under different policies are not comparable,
in the same way two QpH numbers over different query sets are not
(benchmark.queries.query_set_id). Every metrics.json records the policy id;
the perf gate refuses to compare a run with a baseline recorded under
another policy, and ``lakebench reproduce`` refuses a package recorded under
another policy.

A metrics.json or package without the field was recorded under the legacy
policy.

Policy history:

- ``m1-legacy``: everything recorded before the id was stamped. That lumps
  two real policies: before f63cb38 Iceberg expire_snapshots and
  remove_orphan_files never succeeded on either engine (LB-172, LB-174), so
  only compaction ran; from f63cb38 they did. Because the two cannot be told
  apart, the perf gate and reproduce accept only runs under the current id.
- ``m2-2026-09-26``: LB-174 fixed statement forms (Trino SET SESSION
  min-retention in the same submission, Spark TIMESTAMP literal); continuous
  expiry floored at 1 h and orphan removal at 24 h 10 min on every path;
  destroy runs DROP only; continuous Delta VACUUM at the 7 d default and no
  continuous Delta OPTIMIZE (no effective Delta maintenance in continuous
  mode); continuous statements get min(600 s, interval / 2) with a round
  budget; every Iceberg table lakebench creates deletes old metadata.json
  files after commit (previous-versions-max 50). Tables created by an
  earlier version and reused keep their old properties.

v1.7 (LB-210) keeps ``m2-2026-09-26``: Trino optimize on a table with more
than 90 identity partitions runs as chunks of at most 90 partitions, so it
no longer fails on the connector's 100-writer limit. The operation, its
threshold and the tables it covers are what m2 already intended; only a
failure stopped it. Compaction outcomes now count tables (a table succeeds
when every chunk does) and name each failed table.

A run with ``--skip-maintenance`` is stamped ``<id>+skipped``: it ran no
table maintenance, so it compares with nothing measured under the policy.

Bump MAINTENANCE_POLICY_ID whenever the maintenance policy changes (which
operations run, at what retention or threshold, on which tables, when, and
under what time bounds), and add a line above. A fix that makes the current
policy's operations succeed as intended, like the v1.7 chunking, does not
bump it; the run's effective maintenance records what actually happened.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

MAINTENANCE_POLICY_ID = "m2-2026-09-26"
LEGACY_MAINTENANCE_POLICY_ID = "m1-legacy"


SKIPPED_SUFFIX = "+skipped"


def skipped_policy_id() -> str:
    """The id stamped on a run made with --skip-maintenance."""
    return MAINTENANCE_POLICY_ID + SKIPPED_SUFFIX


def not_current(actual: str | None) -> str | None:
    """Why a run under *actual* cannot be gated by this version, or None."""
    got = actual or LEGACY_MAINTENANCE_POLICY_ID
    if got == MAINTENANCE_POLICY_ID:
        return None
    return (
        f"run was measured under maintenance policy {got}, not the current "
        f"{MAINTENANCE_POLICY_ID}; only runs under the current policy are gated"
    )


def recorded_policy(record: Mapping[str, Any] | None) -> str:
    """The policy id a metrics.json dict or package metadata was recorded under."""
    value = (record or {}).get("maintenance_policy_id")
    return str(value) if value else LEGACY_MAINTENANCE_POLICY_ID


def policy_mismatch(expected: str | None, actual: str | None) -> str | None:
    """Why numbers under *actual* cannot stand against *expected*, or None."""
    a = expected or LEGACY_MAINTENANCE_POLICY_ID
    b = actual or LEGACY_MAINTENANCE_POLICY_ID
    if a == b:
        return None
    return (
        f"maintenance policy differs ({a} vs {b}); numbers measured under different "
        "table-maintenance policies are not comparable"
    )


#: The per-statement query id in a Trino error ("Query 20260929_..._abcde
#: failed: "), dropped so the same error in two rounds reads as one.
_QUERY_ID = re.compile(r"Query \S+ failed: ")

#: Coarse per-operation classes for the effective maintenance identity.
RAN = "ran"
#: The operation executed, but at a retention no file written inside the
#: window can meet: continuous Delta VACUUM at Delta's 7 d default while
#: streams are live. It ran; it had no effect on what the window measured.
RAN_NO_EFFECT = "ran_no_effect"
NOT_SUPPORTED = "not_supported"
SKIPPED_BY_USER = "skipped_by_user"
FAILED = "failed"
NOT_RUN = "not_run"

#: Owner decision #46: Delta continuous ships in v1.6 with no effective
#: maintenance, and the evidence says so.
DELTA_CONTINUOUS_LIMITATION = (
    "known v1.6 limitation (owner decision #46): Delta continuous runs with no "
    "effective table maintenance (no OPTIMIZE; VACUUM, where it runs, keeps the 7 d "
    "default and removes nothing inside the window), so small files accumulate "
    "and in-window QpH can decline; a composite QpH median over the rounds is not "
    "a steady-state figure"
)

#: Operations in the identity, per table format, and the maintenance call
#: kind that runs each (the run paths record outcomes per call kind:
#: ``expire`` runs expire_snapshots and remove_orphan_files, or Delta VACUUM).
ICEBERG_OPERATIONS = ("expire_snapshots", "remove_orphan_files", "compaction")
DELTA_OPERATIONS = ("vacuum", "compaction")
OPERATION_KIND = {
    "expire_snapshots": "expire",
    "remove_orphan_files": "expire",
    "vacuum": "expire",
    "compaction": "compaction",
}
# Worst first: the coarse per-kind class is the worst of its operations.
_SEVERITY = (FAILED, NOT_RUN, SKIPPED_BY_USER, NOT_SUPPORTED, RAN_NO_EFFECT, RAN)


def operations_for(table_format: str | None) -> tuple[str, ...]:
    """The maintenance operations the identity names for *table_format*."""
    return DELTA_OPERATIONS if (table_format or "").lower() == "delta" else ICEBERG_OPERATIONS


def _worst(values: list[str]) -> str:
    for v in _SEVERITY:
        if v in values:
            return v
    return RAN


def effective_maintenance(
    policy_id: str | None,
    *,
    table_format: str | None,
    query_engine: str | None,
    mode: str | None,
    pre_benchmark_maintenance: bool | None = True,
    compaction_enabled: bool | None = True,
    stopped: bool | None = False,
    outcomes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """The maintenance a run actually got under *policy_id*.

    Each operation gets a coarse class, and the identity ``id`` carries one
    per operation: Iceberg ``expire_snapshots``, ``remove_orphan_files`` and
    ``compaction``; Delta ``vacuum`` and ``compaction``. So a run whose
    orphan removal failed while expiry succeeded is not stamped the same as
    one where both ran, and the two are not like-for-like. Classes:
    ``ran``, ``ran_no_effect`` (it executed, but at a retention nothing
    written in the window can meet: continuous Delta VACUUM at the 7 d
    default), ``not_supported`` (the composition cannot run it and it was
    not executed: DuckDB, Delta OPTIMIZE, Delta VACUUM on Spark Thrift),
    ``skipped_by_user`` (--skip-maintenance,
    pre_benchmark_maintenance off, --skip-benchmark, continuous compaction
    disabled), ``failed`` (it was attempted and no statement succeeded) or
    ``not_run`` (the run ended before the maintenance phase). Partial
    success, per-round timeouts and a stopped budget are ``detail`` and
    ``reasons``, not identity: one timed-out round in a 24 h run does not
    make it a different experiment.

    *outcomes* is what the run's maintenance calls recorded (one dict per
    call: ``kind``, statement counts and per-operation ``operations``, or
    ``skipped``, ``user_skip`` or ``error``). A call recorded without
    per-operation counts applies its totals to every operation it runs.
    None (a record from before outcomes, or a planned run) falls back to the
    rules alone, labelled so in ``basis``. Outcomes only turn an operation
    down from what the rules allow, never up.

    Records written before per-operation identity carry
    ``<policy>:expire=<class>,compaction=<class>``. They still load; their
    id matches no current id, which is right, because ``expire=ran`` there
    could hide a failed remove_orphan_files.

    An operation is never ``not_supported`` when a statement for it
    executed: an executed operation is ``ran``, ``ran_no_effect`` or (when
    no statement succeeded) ``failed``.

    Compaction outcomes recorded from v1.7 count tables (``unit:
    "tables"``), carry ``failures`` (``{table, statement, error}``) and
    ``statements``; each failed table adds "compaction failed on <table>:
    <error>" to ``reasons``, and ``detail`` gains ``compaction_failures``
    (the table list) and ``compaction_statements`` (the statements
    attempted). ``failed`` still means no statement succeeded, so a table
    whose chunks partly succeeded is partial: it stays ``ran`` in ``id`` and
    reads ``partial`` in ``detail_id`` (LB-210).

    ``ran_no_effect`` assumes the window is shorter than the 7 d retention;
    a longer continuous run under-claims (it reads no effect where VACUUM
    could have removed files), never the other way round.

    Returns ``{"id", "detail_id", "operations", "expire", "compaction",
    "detail", "basis", "reasons", "known_limitations"}``; ``expire`` and
    ``compaction`` are the worst class over the operations of that kind.
    ``known_limitations`` names the documented limits of the run's
    maintenance (Delta continuous runs with no effective maintenance in
    v1.6, owner decision #46).
    """
    policy = policy_id or LEGACY_MAINTENANCE_POLICY_ID
    fmt = (table_format or "").lower()
    engine = (query_engine or "").lower()
    continuous = (mode or "batch").lower() in ("sustained", "continuous")
    reasons: list[str] = []
    cls = {"expire": RAN, "compaction": RAN}
    live_vacuum = False

    def turn(kinds, value, reason):
        for k in kinds:
            if cls[k] == RAN:
                cls[k] = value
        reasons.append(reason)

    both = ("expire", "compaction")
    if policy.endswith(SKIPPED_SUFFIX):
        turn(both, SKIPPED_BY_USER, "--skip-maintenance")
    elif engine in ("duckdb", "none", ""):
        turn(both, NOT_SUPPORTED, f"query engine {engine or 'none'} cannot run table maintenance")
    elif not continuous and pre_benchmark_maintenance is False:
        turn(both, SKIPPED_BY_USER, "pre_benchmark_maintenance is off")
    elif fmt == "delta":
        turn(
            ("compaction",),
            NOT_SUPPORTED,
            "Delta OPTIMIZE is never run (it exhausts engine memory)",
        )
        if engine == "spark-thrift":
            turn(
                ("expire",),
                NOT_SUPPORTED,
                "Delta VACUUM is skipped on Spark Thrift (it OOMs at 4Gi)",
            )
        elif continuous:
            # VACUUM executes; whether it ran is for the outcomes to say.
            # What it can remove is decided below, once it is known to have run.
            live_vacuum = True
    elif continuous and compaction_enabled is False:
        turn(("compaction",), SKIPPED_BY_USER, "continuous compaction is disabled")

    ops = operations_for(fmt)
    applied: set[str] = set()
    # Per operation (expire_snapshots, remove_orphan_files, vacuum): the
    # retentions it ran at and its statement counts. The merged
    # ``applied_retention`` is the expire retention only.
    per_op: dict[str, dict[str, Any]] = {}
    if outcomes is not None:
        for o in outcomes:
            for op in o.get("operations") or []:
                name = str(op.get("operation") or "unknown")
                rec = per_op.setdefault(
                    name,
                    {"kind": o.get("kind"), "applied_retention": [], "total": 0, "succeeded": 0},
                )
                if op.get("retention") and str(op["retention"]) not in rec["applied_retention"]:
                    rec["applied_retention"] = sorted(
                        [*rec["applied_retention"], str(op["retention"])]
                    )
                rec["total"] += int(op.get("total") or 0)
                rec["succeeded"] += int(op.get("succeeded") or 0)
        for o in outcomes:
            if o.get("error"):
                reasons.append(f"{o.get('kind', 'maintenance')} call failed: {o['error']}")
            if o.get("note"):
                reasons.append(f"{o.get('kind')}: {o['note']}")
            if o.get("retention"):
                applied.add(str(o["retention"]))
            if o.get("user_skip") and o.get("kind") in cls:
                turn((o["kind"],), SKIPPED_BY_USER, f"{o['kind']} skipped: {o['user_skip']}")
    op_cls = {op: cls[OPERATION_KIND[op]] for op in ops}
    detail = {op: ("on" if v == RAN else "off") for op, v in op_cls.items()}
    if outcomes is not None:
        for kind in both:
            if cls[kind] != RAN:
                continue
            # A phase that raised before any statement was attempted never
            # reached this operation: not an attempt.
            mine = [
                o
                for o in outcomes
                if o.get("kind") in (kind, "maintenance") and not o.get("before_statements")
            ]
            for o in mine:
                if o.get("kind") == kind and o.get("skipped"):
                    reasons.append(f"{kind} skipped: {o['skipped']}")
            for op in (x for x in ops if OPERATION_KIND[x] == kind):
                if not mine:
                    op_cls[op], detail[op] = NOT_RUN, "off"
                    reasons.append(f"{op}: never reached (the run ended before maintenance)")
                    continue
                total = ok = 0
                # Outcomes counted per table (v1.7, LB-210) still decide
                # ``failed`` by statements, so the id rule is unchanged: a
                # table whose chunks partly succeeded is partial, not failed.
                stmt_total = stmt_ok = 0
                unit = "statements"
                for o in mine:
                    if o.get("kind") != kind:
                        continue
                    if o.get("operations") is not None and kind == "expire":
                        for rec in o["operations"]:
                            if rec.get("operation") == op:
                                total += int(rec.get("total") or 0)
                                ok += int(rec.get("succeeded") or 0)
                    else:
                        total += int(o.get("total") or 0)
                        ok += int(o.get("succeeded") or 0)
                        if o.get("unit") == "tables":
                            unit = "table compactions"
                        stmt_total += int(o.get("statements_total", o.get("total")) or 0)
                        stmt_ok += int(o.get("statements_succeeded", o.get("succeeded")) or 0)
                # A round or phase that raised counts as one failed attempt.
                raised = sum(1 for o in mine if o.get("error"))
                total += raised
                if kind == "expire" or not stmt_total:
                    stmt_total, stmt_ok = total, ok
                else:
                    stmt_total += raised
                if stmt_total == 0 or stmt_ok == 0:
                    op_cls[op], detail[op] = FAILED, "off"
                    reasons.append(
                        f"{op}: no statement ran"
                        if stmt_total == 0
                        else f"{op}: 0 of {stmt_total} statements succeeded"
                    )
                elif ok < total:
                    detail[op] = "partial"
                    reasons.append(f"{op}: {ok} of {total} {unit} succeeded")
    if live_vacuum and op_cls.get("vacuum") == RAN:
        retention = ", ".join((per_op.get("vacuum") or {}).get("applied_retention") or []) or (
            ", ".join(sorted(applied)) or "the 7 d default"
        )
        op_cls["vacuum"] = RAN_NO_EFFECT
        if detail.get("vacuum") == "on":
            detail["vacuum"] = "no_effect"
        reasons.append(
            f"vacuum ran at {retention} retention (Delta's 7 d default while streams are "
            "live): no file written in the window was eligible, so it removed nothing "
            "the window measured"
            if outcomes is not None
            else "vacuum runs at Delta's 7 d default while streams are live: no file "
            "written in the window is eligible"
        )
    # Which tables compaction failed on, and what ran (LB-210): a partial
    # compaction stays out of ``id`` and is named here and in ``reasons``.
    compaction_detail: dict[str, Any] = {}
    compaction_records = [
        o for o in (outcomes or []) if o.get("kind") == "compaction" and "statements" in o
    ]
    if compaction_records:
        failed_tables: list[str] = []
        statements: list[str] = []
        # One reason per (table, error), with a count: a chunk that fails
        # every round of a long run is one line, not one per round.
        failure_counts: dict[tuple[str, str], int] = {}
        for o in compaction_records:
            for f in o.get("failures") or []:
                table = str(f.get("table") or "unknown")
                error = _QUERY_ID.sub("", str(f.get("error") or "no error text"))
                if table not in failed_tables:
                    failed_tables.append(table)
                failure_counts[(table, error)] = failure_counts.get((table, error), 0) + 1
            for sql in o.get("statements") or []:
                if sql not in statements:
                    statements.append(str(sql))
        for (table, error), count in failure_counts.items():
            times = f" ({count} times)" if count > 1 else ""
            reasons.append(f"compaction failed on {table}: {error}{times}")
        compaction_detail = {
            "compaction_failures": failed_tables,
            "compaction_statements": statements,
        }
    limitations: list[str] = []
    if fmt == "delta" and continuous:
        limitations.append(DELTA_CONTINUOUS_LIMITATION)
    detail_parts = [f"{op}={detail[op]}" for op in ops]
    if stopped and RAN in op_cls.values():
        detail_parts.append("stopped")
        reasons.append("pre-benchmark maintenance stopped on its budget")
    coarse = {k: _worst([v for op, v in op_cls.items() if OPERATION_KIND[op] == k]) for k in both}
    return {
        "id": f"{policy}:" + ",".join(f"{op}={op_cls[op]}" for op in ops),
        "detail_id": f"{policy}:" + ",".join(detail_parts),
        "operations": dict(op_cls),
        "expire": coarse["expire"],
        "compaction": coarse["compaction"],
        "detail": {
            **detail,
            **({"applied_retention": sorted(applied)} if applied else {}),
            **({"operations": per_op} if per_op else {}),
            **compaction_detail,
        },
        "basis": "recorded outcomes"
        if outcomes is not None
        else "policy rules (no outcomes recorded)",
        "reasons": reasons,
        "known_limitations": limitations,
    }
