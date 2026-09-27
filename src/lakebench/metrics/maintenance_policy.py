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

A run with ``--skip-maintenance`` is stamped ``<id>+skipped``: it ran no
table maintenance, so it compares with nothing measured under the policy.

Bump MAINTENANCE_POLICY_ID whenever what maintenance does, when it runs, or
how long it may take changes, and add a line above.
"""

from __future__ import annotations

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
    """The maintenance a run actually got under *policy_id*, which is not
    always what the policy asks for: DuckDB runs none, Delta skips OPTIMIZE
    everywhere and VACUUM on Spark Thrift, continuous Delta has none in
    effect, and a stopped round did not finish.

    *outcomes* is what the run's maintenance calls recorded (one dict per
    call: ``kind`` expire or compaction, statement counts, or a ``skipped``
    or ``error`` reason). When given it decides: a kind whose statements all
    failed, or that never ran, is off, and one where only some succeeded is
    ``partial``. It can only turn a kind down from what the rules above
    allow, never up (a continuous Delta VACUUM at 7 d succeeds and still
    expires nothing inside the window). None (a record without outcomes, or
    a planned run) falls back to the rules alone.

    Returns ``{"id", "expire", "compaction", "reasons"}``; ``id`` is
    ``<policy>:expire=<on|partial|off>,compaction=<on|partial|off>[,stopped]``.
    Two runs with different effective ids measured under different execution
    conditions.
    """
    policy = policy_id or LEGACY_MAINTENANCE_POLICY_ID
    fmt = (table_format or "").lower()
    engine = (query_engine or "").lower()
    continuous = (mode or "batch").lower() in ("sustained", "continuous")
    reasons: list[str] = []
    expire = compaction = True
    if policy.endswith(SKIPPED_SUFFIX):
        expire = compaction = False
        reasons.append("--skip-maintenance")
    elif engine in ("duckdb", "none", ""):
        expire = compaction = False
        reasons.append(f"query engine {engine or 'none'} cannot run table maintenance")
    elif not continuous and pre_benchmark_maintenance is False:
        expire = compaction = False
        reasons.append("pre_benchmark_maintenance is off")
    elif fmt == "delta":
        compaction = False
        reasons.append("Delta OPTIMIZE is never run (it exhausts engine memory)")
        if continuous:
            expire = False
            reasons.append("continuous Delta VACUUM keeps the 7 d default: no effect in a window")
        elif engine == "spark-thrift":
            expire = False
            reasons.append("Delta VACUUM is skipped on Spark Thrift (it OOMs at 4Gi)")
    elif continuous and compaction_enabled is False:
        compaction = False
        reasons.append("continuous compaction is disabled")
    states = {"expire": "on" if expire else "off", "compaction": "on" if compaction else "off"}
    if outcomes is not None:
        for o in outcomes:
            if o.get("error"):
                reasons.append(f"{o.get('kind', 'maintenance')} call failed: {o['error']}")
        for kind in ("expire", "compaction"):
            if states[kind] == "off":
                continue
            ran = [o for o in outcomes if o.get("kind") == kind and o.get("total")]
            total = sum(int(o.get("total") or 0) for o in ran)
            ok = sum(int(o.get("succeeded") or 0) for o in ran)
            for o in outcomes:
                if o.get("kind") == kind and o.get("skipped"):
                    reasons.append(f"{kind} skipped: {o['skipped']}")
            if total == 0 or ok == 0:
                states[kind] = "off"
                reasons.append(
                    f"{kind}: no statement ran" if total == 0 else f"{kind}: 0 of {total} succeeded"
                )
            elif ok < total:
                states[kind] = "partial"
                reasons.append(f"{kind}: {ok} of {total} statements succeeded")
    expire = states["expire"] != "off"
    compaction = states["compaction"] != "off"
    parts = [f"expire={states['expire']}", f"compaction={states['compaction']}"]
    if stopped and (expire or compaction):
        parts.append("stopped")
        reasons.append("pre-benchmark maintenance stopped on its budget")
    return {
        "id": f"{policy}:" + ",".join(parts),
        "expire": states["expire"],
        "compaction": states["compaction"],
        "basis": "recorded outcomes"
        if outcomes is not None
        else "policy rules (no outcomes recorded)",
        "reasons": reasons,
    }
