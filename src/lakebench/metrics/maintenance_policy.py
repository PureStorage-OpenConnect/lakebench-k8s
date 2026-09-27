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


#: Coarse per-operation classes for the effective maintenance identity.
RAN = "ran"
NOT_SUPPORTED = "not_supported"
SKIPPED_BY_USER = "skipped_by_user"
FAILED = "failed"
NOT_RUN = "not_run"


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

    Each operation (expire, compaction) gets a coarse class, which is what
    the identity ``id`` carries: ``ran``, ``not_supported`` (the
    composition cannot run it: DuckDB, Delta OPTIMIZE, Delta VACUUM on
    Spark Thrift, continuous Delta VACUUM at its 7 d default),
    ``skipped_by_user`` (--skip-maintenance, pre_benchmark_maintenance off,
    --skip-benchmark, continuous compaction disabled), ``failed`` (it was
    attempted and no statement succeeded) or ``not_run`` (the run ended
    before the maintenance phase). Partial success, per-round timeouts and a
    stopped budget are ``detail`` and ``reasons``, not identity: one timed-out
    round in a 24 h run does not make it a different experiment.

    *outcomes* is what the run's maintenance calls recorded (one dict per
    call: ``kind``, statement counts, or ``skipped``, ``user_skip`` or
    ``error``). None (a record from before outcomes, or a planned run) falls
    back to the rules alone, labelled so in ``basis``. Outcomes only turn an
    operation down from what the rules allow, never up.

    Returns ``{"id", "detail_id", "expire", "compaction", "detail", "basis",
    "reasons"}``.
    """
    policy = policy_id or LEGACY_MAINTENANCE_POLICY_ID
    fmt = (table_format or "").lower()
    engine = (query_engine or "").lower()
    continuous = (mode or "batch").lower() in ("sustained", "continuous")
    reasons: list[str] = []
    cls = {"expire": RAN, "compaction": RAN}

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
        if continuous:
            turn(
                ("expire",),
                NOT_SUPPORTED,
                "continuous Delta VACUUM keeps the 7 d default: no effect in a window",
            )
        elif engine == "spark-thrift":
            turn(
                ("expire",),
                NOT_SUPPORTED,
                "Delta VACUUM is skipped on Spark Thrift (it OOMs at 4Gi)",
            )
    elif continuous and compaction_enabled is False:
        turn(("compaction",), SKIPPED_BY_USER, "continuous compaction is disabled")

    detail = {k: ("on" if v == RAN else "off") for k, v in cls.items()}
    if outcomes is not None:
        for o in outcomes:
            if o.get("error"):
                reasons.append(f"{o.get('kind', 'maintenance')} call failed: {o['error']}")
            if o.get("user_skip") and o.get("kind") in cls:
                turn((o["kind"],), SKIPPED_BY_USER, f"{o['kind']} skipped: {o['user_skip']}")
        for kind in both:
            if cls[kind] != RAN:
                detail[kind] = "off"
                continue
            mine = [o for o in outcomes if o.get("kind") in (kind, "maintenance")]
            total = sum(int(o.get("total") or 0) for o in mine if o.get("kind") == kind)
            ok = sum(int(o.get("succeeded") or 0) for o in mine if o.get("kind") == kind)
            # A round or phase that raised counts as one failed attempt.
            total += sum(1 for o in mine if o.get("error"))
            for o in mine:
                if o.get("kind") == kind and o.get("skipped"):
                    reasons.append(f"{kind} skipped: {o['skipped']}")
            attempted = bool(mine)
            if not attempted:
                cls[kind], detail[kind] = NOT_RUN, "off"
                reasons.append(f"{kind}: never reached (the run ended before maintenance)")
            elif total == 0 or ok == 0:
                cls[kind], detail[kind] = FAILED, "off"
                reasons.append(
                    f"{kind}: no statement ran" if total == 0 else f"{kind}: 0 of {total} succeeded"
                )
            elif ok < total:
                detail[kind] = "partial"
                reasons.append(f"{kind}: {ok} of {total} statements succeeded")
    detail_parts = [f"expire={detail['expire']}", f"compaction={detail['compaction']}"]
    if stopped and RAN in cls.values():
        detail_parts.append("stopped")
        reasons.append("pre-benchmark maintenance stopped on its budget")
    return {
        "id": f"{policy}:expire={cls['expire']},compaction={cls['compaction']}",
        "detail_id": f"{policy}:" + ",".join(detail_parts),
        "expire": cls["expire"],
        "compaction": cls["compaction"],
        "detail": detail,
        "basis": "recorded outcomes"
        if outcomes is not None
        else "policy rules (no outcomes recorded)",
        "reasons": reasons,
    }
