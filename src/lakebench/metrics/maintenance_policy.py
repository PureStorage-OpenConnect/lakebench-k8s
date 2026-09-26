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
