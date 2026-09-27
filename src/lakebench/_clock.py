"""One clock for recorded timestamps.

Metrics timestamps are UTC with the zone attached, so a metrics.json never
mixes naive host-local times with the UTC continuous window. Run ids keep
their host-local ``YYYYmmdd-HHMMSS`` form: they are identifiers, sorted as
strings next to older runs, and are not parsed back into times.
"""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """The current time as an aware UTC datetime."""
    return datetime.now(timezone.utc)
