"""Keep a streaming driver's whole log across kubelet log rotation (LB-270).

``read_namespaced_pod_log`` returns only the kubelet's current log file. A
streaming driver of a long continuous window writes more than one file holds
(``containerLogMaxSize``), so a single read at window end saw only the last
few gold cycles and the continuous gate, freshness and time to detect were
computed from a partial log.

The window loop polls each driver every health check and appends the lines
it has not seen, matched by their kubelet timestamps. Each read asks for the
lines since the newest kept line (plus a margin), so consecutive reads
overlap; a read whose oldest line is newer than the newest kept line means
lines were rotated away between polls, and that is recorded as a gap rather
than hidden.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone

#: Seconds added to every incremental read so consecutive reads overlap
#: despite clock skew between this host and the node.
OVERLAP_SECONDS = 60

#: reader(job, since_seconds) -> log text with kubelet timestamps, or None.
Reader = Callable[[str, "int | None"], "str | None"]


def _parse_ts(stamp: str) -> tuple[datetime, int] | None:
    """A kubelet RFC 3339 timestamp as (whole second, nanoseconds), or None.
    The fraction is trimmed of trailing zeros, so it is compared as a
    number, not as text."""
    try:
        whole, _, rest = stamp.rstrip("Z").partition(".")
        sec = datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        return sec, int((rest or "0").ljust(9, "0")[:9])
    except ValueError:
        return None


def stamp_of(ts: tuple[datetime, int]) -> str:
    return ts[0].strftime("%Y-%m-%dT%H:%M:%S") + f".{ts[1]:09d}Z"


class DriverLogAccumulator:
    """Per job: every log line seen so far, and any gap rotation caused."""

    def __init__(self, reader: Reader) -> None:
        self._read = reader
        self._lines: dict[str, list[str]] = {}
        self._last: dict[str, tuple[tuple[datetime, int], set[str]]] = {}
        self.gaps: dict[str, list[str]] = {}

    def poll(self, job: str) -> None:
        """Read the lines the job's driver wrote since the newest kept line."""
        last = self._last.get(job)
        since = None
        if last is not None:
            age = (datetime.now(timezone.utc) - last[0][0]).total_seconds()
            since = max(1, int(age) + OVERLAP_SECONDS)
        text = self._read(job, since)
        if text:
            self._merge(job, text)

    def _merge(self, job: str, text: str) -> None:
        kept = self._lines.setdefault(job, [])
        last = self._last.get(job)
        first = True
        for raw in text.splitlines():
            stamp, _, line = raw.partition(" ")
            ts = _parse_ts(stamp)
            if ts is None:
                continue
            if first and last is not None and ts > last[0]:
                self.gaps.setdefault(job, []).append(
                    f"lines between {stamp_of(last[0])} and {stamp} were rotated away "
                    "before they were read"
                )
            first = False
            if last is not None and (ts < last[0] or (ts == last[0] and line in last[1])):
                continue
            kept.append(line)
            last = (ts, {line}) if last is None or ts > last[0] else (ts, last[1] | {line})
        if last is not None:
            self._last[job] = last

    def text(self, job: str) -> str | None:
        """Every line kept for *job*, without timestamps, or None if none."""
        lines = self._lines.get(job)
        return "\n".join(lines) + "\n" if lines else None
