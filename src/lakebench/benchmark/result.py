"""Query execution result dataclass.

Separated from executor.py to avoid circular imports when module
implementations import the result type.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class QueryExecutorResult:
    """Result of a single query execution via an engine executor."""

    sql: str
    engine: str
    duration_seconds: float
    rows_returned: int
    raw_output: str
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


_TRACEBACK_HEADER = "Traceback (most recent call last):"
# Lines an engine CLI uses to state the failure itself (beeline "Error:",
# Hive "FAILED:", Trino "Query ... failed:"), as opposed to a log4j or hadoop
# WARN line that happens to mention an exception.
_ERROR_LEAD = re.compile(r"^(error:|failed:|query \S+ failed:)", re.IGNORECASE)
_KUBECTL_EXIT = re.compile(r"^command terminated with exit code \d+$")


def summarise_engine_error(text: str, limit: int = 300) -> str:
    """Return the line of an engine's stderr that says what went wrong.

    A Python traceback puts the frames first and the exception last, so the
    first 200 characters of stderr (what the executors used to keep) show
    only ``Traceback (most recent call last): File "<string>", line 1`` and
    lose the error entirely. For a traceback this returns the final exception
    block: the first unindented line after the last traceback header, plus
    its continuation lines, skipping DuckDB's caret pointer. The last header
    is used so a chained exception reports the one that was actually raised.

    Text without a traceback (Trino CLI, beeline) is returned from the first
    line that states the failure (``Error:``, ``FAILED:``, ``Query ...
    failed:``) onward, or whole when no line does, truncated to ``limit``.
    """
    stripped = (text or "").strip()
    if not stripped:
        return "Unknown error"

    # kubectl appends its own exit line after the container's stderr.
    lines = [line for line in stripped.splitlines() if not _KUBECTL_EXIT.match(line.strip())]
    headers = [
        i
        for i, line in enumerate(lines)
        if line.strip() == _TRACEBACK_HEADER
        # Interpreter-shutdown noise ("Exception ignored in: ... __del__")
        # prints its own traceback after the real one.
        and not (i > 0 and lines[i - 1].startswith("Exception ignored in"))
    ]
    header = headers[-1] if headers else -1
    if header >= 0:
        block: list[str] = []
        for line in lines[header + 1 :]:
            if not block:
                # Frame lines are indented; the exception starts at column 0.
                if not line or line[0].isspace():
                    continue
            elif line.strip() == "":
                continue
            if set(line.strip()) <= {"^"}:
                continue
            block.append(line.strip())
        if block:
            summary = " ".join(block)
            # The private module path adds nothing for a reader.
            if summary.startswith("_duckdb."):
                summary = summary[len("_duckdb.") :]
            return summary[:limit]

    kept = [line.strip() for line in lines if line.strip() and not set(line.strip()) <= {"^"}]
    for i, line in enumerate(kept):
        if _ERROR_LEAD.match(line):
            # Start at the stated failure and keep what follows (a beeline
            # "Caused by:" root cause), dropping only the log noise before it.
            return " ".join(kept[i:])[:limit]
    return " ".join(kept)[:limit] or stripped[:limit]
