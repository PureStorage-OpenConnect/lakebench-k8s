"""Query execution result dataclass.

Separated from executor.py to avoid circular imports when module
implementations import the result type.
"""

from __future__ import annotations

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
