"""Per-query sample counts and within-round spread from recorded query dicts.

A benchmark round times each query ``iterations`` times and scores it by the
median (LB-150). The recorded query dict carries every sample. Records
written before that carry only ``elapsed_seconds`` and read as one sample.
These helpers work on the dicts, so the scorecard, the perf gate and
reproduce read new and old metrics.json the same way.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def query_samples(q: Mapping[str, Any]) -> list[float]:
    """The timed samples of one recorded query; [elapsed_seconds] when absent."""
    samples = q.get("samples")
    if isinstance(samples, list) and samples:
        return [float(t) for t in samples if isinstance(t, (int, float))]
    elapsed = q.get("elapsed_seconds")
    return [float(elapsed)] if isinstance(elapsed, (int, float)) else []


def samples_per_query(queries: Iterable[Mapping[str, Any]]) -> int | None:
    """Smallest sample count over the successful queries; None when none succeeded.

    Successful queries only: a failed query stops repeating at its first
    failure, so its count says nothing about how the round was measured.
    """
    counts = [len(query_samples(q)) for q in queries if q.get("success")]
    counts = [c for c in counts if c > 0]
    return min(counts) if counts else None


def spread(queries: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Within-round spread over the successful queries of one round.

    Summing every query's fastest sample, and separately its slowest, gives
    the fastest and slowest round the samples allow. ``qph_low`` and
    ``qph_high`` are the QpH of those two rounds and ``relative_range`` is
    their seconds apart over the median-scored seconds. With one sample per
    query the three collapse onto the score: the spread is unmeasured, not
    zero, which ``samples_per_query == 1`` says.
    """
    ok = [q for q in queries if q.get("success") and query_samples(q)]
    if not ok:
        return {"samples_per_query": 0, "qph_low": 0.0, "qph_high": 0.0, "relative_range": 0.0}
    med = sum(float(q.get("elapsed_seconds") or 0.0) for q in ok)
    lo = sum(min(query_samples(q)) for q in ok)
    hi = sum(max(query_samples(q)) for q in ok)
    n = len(ok)
    return {
        "samples_per_query": min(len(query_samples(q)) for q in ok),
        "qph_low": round(n / hi * 3600, 1) if hi > 0 else 0.0,
        "qph_high": round(n / lo * 3600, 1) if lo > 0 else 0.0,
        "relative_range": round((hi - lo) / med, 4) if med > 0 else 0.0,
    }
