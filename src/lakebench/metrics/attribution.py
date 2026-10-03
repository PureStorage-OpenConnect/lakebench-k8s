"""Where an AML batch run's time went, and how close each stage came to its
budget.

Two derived blocks, read from fields the collector already holds:

- ``attribution(metrics)``: for the gold-finalize job, the rule that took
  longest (``jobs[].rule_elapsed_s``), that rule's heaviest Spark stage
  (``jobs[].stage_profile``, from the driver's status store) and the TM
  operations pass's share (``jobs[].tm_ops.elapsed_seconds``). Published as
  ``experiment.attribution``.
- ``headroom_pct(metrics)``: per batch stage, ``100 x (1 - elapsed /
  per-job timeout)`` with the per-job timeout the run used
  (``job_timeout_seconds``), and for the timed benchmark ``benchmark_query``,
  ``100 x (1 - slowest query / per-query timeout)``
  (``benchmark_query_timeout_seconds``): no per-job timeout applies to the
  benchmark, its queries are bounded one by one. Published as
  ``limits.headroom_pct``; 25 or more means at most 75% of the budget used.

Both are diagnostics: neither enters identity, a verdict or a comparison.
"""

from __future__ import annotations

from typing import Any

GOLD_JOB = "gold-finalize"


def _gold_job(metrics: Any) -> Any | None:
    """The last gold-finalize job of the run that carries per-rule times."""
    found = None
    for job in getattr(metrics, "jobs", None) or []:
        if getattr(job, "job_type", None) == GOLD_JOB and getattr(job, "rule_elapsed_s", None):
            found = job
    return found


def _share(part: float | None, whole: float | None) -> float | None:
    if part is None or not whole or whole <= 0:
        return None
    return round(float(part) / float(whole), 4)


def attribution(metrics: Any) -> dict[str, Any] | None:
    """``{job, job_elapsed_s, dominant_rule, rule_elapsed_s, share_of_job,
    dominant_stage, tm_elapsed_s, tm_share, profile}`` or None when the run
    has no gold-finalize job with per-rule times (C360, continuous, a record
    from before per-rule times were recorded).

    ``share_of_job`` is the dominant rule's elapsed time over the gold job's
    elapsed time. ``dominant_stage`` is the heaviest stage of that rule by
    summed executor run time, with its share of the rule's own executor time
    across its logged stages (``share_of_rule_exec``), or None when the
    rule's profile is missing; ``profile`` then says why (``unavailable``
    with the reason, ``no_stage``, or ``missing``). A profile flagged
    incomplete, truncated or lossy is passed on with its flags: the stage it
    names may not be the heaviest."""
    job = _gold_job(metrics)
    if job is None:
        return None
    times: dict[str, float] = dict(job.rule_elapsed_s)
    rule = max(sorted(times), key=lambda r: times[r])
    job_s = getattr(job, "elapsed_seconds", None) or None
    stages = (getattr(job, "stage_profile", None) or {}).get(rule)
    unavailable = (getattr(job, "stage_profile_unavailable", None) or {}).get(rule)
    stage: dict[str, Any] | None = None
    if stages:
        top = stages[0]
        total_exec = sum(float(s.get("exec_s") or 0.0) for s in stages)
        stage = {
            "stage": top.get("stage"),
            "name": top.get("name"),
            "tasks": top.get("tasks"),
            "exec_s": top.get("exec_s"),
            "wall_s": top.get("wall_s"),
            "max_task_s": top.get("max_task_s"),
            "share_of_rule_exec": _share(top.get("exec_s"), total_exec),
            "complete": top.get("complete"),
            "truncated": top.get("truncated"),
            "lossy": top.get("lossy"),
        }
        profile = "read"
    elif unavailable:
        profile = f"unavailable: {unavailable}"
    elif stages is not None:
        profile = "no_stage"
    else:
        profile = "missing"
    tm_s = (getattr(job, "tm_ops", None) or {}).get("elapsed_seconds")
    return {
        "job": GOLD_JOB,
        "job_elapsed_s": job_s,
        "dominant_rule": rule,
        "rule_elapsed_s": times[rule],
        "share_of_job": _share(times[rule], job_s),
        "dominant_stage": stage,
        "tm_elapsed_s": tm_s,
        "tm_share": _share(tm_s, job_s),
        "profile": profile,
    }


def benchmark_query_headroom(metrics: Any) -> float | None:
    """``100 x (1 - slowest query / per-query timeout)`` for the run's timed
    benchmark, or None when there is no benchmark, no recorded per-query
    timeout, or a query failed (a query that timed out has no headroom)."""
    bench = getattr(metrics, "benchmark", None)
    timeout = getattr(metrics, "benchmark_query_timeout_seconds", None)
    queries = list(getattr(bench, "queries", None) or []) if bench is not None else []
    if not timeout or timeout <= 0 or not queries:
        return None
    if any(not q.get("success", False) for q in queries):
        return None
    # Every timed sample, not the per-query median: one sample near the
    # timeout is what the timeout bounds.
    slowest = max(
        max(
            [float(t) for t in (q.get("samples") or [])] or [float(q.get("elapsed_seconds") or 0.0)]
        )
        for q in queries
    )
    return round(100.0 * (1.0 - slowest / float(timeout)), 1)


def headroom_pct(metrics: Any) -> dict[str, float | None] | None:
    """``{job_type: pct}`` for each batch job against the per-job timeout,
    and ``benchmark_query`` for the timed benchmark against its per-query
    timeout (the limit that bounds the benchmark phase), or None when the
    run did not record its per-job timeout. A job type that ran more than
    once (multi-cycle runs) reports its slowest run; a failed job reads None
    (it has no headroom to report). Negative means over budget."""
    timeout = getattr(metrics, "job_timeout_seconds", None)
    if not timeout or timeout <= 0:
        return None
    worst: dict[str, float] = {}
    failed: set[str] = set()
    for job in getattr(metrics, "jobs", None) or []:
        elapsed = getattr(job, "elapsed_seconds", None) or 0.0
        jt = getattr(job, "job_type", None)
        if not jt:
            continue
        if getattr(job, "success", True) is False:
            failed.add(jt)
        elif elapsed > 0:
            worst[jt] = max(worst.get(jt, 0.0), float(elapsed))
    out: dict[str, float | None] = {
        jt: round(100.0 * (1.0 - s / float(timeout)), 1) for jt, s in worst.items()
    }
    for jt in failed:
        out[jt] = None
    if getattr(metrics, "benchmark", None) is not None:
        out["benchmark_query"] = benchmark_query_headroom(metrics)
    return dict(sorted(out.items()))
