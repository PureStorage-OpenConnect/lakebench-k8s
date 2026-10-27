"""AML steps after the detection stages: recall scoring for batch and
continuous runs, and the time-travel reads of the snapshots the continuous
ticks read.

The continuous AML scorer runs over the snapshots the last completed tick
inside the window recorded (covered mode). Each table is read at the
snapshot that tick pinned (FOR VERSION AS OF), so gold.alerts being
rewritten by a later tick does not reach the score.
"""

from __future__ import annotations

from typing import Any

from lakebench.cli._helpers import console, print_info, print_success, print_warning

#: The covered-mode table names, in tick-record terms.
COVERED_OPTIONS = (
    ("txns", "pinned_txns"),
    ("entities", "pinned_entities"),
    ("accounts", "pinned_accounts"),
    ("versions", "pinned_versions"),
    ("alerts", "committed_alerts"),
    ("status", "committed_status"),
)


def _s3_client(cfg):
    from lakebench.s3 import s3_client_from_config

    return s3_client_from_config(cfg.platform.storage.s3)


def scoring_count_line(summary: dict) -> str:
    """'6 of 15 typologies scored; 8 no rule, 1 rule skipped' from a
    recall.json summary: never every manifest typology as scored. A covered
    summary says how many instances its last tick covered."""
    if summary.get("mode") == "covered":
        if summary.get("status") != "scored":
            return f"not scored: {summary.get('reason') or 'no reason given'}"
        cov = summary.get("covered") or {}
        return (
            f"covered mode: {cov.get('covered_instances', 0):,} of "
            f"{cov.get('corpus_instances', 0):,} instances covered by the last tick"
        )
    typs = summary.get("typologies", []) or []
    counts = summary.get("typology_counts")
    if counts is None:
        counts = {}
        for t in typs:
            st = t.get("detection_status") or "unknown"
            counts[st] = counts.get(st, 0) + 1
    labels = (
        ("partial", "partial"),
        ("no_rule", "no rule"),
        ("rule_skipped", "rule skipped"),
        ("rule_error", "rule error"),
        ("unknown", "unknown"),
    )
    rest = [f"{counts[k]} {lab}" for k, lab in labels if counts.get(k)]
    line = f"{counts.get('scored', 0)} of {len(typs)} typologies scored"
    return line + (f"; {', '.join(rest)}" if rest else "")


def not_scored(reason: str) -> dict[str, Any]:
    """A continuous ``financial_scoring`` that holds no score, and why."""
    return {"mode": "covered", "status": "not_scored", "reason": reason}


def _rules_meta():
    """RulesMeta from the pure rule metadata and the reason-code vocabulary.

    The three rule mappings come from aml.rule_meta (pure); the reason-code
    vocabulary from spark/scripts/aml_reason_codes.py, whose module top is
    pure (its pyspark imports are function-local), so the CLI can import it."""
    from lakebench.aml.rule_meta import (
        CUSTOMER_SCOPED_RULES,
        HIGH_PRIORITY_CUTOFFS,
        RULE_TARGET_TYPOLOGY,
    )
    from lakebench.aml.scoring_queries import RulesMeta
    from lakebench.spark.scripts.aml_reason_codes import REASON_CODES, vocabulary_digest

    return RulesMeta(
        reason_codes=REASON_CODES,
        reason_code_vocabulary=vocabulary_digest(HIGH_PRIORITY_CUTOFFS),
        customer_scoped_rules=CUSTOMER_SCOPED_RULES,
        rule_target_typology=RULE_TARGET_TYPOLOGY,
    )


def _write_recall_json(cfg, run_id: str, record: dict) -> None:
    """Write the record as recall.json under gold/scoring/<run_id>/, so the
    held-out audit and anything else listing that prefix still find it."""
    import json as _json

    try:
        _s3_client(cfg).raw_client.put_object(
            Bucket=cfg.platform.storage.s3.buckets.gold,
            Key=f"scoring/{run_id}/recall.json",
            Body=_json.dumps(record).encode(),
        )
    except Exception as e:  # noqa: BLE001 -- best effort, never fails the run
        print_warning(f"Could not write recall.json: {e}")


def score_financial_via_queries(
    cfg,
    run_id: str,
    *,
    covered: dict | None,
    timeout: int,
) -> dict[str, Any] | None:
    """Score recall and precision with SQL on the query engine.

    Covered mode: *covered* is the scored tick record; every table is read at
    the snapshot that tick pinned and alert_set comes from the tick. Batch:
    *covered* is None, tables read at their current snapshot. Returns the
    financial_scoring record; a query that fails on the engine returns
    ``not_scored`` with the engine's error as its reason. None only on an
    unexpected failure. Best effort: a scoring failure never fails a
    pipeline that already succeeded.
    """
    try:
        from lakebench.aml import scoring_queries as sq
        from lakebench.benchmark.executor import get_executor

        executor = get_executor(cfg)
        meta = _rules_meta()
        snapshot_ids = None
        alert_set = None
        if covered is not None:
            snapshot_ids = {name: int(covered[key]) for name, key in COVERED_OPTIONS}
            alert_set = covered.get("alert_set")

        console.print()
        console.print("[bold]Stage: financial score[/bold]")
        if covered is not None:
            print_info(
                "Scoring recall over what the last completed tick saw "
                f"(cycle {covered.get('cycle')})..."
            )
        else:
            print_info("Scoring recall/precision against the datagen manifest...")

        try:
            record = sq.score(
                executor.query_rows,
                catalog=executor.catalog_name,
                snapshot_ids=snapshot_ids,
                run_id=run_id,
                meta=meta,
                alert_set=alert_set,
                timeout=timeout,
            )
        except sq.NotScored as e:
            record = _not_scored_record(str(e), covered)
        except RuntimeError as e:
            # An engine failure: said here, and its text is the record's
            # reason, in the record the callers write when no score came back.
            print_warning(f"Financial scoring did not complete: {e}")
            if covered is None:
                reason = f"the scoring queries did not complete: {e}"
                return {"mode": "batch", "status": "not_scored", "reason": reason}
            return not_scored(f"the score query did not complete: {e}")

        _write_recall_json(cfg, run_id, record)
        print_success(scoring_count_line(record))
        return record
    except Exception as e:  # noqa: BLE001 -- best effort, never fails the run
        print_warning(f"Financial scoring did not complete: {e}")
        return None


def _not_scored_record(reason: str, covered: dict | None) -> dict[str, Any]:
    """The record of a score the queries refused: batch has no covered
    block; covered carries the tick's snapshot ids."""
    if covered is None:
        return {"mode": "batch", "status": "not_scored", "reason": reason}
    body: dict[str, Any] = {
        "mode": "covered",
        "status": "not_scored",
        "reason": reason,
        "computed_by": "lb-score-query",
        "covered": {f"{name}_snapshot": covered.get(key) for name, key in COVERED_OPTIONS},
    }
    return body


def continuous_scoring(
    cfg,
    run_id: str,
    tick: dict | None,
    tick_reason: str,
    job_manager,
    monitor,
    timeout,
    *,
    interrupt=None,
    run_failed: bool = False,
) -> dict[str, Any]:
    """``financial_scoring`` for a continuous AML run: the covered score of
    the last completed tick inside the window, or ``not_scored`` with the
    reason."""
    if run_failed:
        return not_scored("the run failed its gates")
    if tick is None:
        return not_scored(tick_reason)
    summary = score_financial_via_queries(cfg, run_id, covered=tick, timeout=timeout)
    if summary is None:
        return not_scored("the score query did not complete")
    if summary.get("mode") != "covered":
        return not_scored("the score did not run in covered mode")
    if summary.get("status") == "not_scored":
        return summary
    summary["tick"] = {
        "cycle": tick.get("cycle"),
        "pinned_at": tick.get("pinned_at"),
        "completed_at": tick.get("completed_at"),
    }
    return summary


# -- time-travel retention check (after the window) ---------------------------


def run_time_travel(
    cfg,
    run_id: str,
    continuous: dict | None,
    *,
    timeout: int = 300,
    run_failed: bool = False,
) -> dict[str, Any] | None:
    """Check the snapshots the run\'s ticks recorded are still readable, and
    record ``continuous.time_travel``.

    Replaces the time-travel Spark job: a metadata read of each table\'s
    ``$snapshots`` says whether the tick snapshot is present, expired (the
    maintenance round that expired it is named) or missing without one; for a
    present snapshot one timed point read confirms a case\'s evidence is still
    there as of the tick. Runs after the covered scorer with the streams
    stopped, so nothing it does is inside a measured interval, and it reads
    through the benchmark query engine, not Spark. A run that failed its gates
    is not read (as it is not scored). Never raises and never fails the run; a
    step that cannot run reads ``not_run``. Returns the record, or None when
    the run has no continuous block.
    """
    from lakebench.aml import time_travel_queries as ttq
    from lakebench.metrics import time_travel as tt_mod

    if continuous is None:
        return None
    pol = tt_mod.policy(
        continuous.get("retention"), cfg.architecture.pipeline.sustained.retention_threshold
    )
    try:
        if run_failed:
            return tt_mod.not_run(continuous, "the run failed its gates", pol)
        records = list((continuous.get("time_travel") or {}).get("ticks") or [])
        if not records:
            return tt_mod.not_run(continuous, "zero recorded snapshots", pol, verdict="fail")

        from lakebench.benchmark.executor import get_executor

        executor = get_executor(cfg)
        console.print()
        console.print("[bold]Stage: time-travel reads[/bold]")
        print_info(
            f"Re-reading {len(records)} recorded transactions snapshot(s) after the window..."
        )
        tt = ttq.retention_record(
            continuous, executor.query_rows, executor.catalog_name, pol, timeout=timeout
        )
        (print_success if tt["verdict"] == "pass" else print_warning)(tt_mod.line(tt))
        return tt
    except Exception as e:  # noqa: BLE001 -- a measurement; never fails the run
        print_warning(f"Time-travel reads failed ({e}); recorded as not run.")
        return tt_mod.not_run(continuous, f"the time-travel step failed: {e}", pol)


def settle_time_travel(cfg, run: Any, reason: str, pipeline_success: bool = True) -> None:
    """The last word on ``continuous.time_travel``, in the run's finally
    block: a continuous AML run that ended before the time-travel step reads
    ``not_run`` with *reason*, so the record never lacks a verdict; one that
    failed a check after the reads (*pipeline_success* False) does not keep
    their verdict, as it does not keep its score (the per-tick results stay
    for diagnosis)."""
    if run is None or cfg.architecture.workload.schema_type.value != "financial":
        return
    continuous = getattr(run, "continuous", None)
    if continuous is None:
        return
    from lakebench.metrics import time_travel as tt_mod

    verdict = (continuous.get("time_travel") or {}).get("verdict")
    if verdict is not None and verdict != "not_run" and not pipeline_success:
        tt = continuous["time_travel"]
        tt.update({"verdict": "not_run", "reason": "the run failed its gates"})
        return
    if verdict is None:
        tt_mod.not_run(
            continuous,
            reason,
            tt_mod.policy(
                continuous.get("retention"),
                cfg.architecture.pipeline.sustained.retention_threshold,
            ),
        )
