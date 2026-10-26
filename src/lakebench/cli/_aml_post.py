"""AML steps after the detection stages: recall scoring for batch and
continuous runs, and the time-travel reads of the snapshots the continuous
ticks read.

The continuous AML scorer runs over the snapshots the last completed tick
inside the window recorded (covered mode). Each table is read at the
snapshot that tick pinned (FOR VERSION AS OF), so gold.alerts being
rewritten by a later tick does not reach the score.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

from lakebench.cli._helpers import console, esc, print_info, print_success, print_warning

GOLD_REFRESH_APP = "lakebench-gold-refresh"

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
    from lakebench.s3 import S3Client

    s3 = cfg.platform.storage.s3
    return S3Client(
        endpoint=s3.endpoint,
        access_key=s3.access_key,
        secret_key=s3.secret_key,
        region=s3.region,
        path_style=s3.path_style,
        ca_cert=s3.ca_cert,
        verify_ssl=s3.verify_ssl,
    )


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
        print_warning(f"Could not write recall.json: {esc(str(e))}")


def score_financial_via_queries(
    cfg,
    run_id: str,
    *,
    covered: dict | None,
    timeout: int,
) -> dict[str, Any] | None:
    """Score recall and precision with SQL on the query engine (the scorer
    that replaces the score_financial Spark job).

    Covered mode: *covered* is the scored tick record; every table is read at
    the snapshot that tick pinned and alert_set comes from the tick. Batch:
    *covered* is None, tables read at their current snapshot. Returns the
    financial_scoring record, or None only on an unexpected failure.
    Best effort: a scoring failure never fails a pipeline that already
    succeeded.
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

        _write_recall_json(cfg, run_id, record)
        print_success(scoring_count_line(record))
        return record
    except Exception as e:  # noqa: BLE001 -- best effort, never fails the run
        print_warning(f"Financial scoring did not complete: {esc(str(e))}")
        return None


def _not_scored_record(reason: str, covered: dict | None) -> dict[str, Any]:
    """The not_scored record, in the shape the Spark scorer wrote: batch has
    no covered block; covered carries the tick's snapshot ids."""
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


# -- time-travel reads (after the window) -------------------------------------

#: Seconds the job's deadline keeps back from the CLI's wait, for writing
#: its partial result (``incomplete``) and stopping. The job also starts no
#: scan the time left could not hold (1.5 times its longest scan so far;
#: the first scan of each pass always starts).
_TT_BUDGET_MARGIN_S = 120
#: How the record labels the time-travel budget (a Lakebench-imposed bound).
TT_BUDGET_LABEL = "BOUNDED BY the Lakebench time-travel budget (the per-job timeout)"


def run_time_travel(
    cfg,
    run_id: str,
    continuous: dict | None,
    job_manager,
    monitor,
    timeout,
    *,
    interrupt=None,
    run_failed: bool = False,
    wall_clock: Callable[[], float] = time.time,
) -> dict[str, Any] | None:
    """Re-read every transactions snapshot the run's ticks recorded
    (``TIME_TRAVEL_FINANCIAL``) and record ``continuous.time_travel``.

    Runs after the covered scorer, with the streams stopped: nothing it
    does is inside a measured interval, and it writes only under the gold
    ``scoring/<run_id>/`` prefix. A run that failed its gates is not read
    (as it is not scored). The job gets an absolute deadline on the
    cluster clock: the CLI's wait (*timeout* from submit) less a margin; a
    wait that ends first deletes the application, so no executor outlives
    the step. Never raises and never fails the run; a job that cannot run
    reads ``not_run``. Returns the record, or None when the run has no
    continuous block."""
    from lakebench.metrics import time_travel as tt_mod

    if continuous is None:
        return None
    submitted = False
    app_name = ""
    policy = tt_mod.policy(
        continuous.get("retention"), cfg.architecture.pipeline.sustained.retention_threshold
    )
    try:
        if run_failed:
            return tt_mod.not_run(continuous, "the run failed its gates", policy)
        records = list((continuous.get("time_travel") or {}).get("ticks") or [])
        if not records:
            return tt_mod.not_run(continuous, "zero recorded snapshots", policy, verdict="fail")
        import json as _json
        import uuid

        from lakebench.spark.job import JobState, JobType

        gold = cfg.platform.storage.s3.buckets.gold
        prefix = f"scoring/{run_id}"
        nonce = uuid.uuid4().hex
        client = _s3_client(cfg)
        fields = (
            "start",
            "cycle",
            "table",
            "snapshot",
            "committed_at",
            "total_records",
            "pos_deletes",
            "eq_deletes",
            "count_source",
        )
        client.raw_client.put_object(
            Bucket=gold,
            Key=f"{prefix}/tt_input.json",
            Body=_json.dumps(
                {
                    "run_id": run_id,
                    "nonce": nonce,
                    "exclude_columns": sorted(tt_mod.SENTINEL_COLUMNS),
                    "records": [{k: r.get(k) for k in fields} for r in records],
                }
            ).encode("utf-8"),
        )
        app_name = f"lakebench-{JobType.TIME_TRAVEL_FINANCIAL.value}"
        budget_s = max(60, int(timeout) - _TT_BUDGET_MARGIN_S)
        # The driver reads the deadline on the cluster clock (cluster minus
        # host offset, when measured).
        offset = (continuous.get("window") or {}).get("cluster_clock_offset_seconds") or 0.0
        deadline = wall_clock() + budget_s + float(offset)
        budget = {"budget_s": budget_s, "label": TT_BUDGET_LABEL}
        console.print()
        console.print("[bold]Stage: time-travel reads[/bold]")
        print_info(
            f"Re-reading {len(records)} recorded transactions snapshot(s) after the window..."
        )
        if interrupt is not None:
            interrupt.creating("SparkApplication", app_name)
        status = job_manager.submit_job(
            JobType.TIME_TRAVEL_FINANCIAL,
            arguments=[
                "--input",
                f"s3a://{gold}/{prefix}/tt_input.json",
                "--hashes",
                f"s3a://{gold}/{prefix}/tt_hashes.json",
                "--output",
                f"s3a://{gold}/{prefix}/time_travel.json",
                "--deadline-epoch",
                f"{deadline:.3f}",
            ],
            cycle_env={"LB_RUN_ID": run_id},
        )
        if interrupt is not None:
            interrupt.submitted(status)
        submitted = status.state != JobState.FAILED
        if status.state == JobState.FAILED:
            return tt_mod.not_run(
                continuous, f"the job was not submitted: {status.message}", policy
            )
        result = monitor.wait_for_completion(app_name, timeout_seconds=timeout, poll_interval=15)
        if interrupt is not None and result.success:
            interrupt.finished("SparkApplication", app_name)
        if not result.success:
            _delete_tt_job(job_manager, app_name)
            submitted = False
            return tt_mod.not_run(continuous, f"the job did not complete: {result.message}", policy)
        submitted = False
        body = client.raw_client.get_object(Bucket=gold, Key=f"{prefix}/time_travel.json")
        out = _json.loads(body["Body"].read())
        if not isinstance(out, dict) or out.get("nonce") != nonce:
            return tt_mod.not_run(continuous, "time_travel.json is not this submission's", policy)
        tt = tt_mod.merge(continuous, out, policy, budget)
        (print_success if tt["verdict"] == "pass" else print_warning)(tt_mod.line(tt))
        return tt
    except Exception as e:  # noqa: BLE001 -- a measurement; never fails the run
        if submitted:
            # The wait raised: the job may still run; it must not outlive the step.
            _delete_tt_job(job_manager, app_name)
        print_warning(f"Time-travel reads failed ({e}); recorded as not run.")
        return tt_mod.not_run(continuous, f"the time-travel step failed: {e}", policy)


def _delete_tt_job(job_manager, app_name: str) -> None:
    """Delete the time-travel application (best effort, said when it fails)."""
    try:
        job_manager._delete_job(app_name)
    except Exception as e:  # noqa: BLE001 -- said; the record stands
        print_warning(f"Could not delete {app_name}: {e}")


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
