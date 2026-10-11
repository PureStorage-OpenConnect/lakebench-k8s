# AML benchmark: gold-finalize timing

## 8.6 Gold-finalize timing

The gold-finalize entry in `metrics.json` (`jobs[]`, job type
`gold-finalize`) records, besides `alerts_by_rule`:

**`rule_elapsed_s`**: wall seconds per detection rule that started (ran,
failed, or skipped for a structural reason such as W1's vertex cap), from the
rule's start to its alerts' commit.

**`stage_profile`**: per rule, its three heaviest Spark stages by summed
executor run time (`exec_s`), with status, task count, wall time, longest task
(`max_task_s`), shuffle read in MB, and the number of stages the rule ran.

- Each rule runs in its own Spark job group, `lb-rule-<rule>-<id>`, which
  tells its stages apart. Stages are read from the driver's status store after
  the rule's commit, outside `rule_elapsed_s`.
- `complete: false`: the status listener had not caught up within 5 s. The
  wait covers every listener queue, so with Spark's event log on, `complete`
  can read false while the status store had caught up.
- `truncated: true`: the store had already dropped some of the rule's jobs or
  stages (for AML gold-finalize it keeps the last 1,000 of each, enough for a
  whole rule; 100 for every other job).
- `lossy: true`: the listener dropped events during the rule, so task totals
  are low. When the starting point could not be read, `truncated` and `lossy`
  are both true.
- An empty list means the rule ran no stage. With no usable list (store
  unreadable, or no stage of the rule while a flag is set), the rule is in
  `stage_profile_unavailable` with the reason.
- Detection is never affected. The wait adds at most 5 s per rule, nothing
  when the listener keeps up. `stage_profile_cost_s` records each rule's read
  time: Lakebench overhead inside the job's time, never in `rule_elapsed_s`.
  The continuous gold [tick](../../glossary.md#tick) does not profile.

**`tm_ops.phases`**: wall seconds per TM pass stage, in pass order `pin`,
`reconcile`, `prior_state`, `plan` (alert-input, replay and disposition
plans), `write_ledger`, `inputs` (the alert-input build), `simulate` (the
per-customer replay), `write_dispositions`, `write_cases`, `coverage`,
`read_back`, `recon_write`, `invariants`.

- Together they make up `tm_ops.elapsed_seconds`.
- The JSON keys are sorted, not in pass order.
- Spark evaluates lazily, so a phase holds the work its own reads and writes
  trigger.
- The alert inputs and the replay are materialised in their own phases,
  after the cycle is recorded as started, so a failure there fails the pass.

**`alert_set_seconds`**: seconds the alert-set fingerprint took
([8.7](scoring.md#87-alert-set)). It runs last in the gold-finalize pod, after
the TM pass.

- It is Lakebench's work, not the pipeline's. So the CLI takes it off the
  stage's `elapsed_seconds` and end time, and so off CPU-seconds, as for the
  Customer 360 check.
- The report prints it beside the stage time ("excludes 1.2s of Lakebench's
  alert-set fingerprint").
- Time to value loses it in a single-cycle run. With `cycles` above 1 the
  earlier cycles' fingerprints stay inside time to value, as the Customer 360
  check's do.

Two diagnostic blocks derive from these; neither enters identity, a verdict
or a comparison:

- `experiment.attribution` (AML batch, `metrics/attribution.py`):
  - the slowest rule (`dominant_rule`, its `rule_elapsed_s` and
    `share_of_job`, the rule's time over the job's);
  - that rule's heaviest stage (`dominant_stage`: stage id, name, tasks,
    executor seconds, wall seconds, longest task, its share of the executor
    time of the rule's three logged stages, and the profile's flags);
  - the TM pass's time and share (`tm_elapsed_s`, `tm_share`);
  - `profile`: `read`, or why the stage is missing (`unavailable:
    <reason>`, `no_stage`, `missing`).
- `limits.headroom_pct` (batch): per stage, `100 x (1 - elapsed / per-job
  timeout)` against the per-job timeout the run gave every stage (recorded as
  `job_timeout_seconds`).
  - A stage that ran more than once reports its slowest run; a failed stage
    reads null.
  - The benchmark phase has no per-job timeout, since its queries are
    bounded one by one. So `benchmark_query` is `100 x (1 - slowest timed
    query sample / per-query timeout)` (recorded as
    `benchmark_query_timeout_seconds`, 900 s for AML).
  - `benchmark_query` is null when a query failed or `lakebench benchmark`
    later replaced the benchmark.
  - 25 or more means at most 75% of the budget used.
