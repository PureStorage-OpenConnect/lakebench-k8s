# Verdict

Reference: what a PASSED verdict asserts, the continuous gate, what happens after the window, and requested versus effective values.

## What a PASSED verdict asserts

The verdict (`verdict.status`, `verdict.gates`) is decided from the saved record, so `run` and `report` agree. A failed or interrupted run did not pass. Besides every stage succeeding and no query failing, PASSED shows:

- **Rows in every layer** (`layer_rows`).
  - Batch: the last bronze-verify, silver-build and gold-finalize job each recorded more than 0 output rows. An unread driver log counts as 0; a missing stage job fails.
  - Continuous: bronze-ingest output rows, silver-stream rows after transforms, and gold-refresh output rows above 0. AML silver logs only committed batches' rows, which stand for it. AML gold-refresh logs no row count; the alerts its time-to-detect lines counted stand for it, so no alerts fails.
  - A continuous layer with no row count passes on bytes alone, warning "rows not measured for gold; bytes > 0", listed in `verdict.qualifiers.layer_rows_unmeasured`. Release evidence refuses that.
- **The expected AML rules ran** (`aml_rules`, financial runs).
  - Batch: no rule errored, detection produced alerts, every rule ran except an allowed skip (`W1_connected_components` for `giant-component` or `vertex-cap`; `W3_round_tripping` or `W17_layering_chain` for `path-cap`). A cap skip is labelled in `limits.bound` and `verdict.qualifiers.rule_caps`; any other skip fails. A gold log with no per-rule counts is a warning.
  - Continuous: no rule ran that the mode leaves out, detection produced alerts, and every expected rule ran without skip or error on the scored [tick](../glossary.md#tick) (when scoring did not run, the last gold cycle completed during the post-window [drain](../glossary.md#drain)). A path-cap skip fails. With no rule status from either tick, the gate falls back to time-to-detect lines and warns.
- **The scale's data** (`scale_ratio`, batch): at least 0.95, as stored to 3 places (never rounded up). 0 (bronze not measured) fails. Multi-cycle: the last bronze-verify's ratio, which reads every cycle.
- **Answers** (`query_answers`): no successful query returned 0 rows unless declared to allow it. Continuous runs: last in-stream round only, and a failed Q9 in a round is tolerated (gold refresh replaces its table).

Other verdict rules:

- A `run --stage` run is judged on its stage's layer, on the rules for gold-finalize, and on the scale ratio for bronze-verify.
- Record not PASSED though every printed check passed: `run` prints `Verdict: <reason>`, exits 1.
- `report` takes the stricter of the stored verdict and one recomputed from the record, so an older record can read failed now.

## Continuous gate

A continuous run passes only on continuous processing inside the window. With gold on an interval, it is refused before start when `run_duration` is under 3 x `gold_refresh_interval`. It fails when:

- a stream never reached RUNNING, was not RUNNING at window close, or restarted inside it (new driver pod or submission);
- bronze ingested no rows inside the window (for example it drained the corpus first), wrote fewer than 2 batches, or wrote its last before half the window;
- silver committed fewer than 2 micro-batches with rows after bronze's first write in the window (a pre-window backlog does not count);
- gold refreshed on new silver data fewer than 2 times after that write (a `(silver idle)` cycle, one that read no silver, or one that read no more silver rows than the last does not count);
- gold freshness was not measured inside the window;
- a stream's driver log could not be read;
- the pipeline was not balanced: a stage's lag, sampled once per batch, rose by more than one cadence across the second half. The bottleneck line names the stage and executor setting ([continuous-tuning.md](continuous-tuning.md)). With under three samples there, the end lag must be no larger than the first half's peak, or within two cadences. Unmeasurable balance does not fail. Datagen to bronze is not judged under `--skip-generate` or with `max_files_per_trigger` set.

Corpus out after half the window: the run passes (`window_arrival_fraction` shows it), and freshness skips gold cycles after silver stopped growing.

- Submission failures (such as a truncated Maven download): printed, journaled, `streaming[].submission_failures`. RUNNING time: `running_at`.
- `continuous.balance`: `balanced`, `measured`, `bottleneck`, `lever`, and per handoff (`datagen->bronze`, `bronze->silver`, `silver->gold`): `samples`, `first_half_max_s`, `end_s`, `cadence_s`, `second_half_growth_s`, `allowance_s`, `keeps_up`, `busy_share`, and `not_judged` with a reason. `trend_samples` keeps the per-batch lag samples.

## After the window

- The streams stop at window end; the run does not wait for silver and gold to finish the rows bronze took. AML gold-refresh first finishes its tick ([drain](../glossary.md#drain)).
- The in-stream rounds are the only query checks: a failed query fails the run (a Q9 failure is tolerated, as contention with gold refresh), and empty answers count in the last round only (`query_answers`, above). Lakebench does not check answer values.
- A Customer 360 run with a benchmark fails when no in-stream round ran, or Q9 failed in every round (`c360 continuous gate: ...`).
- `experiment.results.query_set_id` names every query the rounds ran, failed ones included; AML gets the 12-query set once a round ran the investigator queries.

## Requested and effective values

`experiment.requested_effective` keeps both for each decision Lakebench or a Spark job makes, each `{requested, effective, source, stage}`:

| Key | Requested | Effective |
|---|---|---|
| `gold_strategy` (Customer 360) | `spark.lb.gold.strategy` (`auto` when unset) | what each gold-finalize job reports it ran, and why (`auto`, `override`, or `cycle` for cycles 2+). Several jobs: `gold_strategy[cycle=N]`. |
| `pipeline_mode` | the mode the command asked for | the pipeline the record shows ran (`not recorded` with no stage) |
| `executors[<job>]` (cluster runs) | the override, else the profile's count at this scale under its cap | batch: most executor pods listed while it ran, sampled every few seconds (replacements count again); stream: the count submitted. Source names an executor cap or concurrent budget that applied. |
| `trickle` (continuous) | `max_files_per_trigger` as configured, or `auto` | the resolved value |

- Only an executor override that ran with fewer executors is a mismatch.
- An unmet request is a qualifier (`verdict.qualifiers.requested_effective`) and a report warning, such as "gold_strategy: requested two_phase_agg, ran simple_agg (auto)".
- Incremental gold a job chose automatically is labelled even under `auto` (it aggregates part of silver; gold jobs no longer make that choice). Incremental gold for a multi-cycle cycle is by design and is not.
- Mismatches are decided at read; `experiment.requested_effective_mismatches` keeps the run's list.
- A mismatch never fails a run or enters the experiment identity. Other caps are in `experiment.limits` and `limits.bound`.
