# AML benchmark: TM operations

See also: config keys in [configuration.md](../../configuration.md#workload----customer-360-and-aml).

## 8.5 TM operations

`tm_operations.status` is `pass`, `fail`, `not_run`, `disabled` or `unknown`;
only `fail` (a violated workflow invariant) fails the run. In continuous mode
the operations pass runs every `continuous_interval_seconds` (default
1,800 s), so a shorter window may record `not_run`.

`tm_operations.ops` reports the alert funnel (payments, monitored, alerts,
escalated, cases, SARs), dispositions, priorities, alert aging and SLA
breaches, case status, filing-days median and p95, late filings and
continuing reviews. Every one depends on simulated parameters
(`spark/scripts/tm_operations.py`, recorded in `tm_operations.ops.simulation`),
so they measure the simulation, not the architecture:

| Parameter | Default |
|---|---|
| analyst accuracy | 0.90 |
| investigator accuracy | 0.95 |
| QA sample | 0.05 |
| late filing | 0.03 |
| no-suspect | 0.05 |
| simulation seed | 20260924 |
| SLA | 60 days |
| lookback | 12 months |
| counterparty scenarios | the scenario list |
| `max_alerts_per_customer` (a Lakebench cap) | 50,000; alerts past it are dispositioned `over_capacity` and the cap is listed in `experiment.limits.bound` |

**The model.** One reporting institution monitors its own customers
(`silver.entities.is_customer`); everyone else is a counterparty. The unit of
work is the business day. An alert is generated the day after its last
payment, and the queue is replayed day by day up to the cycle's as-of date
(the day after the newest payment). Anything decided later is the open
backlog.

| Table | Stage | What it holds |
|---|---|---|
| `gold.tm_reconciliation` | 1 | one set of rows per cycle (batch) or operations pass (continuous), appended: customers; source, bronze and silver payments; monitored vs excluded by reason (`no_customer_party`, `dq_unconvertible_currency`, and `in_flight` in continuous); DQ rule failures; the funnel alerts -> escalated -> cases -> SARs |
| `gold.scenario_coverage` | 1 | scenario-to-typology matrix with this cycle's rule status, alert volume and planted instances; planted typologies no scenario targets appear as `gap` |
| `gold.alert_dispositions` | 6 | triage priority (scenario weight x CRR tier: low, medium, high, critical); a never-NULL disposition (below); aging against the SLA; QA re-review; the alert's first-seen cycle |
| `gold.cases` | 7, 8 | customer-keyed cases, at most one open per customer, with the alerts they pulled in and the payments of the lookback window; determination, `sar_filed` / `no_sar`, the filing limit that applied, the continuing-activity review and what happened to it |

`gold.alert_dispositions` dispositions:

- `escalated`, `closed_nfa`, `attached` (to the customer's open case),
  `pending_l1`;
- `out_of_scope` (non-customer);
- `over_capacity` (past the per-customer cap). The cap is applied after
  identity matching, so an alert already in the workflow is never capped.

**Simulated decisions.** An alert is truly suspicious when it touches a
planted (non-control) typology payment. The L1 analyst decides correctly with
probability `analyst_accuracy`; the L2 investigator and QA reviewer with
`investigator_accuracy`. Every draw is a hash of the seed and a stable key
(the alert's identity, the case id), so a rerun reproduces the decisions. The
report says every operations number is conditional on these values.

**Regulatory clocks.**

- 31 CFR 1020.320: file within 30 days of the determination, 60 when no
  suspect is identified (`no_suspect_rate` of new cases).
- FinCEN continuing activity: review 90 days after each SAR; file the
  continuing SAR within 120 days of the prior one.
- A review falling due while the customer's case is under investigation is
  folded into it.
- A review falling due while that case is determined and only waiting to file
  is never credited to its investigation. The SAR that case then files covers
  the activity and is recorded as the continuing-activity filing
  (`superseded_by_sar`).
- Late filings are drawn at `late_filing_rate` and flagged `filed_late`.

**Alert identity across cycles.** Detection re-runs over the whole corpus each
cycle, so an alert's window can grow as payments arrive. The layer matches
each alert to the last completed cycle's, read at the table snapshots that
cycle recorded in the ledger:

- Same content first. Else the same rule on the same customer, largest
  overlap first, where:
  - its last payment moved forward by at most 31 days;
  - it holds at least a quarter of the payments the prior was first raised
    on (a frozen 32-hash sketch, checked against all of the new alert's
    payments), and never fewer than two.
- A new alert sharing a single payment with a prior never takes its
  identity.
- Rules cap the related-payment list by payment id. An alert growing past
  about three times that cap between cycles can lose its identity: carried as
  withdrawn and raised again as new. That inflates alert counts but never
  moves a decision.
- A matched alert keeps its key, generated date, truth and priority as first
  seen. An alert detection stops emitting is kept (`in_current_detection`
  false). A new alert whose payments predate the previous cycle is dated on
  this cycle, the first day it could have been raised.
- The day-by-day replay is causal and its inputs for seen alerts are frozen,
  so recomputing it reproduces every earlier decision; the `history_stable`
  invariant checks that against the previous cycle's table.
- Case activity counts are recomputed each cycle from silver.

**Who a scenario alerts on.**

- W2, W5, W6, W7 and W8 alert only when the subject (`entity_id`) is a
  customer in `silver.entities`; the counterparty stays in
  `related_entity_ids`.
- The graph scenarios (W1, W3, W4, W17) run across the whole payment network
  and can alert on an account at another bank. They are the declared
  `counterparty_scenarios`.
- The scope is fixed in `detection_rules.py` (`CUSTOMER_SCOPED_RULES`). The
  config list only tells the invariant which non-customer alerts to expect.
- A customer-scoped rule on a silver with no customer flag, or no customer,
  reports `skipped`, not zero alerts.

**Workflow invariants**, checked on the tables as written, every cycle:

- the monitored population is not empty; monitored + excluded = source;
- every alert has a disposition row, none NULL, one per alert identity;
- alerts on non-customers come only from scenarios declared
  customer-and-counterparty;
- escalated <= alerts; alert-driven cases <= escalated; SARs <= cases; the
  funnel is monotone;
- at most one open case per customer;
- every SAR past 90 days has its review (or waits on a case pending filing);
  no review is credited to an already-determined case;
- decided history is unchanged from the previous cycle;
- the workflow completed after the TM tables were written.

**The operations verdict** is separate from detection and is kept in
`metrics.json` as `tm_operations`, heading the report section.

| Status | Meaning |
|---|---|
| `fail` | the layer ran and an invariant is violated; fails the run |
| `not_run` | no manifest, a layer error, or a continuous window that ended before the manifest was ready; says why and leaves detection scoring alone |
| `unknown` | no driver log captured for some cycle, or an invariant could not be checked (the raw source files failed to list) |
| `disabled` | skips the gate |

**Continuous passes.** Gold-refresh runs the layer every
`continuous_interval_seconds` (at least 60), counted from the end of the last
pass so detection [ticks](../../glossary.md#tick) always run between passes. A final pass is timed to
finish before the window closes. The close is the first gold-refresh
driver's start, persisted in its checkpoint, plus the window length.

- One pass over the full corpus takes minutes at scale 10, and gold is not
  refreshed meanwhile. A freshness sample is logged after each pass, and the
  pass time counts in the freshness score. Samples after a pass follow the
  tick's own rule (only while data is moving), so a drained corpus's idle
  time never becomes the score.
- The continuous jobs carry the CLI run id, so a restarted driver appends to
  the same ledger. Each pass takes its cycle number in the ledger before it
  writes. A pass that fails after writing is reported as a failure and never
  carried from.
- Silver, then bronze, are pinned before the raw files are counted.
  In-flight payments are bounded, not inferred:
  - Bronze must hold exactly the rows of the raw files its stream's
    checkpoint log says it took up to the pinned snapshot's batch
    (`spark.sql.streaming.epochId`), so the rest are files not yet taken.
  - Bronze rows silver has not taken (by `uetr`) must have landed within
    900 s before silver's newest landing time, since a file can become
    visible after a later-landed one.
  - Anything else is `unaccounted` and fails reconciliation.
- The continuous reset drops all four tables.

**Investigator queries** (class `investigator`; [6](queries.md#6-query-set)):
customer 360 for the top open case, the 12-month activity review of the
newest case, the counterparty and two-hop view, and open cases older than 60
days.

- They read only this run's rows, and are left out unless this run's TM
  verdict is `pass` or `fail`, so a disabled or not-run layer never times
  empty or stale tables.
- The standalone `benchmark` command includes them only when the
  deployment's newest run had a `pass` or `fail` verdict, since each run
  overwrites the tables.
- Continuous rounds include them once the run has a case
  ([4.3](pipeline.md#43-continuous-mode), step 5): the 8-query set runs before
  the first TM pass, labelled `absent_no_cases` (`probe_failed` when the probe
  errors). Such a run's rounds usually span both sets, so its in-stream
  composite QpH reads `blended`, with the median per set in
  `scores.composite_qph_by_set` ([8.2](continuous-metrics.md#82-continuous)).

**Investigators under load** (`architecture.benchmark.investigator_sessions`,
`benchmark/investigator_sessions.py`).

- Set to N (1 to 32) on an AML config with TM operations on Trino or Spark
  Thrift; refused at load otherwise. `run` refuses it in batch, so it needs a
  continuous run.
- It adds one round right after the first in-stream round that included the
  investigator queries (that round is the baseline). N sessions run
  concurrently, each working one case of this run. Cases are picked in IQ1's
  queue order: open cases first, by priority, oldest first; closed cases fill
  in when fewer are open. Each session runs IQ1, IQ2 and IQ3 bound to its
  case and IQ4 unchanged, once each.
- It is not a benchmark round: in-stream QpH and the round count do not move.
  It takes window time, so the round count can be lower than without it.
- Each session's query timeouts are derived from the time left in the window
  so the round ends inside it (`status: no_time` when not enough remains).
- `continuous.investigators` records `sessions_requested`, `sessions_run`,
  per-query latency p50 and p95, the baseline round's time per query,
  failed or empty queries, and `status`.
- `status`: `pass`; `fail` when a session query failed or a session's IQ1 or
  IQ3 returned no rows (fails the investigators check, not the run);
  `no_cases`; `no_rounds`; `no_time`; `case_query_failed`.
- The verdict carries the investigators check and a label noting which
  detection ticks overlapped the session window. Time to detect and
  continuous throughput keep their values and include those ticks.
- `experiment.investigators` holds `{requested, run}`. The identity key
  `investigator sessions` is the number that ran, an outcome condition: runs
  that ran different numbers are not like-for-like.
