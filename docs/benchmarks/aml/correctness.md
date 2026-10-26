# AML benchmark: correctness contract

## 5. Correctness contract

A PASSED batch run asserts the gated rows below, including the record gates
the verdict applies before the run is saved. Row relations between layers
(bronze = generated rows, `silver.transactions` = bronze, the relations of
[2.3](data-model.md#23-silver-tables)) are expected at a fixed seed but not
checked; check them from `metrics.json` (`jobs[].output_rows`,
`jobs[].silver_tables`) before publishing.

| Check | Where | Gates? |
|---|---|---|
| Non-empty datagen prefix at `--generate` without `--regenerate` ([4.1](pipeline.md#41-batch-mode)) | `lakebench run` | yes (exit 3 before datagen; exit 4 when bronze cannot be listed) |
| Datagen wait budget exceeded | `lakebench run` | yes (exit 1; "datagen timed out") |
| Missing required bronze column; NULL settlement date | bronze-verify | yes (stage fails) |
| NULL `uetr`/`txn_id`/`msg_id` | bronze-verify | no (warning) |
| Silver frames non-empty, accounts = distinct IBANs, output > 0 | silver-build | yes (stage fails) |
| `detection_status` written | gold-finalize | yes (stage fails) |
| Any stage failure | `lakebench run` | yes (exit 1) |
| Any rule `error`; zero alerts (the scorer's `financial_scoring.total_alerts`, else per-rule log counts) | AML batch gate | yes |
| Scoring failed or refused the corpus (`financial_scoring.status: not_scored`) | AML batch gate; record gate `aml_rules` | yes, with the reason |
| Per-rule counts and the scorer's alert total both missing | AML batch gate | CLI warns; the `layer_rows` record gate fails a gold-finalize whose log was not read |
| A skipped rule whose target typology is in the pre-registered behavioural subset | AML batch gate | no (warning, every run) |
| Every layer has rows (a continuous layer with no row figure passes on its measured size) | record gate `layer_rows` | yes |
| Batch: expected rules ran, none errored, detection alerted, every skip allowed (W1 `giant-component` or `vertex-cap`; W3 and W17 `path-cap`). A W3 or W17 `edge-cap` skip is not allowed | record gate `aml_rules` | yes; an allowed cap skip passes, labelled in `verdict.qualifiers.rule_caps` |
| Continuous: on the scored [tick](../../glossary.md#tick) (without scoring, the last completed and committed tick), every expected rule ran with no skip (path-cap included) or error and no mode-excluded rule ran; gold-refresh counted alerts. With no scored-tick status recorded, rules come from the time-to-detect lines (warning) | record gate `aml_rules` | yes |
| Batch bronze reached 95% of the scale's expected volume | record gate `scale_ratio` | yes |
| TM invariants ([8.5](tm-operations.md#85-tm-operations)) | TM verdict | only on `fail`; `unknown` (an unchecked invariant, an interrupted pass, an unparsed cycle log), `not_run` and `disabled` warn |
| Any failed query; a non-`allow_empty` query with 0 rows | benchmark gate; record gate `query_answers` (0 rows only) | yes. Batch: the scored round. Continuous: a failed query in any round fails; a timeout on a query that passed in an earlier round is recorded in `query_sla_crossings` instead. The empty check applies to the last round only |
| Recall, precision, false-positive rate | scoring (SQL on Trino) | no; scoring is best effort |
| Subject-customer check (every planted subject is a customer) | scoring (SQL on Trino) | no (warning) |
| Reference detector, band-leakage report, fidelity gate | `financial reference-score`, `scripts/aml_gate.py` | not part of `run` |
| Continuous: data arrived in the window, silver committed and gold refreshed on new data at least twice inside it, gold freshness measured | continuous window gate | yes |
| Continuous: gold-refresh logs present, a cumulative-alerts line, peak alerts > 0 | AML continuous gate | yes |
| Continuous: ingest ratio at least 0.95, unless the [trickle](../../glossary.md#trickle) bounded intake and the pipeline kept pace, or a capacity run with bronze at capacity (then a warning). Not scored (a warning, not a failure) when the bronze driver log was incomplete, since the row count it is computed from is then partial | record gates | yes |
| Continuous: balanced; no handoff's lag rose by more than one cadence across the window's second half | balance check | no (warning); recorded and shown in the report's Balance card, peak gold freshness beside it |

No AML query has fixed expected results, and Lakebench does not compare
query answers between runs: a reader compares row counts by eye
([10](comparability.md#10-comparability)), and an answer wrong the same way
on both sides is not detected. The detection gate reads the last gold-finalize
job. The TM verdict reads every cycle and is `unknown` when any cycle's log
was not parsed.
