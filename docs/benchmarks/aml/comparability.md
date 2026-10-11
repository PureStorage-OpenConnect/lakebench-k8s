# AML benchmark: disclosure and comparability

## 9. Disclosure requirements

`metrics.json` carries an `experiment` block, schema `exp2` or `exp1`
([7.3](execution-rules.md#73-prohibited-changes-invalidate-a-result-or-are-refused);
`v2_unavailable` names what was missing, `metrics/experiment.py`). A
published AML result must show:

| Field | Source in metrics.json |
|---|---|
| Workload `financial`, version `aml-3`, `parameters_id` and parameters | `experiment.workload` (`parameters` holds the TM operations settings, `w1_max_vertices`, `retention_workload`, `retention_months`) |
| Generator model version | `experiment.workload.generator_model_version` |
| Configured image, observed pod image and digest, seed, corpus role, perturbation, corpus id, corpus id v2 or why there is none, and whether the corpus was observed or only declared | `experiment.corpus`, `experiment.corpus.datagen`, `experiment.corpus.id_v2` (or `id_v2_unavailable`), `experiment.corpus.observed` |
| Recipe, catalog, format, engine and query-engine versions, query access path | `experiment.architecture`, `experiment.architecture.access_paths` |
| Support state and basis | `experiment.support` |
| System | `experiment.system` (`cluster` or `local`), `experiment.system_identity` (the observed cluster and object-store fingerprint) |
| Scale, mode | `experiment.corpus.scale`, `experiment.mode` |
| Maintenance policy id, settings, effective outcome per operation | `maintenance_policy_id`, `experiment.maintenance_settings`, `experiment.effective_maintenance` |
| Stages executed and skipped; rules executed, skipped (with reason) and errored | `experiment.stages`, `experiment.rules`. Continuous: disclose the mode-excluded W1, W7 and W8 from `experiment.support.mode_note` (they are not in `experiment.rules.skipped`) |
| Caps configured and caps that bound | `experiment.limits` (`bound`, `bound_kinds`; continuous `trickle_bound`) |
| Repetitions | `experiment.repetitions` (each record is one run, `runs: 1`; `run --repeat` records a series whose members are listed in its manifest; label a one-run figure n=1) |
| Query set id and per-query fingerprints, or `not_checked` with the reason | `experiment.results.query_set_id`, `.fingerprints`, `.not_checked` |
| AML scoring and its mode | `financial_scoring` (`mode`, `status`, `reason`) |
| Code provenance | `provenance`, `experiment.lakebench` (`lakebench_version`, `git_sha`, `git_dirty`, `install`, `tree_sha256`, the dependency set and observed image digests) |

Continuous runs also disclose the window, `continuous.trickle`,
`limits.trickle_bound`, `intake_limit` and `corpus_ingest_ratio`: a window
that did not drain the corpus measures a subset of it, and the arrival rate
is Lakebench-set. Disclose every skipped rule and any skipped-rule warning
the batch gate printed (not enforced).

## 10. Comparability

Lakebench does not compare runs; a reader compares two reports
([Comparing runs](../../benchmarking/comparing.md)). Two AML runs are
comparable only when (`metrics/comparability.py`):

- both passed and carry experiment blocks of the same identity version;
- every Workload and Corpus identity key
  ([7.3](execution-rules.md#73-prohibited-changes-invalidate-a-result-or-are-refused))
  is equal, and neither has a corpus problem (config and datagen pods
  disagree);
- they ran the same query set with every result fingerprint matching, and
  for batch the alert set matches rule by rule (a different alert set is a
  different result; [8.7](scoring.md#87-alert-set)).

- Results are not checked for AML continuous runs
  (`experiment.results.not_checked`) or a `*-none` recipe, so such runs cannot
  show matching answers.
- Differing execution conditions
  ([7.2](execution-rules.md#72-permitted-tuning-still-publishable)), or only
  the dependency pinset, leave the pair comparable but not like-for-like: a
  difference in the numbers may come from them.
  When architecture and system both differ, no difference can be put down to
  either.
- Batch and continuous runs are never comparable (mode is a Workload key).
- A batch run whose TM layer did not run (8 queries) is not comparable with
  one where it ran (12), because the query sets differ.
- `reproduce` refuses a different maintenance policy id.
- Records written by Lakebench 1.6 (exp1, workload `aml-1`), including the
  three cited runs, are not comparable with this release's records: they are reference figures, not baselines.
