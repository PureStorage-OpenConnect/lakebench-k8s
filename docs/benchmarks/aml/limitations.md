# AML benchmark: known limitations

## 12. Known limitations

- **Proven on one system type.** Every published AML record ran on OpenShift
  with Portworx scratch and FlashBlade S3. No minimum cluster is published;
  `lakebench plan` reports one per config.
- **Only release-matrix cells can be supported.** AML is supported only on
  the cells the [support table](../../compatibility-matrix.md#support-states)
  lists; every other valid combination
  ([11](../AML.md#11-supported-compositions)), Spark 3.5 and 4.0 included, is
  unverified. AML is unverified above scale 300 and refused above 800.
- **Batch detection covers 9 of 17 planted types, 8 when W1 skips**
  ([3.6](typologies.md#36-typologies)). W1 skips as `giant-component` (seen
  at scale 1 and 10) or `vertex-cap`, a Lakebench cap (seen at scale 100).
  A skipped W1 leaves the pre-registered behavioural typology
  `gather_scatter` with no detector, and the run warns. Recall describes
  this rule set, not the architecture.
- **Continuous runs a different workload slice.**
  - W2, W3, W4, W5 (transaction screen only), W6 and W17 run every
    [tick](../../glossary.md#tick); W1, W7 and W8 do not, and their
    typologies read `mode-excluded`.
  - Covered scoring leaves the sanctions instances only a rescreen can find
    out of recall and counts them
    (`covered.typologies[].rescreen_only_excluded`).
  - Each tick re-detects only the alerts that rows new since the rule's last
    pass can change: it reads silver over their days widened each way by the
    rule's reach (two days for W2, a week for W4, about six weeks for W3,
    eight for W17) and merges the result with the standing alerts.
  - The alerts equal a full recompute over the same silver while no path cap
    binds (W3 and W17's caps apply to the rows a tick reads). Tick cost
    follows the arrival rate, not the corpus.
  - Continuous W4 raises one alert per entity per week of payments; batch W4
    one per entity over the corpus.
  - Continuous results are not comparable with batch.
- **Continuous recall is `recall_covered`**
  ([8.4](scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails)),
  not batch `recall`; no published record carries one yet.
- **Continuous results are never checked across runs**: alerts depend on
  when ticks ran, so a comparison including one is at best NOT ESTABLISHED.
- **Continuous throughput follows the offered load** (4 MB/s per scale unit by
  default). When the pipeline keeps up, `sustained_throughput_rps` is that
  load; it is a capacity only when datagen stays ahead with bronze busy
  (`datagen_ahead`, `intake_limit: bronze_capacity`). Under `--skip-generate`
  or a set `max_files_per_trigger` the [trickle](../../glossary.md#trickle)
  can bound it; `limits.trickle_bound` says whether it did, and
  `limits.bound` names it when it did.
- **No expected results and no row relations checked**
  ([5](correctness.md#5-correctness-contract)). Correctness across
  architectures rests on equal alert sets between batch runs; query answers
  are compared by eye.
- **Repeatability.** Each record is one run (`runs: 1`). `run --repeat` (up to
  20 runs) runs a series on one corpus; Lakebench does not summarise it, so
  read the members' records for the spread. Every published AML figure,
  investigator and time-travel included, is n=1 (one run per arm) on one
  system.
- **The batch corpus is fixed in time and zone-less**
  ([3.5](generation.md#35-content-time-range-and-dirty-data)).
  `corpus.timestamp_start` and `timestamp_end` are the config's values, not
  the corpus's range.
- **Corpus id v1 hashes inert fields**: on exp1 records an edit to a field
  the AML generator ignores (the timestamps, `dirty_data_ratio`, the
  Customer 360 `unique_customers`) makes identical corpora not comparable.
  Only an image that writes corpus markers produces v2
  ([3.1](generation.md#31-generator-identity)).
- **Bronze tables are unpartitioned** (zero-copy registration), so a bronze
  scan prunes only on per-file min/max statistics.
- **TM operations figures are simulations**
  ([8.5](tm-operations.md#85-tm-operations)); recall and false-positive rate
  are measured from alerts against the manifest.
- **The reference-model score reads unsealed silver.** `lakebench financial
  reference-score` reads silver without the sealed-batch filter the rules and
  the covered scorer use. Rows from a batch
  whose seal marker never landed (a crash between the transaction commit and
  the marker) are counted there and nowhere else.
- **The [look](../../glossary.md#look) guard is per host.** The corpus ledger
  recording a registered corpus and its bronze prefix is a local file
  ([held-out seed protocol](../../development.md#held-out-seed-protocol)).
  A config with no protected `datagen.corpus_role` on another host pointed
  at that prefix is refused only by bronze-verify's manifest check, after
  the deploy. Take every look from one host.
- **`--generator-image` checks the corpus only on a registered look.** A
  registered look or calibration shard through `scripts/aml_gate.py` needs a
  digest-pinned reference (`...@sha256:<digest>`). A registered look refuses
  one that is not the same string the per-typology predictions used
  (`repo:tag@sha256:<digest>` and `repo@sha256:<digest>` for one image are
  refused against each other), and a corpus this host's ledger does not
  record as generated on that digest. A calibration shard's corpus is not
  checked; compare its markers' `build_commit` with
  `src/lakebench/config/datagen_lineage.yaml` by hand.
- **A lost executor can fail a silver micro-batch.** An executor lost while
  a silver-stream MERGE runs fails that micro-batch, and the stream restarts
  through its replay path.
- **No counter-leakage hard negatives.** The generator plants no legitimate
  accounts built to mimic a typology's shortcut signature (for example
  long-quiet seasonal or travel accounts that break the dormancy gap
  signature). The leakage gate runs, but a detector can still score on such a
  shortcut.
- **Bronze-ingest restarts longer than 1 h are not safe.** Iceberg snapshot
  expiry is floored at 1 h while streams are live, so a bronze-ingest driver
  down longer can replay a batch and append duplicates. Bronze expiry is held
  back from the silver stream's committed offset, so a silver restart is not
  limited this way.
- **AML datagen throughput is reported, not gated**
  ([8.3](metrics.md#83-both-modes)).
