# AML benchmark: known limitations

## 12. Known limitations

- **Proven on one system type.** Every published AML record ran on OpenShift
  with Portworx scratch and FlashBlade S3. The minimum cluster for a config
  is what `lakebench plan` reports.
- **Supported tuples outside the release matrix are unverified.** AML is
  supported only on the cells the
  [support table](../../compatibility-matrix.md#support-states) lists; every
  other combination is unverified ([11](../AML.md#11-supported-compositions)).
  Spark 3.5 is accepted but in no release-matrix row, so its runs are
  unverified. AML is unverified above scale 300 and refused above 800.
- **Batch detection covers 8 of 17 planted types**
  ([3.6](typologies.md#36-typologies)). W1 skipped as `giant-component` at
  scale 1 and 10 in the 1.6 batch records; at scale 100 its skip reason is
  `vertex-cap`, a Lakebench cap. With W1 skipped, the pre-registered
  behavioural typology `gather_scatter` has no detector, and every such run warns. Recall
  describes this rule set, not the architecture.
- **Continuous runs a different workload slice.**
  - W2, W3, W4, W5 (transaction screen only), W6 and W17 run every [tick](../../glossary.md#tick); W1,
    W7 and W8 do not, and their typologies read `mode-excluded`.
  - Covered scoring leaves the sanctions instances only a rescreen can find
    out of recall and counts them
    (`covered.typologies[].rescreen_only_excluded`).
  - Each tick re-detects only what rows new since the rule's last pass can
    change. That is alerts around the days those rows fall on, from silver
    over those days widened by the rule's windows (a day for W2, about six
    weeks for W3 and W17, a week for W4), merged with the standing alerts.
  - The alerts equal a full recompute over the same silver while no path cap
    binds (W3 and W17's caps apply to the rows a tick reads). So tick cost
    follows the arrival rate, not the corpus or datagen pod drift.
  - Continuous W4 raises one alert per entity per week of payments; batch W4
    one per entity over the corpus.
  - Continuous results are not comparable with batch.
- **Continuous recall is `recall_covered`**
  ([8.4](scoring.md#84-aml-scoring-reported-a-batch-run-without-a-result-fails)),
  not batch `recall`; no published record carries one yet.
- **Continuous results are never checked across runs**: alerts depend on
  when ticks ran, so any comparison including one is NOT ESTABLISHED at best. The published
  continuous record (1.6, scale 1, n=1) ingested 43.7% of the corpus in the
  default 1,800 s window.
- **Continuous throughput follows the offered load** (4 MB/s per scale unit by
  default). When the pipeline keeps up, `sustained_throughput_rps` is that
  load. It is a capacity only when datagen stays ahead with bronze busy
  (`datagen_ahead`, `intake_limit: bronze_capacity`). Under `--skip-generate`
  or a set `max_files_per_trigger` the [trickle](../../glossary.md#trickle) bounds it, named in
  `limits.bound` and `limits.trickle_bound`. The published continuous record
  ran under a trickle, predates that label and shows `bound: []`.
- **No expected results and no row relations checked**
  ([5](correctness.md#5-correctness-contract)). Correctness across
  architectures rests on equal alert sets between batch runs; query answers
  are compared by eye.
- **Repeatability.** Each record is one run (`runs: 1`). `run --repeat` (up to
  20 runs) runs a series on one corpus; Lakebench does not summarise it, so
  read the members' records for the spread. Every published AML record is
  n=1, and any investigator or time-travel figure from this release is one
  run per arm on one system (n=1).
- **The batch corpus is fixed in time and zone-less**
  ([3.5](generation.md#35-content-time-range-and-dirty-data)). The ignored
  timestamp and dirty-data fields are still recorded and hashed into corpus
  id v1, and `corpus.timestamp_start` and `timestamp_end` are the config's
  values, not the corpus's range.
- **Corpus id v1 hashes inert fields**: on exp1 records an edit to a field
  the AML generator never receives (the timestamps, `dirty_data_ratio`, the
  Customer 360 `unique_customers`) makes identical corpora not comparable. Only an image that writes corpus markers produces v2
  ([3.1](generation.md#31-generator-identity)).
- **Bronze tables are unpartitioned** (zero-copy registration), so bronze
  scans are full scans on every engine.
- **TM operations figures are simulations**
  ([8.5](tm-operations.md#85-tm-operations)); recall and false-positive rate
  are measured from alerts against the manifest.
- **`lakebench financial reproduce` pins the three silver tables only.**
  - W5 and W6 read the bronze watchlist as it is now, and W1 its vertex cap
    from the current config (listed as `not_pinned`).
  - The W3 and W17 path budgets depend on the job's executor count and
    scratch size.
  - A W2 or W4 alert is the clean check. Continuous alerts are not
    reproduced.
- **The reference-model score reads unsealed silver.** `lakebench financial
  reference-score` reads silver without the sealed-batch filter the rules and
  the covered scorer use (`spark/scripts/aml_features.py`). Rows from a batch
  whose seal marker never landed (a crash between the transaction commit and
  the marker) are counted there and nowhere else.
- **The [look](../../glossary.md#look) guard is per host.** The corpus ledger recording a registered
  corpus and its bronze prefix is a local file
  ([3.3](seed-policy.md#33-seed-policy)). A development config on another
  host pointed at that prefix is refused only by bronze-verify's manifest
  check, after the deploy. Take every look from one host.
- **`--generator-image` is a string, not a check of the corpus.** A registered
  look or calibration shard through `scripts/aml_gate.py` needs a
  digest-pinned reference (`...@sha256:<digest>`). A registered look refuses
  one that is not the same string the per-typology predictions used:
  `repo:tag@sha256:<digest>` and `repo@sha256:<digest>` for one image are
  refused against each other. `aml_gate.py` does not check that the scored
  corpus was written by that image; the corpus markers' `build_commit` is
  checked against `config/datagen_lineage.yaml` by hand.
- **A materialised MERGE source can fail a micro-batch.** An executor lost
  between materialising a silver-stream MERGE source and the MERGE fails that
  micro-batch, and the stream restarts through its replay path. No published
  record shows AML continuous on Spark 4.1; the published continuous record
  ran Spark 4.0.2.
- **No counter-leakage hard negatives.** The generator plants no legitimate
  accounts built to mimic a typology's shortcut signature (for example
  naturally long-quiet seasonal or travel accounts that break the dormancy gap
  signature). The leakage gate runs, but a detector can still score on such a
  shortcut.
- **Stream restarts longer than 1 h are not safe.** Iceberg snapshot expiry is
  floored at 1 h while streams are live. A bronze-ingest driver down longer
  can replay a batch and append duplicates; a silver stream down longer may
  resume from an expired snapshot and fail. Short restarts are fine.
- **The concurrency degradation ratio has only a reduced-form measurement**:
  1.6 measured investigator-query latency on Trino at scale 10, idle against
  loaded, 30 executions each. The full measurement (Spark and Trino, scale 10
  and 100, idle, beside one other workload and beside everything, at least 100
  executions each) has no cited result.
- **AML datagen throughput is reported, not gated**
  ([8.3](metrics.md#83-both-modes)). Throughput figures published before the
  generator freeze are superseded; no measurement on the frozen generator is
  cited.
- **Typology coverage is maintained by hand.** No test checks that every
  manifest typology is in `RULE_TARGETS` or `UNMAPPED_TYPOLOGIES`; add a new
  typology to one of them.

### Recorded 1.6 figures

Figures labelled "recorded" come from three run records written by
Lakebench 1.6, not in the repository:

| Record | Recipe | Verdict |
|---|---|---|
| 1.6 batch, scale 1 | polaris-iceberg-spark-trino | PASSED |
| 1.6 batch, scale 10 | hive-iceberg-spark-trino | PASSED |
| 1.6 continuous, scale 1 | hive-iceberg-spark-trino | PASSED |

- They carry workload version `aml-1` and experiment schema `exp1`.
- They predate the continuous [drain](../../glossary.md#drain), covered
  scoring, reason codes and record gates.
- Each is n=1 at its own development seed, on Spark 4.0.2.
- They are not comparable with a 1.7 run ([10](comparability.md#10-comparability)).
