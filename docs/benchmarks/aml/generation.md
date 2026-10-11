# AML benchmark: data generation

See also: seeds in [3.3](seed-policy.md#33-seed-policy); typologies in [3.6](typologies.md#36-typologies).

## 3.1 Generator identity

The Rust generator in `datagen_rs/` writes the corpus, entered via
`datagen_rs/entrypoint.py` with `--schema financial` (the default). Its
identity has two parts.

**Model version.** `MODEL_VERSION = "datagen-v2-rs-0.4"`
(`datagen_rs/src/model.rs`).

- Stamped as a `model_version` column in the party, account, manifest and
  watchlist files; never in pacs.008 rows.
- A generator built from this release's source also writes it into each
  node's completion marker (`_corpus/c<cycle>-node-<node>.json`) and prints
  it with `--version`.
- Lakebench stamps `experiment.workload.generator_model_version` from a table
  a test keeps equal to `model.rs` (`metrics/experiment.py`), not from the
  corpus.

**Image.** `images.datagen` defaults to
`docker.io/sillidata/lb-datagen:5d7ce61a@sha256:ed4057e097f09fdd3e37631bc37eb88e5fce561cb8ebe06cd6fa2fd7d23e4bfc`;
the runtime pulls the digest.

- 1.7.0 shipped `lb-datagen:2a36ae21` (`sha256:0502b700...`). Its five-case
  byte-compare against the 1.6 image `lb-datagen:1.6.0` is equal (financial
  seed 43 with and without the robustness perturbation, Customer 360 seed 42,
  and both at two cycles; markers excluded;
  `tests/fixtures/datagen_reference/compare-0502b7002999.json`).
- The generator lineage in the corpus identity is the observed image digest,
  mapped through `config/datagen_lineage.yaml`
  ([7.3](execution-rules.md#73-prohibited-changes-invalidate-a-result-or-are-refused)).
  `5d7ce61a` has no row there, so its corpora get a corpus id of their own.
- The registered v1.7 [look](../../glossary.md#look) names `2a36ae21` by digest. A registered look
  never uses a tag: `scripts/aml_gate.py --registered` refuses to start
  without a digest-pinned `--generator-image`, and refuses one that differs
  from the image the committed per-typology predictions were made with.

**Markers and arguments.** A generator built from this release's source:

- writes one completion marker per node and cycle under `_corpus/`, holding
  the resolved generator arguments and their sha256; `--print-resolved-args`
  prints the same object without writing;
- refuses, with exit 2, an unknown flag, an abbreviated flag and any stray
  argument, so a typo never runs as a silent default.

Lakebench folds the markers into corpus id v2 (`metrics/corpus_identity.py`).

The default image writes the markers, runs the generator-start held-out check
and reads `LB_DATAGEN_SEED`.

- A run on it records corpus id v2.
- Its experiment block is identity v2 (exp2) when the run-start identity
  version and a system identity with at least one observed part also exist.
  Otherwise it is exp1 and names what was missing in `v2_unavailable`
  (`metrics/experiment.py`).
- An image built from older source, `1.6.0` included, writes no markers. Its
  runs stay exp1, whose corpus id hashes config fields the AML generator
  never reads ([12](limitations.md#12-known-limitations)).
- Such an image cannot generate a registered corpus, whose seed reaches the
  pod only as `LB_DATAGEN_SEED`.

## 3.2 Scale factor

Scale maps to domain dimensions in `config/scale.py` and, independently, in
the generator (`datagen_rs/src/world.rs`):

| Dimension | Formula | Scale 1 | Scale 10 |
|---|---|---|---|
| Parties (population) | round(111,111 x scale), at least 100 in the generator (Lakebench's estimate floors at 1) | 111,111 | 1,111,110 |
| Base payments | population x 4 per month x 60 months (about 26.7M per scale unit) | 26,666,640 | 266,666,400 |
| Screening rows | added on top of base payments; count depends on the seed | 5,227 (recorded, by difference) | 52,194 (recorded, by difference) |
| Expected pacs.008 size (the `scale_ratio` denominator) | scale x GB per scale unit: measured 8.47 at scale 1 and 9.36 at scale 10, interpolated in log10(scale) between them and held constant outside (`config/scale.py`) | 8.47 GB | 93.6 GB |
| Files at 64 MiB, snappy | clamp(rows x 345 B / file size, 64, rows / 1000) | 137 | 1,371 |

**World.** 55% persons, 40% companies, 5% financial institutions; about 89%
of homes are US; 1 to 4 accounts per entity; half the population are
customers of the reporting institution.

**Datagen pods** (`config/autosizer.py`):

- Unset parallelism follows the scale: 2 pods up to scale 5, 4 above 5 up to
  10; above scale 50 it is raised to what the cluster's CPU allows; above
  scale 100 financial gets at least 8 pods.
- Continuous mode with `cpu` and `parallelism` both unset: the cores that
  offer 4 MB/s per scale unit at 28 MB/s per core (100m steps, at least
  200m). Pods hold up to 8 cores, still with the 8-pod floor above scale 100.
- Each pod runs one generator thread per started core (`CPU_LIMIT`).
- A value you set is used exactly, with a warning when the cluster cannot fit
  it (a continuous run is then refused at preflight) or it is under that
  floor.
- Pod memory comes from a sizing model.
- Neither pod count nor memory changes row content. The pod count is passed
  as `--total-nodes`, which is part of the resolved arguments corpus id v2
  hashes. Above scale 50, two clusters of different size therefore produce
  different corpus ids unless `datagen.parallelism` is set to a value both
  fit ([7.3](execution-rules.md#73-prohibited-changes-invalidate-a-result-or-are-refused)).

**Size and limits.**

- Size is measured to scale 10 (8.47 GB at scale 1, 93.6 GB at scale 10);
  bytes per row grow between the two. [data-generation.md](../../data-generation.md)
  has the runs. Two scale-100 runs on other setups read within 2% of the
  scale-10 size per unit.
- At 9.36 GB per unit, scale 10000 (a tier-1 universal bank's AML retention
  target) is about 94 TB.
- The schema accepts up to scale 10000. AML datagen is banded: supported to
  300, unverified to 800, refused above 800, where a datagen pod would exceed
  the 16 GiB per-pod memory cap (a Lakebench cap).
- The pipeline has run end to end only up to scale 100, on the pre-freeze
  generator.

## 3.4 Delivery modes and identity

`datagen.mode` sets the S3 delivery pattern for pacs.008 files only:

| Mode | Delivery |
|---|---|
| `batch` | one PUT per file |
| `continuous` | each file streamed through an S3 multipart upload as row groups close |
| `auto` | `continuous` at every scale |

- Party and account are always multipart; manifest and watchlist always a
  single PUT.
- Row content is independent of delivery mode, thread count and node count.
  Generator tests check row-multiset identity across delivery modes and
  across thread, file-size and node layouts (`datagen_rs/tests/cycles.rs`).
- `datagen.file_size` is fixed at `64mb` for every workload; any other value
  is refused at config load.

## 3.5 Content, time range and dirty data

**Window.**

- Batch: fixed, 2021-01-01 00:00 to 2026-01-01 00:00 (60 months, 1,826
  days). Lakebench does not pass `--corpus-months`.
- Continuous: Lakebench passes `--corpus-months 24` and `--deliver-until`.
  Datagen writes a 24-month history, then successive 24-month periods
  (epochs) of the same bank until the run stops it. Each epoch has its own
  seed salt, new planted patterns, `part-eNNNN-*` files and a
  `manifest-eNNNN.parquet` answer key. Parties, accounts and the watchlist
  stay the same.
- Timestamps are zone-less (read as UTC by the Spark stages and query
  engines) and whole-second.
- Volume follows a weekday, salary-day, quarter-end and intraday calendar
  with per-country holiday roll-forward (`datagen_rs/src/timing.rs`).

**Ignored config fields.** The generator ignores `datagen.timestamp_start`,
`timestamp_end` and `dirty_data_ratio`; there is no dirty-data injection.
`timestamp_end` still matters downstream: it is the first rung of the silver
data clock, which sets `silver.entity_profiles.profile_updated_ts`.

- Unset (the default), the clock falls through to the clock bronze-verify
  recorded, then `timestamp_start`, then the run date.
- Both published batch records show `data_clock_source: fallback_default`,
  so their `profile_updated_ts` is the run date.
- No query or rule reads that column. It is still a business column of a
  frozen silver table: two runs of one seed on different dates differ in it
  unless `timestamp_end` is set, which does not change the corpus.

**Noise.** The only deliberate noise:

- synthetic-identity PII overrides in the party file;
- in the screening track, creditor names written as an alias, a typo, a
  token-order swap, a different romanisation or a dropped suffix, plus
  namesake decoys.

**Name pools** (`datagen_rs/src/realism.rs`).

- Given names and company descriptors come from fixed pools. Surnames and
  company name heads come from pools that grow with the population, so
  screening namesakes per watchlist entry stay flat across scale.
- From `datagen-v2-rs-0.4`, the scaled pools replaced fixed ones that caused
  W5 and W6 false matches to grow with the square of scale.

## 3.7 Multi-cycle

With `--cycles N`, cycle n emits the rows whose calendar mass falls in
[n/N, (n+1)/N); the union of all cycles equals the one-shot corpus.

- Cycle 0 keeps the one-shot names (`part-NNNNNN.parquet`,
  `manifest/manifest.parquet`).
- Cycle n > 0 writes `part-cNNN-NNNNNN.parquet` and
  `manifest/manifest-cNNN.parquet`, so bronze accumulates.
- Party, account and watchlist are written in cycle 0 only.
- The deployer's per-cycle timestamp windows do not apply to the financial
  schema.
