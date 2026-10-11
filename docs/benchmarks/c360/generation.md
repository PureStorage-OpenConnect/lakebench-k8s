# Customer 360 benchmark: data generation

## 3. Data generation

**Generator identity.** The Rust generator in `datagen_rs/` (entry
`datagen_rs/entrypoint.py`, driver `customer360_main` in
`datagen_rs/src/bin/generate.rs`). Customer 360 has no `MODEL_VERSION`
(`DATAGEN_MODEL_VERSIONS["customer360"] = None`); its identity is the
generator image.

- Corpus id v1 hashes the configured image reference.
- Corpus id v2 binds the image's lineage: the digest that wrote the corpus,
  mapped to the root of its output-neutral re-pin chain in
  `src/lakebench/config/datagen_lineage.yaml` (`declared:<tag>` when no digest
  was observed). A re-pin with byte-compare evidence keeps the id; any other
  image change moves it (`metrics/corpus_identity.py`).
- Default image: `docker.io/sillidata/lb-datagen:5d7ce61a`
  (sha256:ed4057e097f09fdd3e37631bc37eb88e5fce561cb8ebe06cd6fa2fd7d23e4bfc).
  It has no lineage row, so its corpora get a corpus id of their own.

**Seed policy.** `datagen.seed` when set, else 42. Any non-negative seed is
accepted; the AML spent-seed and [registered-look](../../glossary.md#look) guards do not apply, and
`corpus_role` is refused for this schema. The seed drives the per-customer
loyalty lookup (`Rng::new(seed)`) and one RNG per file
(`Rng::new(seed + file_id)`).

**Scale to dimensions.**

| Quantity | Formula (code) | Scale 1 | Scale 10 |
|---|---|---|---|
| Customer id space | `max(1, round(scale x 100,000))`, or `customer360.unique_customers` | 100,000 | 1,000,000 |
| Target bytes | `approx_bronze_gb / 1024` TB, 6 decimals | 0.009766 TB | 0.097656 TB |
| Files | `max(1, target_bytes // file_size)` (about 16,000 at scale 100) | 160 | 1,599 |
| Rows per file | `max(1000, file_size / bytes_per_row)`, bytes_per_row 4,332 (snappy, default) | 15,491 | 15,491 |
| Rows | files x rows per file | 2,478,560 | 24,770,109 |

- `bytes_per_row` depends on the codec (`DG_COMPRESSION`: zstd 2,233, lz4
  4,356, none 4,399). Nothing in the Lakebench config or Job template sets
  `DG_COMPRESSION`, so snappy applies unless the Job is edited by hand.
- The payload size is fixed at 2 KiB; the file size at 64 MB
  (`datagen.file_size` refuses any other value).
- Each pod of the Kubernetes Indexed Job gets a `JOB_COMPLETION_INDEX`. File
  `N` goes to pod `N % total_nodes`, so files spread evenly whatever the pod
  count.
- A file's content depends only on the seed, file id, rows per file, customer
  id space, dirty ratio and timestamp window, so pod count does not change
  the corpus.
- There is no checkpoint-resume: an interrupted generate starts again from
  the beginning.

**Row model.**

- Sessions of 5 to 20 rows: one customer, one `session_id`, a timestamp anchor
  drawn uniformly in the window, rows within 30 minutes of it.
- Customer ids (1-based) from a truncated Zipf sampler: the 500 lowest ids
  receive 40% of sessions with Zipf(1.2) weights, the rest uniform over the
  remaining ids.
- Interaction type weights: purchase 0.18, browse 0.35, support 0.12, login
  0.20, abandoned_cart 0.15.
- Purchase amounts `clamp(exp(N(4.3, 1.2)), 1, 9999.99)` rounded to cents;
  every other row 0.0.
- Page views 1 to 20 on purchase and browse, else 0; time on site 30 to
  3,629 s when page views > 0.
- Support rows carry a ticket, issue and satisfaction 1 to 5; others NULL.
- Login and support rows: NULL `product_id`, `product_category`,
  `click_count` and `items_in_cart`, NaN `cart_value`. Store and call_center
  rows: NULL device and browser.
- 60% of customer ids are loyalty members (tiers 70/20/10), fixed per id for
  the run. 40% of rows carry a campaign.
- Drawn per row, independent of dirty injection: `data_source` weights
  0.70/0.15/0.10/0.05; `data_quality_flag` clean 0.92, duplicate_suspected
  0.02, incomplete_data 0.03, format_inconsistent 0.03.

**Dirty-data injection.** `dirty_data_ratio` (default 0.08, validated to
[0, 1]) is the target aggregate share of rows selected for corruption per
field.

- Per-row probability `ratio x DIRTY_RATE_BY_SOURCE[source] / weighted_mean`,
  clamped at 1.0. Per-source shape: primary_system 0.005, legacy_import 0.35,
  manual_entry 0.15, third_party_api 0.05; weighted mean about 0.0735.
- Four independent Bernoulli passes select rows of each field:
  - `email_raw`, 6 modes: missing `@`, ALL CAPS, leading or trailing
    whitespace, double `@@`, missing TLD, `@` replaced with `.at.`;
  - `phone_raw`, 4 modes: digits only, extra dashes, truncated, letter `O`
    for digit `0`;
  - `city_raw`, 10 misspellings (`Chicago` -> `Chicgao`, `Phoenix` ->
    `Pheonix`, `Los Angeles` -> `Los Angelas`);
  - `state_raw`, casing and abbreviation variants (`CA`, `california`,
    `CALIFORNIA`, `Calif.`).
- A selected email, phone or state is always rewritten. A selected city is
  rewritten only when it has a misspelling.
  - 8 of the 22 city entries (San Antonio, San Diego, Dallas and San Jose,
    two entries each) have none.
  - Cities are drawn uniformly, so the realised `city_raw` share is about
    14/22 (0.64) of the ratio.
- The selection share equals the ratio up to about 0.21; above that
  legacy_import saturates and the aggregate falls short (0.5 gives about
  29%).
- Independently, 10% of emails carry a `.DUPLICATE` marker before the `@`
  (`user123.DUPLICATE@gmail.com`), simulating duplicate upstream records
  (`duplicate_email_pct`, fixed, not configurable from Lakebench).

**Time range.** `event_timestamp` is drawn in `[timestamp_start,
timestamp_end)`, dates `YYYY-MM-DD`. Single-cycle default (from the Rust
binary when the config sets neither): 2024-01-01 to 2025-01-01 (366 days).
Multi-cycle default: 2024-01-01 to 2025-12-31 (730 days), split into `cycles`
equal slices (the last takes the remainder).

**Multi-cycle windows** (`pipeline.cycles = N > 1`, batch only).

- The CLI runs one datagen Job per cycle before that cycle's stages, unless
  `--skip-generate` reuses a finished multi-cycle corpus of the same config
  (checked against the corpus series marker, below).
- `run --generate`, `run --generate-only` and `generate` on a multi-cycle
  config are refused (exit 2): a whole corpus generated first would be read
  again by cycle 0 ([4.1](pipeline.md#41-batch-mode-pipelinemode-batch)).
- Windows: `config/c360_run.py` `cycle_windows`. Cycle n gets its time slice,
  `target_tb / N`, and `--cycle n --cycles N`. For n > 0 the file id handed to
  the row generator is `fid + (n << 32)` (disjoint RNG streams and row ids),
  and objects are named `part-cNNN-NNNNNN.parquet`.
- The customer id space and loyalty lookup are the same in every cycle. A
  multi-cycle corpus differs from a single-cycle one at the same seed and
  scale.

**Delivery modes and byte identity.** `datagen.mode` sets the S3 delivery
pattern: `batch` (one PUT per file), `continuous` (S3 multipart as row groups
close), `auto` = `continuous`; the entrypoint forwards it to the generator.
Row content is byte-identical across delivery modes at a fixed seed (Rust
row-identity tests). Delivery mode is independent of pipeline mode.

**Fresh generate** (`deploy/datagen.py`, `cli/_helpers.py`,
`cli/_run_args.py`).

| Case | Result |
|---|---|
| Batch `run --generate` or `generate` over a non-empty datagen prefix (`customer/interactions/`) | refused (exit 3) |
| Same, bucket this deployment can prove it owns | `--regenerate` deletes that prefix (aborting incomplete uploads; never the rest of the bucket); `--skip-generate` reuses the data when its corpus series marker matches the config |
| Same, bucket it cannot prove it owns | only `--allow-stale-bronze` proceeds, writing over the objects and recording that |
| Bucket unreadable for the check | exit 4 |
| Inside datagen, before cycle 0 writes | the prefix is cleared again on an owned bucket; on any other bucket a non-empty prefix stops datagen unless `--allow-stale-bronze` |
| An earlier datagen Job | deleted first; refused (exit 3) if its pods still run five minutes later; exit 4 if they cannot be listed |
| `--regenerate` without `--generate` or `--generate-only`, on a local run, or on a continuous run other than `--generate-only` | refused (exit 2) |
| `--allow-stale-bronze` on any run other than one that generates into bronze (`--generate`, `--generate-only` or a multi-cycle batch run) | refused (exit 2) |

Continuous runs other than `--generate-only` skip the CLI gate: their
preflight clears raw data unless `--skip-generate`
([4.2](pipeline.md#42-continuous-mode-pipelinemode-continuous-or-run---continuous)).
If objects land in the prefix after that reset on a bucket this deployment
cannot prove it owns, datagen refuses to start (exit 3).
`--allow-stale-bronze` does not apply to a continuous run other than
`--generate-only`.

**Corpus series marker.** Every generate writes
`customer/interactions/_corpus/series.json` (`deploy/corpus.py`). It holds:

- the cycle count, the cycles whose datagen Job finished, and each cycle's
  window;
- the generation parameters (seed, scale, customer id space, file size,
  target size per cycle, dirty ratio, image, window bounds);
- the image digest the datagen pods ran.

- It is written unfinished when the generate starts and completed cycle by
  cycle. Every clear of the prefix first writes a marker saying a clear is
  under way, and keeps it.
- A batch run that reuses bronze (`--skip-generate`, or one cycle without
  `--generate`) is refused (exit 3, `run.series_mismatch`) when the marker is
  unfinished or names another cycle count, window or generation than the
  config's.
- A single-cycle run over a corpus with no marker (1.6) proceeds, unless the
  prefix holds later-cycle files.
- No stage reads `_corpus/`.
