# Data Generation

Lakebench generates synthetic data to populate the bronze S3 bucket before
running the pipeline. The `generate` command submits a Kubernetes Indexed Job
that runs parallel datagen pods, each producing Parquet files written directly
to S3.

## Basic Usage

```bash
lakebench generate my-config.yaml
```

By default, this submits the datagen job and waits for completion. The command
displays a progress bar showing pod completions and elapsed time.

## Scale Factor and Data Volume

The `workload.datagen.scale` field in your config controls how
much data is generated. For the Customer360 schema, one scale unit produces
approximately 10 GB of on-disk bronze Parquet data.

| Scale | Bronze Size | Customers (Customer360) | Approximate Rows | Typical Time |
|---|---|---|---|---|
| 1 | ~10 GB | 100,000 | 2.4 M | Minutes |
| 10 | ~100 GB | 1,000,000 | 24 M | 15--30 min |
| 100 | ~1 TB | 10,000,000 | 240 M | 1--3 hours |

Datagen scale is banded per workload. Customer 360 is supported up to scale
300 and unverified up to 600; AML (financial) is supported up to 300 and
unverified up to 800. Above the ceiling `deploy` and `generate` refuse the
config, because a datagen pod would exceed the 16 GiB per-pod memory cap
(a Lakebench-imposed cap); in the unverified range they warn. The run's
support state records the band.

These values assume the Customer360 workload schema (the default). The
financial (AML) schema has 111,111 entities and about 26.7 M transactions per
scale unit. Its size estimate (`src/lakebench/config/scale.py`) is about
8.4 GB of pacs.008 Parquet per scale unit, measured on the pre-freeze
generator; v1.6 has no size measurements on the frozen generator (deferred
to v1.7).

Set the scale in your config file:

```yaml
workload:
  datagen:
    scale: 100    # ~1 TB of bronze data
```

## Command Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--timeout` | `-t` | `0` | Timeout in seconds when waiting. `0` auto-computes it from scale, parallelism and a conservative per-pod throughput |
| `--yes` | `-y` | `false` | Skip confirmation prompt |

Without `--yes`, the command prompts for confirmation before submitting
the datagen job. Use `--yes` for scripts and CI/CD pipelines.

### Examples

Generate and wait (default behavior):

```bash
lakebench generate my-config.yaml
```

Generate with a longer timeout for large scales:

```bash
lakebench generate my-config.yaml --timeout 14400
```

## How It Works

Under the hood, `lakebench generate` creates a Kubernetes
[Indexed Job](https://kubernetes.io/docs/concepts/workloads/controllers/job/#indexed-job)
with `parallelism` set from the config (default: 4). Each pod in the Job:

1. Receives its index as an environment variable and computes its slice of the
   total data to generate.
2. Generates synthetic Parquet files using the configured workload schema
   (Customer360 by default).
3. Writes files directly to S3 under a fixed prefix the Spark stages read:
   `s3://<bronze-bucket>/customer/interactions/` for Customer 360 and
   `s3://<bronze-bucket>/pacs008/` for the financial schema. v1.7 removed
   the `medallion.bronze.path_template` key: the Customer 360 Spark stages
   always read `customer/interactions/` whatever it said, so a custom bronze
   layout is not supported. (The financial stages did read it, through
   `LB_FINANCIAL_BRONZE_PREFIX`.) A config that still names the fixed layout
   loads with a note; another layout is refused by the commands that change
   data.
4. Reports completion status back to Kubernetes.

The datagen mode (`auto`, `batch`, or `continuous`) is the S3 delivery
pattern, not a content or resource tier. Row content is byte-for-byte
identical across modes at a fixed seed. `batch` buffers each Parquet file
in memory and issues one S3 PUT per file; `continuous` uploads each file
via S3 multipart as row-groups close, so files arrive progressively rather
than in bursts. `auto` resolves to `continuous` at every scale (owner D18,
2026-09-28); the pre-v1.6 scale-threshold behaviour was removed in v1.6.
Pod CPU and memory are sized by scale via the autosizer, independently of
delivery mode.

### Delivery mode vs pipeline mode

Two independent config fields spell their values `batch` / `continuous`.
Content is set by seed, S3 layout by delivery mode, and stage graph by
pipeline mode. The two mode fields do not constrain each other.

| Concern | Config field | Type | What it controls |
|---|---|---|---|
| S3 delivery layout | `workload.datagen.mode` | `DatagenMode` | How the corpus lands in S3: `batch` = one PUT per Parquet file; `continuous` = S3 multipart upload as row-groups close. Row content is byte-identical at a fixed seed. |
| Stage graph | `architecture.pipeline.mode` | `PipelineMode` | How the medallion stages run: `batch` = sequential (bronze -> silver -> gold once); `continuous` = concurrent jobs over a corpus that keeps arriving. `sustained` is a deprecated alias for `continuous`. |

The composition `datagen.mode: batch` with `pipeline.mode: continuous` is
a valid config: all bronze files land in one burst, then the continuous
pipeline trickle-reads them. Lakebench does not use `streaming` as a mode
name; the continuous pipeline uses Spark Structured Streaming internally,
but the operator-facing name is `continuous`.

### 2026-09-28: default delivery mode changed to continuous

The default for `datagen.mode: auto` moved from `batch` (at scale <= 10)
to `continuous` (at every scale). Same-seed corpora remain byte-identical;
only the S3 upload pattern changed. If a run depended on batch-style
bursty uploads (bandwidth ceilings, RSS profile), set `mode: batch`
explicitly. Measured 2026-09-28: `continuous` is faster than `batch` at
scale 1 for `customer360` (upload-generation overlap) and 10-16% slower
at scale 10 because per-file multipart overhead grows with file count.
Choose the mode from file count and network profile rather than accepting
the default.

### `--delivery-mode` (internal render arg)

`deploy/datagen.py` translates `datagen.mode` into a `--delivery-mode`
argv on the datagen container (see `templates/datagen/job.yaml.j2`).
Operators do not set it directly; it appears in rendered Job manifests
as an aid when troubleshooting a job. The container entrypoint accepts
`auto`, `batch` and `continuous`, maps `auto` to `continuous`, and passes
the result to the Rust binary, which accepts only `batch` or `continuous`.
A rendered manifest can be replayed by hand through the entrypoint.

### Per-pod resources

The autosizer sizes datagen pods the same way in both modes, and honours
values you set in the config:

| Field | When unset | When set |
|---|---|---|
| `cpu` | `8` | Used as given |
| `memory` | Derived from the measured peak RSS for the schema, scale, thread count at the fixed 64mb file size, at least `4Gi` | Used as given |
| `generators` | `0` (auto): one generator thread per pod CPU | Used as the thread count |

| `memory` default at 8 CPU | Scale 1 | Scale 100 | Scale 300 | Scale 800 |
|---|---|---|---|---|
| AML (financial) | 7Gi | 8Gi | 10Gi | 16Gi |
| Customer 360 | 4Gi | 4Gi | 4Gi | 4Gi |

A datagen pod never requests more than 16Gi. If the CPU you set would need
more, the request stays at 16Gi and each pod runs fewer generator threads;
the autosizer says so. The entrypoint also lowers the thread count if a
memory limit you set cannot hold that many threads, rather than risk an
OOMKill. Above scale 100, AML datagen runs at least 8 pods: each pod holds
typology rows only for the files it writes, so fewer pods means more memory
per pod.

### Scale limits

Every datagen pod holds the whole population's state, so per-pod memory grows
with scale whatever the pod count. Each workload has a scale band, measured on
the cluster at 8 CPU per pod with the 16Gi cap:

| Workload | Supported (measured) | Unverified (modelled to fit) | Refused |
|---|---|---|---|
| AML (financial) | up to 300 | above 300, up to 800 | above 800 |
| Customer 360 | up to 300 | above 300, up to 600 | above 600 |

An unverified scale runs with a warning and is recorded as unverified in the
run's support state. A refused scale stops `deploy`, `generate` and `run`
before anything starts.

The number of datagen pods (parallelism) also scales with the scale factor:

| Scale Range | Default Parallelism |
|:-----------:|:-------------------:|
| 1--10 | 2--4 |
| 11--50 | 4--10 |
| 51--500 | 8--50 |
| 501+ | 16+ |

These defaults are adjusted by the autosizer when connected to a cluster.
Use `datagen.parallelism` in the config to override.

## Monitoring Progress

While datagen is running, you can monitor progress in several ways:

Check overall status:

```bash
lakebench status my-config.yaml
```

Watch individual pods:

```bash
kubectl get pods -n <namespace> -l job-name=lakebench-datagen --watch
```

Check pod logs for a specific worker:

```bash
kubectl logs -n <namespace> -l job-name=lakebench-datagen --tail=50
```

## Re-running Data Generation

`lakebench generate` (and `run --generate`, and a multi-cycle run before
its first cycle) refuses to write into a bronze datagen prefix that already
holds data: it exits 3 (refused) and names the prefix, so an existing corpus is never
overwritten by accident. What it does next depends on whether this
deployment owns the bronze bucket: it carries this deployment's and this
cluster's stamp (a tag, or on FlashBlade the `.lakebench/owner.json` marker),
or this namespace's created-buckets record lists it (a bucket 1.6 created,
stamped on the next deploy). A multi-cycle run takes the same rule before
cycle 0 (1.6 cleared an owned prefix silently) and takes `--regenerate`
without `--generate`; the continuous run refuses any bucket it does not own
before it starts (exit 3, or 4 when ownership cannot be checked).

| Bucket | Prefix | Flag | Result |
|---|---|---|---|
| owned | empty | any | generate |
| owned | holds data | none | exit 3 |
| owned | holds data | `--regenerate` | clear the datagen prefix (and abort its incomplete multipart uploads), then generate |
| not owned | empty | any | generate |
| not owned | holds data | none | exit 3: pass `--allow-stale-bronze`, or clear the prefix yourself |
| not owned | holds data | `--regenerate` | exit 3: Lakebench never empties a bucket this deployment did not create |
| not owned | holds data | `--allow-stale-bronze` | generate over it; `metrics.json` records `datagen.stale_bronze` and the report says "bronze held N objects before generate; rows may be over-counted" |

```bash
lakebench generate my-config.yaml --regenerate
```

`--regenerate` clears only the datagen prefix; other data in the bucket
(stream checkpoints, another workload's prefix) stays. To keep the existing
corpus instead, run the pipeline with `run --skip-generate`, or without
`--generate` (single-cycle; a multi-cycle run keeps it only with
`--skip-generate`, and a multi-cycle AML run cannot reuse it).

Every generate writes a corpus series marker,
`<datagen prefix>/_corpus/series.json`: the cycle count, the cycles whose
datagen Job finished, each cycle's window, the generation parameters and the
image digest the datagen pods ran. It is written when the generate starts,
with no cycle finished, and updated after each cycle's Job succeeds, so an
interrupted generate leaves a marker that says so; every clear of the prefix
(`--regenerate`, a fresh generate, a continuous reset, `clean bronze`) first
writes a marker that says a clear is under way and keeps it until the clear
is done. A prefix holding only that marker counts as empty. A run that reuses the
corpus is refused (exit 3) when the marker is unfinished or describes
another cycle count, window or generation than the config's; see "Reusing a
corpus" under `run` in the [CLI reference](cli-reference.md#run). No Spark
stage reads `_corpus/`. `lakebench generate` refuses a multi-cycle config
(exit 2): `run` generates each cycle before its stages.

The deployer applies the same rule before the first cycle: it
clears the datagen prefix of an owned bucket, so a smaller generate never
inherits a larger earlier generate's `part-*` files, and refuses a non-empty
prefix in any other bucket unless `--allow-stale-bronze` was passed. Before
1.7 it skipped such a bucket silently and silver over-counted the stale
files. Before the gate lists or clears the prefix, `generate`, `run
--generate` and a multi-cycle run delete an earlier `lakebench-datagen` Job
and wait until none of its pods (label `app=lakebench-datagen`) is still
running, since a pod in its grace period could otherwise land a file in the
cleared prefix that silver would count as this run's. The wait is bounded
at five minutes; a pod still running then refuses with exit 3
(`datagen.pods_live`), and pods that cannot be listed exit 4. A
continuous run does the same before its reset clears the raw prefix. The
deployer then follows the gate's decision, not the `--allow-stale-bronze`
flag: objects that appear after the gate found the prefix empty are
refused.

A continuous run's own datagen never takes `--allow-stale-bronze`
(`run --continuous --generate-only` does): its reset has already cleared
the datagen prefix, so objects found there were put there since by another
writer, and the refusal (exit 3) says to re-run once nothing writes there.
A `run` without datagen after a `generate --allow-stale-bronze`
records the same `datagen.stale_bronze` (the generate leaves the note under
`lakebench-output/datagen/`). Every generate that proceeds clears the
silver-state `bronze_data_clock`, since bronze is being replaced; the next
bronze-verify writes it again.

## Configuration Options

The full set of datagen-related configuration fields:

```yaml
workload:
  schema: customer360          # Workload schema: customer360 or financial
  datagen:
    scale: 10                  # Abstract scale factor (1 unit ~ 10 GB)
    mode: auto                 # auto | batch | continuous
    parallelism: 4             # Parallel Kubernetes pods (autosizer sizes it when unset)
    file_size: 64mb            # Fixed; the only accepted value
    dirty_data_ratio: 0.08     # Fraction of intentionally dirty records
    cpu: "8"                   # CPU per pod (autosizer default when unset)
    memory: 4Gi                # Memory per pod (autosizer derives it when unset)
    generators: 0              # Per-pod generator threads (0 = follow pod CPU)
    # Timestamp range -- affects Iceberg partition count.
    # Silver partitions by interaction_date (from event_timestamp).
    # Continuous mode: use a narrow range (days/weeks) to avoid
    # small-file proliferation across many date partitions.
    # Batch mode: wider ranges are fine (single compaction pass).
    # See docs/configuration.md#timestamp-range-impact for details.
    timestamp_start: null      # Start date for timestamps (ISO format)
    timestamp_end: null        # End date for timestamps (ISO format, exclusive)
```

The `dirty_data_ratio` field controls the fraction of records that contain
intentional quality issues (duplicates, missing fields, format inconsistencies).
This exercises the bronze-verify and silver-build data quality logic during
pipeline execution. The generator applies it to the customer360 schema only.

## Custom Datagen Images

The datagen container image is configurable:

```yaml
images:
  datagen: my-registry/my-datagen:latest
  pull_policy: Always
```

The default image (`docker.io/sillidata/lb-datagen:1.6.0`, digest
`sha256:5fda9025fb9b455b390e1138d82e9f6ef16d214dfa9419815be0111d2f6fce0a`,
generator version `datagen-v2-rs-0.3`; output-identical to the v1.6 AML
generator freeze but not the registered-look image, see
`docs/internal/aml-protocol.md`) is built from the `datagen_rs/` directory in this repository. To build and push a custom
image:

```bash
cd datagen_rs/
podman build -t my-registry/my-datagen:latest .
podman push my-registry/my-datagen:latest
```

Set `pull_policy: Always` in your config after pushing a new image to ensure
Kubernetes pulls the latest version.

## Workload Schemas

Lakebench supports multiple workload schemas that map the scale factor to
different domain dimensions:

| Schema | Entity | Events | Description |
|---|---|---|---|
| `customer360` | Customers | Interactions (purchase, browse, support) | Default. Multi-channel customer analytics. |
| `financial` | Entities (parties and their accounts) | pacs.008 transactions (4 per entity per month, 60 months) | AML transaction monitoring. |

Set the schema in your config:

```yaml
workload:
  schema: customer360
```

Customer360 produces approximately 10 GB of bronze data per scale unit;
financial is estimated at about 8.4 GB (pre-freeze measurement, see above).
The domain dimensions (number of entities, events per entity, date range)
vary by schema and are defined in `src/lakebench/config/scale.py`; the Arrow
schemas are in `datagen_rs/src/schema.rs`. `schema: custom` is rejected at
config load, and there is no IoT schema.

### Financial bronze layout

With the default path template the financial prefix is `pacs008/`, and
under `s3://<bronze-bucket>/pacs008/` the generator writes:

| Key | Contents |
|---|---|
| `bronze/pacs008/part-NNNNNN.parquet` | pacs.008 payment messages |
| `bronze/party.parquet` | party master: identity, address, customer flag, customer type, CRR score and tier (no sanctions, PEP or initial risk columns) |
| `bronze/account.parquet` | account master: IBAN, holder, bank, currency, dates, balance |
| `bronze/watchlist.parquet` | synthetic dated sanctions and PEP lists |
| `manifest/manifest.parquet` | planted typology ground truth used for scoring |

Cycles after the first in a multi-cycle run add a cycle suffix
(`part-cNNN-NNNNNN.parquet`, `manifest-cNNN.parquet`).
