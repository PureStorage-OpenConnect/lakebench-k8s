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
| 1000 | ~10 TB | 100,000,000 | 2.4 B | 6--12+ hours |

These values assume the Customer360 workload schema (the default). The
financial (AML) schema has 111,111 entities and about 26.7 M transactions per
scale unit. Its size estimate (`src/lakebench/config/scale.py`) is about
8.4 GB of pacs.008 Parquet per scale unit, measured on the pre-freeze
generator; sizes on the v1.6 frozen generator are pending.

Set the scale in your config file:

```yaml
workload:
  datagen:
    scale: 100    # ~1 TB of bronze data
```

## Command Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--wait` | `-w` | `true` | Wait for data generation to complete |
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
3. Writes files directly to S3 at the path
   `s3://<bronze-bucket>/<path_template>/` (default path template:
   `customer/interactions`; the financial schema writes under `pacs008`).
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
as an aid when troubleshooting a job. The Rust binary accepts the same
three values (`auto`, `batch`, `continuous`), so a rendered manifest can
be replayed by hand.

### Per-pod resources

The autosizer sizes datagen pods the same way in both modes, and honours
values you set in the config:

| Field | When unset | When set |
|---|---|---|
| `cpu` | `8` | Used as given |
| `memory` | Derived from the measured peak RSS for the schema, scale, thread count and `file_size`, at least `4Gi` | Used as given |
| `generators` | `0` (auto): one generator thread per pod CPU | Used as the thread count |

The entrypoint lowers the thread count if the pod's memory limit cannot hold
that many threads, rather than risk an OOMKill.

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

Data generation is idempotent in the sense that re-running it overwrites
existing data in the bronze bucket. If you need fresh data:

1. Run `lakebench generate` again. New files will be written alongside or
   overwriting existing data in the bronze bucket.
2. If you want a clean slate, empty the bronze bucket first:

   ```bash
   lakebench clean bronze my-config.yaml --force
   lakebench generate my-config.yaml
   ```

## Configuration Options

The full set of datagen-related configuration fields:

```yaml
workload:
  schema: customer360          # Workload schema: customer360 or financial
  datagen:
    scale: 10                  # Abstract scale factor (1 unit ~ 10 GB)
    mode: auto                 # auto | batch | continuous
    parallelism: 4             # Number of parallel Kubernetes pods
    file_size: 64mb            # Target Parquet file size (per-thread memory scales with it)
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

The default image (`docker.io/sillidata/lb-datagen:25f1aa8`, digest
`sha256:8dbc2705c6d95dbc3a259b3d9e3007e5cd951db3df2655afc66d357fd1fed5f7`;
the v1.6 AML generator-freeze commit, generator version `datagen-v2-rs-0.3`)
is built from the `datagen_rs/` directory in this repository. To build and push a custom
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
