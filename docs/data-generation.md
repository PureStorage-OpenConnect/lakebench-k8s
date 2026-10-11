# Data Generation

Guide: fill the bronze bucket with synthetic data for the pipeline.

`lakebench generate` submits a Kubernetes Indexed Job. Its pods run in
parallel and write Parquet files straight to S3. Every flag and exit code is
in [CLI Reference](cli-reference.md#generate).

## Basic usage

```bash
lakebench generate my-config.yaml                    # prompts, submits, waits
lakebench generate my-config.yaml --timeout 14400    # longer wait for large scales
```

- The command asks for confirmation first. Pass `--yes` in scripts and CI.
- It waits for the Job and shows a progress bar of pod completions and
  elapsed time.
- `--timeout 0` (the default) computes the wait from scale, parallelism and
  a conservative per-pod throughput.
- On a continuous config, `generate` writes until the datagen lead plus the
  window has passed, as a continuous run's datagen does.

## Scale Factor and Data Volume

`workload.datagen.scale` sets how much data is generated:

```yaml
workload:
  datagen:
    scale: 100    # ~1 TB of bronze data
```

Customer 360 (the default schema) writes about 10 GB of bronze Parquet per
scale unit:

| Scale | Bronze size | Customers | Approximate rows | Typical time |
|---|---|---|---|---|
| 1 | ~10 GB | 100,000 | 2.4 M | Minutes |
| 10 | ~100 GB | 1,000,000 | 24 M | 15--30 min |
| 100 | ~1 TB | 10,000,000 | 240 M | 1--3 hours |

Financial (AML) has 111,111 entities and about 26.7 M transactions per scale
unit. Its size is measured, not linear:

- Measured with the default 64 MB files on the v1.6 generator, the pacs.008
  bronze-verify read was 8.47 GB at scale 1 (n=2) and 93.6 GB at scale 10
  (n=1).
- Rows are linear in scale. Bytes per row grow from about 318 to 351
  between those two points.
- Between scale 1 and 10 the size per unit is interpolated in log scale.
  Below 1 it is the scale-1 value; above 10, the scale-10 value.
- Two scale-100 runs on another setup read 0.4% (128 MB files) and 1.8% (an
  earlier generator) above that.
- `scale_ratio` divides the bronze a run read by this size.

### Scale limits

Every datagen pod holds the whole population's state, so per-pod memory
grows with scale whatever the pod count. Each workload has a band, measured
on the cluster at 8 CPU per pod with the 16 GiB per-pod memory limit (a
Lakebench cap):

| Workload | Supported (measured) | Unverified (modelled to fit) | Refused |
|---|---|---|---|
| AML (financial) | up to 300 | above 300, up to 800 | above 800 |
| Customer 360 | up to 300 | above 300, up to 600 | above 600 |

- An unverified scale runs with a warning.
- A refused scale stops `deploy`, `generate` and `run` before anything
  starts: a datagen pod would need more than 16 GiB.
- The run's support state records the band.

## Batch and continuous modes

Lakebench supports batch and continuous pipeline modes. See
[Running pipelines](running-pipelines.md) for mode details and
configuration.

## How it works

`lakebench generate` creates a Kubernetes
[Indexed Job](https://kubernetes.io/docs/concepts/workloads/controllers/job/#indexed-job)
with `parallelism` from the config (default 4). Each pod:

1. Gets its index from an environment variable and computes its slice of the
   data.
2. Generates Parquet files for the configured schema (Customer 360 by
   default).
3. Writes them to S3 under a fixed prefix the Spark stages read:
   `s3://<bronze-bucket>/customer/interactions/` for Customer 360,
   `s3://<bronze-bucket>/pacs008/` for financial.
4. Reports completion to Kubernetes.

A custom bronze layout is not supported. v1.7 removed
`medallion.bronze.path_template`: the Customer 360 stages always read
`customer/interactions/` whatever it said (the financial stages did read it,
through `LB_FINANCIAL_BRONZE_PREFIX`). A config that still names the fixed
layout loads with a note. Another layout is refused by the commands that
change data.

### Delivery mode vs pipeline mode

Two independent fields both use the values `batch` and `continuous`. Seed
sets content, delivery mode sets how files land in S3, pipeline mode sets the
stage graph. Neither mode field constrains the other.

| Concern | Config field | Type | What it controls |
|---|---|---|---|
| S3 delivery | `workload.datagen.mode` | `DatagenMode` | `batch`: each Parquet file is buffered in memory and sent in one PUT. `continuous`: each file is sent by S3 multipart upload as row groups close, so files arrive steadily rather than in bursts. `auto` (default) resolves to `continuous` at every scale. |
| Stage graph | `architecture.pipeline.mode` | `PipelineMode` | `batch`: stages run once in sequence (bronze -> silver -> gold). `continuous`: concurrent jobs over a corpus that keeps arriving. `sustained` is a deprecated alias for `continuous`. |

- Row content is byte-identical across delivery modes at a fixed seed.
- Pod CPU and memory are sized by scale, not by delivery mode.
- `datagen.mode: batch` with `pipeline.mode: continuous` is valid. Datagen
  still writes for the whole window; each file lands in one PUT.
- `streaming` is not a mode name. The continuous pipeline uses Spark
  Structured Streaming internally, but the name you set is `continuous`.

On 2026-09-28 the `auto` default moved from `batch` (at scale <= 10) to
`continuous` at every scale. If a run depended on bursty uploads (bandwidth
ceilings, RSS profile), set `mode: batch`. `continuous` overlaps generation
with upload; `batch` uploads each file once it is written. No measurement of
the speed difference is published yet, so choose from file count and network
profile rather than taking the default.

`deploy/datagen.py` turns `datagen.mode` into a `--delivery-mode` argument on
the datagen container (see `templates/datagen/job.yaml.j2`). You do not set
it; it shows in rendered Job manifests, which helps when troubleshooting.
The entrypoint accepts `auto`, `batch` and `continuous`, maps `auto` to
`continuous`, and passes the result to the Rust binary, which accepts only
`batch` or `continuous`. A rendered manifest can be replayed by hand through
the entrypoint.

### Per-pod resources

Lakebench sizes datagen pods from scale and keeps any value you set:

| Field | When unset | When set |
|---|---|---|
| `cpu` | `8` in batch. In continuous with `parallelism` also unset: the cores that offer the scale's load, in 100m steps (at least 200m), in pods of up to 8 cores | Used as given |
| `memory` | Derived from the measured peak RSS for the schema, scale and thread count at the fixed 64mb file size; at least `4Gi` | Used as given |
| `generators` | `0` (auto): one generator thread per started core of the pod's CPU (`CPU_LIMIT`; a 1300m pod runs 2) | Used as the thread count |

| `memory` default at 8 CPU | Scale 1 | Scale 100 | Scale 300 | Scale 800 |
|---|---|---|---|---|
| AML (financial) | 7Gi | 8Gi | 10Gi | 16Gi |
| Customer 360 | 4Gi | 4Gi | 4Gi | 4Gi |

- A datagen pod never requests more than 16Gi. If the CPU you set would need
  more, the request stays at 16Gi, each pod runs fewer generator threads,
  and Lakebench says so.
- The entrypoint also lowers the thread count when a memory limit you set
  cannot hold that many threads, rather than risk an OOMKill.
- Above scale 100, AML datagen runs at least 8 pods unless you set
  `datagen.parallelism` lower (the run then warns). Each pod holds typology
  rows only for the files it writes, so fewer pods means more memory per pod.

Default pod count by scale:

| Scale range | Default parallelism |
|:-----------:|:-------------------:|
| 1--10 | 2--4 |
| 11--50 | 4--10 |
| 51--500 | 8--50 |
| 501+ | 16+ |

Lakebench adjusts these when it can read the cluster. Set
`datagen.parallelism` to override.

## Monitoring progress

```bash
lakebench status my-config.yaml                                           # overall
kubectl get pods -n <namespace> -l job-name=lakebench-datagen --watch     # pods
kubectl logs -n <namespace> -l job-name=lakebench-datagen --tail=50       # worker logs
```

## Re-running data generation

`lakebench generate`, `run --generate` and a multi-cycle run (before its
first cycle) refuse to write into a bronze datagen prefix that holds data.
They exit 3 and name the prefix, so a corpus is never overwritten by
accident.

What happens next depends on whether this deployment owns the bucket. It
does when the bucket carries this deployment's and this cluster's stamp (a
tag, or on FlashBlade the `.lakebench/owner.json` marker), or when this
namespace's created-buckets record lists it (a bucket 1.6 created, stamped on
the next deploy).

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

- `--regenerate` clears only the datagen prefix. Other data in the bucket
  (stream checkpoints, another workload's prefix) stays.
- A multi-cycle run follows the same rule before cycle 0 and takes
  `--regenerate` without `--generate`. (1.6 cleared an owned prefix
  silently.)
- A continuous run refuses any bucket it does not own before it starts
  (exit 3, or 4 when ownership cannot be checked).
- To keep the corpus instead, run with `run --skip-generate`, or without
  `--generate` (single-cycle). A multi-cycle run keeps it only with
  `--skip-generate`; a multi-cycle AML run cannot reuse it.
- `lakebench generate` refuses a multi-cycle config (exit 2): `run`
  generates each cycle before its stages.

### The corpus series marker

Every generate writes `<datagen prefix>/_corpus/series.json`: the cycle
count, the cycles whose datagen Job finished, each cycle's window, the
generation parameters and the image digest the datagen pods ran.

- It is written when the generate starts, with no cycle finished, and
  updated after each cycle's Job succeeds. An interrupted generate leaves a
  marker that says so.
- A clear of the prefix (`--regenerate`, a fresh generate, a continuous
  reset) first writes a marker saying a clear is under way, and keeps it
  until the clear is done.
- A prefix holding only that marker counts as empty.
- A run that reuses the corpus is refused (exit 3) when the marker is
  unfinished or describes another cycle count, window or generation than the
  config's. See "Reusing a corpus" under [run](cli-reference.md#run).
- No Spark stage reads `_corpus/`.

### Before the first cycle

The deployer applies the same ownership rule:

- It clears the datagen prefix of an owned bucket, so a smaller generate
  never inherits a larger earlier generate's `part-*` files.
- It refuses a non-empty prefix in any other bucket unless the gate allowed
  it (`--allow-stale-bronze` on a prefix that already held objects). It
  follows the gate's decision, not the flag: objects that appear after the
  gate found the prefix empty are refused.
- It never skips such a bucket silently, which made silver over-count the
  stale files before 1.7.

Before the gate lists or clears the prefix, `generate`, `run --generate` and
a multi-cycle run delete any earlier `lakebench-datagen` Job. They wait
until none of its pods (label `app=lakebench-datagen`) is still running,
since a pod in its grace period could land a file in the cleared prefix that
silver would count as this run's.

- The wait is at most five minutes. A pod still running then refuses with
  exit 3 (`datagen.pods_live`).
- Pods that cannot be listed exit 4.
- A continuous run does the same before its reset clears the raw prefix.

A continuous run's own datagen never takes `--allow-stale-bronze`
(`run --continuous --generate-only` does). Its reset has already cleared the
prefix, so objects found there came from another writer since. The refusal
(exit 3) says to rerun once nothing writes there.

A `run` without datagen after a `generate --allow-stale-bronze` records the
same `datagen.stale_bronze`; the generate leaves the note under
`lakebench-output/datagen/`. Every generate that proceeds clears the
silver-state `bronze_data_clock`, since bronze is being replaced. The next
bronze-verify writes it again.

## Configuration options

```yaml
workload:
  schema: customer360          # Workload schema: customer360 or financial
  datagen:
    scale: 10                  # Abstract scale factor (1 unit ~ 10 GB)
    mode: auto                 # auto | batch | continuous
    parallelism: 4             # Parallel Kubernetes pods (sized from scale when unset)
    file_size: 64mb            # Fixed; the only accepted value
    dirty_data_ratio: 0.08     # Fraction of intentionally dirty records
    cpu: "8"                   # CPU per pod (sized from scale when unset)
    memory: 4Gi                # Memory per pod (derived when unset)
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

`dirty_data_ratio` is the fraction of records with deliberate quality issues
(duplicates, missing fields, format inconsistencies). They exercise the
bronze-verify and silver-build quality logic. The generator applies it to the
customer360 schema only.

## Custom datagen images

The default image, and building and configuring your own:
[Building Custom Datagen Images](datagen-custom-images.md).

## Workload schemas

| Schema | Entity | Events | Description |
|---|---|---|---|
| `customer360` | Customers | Interactions (purchase, browse, support) | Default. Multi-channel customer analytics. |
| `financial` | Entities (parties and their accounts) | pacs.008 transactions (4 per entity per month, 60 months) | AML transaction monitoring. |

```yaml
workload:
  schema: customer360
```

- Customer 360 writes about 10 GB of bronze per scale unit. Financial writes
  about 8.5 GB at scale 1 and 9.4 GB per unit from scale 10 (measured to
  scale 10, see above).
- Entity counts, events per entity and date ranges are in
  `src/lakebench/config/scale.py`. The Arrow schemas are in
  `datagen_rs/src/schema.rs`.
- `schema: custom` is rejected at config load. There is no IoT schema.

### Financial bronze layout

The financial prefix is fixed at `pacs008/`. Under
`s3://<bronze-bucket>/pacs008/` the generator writes:

| Key | Contents |
|---|---|
| `bronze/pacs008/part-NNNNNN.parquet` | pacs.008 payment messages |
| `bronze/party.parquet` | party master: identity, address, customer flag, customer type, CRR score and tier (no sanctions, PEP or initial risk columns) |
| `bronze/account.parquet` | account master: IBAN, holder, bank, currency, dates, balance |
| `bronze/watchlist.parquet` | synthetic dated sanctions and PEP lists |
| `manifest/manifest.parquet` | planted typology ground truth used for scoring |

- Cycles after the first in a multi-cycle run add a cycle suffix
  (`part-cNNN-NNNNNN.parquet`, `manifest-cNNN.parquet`).
- A continuous corpus adds a 24-month epoch after the history until the run
  stops it, with an epoch suffix (`part-eNNNN-*.parquet`,
  `manifest-eNNNN.parquet`).
