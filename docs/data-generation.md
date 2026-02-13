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

The `architecture.workload.datagen.scale` field in your config controls how
much data is generated. One scale unit produces approximately 10 GB of
on-disk bronze Parquet data.

| Scale | Bronze Size | Customers (Customer360) | Approximate Rows | Typical Time |
|---|---|---|---|---|
| 1 | ~10 GB | 100,000 | 2.4 M | Minutes |
| 10 | ~100 GB | 1,000,000 | 24 M | 15--30 min |
| 100 | ~1 TB | 10,000,000 | 240 M | 1--3 hours |
| 1000 | ~10 TB | 100,000,000 | 2.4 B | 6--12+ hours |

These values assume the Customer360 workload schema (the default). Other
schemas (IoT, Financial) produce equivalent data volumes at the same scale
factor but with different domain entities.

Set the scale in your config file:

```yaml
architecture:
  workload:
    datagen:
      scale: 100    # ~1 TB of bronze data
```

## Command Flags

| Flag | Short | Default | Description |
|---|---|---|---|
| `--wait` | `-w` | `true` | Wait for data generation to complete |
| `--timeout` | `-t` | `7200` | Timeout in seconds when waiting |
| `--resume` | | `false` | Resume from checkpoint if a previous generation was interrupted |

### Examples

Generate and wait (default behavior):

```bash
lakebench generate my-config.yaml
```

Submit the job and return immediately without waiting:

```bash
lakebench generate my-config.yaml --no-wait
```

Generate with a longer timeout for large scales:

```bash
lakebench generate my-config.yaml --timeout 14400
```

Resume an interrupted generation:

```bash
lakebench generate my-config.yaml --resume
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
   `customer/interactions`).
4. Reports completion status back to Kubernetes.

The datagen mode (`auto`, `batch`, or `continuous`) determines per-pod resource
allocation:

- **batch** (scale <= 10, or explicit): 1 generator process, 1 uploader thread
  per pod. Low resource profile (4 CPU, 4Gi memory). Best for small datasets.
- **continuous** (scale > 10, or explicit): 8 generator processes, 2 uploader
  threads per pod. High resource profile (8 CPU, 32Gi memory). Best for large
  datasets where sustained throughput matters.
- **auto** (default): Selects batch or continuous based on scale factor.

### Resource Allocation by Mode

| Mode | CPU/pod | Memory/pod | Generators/pod | Uploaders/pod | Trigger |
|------|--------:|-----------:|:--------------:|:-------------:|---------|
| batch | 4 | 4Gi | 1 | 1 | scale <= 10 (auto) |
| continuous | 8 | 32Gi | 8 | 2 | scale > 10 (auto) |

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

Check how much data has been written to S3:

```bash
lakebench info my-config.yaml
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
architecture:
  workload:
    schema: customer360          # Workload schema: customer360, iot, financial
    datagen:
      scale: 10                  # Abstract scale factor (1 unit ~ 10 GB)
      mode: auto                 # auto | batch | continuous
      parallelism: 4             # Number of parallel Kubernetes pods
      file_size: 512mb           # Target Parquet file size
      dirty_data_ratio: 0.08     # Fraction of intentionally dirty records
      cpu: "2"                   # CPU request per pod
      memory: 4Gi                # Memory request per pod
      generators: 0              # Per-pod generator processes (0 = auto)
      uploaders: 0               # Per-pod uploader threads (0 = auto)
      timestamp_start: null      # Start date for timestamps (ISO format)
      timestamp_end: null        # End date for timestamps (ISO format)
      checkpoint:
        enabled: true
        path: ".lakebench_checkpoint.json"
```

The `dirty_data_ratio` field controls the fraction of records that contain
intentional quality issues (duplicates, missing fields, format inconsistencies).
This exercises the bronze-verify and silver-build data quality logic during
pipeline execution.

## Custom Datagen Images

The datagen container image is configurable:

```yaml
images:
  datagen: my-registry/my-datagen:latest
  pull_policy: Always
```

The default image (`lakebench/datagen:latest`) is built from the `datagen/`
directory in this repository. To build and push a custom image:

```bash
cd datagen/
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
| `iot` | Sensors | Readings (1/min, 30 days) | IoT telemetry pipeline. |
| `financial` | Accounts | Transactions (4/month, 12 months) | Financial transaction processing. |

Set the schema in your config:

```yaml
architecture:
  workload:
    schema: customer360
```

All schemas produce approximately 10 GB of bronze data per scale unit. The
domain dimensions (number of entities, events per entity, date range) vary
by schema.
