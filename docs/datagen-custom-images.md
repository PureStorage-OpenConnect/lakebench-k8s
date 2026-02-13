# Building Custom Datagen Images

The Lakebench data generator runs as a container image deployed to Kubernetes.
The default image (`lakebench/datagen:latest`) produces the Customer360 schema
described in [datagen-schema.md](datagen-schema.md). You can build a custom
image to add columns, change statistical distributions, use a different data
domain, or adjust dependencies.

## Why Customize

Common reasons to build your own datagen image:

- **Add columns** to the schema (e.g., geographic coordinates, product SKUs,
  additional demographic fields).
- **Change distributions** -- adjust the Zipf alpha for customer IDs, modify
  interaction type weights, or tune the log-normal parameters for transaction
  amounts.
- **Use a different data domain** -- replace the Customer360 schema entirely
  with IoT sensor data, financial transactions, or clickstream events.
- **Update dependencies** -- pin specific versions of PyArrow, NumPy, or boto3
  for compatibility with your environment.
- **Add post-processing** -- inject custom corruption patterns, additional
  quality flags, or domain-specific realism features.

## Prerequisites

- **podman** (recommended on RHEL/OpenShift) or **docker**
- Access to a container registry (Docker Hub, private registry, or OpenShift
  internal registry)
- Registry credentials configured (`podman login` or `docker login`)

## Build and Push

From the repository root, build and push the image:

```bash
cd lakebench/datagen

# Build the image
podman build -t your-registry/lb-datagen:custom .

# Push to your registry
podman push your-registry/lb-datagen:custom
```

If you use Docker instead of podman, substitute `docker` for `podman` in both
commands.

## Configure Lakebench to Use the Custom Image

Update your Lakebench YAML configuration to point to the new image:

```yaml
images:
  datagen: your-registry/lb-datagen:custom

platform:
  compute:
    spark:
      pull_policy: Always  # Forces Kubernetes to pull the latest image
```

Setting `pull_policy: Always` is important after pushing a new image tag. Without
it, Kubernetes may use a cached version of the image if the tag already existed
on the node.

You can also set `pull_policy` at the top level of the `images` block:

```yaml
images:
  datagen: your-registry/lb-datagen:custom
  pull_policy: Always
```

## Anatomy of generate.py

The generator script is a single file (`datagen/generate.py`) with a
straightforward structure. Understanding these sections helps you make targeted
modifications.

### Constants (lines 42-122)

Global lists and dictionaries that define the data domain:

- `INTERACTION_TYPES` / `INTERACTION_WEIGHTS` -- event types and their
  probability weights.
- `PRODUCT_CATEGORIES`, `CURRENCIES`, `CHANNELS`, `DEVICE_TYPES`, `BROWSERS` --
  categorical value pools.
- `LOYALTY_TIERS` -- loyalty program tiers (`bronze`, `silver`, `gold`).
- `DATA_QUALITY_FLAGS` / `DATA_QUALITY_WEIGHTS` -- quality flag distribution.
- `DATA_SOURCES` / `DATA_SOURCE_WEIGHTS` -- data source origin distribution.
- `CITIES` -- city/state pairs with intentional inconsistencies.
- `DIRTY_CITY_VARIANTS` / `DIRTY_STATE_VARIANTS` -- corruption dictionaries for
  dirty data injection.

To add new categorical columns, add a new list here and reference it in the
data generation function.

### Config Class (lines 129-181)

The `Config` class holds all generation parameters: target size, worker count,
seed, file size, S3 credentials, timestamp range, customer ID range, dirty data
ratio, and derived values like rows per file. S3 credentials are read from
environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`S3_ENDPOINT`).

### Loyalty Lookup (lines 212-236)

`build_loyalty_lookup()` pre-generates a NumPy array that maps every customer ID
to a deterministic (is_member, tier) tuple, so the same customer
always has the same loyalty attributes. Modify the membership ratio (default
60%) or tier split (default 70/20/10) here.

### Generator Functions (lines 243-368)

Utility functions for generating individual column types:

- `generate_uuids()` -- UUID v4 strings
- `generate_timestamps()` -- random timestamps within the configured date range
- `generate_emails()` -- email addresses with optional `.DUPLICATE` markers and
  corruption modes
- `generate_phones()` -- phone numbers in mixed formats with corruption
- `generate_ips()` -- random IPv4 addresses
- `generate_user_agents()` -- synthetic browser user-agent strings
- `generate_fingerprints()` -- 64-character hex session fingerprints
- `generate_payloads()` -- random hex payloads (compression anchor)
- `generate_categorical()` -- uniform random selection from a list
- `generate_weighted_categorical()` -- weighted random selection

### generate_file_data() (lines 408-650)

This is the core function. It generates all 41 columns for a single Parquet
file, applying all seven realism features (Zipf customer IDs, weighted
interaction types, conditional nulls, dirty data corruption, channel-device
coherence, log-normal transaction amounts, customer-consistent loyalty). It
returns a PyArrow Table with an explicit schema.

**To add a new column:**

1. Generate the column data (add a new array or list to the `data` dictionary).
2. Add the corresponding field to the `pa.schema([...])` definition at the
   bottom of the function.

**To change a distribution:** Modify the relevant NumPy call. For example, to
change the Zipf alpha from 1.5 to 2.0, update the `rng.zipf(1.5, size=rows)`
call.

### Continuous Mode (lines 688-909)

A multiprocessing pipeline with 8 generator processes and 2 uploader threads,
connected via a bounded queue. Used for large-scale generation (100+ GB) where
sustained throughput matters. You generally do not need to modify this section
unless you are changing the upload behavior.

### main() (lines 938-1107)

CLI entry point with `argparse`. Handles argument parsing, node ID resolution
from `JOB_COMPLETION_INDEX` (Kubernetes Indexed Jobs), checkpoint
loading/saving, and orchestration of either batch or continuous mode.

## Testing Locally

Test your changes locally before building a container image. The generator can
write to a local directory or to an S3-compatible endpoint:

```bash
# Write to local S3 (e.g., MinIO running locally)
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export S3_ENDPOINT=http://localhost:9000

python generate.py \
  --target-tb 0.001 \
  --workers 1 \
  --bucket test-bronze \
  --mode batch \
  --seed 42
```

This generates a minimal dataset (~1 GB) to verify schema changes, column
types, and corruption patterns. Inspect the output with PyArrow:

```python
import pyarrow.parquet as pq

table = pq.read_table("/path/to/output/part-000000.parquet")
print(table.schema)
print(table.to_pandas().head())
```

## Dockerfile Reference

The Dockerfile is minimal:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generate.py .
ENTRYPOINT ["python", "generate.py"]
```

Dependencies (`requirements.txt`):

```
pyarrow>=14.0.0
boto3>=1.34.0
numpy>=1.26.0
tqdm>=4.66.0
```

If your custom generator needs additional libraries (e.g., `faker` for
realistic names, `geopandas` for geographic data), add them to
`requirements.txt` before building.

## Registry Options

### Docker Hub

```bash
podman login docker.io
podman build -t docker.io/youruser/lb-datagen:custom .
podman push docker.io/youruser/lb-datagen:custom
```

### Private Registry

```bash
podman login registry.example.com
podman build -t registry.example.com/lakebench/lb-datagen:custom .
podman push registry.example.com/lakebench/lb-datagen:custom
```

If the registry uses a self-signed certificate, add `--tls-verify=false` to the
login and push commands.

### OpenShift Internal Registry

```bash
podman login -u $(oc whoami) -p $(oc whoami -t) image-registry.openshift-image-registry.svc:5000
podman build -t image-registry.openshift-image-registry.svc:5000/lakebench/lb-datagen:custom .
podman push image-registry.openshift-image-registry.svc:5000/lakebench/lb-datagen:custom
```

Then reference the image with its internal service URL in your config:

```yaml
images:
  datagen: image-registry.openshift-image-registry.svc:5000/lakebench/lb-datagen:custom
```
