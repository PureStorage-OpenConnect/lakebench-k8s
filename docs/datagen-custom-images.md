# Building Custom Datagen Images

The Lakebench data generator runs as a container image deployed to Kubernetes.
The default image (`docker.io/sillidata/lb-datagen:034f998`, digest
`sha256:0dc67b26e6130acebd796082137fde8c6dac57e8cd668039d585ae9d086d29dc`) is built from
`datagen_rs/` (Rust) and produces both the Customer360 schema
([datagen-schema.md](datagen-schema.md)) and the Financial `pacs.008` schema,
dispatched by `--schema`. You can build a custom image to add columns, change
statistical distributions, use a different data domain, or adjust the row
generator.

## Why Customize

Common reasons to build your own datagen image:

- **Add columns** to the schema (e.g., geographic coordinates, product SKUs,
  additional demographic fields).
- **Change distributions** -- adjust the Zipf parameters for customer IDs, modify
  interaction type weights, or tune the log-normal parameters for transaction
  amounts.
- **Use a different data domain** -- replace the Customer360 schema entirely
  with IoT sensor data, financial transactions, or clickstream events.
- **Change the codec or file sizing** -- set `DG_COMPRESSION` in the pod env,
  tune `--file-size-mb` on the CLI, or update the codec-conditional
  `bytes_per_row` tables in `datagen_rs/src/writer.rs`.
- **Add post-processing** -- inject custom corruption patterns, additional
  quality flags, or domain-specific realism features (see the 7 realism
  features in `datagen_rs/src/customer360_realism.rs`).

## Prerequisites

- **podman** (recommended on RHEL/OpenShift) or **docker**
- **Rust toolchain** -- the Dockerfile compiles the datagen binary from source
  in a multi-stage build, so no local Rust install is required unless you want
  to run `cargo test` before building.
- Access to a container registry (Docker Hub, private registry, or OpenShift
  internal registry)
- Registry credentials configured (`podman login` or `docker login`)

## Build and Push

From the repository root, build and push the image:

```bash
cd datagen_rs

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
  pull_policy: Always  # Forces Kubernetes to pull the image again
```

Setting `pull_policy: Always` is important after pushing a new image tag. Without
it, Kubernetes may use a cached version of the image if the tag already existed
on the node. Prefer a new, immutable tag per build over reusing one.

A custom image is not the frozen AML generator (`datagen-v2-rs-0.3`, image
tag `034f998`, digest
`sha256:0dc67b26e6130acebd796082137fde8c6dac57e8cd668039d585ae9d086d29dc`).
The run records the image reference you configured, and AML results from a
modified generator are not comparable with results from the frozen one.
Freeze identity is defined by `MODEL_VERSION` in `datagen_rs/src/model.rs`:
a rebuild that keeps `MODEL_VERSION` unchanged and produces byte-identical
output at a fixed seed still belongs to the same freeze even under a
different image tag.

## Anatomy of `datagen_rs/`

The generator is a Rust crate. The files you are most likely to change:

| File | Contents |
|---|---|
| `src/bin/generate.rs` | The `generate` binary: argument parsing, file-ID assignment per node (file `N` goes to node `N % total_nodes`), the rayon thread pool and the upload loop, for both schemas |
| `src/schema.rs` | Arrow schemas: `customer360_schema()` (41 fields) and `pacs008_schema()` |
| `src/customer360.rs` | `build_batch()`, which builds one Customer360 file: sessions, customer IDs, conditional nulls, dirty-data passes |
| `src/customer360_realism.rs` | Customer360 value pools and weights (`INTERACTION_WEIGHTS`, `DATA_QUALITY_WEIGHTS`, `DATA_SOURCE_WEIGHTS`, `DIRTY_RATE_BY_SOURCE`, `CITIES`, `DIRTY_CITY_VARIANTS`, `DIRTY_STATE_VARIANTS`), the loyalty lookup (60% members, 70/20/10 tier split) and the truncated-Zipf `CustomerIdSampler` |
| `src/writer.rs` | Parquet writer properties, `DG_COMPRESSION`, and the per-codec bytes-per-row tables that size files |
| `src/model.rs`, `src/world.rs`, `src/typology.rs`, `src/party.rs` | The financial (AML) world model, planted typologies, party and account tables, and `MODEL_VERSION` |
| `entrypoint.py` | Container entrypoint: maps the Kubernetes Job's arguments onto the binary, sizes threads from the pod CPU and memory limit, reads the node ID from `JOB_COMPLETION_INDEX` |

**To add a Customer360 column:** add the field to `customer360_schema()` in
`src/schema.rs`, build the column in `build_batch()` in `src/customer360.rs`,
and update the schema tests in `src/schema.rs`.

**To change a distribution:** edit the weight arrays in
`src/customer360_realism.rs`, or the transaction-amount log-normal parameters
(mu 4.3, sigma 1.2) in `src/customer360.rs`.

The binary always writes the whole corpus up front; there is no separate
continuous-mode path and no checkpoint-resume.

## Testing Locally

Run the tests, then generate a small corpus into a local directory.
`DG_LOCAL_DIR` makes the binary write `<dir>/<bucket>/<prefix>/<key>` on the
local filesystem instead of S3, so no credentials are needed:

```bash
cd datagen_rs
cargo test --release --locked

DG_LOCAL_DIR=/tmp/lb-datagen cargo run --release --bin generate -- \
  --schema customer360 \
  --bucket test-bronze \
  --seed 42 \
  --scale 0.01 \
  --target-tb 0.0001 \
  --threads 1
```

This writes a single 64 MB file, enough to verify schema changes, column
types, and corruption patterns. Inspect the output with PyArrow:

```python
import pyarrow.parquet as pq

table = pq.read_table("/tmp/lb-datagen/test-bronze/customer/interactions/part-000000.parquet")
print(table.schema)
print(table.to_pandas().head())
```

## Dockerfile Reference

`datagen_rs/Dockerfile` is a two-stage build. Stage one compiles the
`generate` binary in `rust:1.98.1-bookworm` (`cargo build --release --locked
--bin generate`). Stage two is `python:3.13-slim` with `boto3`, the binary at
`/app/datagen_rs` and `entrypoint.py`, which is the image entrypoint. S3
credentials come from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
`S3_ENDPOINT` in the pod environment.

Add Rust dependencies to `Cargo.toml` (and commit the updated `Cargo.lock`,
since the build uses `--locked`).

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
