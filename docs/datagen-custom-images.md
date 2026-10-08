# Building Custom Datagen Images

The Lakebench data generator runs as a container image deployed to Kubernetes.
The default image (`docker.io/sillidata/lb-datagen:3cb67f92`, digest
`sha256:e1e37d43682f87378b27ea9ff33a2a74885350c76b48caacd9709199c0be83b9`) is built from
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
podman build --build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD) -t your-registry/lb-datagen:custom .

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

A custom image is not the AML generator the registered looks use
(`datagen-v2-rs-0.3`). The registered v1.7 looks pin `lb-datagen:2a36ae21`
(digest `sha256:0502b700299948f43bb1b999d7ba29262a509306658b4e5f7c48738f88d31f04`),
which 1.7.0 shipped as its default. The 1.7.1 default `3cb67f92` has no
lineage row, so its corpora get a corpus id of their own. The run records the image reference you configured, and AML results from a
modified generator are not comparable with results from the look image.

**Corpus identity of a custom image.** From v1.7 a run records a second
corpus id, `corpus.id_v2`, taken from what the generator itself wrote
rather than from the config: each datagen node leaves a completion marker
under `<bronze prefix>/_corpus/` carrying a hash of the arguments it
resolved, and the run hashes those markers together with the image's
lineage. Two consequences for a custom image:

- An image built from `datagen_rs/` older than the v1.7 generator writes no
  markers. Its runs record `corpus.id_v2: null` with the reason in
  `corpus.id_v2_unavailable`; only the v1 corpus id identifies their
  corpus.
- Lineage is the image digest the run read from `_corpus/series.json`,
  mapped through `src/lakebench/config/datagen_lineage.yaml` when the run
  is observed. A digest not listed there is its own lineage, so a rebuilt
  image, even an output-identical one, gives a different corpus id. A row
  that maps a rebuilt digest to the image it re-pins needs byte-compare
  evidence: the five-case result file
  `tests/fixtures/datagen_reference/compare-<first 12 hex>.json`, whose
  sha256 the row pins (`evidence_sha256`) and which the unit suite checks.
  Without an observed digest the lineage reads `declared:<image tag>`,
  which never equals an observed one.

## Anatomy of `datagen_rs/`

The generator is a Rust crate. The files you are most likely to change:

| File | Contents |
|---|---|
| `datagen_rs/src/bin/generate.rs` | The `generate` binary: argument parsing, file-ID assignment per node (file `N` goes to node `N % total_nodes`), the rayon thread pool and the upload loop, for both schemas |
| `datagen_rs/src/schema.rs` | Arrow schemas: `customer360_schema()` (41 fields) and `pacs008_schema()` |
| `datagen_rs/src/customer360.rs` | `build_batch()`, which builds one Customer360 file: sessions, customer IDs, conditional nulls, dirty-data passes |
| `datagen_rs/src/customer360_realism.rs` | Customer360 value pools and weights (`INTERACTION_WEIGHTS`, `DATA_QUALITY_WEIGHTS`, `DATA_SOURCE_WEIGHTS`, `DIRTY_RATE_BY_SOURCE`, `CITIES`, `DIRTY_CITY_VARIANTS`, `DIRTY_STATE_VARIANTS`), the loyalty lookup (60% members, 70/20/10 tier split) and the truncated-Zipf `CustomerIdSampler` |
| `datagen_rs/src/writer.rs` | Parquet writer properties, `DG_COMPRESSION`, and the per-codec bytes-per-row tables that size files |
| `datagen_rs/src/model.rs`, `datagen_rs/src/world.rs`, `datagen_rs/src/typology.rs`, `datagen_rs/src/party.rs` | The financial (AML) world model, planted typologies, party and account tables, and `MODEL_VERSION` |
| `datagen_rs/entrypoint.py` | Container entrypoint: maps the Kubernetes Job's arguments onto the binary, sizes threads from the pod CPU and memory limit, reads the node ID from `JOB_COMPLETION_INDEX` |

**To add a Customer360 column:** add the field to `customer360_schema()` in
`datagen_rs/src/schema.rs`, build the column in `build_batch()` in `datagen_rs/src/customer360.rs`,
and update the schema tests in `datagen_rs/src/schema.rs`.

**To change a distribution:** edit the weight arrays in
`datagen_rs/src/customer360_realism.rs`, or the transaction-amount log-normal parameters
(mu 4.3, sigma 1.2) in `datagen_rs/src/customer360.rs`.

In batch the binary writes the whole corpus once. In continuous mode the
entrypoint passes `--deliver-until`: the binary keeps writing (AML: 24-month
epochs; Customer 360: time slices) until that deadline or the `_corpus/stop`
marker.

A custom image that keeps `entrypoint.py` inherits its strict argument
parsing: any flag the entrypoint does not declare exits 2, so a new flag
needs a declaration there and an entry in the generator's per-schema flag
table (`datagen_rs/src/bin/generate.rs`).

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
types, and corruption patterns. The financial schema also needs the held-out
hash file, which a pod gets from the `lakebench-heldout-hashes` ConfigMap;
locally, point `LB_HELDOUT_HASHES` at the tracked copy, or the binary exits 2:

```bash
LB_HELDOUT_HASHES=../src/lakebench/spark/data/aml/heldout_hashes.json \
DG_LOCAL_DIR=/tmp/lb-datagen cargo run --release --bin generate -- \
  --bucket test-bronze --seed 43 --scale 0.01 --threads 1
```

Inspect the Customer 360 output with PyArrow:

```python
import pyarrow.parquet as pq

table = pq.read_table("/tmp/lb-datagen/test-bronze/customer/interactions/part-000000.parquet")
print(table.schema)
print(table.to_pandas().head())
```

## Dockerfile Reference

`datagen_rs/Dockerfile` is a two-stage build. Stage one compiles the
`generate` binary in `rust:1.98.1-bookworm` (both base images are pinned by
digest in the Dockerfile; `cargo build --release --locked
--bin generate`). Stage two is `python:3.14-slim` with the binary at
`/app/datagen_rs` and `entrypoint.py`, which is the image entrypoint. The
runtime base is part of the generator's identity: the binary uses its glibc
math library, so another base can change corpus bytes. S3 credentials come
from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `S3_ENDPOINT` in the pod
environment.

Build with `--build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD)` (the build
refuses to run without it): the image's
OCI labels (`org.opencontainers.image.revision`, `.source`, `.version` and
`io.lakebench.model-version`), `generate --version` and every per-node
marker name that commit. `podman run <image> --version` prints
`datagen_rs <model version> <commit>`; `--print-resolved-args` added to a
Job's arguments prints the resolved corpus arguments and their hash as JSON
and writes nothing. For the financial schema it still needs what a run
needs before writing: the held-out hash file (`LB_HELDOUT_HASHES`, mounted
from the repository's `src/lakebench/spark/data/aml/heldout_hashes.json`)
and a seed (`--seed`, or `LB_DATAGEN_SEED` for a registered corpus).

After its last file, each pod writes
`<prefix>/_corpus/c<cycle>-node-<node>.json`: the files, rows and bytes it
wrote, the seed (salted hash for the financial schema), and `corpus_args`,
the arguments as the generator resolved them, including the parquet writer
settings, with their sha256. Spark never reads it as data (the directory
starts with `_`).

Add Rust dependencies to `Cargo.toml` (and commit the updated `Cargo.lock`,
since the build uses `--locked`).

## Registry Options

### Docker Hub

```bash
podman login docker.io
podman build --build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD) -t docker.io/youruser/lb-datagen:custom .
podman push docker.io/youruser/lb-datagen:custom
```

### Private Registry

```bash
podman login registry.example.com
podman build --build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD) -t registry.example.com/lakebench/lb-datagen:custom .
podman push registry.example.com/lakebench/lb-datagen:custom
```

If the registry uses a self-signed certificate, add `--tls-verify=false` to the
login and push commands.

### OpenShift Internal Registry

```bash
podman login -u $(oc whoami) -p $(oc whoami -t) image-registry.openshift-image-registry.svc:5000
podman build --build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD) -t image-registry.openshift-image-registry.svc:5000/lakebench/lb-datagen:custom .
podman push image-registry.openshift-image-registry.svc:5000/lakebench/lb-datagen:custom
```

Then reference the image with its internal service URL in your config:

```yaml
images:
  datagen: image-registry.openshift-image-registry.svc:5000/lakebench/lb-datagen:custom
```
