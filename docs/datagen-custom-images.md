# Building Custom Datagen Images

Reference: building, testing and configuring a custom data generator image, and how its corpora are identified.

- The generator runs as a container image on Kubernetes.
- Default image: `docker.io/sillidata/lb-datagen:5d7ce61a` (digest
  `sha256:ed4057e097f09fdd3e37631bc37eb88e5fce561cb8ebe06cd6fa2fd7d23e4bfc`),
  built from `datagen_rs/` (Rust).
- It produces the Customer360 schema ([Customer 360 data model](benchmarks/c360/data-model.md#21-bronze-columns-41-arrow-schema-customer360_schema))
  and the Financial `pacs.008` schema, chosen by `--schema`.

## Why Customize

- **Add columns** (for example geographic coordinates, product SKUs,
  demographic fields).
- **Change distributions**: the customer-ID Zipf parameters, interaction type
  weights, or the transaction-amount log-normal parameters.
- **Use a different data domain**: replace Customer360 with IoT sensor data,
  financial transactions or clickstream events.
- **Change the codec or file sizing**: set `DG_COMPRESSION` in the pod env,
  tune the binary's `--file-size-mb`, or update the codec-conditional
  `bytes_per_row` tables in `datagen_rs/src/writer.rs`.
- **Add post-processing**: custom corruption patterns, quality flags or
  realism features (the 7 realism features are in
  `datagen_rs/src/customer360_realism.rs`).

## Prerequisites

- **podman** (recommended on RHEL/OpenShift) or **docker**.
- Access to a container registry (Docker Hub, private, or the OpenShift
  internal registry), with credentials configured (`podman login` or
  `docker login`).
- A Rust toolchain only to run `cargo test` before building. The Dockerfile
  compiles the binary from source in a multi-stage build.

## Build and Push

```bash
cd datagen_rs

# Build the image
podman build --build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD) -t your-registry/lb-datagen:custom .

# Push to your registry
podman push your-registry/lb-datagen:custom
```

With Docker, substitute `docker` for `podman`.

| Registry | Login | Image tag |
|---|---|---|
| Docker Hub | `podman login docker.io` | `docker.io/youruser/lb-datagen:custom` |
| Private | `podman login registry.example.com` | `registry.example.com/lakebench/lb-datagen:custom` |
| OpenShift internal | `podman login -u $(oc whoami) -p $(oc whoami -t) image-registry.openshift-image-registry.svc:5000` | `image-registry.openshift-image-registry.svc:5000/lakebench/lb-datagen:custom` |

- A private registry with a self-signed certificate: add `--tls-verify=false`
  to the login and push commands.
- OpenShift: reference the image by its internal service URL in the config.

## Configure Lakebench to Use the Custom Image

```yaml
images:
  datagen: your-registry/lb-datagen:custom
  pull_policy: Always  # Forces Kubernetes to pull the image again
```

- Set `pull_policy: Always` after pushing a new image under an existing tag.
  Without it, a node may use its cached image.
- Prefer a new, immutable tag per build.

**AML comparability.**

- A custom image is not the AML generator the registered looks use
  (`datagen-v2-rs-0.3`).
- The registered v1.7 looks pin `lb-datagen:2a36ae21` (digest
  `sha256:0502b700299948f43bb1b999d7ba29262a509306658b4e5f7c48738f88d31f04`),
  which 1.7.0 shipped as its default.
- The 1.7.1 default `5d7ce61a` has no lineage row, so its corpora get a corpus
  id of their own.
- The run records the configured image reference. AML results from a modified
  generator are not comparable with results from the look image.

**Corpus identity of a custom image.** A run records a second corpus id,
`corpus.id_v2`, from what the generator wrote rather than from the config.
Each datagen node leaves a completion marker under `<bronze prefix>/_corpus/`
with a hash of the arguments it resolved; the run hashes those markers with
the image's lineage. For a custom image:

- An image built from `datagen_rs/` older than the v1.7 generator writes no
  markers. Its runs record `corpus.id_v2: null`, with the reason in
  `corpus.id_v2_unavailable`; only the v1 corpus id identifies the corpus.
- Lineage is the image digest the run read from `_corpus/series.json`, mapped
  through `src/lakebench/config/datagen_lineage.yaml` when the run is observed.
- A digest not listed there is its own lineage, so a rebuilt image, even an
  output-identical one, gives a different corpus id.
- A row that maps a rebuilt digest to the image it re-pins needs byte-compare
  evidence: the five-case result file
  `tests/fixtures/datagen_reference/compare-<first 12 hex>.json`, whose sha256
  the row pins (`evidence_sha256`) and the unit suite checks.
- Without an observed digest the lineage reads `declared:<image tag>`, which
  never equals an observed one.

## Anatomy of `datagen_rs/`

| File | Contents |
|---|---|
| `datagen_rs/src/bin/generate.rs` | The `generate` binary: argument parsing, file-ID assignment per node (file `N` goes to node `N % total_nodes`), the rayon thread pool and the upload loop, for both schemas |
| `datagen_rs/src/schema.rs` | Arrow schemas: `customer360_schema()` (41 fields) and `pacs008_schema()` |
| `datagen_rs/src/customer360.rs` | `build_batch()`, which builds one Customer360 file: sessions, customer IDs, conditional nulls, dirty-data passes |
| `datagen_rs/src/customer360_realism.rs` | Customer360 value pools and weights (`INTERACTION_WEIGHTS`, `DATA_QUALITY_WEIGHTS`, `DATA_SOURCE_WEIGHTS`, `DIRTY_RATE_BY_SOURCE`, `CITIES`, `DIRTY_CITY_VARIANTS`, `DIRTY_STATE_VARIANTS`), the loyalty lookup (60% members, 70/20/10 tier split) and the truncated-Zipf `CustomerIdSampler` |
| `datagen_rs/src/writer.rs` | Parquet writer properties, `DG_COMPRESSION`, and the per-codec bytes-per-row tables that size files |
| `datagen_rs/src/model.rs`, `datagen_rs/src/world.rs`, `datagen_rs/src/typology.rs`, `datagen_rs/src/party.rs` | The financial (AML) world model, planted typologies, party and account tables, and `MODEL_VERSION` |
| `datagen_rs/entrypoint.py` | Container entrypoint: maps the Kubernetes Job's arguments onto the binary, sizes threads from the pod CPU and memory limit, reads the node ID from `JOB_COMPLETION_INDEX` |

- **Add a Customer360 column:** add the field to `customer360_schema()` in
  `datagen_rs/src/schema.rs`, build it in `build_batch()` in
  `datagen_rs/src/customer360.rs`, and update the schema tests in
  `datagen_rs/src/schema.rs`.
- **Change a distribution:** edit the weight arrays in
  `datagen_rs/src/customer360_realism.rs`, or the transaction-amount
  log-normal parameters (mu 4.3, sigma 1.2) in `datagen_rs/src/customer360.rs`.
- **Delivery:** in batch the binary writes the whole corpus once. In
  continuous mode the entrypoint passes `--deliver-until`: the binary keeps
  writing (AML: 24-month epochs; Customer 360: time slices) until that
  deadline or the `_corpus/stop` marker.
- **New flags:** `entrypoint.py` parses arguments strictly; any flag it does
  not declare exits 2. A new flag needs a declaration there and an entry in
  the generator's per-schema flag table (`datagen_rs/src/bin/generate.rs`).

## Testing Locally

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

This writes one 64 MB file, enough to check schema changes, column types and
corruption patterns.

The financial schema also needs the held-out hash file, which a pod gets from
the `lakebench-heldout-hashes` ConfigMap. Locally, point `LB_HELDOUT_HASHES` at
the tracked copy, or the binary exits 2:

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

`datagen_rs/Dockerfile` is a two-stage build:

1. `rust:1.98.1-bookworm` compiles the binary (`cargo build --release --locked
   --bin generate`).
2. `python:3.14-slim` holds the binary at `/app/datagen_rs` and
   `entrypoint.py`, the image entrypoint.

- Both base images are pinned by digest in the Dockerfile.
- The runtime base is part of the generator's identity: the binary uses its
  glibc math library, so another base can change corpus bytes.
- S3 credentials come from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
  `S3_ENDPOINT` in the pod environment.
- Add Rust dependencies to `Cargo.toml` and commit the updated `Cargo.lock`
  (the build uses `--locked`).

**Build commit.** The build refuses to run without
`--build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD)`. That commit appears in:

- the OCI labels `org.opencontainers.image.revision`, `.source`, `.version`
  and `io.lakebench.model-version`;
- `generate --version`, and `podman run <image> --version`, which prints
  `datagen_rs <model version> <commit>`;
- every per-node marker.

**`--print-resolved-args`**, added to a Job's arguments, prints the resolved
corpus arguments and their hash as JSON and writes nothing. For the financial
schema it still needs the held-out hash file (`LB_HELDOUT_HASHES`, mounted from
the repository's `src/lakebench/spark/data/aml/heldout_hashes.json`) and a seed
(`--seed`, or `LB_DATAGEN_SEED` for a registered corpus).

**Per-node marker.** After its last file, each pod writes
`<prefix>/_corpus/c<cycle>-node-<node>.json`. It holds the files, rows and
bytes it wrote, and the seed (a salted hash for the financial schema). It
also holds `corpus_args`, the arguments as the generator resolved them,
including the Parquet writer settings, with their sha256. Spark never reads it as data (the directory
starts with `_`).
