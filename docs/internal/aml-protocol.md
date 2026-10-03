# AML registered evaluation protocol

Maintainer material, not shipped with the package. This is the mechanism
behind DESIGN.md invariant 7 (held-out evaluation data). The constants live in
`src/lakebench/spark/data/aml/aml_preregistration.json`; only the copy at
the integrate branch tip is authoritative (older lane checkouts carry stale
versions, so rebase an AML-touching lane before using its constants); decision numbers (#NN) refer to section 9 of the owner's local decision
log, and R-numbers to its rules.

## Seeds and corpus roles

- The evaluation and robustness seeds are held out. They are recorded only as
  salted hashes in `src/lakebench/spark/data/aml/heldout_hashes.json`
  (append-only; the hashes are also compiled into the Python guard), never in
  plaintext, and a seed is checked by hashing it. Their corpora are generated and
  scored only as the registered look for their role
  (`scripts/aml_gate.py --registered`, `corpora.registered_looks_open`), once
  each, after the generator freeze.
- Spent seeds (42, 50000042) are refused for the AML schema
  (`config/datagen_seed.py`). A seed is spent when a look at it is taken,
  voided or burned: a look or burn is recorded in `aml_registered_looks.json`
  and the seed is appended to `heldout_hashes.json`'s `spent` (the
  pre-registration's `corpora.spent_seeds` changes only with the owner's
  locked-file commit).
- A held-out seed that becomes public, or whose started look is voided, is
  burned and replaced, never reused. The owner runs `oa-seed-redraw.py`
  (kept outside the repository, beside `oa-heldout-init.py`). It draws a
  new uniform 63-bit seed per role with Python's `secrets`, redrawn until
  it passes the pre-registration's seed guard against the calibration,
  replicate and spent seeds; writes each to an owner-only file (mode 0600,
  in `~/.lakebench-heldout-seeds`, outside every git tree) and never prints
  it; appends its salted hash to the role in `heldout_hashes.json` and to
  the compiled floor in `config/datagen_seed.py`; records the old seed as
  `burned`, with the owner's reason, in `aml_registered_looks.json`
  (`datagen_seed.burn_seed`; a seed with a `started` look is burned only
  with `--void` and a reason naming the void decision); and appends the old seed to the hash file's `spent`. The
  append-only rule (`heldout_history_problems`) accepts that spent append
  only beside a completed look or a burn for the same role and seed. The
  Rust floor keeps the first hashes until the next datagen image; the
  generator reads every hash from the mounted file. The evaluation and
  robustness seeds that appeared in public history on 2026-09-24 and
  2026-09-25 are retired this way (OA2). Every agent on the lab host runs
  as root, so the 0600 mode keeps the seed files from other accounts, not
  from an agent: no agent reads that directory.
- Calibration seeds are 43 and the replicates C1-C4
  (`corpora.calibration_replicate_seeds`). Tuning is allowed only on these.

## What may change, and when

- Datagen is never tuned to a rule's recall or false-positive rate, on any
  seed (R2). A generator change is legitimate only for a realism property
  measured independently of any rule.
- Every gated constant in the prereg JSON (subsets, band, K/N, leakage caps,
  unit, features, scale, rules) is locked at prereg 3.6.0. Earlier changes are
  in its changelog (3.5.0 reference model and leakage cap after the voided
  D0, #37/#38; 3.6.0 D8 rule, #45/#45a). Any further change needs an owner
  decision, a decision-log row and a fresh evaluation seed.
- Everything that changes AML generator output lands before the freeze:
  sanctions and PEP planting (#50), and moving answer keys out of bronze.
- The freeze spans bronze AND silver (#55). It covers the AML generator
  output (bronze rows, manifest, MODEL_VERSION at a fixed seed) and the
  silver transform output: the business-column content of the six silver
  tables `silver_build_financial` writes (silver.transactions, .entities,
  .accounts, .account_statements, .counterparty_edges, .entity_profiles)
  at a fixed seed. Per-run sentinels (`_batch_id`, `_stream_id`,
  `ingest_ts`, and the sidecar's `committed_at`) are not part of the
  frozen content -- they vary per run by design and are excluded from the
  parity definition, exactly as the batch/stream parity tests exclude
  them. Silver was hardened and its batch vs stream row-content parity
  proven before this extension; freezing bronze->silver gives gold,
  detection, scoring and replay a stable silver contract to build on.
- After the freeze, any change that alters AML generator output OR silver
  transform output (AML rows, manifest, MODEL_VERSION, or any frozen
  silver business-column content at a fixed seed) voids the freeze: it
  needs a MODEL_VERSION bump, a re-run of the calibration table and a new
  evaluation seed. Customer 360 only and output-neutral changes to
  `datagen_rs/` or `silver_build_financial.py` are fine when a
  byte-identical AML corpus and byte-identical silver business columns at
  seed 43 prove them so; the spark-tier batch/stream parity tests
  (statements, profiles, dimensions, replay-idempotency) are the standing
  automated guard, matching how MODEL_VERSION plus the seed-43
  byte-compare guards the generator (no separate silver-hash gate).

## The look image

The registered looks, the calibration corpora and the Level-2 predictions
use one datagen image (DAT-2), named by digest:

- **Digest:** `sha256:48e18a417bf85528392afeb9b8222bfd3cc1d5f3db3bf1d7d0623e6a4f6ea4b1`,
  tag `lb-datagen:a592385`, built from integrate
  `a592385053b038d66dc491ee3c8dfb55dfab3d84` with `LB_BUILD_COMMIT` set
  (the image's `org.opencontainers.image.revision` label and `datagen_rs
  --version` report it), MODEL_VERSION `datagen-v2-rs-0.3`. It is
  `ImagesConfig.datagen`'s default, and a look config pins
  `images.datagen` to the same tag-and-digest string explicitly.
- **Output neutrality:** the five-case byte-compare against the v1.6 release
  image (`sha256:5fda9025...`; F0, F1, C0, F2, C2 on development seeds 43 and
  42, `_corpus/` excluded) is equal:
  `tests/fixtures/datagen_reference/compare-48e18a417bf8.json`, the
  evidence of the image's row in `src/lakebench/config/datagen_lineage.yaml`.
- **Pull locations:** `docker.io/sillidata/lb-datagen@sha256:48e18a41...`
  (Docker Hub), the only location today. The second location the plan
  named, the cluster's internal registry, is not available: the OpenShift
  image registry on the lab cluster is `Removed`. The second location is an
  owner decision (OA3), pending; until it is made, DAT-2's "pulls succeed
  from both locations" is not met, and a Docker Hub outage blocks a look.
- **Library pins.** Generator: the build stage
  `rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e`,
  the runtime base
  `python:3.14-slim@sha256:7bf6c3111fe094f8ee1a1cbcdc63c4cfb345b0e3df42d5aa9a90b3b4b022ab6d`
  (its glibc and libm are part of generator identity), and
  `datagen_rs/Cargo.lock` with sha256
  `481940aff76e8325b816e0d0fb4b95ead232c1795bdf6c63c191d3ebdba297a0`
  (parquet and arrow 53.4.1, rand 0.8.8, rand_chacha 0.3.1, mimalloc
  0.1.52, ring 0.17.14, serde_json 1.0.151), built with `cargo build
  --release --locked`. Reference scorer: `REFERENCE_PY_DEPS` in
  `modules/pipeline_engines/spark/job.py` (numpy 2.2.6, scipy 1.15.3,
  pandas 2.3.3, scikit-learn 1.7.2, joblib 1.5.2, threadpoolctl 3.6.0,
  python-dateutil 2.9.0.post0, pytz 2025.2, tzdata 2025.2, six 1.17.0).
  `tests/test_aml_protocol_look_image.py` fails when this section and the
  tree disagree.

## The registered looks

The one-shot looks are already approved (#41, #42). Take them once:

1. the freeze commit (including the items above) and MODEL_VERSION are
   recorded;
2. the paired run-to-run standard deviation is reported (#44a);
3. per-typology predictions are committed (#46).

No tracked file holds the evaluation or robustness seed. The owner supplies
the seed for its look out of band; the operator sets it as
`workload.datagen.seed` with the matching `corpus_role`, and every guard
checks it by hash against `heldout_hashes.json`. A wrong value is refused.

On the cluster that seed is held only in a Secret in the deployment's
namespace, `lakebench-datagen-seed-<first 16 hex of its salted hash>`
(written by `generate`, immutable, labelled `app.kubernetes.io/component=
datagen-seed`, replaced by a registered generate for another seed, deleted
by `destroy`; a development generate does not touch it).
The datagen Job and the reference scorer read it as `LB_DATAGEN_SEED` from
that Secret, so it is in no Job argument, pod spec or SparkApplication spec,
and no Spark job of that deployment gets `LB_SEED`. `scripts/aml_gate.py`
reads a held-out seed only from `--seed-file PATH` (one integer, `chmod
600`) and refuses it on `--seed` in every mode.

What this does not hide: anyone who can read the corpus bucket can recover
the seed from the manifest's instance seeds, by design; the reference
scorer's report records it as `corpus_seed`; and the run record
(`metrics.json`, `report.html`) still stores the configured seed until the
run record writes the seed's salted hash instead. Do not check in the run
output of a registered generate before its look is recorded.

D8 and A6 are reported beside the result and do not gate it (#46, #47). The
result is published pass or fail.
