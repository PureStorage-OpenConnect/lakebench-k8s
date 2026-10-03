# AML registered evaluation protocol

Maintainer material, not shipped with the package. This is the mechanism
behind DESIGN.md invariant 7 (held-out evaluation data). The constants live in
`src/lakebench/spark/data/aml/aml_preregistration.json`; only the copy at
the integrate branch tip is authoritative (older lane checkouts carry stale
versions, so rebase an AML-touching lane before using its constants); decision numbers (#NN) refer to the owner decision log kept
locally in `dev-artifacts/AML-GOALS.md` section 9, and R-numbers to its rules.

## Seeds and corpus roles

- Evaluation seed 50000043 and robustness seed 90000042 are generated and
  scored only as the registered look for their role
  (`scripts/aml_gate.py --registered`, `corpora.registered_looks_open`), once
  each, after the generator freeze.
- Spent seeds (42, 50000042) are refused for the AML schema
  (`config/datagen_seed.py`). A seed is appended to `spent_seeds` when a look
  at it is taken, voided or burned.
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

## The registered looks

The one-shot looks are already approved (#41, #42). Take them once:

1. the freeze commit (including the items above) and MODEL_VERSION are
   recorded;
2. the paired run-to-run standard deviation is reported (#44a);
3. per-typology predictions are committed (#46).

The corpus a look scores must be the one `lakebench generate
--registered-corpus` wrote on the look host: `scripts/aml_gate.py
--registered` requires a `generated` corpus-ledger entry for that role and
seed whose corpus fingerprint (data files by path and size, manifests by
sha256) matches the local copy, that was pinned to the `--generator-image`
digest with every datagen pod running one image, and with no other
generation's Job in that bronze prefix meanwhile, as far as this host's
ledger shows (owner, 10-03). So the look host is the generation host, and
the corpus is copied from S3 unchanged.

D8 and A6 are reported beside the result and do not gate it (#46, #47). The
result is published pass or fail.
