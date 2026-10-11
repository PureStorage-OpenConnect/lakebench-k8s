# AML benchmark: seed policy

## 3.3 Seed policy

A financial seed is mandatory: the generator exits 2 with no seed, and (default
image, or any image built from this release's source) with both `--seed` and
`LB_DATAGEN_SEED`. Resolution and refusals run at config load
(`config/datagen_seed.py`, called from the workload validator in
`config/schema.py`); a refused config exits 2 before anything deploys.

| Seed class | Rule |
|---|---|
| Unset | resolves to calibration seed 43. Refused with `corpus_role: evaluation` or `robustness` (a registered corpus names its seed, checked by hash). With `corpus_role: calibration` it uses 43 |
| Development | any seed neither spent nor held out. The AML protocol allows generator tuning only on seed 43 and its four pre-registered replicates; Lakebench does not enforce that. `corpus_role: calibration` requires seed 43 |
| Spent | refused for every use, a declared role included: the pre-registration's spent list (42 and 50000042), every seed with a recorded or burned [look](../../glossary.md#look) in `aml_registered_looks.json`, and the spent list in `heldout_hashes.json` |
| Spent, at generator start | the default generator refuses again the spent seeds it knows (its compiled list and the hash file's spent list), not seeds known only from a recorded look |
| Held out (evaluation, robustness) | the repository stores no held-out seed, only a salted SHA-256 hash per role in `spark/data/aml/heldout_hashes.json` (append-only). The current hashes are also compiled into the guard |
| Held out, acceptance | accepted while the pre-registration has registered looks open, with the matching `corpus_role` and a seed hashing to that role; refused without the role or when it hashes to another role |

`datagen.robustness_perturbation` (financial only): at config load required
with `corpus_role: robustness`, refused with `calibration` or `evaluation`,
allowed without a role on any seed the guard accepts. At start the generator
refuses the robustness seed without it, and the evaluation seed or a
calibration replicate with it. The reference scorer reads the perturbation
from the manifest's stamp, not the config.

**Held-out checks.** Five points hash the seed they see against the per-role
hashes; no refusal message prints a held-out seed:

1. config load;
2. the look guard after it (below);
3. generator start (`datagen_rs/src/heldout.rs`, default image; images built
   from older source, `1.6.0` included, lack it), which refuses a financial
   corpus without the hash file Lakebench mounts on the datagen pod;
4. bronze-verify (`spark/scripts/bronze_verify_financial.py`) and the recall
   scorer (`spark/scripts/score_financial.py`), which recover the corpus seed
   from every manifest row and refuse a held-out or spent corpus;
5. the cluster reference scorer (`spark/scripts/score_financial_reference.py`),
   which does the same and refuses a spent-seed corpus, or a held-out one
   outside its registered run, whatever the config claims.

**Registered corpora.** A financial config that declares `corpus_role:
evaluation` or `robustness`, or names a seed hashing to a held-out role:

- `lakebench generate` writes the seed into a Kubernetes Secret in the
  deployment's namespace, named after its salted hash (`config/seed_secret.py`).
  The datagen Job and the reference scorer read it as `LB_DATAGEN_SEED`, so it
  appears in no Job argument, pod spec or SparkApplication spec.
  `lakebench destroy` deletes it. Every other seed, 43 included, is a plain
  argument.
- Set `datagen.corpus_role` only on the one deployment that generates a
  registered corpus, and score it only with `scripts/aml_gate.py --registered`.
  That script takes the held-out seed only from `--seed-file` (refused on
  `--seed`), records it as spent before any model is fitted, and records the
  report's sha256 before printing a verdict. No `lakebench` command records a
  look.

**The look guard** (`aml/look_guard.py`). `run`, `benchmark`, `query`
and the `financial` commands refuse a protected corpus right after
config load, before any cluster call, with exit 2 (`run.protected_corpus`).
Protected: the config declares `corpus_role: evaluation` or `robustness`,
names a seed hashing to a held-out role, or (AML) points its bronze datagen
prefix at one where this host generated a registered corpus.

- `generate` writes a protected corpus only with `--registered-corpus --yes`
  and a digest-pinned `images.datagen`. It records the attempt in the host's
  corpus ledger (`~/.lakebench/aml_corpora.jsonl`, `LB_AML_CORPORA_LEDGER`)
  before its first cluster call, then `generated` (with the corpus
  fingerprint and pod image ids) or `failed`. It refuses a seed whose look
  was already taken.
- `scripts/aml_gate.py --registered` scores a look only when the ledger holds
  a matching `generated` entry for the corpus it reads.
- Financial bronze-verify reads every manifest row first and stops (exit 2)
  on a held-out or spent seed's corpus, a manifest no corpus seed can be
  recovered from, or a batch corpus with no manifest. A `run --stage` subset
  runs that check alone first; a check that could not run (storage error)
  exits 1.
- `scripts/aml_heldout_audit.py` lists this host's run records, journals,
  ledgers and ledger buckets that touch a protected corpus. An AML record is
  also known by its workload name, so one missing its corpus block is listed
  as unidentified.
- A run record, its datagen fleet record and its HTML report never show a
  protected seed (held out, spent or with a recorded look). It is recorded as
  its salted reference (`{"seed_ref": ..., "role": ...}`, the role set for a
  held-out seed), and as withheld (`{"seed_ref": null}`) when the held-out
  record cannot be read. Other seeds stay in plaintext, so a record names its
  corpus; the corpus id is unchanged.
- The release evidence also rests on the expected-results corpus id and
  bronze-verify's in-run refusal.
- `destroy` and the read-only commands skip the load-time seed check, so a
  deployment that generated a registered corpus can still be torn down.

A protected corpus cannot be scored by accident from Lakebench, and the
scorers' manifest checks catch a hand-made datagen Job. They read only the
manifest, so transaction files from another generator run left in the same
bronze prefix go undetected. Generate each corpus into its own prefix.

**Hash strength.** The salt in `heldout_hashes.json` is public, so a hash
hides only a uniform 63-bit seed. The current 8-digit seeds are recovered in
seconds; they are already public, so the hash records their role. A held-out
seed added later must be a uniform 63-bit draw.

**Publishing and looks.**

- Numbers published for comparison should cite the run's seed and, when it
  is 43, say so.
- The AML protocol grants each held-out seed exactly one registered look.
- The look, its calibration and its predictions use one datagen image, named
  by digest.
- A change to generator output needs a new image and a `MODEL_VERSION` bump,
  and a look on that image needs its calibration re-run.
- Datagen is never tuned to a rule's recall or false-positive rate on any
  seed.
