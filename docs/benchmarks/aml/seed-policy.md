# AML benchmark: seed policy

## 3.3 Seed policy

Every financial run has a seed. An unset `workload.datagen.seed` resolves to
43, the calibration corpus: the generator, the rule thresholds and the
reference features were developed on it, so a seed-43 figure is in-sample.
Any other seed that is neither spent nor held out is a development seed and
runs normally.

- **Cite the seed.** Published numbers name the run's seed and, when it is
  43, say so.
- **Spent seeds are refused** for every use: 42, 50000042 and every seed
  whose registered [look](../../glossary.md#look) was taken.
- **Held-out seeds are refused.** The repository stores the evaluation and
  robustness seeds only as salted hashes. A config that names one, or
  declares `datagen.corpus_role: evaluation` or `robustness`, is refused
  with exit 2: at config load, and by `run`, `benchmark`, `query` and the
  `financial` commands before any cluster call. Bronze-verify and the
  scorers read every manifest row and refuse a held-out or spent corpus
  whatever seed the config claims.
- A run record and its HTML report show a protected seed only as a salted
  reference (`{"seed_ref": ..., "role": ...}`).
- Generate each corpus into its own bronze prefix. The scorers' checks read
  only the manifest, so transaction files from another generator run left in
  the same prefix go undetected.

Registered looks at held-out corpora follow a maintainer protocol
([development.md](../../development.md#held-out-seed-protocol)).
