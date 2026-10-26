# AML benchmark: seed policy

## 3.3 Seed policy

Every financial run has a seed. An unset `workload.datagen.seed` resolves to
43, the calibration corpus: the generator, the rule thresholds and the
reference features were developed on it, so a seed-43 figure is in-sample.
Any other seed that is neither spent nor held out is a development seed and
runs normally.

- Published numbers name the run's seed and, when it is 43, say so.
- Spent seeds are refused for every use: 42, 50000042 and every seed
  whose registered [look](../../glossary.md#look) was taken.
- Held-out seeds are refused. The repository stores the evaluation and
  robustness seeds only as salted hashes. Config load refuses one, exit 2,
  unless `datagen.corpus_role` declares its role. `run`, `benchmark`,
  `query`, the `financial` commands and `generate` without
  `--registered-corpus` then refuse the seed or the role before any
  cluster call. Teardown, read and inspect commands (`destroy`, `status`,
  `report`, `config show`) skip both checks. Bronze-verify and the
  scorers read every manifest row and refuse a held-out or spent corpus
  whatever seed the config claims.
- A run record and its HTML report show a protected seed only as a salted
  reference (`{"seed_ref": ..., "role": ...}`).
- Generate each corpus into its own bronze prefix. The scorers' checks read
  only the manifest, so transaction files from another generator run left in
  the same prefix go undetected.

Registered looks at held-out corpora follow a maintainer protocol
([development.md](../../development.md#held-out-seed-protocol)).
