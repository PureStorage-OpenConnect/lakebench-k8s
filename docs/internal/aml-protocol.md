# AML registered evaluation protocol

Maintainer material, not shipped with the package. This is the mechanism
behind DESIGN.md invariant 7 (held-out evaluation data). The constants live in
`src/lakebench/spark/data/aml/aml_preregistration.json`, which is
authoritative; decision numbers (#NN) refer to the owner decision log kept
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
- After the freeze, any change that alters AML generator output (AML rows,
  manifest or MODEL_VERSION at a fixed seed) voids the freeze: it needs a
  MODEL_VERSION bump, a re-run of the calibration table and a new evaluation
  seed. Customer 360 only and output-neutral changes to `datagen_rs/` are
  fine when a byte-identical AML corpus at seed 43 proves them so.

## The registered looks

The one-shot looks are already approved (#41, #42). Take them once:

1. the freeze commit (including the items above) and MODEL_VERSION are
   recorded;
2. the paired run-to-run standard deviation is reported (#44a);
3. per-typology predictions are committed (#46).

D8 and A6 are reported beside the result and do not gate it (#46, #47). The
result is published pass or fail.
