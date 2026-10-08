# Reproduction packages

Each YAML file in this directory pins a published lakebench benchmark result
to a specific commit, config, and set of expected numbers. Run any of them
against your own cluster with:

```
lakebench reproduce docs/reproductions/<name>.yaml
```

The command refuses if the config's namespace or bucket already exists. It
deploys, generates data, runs the pipeline, destroys only what it created
(unless `--keep`), compares actual against expected, and exits:

- **0** -- every metric within its tolerance band
- **14** -- requirement unmet: performance drift over the recorded band, a
  correctness violation (scale mismatch, a missing correctness metric),
  commit drift without `--allow-commit-drift`, or a run that differs from
  the package (maintenance policy, sample count, experiment identity or
  benchmark results)
- **2** -- refused before running: a package that does not parse, a
  missing config, a config whose sample count or maintenance policy
  differs from the package's, or a registered look's package without
  `--report`
- **3** -- refused: an existing namespace or bucket, a replaced deployment,
  or a held-out corpus the package would regenerate
- **1** -- the pipeline could not run, or its run could not be found

A registered AML look's package is never rerun. Pass `--report` with the
look's report: a match exits 0, a mismatch 14.

See [docs/deep-dive/reproduce.md](../deep-dive/reproduce.md) for the full
contract.

## Recording a new package

After a clean run, capture it as a package:

```
lakebench reproduce --record <RUN_ID> \
                    --write docs/reproductions/<name>.yaml \
                    --config-reference ../../examples/<config>.yaml
```

`config_reference` is stored as given and resolved relative to the
package file's directory at verify time, so a package in
`docs/reproductions/` names a repository example as
`../../examples/<config>.yaml`. `--config PATH` overrides it.

The recorded package embeds:
- The commit SHA at record time
- A summary of the run's config (no credentials)
- The expected numbers for every populated metric
- The query-set id, maintenance policy id, samples per query, experiment
  identity and per-query result fingerprints
- Default tolerance bands (20% performance, 0% correctness)

Credentials rotate independently of the package; verify runs use whatever
credentials the referenced config resolves at run time.

## Available packages

| File | Recipe | Scale | Mode | Notes |
|---|---|---|---|---|
| `c360-scale-0-1.yaml` | polaris-iceberg-spark-trino | 0.1 | batch | Legacy, verify refuses it (exit 2). Kept for the honesty test only |

Add rows to this table when you land a new package.

A package records the `maintenance_policy_id` of its source run, and
`lakebench reproduce` refuses (exit 2, before running anything) when it
differs from the policy of the running version. `reproduce --record`
refuses a source run from another policy.

### The legacy package

`c360-scale-0-1.yaml` records a run at commit `ead6722` with v1.6-shaped
keys. It is retained so the honesty test can prove `lakebench reproduce`
refuses legacy packages, not as a template. It has no
`maintenance_policy_id` or `experiment_identity` and its config reference
does not resolve, so every attempt exits 2 before running. Re-record from a
current run; do not copy from it.
