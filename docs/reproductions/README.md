# Reproduction packages

Each YAML file in this directory pins a published lakebench benchmark result
to a specific commit, config, and set of expected numbers. Run any of them
against your own cluster with:

```
lakebench reproduce docs/reproductions/<name>.yaml
```

The command destroys any existing deployment of the referenced config, then
deploys it, generates data, runs the pipeline, destroys the deployment again
(unless `--keep`), compares actual vs expected, and exits:

- **0** -- every metric within its tolerance band
- **14** -- requirement unmet: performance drift over the recorded band, a
  correctness violation (scale mismatch, a missing correctness metric),
  commit drift without `--allow-commit-drift`, or a run that differs from
  the package (maintenance policy, sample count, experiment identity or
  benchmark results)
- **2** -- refused before running: a package that does not parse, a
  missing config, or a config whose sample count or maintenance policy
  differs from the package's
- **1** -- the pipeline could not run, or its run could not be found

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
| `c360-scale-0-1.yaml` | polaris-iceberg-spark-trino | 0.1 | batch | **Legacy, verify refuses it (exit 2).** Kept for history; see below |

Add rows to this table when you land a new package.

A package records the `maintenance_policy_id` of its source run, and
`lakebench reproduce` refuses (exit 2, before running anything) when it
differs from the policy of the running version. `reproduce --record`
refuses a source run from another policy.

**`c360-scale-0-1.yaml` is a legacy package that `lakebench reproduce` refuses.** Every attempt
exits 2, for several independent reasons:

- It was recorded at commit `ead6722`, so any other HEAD is commit drift
  (exit 14 unless `--allow-commit-drift`).
- Its `config_reference` (`examples/c360-scale-0-1.yaml`) resolves
  relative to this directory, to `docs/reproductions/examples/...`, and no
  such example exists anywhere in the repository; `--config` is required.
- It has no `maintenance_policy_id`, so it counts as the legacy policy.
- It has no `experiment_identity`.

It has to be re-recorded from a new run made with the current version.
It is kept for history only.
