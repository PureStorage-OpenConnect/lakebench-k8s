# Reproduction packages

Each YAML file in this directory pins a published lakebench benchmark result
to a specific commit, config, and set of expected numbers. Run any of them
against your own cluster with:

```
lakebench reproduce docs/reproductions/<name>.yaml
```

The command deploys the referenced config, generates data, runs the pipeline,
compares actual vs expected, and exits:

- **0** -- every metric within its tolerance band
- **1** -- performance drift over the recorded band
- **2** -- correctness violation (missing stages, scale mismatch)

See [docs/deep-dive/reproduce.md](../deep-dive/reproduce.md) for the full
contract.

## Recording a new package

After a clean run, capture it as a package:

```
lakebench reproduce --record <RUN_ID> \
                    --write docs/reproductions/<name>.yaml \
                    --config-reference examples/<config>.yaml
```

The recorded package embeds:
- The commit SHA at record time
- The config snapshot (redacted -- no credentials)
- The expected numbers for every populated metric
- Default tolerance bands (20% performance, 0% correctness)

Credentials rotate independently of the package; verify runs use whatever
credentials the referenced config resolves at run time.

## Available packages

| File | Recipe | Scale | Mode | Notes |
|---|---|---|---|---|
| `c360-scale-0-1.yaml` | polaris-iceberg-spark-trino | 0.1 | batch | Baseline c360 sanity run |

Add rows to this table when you land a new package.
