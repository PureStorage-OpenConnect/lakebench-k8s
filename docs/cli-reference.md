# CLI Reference

Lakebench provides a single `lakebench` command with subcommands for every
stage of the deployment and benchmarking lifecycle. The CLI is built with
Typer and Rich.

```
lakebench COMMAND [ARGUMENTS] [OPTIONS]
```

Each command's usage line, arguments, options and named exit paths below
are generated from the CLI by `scripts/gen_cli_reference.py` (the blocks
between `BEGIN GENERATED` and `END GENERATED` markers); a unit test fails
when they drift. Change a flag's description in its help text in the code,
and the prose here outside the blocks.

Most commands accept an optional config file argument. If omitted, the CLI
looks for `./lakebench.yaml` in the current directory.

Most commands also take the config as `--file` / `-f` (on `destroy`,
`clean` and `logs` only the long form `--file`; see [clean](#clean)).

Several commands prompt for confirmation before running. Use `--yes` / `-y`
to skip the prompt; `destroy` and `clean` also accept `--force` as the same
flag.

Exit codes are listed in [Exit Codes](exit-codes.md).

### Machine-readable output (`--json`)

`plan`, `status`, `report`, `config recipes`, `compare` and `query` take
`--json`. The command then writes exactly one JSON document to stdout and
every human line to stderr:

```json
{"schema": "lb-cli/1", "command": "status", "exit_code": 1,
 "data": {"namespace": "...", "verdict": "drift", "components": [...]},
 "errors": [{"code": 1, "path": null, "what": "Drift: ...", "why": null,
             "next": null, "where": null}]}
```

`exit_code` is always the process's exit code, including for an unknown
option or a bad value (`--help` prints help only). `data` is the command's
result, kept on a verdict exit such as `status` drift, and `null` when the
command failed; `errors` holds each error the command reported, with its
exit-code `path` when it has one (see [Exit Codes](exit-codes.md)). The
shape of each command's `data` is a TypedDict in `lakebench/cli/_json.py`:
`lb-cli/1` may gain keys, and never loses or retypes one. `query --json` names the engine and keeps
its rows as it prints them: Trino's CSV has no header (`columns` null),
Spark Thrift's tsv2 has one, and DuckDB returns up to 100 rows as Python
reprs; `count` is the rows the engine returned. `report --json` gives the
verdict and scores as stored, never recomputed. `compare --json`
carries the `cmp2` document `--format json` writes; `--json` does not
combine with `--format` on `report`, `compare` or `query`, nor with
`status --local` or `query --interactive`. `plan --json` makes no cluster
call, as before.

## Commands

### init

Write a starter configuration file.

<!-- BEGIN GENERATED: cli init (scripts/gen_cli_reference.py) -->
```
lakebench init [OPTIONS]
```

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--output` | `-o` | path | `lakebench.yaml` | Output file path for configuration |
| `--name` | `-n` | text |  | Deployment name (default: lb-<user>-<4 hex>, unique per init) |
| `--scale` | `-s` | float |  | Scale factor: 1 is about 10 GB of bronze for customer360, 8.4 GB for financial (default 1; 0.1 with --local) |
| `--endpoint` |  | text |  | S3 endpoint URL (e.g. http://your-s3:80 or https://your-s3:443) |
| `--credentials-env` |  | text | `LAKEBENCH_S3` | Environment variable prefix for the S3 credentials: the config references ${PREFIX_ACCESS_KEY} and ${PREFIX_SECRET_KEY} |
| `--namespace` |  | text |  | Kubernetes namespace (default: same as deployment name) |
| `--recipe` | `-r` | text |  | Architecture recipe (default polaris-iceberg-spark-trino; see 'config recipes') |
| `--workload` | `-w` | text |  | Workload schema (customer360 \| financial). Default is customer360. |
| `--overwrite` |  | flag |  | Overwrite an existing file |
| `--force` |  | flag |  | Old spelling of --overwrite |
| `--from` |  | path |  | Rewrite an older config in the current format: keeps its name and buckets, moves plaintext secrets to ${VAR} references and lists every moved or dropped key. Never writes over OLD. |
| `--local` |  | flag |  | Generate a config for local mode (podman/docker, no Kubernetes) |
<!-- END GENERATED: cli init -->

- `--overwrite` keeps the file's name, and refuses (exit 3) a change of
  namespace, buckets, endpoint or recipe under that name. `--force` is its
  old spelling (`-f` is deprecated here).
- `--credentials-env PREFIX`: the file references `${PREFIX_ACCESS_KEY}` and
  `${PREFIX_SECRET_KEY}`.

The file has 12 lines of settings: the name, the recipe once, the workload
and scale, the endpoint and the two S3 credentials as `${VAR}` references.
The components the recipe sets are written as a commented block; written
out, they must agree with the recipe or the config is refused at load. No
plaintext secret is written, and no Polaris client secret: deploy creates
one for a new Polaris. `init` prints what it chose on stderr, with or
without a terminal, and refuses (exit 2, nothing written) a combination
that would not load, such as `--workload financial` with a Delta recipe.

The wizard is removed. `--interactive`, `-i` and `--advanced` print one line
saying so and write the default config. `--access-key` and `--secret-key`
are refused with exit 2; export the variables instead.

```bash
# Default: Polaris, customer360, scale 1
lakebench init
export LAKEBENCH_S3_ACCESS_KEY=... LAKEBENCH_S3_SECRET_KEY=...

# Another recipe and endpoint, credentials from MY_LAB_ACCESS_KEY / MY_LAB_SECRET_KEY
lakebench init -r hive-iceberg-spark-trino --endpoint http://my-s3:80 --credentials-env MY_LAB
```

### compare

Compare two sides of stored run records. `compare` is read-only: it reads
`metrics.json` files and series manifests, and deploys, runs, generates and
destroys nothing.

<!-- BEGIN GENERATED: cli compare (scripts/gen_cli_reference.py) -->
```
lakebench compare SIDE_A SIDE_B [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `SIDE_A` | yes | Side A (the baseline): run ids, run directories, metrics.json files, series:<id> or a config, comma-separated |
| `SIDE_B` | yes | Side B (the candidate), in the same forms |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--runs-dir` |  | path, repeatable |  | Directory of run-<id>/ records (repeatable; default lakebench-output/runs) |
| `--format` |  | text | `table` | Output format: table, json, csv |
| `--output` | `-o` | path |  | Write the comparison (json, or csv) to this file |
| `--json` |  | flag |  | Write one lb-cli/1 JSON document to stdout; human text goes to stderr |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `0` `compare.like_for_like`: `compare` finds the sides like-for-like
- `2` `compare.equal_names`: `compare` was given two configs with the same deployment name and different contents
- `2` `compare.bad_ref`: a `compare` side names a run, record, series or config that resolves to no record
- `2` `compare.same_runs`: the two `compare` sides resolve to the same runs, or share a run
- `2` `compare.unreadable_record`: a `compare` record or series manifest cannot be read, or two files disagree about one run
- `2` `compare.removed_flag`: a flag of the `compare` that ran both configs; the message names the replacement
- `10` `compare.not_comparable`: `compare` verdict
- `11` `compare.not_established`: `compare` verdict
- `12` `compare.not_like_for_like`: `compare` verdict
- `13` `compare.confounded`: `compare` verdict
<!-- END GENERATED: cli compare -->

- `--runs-dir` can be repeated to search several directories; series
  manifests are read from each directory's sibling `series/`.
- `--output` writes the comparison (JSON, or CSV with `--format csv`);
  nothing is written without it. A path that is one of the inputs, is named
  `metrics.json`, or lies inside a runs or series directory is refused.

A side is one or more comma-separated refs. Each ref is tried in this order:

1. `series:<id>`: the members of a `run --repeat` series, read from its
   manifest `<output dir>/series/<id>.json`.
2. A run id (`20260929-212900-5105a0`, with or without `run-`), looked up in
   every `--runs-dir` (default `lakebench-output/runs`). Two directories
   holding different files for one run id are refused.
3. A run directory holding `metrics.json`, or a path to a `metrics.json`.
4. A config (`.yaml`/`.yml`): the latest run record of its deployment name,
   by `start_time`. When that record belongs to a series, every member the
   series manifest names is taken; a series whose manifest is missing is
   refused (pass the run ids instead). A record in the runs directories
   that cannot be read is skipped with a warning naming it.

Side A is the baseline and B the candidate; deltas are B relative to A.

```bash
lakebench compare 20260929-212900-5105a0 20260929-214442-825153
lakebench compare a.yaml b.yaml --format json -o comparison.json
lakebench compare series:s-20261101-120000-a1b2c3 b.yaml
lakebench compare r1,r2,r3 r4,r5,r6 --runs-dir lakebench-output/runs
```

The flags of the earlier `compare`, which ran both configs (`--keep`,
`--scale`, `--skip-benchmark`, `--timeout`, `--local`, `--generate`,
`--yes`), are refused with exit 2 and the replacement: run each side first
with `lakebench run`, then compare.

**Resolution.** Before anything else `compare` prints, on stderr, how each
side resolved: its refs, deployment, series and every member with its
verdict, for example `A: a.yaml -> deployment lb-a1, series s-..., 3 runs:
r1 passed, r2 passed, r3 FAILED: ... (excluded)`. A member whose verdict did
not pass is excluded and listed; n counts the passed members. A record
written before the experiment block is not excluded: the pair is refused
on it. Two sides that resolve to the same runs, or share a run, are
refused, as are two configs with one deployment name and different
contents, whether on two sides or listed on one ("a name is one
deployment"). The same config, or a byte-equal copy, given twice is not an
equal-name refusal; given as both sides it is the same runs, refused as
such.

**Verdicts.** The pair is decided by the comparability ladder over the
passed members, and the exit code is the verdict:

| Verdict | Meaning | Exit |
|---|---|---|
| LIKE-FOR-LIKE | Same experiment, equal results, same execution conditions. The attribution says what differs: the architecture, the system, or nothing (a repeat) | 0 |
| NOT COMPARABLE | Different experiments (workload, version, mode, corpus, seed, scale, generator), different results, a run that did not pass, a record without the experiment block, or a side whose runs are not one experiment (a side whose runs differ only in in-stream rounds or investigator sessions is one experiment: NOT LIKE-FOR-LIKE) | 10 |
| NOT ESTABLISHED | Nothing contradicts the pair, but a side has no checked results (no benchmark, a recipe without a query engine, a continuous run without an end-of-run result check) | 11 |
| NOT LIKE-FOR-LIKE | Comparable, but an execution condition differs: effective maintenance or its compaction operation (maintenance skipped by the user on both sides counts as the same, across table formats), maintenance settings, benchmark iterations or mode, in-stream rounds (between the sides or inside one), the Lakebench limits that bound; or the only architecture difference is the dependency set | 12 |
| CONFOUNDED | Comparable, but the architecture and the system both differ, so no difference can be put down to either | 13 |

Usage errors (a ref that resolves to nothing, the same runs on both sides,
an unreadable record or manifest, a removed flag, an unsupported format)
exit 2.

**The missing condition.** Every verdict comes with the one condition the
pair lacks, for the first ladder step that failed, and, where one exists,
the command that supplies it, for example "corpus scale differs (1 vs 10).
Missing: the same corpus. Set `architecture.workload.datagen.scale: 1` in
b.yaml, then `lakebench run b.yaml --generate --regenerate`" (a continuous
side regenerates with `lakebench run <config> --continuous`). Some
conditions have no command: two table formats that run different
maintenance operations, compaction by different engines, a Lakebench
bound, a differing round count. Commands name the side's config from its
record (`provenance.config_path`), or "the config of deployment <name>"
for a record that does not carry it. A continuous side cannot be repeated
with `--repeat`, so its hint says to run it again with `--continuous` and
pass the run ids. Two continuous runs on one side whose in-stream rounds
differ are still one experiment (rounds are an outcome of speed): the pair
is NOT LIKE-FOR-LIKE, with no command. A run with no in-stream round (its
QpH is the post-stream benchmark) beside runs with rounds is not one
experiment.

**Metrics.** Each score is shown with the median, range and n of each side
and the delta of the medians. No winner is named and no colour marks a
better side: the winner rule is not in this release. Each row's
`assessment` says what may be read from it:

| Assessment | When |
|---|---|
| `withheld` | The pair is NOT COMPARABLE or NOT ESTABLISHED; the delta is not computed |
| `not_directional` | The score has no better side (correctness, guard and diagnostic scores, scores that follow the config, a score the registry does not know, or a mode-dependent score on a record without a mode) |
| `confounded` | The pair is confounded |
| `not_assessed` | A median over in-stream rounds (`composite_qph`, `in_stream_composite_qph`, `qph_degradation_pct`) on a side whose rounds ran different query sets or all missed the same query, on any pair that is not withheld (the hint names the side); every other directional row: on a NOT LIKE-FOR-LIKE pair because the pair is not like-for-like, otherwise because the winner rule is not in this release |
| `capped` | On a like-for-like pair, a Lakebench limit bound the row on a passed member of either side (a bound kind the row depends on, or the trickle of a continuous run): the figure measures that limit, not the system |

Whatever its assessment, a row that a Lakebench limit bound on either side
lists the limit in `capped_by` (`bound_by` in the CSV), and the table says
BOUNDED BY it. Directions come from the metric registry
(`metrics/metric_registry.py`).

**Output.** `--format json` (and `-o`) writes the `cmp2` document:
`verdict`, `exit_code`, `step`, `attribution`, `missing` (`condition`,
`command`, `hint`), `cause`, `reasons`, `notes`, `sides` (per side: `refs`,
`deployment`, `series`, `members` with `run_id`, `verdict`, `digest`,
`excluded` and `reason`, `n_attempted`, `n_passed`, the first passed
member's `experiment` block, `support`, `bound`), `groups` (the differing
keys per identity group), `warnings` and `metrics` (per row: `metric`,
`unit`, `direction`, `a` and `b` with `median`, `min`, `max`, `values`, `n`,
`delta_pct`, `assessment`, `winner` (always null), `missing`, `hint`,
`capped_by`, and `rounds`, which says why a round median is not assessed
for its rounds, else null). `--format csv` writes the header fields as `# key: value`
lines, then one row per metric with `metric`, the medians and ranges,
`delta_pct`, `verdict`, `attribution`, `n_a`, `n_b`, `assessment`,
`bound_by` and `rounds`; the table prints the `rounds` reason after the
assessment. A protected or spent AML seed is never printed; it reads
`<protected seed>`.

### config

Configuration management subcommands.

```
lakebench config show CONFIG_FILE       # show resolved config with source annotations
lakebench config validate CONFIG_FILE   # validate config + test connectivity
lakebench config storage CONFIG_FILE    # check the S3 backend supports what lakebench needs
lakebench config recommend CONFIG_FILE  # largest scale the cluster holds, sized from this config
lakebench config recipes [NAME]         # list recipes, what each one trades off, and support states
```

`config validate` and `validate` load the config as `deploy` does, so they
fail on a config with no `name:` or with a removed key.

`config show` prints the config's support state and its peak requested
resources. `config recipes` lists, for every recipe, its support state per
workload and mode (Customer 360 and AML, batch and continuous: supported,
unverified or unsupported) and whether it runs in local mode; the detail
view (`config recipes NAME`) gives the basis for each state. See
[Compatibility Matrix](compatibility-matrix.md#support-states).

`config validate` and `config recipes` both take `--local`: for `validate`,
it validates for local (podman/docker) mode instead of Kubernetes; for
`recipes`, it filters the list down to recipes that can run in local mode.

#### config storage

Runs graded conformance checks against the configured S3 endpoint and reports
what the backend does. Diagnostic only: it never gates `deploy` or `run`, so a
store lakebench has not seen before is checked rather than refused.

Use `--no-full` when the account cannot create buckets: the write and
multipart checks are then reported as skipped, not failed.

Exit code 0 means no required check failed. Exit code 1 means a required check
failed; 2 means the config could not be loaded or names no S3 endpoint.

Checks are graded. **Required** failures (connectivity, bucket enumeration,
object operations, multipart abort) mean lakebench cannot run against the store.
**Advisory** results (sigv4 region strictness) are recorded because they change
how lakebench configures Spark, not because they are defects.

See [Storage Backends](storage-backends.md) for validated backends, what each
check covers, and what it deliberately does not.

#### config recommend

`config recommend CONFIG` sizes the config with `lakebench.config.sizing`,
the source the `run` capacity preflight also uses: each scale is decided by
the check the preflight makes (batch: a `run --generate`). Continuous mode
prints two answers: a plain `run`, whose datagen Job is counted beside the
streams, and a corpus generated before the streams start (`generate`, then
`run --skip-generate` within an hour). A scale above the workload's largest
measured scale (300) is labelled unverified.

#### Subcommand reference

<!-- BEGIN GENERATED: cli config (scripts/gen_cli_reference.py) -->
#### `config show`

Show fully resolved configuration with source annotations.

```
lakebench config show [CONFIG_FILE]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Configuration file path |

#### `config validate`

Validate configuration and test connectivity.

```
lakebench config validate [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Configuration file path |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--local` |  | flag |  | Validate for local mode instead of Kubernetes |

#### `config storage`

Validate that the S3 backend supports the operations lakebench needs.

```
lakebench config storage [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Configuration file path |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--full` / `--no-full` |  | flag | `--full` | Create a temporary bucket for write and multipart checks. Use --no-full when the account cannot create buckets; those checks are then skipped rather than failed. |

#### `config recommend`

Show sizing guidance for your cluster, sized from this config.

```
lakebench config recommend [CONFIG_FILE]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Configuration file path (sized as written, at each scale) |

#### `config recipes`

List architecture recipes and what each one trades off.

```
lakebench config recipes [NAME] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `NAME` | no | Show full detail for one recipe |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--local` |  | flag |  | Show only recipes that run in local mode |
| `--json` |  | flag |  | Write one lb-cli/1 JSON document to stdout; human text goes to stderr |
<!-- END GENERATED: cli config -->

### validate

Equivalent to `lakebench config validate`. Validate configuration and test connectivity to S3 and Kubernetes.

<!-- BEGIN GENERATED: cli validate (scripts/gen_cli_reference.py) -->
```
lakebench validate [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--verbose` | `-v` | flag |  | Show detailed validation output |
<!-- END GENERATED: cli validate -->

Checks performed: YAML syntax, required fields, S3 endpoint reachability,
S3 credential validity, Kubernetes context accessibility, namespace status,
platform security (SCC on OpenShift), storage classes, Spark Operator status,
and the configured scale. Executor sizing is not graded: it is the job
profiles'. Before the first deploy, a
namespace the Spark Operator does not watch yet is reported as advisory:
`deploy` adds it to the watch list under the cluster lock.

### plan

Show what each config needs before anything is deployed. Read-only.

<!-- BEGIN GENERATED: cli plan (scripts/gen_cli_reference.py) -->
```
lakebench plan CONFIG_FILES [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILES` | yes | One or more configuration files |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--offline` |  | flag |  | Make no cluster call: size without a cluster |
| `--cores` |  | integer, at least 1 |  | Cluster CPU cores to size against (implies offline) |
| `--memory` |  | integer, at least 1 |  | Cluster memory in GB to size against (with --cores) |
| `--name` |  | text |  | The deployment name for a config that sets none |
| `--json` |  | flag |  | Write one lb-cli/1 JSON document to stdout; human text goes to stderr |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `0` `plan.ok`: `plan` finds every prerequisite and enough capacity
- `4` `plan.missing_storage_class`: `plan` finds a prerequisite failing (the scratch StorageClass, the Spark Operator, Stackable or another check), cannot check one of those three, finds too little free capacity, or cannot read a config value the sizing needs
<!-- END GENERATED: cli plan -->

For each config `plan` prints the components and recipe with their support
state; the minimum cluster from the same sizing function the `run` capacity
preflight and the README tables use, with the scratch request ("not
requested (scratch disabled)" when off); the cluster prerequisites from the
registry `deploy` checks (`docs/prerequisites.md`) and the run's free
capacity check; where the Polaris client secret comes from (a `${VAR}`
reference is named, a value is never printed); and the hosts outside the
cluster the deployment contacts (the hosts the dependency server resolves
the jars, wheels and DuckDB extensions from at deploy, which are the
`egress-hosts` prerequisite's list: Maven Central and its Google mirror,
PyPI where used, or the `platform.deps` mirrors; the observability chart
when enabled; the image registries). With several
configs it then names the experiment-identity and execution-condition
differences between each one and the first; configs on different cluster
contexts are planned one at a time (a second context in one process is
refused, exit 3).

Online, any prerequisite that fails, a scratch StorageClass, Spark Operator
or Stackable that cannot be checked, or too little free capacity ends with
exit 4; a missing shared component prints `Next: (cluster admin) lakebench
admin install --component <component>`. An unreachable cluster is exit 4
with "use --offline". With `--offline`, `--cores/--memory` or `--json`
there is no cluster call and prerequisites read "not checked (offline)";
`--cores/--memory` checks the aggregate only (the node shape is unknown, so
the largest pod is not checked) and is exit 4 when the config does not
fit. A config that does not load, or that `deploy` would refuse at load
(for example a name too long for the names derived from it), is exit 2.

```bash
lakebench plan lakebench.yaml --offline
lakebench plan hive.yaml polaris.yaml --cores 434 --memory 4349
```

### deploy

Deploy lakehouse infrastructure to Kubernetes.

<!-- BEGIN GENERATED: cli deploy (scripts/gen_cli_reference.py) -->
```
lakebench deploy [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--dry-run` |  | flag |  | Show what would be deployed without making changes |
| `--yes` | `-y` | flag |  | Skip confirmation prompt |
| `--timeout` | `-t` | integer | `3600` | Global deployment timeout in seconds (0 = no timeout); bounds the waits in every step |
| `--local` |  | flag |  | Deploy locally with podman/docker instead of Kubernetes |
| `--workdir` |  | path |  | Host directory for local mode state (default: ~/.lakebench/local/<name>) |
| `--force-legacy` |  | flag |  | Claim ownership without tag proof. Covers two cases: (1) a pre-1.5 annotation-less namespace or untagged bucket being migrated; (2) a bucket on a backend that does not implement bucket tagging AND does not match the deployment-name prefix. Use only when you have confirmed the resources are yours -- a mistake can silently take over another team's storage. |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `3` `deploy.state_copied`: `deploy` found a state written for another directory or host (a copied directory)
- `3` `deploy.identity_foreign`: the namespace or a bucket is owned by another deployment, or has no lakebench ownership proof (`deploy`, `destroy`, `clean`)
- `4` `deploy.state_unrecordable`: `deploy` could not read the namespace or write the nonce to the directory's state
<!-- END GENERATED: cli deploy -->

Before it creates anything, deploy runs `run`'s cluster capacity check
(read-only, without datagen: deploy does not generate) and refuses with exit
4 when the cluster's free capacity (allocatable minus what pods in other
namespaces request) or its largest free node cannot hold the config's
pipeline and always-on pods, sized against the worker nodes' allocatable
as the deploy itself sizes. When only the pod side of free capacity cannot
be read (a pod list it may not read, a pod on a node the list does not
show) it checks the workers' allocatable instead. When it cannot read
capacity otherwise (an unreachable cluster, a node list it may not read,
no worker or no schedulable node) it skips the check with a warning, since
deploy has no `--skip-preflight` and `run`'s preflight refuses until the
capacity can be read. A worker node quantity it cannot read refuses.
`--dry-run` prints the result without refusing.

Deploys components in order: namespace, secrets, S3 buckets, scratch
StorageClass check (it must already exist; `lakebench admin install
--component scratch-storage-class` installs it), PostgreSQL, catalog (Hive or
Polaris), Spark RBAC, Unity Catalog (only when `catalog.type` is `unity`; no
recipe uses it), Spark Operator check and watch-list entry for the
namespace (under the cluster lock), the dependency server (`lb-deps`:
resolves the jars and wheels onto its own PVC and serves them in the
namespace), then the query engine (Trino, Spark Thrift or DuckDB), and
optionally this deployment's monitors on the shared observability stack.
Deploy never installs a shared component (the Spark Operator, the Stackable
operators, the scratch StorageClass, the observability stack): a missing one
fails its step with the `lakebench admin install --component` command. The
Spark Operator step always runs: it checks the operator is ready and adds the
namespace to its watch list.

With `--local`, lakebench runs the same pipeline against podman/docker
containers on the local machine instead of a Kubernetes cluster, useful for
testing a recipe without a cluster available.

### generate

Generate synthetic data to the bronze S3 bucket.

<!-- BEGIN GENERATED: cli generate (scripts/gen_cli_reference.py) -->
```
lakebench generate [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--timeout` | `-t` | integer | `0` | Timeout in seconds when waiting for completion. 0 (default) auto-computes from scale, parallelism and a conservative per-pod throughput; pass a positive int to override. |
| `--yes` | `-y` | flag |  | Skip confirmation prompt |
| `--regenerate` |  | flag |  | Clear the datagen prefix in the bronze bucket before generating, when this deployment owns the bucket. Without this flag, a non-empty bronze prefix is refused (exit 3) so existing datagen output is never overwritten silently. Never clears a bucket this deployment cannot prove it owns. |
| `--allow-stale-bronze` |  | flag |  | Generate over objects already in the datagen prefix of a bronze bucket this deployment did not create. Rows may be over-counted; the run records it. |
<!-- END GENERATED: cli generate -->

- `--regenerate` clears the datagen prefix, not the whole bucket, and only
  on a bucket this deployment owns (its stamp, or a bucket it created). On
  any other bucket it is refused (exit 3); `lakebench admin reclaim-bucket`
  claims one.
- `--allow-stale-bronze`: `run` records the over-count in `metrics.json`
  (`datagen.stale_bronze`) and the report shows "bronze held N objects
  before generate".

Runs parallel Kubernetes Jobs to produce Parquet files. At scale 100 this
generates approximately 1 TB of data. Use `--timeout` for large scales that
may take hours. Without `--yes`, the command prompts for confirmation before
submitting jobs. The Rust generator has no checkpoint-resume; an interrupted
run is re-run from the start.

**Exit codes**: `0` on success, `1` on generic failure and when datagen
exceeds its wait budget (`--timeout`), `3` (refused) when the bronze datagen
prefix is non-empty and neither `--regenerate` (on a bucket this deployment
owns) nor `--allow-stale-bronze` (on one it does not) applies, or
`--regenerate` was passed for a bucket it does not own or with an empty
datagen prefix, and `4` when bronze or its ownership cannot be checked;
when datagen exceeds its wait budget the datagen Job and any leftover
streaming SparkApplication consuming the trickle are stopped before exit.
Only the initial-pass datagen is guarded by this exit code; per-cycle
datagen inside a multi-cycle run reports its own timeout independently.

### run

Execute the data pipeline (batch or continuous).

<!-- BEGIN GENERATED: cli run (scripts/gen_cli_reference.py) -->
```
lakebench run [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--stage` | `-s` | text |  | Run specific stage only (bronze-verify, silver-build, gold-finalize) |
| `--timeout` | `-t` | integer |  | Timeout per job in seconds (auto-scaled from data scale if omitted) |
| `--skip-benchmark` |  | flag |  | Skip the query benchmark after pipeline completion |
| `--continuous` |  | flag |  | Run in continuous mode: bronze-ingest -> silver-stream -> gold-refresh |
| `--duration` |  | integer |  | Continuous run duration in seconds (default: from config, typically 1800) |
| `--generate` |  | flag |  | Run datagen before pipeline stages (single-cycle batch only: a multi-cycle run generates in its cycles and refuses it; continuous always runs datagen) |
| `--skip-preflight` |  | flag |  | Skip prerequisite checks (including the capacity check) and infrastructure validation; the record says capacity not checked |
| `--skip-deploy` |  | flag |  | Skip the deploy and the infrastructure readiness check; the read-only prerequisite checks, cluster capacity included, still run |
| `--skip-generate` |  | flag |  | Assume data already exists in bronze bucket |
| `--regenerate` |  | flag |  | With --generate: clear the datagen prefix in the bronze bucket before generating, when this deployment owns the bucket. Without this flag, a non-empty bronze prefix is refused (exit 3) so existing datagen output is never overwritten silently. Never clears a bucket this deployment does not own. A multi-cycle run clears an owned prefix before cycle 0 without it. Refused without --generate or --generate-only. |
| `--allow-stale-bronze` |  | flag |  | On a batch run with --generate or more than one cycle, or with --generate-only: generate over objects already in the datagen prefix of a bronze bucket this deployment did not create. Rows may be over-counted; metrics.json records it (datagen.stale_bronze). |
| `--skip-maintenance` |  | flag |  | Skip pre-benchmark maintenance (compaction, snapshot expiry) |
| `--force-rebuild` |  | flag |  | Silver batch only: opt in to a full rebuild that would drop an existing populated silver table. Atomically bumps the deployment's silver rebuild epoch so downstream Delta idempotency keys move to a new namespace. |
| `--force-reset` |  | flag |  | Continuous c360 only: allow the run to drop existing bronze_raw, silver and gold tables, stream checkpoints and raw data before starting |
| `--deploy-only` |  | flag |  | Deploy infrastructure and exit (do not generate or run pipeline) |
| `--generate-only` |  | flag |  | Deploy + generate data and exit (do not run pipeline) |
| `--yes` | `-y` | flag |  | Skip all confirmation prompts |
| `--local` |  | flag |  | Run locally with podman/docker instead of Kubernetes |
| `--workdir` |  | path |  | Host directory for local mode state (default: ~/.lakebench/local/<name>) |
| `--repeat` |  | integer, 1 to 20 |  | Run the batch pipeline N times as one series over one corpus: repetition 1 as asked, then N-1 rebuilds from the same bronze |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `0` `run.pass`: `run` finished and its verdict passed
- `1` `run.verdict_failed`: `run` finished with a failing verdict
- `1` `run.datagen_timeout`: datagen did not finish in time; the record says "datagen timed out" in verdict.reasons
- `1` `run.namespace_gone`: the namespace was deleted, or deleted and deployed again, during a continuous `run`, or could not be read three times over a minute; the record names it in abort_reason
- `1` `repeat.no_verified_corpus`: `run --repeat` found no verified corpus to reuse after repetition 1
- `2` `run.args`: a `run` argument or combination is refused before any cluster call
- `3` `run.deps_mismatch`: the recorded dependency set does not check, or the server or a query engine pod runs another set than the deployment recorded
- `3` `run.bronze_nonempty`: datagen would write over a non-empty bronze prefix: without --regenerate, or with it on a bucket this deployment cannot prove it owns (a continuous run too, when objects land in the prefix after its reset)
- `3` `series.corpus_changed`: the bronze corpus changed during or between repetitions of `run --repeat`
- `4` `run.prereq_failed`: a `run` preflight check failed
- `4` `capacity.shortfall`: free cluster capacity is below the run's floor, or its largest pod fits no node
- `4` `capacity.unknown`: the run's capacity check could not read the nodes or pods (the check fails closed)
- `4` `run.deps_missing`: the deployment has no dependency server (deployed by 1.6, or never deployed)
- `4` `run.deps_stale`: the dependency set is not verified for this config: the deploy did not finish, the request changed since deploy, or the server has no Ready pod
- `5` `run.namespace_missing_no_yes`: `run` would create a missing namespace and was not given --yes
- `130` `run.interrupted`: `run` interrupted by SIGINT or SIGTERM; the record is sealed as interrupted and the run's unfinished jobs are stopped

**Refused arguments.** `run` checks every option before it makes any
cluster call, and exits 2 (usage) naming the first refused one:

- `--stage` that is not `bronze-verify`, `silver-build` or `gold-finalize`;
- `--stage` with a continuous run (flag or config);
- `--deploy-only` with `--generate-only`;
- `--deploy-only` with `--stage`, `--generate` or `--skip-generate`;
- `--skip-deploy` with `--deploy-only` or `--generate-only`;
- `--generate-only` with `--skip-generate`;
- `--local` with `--deploy-only`, `--generate-only`, `--force-rebuild` or `--skip-maintenance`;
- `--regenerate` without `--generate` or `--generate-only`;
- `--regenerate` with `--local`, or with a continuous run other than `--generate-only`;
- `--allow-stale-bronze` on a run that does not generate into bronze (only `--generate`, `--generate-only` or a multi-cycle batch run take it; not `--local`, `--deploy-only` or a continuous run other than `--generate-only`);
- `--skip-generate` with `--generate`;
- `--generate` on a multi-cycle batch run (`cycles` above 1);
- `--force-reset` on a batch run;
- `--force-rebuild` on a continuous run;
- `--duration` on a batch run;
- `--duration` below 60;
- `--timeout` below 1;
- `--repeat` below 1 or above 20;
- `--repeat` with a continuous run;
- `--repeat` with `cycles` above 1;
- `--repeat` with `--stage`, `--local`, `--deploy-only` or `--generate-only`.
<!-- END GENERATED: cli run -->

- `--timeout`, when omitted, is `max(3600, scale * 120)` seconds per job;
  the AML workload adds 900 s and never goes below its bronze-verify budget.
- `--skip-deploy` still runs the read-only prerequisite checks, cluster
  capacity included, and a failed one fails the run with exit 4.
- `--skip-generate` is refused with `--generate`.
- `--force-rebuild`: on Delta the silver table's own log has the last word;
  the rebuild writes under an epoch above every one the table has used, even
  if the counter reads lower.
- `--force-reset`: without it a continuous run over existing state refuses
  and lists what it would delete. Raw data alone from `lakebench generate` on
  a deployment with no tables or checkpoints is not refused: continuous runs
  generate their own data, so a separate `generate` before `run --continuous`
  is not needed.
- `--repeat N` takes 1 to 20; see below.

**Repeating a run.** `run --repeat N` runs the batch pipeline N times as one
series. Repetition 1 runs as the other options ask and may generate;
repetitions 2 to N never generate and rebuild silver and gold from the same
bronze (as `--force-rebuild`). The config is loaded once, so an edit during
the series changes nothing that runs. The datagen prefix in the bronze
bucket is listed before repetition 1 (when it does not generate), after it,
and before every later repetition, and each record's own pre-save listing
must match repetition 1's: a difference, even a same-size rewrite of one
object, stops the series with exit 3 and the changed repetition is not
counted. A later repetition inherits repetition 1's corpus identity only
under that check (`experiment.corpus.inherited_from`). A repetition that
fails its verdict does not stop the series; Ctrl-C does (exit 130). The
series stops after repetition 1 when its bronze-verify or silver-build did
not pass, or when its datagen Job is still running or cannot be read; and
before any repetition when one of the deployment's SparkApplications is
still running or they cannot be listed. Each record carries `series {id, index, size}`, and
`lakebench-output/series/<id>.json` lists the repetitions, which are members
of the series' corpus, which passed, and the corpus (with `stale_bronze` when
repetition 1's bronze was generated with `--allow-stale-bronze` over objects
already there, by its own `--generate` or an earlier `generate`: rows may be
over-counted in every repetition). The manifest is the
authority on membership: only repetitions that passed and are members
count. Exit: 0 when every repetition passed, 1 when any did not, 3 when bronze or
the corpus changed, 130 on an interrupt. A repetition that stops before
saving a record, and repetition 1 when it exits 2 to 5, stop the series
with that repetition's own code.

The refused arguments are listed with the flags above.

`--local` with a continuous run, and any workload, recipe and mode `run`
does not support, are refused just after these, also before any cluster
call. Benchmark settings `run` does not honour are refused when the config
loads.

The run command executes 7 phases:

1. **Prerequisites** -- check kubectl, helm, K8s cluster, S3, Spark Operator
2. **Infrastructure** -- verify deployed components are ready
3. **Generate** -- optional datagen (with `--generate`)
4. **Pipeline** -- bronze-verify, silver-build, gold-finalize
5. **Maintenance** -- pre-benchmark compaction + snapshot expiry (measures cost)
6. **Benchmark** -- query benchmark with pre/post compaction QpH comparison
7. **Results** -- scorecard with maintenance value metrics

In continuous mode (`--continuous`), `run` launches the three stream jobs
(bronze-ingest, silver-stream, gold-refresh) together, runs in-stream
benchmark rounds and maintenance during the measurement window, gates on
continuous output inside the window, then lets the corpus settle and
fingerprints the query set over the settled tables. See
[Running Pipelines](running-pipelines.md#continuous-mode). The run reads
its namespace every 30 s during the window and the settle wait, before and
after each benchmark round, before each maintenance and compaction round,
and before it stops its streams. When the namespace is gone (deleted, being
deleted, or deleted and deployed again), or three reads in a row fail, the
run stops at that read, exits 1 and saves its record with `abort_reason`
(the reason and the run second). A maintenance or compaction statement in
flight finishes first. The run does not try to stop streams that went with
the namespace, or that belong to a deployment that replaced it.

**Interrupting a run.** Ctrl-C (SIGINT) or SIGTERM stops `run` and exits
130. The run first deletes the SparkApplications and the datagen Job it
created and has not seen finish: a stage, or a datagen Job, that completed
is kept, so its logs stay readable. Each delete carries the uid of the
object this run created, so an object of the same name that another
invocation created since is left alone. The cleanup takes at most about
60 s. The record is then saved: its verdict is INTERRUPTED (FAILED when
something had already failed before the interrupt, never PASSED), and its
`interrupted` block names the signal, the stage, and the objects stopped,
left and skipped. After an interrupt the run does not measure bucket sizes
or read Prometheus; it still lists the datagen prefix once to record which
corpus it read (a corpus cut short is recorded as incomplete). A signal that arrives while the results are being
gathered at the end of a run does not stop the record being written; the
record is sealed the same way, at stage `results` (a run that had already
failed keeps its own exit code). One that arrives after the record is
saved changes nothing.

Press Ctrl-C a second time to cut the cleanup short: what it had not reached
is recorded as skipped (a quick double press can skip all of it). A third
press stops at once and may lose the record. For anything left or skipped
the run prints the `kubectl delete` that stops it. While the run holds the
cluster lease (the Spark Operator watch-list heal at the start), the
interrupt waits for that shared change to finish, as for any command (see
[Troubleshooting](troubleshooting.md)). SIGHUP (a closed terminal or a
dropped SSH session) is not handled and ends the run without a record or a
cleanup: run long jobs under `tmux` or `nohup`. Trino queries of an
interrupted benchmark or maintenance step are not cancelled. `report
--list` shows such a run as Interrupted; the HTML report shows it as
failed, with the interrupt as its reason.

A recipe without a query engine (`*-none`) skips the benchmark and exits 0
with no QpH. `run` refuses an unsupported workload x recipe x mode
combination before anything is deployed; `--local` refuses AML configs and
continuous mode.

### stop

Stop every job Lakebench started in the deployment.

<!-- BEGIN GENERATED: cli stop (scripts/gen_cli_reference.py) -->
```
lakebench stop [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--name` |  | text |  | The deployment name, for a config with no name: in a directory with several nameless configs, or a v1.6 directory (only .lakebench/state.json). Must equal the config's own name when it has one. |
| `--dry-run` |  | flag |  | List what would be stopped without deleting anything |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `1` `stop.api_error`: `stop` could not list or delete a job; it still tried every other deletion
<!-- END GENERATED: cli stop -->

Deletes every SparkApplication named `lakebench-*` in the deployment's
namespace that has not finished (the continuous streams and any batch stage
left running by a CLI that died) and the datagen Job `lakebench-datagen`,
with its pods, while it runs. A SparkApplication that has `COMPLETED` or
`FAILED`, and a datagen Job that has finished, are left in place and listed,
so the logs of a failed stage stay readable with `lakebench logs`. A job
already gone is reported as not running. When a deletion fails, `stop` still
tries every other one, prints one line per failure and exits 1. A missing
namespace means nothing to stop (exit 0). A cluster that cannot be reached, or
a refused read of the namespace, exits 4 before anything is deleted. A refused
list of the SparkApplications or read of the datagen Job is a failure like a
refused deletion: the other targets are still stopped and the exit is 1.

### benchmark

Run the query engine benchmark independently.

<!-- BEGIN GENERATED: cli benchmark (scripts/gen_cli_reference.py) -->
```
lakebench benchmark [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--mode` | `-m` | text |  | Benchmark mode: power, throughput, or composite (overrides config) |
| `--streams` | `-s` | integer |  | Number of concurrent query streams for throughput/composite (overrides config) |
| `--cold` |  | flag |  | Flush Iceberg metadata cache before each query (cold run) |
| `--iterations` | `-n` | integer, at least 1 |  | Timed runs per query, scored by the median (overrides architecture.benchmark.iterations, default 3) |
| `--class` | `-c` | text |  | Run only queries of a specific class (scan, filter_prune, aggregation, analytics, operational; AML also investigator) |
<!-- END GENERATED: cli benchmark -->

- `--class` takes `scan`, `filter_prune`, `aggregation`, `analytics`,
  `operational`, and for AML also `investigator`. A name that matches no
  query runs nothing.

Executes the workload's query set (8 queries for Customer 360, 12 for AML)
against the silver and gold layers and reports Queries per Hour (QpH), the
median of `--iterations` timed samples per query. Each successful query is
then run once more, untimed, to record its result fingerprint. Power mode runs queries sequentially. Throughput mode runs
N concurrent streams. Composite mode runs both and reports the geometric mean.

The result is saved as a record of its own under a new run id:
`record_kind: "benchmark"`, `parent_run_id` the deployment's latest run (a
copy of that run's record with the new benchmark as its QpH, scores and
query stage, and `provenance.benchmark` naming the code and time of the
benchmark; a continuous run's in-stream rounds and the run's maintenance
QpH pair are not copied). The run's
own record is never rewritten, and the perf gate never takes a benchmark
record as a run. Nothing is recorded when the deployment has no run record,
or when the query engine now runs another dependency set than that run
recorded.

### query

Execute SQL queries against the configured query engine.

<!-- BEGIN GENERATED: cli query (scripts/gen_cli_reference.py) -->
```
lakebench query [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--sql` | `-q` | text |  | SQL query to execute |
| `--example` | `-e` | text |  | Run a built-in example query (count, revenue, channels, engagement, funnel, clv) |
| `--sql-file` |  | path |  | Read SQL from file (use '-' for stdin) |
| `--interactive` | `-i` | flag |  | Start interactive SQL shell (REPL) |
| `--format` | `-o` | text | `table` | Output format: table (default), json, csv |
| `--show-query` |  | flag |  | Show the SQL query before executing |
| `--timeout` | `-t` | integer | `120` | Query timeout in seconds |
| `--json` |  | flag |  | Write one lb-cli/1 JSON document to stdout; human text goes to stderr |
<!-- END GENERATED: cli query -->

Specify exactly one of `--sql`, `--example`, `--sql-file`, or `--interactive`.
(`--file` / `-f` is the config file, as on other commands.) The result is
printed and journalled; no run record is written.

```bash
lakebench query --example count
lakebench query --sql "SELECT count(*) FROM lakehouse.gold.customer_executive_dashboard"
lakebench query --interactive
```

### status

Show deployment status of Lakebench components in the cluster.

<!-- BEGIN GENERATED: cli status (scripts/gen_cli_reference.py) -->
```
lakebench status [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (optional) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--namespace` | `-n` | text |  | Kubernetes namespace to check |
| `--local` |  | flag |  | Show local mode status instead of Kubernetes |
| `--workdir` |  | path |  | Host directory for local mode state (default: ~/.lakebench/local/<name>) |
| `--name` |  | text |  | The deployment name, for a config with no name: in a directory with several nameless configs, or a v1.6 directory (only .lakebench/state.json). Must equal the config's own name when it has one. |
| `--json` |  | flag |  | Write one lb-cli/1 JSON document to stdout; human text goes to stderr |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `0` `status.ok`: `status` finds every listed component ready
- `1` `status.drift`: `status` finds a component of the config not ready or not found (with only `--namespace`: one not ready, or none found)
- `1` `status.namespace_missing`: `status` finds no namespace
<!-- END GENERATED: cli status -->

Displays a table of the deployment's components (PostgreSQL, the configured
catalog and query engine) with their readiness and replica counts, and the
datagen job's progress while it runs. With only `--namespace` and no config,
it lists every component lakebench can deploy, plus the shared Prometheus and
Grafana.

Exit codes: 0 when every component of the config is ready; 1 when the
namespace does not exist, or a component is not ready, scaled to zero or not
found (drift). With only `--namespace`, a component that is absent is not
drift, but one that is not ready is, and so is a namespace with none of
them. 4 when the cluster is unreachable or a read is refused.

### clean

Delete data without destroying infrastructure.

<!-- BEGIN GENERATED: cli clean (scripts/gen_cli_reference.py) -->
```
lakebench clean TARGET [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `TARGET` | yes | What to clean: silver, gold |
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` |  | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--force` / `--yes` | `-y` | flag |  | Skip confirmation prompt |
| `--force-legacy` |  | flag |  | Clean a bucket that has no lakebench ownership tag (legacy). Caution: another team's data may live in an untagged bucket. Refuses always on foreign-tagged buckets regardless of this flag. |
| `--allow-unverified-cluster` |  | flag |  | Proceed when the kubeconfig cannot prove which cluster it points at (no CA data for the api-server fingerprint). Same meaning as on destroy. |
<!-- END GENERATED: cli clean -->

On a backend without bucket tagging, `clean` (like `destroy` and the
continuous reset) empties a bucket only when the namespace records creating
it or adopting it empty.

Before emptying a layer's bucket, `clean` unregisters that layer's tables
through the deployment's Trino or Spark Thrift pod (see
[Deployment](deployment.md)). A table it cannot unregister makes it exit 1,
and that bucket is left as it is unless the table's files are already gone.

`-f` is not accepted on `destroy` or `clean`: it exits 2 and names `--force` / `-y`,
because `-f` means `--file` everywhere else. `LAKEBENCH_LEGACY_SHORT_F=1` restores the old
meaning (force) with a warning for this release only.

Valid targets:

| Target | Action |
|---|---|
| `silver` | Empty the silver S3 bucket |
| `gold` | Empty the gold S3 bucket |

`bronze` and `data` are refused (exit 2) and name `lakebench run CONFIG
--generate --regenerate`, which clears the run's datagen prefix and
generates afresh (`data` also names `clean silver` and `clean gold`; on a
bucket the deployment did not create, `admin reclaim-bucket` comes first).
`metrics` and `journal`, and the old `--metrics-dir`/`-m`, are refused: run
records and journals are evidence, and the CLI does not delete them. A
refusal echoes no argument.

### destroy

Tear down the resources this deployment owns.

<!-- BEGIN GENERATED: cli destroy (scripts/gen_cli_reference.py) -->
```
lakebench destroy [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Path to configuration YAML file (default: ./lakebench.yaml) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` |  | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--force` / `--yes` | `-y` | flag |  | Skip confirmation prompt |
| `--local` |  | flag |  | Tear down the local stack instead of Kubernetes |
| `--workdir` |  | path |  | Host directory for local mode state (default: ~/.lakebench/local/<name>) |
| `--remove-data` |  | flag |  | Local mode: also delete generated data and the Ivy cache |
| `--allow-unverified-cluster` |  | flag |  | Bypass the api-server fingerprint match when it cannot be computed on one or both sides. Only use when you know the current kubectl context is correct (dev environment with a broken kubeconfig, etc). |
| `--force-legacy` |  | flag |  | Destroy without tag / annotation proof of ownership. Covers two cases: (1) legacy pre-ownership namespace or bucket that carries no lakebench identity annotation / ownership tag; (2) a bucket on an S3 backend that does not implement tagging AND does not match the deployment-name prefix. Caution: another workload's data may live there. Prefer `lakebench admin migrate-deployment <namespace>` first (case 1) or rename the bucket to start with the deployment name (case 2). Refuses always on foreign-tagged buckets or namespaces regardless of this flag. |
| `--namespace-timeout` |  | integer, at least 0 | `600` | Seconds to wait for the namespace to finish terminating after the delete is issued (PVC and pod finalizers can hold it for minutes). A namespace still terminating at the deadline is not reported as deleted and destroy exits 6. 0 skips the wait, so destroy exits 6 unless the namespace is already gone. |
| `--name` |  | text |  | The deployment name, for a config with no name: in a directory with several nameless configs, or a v1.6 directory (only .lakebench/state.json). Must equal the config's own name when it has one. |
| `--keep-buckets` |  | flag |  | Empty the S3 buckets but do not delete them. By default destroy deletes the emptied buckets this deployment created (listed in the namespace's created-buckets record) and provably owns (ownership tag, or name prefix on backends without tagging) when create_buckets is true. Without tagging, a bucket is emptied only if the namespace records creating it or adopting it empty. |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `3` `destroy.incarnation_mismatch`: `destroy` found the namespace is not the deployment incarnation it checked or was told to expect
- `3` `destroy.redeployed`: "Destroy NOT completed": the namespace now belongs to a newer deployment
- `3` `destroy.unverified_cluster`: "Destroy NOT completed": this cluster has no fingerprint, so buckets are kept
- `6` `destroy.namespace_terminating`: `destroy` finished its steps but the namespace is still terminating
<!-- END GENERATED: cli destroy -->

Removes everything in this order: ownership check, Spark jobs, orphaned
pods, datagen jobs, table removal from the catalog, S3 bucket contents
(then the buckets themselves, see below), observability, the query engine,
the catalog, PostgreSQL, RBAC and secrets, and the namespace. Destroy runs no
table maintenance: no Iceberg `expire_snapshots` or `remove_orphan_files` and
no Delta `VACUUM` before the drops.

Table removal never deletes files. On Trino it runs
`CALL <catalog>.system.unregister_table(...)`, because Trino's `DROP TABLE`
deletes every file an Iceberg table references (including datagen files
registered with `add_files`) and a managed Delta table's directory. On Spark
Thrift an Iceberg `DROP TABLE` (no `PURGE`) removes only the catalog entry. A
Spark Thrift `DROP TABLE` of a Delta table deletes its directory, so it runs
only when destroy is emptying every bucket of the deployment; otherwise the
tables are left registered and reported. Files are removed only by the bucket
step, from buckets destroy proves it owns. A later run refuses to create a
Delta table over a `_delta_log` that is not in the catalog (for example after
`--keep-buckets` with the namespace deleted): delete that table directory, or
use other buckets. The scratch StorageClass is shared
cluster-scoped infrastructure and is never deleted.

S3 buckets are emptied, then deleted only if lakebench created them: deploy
records each bucket it creates (a `lakebench.created` tag where the backend
supports tagging, and the `lakebench.deployment/created-buckets` namespace
annotation), and destroy deletes a bucket only when it is in that record and
its ownership checks out. Buckets deploy adopted, pre-provisioned buckets
(`create_buckets: false`), and buckets emptied under `--force-legacy` are
emptied but kept. If a recorded bucket cannot be emptied or deleted, the
namespace is kept as the ownership record so a re-run can finish.

Destroy waits for the namespace to be gone before reporting it deleted. If a
concurrent destroy of the same deployment finished first and a redeploy has
re-created the name, destroy stops and leaves the new deployment alone.

Exit codes: `0` everything removed; `1` a step failed (see the summary);
`2` the config did not load, including a nameless config with no `--name`
in a directory that cannot name its deployment; `3` a nameless config could
not prove the deployment is its own, or the namespace was redeployed since
that check (nothing deleted either way), or a redeploy was found partway
through (destroy stops there, and the steps before it may have removed
components); `5` the confirmation prompt was
declined (no side effects); `6` everything else succeeded but the namespace
was still terminating at `--namespace-timeout` (usually a PVC or pod
finalizer; check with `kubectl get ns <namespace>` before re-deploying
under the same name). The full table is in [Exit Codes](exit-codes.md).

### report

Read a saved benchmark run. The default action prints the summary and
points at the delivered `run-<id>/report.html` without modifying it.
Use `--render` to regenerate a fresh HTML report at
`lakebench-output/reports/report-<run_id>-<ts>.html` without touching the
delivered file.

<!-- BEGIN GENERATED: cli report (scripts/gen_cli_reference.py) -->
```
lakebench report [RUN|CONFIG] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `RUN\|CONFIG` | no | A run id, or a configuration YAML file: its deployment's latest run record, so a parallel deployment's newer run is not reported by mistake. Default: ./lakebench.yaml when it exists, otherwise the latest run of any deployment. |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--metrics` | `-m` | path | `lakebench-output/runs` | Directory containing run subdirectories |
| `--run` | `-r` | text |  | Specific run ID to report on (default: latest) |
| `--list` | `-l` | flag |  | List available runs instead of reporting on one |
| `--render` |  | flag |  | Regenerate HTML. Writes a fresh timestamped file at lakebench-output/reports/report-<run_id>-<ts>.html without touching the delivered run-<id>/report.html. |
| `--output` |  | path |  | Explicit output path for --render. Refuses to overwrite an existing file at this path unless --force is also given. |
| `--force` |  | flag |  | Allow --render to overwrite an existing file at --output. Requires both --render and --output. |
| `--summary` | `-s` | flag |  | Also print the key scores when rendering (default action already prints them). |
| `--format` | `-o` | text |  | Print the run's stage matrix instead of the summary: table, json or csv (json is the pipeline benchmark block) |
| `--json` |  | flag |  | Write one lb-cli/1 JSON document to stdout; human text goes to stderr |
<!-- END GENERATED: cli report -->

- `--format` does not combine with `--render` or `--list`.

The optional argument is a run id (a leading `run-` is dropped) or a config
file. A config scopes the lookup to that deployment's latest run, so on a
shared `lakebench-output` tree `report other.yaml` does not pick up another
deployment's newer run. With no argument and no `--run`, `report` uses
`./lakebench.yaml` when it exists and says so ("Showing the latest record of
deployment NAME"); without one it reads the latest run of any deployment. A
record written by `lakebench benchmark` (`record_kind: "benchmark"`) is
never the latest run; read it by its run id. `--list` shows each record's
kind.

### logs

Show logs from a component of the deployment.

<!-- BEGIN GENERATED: cli logs (scripts/gen_cli_reference.py) -->
```
lakebench logs [CONFIG] [COMPONENT] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG` | no | Path to configuration YAML file (default: ./lakebench.yaml) |
| `COMPONENT` | no | Component to read: datagen, a stage (bronze-verify, silver-build, gold-finalize, bronze-ingest, silver-stream, gold-refresh, score-financial, ...), spark-driver, trino, trino-worker, thrift, duckdb, hive, polaris, postgres |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` |  | path |  | Path to configuration YAML file (alternative to positional argument) |
| `--follow` | `-F` | flag |  | Follow log output (the newest matching pod) |
| `--lines` | `-n` | integer, at least 1 | `100` | Number of lines to show per pod |
| `--name` |  | text |  | The deployment name, for a config with no name: in a directory with several nameless configs, or a v1.6 directory (only .lakebench/state.json). Must equal the config's own name when it has one. |
| `--previous` |  | flag |  | Read the previous (crashed or restarted) container instead |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `1` `logs.no_pod`: `logs` found no pod for the component, or none with a log to read yet (a container still starting, no previous container for `--previous`)
<!-- END GENERATED: cli logs -->

- `--file` names the config; then give only `COMPONENT`.
- `--follow` follows the newest matching pod, like `tail -f` (`-f` is
  deprecated here).

Components: `datagen` (the datagen Job pods); each pipeline stage's Spark
driver (`bronze-verify`, `silver-build`, `gold-finalize`, `bronze-ingest`,
`silver-stream`, `gold-refresh`, `replay-financial`, `reproduce-financial`,
`score-financial`, `score-financial-reference`); `spark-driver` (every Spark
driver); `trino` (coordinator), `trino-worker`, `thrift`, `duckdb`, `hive`,
`polaris` and `postgres`.

`logs` reads through the Kubernetes API, not `kubectl`. Log text goes to
stdout unformatted; when several pods match, each pod's lines follow a
header on stderr. `lakebench logs COMPONENT` alone uses `./lakebench.yaml`,
and the 1.6 order `lakebench logs COMPONENT CONFIG_FILE` still works with a
one-line warning. A pod the API has no log for yet (a container still
starting, or no previous container with `--previous`) gets a warning with
the API's message and the other pods are still read. Exit codes: 1 when no
pod matches or none has a log to read, 2 for an unknown component, 4 for an
API error (a refused read, a server error) or an unreachable cluster.

### journal

View command and execution provenance journal.

<!-- BEGIN GENERATED: cli journal (scripts/gen_cli_reference.py) -->
```
lakebench journal [OPTIONS]
```

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--session` | `-s` | text |  | Show events for a specific session |
| `--last` | `-n` | integer | `10` | Show last N sessions |
| `--dir` |  | path | `lakebench-output/journal` | Journal directory |
<!-- END GENERATED: cli journal -->

Displays the history of all lakebench operations including deploys, data
generation, pipeline runs, and teardowns.

### reproduce

Record a reproduction package from a saved run, or verify a later run
against one. See [Reproduce](deep-dive/reproduce.md).

<!-- BEGIN GENERATED: cli reproduce (scripts/gen_cli_reference.py) -->
```
lakebench reproduce [PACKAGE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `PACKAGE` | no | Path to a reproduction package YAML (verify mode) |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--record` |  | text |  | Record mode: build a package from this saved run ID |
| `--write` |  | path |  | Record mode: write the package to this path |
| `--config-reference` |  | text |  | Record mode: store this relative config path in the package |
| `--config` | `-c` | path |  | Verify mode: config YAML to run instead of the package's config_reference |
| `--timeout` | `-t` | integer |  | Verify mode: per-job timeout in seconds |
| `--keep` |  | flag |  | Verify mode: do not destroy the deployment after the run. reproduce never destroys before the run: it refuses an existing namespace or bucket, and destroys only what it created. |
| `--allow-commit-drift` |  | flag |  | Verify mode: run even when HEAD differs from the recorded commit. Default is to refuse with exit 14 -- comparing numbers across code paths cannot claim to reproduce anything. |
| `--dry-run` |  | flag |  | Verify mode: parse the package and exit without running the pipeline |
| `--report` |  | path |  | Verify mode, registered looks only: the look's report; its sha256 is checked against the look record and nothing is run |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `2` `reproduce.report_required`: `reproduce` of a registered look's package without --report (a look is never rerun)
- `3` `reproduce.existing_namespace`: `reproduce` would reuse a namespace or bucket that already exists
- `3` `reproduce.nonce_changed`: the deployment `reproduce` created was replaced before its run or its destroy
- `3` `reproduce.held_out`: `reproduce` would regenerate a held-out corpus (its look has not run, its seed or the look record cannot be read, or the config names one)
- `14` `reproduce.drift`: `reproduce` ran and a metric drifted outside its tolerance band (correctness, or performance), or the run did not follow the package's protocol
- `14` `reproduce.commit_drift`: `reproduce` was asked to verify a package recorded at another commit, without --allow-commit-drift
- `14` `reproduce.verify_out_of_band`: `reproduce --report` of a registered look: the report does not match the look record, or the record holds no report sha256
<!-- END GENERATED: cli reproduce -->

Exit codes: `0` pass; `14` (requirement unmet) for performance or
correctness drift, commit drift without `--allow-commit-drift`, or a run that
did not follow the package (different samples, maintenance policy, experiment
or benchmark results); `2` for a package or config refused before running
(including `create_namespace: false` or `create_buckets: false`); `3` when
the namespace or a bucket already exists, or when another deploy replaced the
deployment reproduce created (then nothing is destroyed); `4` when the
namespace or buckets cannot be read; `1` when the pipeline could not run.
A package from a registered evaluation or robustness look is never rerun:
`--report` matching the look record exits `0`, a mismatch `14`, no
`--report` `2`; a held-out package whose look has not run is refused with
`3`. 1.6 used `1` for performance drift and
`2` for correctness drift.

### financial

AML (financial workload) operator actions. Each submits a Spark job against
the deployment in the config; `--wait/--no-wait` (default wait) controls
whether the command waits for it.

| Subcommand | Purpose |
|---|---|
| `financial score` | Compute rule recall from the datagen manifest and `gold.alerts` |
| `financial reference-score` | Run the reference detector and leakage gate over silver and the manifest |
| `financial replay` | Rerun one detection rule against a historical Iceberg snapshot (the output defaults to the gold alerts table with an `_replay` suffix) |
| `financial reproduce` | Reproduce one past alert via Iceberg time travel |

See [AML Scoring](aml-scoring.md).

<!-- BEGIN GENERATED: cli financial (scripts/gen_cli_reference.py) -->
#### `financial replay`

Rerun a detection rule against a historical Iceberg snapshot (W8).

```
lakebench financial replay CONFIG [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG` | yes | Lakebench config YAML |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--rule` |  | text |  | Rule id, e.g. W2_structuring |
| `--depth-months` |  | integer | `60` | Snapshot depth in months |
| `--threshold` |  | float |  | Rule-specific threshold override |
| `--output-alerts` |  | text |  | Fully-qualified output alerts table (catalog.namespace.table). Defaults to the config's gold alerts table with an _replay suffix, so a replay never overwrites the batch run's alerts. Multiple rules can share the table: replay does DELETE WHERE rule_id=X before appending, so each rule owns its rows. |
| `--wait` / `--no-wait` |  | flag | `--wait` | Wait for job completion |

#### `financial reproduce`

Reproduce a specific past alert via Iceberg time-travel (W10).

```
lakebench financial reproduce CONFIG [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG` | yes | Lakebench config YAML |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--alert-id` |  | text |  | Alert id to reproduce |
| `--wait` / `--no-wait` |  | flag | `--wait` | Wait for job completion |

#### `financial score`

Compute recall from datagen manifest and gold.alerts.

```
lakebench financial score CONFIG [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG` | yes | Lakebench config YAML |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--manifest` |  | text |  | S3 URI to datagen manifest.parquet |
| `--output` |  | text |  | S3 URI for recall.parquet output |
| `--wait` / `--no-wait` |  | flag | `--wait` | Wait for job completion |

#### `financial reference-score`

Run the reference detector + leakage gate over silver + manifest.

```
lakebench financial reference-score CONFIG [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG` | yes | Lakebench config YAML |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--manifest` |  | text |  | S3 URI to datagen manifest.parquet |
| `--output-prefix` |  | text |  | S3 URI prefix for leakage_report.parquet + reference_metrics.parquet |
| `--leakage-threshold` |  | float | `0.1` | Min baseline/typology ratio in a structuring band to pass the gate |
| `--wait` / `--no-wait` |  | flag | `--wait` | Wait for job completion |

Exit paths of every `financial` subcommand:

- `4` `financial.k8s_unreachable`: a `financial` command cannot reach the Kubernetes API
<!-- END GENERATED: cli financial -->

### admin

Cluster-admin operations on shared infrastructure. Mutating subcommands take
the cluster-wide `lakebench-cluster-lock` lease.

| Subcommand | Options | Purpose |
|---|---|---|
| `admin status` | `--operator-namespace` (default `spark-operator`) | Installed operators, lease state, lakebench namespaces, controller `/tmp` size and storage evictions |
| `admin doctor [CONFIG]` | `-f/--file` | Read-only report on the shared components (the prerequisite checks of [Prerequisites](prerequisites.md): scratch StorageClass, Spark Operator, Stackable, observability stack, OpenShift SCC role), the controller `/tmp` and the lease. Without a config, every component at its default name, with Stackable and the observability stack reported but not failing. Exits 1 when a check fails or cannot run |
| `admin install [CONFIG]` | `--component/-c C` (repeatable: `scratch-storage-class`, `spark-operator`, `stackable`, `observability`, or `all` for what the config uses; `all` needs a config), `--version C=V` (repeatable, exact chart version), `--allow-version-change`, `--dry-run`, `-y/--yes`, `--controller-tmp-size` (spark-operator fresh install, default 8Gi, floor 4Gi), `-f/--file` | Install the shared components that are missing, under the cluster lease, at `--version`, else the config's pin, else the Lakebench default. An installed component is never changed: with everything installed and ready it exits 0 and changes nothing. A config pin that differs from the installed version is kept with a warning; a stale shared Grafana dashboard is re-applied. Chart repos are refreshed before the lease is taken. Exit 1: a status that cannot be read, a release not `deployed`, an install that failed, or a component not ready. Exit 2: a malformed request, a version change without `--allow-version-change`, or `--controller-tmp-size` for an installed operator. Exit 3: refused (a version change, which Lakebench does not automate because helm leaves a chart's `crds/` at the installed version; a second Spark Operator or kube-prometheus-stack; leftover CRDs; a partial Stackable install that is ambiguous or at another version; a StorageClass whose parameters differ from the config's) |
| `admin repair-operator [CONFIG]` | `--dry-run`, `--controller-tmp-size` (default 8Gi), `-f/--file` | Under the cluster lease: roll a `pending-upgrade` or `pending-rollback` release whose pending revision started at least 10 minutes ago (API server clock) back to the newest deployed revision that watches no deleted or Terminating namespace, and only to a watch-all revision when the operator watches every namespace (exit 3 otherwise, and for `pending-install`); set the watch list with one upgrade to the namespaces the Helm values, the controller or the webhook list that are still Active (exit 3 when some of them watch every namespace and others do not); raise a controller `/tmp` smaller than the given size. No release exits 4. `--dry-run` reads without the lease |
| `admin migrate-deployment NAMESPACE [CONFIG]` | `--api-server-fingerprint`, `-f/--file` | Stamp identity annotations on a legacy pre-ownership namespace |
| `admin reclaim-bucket BUCKET [CONFIG]` | `--force-nonempty`, `-f/--file` | Rewrite a bucket's ownership tag to this deployment and this cluster, or on a backend without tagging its owner marker (`.lakebench/owner.json`); refused (exit 3) when the bucket holds objects unless `--force-nonempty`, exit 4 when the cluster fingerprint cannot be computed |
| `admin release-lock` | `--force` | Release an expired cluster lease; `--force` releases a live one (last resort) |

The Spark Operator runs spark-submit in its controller pod, which caches
jars under `/tmp`; the chart default of 1Gi is too small and gets the
controller evicted. See [Troubleshooting](troubleshooting.md).

<!-- BEGIN GENERATED: cli admin (scripts/gen_cli_reference.py) -->
#### `admin status`

Show installed operators, lease state, and lakebench-annotated namespaces.

```
lakebench admin status [OPTIONS]
```

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--operator-namespace` |  | text | `spark-operator` | Namespace of the Spark Operator. |

#### `admin doctor`

Read-only report on the shared cluster components and the lease.

```
lakebench admin doctor [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Config file to check against (optional). |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Alternative to positional argument. |

#### `admin release-lock`

Force-release a stale cluster lease.

```
lakebench admin release-lock [OPTIONS]
```

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--force` |  | flag |  | Release even a live lease. Use only when you are certain the prior holder crashed and cannot release itself. |

#### `admin install`

Install the shared cluster components a deployment needs.

```
lakebench admin install [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Config whose names and versions to use (optional). |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Alternative to positional argument. |
| `--component` | `-c` | text, repeatable |  | scratch-storage-class, spark-operator, stackable, observability, or all (what the config uses). Repeatable. |
| `--version` |  | text, repeatable |  | COMPONENT=VERSION, the exact chart version for a component that is not installed. Repeatable. An installed component keeps its version. |
| `--allow-version-change` |  | flag |  | For an installed component at another version: list what a change needs. Lakebench refuses the change itself (exit 3). |
| `--dry-run` |  | flag |  | Show what would be installed; change nothing. |
| `--yes` | `-y` | flag |  | Do not ask before installing. |
| `--controller-tmp-size` |  | text |  | spark-operator only, fresh install: sizeLimit of the controller's /tmp emptyDir (default 8Gi). An installed operator is resized with 'admin repair-operator --controller-tmp-size'. |

#### `admin migrate-deployment`

Stamp deployment-identity annotations on a legacy namespace.

```
lakebench admin migrate-deployment NAMESPACE [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `NAMESPACE` | yes | Namespace to migrate. |
| `CONFIG_FILE` | no | Config for this deployment (optional but recommended). |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Alternative to positional argument. |
| `--api-server-fingerprint` |  | text |  | Override the api-server fingerprint (advanced). |

#### `admin repair-operator`

Repair the shared Spark Operator: stale watch entries, a pending release, a small /tmp.

```
lakebench admin repair-operator [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG_FILE` | no | Config providing operator namespace/version (optional). |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Alternative to positional argument. |
| `--dry-run` |  | flag |  | Show the repairs without applying them. |
| `--controller-tmp-size` |  | text | `8Gi` | Raise the controller's /tmp emptyDir sizeLimit to this when it is smaller. |

#### `admin reclaim-bucket`

Rewrite bucket ownership tag to the caller's deployment.

```
lakebench admin reclaim-bucket BUCKET [CONFIG_FILE] [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `BUCKET` | yes | Bucket to reclaim. |
| `CONFIG_FILE` | no | Config providing S3 endpoint/credentials + target deployment name. |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--file` | `-f` | path |  | Alternative to positional argument. |
| `--force-nonempty` |  | flag |  | Rewrite the ownership tag even if the bucket has objects. Dangerous: another team's data may live there. Default is to refuse when objects are present. |
<!-- END GENERATED: cli admin -->

### version

Show version information.

<!-- BEGIN GENERATED: cli version (scripts/gen_cli_reference.py) -->
```
lakebench version
```

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `0` `version.ok`: `lakebench version` prints the version
<!-- END GENERATED: cli version -->

No flags. Prints the installed lakebench version.

### Renamed, refused and deprecated commands

Old names still parse so that a 1.6 command line gets an answer instead of
a usage error: an alias prints one line on stderr and runs the new command,
a refusal exits 2 and names what to run instead, without echoing anything
you passed. None of them appears in `--help` or in the reference above. The
list is `lakebench.cli._aliases`.

<!-- BEGIN GENERATED: cli aliases (scripts/gen_cli_reference.py) -->
| Old | What happens | Use instead |
|---|---|---|
| `results` | alias: one line on stderr, then runs the new command; removed in v1.8 | `report --format table` |
| `admin install-spark-operator` | alias: one line on stderr, then runs the new command; removed in v1.8 | `admin install --component spark-operator` |
| `admin install-scratch-storage-class` | alias: one line on stderr, then runs the new command; removed in v1.8 | `admin install --component scratch-storage-class` |
| `info` | hidden and deprecated since 1.3; still runs | `config show` |
| `recommend` | hidden and deprecated since 1.3; still runs | `config recommend` |
| `init --interactive` | accepted: the init wizard is removed; init writes a default config (see init --help) | nothing (drop it) |
| `init -i` | accepted: the init wizard is removed; init writes a default config (see init --help) | nothing (drop it) |
| `init --advanced` | accepted: the init wizard is removed; init writes a default config (see init --help) | nothing (drop it) |
| `run --sustained` | accepted: a deprecated alias; prints a warning | `--continuous` |
| `recommend --extended` | accepted: a deprecated alias; prints a warning | `--slow-datagen` |
| `recommend -e` | accepted: a deprecated alias; prints a warning | `--slow-datagen` |
| `config upgrade` | refused (exit 2): it rewrote configs lossily and wrote secrets in plaintext | lakebench init --from OLD.yaml -o NEW.yaml |
| `clean bronze` | refused (exit 2): a run regenerates its own corpus, so the corpus a record names is the one it read | lakebench run CONFIG --generate --regenerate; on a bucket this deployment did not create, `lakebench admin reclaim-bucket` first (an owner action) |
| `clean data` | refused (exit 2): a run regenerates its own corpus, so the corpus a record names is the one it read | lakebench clean silver CONFIG and lakebench clean gold CONFIG, then lakebench run CONFIG --generate --regenerate; on a bucket this deployment did not create, `lakebench admin reclaim-bucket` first (an owner action) |
| `clean metrics` | refused (exit 2): run records and journals are evidence, and the CLI does not delete them | nothing |
| `clean journal` | refused (exit 2): run records and journals are evidence, and the CLI does not delete them | nothing |
| `compare --keep`, `compare --generate`, `compare --local`, `compare --skip-benchmark`, `compare --timeout`, `compare --scale`, `compare --yes`, `compare -y` | refused (exit 2): compare reads stored records and no longer runs configs | lakebench run A.yaml and lakebench run B.yaml (add --repeat 3), then lakebench compare A.yaml B.yaml |
| `init --access-key`, `init --secret-key` | refused (exit 2): init writes a reference to the variable, never the key | export LAKEBENCH_S3_ACCESS_KEY and LAKEBENCH_S3_SECRET_KEY (or the names --credentials-env PREFIX gives) |
| `clean --metrics-dir`, `clean -m` | refused (exit 2): run records and journals are evidence, and the CLI does not delete them | nothing |
<!-- END GENERATED: cli aliases -->

## Common Patterns

### Typical Workflow

```bash
lakebench init                        # create config
lakebench plan                        # what it needs: sizing, prerequisites, egress
lakebench config validate             # check connectivity
lakebench deploy --yes                # deploy infrastructure
lakebench generate --timeout 14400    # generate data (large scales need hours)
lakebench run --timeout 7200          # run pipeline + benchmark, delivers report.html
lakebench report                      # print the summary of the delivered report
lakebench destroy --force             # tear down everything
```

### Re-running the Pipeline

```bash
lakebench run --generate --regenerate # clear the datagen prefix, regenerate, re-run
```

### Running a Single Stage

```bash
lakebench run --stage silver-build --timeout 3600
```

### Continuous Pipeline

```bash
lakebench run --continuous --duration 3600
lakebench stop                        # stop the continuous jobs manually
```
