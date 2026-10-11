# CLI Reference

Reference: every `lakebench` command, with its flags, exit paths and behaviour.

```
lakebench COMMAND [ARGUMENTS] [OPTIONS]
```

- The CLI is built with Typer and Rich.
- Each command's usage line, arguments, options and named exit paths are
  generated from the CLI by `scripts/gen_cli_reference.py`, between the
  `BEGIN GENERATED` and `END GENERATED` markers.
  `scripts/gen_docs.py --check` (run by `make release-check`) fails when
  they drift. Change a flag's description in its help text in the code, and
  the prose here outside the blocks.
- The config file argument is optional. Without it, the CLI reads
  `./lakebench.yaml`.
- Most commands also take the config as `--file` / `-f`. On `destroy`,
  `clean` and `logs` only the long form `--file` works (see
  [clean](#clean)).
- `--yes` / `-y` skips a confirmation prompt. On `destroy` and `clean`,
  `--force` is the same flag.
- Exit codes are in [Exit Codes](exit-codes.md). What `deploy`, `status`,
  `destroy` and `clean` do step by step is in [Deployment](deployment.md).

## Machine-readable output (`--json`)

`plan`, `status`, `report`, `config recipes` and `query` take `--json`. The
command then writes exactly one JSON document to stdout and every human
line to stderr:

```json
{"schema": "lb-cli/1", "command": "status", "exit_code": 1,
 "data": {"namespace": "...", "verdict": "drift", "components": [...]},
 "errors": [{"code": 1, "path": null, "what": "Drift: ...", "why": null,
             "next": null, "where": null}]}
```

- `exit_code` is always the process's exit code, also for an unknown option
  or a bad value. `--help` prints help only.
- `data` is the command's result. It is kept on a verdict exit such as
  `status` drift, and is `null` when the command failed.
- `errors` holds each error the command reported, with its exit-code `path`
  when it has one (see [Exit Codes](exit-codes.md)).
- Each command's `data` shape is a TypedDict in `lakebench/cli/_json.py`.
  `lb-cli/1` may gain keys; it never loses or retypes one.
- `query --json` names the engine and keeps its rows as it prints them.
  Trino's CSV has no header (`columns` null), Spark Thrift's tsv2 has one,
  and DuckDB returns up to 100 rows as Python reprs. `count` is the rows the
  engine returned.
- `report --json` gives the scores as stored and the verdict three ways:
  `verdict_stored` as the record holds it, `verdict_recomputed` from the
  record's own fields, and `verdict`, the stricter of the two.
  `report --list --json` rows carry the same three.
- `--json` does not combine with `--format` on `report` or `query`, nor
  with `status --local` or `query --interactive`.
- `plan --json` makes no cluster call.

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
| `--scale` | `-s` | float |  | Scale factor: 1 is about 10 GB of bronze for customer360, 8.5 GB for financial, 9.4 GB per unit from scale 10 (default 1; 0.1 with --local) |
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

- `--overwrite` keeps the file's name. It refuses (exit 3) a change of
  namespace, buckets, endpoint or recipe under that name. `--force` is its
  old spelling (`-f` is deprecated here).
- `--credentials-env PREFIX`: the file references `${PREFIX_ACCESS_KEY}` and
  `${PREFIX_SECRET_KEY}`.
- `--from OLD`: see
  [Converting a 1.6 config](../UPGRADING-1.7.md#converting-a-16-config-with-init---from).

What `init` writes:

- 12 lines of settings: the name, the recipe once, the workload and scale,
  the endpoint, and the two S3 credentials as `${VAR}` references.
- The components the recipe sets, as a commented block. Uncommented, they
  must agree with the recipe or the config is refused at load.
- No plaintext secret and no Polaris client secret: deploy creates one for
  a new Polaris.

`init` prints what it chose on stderr, with or without a terminal. It
refuses (exit 2, nothing written) a combination that would not load, such
as `--workload financial` with a Delta recipe.

The wizard is removed. `--interactive`, `-i` and `--advanced` print one line
saying so and write the default config. `--access-key` and `--secret-key`
are refused (exit 2); export the variables instead.

```bash
# Default: Polaris, customer360, scale 1
lakebench init
export LAKEBENCH_S3_ACCESS_KEY=... LAKEBENCH_S3_SECRET_KEY=...

# Another recipe and endpoint, credentials from MY_LAB_ACCESS_KEY / MY_LAB_SECRET_KEY
lakebench init -r hive-iceberg-spark-trino --endpoint http://my-s3:80 --credentials-env MY_LAB
```

### config

Configuration management subcommands.

```
lakebench config show CONFIG_FILE       # show resolved config with source annotations
lakebench config validate CONFIG_FILE   # validate config + test connectivity
lakebench config storage CONFIG_FILE    # check the S3 backend supports what lakebench needs
lakebench config recommend CONFIG_FILE  # largest scale the cluster holds, sized from this config
lakebench config recipes [NAME]         # list recipes, what each one trades off, and support states
```

- `config validate` and `validate` load the config as `deploy` does. They
  fail on a config with no `name:` or with a removed key.
- `config show` prints the config's support state and its peak requested
  resources.
- `config recipes` lists, per recipe, its support state per workload and
  mode (Customer 360 and AML, batch and continuous: supported, unverified or
  unsupported) and whether it runs in local mode. `config recipes NAME`
  gives the basis for each state. See
  [Compatibility Matrix](compatibility-matrix.md#support-states).
- `--local`: `config validate` validates for local (podman/docker) mode
  instead of Kubernetes; `config recipes` lists only recipes that run in
  local mode.

#### config storage

Runs graded conformance checks against the configured S3 endpoint and
reports what the backend does. It is diagnostic only and never gates
`deploy` or `run`, so a store Lakebench has not seen is checked rather than
refused.

- **Required** checks: connectivity, bucket enumeration, object operations,
  multipart abort. A failure means Lakebench cannot run against the store.
- **Advisory** checks: sigv4 region strictness. These are recorded because
  they change how Lakebench configures Spark, not because they are defects.
- `--no-full`, for an account that cannot create buckets, reports the write
  and multipart checks as skipped, not failed.
- Exit 0: no required check failed. Exit 1: a required check failed. Exit
  2: the config did not load or names no S3 endpoint.

Validated backends, what each check covers and what it does not:
[Storage Backends](storage-backends.md).

#### config recommend

`config recommend CONFIG` sizes the config with `lakebench.config.sizing`,
the source the `run` capacity preflight also uses.

- Each scale is decided by the check the preflight makes (batch: a
  `run --generate`).
- Continuous mode prints two answers: a plain `run`, whose datagen Job is
  counted beside the streams, and a corpus generated before the streams
  start (`generate`, then `run --skip-generate` within an hour).
- A scale above the workload's largest measured scale (300) is labelled
  unverified.

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

Runs the same checks as `lakebench config validate`: the config, S3 and
Kubernetes. The flags differ: `validate` takes `-f/--file` and
`-v/--verbose`; `config validate` takes `--local`.

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

Checks: YAML syntax, required fields, S3 endpoint reachability, S3
credential validity, Kubernetes context access, namespace status, platform
security (SCC on OpenShift), storage classes, Spark Operator status, and the
configured scale.

- Executor sizing is not graded: it comes from the job profiles.
- Before the first deploy, a namespace the Spark Operator does not watch yet
  is reported as advisory: `deploy` adds it to the watch list under the
  cluster lock.

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

For each config `plan` prints:

- the components and recipe, with their support state;
- the minimum cluster, from the same sizing function the `run` capacity
  preflight and the README tables use, with the scratch request ("not
  requested (scratch disabled)" when off);
- the cluster prerequisites from the registry `deploy` checks
  ([Prerequisites](prerequisites.md)), and the run's free capacity check;
- where the Polaris client secret comes from (a `${VAR}` reference is
  named; a value is never printed);
- the hosts outside the cluster the deployment contacts:
  - the hosts the dependency server resolves jars, wheels and DuckDB
    extensions from at deploy. This is the `egress-hosts` prerequisite's
    list: Maven Central and its Google mirror, PyPI where used, or the
    `platform.deps` mirrors;
  - the observability chart when enabled;
  - the image registries.

With several configs it then names the experiment-identity and
execution-condition differences between each one and the first. Configs on
different cluster contexts are planned one at a time; a second context in
one process is refused (exit 3).

Online:

- A failing prerequisite, a scratch StorageClass, Spark Operator or
  Stackable that cannot be checked, or too little free capacity: exit 4.
- A missing shared component prints
  `Next: (cluster admin) lakebench admin install --component <component>`.
- An unreachable cluster: exit 4, with "use --offline".

Offline (`--offline`, `--cores/--memory` or `--json`):

- No cluster call. Prerequisites read "not checked (offline)".
- `--cores/--memory` checks the aggregate only: the node shape is unknown,
  so the largest pod is not checked. Exit 4 when the config does not fit.

A config that does not load, or that `deploy` would refuse at load (for
example a name too long for the names derived from it), is exit 2.

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

- `3` `deploy.existing_namespace`: `deploy --require-new` found the namespace or a bucket already exists; nothing that existed was changed
- `3` `deploy.state_copied`: `deploy` found a state written for another directory or host (a copied directory)
- `3` `deploy.identity_foreign`: the namespace or a bucket is owned by another deployment, or has no lakebench ownership proof (`deploy`, `destroy`, `clean`)
- `4` `deploy.state_unrecordable`: `deploy` could not read the namespace or write the nonce to the directory's state
<!-- END GENERATED: cli deploy -->

The steps in order, the capacity check deploy runs first, the `--timeout`
rules and the fixes for dependency server failures are in
[Deployment](deployment.md#deploying-infrastructure).

- Deploy never installs a shared component (the Spark Operator, the
  Stackable operators, the scratch StorageClass, the observability stack).
  A missing one fails its step and names its
  `lakebench admin install --component` command.
- The Spark Operator step always runs: it checks the operator is ready and
  adds the namespace to its watch list.
- `--local` runs the same pipeline in podman or docker containers on this
  machine, to test a recipe without a cluster.

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
| `--registered-corpus` |  | flag |  | Generate the registered evaluation or robustness AML corpus (the config declares the role and its seed). Needs --yes; refuses --allow-stale-bronze. The attempt is recorded in ~/.lakebench/aml_corpora.jsonl (LB_AML_CORPORA_LEDGER) before the first cluster call. Without this flag a config that names a protected corpus is refused (exit 2). |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `2` `generate.multi_cycle`: `generate` with a multi-cycle config (`cycles` above 1): `run` generates each cycle
<!-- END GENERATED: cli generate -->

- `--regenerate` clears the datagen prefix, not the whole bucket, and only
  on a bucket this deployment owns (its stamp, or a bucket it created). On
  any other bucket it is refused (exit 3); `lakebench admin reclaim-bucket`
  claims one.
- `--allow-stale-bronze`: `run` records the over-count in `metrics.json`
  (`datagen.stale_bronze`), and the report shows "bronze held N objects
  before generate".
- `--registered-corpus` needs `corpora.registered_looks_open` true in the
  pre-registration and `images.datagen` pinned by digest
  (`repo@sha256:...`).
  - It is refused (exit 2) for a config that names no protected corpus,
    with `--allow-stale-bronze`, and for a seed that already has a look.
    A look is found in the look ledger or in any commit of the look record.
    A shallow clone, a tree that is not a git checkout, or a pip install
    cannot show that history, so it is refused there too.
  - The attempt is appended to `~/.lakebench/aml_corpora.jsonl`
    (`LB_AML_CORPORA_LEDGER`, one ledger per host): before the first
    cluster call, `submitting` before the datagen Job is created, then
    `generated` or `failed`. The seed is recorded only by its salted hash.
  - The `generated` entry records the corpus fingerprint (every data file's
    path and size, each manifest file's sha256).
    `scripts/aml_gate.py --registered` requires the scored corpus to match
    it.
  - A development config whose bronze prefix is in that ledger is refused
    by every data command.

Behaviour:

- Runs parallel Kubernetes Jobs that write Parquet files. Scale 100 is
  about 1 TB.
- A multi-cycle config (`cycles` above 1) is refused (exit 2) before any
  cluster call: `lakebench run` generates each cycle before its stages.
- A finished generate records the corpus series marker (see
  [Reusing a corpus](#reusing-a-corpus)).
- Use `--timeout` for large scales that may take hours.
- Without `--yes`, it prompts before submitting jobs.
- The Rust generator has no checkpoint-resume. Re-run an interrupted
  generate from the start.

Exit codes:

- `0` success.
- `1` a generic failure, or datagen exceeded its wait budget (`--timeout`).
  On a timeout the datagen Job, and any leftover streaming SparkApplication
  reading its output, are stopped before exit. Only the initial datagen
  pass is guarded this way; each cycle's datagen inside a multi-cycle run
  reports its own timeout.
- `2` a protected AML corpus without `--registered-corpus`, or the flag on a
  config that names none, without `--yes`, or on a seed with a look.
- `3` (refused):
  - the bronze datagen prefix is non-empty and neither `--regenerate` (on a
    bucket this deployment owns) nor `--allow-stale-bronze` (on one it does
    not) applies;
  - `--regenerate` on a bucket it does not own;
  - datagen writes at the bucket root (no prefix).
- `4` bronze or its ownership cannot be checked.

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
| `--skip-generate` |  | flag |  | Batch: reuse the corpus already in bronze. Refused (exit 3) when its series marker says the generate did not finish or was made for another cycle count, window or generation than the config's; a multi-cycle run needs a marker |
| `--regenerate` |  | flag |  | Clears the datagen prefix in a bronze bucket this deployment created, before generating (before cycle 0 of a multi-cycle run); refused on any other bucket. Without it, a non-empty datagen prefix is refused (exit 3), so existing datagen output is never overwritten silently. Takes --generate on a single-cycle run, nothing more on a multi-cycle run, and is refused when the run does not generate. |
| `--allow-stale-bronze` |  | flag |  | On a batch run with --generate, a multi-cycle batch run without --skip-generate, or --generate-only: generate over objects already in the datagen prefix of a bronze bucket this deployment did not create. Rows may be over-counted; metrics.json records it (datagen.stale_bronze). |
| `--skip-maintenance` |  | flag |  | Skip maintenance (compaction, snapshot expiry): before the benchmark in batch, and the in-stream maintenance rounds in continuous |
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
- `3` `run.series_mismatch`: a `run` that reuses the corpus (`--skip-generate`, or one cycle without `--generate`) finds its series marker unfinished, unreadable, or written for another cycle count, window or generation than the config's
- `2` `run.protected_corpus`: a protected AML corpus (evaluation or robustness role or seed, or a run record from one) reached a command that reads or scores data; `generate --registered-corpus` got a config naming none; or bronze-verify, or its check before `run --stage`, refused the manifest (held-out or spent seed, no corpus seed, missing, unreadable)
- `3` `run.deps_mismatch`: the recorded dependency set does not check, or the server or a query engine pod runs another set than the deployment recorded
- `3` `run.bronze_nonempty`: datagen would write over a non-empty bronze prefix: without --regenerate, or with it on a bucket this deployment cannot prove it owns (a continuous run too, when objects land in the prefix after its reset)
- `3` `series.corpus_changed`: the bronze corpus changed during or between repetitions of `run --repeat`
- `4` `run.prereq_failed`: a `run` preflight check failed
- `4` `capacity.shortfall`: free cluster capacity is below the run's floor, or its largest pod fits no node
- `4` `capacity.unknown`: the run's capacity check could not read the nodes or pods (the check fails closed)
- `4` `run.no_corpus`: a `run` that reuses the corpus (`--skip-generate`, or one cycle without `--generate`) finds no corpus in bronze: nothing was generated yet
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
- `--regenerate` on a run that does not generate (only `--generate`, `--generate-only` or a multi-cycle batch run without `--skip-generate` take it);
- `--regenerate` with `--local`, or with a continuous run other than `--generate-only`;
- `--allow-stale-bronze` on a run that does not generate into bronze (only `--generate`, `--generate-only` or a multi-cycle batch run without `--skip-generate` take it; not `--local`, `--deploy-only` or a continuous run other than `--generate-only`);
- `--skip-generate` with `--generate`;
- `--generate` on a multi-cycle batch run (`cycles` above 1);
- `--generate-only` on a multi-cycle batch run (`cycles` above 1);
- `--skip-generate` on a multi-cycle financial (AML) batch run;
- `--force-reset` on a batch run;
- `--force-rebuild` on a continuous run;
- `--duration` on a batch run;
- `--duration` below 60;
- `--timeout` below 1;
- `--repeat` below 1 or above 20;
- `--repeat` with a continuous run;
- `--repeat` with `cycles` above 1;
- `--repeat` with `--stage`, `--local`, `--deploy-only` or `--generate-only`;
- `benchmark.investigator_sessions` outside an AML continuous run with TM operations on trino or spark-thrift.
<!-- END GENERATED: cli run -->

- `--timeout`, when omitted, is `max(3600, scale * 120)` seconds per job.
  The AML workload adds 900 s and never goes below its bronze-verify budget.
- `--skip-deploy` still runs the read-only prerequisite checks, cluster
  capacity included. A failed one fails the run with exit 4.
- `--skip-generate`: the corpus series marker must describe a finished
  generate of this config (see [Reusing a corpus](#reusing-a-corpus)).
- `--allow-stale-bronze`: every later run that reuses that corpus records
  the note too (`datagen.stale_bronze`).
- `--force-rebuild`: on Delta the silver table's own log has the last word.
  The rebuild writes under an epoch above every one the table has used,
  even if the counter reads lower.
- `--force-reset`: without it, a continuous run over existing state refuses
  and lists what it would delete. Raw data alone from `lakebench generate`,
  on a deployment with no tables or checkpoints, is not refused. Continuous
  runs generate their own data, so a separate `generate` before
  `run --continuous` is not needed.

More refusals, also before any cluster call, after the refused arguments
above:

- `--local` with a continuous run;
- `--local` with an AML config;
- any workload, recipe and mode `run` does not support (checked before
  anything is deployed).

Benchmark settings `run` does not honour are refused when the config loads.

A config naming a protected AML corpus (the evaluation or robustness role
or seed) is refused before any cluster call (exit 2,
`run.protected_corpus`). For the financial workload, bronze-verify first
reads every row of the corpus manifest and stops (exit 2) on:

- a corpus from a held-out or spent seed;
- a manifest no corpus seed can be recovered from;
- a batch corpus with no manifest.

`--stage silver-build` or `gold-finalize` runs that check alone first. A
check that could not run (a storage error) exits 1.

A recipe without a query engine (`*-none`) skips the benchmark and exits 0
with no QpH.

#### Batch phases

1. **Prerequisites** -- check kubectl, helm, the cluster, S3, Spark Operator
2. **Infrastructure** -- verify deployed components are ready
3. **Generate** -- optional datagen (with `--generate`)
4. **Pipeline** -- bronze-verify, silver-build, gold-finalize
5. **Maintenance** -- pre-benchmark compaction and snapshot expiry (measures cost)
6. **Benchmark** -- query benchmark with pre/post compaction QpH comparison
7. **Results** -- scorecard with maintenance value metrics

#### Continuous mode

With `--continuous`, `run`:

1. launches the three stream jobs (bronze-ingest, silver-stream,
   gold-refresh) together;
2. runs in-stream benchmark rounds and maintenance during the measurement
   window, and gates on continuous output inside the window;
3. lets the corpus settle, then fingerprints the query set over the
   settled tables.

See [Running Pipelines](running-pipelines.md#continuous-mode).

The run reads its namespace every 30 s during the window and the settle
wait, before and after each benchmark round, before each maintenance and
compaction round, and before it stops its streams.

- The run stops at that read when the namespace is gone (deleted, being
  deleted, or deleted and deployed again), or three reads in a row fail. It
  exits 1 and saves its record with `abort_reason` (the reason and the run
  second).
- A maintenance or compaction statement in flight finishes first.
- The run does not try to stop streams that went with the namespace, or
  that belong to a deployment that replaced it.

#### Repeating a run

`run --repeat N` runs the batch pipeline N times as one series.

- Repetition 1 runs as the other options ask and may generate. Repetitions
  2 to N never generate; they rebuild silver and gold from the same bronze
  (as `--force-rebuild`).
- The config is loaded once, so an edit during the series changes nothing
  that runs.
- The datagen prefix in the bronze bucket is listed before repetition 1
  (when it does not generate), after it, and before every later
  repetition. Each record's own pre-save listing must match repetition 1's.
  A difference, even a same-size rewrite of one object, stops the series
  with exit 3, and the changed repetition is not counted.
- A later repetition inherits repetition 1's corpus identity only under
  that check (`experiment.corpus.inherited_from`).
- A repetition that fails its verdict does not stop the series. Ctrl-C does
  (exit 130).
- The series stops after repetition 1 when its bronze-verify or
  silver-build did not pass, or when its datagen Job is still running or
  cannot be read. It stops before any repetition when one of the
  deployment's SparkApplications is still running or they cannot be listed.
- Each record carries `series {id, index, size}`.
  `lakebench-output/series/<id>.json` lists the repetitions, which are
  members of the series' corpus, which passed, and the corpus.
- The corpus carries `stale_bronze` when repetition 1's bronze was
  generated with `--allow-stale-bronze` over objects already there, by its
  own `--generate` or an earlier `generate`. Rows may then be over-counted
  in every repetition.
- The manifest is the authority on membership: only repetitions that passed
  and are members count.
- Exit: 0 when every repetition passed, 1 when any did not, 3 when bronze or
  the corpus changed, 130 on an interrupt. A repetition that stops before
  saving a record, and repetition 1 when it exits 2 to 5, stop the series
  with that repetition's own code.

#### Reusing a corpus

Every generate (`generate`, `run --generate`, each cycle of a
multi-cycle run, and a continuous run's datagen) writes a corpus series
marker, `<datagen prefix>/_corpus/series.json`, in the bronze bucket. What
it holds, how a clear marks the prefix, and why a prefix holding only the
marker counts as empty are in
[Data Generation](data-generation.md#the-corpus-series-marker). Its
generation parameters are the seed, scale, customer id space, file size,
dirty ratio and image.

A batch run that reuses the corpus (`--skip-generate`, or a single-cycle
run without `--generate`) reads the marker after the prerequisite checks
and before anything is deployed or submitted.

- Refused (exit 3, `run.series_mismatch`) when the marker says the generate
  did not finish (an interrupted generate or clear, or a continuous run's
  corpus).
- Refused (exit 3) when it names another cycle count, window or generation
  than the config's: the seed, scale, customer id space, file size, target
  size per cycle, dirty ratio, image, window bounds and, for AML, the
  robustness perturbation. The image digest is not compared.
- Exit 4 when bronze cannot be read.

A clear of the datagen prefix that stops part way never leaves part of a
corpus unmarked. A destroy that keeps the bronze bucket and stops while
emptying it is not covered.

- A single-cycle run over a corpus with no marker (from 1.6 or an older
  `generate`) proceeds and records `cycle_series.marker: "absent"`, unless
  the corpus holds files of cycles after the first (`part-cNNN-*`, a
  multi-cycle corpus).
- A multi-cycle `--skip-generate` needs a marker.
- A multi-cycle run without `--skip-generate` generates every cycle. A
  non-empty datagen prefix, a leftover marker included, is refused (exit 3)
  unless `--regenerate`.
- A multi-cycle financial (AML) run cannot reuse its corpus: its stages
  read the whole bronze prefix every cycle.
- The run records `cycle_series {marker, reused, cycles_total, windows}`
  (`marker`: `read` or `absent` for a reuse; `begun`, `written`,
  `unwritten` or `conflict` for a generate), and each cycle records
  `datagen_skipped`.
- A generate whose marker another run has replaced since it began stops
  with exit 3. Two runs in one namespace are not supported.

#### Interrupting a run

Ctrl-C (SIGINT) or SIGTERM stops `run` with exit 130.

- The run first deletes the SparkApplications and the datagen Job it
  created and has not seen finish. A stage or datagen Job that completed is
  kept, so its logs stay readable.
- Each delete carries the uid of the object this run created, so an object
  of the same name that another invocation created since is left alone.
- The cleanup takes at most about 60 s.
- The record is then saved. Its verdict is INTERRUPTED (FAILED when
  something had already failed before the interrupt, never PASSED). Its
  `interrupted` block names the signal, the stage, and the objects stopped,
  left and skipped.
- After an interrupt the run does not measure bucket sizes or read
  Prometheus. It still lists the datagen prefix once to record which corpus
  it read; a corpus cut short is recorded as incomplete.
- A signal while the results are gathered at the end of a run does not stop
  the record being written. The record is sealed the same way, at stage
  `results`; a run that had already failed keeps its own exit code. A
  signal after the record is saved changes nothing.
- A second Ctrl-C cuts the cleanup short: what it had not reached is
  recorded as skipped (a quick double press can skip all of it). A third
  press stops at once and may lose the record.
- For anything left or skipped, the run prints the `kubectl delete` that
  stops it.
- While the run holds the cluster lease (the Spark Operator watch-list heal
  at the start), the interrupt waits for that shared change to finish, as
  for any command (see [Troubleshooting](troubleshooting.md)).
- SIGHUP (a closed terminal or a dropped SSH session) is not handled. It
  ends the run without a record or a cleanup: run long jobs under `tmux` or
  `nohup`.
- Trino queries of an interrupted benchmark or maintenance step are not
  cancelled.
- `report --list` shows such a run as Interrupted. The HTML report shows it
  as failed, with the interrupt as its reason.

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

`stop` deletes, in the deployment's namespace:

- every SparkApplication named `lakebench-*` that has not finished (the
  continuous streams, and any batch stage left running by a CLI that died);
- the datagen Job `lakebench-datagen` and its pods, while it runs.

A SparkApplication that has `COMPLETED` or `FAILED`, and a finished datagen
Job, are left in place and listed, so a failed stage's logs stay readable
with `lakebench logs`. A job already gone is reported as not running.

- When a deletion fails, `stop` still tries every other one, prints one
  line per failure and exits 1. A refused list of the SparkApplications, or
  a refused read of the datagen Job, counts as such a failure.
- A missing namespace means nothing to stop (exit 0).
- An unreachable cluster, or a refused read of the namespace, exits 4
  before anything is deleted.

For an AML (financial) deployment whose `lakebench-gold-refresh` is
running, `stop` first asks it to finish the detection pass it is running
(see
[AML scoring](aml-scoring.md#continuous-recall-over-covered-instances)) and
waits up to 300 s. If that is not confirmed, or Ctrl-C ends the wait, `stop`
warns "gold.alerts may hold a partial tick" (a tick is one detection pass)
and deletes the jobs anyway.

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

The benchmark runs the workload's query set (8 queries for Customer 360, 12
for AML) against the silver and gold layers and reports Queries per Hour
(QpH).

- QpH uses the median of `--iterations` timed samples per query.
- Each successful query then runs once more, untimed, to record its result
  fingerprint.
- Power mode runs queries one after another. Throughput mode runs N
  concurrent streams. Composite mode runs both and reports the geometric
  mean.

The result is saved as a record of its own under a new run id:

- `record_kind: "benchmark"`, with `parent_run_id` the deployment's latest
  run.
- It is a copy of that run's record with the new benchmark as its QpH,
  scores and query stage, and `provenance.benchmark` naming the code and
  time of the benchmark. A continuous run's in-stream rounds and the run's
  maintenance QpH pair are not copied.
- The run's own record is never rewritten.
- Nothing is recorded when the deployment has no run record, or when the
  query engine now runs another dependency set than that run recorded.

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

Give exactly one of `--sql`, `--example`, `--sql-file` or `--interactive`.
`--file` / `-f` is the config file, as on other commands. The result is
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

What the table shows, with a config or with only `--namespace`, is in
[Deployment](deployment.md#checking-status).

- A component scaled to zero counts as not ready.
- With only `--namespace`, an absent component is not drift.
- Exit 4: the cluster is unreachable or a read is refused.

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

What `clean` empties, the targets it refuses and how it removes catalog
entries first are in [Deployment](deployment.md#selective-cleanup).

`-f` is not accepted on `destroy` or `clean`: it exits 2 and names
`--force` / `-y`, because `-f` means `--file` everywhere else. With
`--force` or `-y` also given, `-f` only warns. `LAKEBENCH_LEGACY_SHORT_F=1`
restores the old meaning (force) with a warning, for this release only.

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

The steps in order, table removal, and which buckets are deleted or kept
are in [Deployment](deployment.md#destroy-order).

Exit codes (full table in [Exit Codes](exit-codes.md)):

- `0` everything removed.
- `1` a step failed (see the summary).
- `2` the config did not load, including a nameless config with no `--name`
  in a directory that cannot name its deployment.
- `3` one of:
  - a nameless config could not prove the deployment is its own, or the
    namespace was redeployed since that check (nothing deleted either way)
  - a redeploy found part way through (destroy stops there, and the steps
    before it may have removed components)
  - another refusal in the list above
- `4` the kubeconfig did not load.
- `5` the confirmation prompt was declined (no side effects).
- `6` everything else succeeded, but the namespace was still terminating at
  `--namespace-timeout`, usually because of a PVC or pod finalizer. Check
  with `kubectl get ns <namespace>` before re-deploying under the same name.

### report

Read a saved benchmark run. The default prints the summary and points at
the delivered `run-<id>/report.html` without changing it. `--render` writes
a fresh HTML report at
`lakebench-output/reports/report-<run_id>-<ts>.html` and leaves the
delivered file alone.

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
- The argument is a run id (a leading `run-` is dropped) or a config file.
  A config scopes the lookup to that deployment's latest run, so on a
  shared `lakebench-output` tree `report other.yaml` does not pick up
  another deployment's newer run.
- With no argument and no `--run`, `report` uses `./lakebench.yaml` when it
  exists and says so ("Showing the latest record of deployment NAME").
  Without one it reads the latest run of any deployment.
- A record written by `lakebench benchmark` (`record_kind: "benchmark"`) is
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

Components:

- `datagen`: the datagen Job pods.
- Each pipeline stage's Spark driver: `bronze-verify`, `silver-build`,
  `gold-finalize`, `bronze-ingest`, `silver-stream`, `gold-refresh`,
  `replay-financial`, `reproduce-financial`, `score-financial`,
  `score-financial-reference`, `time-travel-financial`.
- `spark-driver`: every Spark driver.
- `trino` (coordinator), `trino-worker`, `thrift`, `duckdb`, `hive`,
  `polaris` and `postgres`.

Behaviour:

- `logs` reads through the Kubernetes API, not `kubectl`.
- Log text goes to stdout unformatted. When several pods match, each pod's
  lines follow a header on stderr.
- `lakebench logs COMPONENT` alone uses `./lakebench.yaml`. The 1.6 order
  `lakebench logs COMPONENT CONFIG_FILE` still works, with a one-line
  warning.
- A pod the API has no log for yet (a container still starting, or no
  previous container with `--previous`) gets a warning with the API's
  message, and the other pods are still read.
- Exit 1: no pod matches, or none has a log to read. Exit 2: an unknown
  component. Exit 4: an API error (a refused read, a server error) or an
  unreachable cluster.

### journal

View the command and execution provenance journal: the history of every
lakebench operation, including deploys, data generation, pipeline runs and
teardowns.

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

### financial

AML (financial workload) operator actions. Each submits a Spark job against
the deployment in the config. `--wait/--no-wait` (default wait) controls
whether the command waits for it.

| Subcommand | Purpose |
|---|---|
| `financial score` | Compute rule recall from the datagen manifest and `gold.alerts` |
| `financial reference-score` | Run the reference detector and leakage gate over silver and the manifest |
| `financial replay` | Rerun one detection rule against a historical Iceberg snapshot (the output defaults to the gold alerts table with an `_replay` suffix) |
| `financial reproduce` | Rerun one batch alert's rule on the snapshots its run's gold read |

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

Reproduce one batch alert from the snapshots its run's gold read.

```
lakebench financial reproduce CONFIG [OPTIONS]
```

| Argument | Required | Description |
|---|---|---|
| `CONFIG` | yes | Lakebench config YAML |

| Flag | Short | Type | Default | Description |
|---|---|---|---|---|
| `--alert-id` |  | text |  | Alert id (gold.alerts.alert_id) to reproduce |
| `--run` |  | text |  | Run id whose record holds the snapshots gold read; default: the latest AML batch run of this deployment |
| `--wait` / `--no-wait` |  | flag | `--wait` | Wait for the result |

Exit paths of this command (the shared ones, such as usage errors, prerequisites, nameless-config and lease refusals and declined confirmations, are in [Exit codes](exit-codes.md)):

- `1` `financial.reproduce.mismatch`: `financial reproduce` ran the alert's rule on the snapshots its run's gold read and did not reproduce the alert (no match, several, different related transactions), or the rule declined to run
- `1` `financial.reproduce.not_found`: `financial reproduce` found no such alert in gold.alerts, or one another run wrote
- `2` `financial.reproduce.no_record`: `financial reproduce` found no AML batch run record of the deployment on this host (or none for `--run`)
- `4` `financial.reproduce.snapshot_gone`: `financial reproduce` cannot read what the alert's run read: the run recorded no read snapshots (before 1.7), or a snapshot expired and the table's content changed

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

Flags and defaults are in the generated tables below.

| Subcommand | Purpose |
|---|---|
| `admin status` | Installed operators, lease state, lakebench namespaces, controller `/tmp` size and storage evictions |
| `admin doctor [CONFIG]` | Read-only report on the shared components, the controller `/tmp` and the lease |
| `admin install [CONFIG]` | Install missing shared components, under the cluster lease |
| `admin repair-operator [CONFIG]` | Repair a stuck release, the watch list and the controller `/tmp`, under the cluster lease |
| `admin migrate-deployment NAMESPACE [CONFIG]` | Stamp identity annotations on a legacy pre-ownership namespace |
| `admin reclaim-bucket BUCKET [CONFIG]` | Rewrite a bucket's ownership tag to this deployment and this cluster, or on a backend without tagging its owner marker (`.lakebench/owner.json`). Refused (exit 3) when the bucket holds objects unless `--force-nonempty`; exit 4 when the cluster fingerprint cannot be computed |
| `admin release-lock` | Release an expired cluster lease; `--force` releases a live one (last resort) |

**`admin doctor`:**

- Runs the prerequisite checks of [Prerequisites](prerequisites.md): scratch StorageClass, Spark Operator, Stackable, observability stack, OpenShift SCC role.
- Without a config, it checks every component at its default name. Stackable and the observability stack are reported but do not fail.
- Exits 1 when a check fails or cannot run.

**`admin install`:**

- `--component all` needs a config. `--controller-tmp-size` has a floor of 4Gi.
- The version is `--version`, else the config's pin, else the Lakebench default.
- An installed component is never changed. With everything installed and ready it exits 0 and changes nothing.
- A config pin that differs from the installed version is kept with a warning. A stale shared Grafana dashboard is re-applied.
- Chart repos are refreshed before the lease is taken.
- Exit 1: a status that cannot be read, a release not `deployed`, an install that failed, or a component not ready.
- Exit 2: a malformed request, a version change without `--allow-version-change`, or `--controller-tmp-size` for an installed operator.
- Exit 3, refused:
  - a version change, which Lakebench does not automate because helm leaves a chart's `crds/` at the installed version
  - a second Spark Operator or kube-prometheus-stack
  - leftover CRDs
  - a partial Stackable install that is ambiguous or at another version
  - a StorageClass whose parameters differ from the config's

**`admin repair-operator`:**

- Rolls back a `pending-upgrade` or `pending-rollback` release whose pending revision started at least 10 minutes ago (API server clock).
- The target is the newest deployed revision that watches no deleted or Terminating namespace. When the operator watches every namespace, only a watch-all revision qualifies.
- Exit 3 when no revision qualifies, and for `pending-install`.
- Sets the watch list with one upgrade to the namespaces named by the Helm values, the controller or the webhook list that are still Active. Exit 3 when some of them watch every namespace and others do not.
- Raises a controller `/tmp` smaller than the given size.
- No release exits 4. `--dry-run` reads without the lease.

The Spark Operator runs spark-submit in its controller pod, which caches
jars under `/tmp`. The chart default of 1Gi is too small and gets the
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

Prints the installed lakebench version. `lakebench --version` (`-V`) does
the same. `-h` is short for `--help` on every command.

### Renamed, refused and deprecated commands

Old names still parse, so a 1.6 command line gets an answer instead of a
usage error. An alias prints one line on stderr and runs the new command. A
refusal exits 2 and names what to run instead, without echoing anything you
passed. None of them appears in `--help` or in the reference above. The
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
| `init --access-key`, `init --secret-key` | refused (exit 2): init writes a reference to the variable, never the key | export LAKEBENCH_S3_ACCESS_KEY and LAKEBENCH_S3_SECRET_KEY (or the names --credentials-env PREFIX gives) |
| `clean --metrics-dir`, `clean -m` | refused (exit 2): run records and journals are evidence, and the CLI does not delete them | nothing |
<!-- END GENERATED: cli aliases -->

## Common Patterns

### Typical Workflow

```bash
lakebench init                              # create lakebench.yaml
lakebench plan lakebench.yaml               # what it needs: sizing, prerequisites, egress
lakebench config validate lakebench.yaml    # check connectivity
lakebench deploy lakebench.yaml --yes       # deploy infrastructure
lakebench generate lakebench.yaml           # generate data; the timeout grows with scale
lakebench run lakebench.yaml                # run pipeline + benchmark, delivers report.html
lakebench report lakebench.yaml             # print the summary of the delivered report
lakebench destroy lakebench.yaml --force    # tear down what this deployment owns
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
