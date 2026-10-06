# Upgrading to Lakebench 1.7

Every change in 1.7 that can break a 1.6 config, command line, script or
comparison, with what to do. The list is `docs/upgrading/breaking-1.7.yaml`;
a unit test checks it against the code, this file and the CHANGELOG. The
full exit-code table is [docs/exit-codes.md](docs/exit-codes.md), and the
renamed and refused commands are listed in
[docs/cli-reference.md](docs/cli-reference.md#renamed-refused-and-deprecated-commands).

Before anything else: redeploy each deployment once with `lakebench deploy
CONFIG` (see [run needs a 1.7 deploy](#run-needs-a-17-deploy)), and give
every config a `name:` and a `recipe:`. `lakebench init --from OLD.yaml -o
NEW.yaml` does both for a 1.6 config: it keeps the deployment's name (from
the config, or the one 1.6 recorded in `.lakebench/state.json`) and its
bucket names, writes the recipe the config resolves to, drops the removed
keys with their fix text, moves plaintext secrets to `${VAR}` references,
and writes nothing unless the new file loads to the same settings as the
old one (docs/configuration.md, "Converting an older config"). A
Customer 360 deployment whose pipeline ran under 1.6 keeps its silver and
gold tables; its first 1.7 batch run rebuilds them, so pass
`--force-rebuild` (the refusal names it), as any repeat run on a
deployment needs.

## Removed config keys

### Config fields nothing read are removed

Twenty config keys nothing read are removed: at its 1.6 default each loads with a note; another value is refused by the commands that change data.

The keys:

- `images.hive`
- `images.prometheus`
- `images.grafana`
- `platform.storage.s3.secret_ref`
- `secret_ref`
- `architecture.catalog.hive.thrift`
- `architecture.catalog.polaris.version`
- `architecture.catalog.unity.version`
- `architecture.table_format.iceberg.file_format`
- `architecture.table_format.iceberg.properties`
- `architecture.table_format.delta.properties`
- `architecture.pipeline.medallion`
- `architecture.workload.customer360.date_range_days`
- `observability.reports`
- `observability.storage_class`
- `observability.prometheus_stack_enabled`
- `observability.s3_metrics_enabled`
- `observability.spark_metrics_enabled`
- `version`
- `description`

**What to do:** Delete the key. Each refusal names it and what to do instead; docs/configuration.md lists them.

### Sizing settings that sized nothing are removed

`platform.compute.spark.driver`, `.executor` and `platform.storage.scratch.size` sized nothing: set to other than their 1.6 default, they are refused.

The keys:

- `platform.compute.spark.driver`
- `platform.compute.spark.executor`
- `platform.storage.scratch.size`

**What to do:** Delete them; set counts with `<job>_executors` and the driver with `driver_memory` and `driver_cores`.

## Refused commands and flags

### config upgrade is refused

`lakebench config upgrade` exits 2 before opening any file: it rewrote configs lossily and wrote secrets in plaintext.

**What to do:** Rewrite the config with `lakebench init --from OLD.yaml -o NEW.yaml`, which keeps its name and buckets and lists every key it moves or drops, or edit it by hand.

### clean bronze and clean data are refused

`clean bronze` and `clean data` are refused (exit 2): a run regenerates its own corpus.

**What to do:** Run `lakebench run CONFIG --generate --regenerate`; for `data`, `clean silver` and `clean gold` first.

### clean metrics and clean journal are refused

`clean metrics`, `clean journal` and `clean --metrics-dir` are refused (exit 2): records and journals are evidence.

**What to do:** Delete run records or journals by hand if you must; the CLI does not.

### compare is removed

`lakebench compare` exits 2 with any arguments: comparing runs is left to the reader, and each report states the corpus, components, result fingerprints and caps needed to judge a comparison.

**What to do:** Run each configuration with `lakebench run`, then read the two reports side by side (`lakebench report CONFIG`); see Comparing Runs in docs/benchmarking.md.

### init refuses credential values

`init --access-key` and `--secret-key` exit 2 without echoing the value; init writes `${VAR}` references.

**What to do:** Export `LAKEBENCH_S3_ACCESS_KEY` and `LAKEBENCH_S3_SECRET_KEY`, or name your own with `--credentials-env PREFIX`.

### Dead flags are removed

`generate --wait` / `-w`, `admin release-lock --expired-only` and `deploy --include-observability` are unknown options (exit 2).

**What to do:** Drop `--wait` and `--expired-only`; set `observability.enabled: true` in the config.

### lbrun.py is removed

The run-from-a-checkout wrapper `lbrun.py` is removed.

**What to do:** Use `PYTHONPATH=src python -m lakebench`.

## Renamed commands and flags

### results is an alias of report

`lakebench results` is an alias of `report --format table` that prints one line on stderr; it is removed in v1.8.

**What to do:** Use `lakebench report --format table` (or `--format json` / `csv`).

### admin install-spark-operator and install-scratch-storage-class are aliases

`admin install-spark-operator` and `admin install-scratch-storage-class` are aliases of `admin install --component`, removed in v1.8.

**What to do:** Use `lakebench admin install --component spark-operator` or `--component scratch-storage-class`.

### init has no wizard

`init --interactive`, `-i` and `--advanced` print one line and write the default config: the wizard is removed.

**What to do:** Drop the flag; pass `--recipe`, `--name`, `--scale` or `--credentials-env` to `init`.

### run --sustained is a deprecated alias

`run --sustained` is a hidden, deprecated alias of `--continuous` and prints a warning.

**What to do:** Use `lakebench run --continuous`.

### recommend --extended is a deprecated alias

`recommend --extended` / `-e` is a deprecated alias of `--slow-datagen`, which is now ignored.

**What to do:** Drop the flag; use `lakebench config recommend CONFIG`.

## Exit codes

### status stop and logs exit non-zero when something is wrong

`status` exits 1 on drift or a missing namespace, `stop` and `logs` exit 1 on a failure, and API errors exit 4 (all were 0).

**What to do:** Scripts that treated these commands' exit 0 as information should read the code; docs/exit-codes.md has the table.

### Usage and config errors exit 2

A config that does not load, an unsupported combination, a bad argument or a nameless config exits 2 (was 1 or 0).

**What to do:** Treat 2 as "nothing ran, fix the command or config".

### Safety refusals exit 3

Ownership, redeploy, non-empty bronze and held-lease refusals exit 3 (was 1 or 2).

**What to do:** Treat 3 as "refused by the safety model"; the message names what to check.

### Missing prerequisites exit 4

A failed preflight check and an unreachable Kubernetes API or S3 bucket exit 4 (was 1 or 2).

**What to do:** Treat 4 as "a prerequisite is missing, nothing ran".

### Declined confirmations exit 5

A declined or unanswerable confirmation exits 5 (was 1 or 3).

**What to do:** Pass `--yes` in scripts, or treat 5 as "not confirmed".

### A namespace still terminating exits 6

`destroy` exits 6 (was 4) when its steps finished but the namespace is still terminating.

**What to do:** Treat 6 as "safe to re-run"; check `kubectl get ns` before redeploying the name.

### reproduce drift exits 14

`reproduce` exits 14 (was 2) for metric drift or commit drift without `--allow-commit-drift`.

**What to do:** Treat 14 as "requirement unmet".

### A datagen timeout exits 1

A `run` whose datagen did not finish in time exits 1 (was 5); the record says "datagen timed out".

**What to do:** Read `verdict.reasons` in `metrics.json` instead of the exit code.

## Comparability and identity

### Customer 360 records carry a new workload version

Customer 360 gold is never silently incremental and a multi-cycle run takes one data clock; records carry workload version `c360-2.dev1` and do not compare with `c360-1`.

**What to do:** Re-run a Customer 360 baseline under 1.7 before comparing; `spark.lb.gold.strategy=incremental` is refused.

### AML records are workload version aml-2

AML alert evidence is capped at 1,000 ids per W4 alert and flagged; records carry workload version `aml-2` and do not compare with `aml-1`.

**What to do:** Re-run an AML baseline under 1.7 before comparing; read the bounded-recall labels in the score.

### System and access path are not execution conditions

Experiment identity v2: the system and the query access path are architecture and system groups, no longer conditions that make a pair not like-for-like.

**What to do:** Re-run a baseline under 1.7 before comparing; when the architecture and the system both differ, no difference can be put down to either.

### Continuous pairs with different round counts are not like-for-like

A continuous record without a stored round count reads it from its rounds; the stored C360 Trino vs Thrift pair (runs 011043-e338c5, 073533-9de9c9) is now not like-for-like.

**What to do:** Compare continuous runs with the same number of in-stream rounds; no identity digest moves.

### Perf gate removed

`scripts/perf_gate.py`, its pinned configs (`benchmarks/perf/`) and the baseline store are gone.

**What to do:** Read `lakebench report` for each run side by side; `lakebench reproduce` re-runs a record.

### Readers take the strictest verdict

`report` reads the stricter of a record's stored verdict and the one recomputed from it: three stored AML batch records without a watchlist now read FAILED, and their Hive-versus-Polaris pair (011123-497f02, 011355-7ad7ad) is not comparable.

**What to do:** Re-run a record that now reads FAILED; `report --json` shows `verdict_stored` and `verdict_recomputed` beside the `verdict` it heads with.

## Version bumps

### The Hive recipes default to Spark 4.1.1

The Hive recipes now default to Spark 4.1.1. A config that does not set `images.spark` runs Spark 4.1.1 (and Delta 4.1.0) where v1.6 ran 4.0.2. Its jars, dependency set and perf fingerprint change, and a deployment made from it must be redeployed before run.

**What to do:** Pin `images.spark: apache/spark:4.0.2-python3` to keep 4.0.2. A config that writes `delta.version: 4.0.0` keeps Spark 4.0.2.

### The default datagen image is the v1.7 look image

The default datagen image is `lb-datagen:2a36ae21`, pinned by digest: the v1.7 look image. A config that does not set `images.datagen` generates with it where v1.6 used `lb-datagen:1.6.0`; its output on the five byte-compare cases is byte-identical to 1.6.0, and the lineage table maps it to the 1.6.0 root.

**What to do:** Nothing for most configs. A config that pins `images.datagen: docker.io/sillidata/lb-datagen:1.6.0` keeps the 1.6 image, which writes no corpus markers and refuses a registered corpus; remove the pin to take the 1.7 image.

## Changed behaviour and defaults

### Removed keys are refused by commands that change data

A removed config key is refused by the commands that change data; read and teardown commands drop it with a note.

**What to do:** Delete the keys the refusal names; `destroy`, `status` and `report` still load the old config.

### Deploy generates the Polaris client secret

A new deployment generates its own Polaris client secret and database passwords; 1.6 used fixed values for every install.

**What to do:** Leave `architecture.catalog.polaris.client_secret` unset; an existing deployment keeps its stored secret.

### run needs a 1.7 deploy

Jobs take every jar and wheel from the deployment's dependency server; `run` on a deployment made by 1.6 exits 4.

**What to do:** Run `lakebench deploy CONFIG` once after upgrading.

### stop drains AML detection first

`stop` on an AML deployment waits up to 300 s for gold-refresh to finish its detection tick before it deletes the jobs; a continuous AML run ends with the same drain (up to 1800 s) and a score job, and fails when the drain times out.

**What to do:** Allow for the wait. Ctrl-C ends it and `stop` still deletes the jobs; a run whose drain fails is a failed run, so rerun it.

### continuous AML runs end with time-travel reads

A continuous AML run that passed its gates ends with one more Spark job after the score job: it re-reads every transactions snapshot the detection ticks recorded (two full scans of each that is still live, and one of the current snapshot), bounded by the per-job timeout; its check is reported beside the verdict and never fails the run.

**What to do:** Allow for the extra job at the end of the run; it grows with the corpus and the number of live snapshots, and stops starting scans before the per-job timeout.

### financial reproduce reruns the alert's rule on what gold read

`financial reproduce` reproduces the alert from the snapshots its run's gold read, which runs record from 1.7 on: exit 0 when reproduced, 1 when not reproduced or not found, 2 when this host has no record of the run, 4 when those snapshots are gone or the run predates 1.7; 1.6 exited 1 after every reproduction it waited for (it could not reproduce), and 0 after a submit with `--no-wait`, which now refuses first when the record cannot drive a reproduction.

**What to do:** Reproduce alerts of 1.7 AML batch runs; `--run RUN_ID` picks a run other than the deployment's latest.

### AML bronze-verify stops on a spent or unverifiable corpus

An AML run over a corpus with no manifest (batch, continuous with `--skip-generate`, or a `run --stage` subset), or over a bucket that holds a corpus from a held-out or spent seed (such as 42), stops at bronze-verify with exit 2; 1.6 only warned about a missing manifest and refused a spent corpus only at reference scoring.

**What to do:** Regenerate the corpus with `lakebench run CONFIG --generate --regenerate` (the calibration seed when `datagen.seed` is unset).

### A protected AML corpus is refused outside its look

`run`, `benchmark`, `query`, `reproduce` and the `financial` commands refuse an evaluation or robustness AML corpus, by role or by seed, with exit 2, before any cluster call.

**What to do:** Use the calibration seed or another unregistered seed. A registered look runs only through `scripts/aml_gate.py --registered`, and its corpus is generated only by `lakebench generate --registered-corpus`.

### Executor overrides are bounded and counted

Executor overrides take 1 to 28 (`driver_cores` 1 to 16), count in the capacity check, and keep a run out of release evidence.

**What to do:** Lower overrides above 28; leave counts unset for evidence runs.

### benchmark writes its own record

`benchmark` saves a record of its own (`record_kind: benchmark`) instead of rewriting the run's; `query` writes no record.

**What to do:** Read a benchmark by the run id it prints: `lakebench report RUN_ID`.

### run refuses arguments it used to ignore

`run` exits 2 before any cluster call on a flag its mode does not use (the list is under `run` in docs/cli-reference.md).

**What to do:** Drop the flag the mode does not use.

### reproduce never destroys before its run

`reproduce` refuses (exit 3) an existing namespace or bucket instead of destroying it, and destroys only what it created.

**What to do:** Run `lakebench destroy CONFIG` first to reuse a deployment's name.

### init writes a first-day config

`init` writes a 12-line config: a new name per `init`, recipe `polaris-iceberg-spark-trino` (was Hive), scale 1 (was 10), `${VAR}` credentials.

**What to do:** Set `--scale`, `--recipe` or `--name` on `init`; `--overwrite` is the new spelling of `--force`.

### A component that contradicts its recipe is refused

A catalog, format or engine that contradicts `recipe:` is refused at load by the commands that change data; 1.6 let it win silently.

**What to do:** Remove the component keys, or change `recipe:` to the components you mean.

### VAR is substituted per value

`${VAR}` is substituted per value, not in the file text: an environment value is no longer parsed as YAML.

**What to do:** Quote references inside flow syntax (`["${A}", "${B}"]`) and close every `${VAR:-default}`.

### A config with no recipe is deprecated

A config with no `recipe:`, or `recipe: default`, loads with a note; v1.8 requires `recipe:`.

**What to do:** Add the recipe the note names.

### Flat top-level keys are deprecated

Flat top-level keys (`endpoint:`, `scale:` and the rest) load with a note naming the nested key.

**What to do:** Write the nested key the note names.

### Deploy never installs a shared component

`deploy` only checks the scratch StorageClass, Spark Operator, Stackable and observability stack; `operator.install: true` is refused.

**What to do:** A cluster admin runs `lakebench admin install --component all CONFIG` once per cluster.

### admin install never changes an installed component

`admin install` installs only what is missing and refuses a version change (exit 2, or 3 with `--allow-version-change`).

**What to do:** Change a shared component's version by hand, after reading what `--allow-version-change` lists.

### Watch-list edits pin the installed chart

`deploy`, `run` and `destroy` edit the Spark Operator watch list on the installed chart, or refuse when it cannot be read.

**What to do:** Re-run once `helm list` answers; a refused removal keeps the namespace.

### admin doctor exits 1 on a failed check

`admin doctor` runs the prerequisite checks and exits 1 when one fails or cannot run.

**What to do:** Fix what it names, or ignore its code where you only wanted the report.

### A config needs a name to change data

A config with no `name:` is refused by the commands that change data, and reads or tears down only a deployment it can prove is its own.

**What to do:** Add `name:` with the deployment's name; the refusal names it.

### Counts are bounded at load

A zero, negative or out-of-range count (Trino workers, generators, ports, cores), which 1.6 accepted, is refused.

**What to do:** Set the count inside its range (docs/configuration.md).

### spark.conf merges over the job defaults

`spark.conf` merges over seven job defaults, and keys Lakebench sets (including `userClassPathFirst` and `spark.kubernetes.*`) are refused.

**What to do:** Keep only your own tuning keys in `spark.conf`; the refusal names what controls each reserved key.

### run refuses benchmark settings it does not run

`run` refuses a config whose benchmark sets `mode: throughput|composite`, `cache: cold` or `streams` above 1.

**What to do:** Use `lakebench benchmark --mode`, `--cold` or `--streams` for those passes.

### driver_memory and the gold strategy are checked at load

A `driver_memory` Spark cannot read (`16Gi`, `1.5g`) and a `spark.lb.gold.strategy` other than `auto`, `simple_agg` or `two_phase_agg` are refused by the commands that change data.

**What to do:** Write `driver_memory` as a whole number with k, m, g or t (`16g`); delete the gold strategy or set one of the three.

### Buckets are stamped with their cluster

Deploy stamps owned buckets with the cluster; a bucket 1.6 adopted is used but no longer emptied or deleted by `destroy`.

**What to do:** Claim it with `lakebench admin reclaim-bucket BUCKET CONFIG --force-nonempty` (an owner action; it holds the 1.6 data) before relying on `destroy`.

### The capacity preflight counts free capacity and fails closed

`run` compares its request with free capacity, not allocatable, and refuses (exit 4) when nodes or pods cannot be read.

**What to do:** Run on a cluster with room, or pass `--skip-preflight` (the record then says capacity not checked).

### Customer 360 batch verdicts gate on sixteen checks

A Customer 360 batch verdict fails on sixteen exact checks only, including when they cannot be evaluated; others are listed, not gating.

**What to do:** Re-read a 1.6 PASS or FAIL under 1.7 rules; `verdict.qualifiers.c360_failed_not_gating` lists the rest.

### Grafana gets a generated password

A new shared observability install gets a generated Grafana password; an existing install keeps `admin`/`lakebench`.

**What to do:** Read it with the command `deploy` prints (Secret `lakebench-observability-grafana`).

### config_snapshot drops the unused sizing blocks

`metrics.json` `config_snapshot` drops `spark.driver` and `spark.executor` and replaces `scratch.size` with `scratch.size_per_job`.

**What to do:** Read executor counts from `jobs[]` and scratch from `scratch.size_per_job` in scripts that parse records.

### A run passes only when its record shows it

A PASSED verdict also needs rows in every layer, the expected AML rules (W1 giant-component or vertex-cap and W3 or W17 path-cap allowed), a batch scale ratio of at least 0.95 and no empty answer; `run` exits 1 when its record does not read PASSED.

**What to do:** Read `verdict.reasons` and `verdict.gates` in `metrics.json`; a run that exited 0 under 1.6 with an empty layer or a skipped rule now exits 1 and says which.

### The datagen generator refuses arguments it cannot read

The 1.7 datagen image (pinned before the release) exits 2 on an unknown, repeated, valueless or unparseable flag, a stray argument, a non-finite float or a Customer 360 `--cycle` without `--cycles`; 1.6 dropped them or used a default.

**What to do:** Lakebench's own Jobs pass valid arguments; correct scripts and manual Jobs that call the image directly (`--node-id abc` used to run as node 0).

### The datagen image builds only with its commit

Building the datagen image needs `--build-arg LB_BUILD_COMMIT=<commit>`; a plain `podman build` of `datagen_rs/` now fails.

**What to do:** Build with `--build-arg LB_BUILD_COMMIT=$(git rev-parse HEAD)`, as docs/datagen-custom-images.md shows.

### Datagen pods honour path style, TLS and CA settings

Datagen pods on the 1.7 image honour `platform.storage.s3.path_style`, `verify_ssl` and `ca_cert`, which 1.6 ignored (path-style, plain HTTP and the system CAs always); a value they cannot read exits 2, and with `ca_cert` set datagen trusts only the CAs in that file.

**What to do:** Keep `path_style: true` for FlashBlade and MinIO, and give `ca_cert` a file the pod can load for an HTTPS endpoint with a private CA; leave `ca_cert` unset for an endpoint a public CA signs.

### Multi-cycle runs and reused corpora check a corpus series marker

A run that reuses bronze exits 3 when its corpus series marker is unfinished or made for another cycle count, window or generation, or is missing on a multi-cycle config or over later cycles' files (4 when bronze cannot be read); a multi-cycle run over a non-empty datagen prefix exits 3 without `--regenerate`; `generate` or `run --generate-only` on a multi-cycle config and `run --skip-generate` on a multi-cycle AML config exit 2.

**What to do:** Keep a finished multi-cycle corpus with `run --skip-generate`; otherwise let `run` generate it, with `--regenerate` (single-cycle: `--generate --regenerate`) on a bucket this deployment created, after `lakebench admin reclaim-bucket` on any other. An AML multi-cycle run generates every cycle.
