# Development Guide

Reference: the code map, test tooling, CI, extension points and why the code is built as it is.

Setup, test tiers, pull requests and review are in
[CONTRIBUTING.md](../CONTRIBUTING.md). The product model and its extension
rules are in [DESIGN.md](DESIGN.md).

## Architecture map

The source lives under `src/lakebench/`. Each subdirectory is one module with
one responsibility:

```
src/lakebench/
  aml/          Pure-Python AML helpers: reference detector, leakage gate, predictions
  benchmark/    Query engine benchmark (QpH; 8 c360 queries, 12 AML queries)
  cli/          CLI package (Typer commands, helpers, continuous pipeline)
  config/       Pydantic config schema, YAML loader, cluster autosizer
  deps/         A deployment's dependency set: what it needs, resolved in its namespace
  deploy/       Deployment engine, destroy, ownership records, cluster lease,
                and re-export shims for deployers
  engine/       PipelineEngine protocol and get_engine() factory
  journal/      Event logging (session-scoped JSONL provenance logs)
  k8s/          Kubernetes client wrapper and OpenShift security (SCC/RBAC)
  metrics/      Pipeline metrics collection, aggregation, and storage
  modules/      Component implementations:
    catalogs/       hive/, polaris/ -- catalog deployers; unity/ is kept but no
                    recipe uses it (Unity is unsupported)
    query_engines/  trino/, spark_thrift/, duckdb/ -- deployers + executors
    pipeline_engines/ spark/ -- job manager, monitor, operator, RBAC
    table_formats/  iceberg/, delta/ -- maintenance SQL builders
  observability/ Platform and S3 metrics collection for the observability stack
  reports/      HTML report generation from benchmark results
  runtime/      Runtime abstraction: Kubernetes, or podman/docker for local runs
  s3/           S3 client (boto3 wrapper with FlashBlade compatibility)
  spark/        Spark job scripts (scripts/), AML data including the
                pre-registration JSON (data/aml/), and re-exports of
                modules/pipeline_engines/spark/
  templates/    Jinja2 templates for Kubernetes manifests
```

Supporting directories:

- `tests/`: the pytest suite, one file per module or behaviour; `tests/spark/`
  holds the PySpark tier.
- `datagen_rs/`: the Rust data generator and its container image (Parquet
  files to S3).
- `examples/`: example configs, at least one per recipe.
- `scripts/`: the release gate, the doc generators and the CI checks.
- `docs/`: the project documentation.

### Where things live

| What | Where |
|---|---|
| Deploy order | `deploy/engine.py`; described in [architecture.md](architecture.md#deployment-order) |
| Spark per-job sizing | `_JOB_PROFILES` and `_SCHEMA_PROFILE_OVERRIDES` in `src/lakebench/modules/pipeline_engines/spark/job.py`. The docs quote these, never the other way round |
| Cluster minimums | `compute_peak_requirements()` in the same file, and `config/sizing.py`, which `info`, `recommend` and the capacity check read |
| Version compatibility tables | `_FORMAT_VERSION_COMPAT`, `_ICEBERG_RUNTIME_SUFFIX` and `_HADOOP_AWS_COMPAT` in `job.py` |
| Valid architectures | `_SUPPORTED_COMBINATIONS` and `_COMBINATION_NOTES` in `src/lakebench/config/schema.py` |
| Recipes | `src/lakebench/config/recipes.py` |
| Support record | `src/lakebench/config/validated_combinations.yaml` and `config/support.py` |
| Queries | `src/lakebench/benchmark/queries.py` (the QpH sets), `benchmark/aml_queries.py` and the SQL under `benchmark/queries/aml/` (the AML score queries) |
| Metric definitions | `src/lakebench/metrics/metric_registry.py`, which the collector's score descriptions read |
| Kubernetes manifests | the Jinja2 templates in `src/lakebench/templates/`; Spark jobs are built in code in `job.py` |
| Reproduction packages | `src/lakebench/cli/_reproduce.py` (`_build_package`); it shares the deploy, generate, run and destroy code with the main CLI |
| Datagen metrics | `datagen_rs/src/metrics.rs` (struct and JSON line), `src/lakebench/metrics/datagen_aggregator.py` (aggregator), `src/lakebench/templates/datagen/job.yaml.j2` (`LB_POD_CPU_REQUEST_MILLI`) |

### Key conventions

- **Configuration** uses Pydantic v2 models (`config/schema.py`). All user
  config is one YAML file, parsed by `config/loader.py`.
- **Deployment** is component-based. Each deployer implements deploy and
  destroy; `deploy/engine.py` runs them in order. Every resource a deployment
  creates carries the ownership stamps of `deploy/ownership.py`.
- **Spark scripts** in `spark/scripts/` are standalone PySpark programs. They
  run in the driver and executor pods, submitted through the Spark Operator.
- **All outputs** (journals, metrics, reports) go to `lakebench-output/`.

## Test tooling

### Dependencies

`pip install -e ".[dev]"` (or `make dev`) installs:

- **Runtime:** Typer, Pydantic v2, Rich, boto3, kubernetes, httpx, Jinja2,
  PyYAML.
- **Dev:** pytest, pytest-cov, pytest-xdist, ruff, mypy, moto (AWS mocking),
  pre-commit.
- **The `[aml]` extra:** numpy, scipy, pandas, scikit-learn, joblib and
  threadpoolctl at exactly the versions the cluster's AML reference detector
  installs (`REFERENCE_PY_DEPS` in
  `src/lakebench/modules/pipeline_engines/spark/job.py`), plus pyarrow.
  - Keep the extra and the tuple equal by hand; no test checks them.
  - The pins have wheels for Python 3.10 to 3.13.

### Running tests

- The tests import `src/` of this checkout, whatever is installed.
  `pyproject.toml` sets pytest's `pythonpath = ["src"]`, so a bare `pytest`
  does too.
- `make check-fast` and `make test` also set `PYTHONPATH=src` for the
  subprocesses some tests start.
- They run one worker per CPU (`pytest-xdist`, one test file per worker).
  `make check-fast XDIST_WORKERS=8` caps the workers on a shared machine.
  `PYTHON=python3.11` picks the interpreter (default `python3`).
- CI does not run the `slow` tests (over 20 s, the one marker in use). Run
  `pytest tests/ -m slow` (about 20 minutes) before a release.
- The whole unit suite serially, slow tests included, stopping at the first
  failure: `pytest tests/ -x -v --ignore=tests/spark`.
- There are no cluster tests. `pyproject.toml` still registers the
  `integration`, `e2e`, `extended` and `stress` markers, but no test carries
  them. Live runs check cluster behaviour.

### Makefile targets

`make help` lists the common targets. The table has every one except the
release steps (`make release-check` and `make rc-<step>`, see
[RELEASING.md](../RELEASING.md)).

| Target | Description |
|--------|-------------|
| `make install` | Install the package in editable mode, without the dev extra |
| `make dev` | Install with the dev extra and set up the pre-commit hooks |
| `make check-fast` | Ruff and the format check on `src/`, `tests/` and `scripts/`, mypy on `src/lakebench/`, then the unit tests in parallel without `tests/spark` and the `slow` tests |
| `make test` | The pytest command of `make check-fast` alone |
| `make test-spark` | The Spark tier on the installed pyspark, with its pinned jars fetched. A missing jar fails the test (`LB_REQUIRE_JARS=1`). CI does not run this tier |
| `make test-unit` | The whole unit suite serially, `slow` tests included |
| `make test-integration`, `make test-e2e` | No tests carry these markers; they collect nothing |
| `make test-cov` | `pytest tests/` with the default marker filter of `pyproject.toml` (no `e2e` or `integration`) and a coverage report in `htmlcov/index.html`; not in `make help` |
| `make lint` | Ruff on `src/`, `tests/` and `scripts/`, as CI runs it |
| `make fmt` | Format `src/`, `tests/` and `scripts/` with ruff and apply auto-fixes |
| `make typecheck` | Mypy on `src/lakebench/` |
| `make clean` | Remove build artifacts, caches, and `__pycache__` directories |

Ruff targets Python 3.10, with a 100-character line and the pycodestyle,
pyflakes, isort, bugbear, comprehensions and pyupgrade rule sets
(`pyproject.toml`). Mypy allows untyped functions. CI runs
`mypy src/lakebench/` and it must pass.

### Hooks and the prose guard

Pre-commit hooks (`.pre-commit-config.yaml`, installed by `make dev`) run on
every `git commit`:

1. **gitleaks**: fails the commit on anything that looks like a credential.
2. **Ruff lint**: checks `src/` and `tests/`, with auto-fixes.
3. **Ruff format**: formats `src/` and `tests/`.
4. **cargo fmt**: checks the Rust datagen when a `.rs` file changes.

- The hooks do not run the tests, and the ruff hooks skip `scripts/`. Run
  `make check-fast` before pushing.
- `pre-commit run --all-files` runs every hook over the whole tree.

The pre-push hook (`scripts/hooks/pre-push`; install:
[CONTRIBUTING.md](../CONTRIBUTING.md#setup)):

- Refuses a push whose branch reaches a commit from before the 2026-09-30
  history rewrite.
- Scans the pushed commits and their messages with gitleaks. It uses
  `.gitleaks.toml` and `.gitleaksignore` from `origin/main`, not the branch's
  own copies.
- Scans what merge commits change, and ignores inline `gitleaks:allow`.
- Refuses the push when git cannot run `--remerge-diff` (git older than 2.36)
  or when gitleaks logs an error. gitleaks exits 0 when its git log fails,
  having scanned nothing.
- Makes a relative `GIT_DIR` (`git --git-dir=.git push`) absolute first.
- Is installed by copying, not through pre-commit: the framework would read
  the pushing branch's config, which an old branch may lack.
- Is shared by every worktree of a clone, so install it from the checkout
  whose copy you want.
- `tests/test_pre_push_hook.py` exercises it. The release gate's
  `pre-push-hook` check fails if the installed copy differs from the tracked
  one.

`python scripts/prose_guard.py` fails on an em dash, an emoji or an AI
attribution line in any tracked file. The release gate's `prose` check runs
it.

- A hit that has to stay goes in `scripts/prose_allowlist.txt` as
  `path:kind:key  # reason`.
- The key is what the hit prints: a hash of the line, so the entry survives
  edits elsewhere in the file.
- An entry whose hit is gone fails the guard.

### Testing the Spark scripts

The scripts in `src/lakebench/spark/scripts/` import each other by plain name
(`from common import ...`), as they do in the driver pod. Tests load them
through the `load_script` fixture in `tests/conftest.py`. It gives each test a
private copy of every script it imports, `common` included:

```python
def test_gate(load_script):
    sbf, common = load_script("silver_build_financial", extra=("common",))
```

- A module whose tests import scripts inside the test body can mark itself
  with `pytestmark = pytest.mark.usefixtures("load_script")`. While a test
  runs, `import common` resolves to that test's copy.
- `load_script_module` keeps one copy per test module, for module-scoped
  fixtures that run script code.
- Do not put the scripts directory on `sys.path` or pop script modules out
  of `sys.modules`. A teardown hook in `tests/conftest.py` fails the test that
  leaves either behind.

### The Spark tier and its jars

`pytest tests/spark` needs pyspark (4.0.1 or 4.1.1), pyarrow and Java 17. A
test that imports pyspark, Delta or py4j goes under `tests/spark`: the unit
suite has no pyspark and would skip it.

Iceberg and Delta tests read the jars from `LB_SPARK_TEST_JARS`, a
comma-separated list of jar files for the installed Spark line:

| Line | Jars |
|------|------|
| pyspark 4.0.1 | `iceberg-spark-runtime-4.0_2.13-1.11.0.jar`, `delta-spark_2.13-4.0.0.jar`, `delta-storage-4.0.0.jar` |
| pyspark 4.1.1 | `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, `delta-spark_4.1_2.13-4.1.0.jar`, `delta-storage-4.1.0.jar` |

- These match the product defaults in `modules/pipeline_engines/spark/job.py`.
- Keep `tests/spark/jars.lock.json` equal to them by hand. After a default
  changes, `python scripts/fetch_test_jars.py --update-lock` rebuilds the
  lock. It confirms each coordinate by its POM and checks the bytes against
  Central's `.sha1`.
- `scripts/fetch_test_jars.py` downloads the pinned jars, checks their sha256
  and caches them in `~/.cache/lakebench-test-jars`. `--print-env` prints the
  variable:

```bash
make test-spark      # fetch, then the tier
jars=$(python scripts/fetch_test_jars.py --leg auto --print-env) && export "$jars"
pytest tests/spark -q -rs
```

A test declares its jars with `@pytest.mark.requires_jars("iceberg")`,
`("delta")` or both. Without them it skips with a reason starting
`LB-JARS missing:`, and the run still exits 0. **Read the skip count.**

Fixtures in `tests/spark/conftest.py`:

| Fixture or mark | What it gives |
|-----------------|---------------|
| `spark_session` | One local session per test module, stopped at the module's end. It carries the Iceberg or Delta extension the module's tests declare, and Delta as `spark_catalog` for Delta |
| `spark_static_conf({...})` | Module mark: static settings applied when the session starts |
| `iceberg_catalog(spark, name, warehouse)` | Registers a Hadoop Iceberg catalog on the session |
| `spark_jars` | The jar list, with `.classpath` for a child's `--jars` |
| `spark_subprocess(script, *args)` | Runs a child in a fresh JVM with the jars, `PYSPARK_PYTHON` and a `PYTHONPATH` of the Spark scripts, `tests/spark`, `src` and the repo root. On a non-zero exit it fails with the last 2000 characters of stdout and stderr |

- To run such a child by hand, give it the same path:
  `PYTHONPATH=src/lakebench/spark/scripts:tests/spark:src:. python tests/spark/test_x.py ...`.
- A test that calls a stream's micro-batch handler directly wraps the call in
  `foreach_batch_harness(spark, handler, df, batch_id)` from
  `tests/spark/_foreach_batch.py`. It sets the local properties a real
  `foreachBatch` sets and restores them afterwards.
- With several writer threads, each thread enters `inside_foreach_batch`
  itself.

### The HTML report: derived numbers and goldens

Every percentage, total and count the HTML report computes goes through
`src/lakebench/reports/derived.py`. It wraps the number in a span naming the
`metrics.json` paths it came from. `tests/test_report_consistency.py`
recomputes each one from the stored record and fails on:

- any difference;
- a unit its own table contradicts (a stored percentage scaled as a fraction,
  bytes shown as GiB without the conversion);
- a percentage in the page text that is neither derived nor a stored string.

Six stored records render into `tests/fixtures/reports/<run>.html`, and
today's render must match them exactly.

- The renderer produced the goldens, so they only show that a page did not
  move. The independent checks are the record recomputation and the phrases
  in `tests/expected/report_goldens.json`, each computed by hand from the
  record.
- There is no update flag. A change that moves the page asserts the number it
  fixes in its own test, edits the affected phrases by hand, and re-renders
  the goldens:

```bash
PYTHONPATH=src python3.11 -m tests.fixtures.report_goldens 5105a0
```

A second agent reviews the golden diff (`git show -- tests/fixtures/reports`)
against the record before the change is ready.

## What CI Runs

`.github/workflows/ci.yml` runs on every branch push and on pull requests to
`main`. The release workflow calls it on the tagged commit. A newer push to a
pull request cancels its older run. A job over its time limit fails.

| Job | Limit | What it runs |
|-----|-------|--------------|
| Lint & Type Check | 12 min | `ruff check` and `ruff format --check` on `src/ tests/ scripts/`, `mypy src/lakebench/` (Python 3.11) |
| Test (3.10, 3.13) | 20 min | The unit tests in parallel, without `tests/spark` and the `slow` tests; one version failing does not cancel the other |
| Secret scan | | gitleaks over the working tree with `.gitleaks.toml` |
| Rust datagen | | `cargo fmt --check`, `cargo clippy --all-targets --locked -- -D warnings`, `cargo test --release --locked` in `datagen_rs/` (Rust 1.98.1) |
| Build Package | | After the four above: build the wheel and sdist, check `py.typed`, install the wheel, then `scripts/package_guard.py --dist dist` |

- The Spark tier, the `slow` AML statistics tests and the full-history
  gitleaks scan (`tests/test_gitleaks_history.py`) run locally before a push,
  not in CI.
- Jobs use a fixed runner image (`ubuntu-24.04`). Every action is pinned by
  commit SHA, with its tag as a trailing comment.

## Adding a recipe

A recipe is one valid architecture, `<catalog>-<format>-<engine>-<query_engine>`.
To add one:

1. Add the 4-tuple to `_SUPPORTED_COMBINATIONS` in
   `src/lakebench/config/schema.py`, and a reason to `_COMBINATION_NOTES` for
   each pairing known not to work.
2. Add its defaults to `RECIPES` in `src/lakebench/config/recipes.py`. One
   recipe per tuple. Several tests pin the counts; update them in the same
   change.
3. Add `examples/<recipe>.yaml`, checked by the example validation.
4. For a new component version, add its entries to the compatibility tables
   in `job.py`. Verify each Maven artifact by fetching its POM, not through
   the search API.
5. Regenerate the generated doc blocks with `python scripts/gen_docs.py`. It
   runs every generator in `scripts/gen_*.py` and the support-state and
   recipe-components blocks (`PYTHONPATH=src python -m lakebench.config.support .`).
   `--check` writes nothing and exits 1 on a stale block, as the drift tests
   do.
6. Leave `src/lakebench/config/validated_combinations.yaml` alone. Only live
   runs on the release tree fill it, so the new recipe starts as unverified.

A new Spark minor needs a live write-path run on each table format before it
is listed ([why](#versions-and-artifacts)).

## Adding a workload

[DESIGN.md](DESIGN.md#61-adding-a-workload) section 6.1 lists the five parts
a workload brings: a generator schema with a seed policy and dimensions,
stage scripts per mode, a correctness contract, a query set with its own
`query_set_id`, and a compatibility declaration. A change to synthetic data
also needs a reference detector and a leakage check
([review rules](../CONTRIBUTING.md#review)).

Its benchmark specification describes what the code does, section by section,
with a source pointer for each statement:

1. Purpose and scope: the business process modelled, what is measured about
   an architecture, and what is not claimed.
2. Data model: every table per layer, with grain, key, partitioning,
   approximate cardinality at scale 1 and 10, and the DDL source.
3. Data generation: generator identity, seed policy, scale to dimensions,
   delivery modes and the byte-identity guarantee, dirty data, time range,
   multi-cycle windows.
4. Pipeline: per mode, the ordered stages, each with inputs, the
   transformation, outputs and the invariants it enforces.
5. Correctness contract: what a correct run produces, and every check that
   fails a run that did not, saying which gate and which only report.
6. Query set: `query_set_id`, and per query the business question, the tables
   it reads and how its result is checked.
7. Execution rules: required stages, permitted tuning, prohibited changes, the
   Lakebench caps, and how a bound cap is reported.
8. Metrics: the primary metric per mode, then each published metric with unit,
   direction, definition, window and any cap it depends on.
9. Disclosure: the fields a published result carries.
10. Comparability: when two runs are comparable and when like-for-like.
11. Supported compositions, with support state and known exclusions.
12. Known limitations.

## Adding a query or a metric

- A query belongs to a workload's set in `src/lakebench/benchmark/queries.py`.
  Changing one changes the set's `query_set_id`, and QpH across ids is
  refused ([DESIGN.md](DESIGN.md#63-adding-a-query) section 6.3).
- A metric is defined once, with unit, direction and meaning, in the metric
  registry (`src/lakebench/metrics/metric_registry.py`, which the collector
  reads; [DESIGN.md](DESIGN.md#64-adding-a-metric) section 6.4).
- Changing what a published metric means is a product decision, not a
  refactor.

## Reproduction packages

- Record each package from a real run
  (`lakebench reproduce --record <run-id> --write <path>`), so its expected
  numbers cannot drift from a real `metrics.json`.
- Default bands are `DEFAULT_TOLERANCES` in `_reproduce.py` (performance 20%,
  correctness 0%).
- Each metric's band and direction come from `metrics/metric_registry.py`, not
  from the package. A malformed package cannot downgrade a correctness metric.
- A PR that changes a performance-affecting code path runs one reproduce
  against a package the change should not regress. It posts expected vs
  actual per metric, the drift percentage and pass / warn / fail.
- A PR that changes a performance number on purpose records a new package from
  the post-change `metrics.json`. The commit message names the old package as
  "supersedes"; do not just delete it.

## The datagen metrics line

Each datagen pod writes one line to stderr on completion. Example (Customer
360):

```
LB_METRICS_JSON {"schema":"customer360","node_id":0,"node_count":8,
  "cores_used":8,"cpu_request_millicores":8000,
  "bucket":"lb-bronze","prefix":"customer/interactions/",
  "target_tb":0.5,"customer_id_max":500000,"dirty_ratio":0.08,
  "file_size_mb":64,"rows_per_file":100000,"total_files":400,
  "files_written":50,"bytes_written":6553600000,"rows_written":5000000,
  "elapsed_s":100.4,"setup_s":0.4,"gen_s":100.0,
  "build_batch_s":440.0,"encode_parquet_s":320.0,"s3_put_s":40.0,
  "throughput_mbps":65.28,"cpu_seconds":803.2,"cpu_hr_per_tb":34.05}
```

- The `LB_METRICS_JSON ` prefix lets grep pull the line from the pod log
  without parsing. The rest is RFC 8259 JSON, so `json.loads` reads it.
- The line is under 1 KiB per pod and needs no extra pod resources. There is
  no flag: every datagen run emits it.
- `cores_used` is the rayon pool. The image entrypoint
  (`datagen_rs/entrypoint.py`) sizes it from `CPU_LIMIT`.
- `cpu_request_millicores` comes from the downward-API env var
  `LB_POD_CPU_REQUEST_MILLI`, set by the Job template. The aggregator prefers
  it when set.
- The aggregator reads each pod's log after the Job succeeds and folds the
  pods into a `FleetSummary` sidecar.
  `_load_latest_datagen_fleet` refuses a sidecar whose `namespace` field does
  not match the run.
- The datagen `StageMetrics` is built in
  `src/lakebench/metrics/collector.py:build_pipeline_benchmark`.
  `tests/test_metrics.py::TestBuildPipelineBenchmark` covers the empty-fleet
  gate.

## The AML pre-registration

The AML measurements are pre-registered
(`src/lakebench/spark/data/aml/aml_preregistration.json`, protocol in
[docs/internal/aml-protocol.md](internal/aml-protocol.md)).

- Its gated constants are locked at prereg 3.6.0. Changing one is a
  maintainer decision and needs a fresh evaluation seed.
- Generator and silver code are not frozen. A registered look, its
  calibration and its predictions name one datagen image by digest, and their
  results hold for that image.
- A change to AML generator output needs a new image and a `MODEL_VERSION`
  bump (`datagen_rs/src/model.rs`).

## Refactoring and shared cluster state

- Before moving code, map which modules import what from where. Each new
  module boundary tends to cost one circular-import fix.
- After moving a function, update every `mock.patch` target that names the
  old module. A re-export at the old path does not help: the patch replaces
  the name on the old module object, where the moved code no longer looks.
- A change that adds a cluster-scoped mutation (the Spark Operator's watch
  list, a Prometheus selector, any shared object) needs a removal path
  symmetric to the add. Both must be safe under concurrent deploys: take the
  `lakebench-cluster-lock` lease (`deploy/cluster_lock.py`) and
  read-modify-write under it.
- Destroying one deployment never touches another's namespace, buckets or
  shared entries. `deploy/ownership.py` holds the stamps destroy checks. The
  design record is `docs/internal/namespace-isolation.md`.

## Why it is built this way

What not to change without a live test. Each statement names the code or test
that holds it. Symptoms and fixes: [Troubleshooting](troubleshooting.md).
Sizing numbers: [Job Profiles](component-spark.md#job-profiles) and
[Sizing](sizing.md). `make rc-generated-docs` (`scripts/gen_docs.py --check`)
fails when the generated tables drift from the code.

### Spark job sizing

**Per-executor sizing is fixed, not autosized.**

- Driver and executor cores, memory, overhead and scratch are set per job type
  in `_JOB_PROFILES`.
- Per-workload changes are in `_SCHEMA_PROFILE_OVERRIDES` (the AML
  bronze-verify CTAS fallback and the continuous AML jobs), merged by
  `job.py:_resolve_job_profile`.
- Values were raised after live failures. Silver-build and gold-finalize
  scratch went up after "No space left on device" at scale 100. Silver and
  gold drivers went up after out-of-memory errors on Spark 4.
- Each entry's comment says why. Do not reduce one without a live run at the
  scale that set it.

**Why AML bronze-verify is larger.**

- `bronze_verify_financial.py` registers the pacs.008 source in place with
  Iceberg `add_files`.
- When `add_files` fails, or the source exceeds 1.5 TiB or 800,000 files
  (`LB_BRONZE_ADD_FILES_MAX_BYTES` / `_FILES`), it rewrites the full source
  through an Iceberg CTAS.
- The CTAS spills roughly twice the per-executor input to local disk.

**Executor counts scale with data, up to a cap.**

- `job.py:executor_count` keeps the base count at scale 10 and below, adds a
  per-job rate above it, and caps at the profile's `max_executors`.
- No cap is above `job.py:_MAX_EXECUTORS_SAFE`. At 32 or more executors the
  fabric8 Kubernetes client in the driver causes API polling storms.
- Every job sets the client by executor count (`job.py:kubernetes_client_conf`):
  - list executor pods from the API server cache
    (`enablePollingWithResourceVersion`);
  - poll less often above 20 executors;
  - request pods in bounded waves (`allocation.batch.size`, `maxPendingPods`).
- A continuous stage that needs more than its cap widens its executors to 8,
  then 16 cores (`streaming_shape`).
- Scale 1 and scale 10 have the same Spark peak (datagen and the always-on
  pods still grow). Data per executor grows with scale inside the flat and
  capped ranges.

**Cluster minimums come from `compute_peak_requirements()`, never from an
estimate.**

- `job.py:compute_peak_requirements` is the one Spark source.
- Batch jobs run one after another (`job.py:BATCH_JOB_TYPES`), so the batch
  peak is the largest single job. Continuous jobs run at once, so the
  continuous peak is their sum.
- Driver pods count Spark's own driver overhead, which the manifests do not
  set (`job.py:_driver_pod_bytes`).
- `src/lakebench/config/sizing.py:plan_requirements` adds datagen and the
  always-on pods. `config show`, `config recommend`, the `run` capacity
  preflight and the generated tables use it.
- Regenerate the tables with `python3.11 scripts/gen_sizing_tables.py`; never
  edit a number. `tests/test_sizing.py` holds hand-derived floors for the
  formula.

**`compute_guidance()` does not size Spark.**

- `src/lakebench/config/scale.py:compute_guidance` maps a scale to a tier.
- For Spark only the tier name is shown (`validate`, and the hidden `info`).
  Its executor and memory hints are not what the jobs request.
- Through `scale.py:full_compute_guidance` it feeds the default Trino and
  datagen sizes (`src/lakebench/config/autosizer.py:resolve_auto_sizing`).

### Spark Operator

**Volumes go through pod templates** (which volumes:
[component-spark.md](component-spark.md#spark-operator)).

- Spark's `spark.kubernetes.*.volumes.*` properties have no `configMap` type.
- Whether the operator's webhook would inject a ConfigMap volume declared on
  the SparkApplication has never been tested live. The template route stays
  until a live test shows it does.
- Code: `job.py:_build_manifest`,
  `src/lakebench/modules/pipeline_engines/spark/scripts_maps.py:scripts_volume`.

**`helm upgrade --reuse-values` does not backfill new chart values.**

- It carries forward only the keys the stored release has. A value a newer
  chart added renders empty, and the operator can crash-loop on it. The 2.4.0
  to 2.5.1 upgrade did, on an empty `--metrics-job-submit-latency-buckets`.
- Every `--reuse-values` upgrade that pins a version must set such values.
- `src/lakebench/modules/pipeline_engines/spark/operator.py:SparkOperatorManager`
  does this in `_reuse_values_backfill` (the latency buckets when a version is
  pinned, and the controller `/tmp` size) and in `_watch_list_pin` for
  watch-list upgrades.
- Helm's `--set` parser splits on unescaped commas, even when argv is a Python
  list. A comma inside a value is written `\,`
  (`_JOB_SUBMIT_LATENCY_BUCKETS_DEFAULT`).

### Versions and artifacts

**Hive 3.1.3 is deliberate.**

- The Stackable HiveCluster takes a `productVersion`, not an image. Lakebench
  renders `src/lakebench/config/schema.py:STACKABLE_HIVE_VERSION`; deploy and
  run provenance record it.
- Stackable resolves the image
  (`oci.stackable.tech/sdp/hive:<version>-stackable<sdp version>`).
- Do not move to Hive 4. Iceberg fails against it with `TApplicationException:
  Invalid method name: 'get_table'`
  ([apache/iceberg#12878](https://github.com/apache/iceberg/issues/12878)).
  Trino's repeated ANALYZE fails against metastore 4.0
  ([trinodb/trino#26214](https://github.com/trinodb/trino/issues/26214)).
- Revisit when both are fixed and a live Iceberg and Trino run passes on
  Hive 4.
- `images.hive` is a removed key for the same reason
  (`schema.py:ImagesConfig`).

**Polaris server and admin tool move together.**

- `images.polaris` and `images.polaris_admin_tool` publish the same version
  set and default to the same tag (`schema.py:ImagesConfig`).
- Polaris dropped the `-incubating` suffix at 1.4.0. Never filter a tag
  listing on `incubating`: it hides every release after 1.3.0.

**The Iceberg runtime artifact depends on both versions.**

- 1.11.0 added a native Spark 4.1 runtime. 1.10.x has none, so Spark 4.1 on
  1.10.x borrows the 4.0 jar.
- Choosing on the Spark version alone requests a missing artifact: a Maven
  resolution error in the driver.
- `job.py:iceberg_runtime_suffix_for` reads `_ICEBERG_NATIVE_RUNTIME_FROM`
  first and falls back to `_ICEBERG_RUNTIME_SUFFIX`.

**Iceberg 1.11 needs Java 17.** Its jars carry class-file major 61 (1.10.x
carried 55), and the plain Spark 3.5 images ship Java 11.

- No version chosen: `schema.py:resolve_format_versions` falls back to
  Iceberg 1.10.1 with a warning.
- Version chosen: `job.py:validate_iceberg_java_runtime` refuses the pairing
  at load, before it can fail as `UnsupportedClassVersionError` in the driver.

**Spark 4.2 is excluded on purpose.**

- `job.py:_SUPPORTED_SPARK_VERSIONS` has no 4.2 entry.
- No Iceberg release ships a 4.2 runtime (as of 2026-09). The borrowed 4.1
  runtime throws `IncompatibleClassChangeError` loading Iceberg's `SparkView`
  at the first table write.
- Adding 4.2 needs a released `iceberg-spark-runtime-4.2_2.13` on Maven
  Central and a 4.2 entry in the runtime-suffix tables.
- It also needs a live write-path run through the whole pipeline, on each
  table format. A runtime built for another Spark line can load and then fail
  on a Spark-internal API at write time. Release notes, artifact metadata and
  a version-bump smoke test do not show this.

**Delta 4.1 renamed its artifact.**

- Delta 4.0.x publishes `delta-spark_2.13`. 4.1.0 and later put the Spark
  minor in the name, `delta-spark_4.1_2.13`.
- `job.py:_delta_spark_artifact` builds both forms.
- Check an artifact by fetching its POM, not with the Maven search API, which
  misses some
  ([Troubleshooting](troubleshooting.md#a-maven-artifact-seems-not-to-exist)).

**`table_format.delta.version` defaults to `auto`.**

- `schema.py:DeltaConfig` declares it. `job.py:resolve_format_version`
  resolves it to the Delta built for the Spark image
  (`_FORMAT_VERSION_DEFAULTS`).
- `_FORMAT_VERSION_COMPAT` refuses an explicit version built for another
  Spark minor.
- A config that names no Delta version keeps working when the Spark image
  changes minor.

### Catalogs and PostgreSQL

- **The Polaris admin tool is a jar.** The admin-tool image has no `polaris`
  binary. The bootstrap Job runs
  `java -jar /deployments/polaris-admin-tool.jar bootstrap ...`
  (`src/lakebench/templates/polaris/bootstrap-job.yaml.j2`).
- **Bootstrap is safe to repeat.** Polaris 1.3.0's admin tool threw `already
  been bootstrapped` on a second run (1.6.0 not checked live); the Job's
  script treats it as success. Jobs are immutable, so the deployer deletes
  the old Job first
  (`src/lakebench/modules/catalogs/polaris/deployer.py:PolarisDeployer`,
  `_delete_old_bootstrap_job`).
- **PostgreSQL DDL through `kubectl exec` checks before it creates.** There
  is no psql `\gexec`, and PostgreSQL refuses `CREATE DATABASE` inside a `DO`
  block. So the Polaris deployer queries for its role and database and
  creates what is missing (`_create_polaris_db`).
  - It sets the password from a SCRAM verifier, not plaintext
    (`src/lakebench/deploy/deployment_secrets.py:scram_sha256_verifier`).
- **Delta with Hive uses the session catalog.** `DeltaCatalog` is a catalog
  extension, so it replaces `spark_catalog`, and tables are
  `spark_catalog.<schema>.<table>`. Set in `job.py:_build_manifest`, passed
  as `LB_ICEBERG_CATALOG=spark_catalog`, read by
  `src/lakebench/spark/scripts/common.py:pipeline_catalog`.
- **The Delta scripts avoid REPLACE TABLE AS SELECT** (RTAS), which Unity's
  `UCSingleCatalog` 0.4.0 lacks. They append to create a table and overwrite
  only an existing one
  (`src/lakebench/spark/scripts/gold_finalize_delta.py:_safe_write_mode`,
  `src/lakebench/spark/scripts/silver_build_delta.py:_table_exists`).
  - Unity 0.4.1 claims a fix, but no Unity combination is in
    `schema.py:_SUPPORTED_COMBINATIONS`.
  - The workaround stays until a Unity recipe is supported and RTAS is proven
    live on it.

### Storage conformance

**`config storage` reports; it does not gate.** S3 implementations differ in
ways mocked tests cannot see, so `lakebench config storage <config>` runs
graded checks against the real backend. How to read them:
[Storage Backends](storage-backends.md).

- Runner and known-backend list:
  `src/lakebench/s3/conformance.py:ConformanceRunner` and `KNOWN_BACKENDS`.
  No test covers them.
- When a check changes, keep them, the command in
  `src/lakebench/cli/_config.py` and [Storage Backends](storage-backends.md)
  in step.

### Table maintenance

**Iceberg expiry and orphan removal are built per engine.**

- `src/lakebench/modules/table_formats/iceberg/maintenance.py:build_maintenance_sql`
  builds the statements.
- `maintenance.py:exec_sql` runs each through `kubectl exec` on the Trino
  coordinator or the Spark Thrift pod. It raises on a non-zero exit, and
  raises `ExecSqlTimeout` when the client times out (the statement may still
  be running in the engine).
- DuckDB is read-only for Iceberg and runs none.
- **Trino** refuses a retention under its system minimum unless the session
  property is set in the same CLI process. So each statement is one
  `--execute` string: `SET SESSION <catalog>.expire_snapshots_min_retention =
  '...'; ALTER TABLE ... EXECUTE expire_snapshots(...)`. The same holds for
  `remove_orphan_files`.
- **Spark** takes `CALL <catalog>.system.expire_snapshots(...)` with a
  `TIMESTAMP` literal. A computed numeric expression fails argument binding on
  Spark 4.

**Two retention floors.**

- Orphan removal never uses a retention below
  `maintenance.py:ORPHAN_MIN_RETENTION_SECONDS` (24 h 10 min). Removing
  orphans beside a live writer can delete files a commit has not yet
  referenced. The builder enforces it whatever the caller passes.
- While streams are live, snapshot expiry is floored at
  `maintenance.py:LIVE_EXPIRE_MIN_RETENTION_SECONDS`, so a stream that falls
  behind keeps the snapshots it still needs.
- The builder does not know about live streams. That floor, and Delta's 7-day
  floor while streams are live, is applied by
  `src/lakebench/cli/_sustained.py:applied_retentions`. The run records its
  output. A new call site must take its retentions from it.
- `retention_threshold` must be a whole number and one unit (`30m`, `1h`,
  `7d`), checked at load (`schema.py:SustainedConfig`).

**Trino compaction is chunked by partition.**

- `maintenance.py:build_compaction_plan` splits one `optimize` into several
  for the tables in `maintenance.py:_COMPACTION_PARTITIONING`.
- It runs after `_sustained.py:_compaction_partitions` reads the table's
  partitions (`$partitions` for identity, `$files` for months).
- Each partition an `optimize` rewrites keeps open Parquet writers, so one
  statement is bounded by partitions, not rows.
- **Customer 360 silver** (identity on `interaction_date`): at most 90
  partitions per statement (`maintenance.py:COMPACTION_CHUNK_PARTITIONS`),
  below Trino's limit of 100 open writers.
- **AML silver** `transactions` and `account_statements` (`months()` on
  `txn_timestamp` and `book_ts`): at most one month with files to merge per
  statement (`maintenance.py:COMPACTION_CHUNK_MONTHS`).
  - Why: a writer buffers up to a row group (128 MB) per partition. One
    statement over 12 to 13 months of small continuous files exceeded the
    2.24 GB per-node query memory on the single scale-1 Trino worker. That is
    about 160 MB a month.
  - Only months optimize rewrites count. Trino 483 drops a data file above
    the threshold. It then skips a partition's only remaining file when that
    file has no deletes.
  - So the read counts, from `$files`, the data files at or under the
    threshold per month. A month with fewer than two shares its neighbour's
    statement.
  - A NULL month (a file under the pre-1.6 `days()` spec, or a NULL
    timestamp) makes Trino rewrite every file it keeps, so every listed month
    counts.
  - Sized at scale 1. Larger workers may spread one month over more local
    writers; not measured.

Each chunk's `WHERE` (statement form:
[Table maintenance](benchmarking/maintenance.md)):

- Trino accepts `optimize ... WHERE` only when the connector applies the whole
  predicate to partitions. Anything else fails with "Unexpected FilterNode
  found in plan; probably connector was not able to handle provided WHERE
  expression".
- For `months()` that means a source-column range with month-start bounds
  (`IcebergUtil.canEnforceRangeWithPartitioningField` in Trino 483), written
  `TIMESTAMP 'YYYY-MM-01 00:00:00.000000 UTC'`.
- The month transform of a `timestamp with time zone` is taken in UTC. An
  explicit UTC literal ignores the session time zone.
- Chunks are contiguous, with open-ended first and last. A partition written
  after the read, or missing from it, is compacted by exactly one statement.
- A failed partition read falls back to one statement, named in the record.
- Trino settings are unchanged. Each chunk commits one snapshot. The record
  names the operation (`trino_optimize` with its threshold).

**Where maintenance runs.**

- Continuous: `_sustained.py:_run_iceberg_maintenance` runs a round every
  `retention_interval` seconds. Unset, it is a third of the window, clamped to
  the field's range (`SustainedConfig.effective_retention_interval`).
- A failing table is logged, and the round and run go on.
- Before a batch benchmark, `run` runs compaction and maintenance under one
  shared budget (`src/lakebench/cli/_run.py:PRE_BENCHMARK_MAINTENANCE_CAP`).
- A round that stops on that budget, or runs beside live streams, is recorded
  (`maintenance_stopped`, `maintenance_live_streams`). Its post-maintenance QpH
  is not a measurement.
- Destroy runs no maintenance.
- Delta maintenance:
  [Troubleshooting](troubleshooting.md#delta-no-compaction-and-vacuum-only-on-trino).

### Metrics and output

- **Requested resources, not usage.** Per-stage executor counts, cores and
  memory in the record are what the profiles requested, and so are the
  core-hour and efficiency figures built from them.
  - A job too fast for the progress callback to see its executors takes the
    count from the per-job executor override, else from the profile and the
    scale (`src/lakebench/cli/_run.py`, using `job.py:get_executor_count`).
- **Size fallbacks.** A job that reports no output size gets the measured S3
  size of its layer. When gold's input size is zero, the measured silver size
  is used. Both are in `collector.py:build_pipeline_benchmark`.
- **The continuous mode is stored as `sustained`.** Records keep
  `pipeline_mode: "sustained"` so older records still read.
  `schema.py:is_continuous_mode` accepts both spellings. New code uses it
  rather than compare with `"sustained"`.
- **One output root.** Journals, run records and reports derive their paths
  from `src/lakebench/_constants.py:DEFAULT_OUTPUT_DIR`, so moving the output
  means changing one constant. The layout is in
  [Operations](operations.md#where-the-output-goes).

## Further reading

- [Architecture](architecture.md): system layers, deploy order and the Spark
  executor profiles.
- [Operators and catalogs](recipes.md#choosing-a-catalog): the Spark Operator and
  the metastores in depth.
- [Releasing](../RELEASING.md): how a release is cut.
