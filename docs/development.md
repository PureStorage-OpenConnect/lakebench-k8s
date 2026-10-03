# Development Guide

This is the maintainer's map of the code: where each part lives, how the
test tiers and CI are wired, and what to touch when you add a recipe, a
workload, a query or a metric. Setup, the test tiers a change needs, the
pull request flow and the style rules are in
[CONTRIBUTING.md](../CONTRIBUTING.md); the product model and its extension
rules are in [DESIGN.md](DESIGN.md).

## Architecture map

The source code lives under `src/lakebench/`. Each subdirectory is a
self-contained module with one responsibility:

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
  modules/      Component implementations (v1.3 modular architecture):
    catalogs/       hive/, polaris/, unity/ -- catalog deployers
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

- `tests/` -- the pytest suite, one file per module or behaviour;
  `tests/spark/` holds the PySpark tier
- `datagen_rs/` -- the Rust data generator and its container image
  (Parquet files to S3)
- `examples/` -- example configs, at least one per recipe
- `scripts/` -- the release gate, the doc generators and the CI checks
- `docs/` -- the project documentation

### Where things live

| What | Where |
|---|---|
| Deploy order | `deploy/engine.py`; the order is described in [architecture.md](architecture.md#deployment-order) |
| Spark per-job sizing | `_JOB_PROFILES` and `_SCHEMA_PROFILE_OVERRIDES` in `src/lakebench/modules/pipeline_engines/spark/job.py`; the docs quote these, never the other way round |
| Cluster minimums | `compute_peak_requirements()` in the same file, and `config/sizing.py`, which `info`, `recommend` and the capacity check read |
| Version compatibility tables | `_FORMAT_VERSION_COMPAT`, `_ICEBERG_RUNTIME_SUFFIX` and `_HADOOP_AWS_COMPAT` in `job.py` |
| Valid architectures | `_SUPPORTED_COMBINATIONS` and `_COMBINATION_NOTES` in `src/lakebench/config/schema.py` |
| Recipes | `src/lakebench/config/recipes.py` |
| Support record | `src/lakebench/config/validated_combinations.yaml` and `config/support.py` |
| Queries | `src/lakebench/benchmark/queries.py` (the QpH sets), `benchmark/aml_queries.py` and the SQL under `benchmark/queries/aml/` (the AML score queries) |
| Metric definitions | `src/lakebench/metrics/metric_registry.py`, which the collector's score descriptions read |
| Kubernetes manifests | the Jinja2 templates in `src/lakebench/templates/`; Spark jobs are built in code in `job.py` |

### Key conventions

- **Configuration** uses Pydantic v2 models (`config/schema.py`). All user-facing
  config flows through a single YAML file parsed by `config/loader.py`.
- **Deployment** follows a component-based architecture. Each deployer
  implements deploy and destroy, and the engine (`deploy/engine.py`)
  orchestrates them in order. Every resource a deployment creates carries
  the ownership stamps of `deploy/ownership.py`.
- **Spark scripts** in `spark/scripts/` are standalone PySpark programs that run
  in the driver and executor pods. They are submitted through the Spark Operator.
- **All outputs** (journals, metrics, reports) go to `lakebench-output/`.

## Environment

Install in editable mode with the development dependencies (`make dev` does
this and installs the pre-commit hooks):

```bash
pip install -e ".[dev]"
```

This installs:

- **Runtime dependencies:** Typer, Pydantic v2, Rich, boto3, kubernetes, httpx,
  Jinja2, PyYAML
- **Dev dependencies:** pytest, pytest-cov, pytest-xdist, ruff, mypy, moto
  (AWS mocking), pre-commit
- **The `[aml]` extra:** numpy, scipy, pandas, scikit-learn, joblib and
  threadpoolctl at exactly the versions the cluster's AML reference detector
  installs (`REFERENCE_PY_DEPS` in
  `src/lakebench/modules/pipeline_engines/spark/job.py`), plus pyarrow.
  `tests/test_reference_pins.py` fails in CI if the extra and the tuple
  drift, or if the installed versions differ from them. Outside CI it reports
  a drifted install as a skip; set `LB_REQUIRE_REFERENCE_PINS=1` to make it
  fail. The pins have wheels for Python 3.10 to 3.13.

### Running tests

```bash
make check-fast
```

That is ruff, the format check and mypy, then the unit tests in parallel
(`pytest-xdist`, one test file per worker) without the Spark tier and
without the AML statistics tests marked `slow`, which CI runs in their own
job. The tests import `src/` of this checkout, whatever is installed:
`pyproject.toml` sets pytest's `pythonpath = ["src"]`, so a bare `pytest`
does too, and `make check-fast` and `make test` also set `PYTHONPATH=src`
for the subprocesses some tests start. It runs one worker per CPU;
`make check-fast XDIST_WORKERS=8` caps the workers on a shared machine, and
`PYTHON=python3.11` picks the interpreter (default `python3`). `make test`
is the same pytest command without the lint and type checks. To run the
whole unit suite serially, slow tests included, and stop at the first
failure:

```bash
pytest tests/ -x -v --ignore=tests/test_e2e.py --ignore=tests/test_integration.py
```

End-to-end and integration tests require a live Kubernetes cluster with S3
storage, a Spark Operator, and a Hive metastore. These are excluded from the
default test run. To run them when you have a cluster available:

```bash
pytest tests/ -v -m "integration"    # Integration tests (K8s + S3)
pytest tests/ -v -m "e2e"            # Full deploy/run/destroy workflow
```

Tests are organized with pytest markers defined in `pyproject.toml`:

| Marker | Description | Requires Cluster |
|--------|-------------|------------------|
| `unit` | Fast, isolated tests with no external dependencies | No |
| `integration` | Require Kubernetes and S3 connectivity | Yes |
| `e2e` | Full deploy/run/destroy workflow | Yes |
| `slow` | Long-running tests | Varies |
| `extended` | Scale matrix tests (scales 1, 10, 50, 100) | Yes |
| `stress` | Stress tests at large scales (250, 500, 1000) | Yes |

A coverage report: `make test-cov`, written to `htmlcov/index.html`.

### Makefile targets

`make help` lists the common targets; the table has every one.

| Target | Description |
|--------|-------------|
| `make install` | Install the package in editable mode, without the dev extra |
| `make dev` | Install with the dev extra and set up the pre-commit hooks |
| `make check-fast` | Ruff and the format check on `src/`, `tests/` and `scripts/`, mypy on `src/lakebench/`, then the unit tests in parallel without `tests/spark` and the `slow` tests (CI's Lint job runs it, budget 8 minutes) |
| `make test` | The pytest command of `make check-fast` alone |
| `make test-spark` | The Spark tier on the installed pyspark, with its pinned jars fetched and `LB_REQUIRE_JARS=1` |
| `make test-unit` | Every test not marked `integration` or `e2e`, serially, `slow` tests included |
| `make test-integration` | Integration tests (requires K8s and S3) |
| `make test-e2e` | End-to-end tests (full workflow) |
| `make test-extended` | Scale matrix tests (scales 1, 10, 50, 100) |
| `make test-stress` | Stress tests at large scales (250, 500, 1000) |
| `make test-cov` | `pytest tests/` with the default marker filter of `pyproject.toml` (no `e2e` or `integration`) and a coverage report; not in `make help` |
| `make lint` | Ruff on `src/`, `tests/` and `scripts/`, as CI runs it |
| `make fmt` | Format `src/`, `tests/` and `scripts/` with ruff and apply auto-fixes |
| `make typecheck` | Mypy on `src/lakebench/` |
| `make clean` | Remove build artifacts, caches, and `__pycache__` directories |

Ruff's configuration in `pyproject.toml` targets Python 3.10 with a
100-character line and the pycodestyle, pyflakes, isort, bugbear,
comprehensions and pyupgrade rule sets. Mypy allows untyped functions; CI
runs `mypy src/lakebench/` and it must pass.

### Pre-commit hooks

`pre-commit install` (part of `make dev`) registers the hooks in
`.pre-commit-config.yaml`, which run on every `git commit`:

1. **gitleaks** -- Fails the commit on anything that looks like a credential.
2. **Ruff lint** -- Checks `src/` and `tests/` and applies auto-fixes where
   possible.
3. **Ruff format** -- Enforces consistent formatting on `src/` and `tests/`.
4. **cargo fmt** -- Checks formatting of the Rust datagen when a `.rs` file
   changes.

The hooks do not run the tests, and the ruff hooks skip `scripts/`: run
`make check-fast` before pushing. `pre-commit run --all-files` runs every
hook over the whole tree.

### Pre-push hook

`scripts/hooks/pre-push` refuses a push whose branch reaches a commit from
before the 2026-09-30 history rewrite, and scans the pushed commits and
their messages with gitleaks using the `.gitleaks.toml` and `.gitleaksignore`
from `origin/integrate/v1.5.0` rather than the branch's own copies. It also
scans what merge commits change and ignores inline `gitleaks:allow`
comments. It refuses the push when git cannot run `--remerge-diff` (git
older than 2.36) or when gitleaks logs an error, because gitleaks exits 0
when the git log it runs fails and has then scanned nothing; a relative
`GIT_DIR` (`git --git-dir=.git push`) is made absolute first. Install it by copying it, not through
pre-commit (the pre-commit framework would read the pushing branch's config,
which an old branch may lack):

```bash
cp scripts/hooks/pre-push "$(git rev-parse --git-common-dir)/hooks/pre-push"
chmod 0755 "$(git rev-parse --git-common-dir)/hooks/pre-push"
```

Run this from the root of a checkout whose `scripts/hooks/pre-push` is the
copy you want: every worktree of a clone shares the installed hook. It needs
`gitleaks` on `PATH`. Do not bypass it with `git push --no-verify`.
`tests/test_pre_push_hook.py` exercises it, and the release gate's
`pre-push-hook` check fails if the installed copy differs from the tracked
one.


### Maintainer checks

- `python scripts/check_doc_readers.py` (also in CI) fails on a tracked doc
  or script that nothing links or reads; `--resolve-paths FILE` checks that
  every path a local notes file names exists.
- `python scripts/check_doc_overlap.py FILE` lists each paragraph of a
  local, untracked instructions file that repeats a tracked doc, and each
  line that repeats a fact (a number with a unit, a backticked identifier
  with a digit) without pointing at a doc that states it. `CHANGELOG.md` is
  not in its corpus. It exits 1 on any report and 2 when it cannot run;
  rewrite a reported line as a pointer. Its unit tests run in CI on planted
  files.
- `python scripts/prose_guard.py` fails on an em dash, an emoji or an AI
  attribution line in any tracked file (`tests/test_prose_style.py` runs it
  in the unit tier, and the release gate's `prose` check runs it too). A
  hit that has to stay goes in `scripts/prose_allowlist.txt` as
  `path:kind:key  # reason`, with the key the hit prints (a hash of the
  line, so the entry survives edits elsewhere in the file); an entry whose
  hit is gone fails the guard.

## The Spark tier

### Testing the Spark scripts

The scripts in `src/lakebench/spark/scripts/` import each other by plain
name (`from common import ...`), as they do in the driver pod. Tests load
them through the `load_script` fixture in `tests/conftest.py`, which gives
each test a private copy of every script it imports, `common` included:

```python
def test_gate(load_script):
    sbf, common = load_script("silver_build_financial", extra=("common",))
```

A module whose tests import scripts inside the test body can mark itself
with `pytestmark = pytest.mark.usefixtures("load_script")`; while a test
runs, `import common` resolves to that test's copy. `load_script_module`
keeps one copy per test module, for module-scoped fixtures that run script
code. Do not put the scripts directory on `sys.path` or pop script modules
out of `sys.modules`: `tests/test_script_loader_static.py` fails on it, and
a teardown hook in `tests/conftest.py` fails the test that leaves the
scripts directory on `sys.path` or a script module in `sys.modules`.

### The Spark tier and its jars

`pytest tests/spark` needs pyspark (4.0.1 or 4.1.1), pyarrow and a Java 17
runtime. A test that imports pyspark, Delta or py4j, or calls
`pytest.importorskip` on one, goes under `tests/spark`: the unit legs in CI
have no pyspark and would skip it, so `tests/test_pyspark_tests_in_spark_tier.py`
fails on such a test anywhere else under `tests/`. Tests that need Iceberg or Delta read the jars from
`LB_SPARK_TEST_JARS`, a comma-separated list of jar files for the installed
Spark line:

| Line | Jars |
|------|------|
| pyspark 4.0.1 | `iceberg-spark-runtime-4.0_2.13-1.11.0.jar`, `delta-spark_2.13-4.0.0.jar`, `delta-storage-4.0.0.jar` |
| pyspark 4.1.1 | `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, `delta-spark_4.1_2.13-4.1.0.jar`, `delta-storage-4.1.0.jar` |

These are the product defaults in `modules/pipeline_engines/spark/job.py`;
`tests/test_spark_jars_lock.py` fails when the lock and the defaults differ.
After a default changes, `python scripts/fetch_test_jars.py --update-lock`
rebuilds the lock: it confirms each coordinate by its POM, takes
`delta-storage` from the delta-spark POM, and checks the bytes against
Central's `.sha1` before writing the sha256 values.
The harness (`tests/spark/conftest.py`) checks each jar against the
installed line with the product's own rules, so a jar built for the other
line, a missing file or a directory fails the tests that need jars instead
of skipping them. There is no ivy-cache fallback, and `LB_TEST_ICEBERG_JAR`
is still read but deprecated.

A test declares what it needs with `@pytest.mark.requires_jars("iceberg")`,
`("delta")` or both. Without the jars it skips with a reason starting
`LB-JARS missing:`; with `LB_REQUIRE_JARS=1` it fails instead, and the run
refuses to start if pyspark itself cannot be imported. Under
`LB_REQUIRE_JARS=1` the run also fails on any skip whose reason mentions
jars, Iceberg or Delta, on any xfail whose reason mentions jars, and on any
other skip not listed in `tests/spark/skip_allowance.txt` (which is empty):

`scripts/fetch_test_jars.py` downloads the jars pinned for a line in
`tests/spark/jars.lock.json` (Maven Central, with the Google mirror as the
fallback), checks each against its sha256 and caches them in
`~/.cache/lakebench-test-jars`; `--print-env` prints the variable:

```bash
make test-spark                      # the fetch below, then the tier with LB_REQUIRE_JARS=1
jars=$(python scripts/fetch_test_jars.py --leg auto --print-env) && export "$jars"
LB_REQUIRE_JARS=1 pytest tests/spark -q -rs
pytest tests/spark -q --lb-reverse   # the same tests in reverse order
pytest tests/spark -q --lb-shard 1/2 # the test files in shard 1 of 2, as one CI job runs
```

Two fixtures give a test a Spark with the jars. The JVM starts once per
pytest process with the jars and no session settings, and `spark_session`
is one session per test module on it, stopped at the module's end. It is shaped like the
product's session for the formats the module's tests declare: the Iceberg
or Delta SQL extension, and Delta as `spark_catalog` for Delta. A module
adds static settings with `pytestmark = pytest.mark.spark_static_conf({...})`
and Iceberg catalogs with the `iceberg_catalog` fixture. A test that needs
a fresh JVM runs a child with `spark_subprocess(script, *args)`, which
passes the jars, `PYSPARK_PYTHON` and a `PYTHONPATH` holding the Spark
scripts and `tests/spark`; on a non-zero exit or a timeout it writes the
whole output to `spark-subprocess.log` under pytest's temporary directory
and shows the last 60 lines plus every `Caused by:` line.
To run such a child by hand, give it the same path:
`PYTHONPATH=src/lakebench/spark/scripts:tests/spark:src python tests/spark/test_x.py ...`.
A test that calls a stream's micro-batch handler directly wraps the call in
`foreach_batch_harness(spark, handler, df, batch_id)` from
`tests/spark/_foreach_batch.py`, which sets the local properties a real
`foreachBatch` sets (the streaming query id the writers read) and restores
them afterwards; with several writer threads, each thread enters
`inside_foreach_batch` itself.
The four AML batch/stream protocol guards (statements, profiles,
dimensions, replay idempotency) have a mutation check,
`tests/spark/test_parity_guard_mutations.py`: it reruns each guard's Spark
child with `LB_PARITY_MUTATE` set, which makes `tests/spark/_parity_mutation.py`
null one business column in that MERGE's source, and asserts the guard
names the failure. A new guard child calls `_parity_mutation.install()`
before it imports the stream script.
`--lb-reverse` and `--lb-shard` are defined in `tests/spark/conftest.py`,
so pass them with `tests/spark` (or a file in it) on the command line.
`--lb-shard K/N` keeps the test files in shard K of N and deselects the
rest. Every file is in exactly one shard, and the split depends only on the
files under `tests/spark` and the recorded seconds per file in
`tests/spark/shard_weights.json` (heaviest file first, each to the shard
with the least time so far; a file with no recorded time weighs the
median). With `--lb-reverse` a shard runs its own tests backwards, so the
order check pairs a file only with the files of its own shard. The
weights only balance the shards, so a new test file needs no entry; refresh
them from the CI jobs' `spark-junit-*` artifacts with
`python scripts/spark_shard_weights.py --source "<run id>" <reports>` when
the shard times drift apart.

## Adding a recipe

A recipe is one valid architecture, `<catalog>-<format>-<engine>-<query_engine>`.
To add one:

1. Add the 4-tuple to `_SUPPORTED_COMBINATIONS` in
   `src/lakebench/config/schema.py`, and a reason to `_COMBINATION_NOTES`
   for each pairing known not to work.
2. Add its defaults to `RECIPES` in `src/lakebench/config/recipes.py`. One
   recipe per tuple; several tests pin the counts, so update them in the
   same change.
3. Add `examples/<recipe>.yaml`, checked by the example validation.
4. For a new component version, add its entries to the compatibility tables
   in `job.py`. Verify each Maven artifact by fetching its POM, not through
   the search API.
5. Regenerate the generated doc blocks:
   `PYTHONPATH=src python -m lakebench.config.support .` for the support
   states and recipe components tables in the README and the docs, and
   `python scripts/gen_config_reference.py` and
   `python scripts/gen_sizing_tables.py` where the change moves their
   inputs. The drift tests fail on a stale block.
6. Leave `src/lakebench/config/validated_combinations.yaml` alone: it is
   filled only from live runs on the release tree, so the new recipe starts
   as unverified.

A new Spark minor version needs a live write-path run on each table format
before it is listed: an Iceberg or Delta runtime built for another Spark
line can load and then fail on a Spark-internal API at write time, which
neither the release notes nor the artifact metadata show.

## Adding a workload

[DESIGN.md](DESIGN.md#61-adding-a-workload) section 6.1 lists the five
parts a workload brings: a generator schema with a seed policy and
dimensions, stage scripts per mode, a correctness contract, a query set
with its own `query_set_id`, and a compatibility declaration.

Its benchmark specification describes what the code does, section by
section, with a source pointer for each statement:

1. Purpose and scope: the business process modelled, what is measured
   about an architecture, and what is not claimed.
2. Data model: every table per layer, with grain, key, partitioning and
   approximate cardinality at scale 1 and 10, and the DDL source.
3. Data generation: generator identity, seed policy, scale to dimensions,
   delivery modes and the byte-identity guarantee, dirty data, time range,
   multi-cycle windows.
4. Pipeline: per mode, the ordered stages, each with inputs, the
   transformation, outputs and the invariants it enforces.
5. Correctness contract: what a correct run produces and every check that
   fails a run that did not, saying which gate and which only report.
6. Query set: `query_set_id`, and per query the business question, the
   tables it reads and how its result is checked.
7. Execution rules: required stages, permitted tuning, prohibited changes,
   and the Lakebench-imposed caps and how a bound cap is reported.
8. Metrics: the primary metric per mode, then each published metric with
   unit, direction, definition, window and any cap it depends on.
9. Disclosure: the fields a published result carries.
10. Comparability: when two runs are comparable, when like-for-like, and
    when `lakebench compare` refuses.
11. Supported compositions, with support state and known exclusions.
12. Known limitations.

A change to synthetic data runs a reference detector or scoring function
against the generated output and a leakage check (a planted signal is
found by the intended rule and absent otherwise); distribution bands alone
do not show that the data still means what it should.

## Adding a query or a metric

A query belongs to a workload's set in `src/lakebench/benchmark/queries.py`;
changing one changes the set's `query_set_id`, and QpH across ids is
refused ([DESIGN.md](DESIGN.md#63-adding-a-query) section 6.3). A metric
is defined once, with unit, direction and meaning, in the metric registry
(`src/lakebench/metrics/metric_registry.py`, which the collector reads;
[DESIGN.md](DESIGN.md#64-adding-a-metric) section 6.4). Changing what a
published metric means is a product decision, not a refactor.

### The HTML report: derived numbers and goldens

Every percentage, total and count the HTML report computes goes through
`src/lakebench/reports/derived.py`, which wraps the number in a span naming
the `metrics.json` paths it came from. `tests/test_report_consistency.py`
recomputes each one from the stored record and fails on any difference, on
a unit its own table contradicts (a stored percentage scaled as a fraction,
bytes shown as GiB without the conversion), on a percentage in the page text
that is neither derived nor a stored string, and on an inline percentage or
count format in `generator.py` or `scorecard.py`.

Six stored records render into `tests/fixtures/reports/<run>.html`, and the
test requires today's render to match them exactly. Because the renderer
produced them, the goldens only show that a page did not move; the
independent checks are the record recomputation and the phrases in
`tests/expected/report_goldens.json`, each computed by hand from the record.
There is no update flag. A change that moves the page asserts the number it
fixes in its own test, edits the affected phrases by hand, and re-renders the
goldens:

```bash
PYTHONPATH=src python3.11 -m tests.fixtures.report_goldens 5105a0
```

A second agent reviews the golden diff (`git show -- tests/fixtures/reports`)
against the record before the change is ready.

## The frozen AML scope

The AML measurements are pre-registered
(`src/lakebench/spark/data/aml/aml_preregistration.json`, protocol in
[docs/internal/aml-protocol.md](internal/aml-protocol.md)), so the code
that produces their inputs is frozen until the registered looks are spent:

- the data generator: `datagen_rs/src/`, `datagen_rs/Cargo.lock`, and the
  image inputs `datagen_rs/Cargo.toml`, `datagen_rs/Dockerfile` and
  `datagen_rs/entrypoint.py`;
- the bronze and silver financial scripts in `src/lakebench/spark/scripts/`
  (`bronze_ingest_financial.py`, `bronze_verify_financial.py`,
  `silver_build_financial.py`, `silver_stream_financial.py`);
- `aml_features.py`, `score_financial_reference.py`,
  `src/lakebench/aml/fidelity_gate.py` and
  `src/lakebench/aml/reference_score.py`;
- the pre-registration JSON, the silver DDL in `deploy/financial_ddl.py`
  and the `REFERENCE_PY_DEPS` pins;
- every symbol a frozen script imports from a module that is not frozen
  (in `common.py`, `detection_rules.py`, `tm_operations.py`,
  `config/datagen_seed.py` and others): an edit to one counts as an edit
  to a frozen file, while a new helper next to it does not.

At this commit the list is policy, held by review; no CI check enforces it
yet. A commit that changes a frozen file names its cost in a `Freeze-cost:`
trailer: `None`, `Parity proof` (the batch and stream parity guards on
both Spark lines plus their mutation check), `Rebuild` (an output-neutral
generator image with a byte-compare at a fixed seed), `Re-derive` (the
calibration and predictions are run again) or `Void` (a new evaluation
seed is drawn, which only the maintainers decide). Any change to the
generator's output after the freeze is `Void`.

## Refactoring and shared cluster state

- Before moving code, map which modules import what from where; each new
  module boundary tends to cost one circular-import fix.
- After moving a function, update every `mock.patch` target that names the
  old module. A re-export at the old path does not help: the patch replaces
  the name on the old module object, and the moved code no longer looks it
  up there, so the patch has no effect on the code under test.
- A change that adds a cluster-scoped mutation (the Spark Operator's watch
  list, a Prometheus selector, any shared object) needs a removal path
  symmetric to the add, and both must be safe under concurrent deploys:
  take the `lakebench-cluster-lock` lease (`deploy/cluster_lock.py`) and
  read-modify-write under it.
- Destroying one deployment never touches another's namespace, buckets or
  shared entries; `deploy/ownership.py` holds the stamps destroy checks.

## What CI Runs

`.github/workflows/ci.yml` runs on every branch push and on pull requests to
`main` and `integrate/**`. A newer push to the same `lane/*` branch or pull
request cancels the older run; on every other branch each pushed head's run
finishes. A tag push does not trigger this workflow; the release workflow
calls it instead. The lint job runs `ruff check src/ tests/ scripts/`,
`ruff format --check src/ tests/ scripts/` and `mypy src/lakebench/` on
Python 3.11, and `scripts/check_action_runtimes.py --verify`, which reads
each action's `action.yml` at its pinned SHA and fails if
`.github/action-runtimes.json` no longer matches it. It also runs
`scripts/check_doc_readers.py`, which fails on any tracked `.md` file, file
under `docs/` or `scripts/`, or top-level script that is neither linked from
`README.md` or `docs/README.md` nor read by code, tests or CI (a mention in a
comment or docstring does not count); link a new page from `docs/README.md`.
`tests/test_doc_links.py` checks that every relative link, `#anchor` and
backticked `src/`, `tests/`, `scripts/` or `datagen_rs/` path in the docs
resolves. `tests/test_citations.py` fails when a shipped file (under `src/`,
`docs/`, `scripts/`, `datagen_rs/` or `examples/`, or the README) gains a
bug id, an internal plan, requirement or work-item id, or a gotcha number,
none of which a reader of the package can look up; state the reason in
words instead. A change to `tests/fixtures/citation_counts.json` that raises
a count is a review blocker, except in the commit that lands the ratchet on
a merge-train tree (`python tests/test_citations.py` retakes it there). The
Lint job runs `make check-fast` on Python 3.11 under an 8-minute budget,
alongside the docs and other static checks: `scripts/ci_budget.py` fails the step when a command runs over its
budget, records the time in the step summary, and leaves the job's
`timeout-minutes` (1.5 times the budget) as a backstop. The test job runs
the same parallel unit tests (`-n auto --dist loadfile`, without
`tests/spark` and the `slow` tests, excluding `tests/test_e2e.py` and
`tests/test_integration.py`) on Python 3.10 and 3.13, the oldest and newest
supported versions, under a 15-minute budget. It runs to the end rather
than stopping at the first failure, prints every skip reason, and a failure
on one Python version does not cancel the other (`fail-fast: false`). On 3.13 it checks per-file
coverage floors with `scripts/check_coverage.py --suite unit` and keeps the
per-file report as the `coverage-unit` artifact for 30 days; floors are
raised from that report, never lowered. The Spark tier runs on two legs,
`pyspark==4.0.1` and `pyspark==4.1.1`, on Java 17, forward and with
`--lb-reverse`, and each of those four passes is split into two jobs with
`--lb-shard 1/2` and `2/2`: eight parallel jobs, each under a
52-minute budget. Every job fetches the jars pinned in
`tests/spark/jars.lock.json` with `scripts/fetch_test_jars.py`, runs with
`LB_REQUIRE_JARS=1` and keeps its JUnit report as a `spark-junit-*`
artifact. The two 4.0 forward jobs collect coverage, and the "Spark
coverage floors" job combines their data and checks the floors with
`scripts/check_coverage.py --suite spark`, keeping the report as the
`coverage-spark` artifact. A failing job uploads every
`spark-subprocess.log`. The "AML statistics (slow)" job runs
the tests marked `slow` (the heavy fidelity-gate fits, the scale invariance
check and the Spark fidelity gate over silver) on Python 3.11 with the pinned
`[aml]` libraries, on every push to `main`, `integrate/**`, `train/*` and
tags (through the release workflow's call) and on every pull request to
`integrate/**` or `main`; it is not run on other branch pushes, and the
unit legs and `make check-fast` deselect the `slow` tests. It also reruns the D8 power simulation in a second
environment with the numpy and scipy versions the pre-registration recorded
its output hash with, since that guard skips on the `[aml]` pins.
The Rust job runs `cargo fmt
--check`, `cargo clippy --all-targets --locked -- -D warnings` and `cargo test
--release --locked` in `datagen_rs/`. A gitleaks job scans the working tree
for credentials, and a second one (`scripts/gitleaks_history.py`, which the
release gate also runs) scans every commit reachable from the pushed or
merged head, including what merge commits change, plus every commit and tag
message, and ignores inline `gitleaks:allow` comments. It fails if gitleaks
scanned no commit, which is how gitleaks reports a `git log` it could not
run. Findings listed in `.gitleaksignore` (the history
baseline: two fingerprints of the old default Polaris secret that PyPI
1.0.0 to 1.4.0 published) are not reported. The scan takes the config and
the baseline from a trusted ref, not from the branch, so a branch cannot
allowlist its own finding. A push to `main`, a pull request to `main` and a
release tag trust `origin/main` first, then `origin/integrate/v1.5.0`;
every other branch trusts `origin/integrate/v1.5.0` first (the ref the
pre-push hook reads), then `origin/main`. A pull request to `main` uses its
own baseline, which the owner reviews, and CI prints its baseline and config
diff against `main`. The scan also runs with
the branch's own config, so a new rule applies at once.
`tests/test_gitleaks_baseline.py` pins the list. The package build runs
only after the lint, test, Spark, Rust and both secret-scan jobs pass; it
does not wait for the slow AML job, which most branches skip. After the
build it installs the pinned gitleaks and runs
`scripts/package_guard.py --dist dist`: no `docs/internal/`
or other maintainer-only member, no binary or link member, no key pattern
or gitleaks finding, and no held-out seed once
the hash file exists, in the wheel, the sdist and the script ConfigMaps
rendered from the wheel.

Every job runs on a fixed runner image (`ubuntu-24.04`, never
`ubuntu-latest`), and every action is pinned by commit SHA with its tag as
a trailing comment (`owner/repo@<sha> # vX.Y.Z`; `dtolnay/rust-toolchain`
has no release tags and names its branch and date instead). To change an action, pin
the new SHA, update its entry in `.github/action-runtimes.json` and run
`GITHUB_TOKEN=$(gh auth token) python scripts/check_action_runtimes.py
--verify`; `tests/test_workflows.py` fails on a floating runner label, an
unpinned or unmapped action, or a Node 20 or older runtime.

The "Contributor path" job runs the quick-path block of `CONTRIBUTING.md`
(between its `contributor-path:begin` and `contributor-path:end` markers)
as written, in a clean `python:3.11-bookworm` container with Java 17, make
and git. `scripts/contributor_path.py` extracts it, fails on a `make`
target the Makefile lacks, and replaces only the one `git clone` line with
a clone of this repository at the commit under test. It runs on every push to `main`, `integrate/**` and
`train/*` and on tags, and on any other push or pull request whose changes
touch `CONTRIBUTING.md`, the `Makefile`, `pyproject.toml`,
`.github/workflows/`, `.pre-commit-config.yaml`, the script itself,
`scripts/fetch_test_jars.py` or `tests/spark/jars.lock.json`; otherwise its
steps are skipped and the step summary says so, so it must not be a
required check. It is outside the fast-path budgets (the block includes
`make test-spark`, and nothing is cached), the package build does not wait
for it, and a release, which calls this workflow on the tag, does. A
command in the block that needs a cluster or a local file fails the job on
the pull request that adds it; `tests/test_contributor_path.py` also fails
in the unit tier on a `make` target the Makefile lacks.

## Further reading

- [Architecture](architecture.md) -- system layers, deploy order and the
  Spark executor profiles
- [DESIGN.md](DESIGN.md) -- the product model, invariants and extension rules
- [Operators and catalogs](operators-and-catalogs.md) -- the Spark Operator
  and the metastores in depth
- [Releasing](releasing.md) -- how a release is cut
