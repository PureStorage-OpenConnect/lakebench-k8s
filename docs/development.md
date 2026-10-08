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
  Keep the extra and the tuple equal by hand; no test checks them. The pins
  have wheels for Python 3.10 to 3.13.

### Running tests

```bash
make check-fast
```

That is ruff, the format check and mypy, then the unit tests in parallel
(`pytest-xdist`, one test file per worker) without the Spark tier and
without the AML statistics tests marked `slow`. CI does not run the `slow`
tests; run `pytest tests/ -m slow` (about 20 minutes) before a release. The tests import `src/` of this checkout, whatever is installed:
`pyproject.toml` sets pytest's `pythonpath = ["src"]`, so a bare `pytest`
does too, and `make check-fast` and `make test` also set `PYTHONPATH=src`
for the subprocesses some tests start. It runs one worker per CPU;
`make check-fast XDIST_WORKERS=8` caps the workers on a shared machine, and
`PYTHON=python3.11` picks the interpreter (default `python3`). `make test`
is the same pytest command without the lint and type checks. To run the
whole unit suite serially, slow tests included, and stop at the first
failure:

```bash
pytest tests/ -x -v --ignore=tests/spark
```

There are no cluster tests. `pyproject.toml` still registers the
`integration`, `e2e`, `extended` and `stress` markers, but no test carries
them. Cluster behaviour is checked by live runs. The one marker in use is
`slow`, for tests over 20 s.

A coverage report: `make test-cov`, written to `htmlcov/index.html`.

### Makefile targets

`make help` lists the common targets; the table has every one.

| Target | Description |
|--------|-------------|
| `make install` | Install the package in editable mode, without the dev extra |
| `make dev` | Install with the dev extra and set up the pre-commit hooks |
| `make check-fast` | Ruff and the format check on `src/`, `tests/` and `scripts/`, mypy on `src/lakebench/`, then the unit tests in parallel without `tests/spark` and the `slow` tests |
| `make test` | The pytest command of `make check-fast` alone |
| `make test-spark` | The Spark tier on the installed pyspark, with its pinned jars fetched. Read the skip count: a missing jar skips, it does not fail |
| `make test-unit` | The whole unit suite serially, `slow` tests included |
| `make test-integration`, `make test-e2e` | No tests carry these markers; they collect nothing |
| `make test-extended`, `make test-stress` | Broken: they name `tests/test_e2e.py`, which does not exist |
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

- `python scripts/prose_guard.py` fails on an em dash, an emoji or an AI
  attribution line in any tracked file (the release gate's `prose` check
  runs it). A
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
out of `sys.modules`: a teardown hook in `tests/conftest.py` fails the test
that leaves the scripts directory on `sys.path` or a script module in
`sys.modules`.

### The Spark tier and its jars

`pytest tests/spark` needs pyspark (4.0.1 or 4.1.1), pyarrow and Java 17.
CI does not run it; run it locally before a change to the Spark scripts
lands. Put a test that imports pyspark, Delta or py4j under `tests/spark`:
the unit suite has no pyspark and would skip it.

Iceberg and Delta tests read the jars from `LB_SPARK_TEST_JARS`, a
comma-separated list of jar files for the installed Spark line:

| Line | Jars |
|------|------|
| pyspark 4.0.1 | `iceberg-spark-runtime-4.0_2.13-1.11.0.jar`, `delta-spark_2.13-4.0.0.jar`, `delta-storage-4.0.0.jar` |
| pyspark 4.1.1 | `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, `delta-spark_4.1_2.13-4.1.0.jar`, `delta-storage-4.1.0.jar` |

These match the product defaults in `modules/pipeline_engines/spark/job.py`.
Keep `tests/spark/jars.lock.json` equal to them by hand; after a default
changes, `python scripts/fetch_test_jars.py --update-lock` rebuilds the
lock. It confirms each coordinate by its POM and checks the bytes against
Central's `.sha1`.

`scripts/fetch_test_jars.py` downloads the pinned jars, checks their
sha256 and caches them in `~/.cache/lakebench-test-jars`. `--print-env`
prints the variable:

```bash
make test-spark      # fetch, then the tier
jars=$(python scripts/fetch_test_jars.py --leg auto --print-env) && export "$jars"
pytest tests/spark -q -rs
```

A test declares the jars it needs with `@pytest.mark.requires_jars("iceberg")`,
`("delta")` or both. Without them it skips with a reason starting
`LB-JARS missing:` and the run still exits 0. **Read the skip count.**

Fixtures in `tests/spark/conftest.py`:

| Fixture or mark | What it gives |
|-----------------|---------------|
| `spark_session` | One local session per test module, stopped at the module's end. It carries the Iceberg or Delta extension the module's tests declare, and Delta as `spark_catalog` for Delta |
| `spark_static_conf({...})` | Module mark: static settings applied when the session starts |
| `iceberg_catalog(spark, name, warehouse)` | Registers a Hadoop Iceberg catalog on the session |
| `spark_jars` | The jar list, with `.classpath` for a child's `--jars` |
| `spark_subprocess(script, *args)` | Runs a child in a fresh JVM with the jars, `PYSPARK_PYTHON` and a `PYTHONPATH` of the Spark scripts, `tests/spark`, `src` and the repo root. On a non-zero exit it fails with the last 2000 characters of stdout and stderr |

To run such a child by hand, give it the same path:
`PYTHONPATH=src/lakebench/spark/scripts:tests/spark:src:. python tests/spark/test_x.py ...`.

A test that calls a stream's micro-batch handler directly wraps the call in
`foreach_batch_harness(spark, handler, df, batch_id)` from
`tests/spark/_foreach_batch.py`. It sets the local properties a real
`foreachBatch` sets and restores them afterwards. With several writer
threads, each thread enters `inside_foreach_batch` itself.

A batch/stream parity guard child can call `_parity_mutation.install()`
before it imports the stream script. With `LB_PARITY_MUTATE` set, it nulls
one business column in that MERGE's source, so the guard can be shown to
catch the difference.

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
5. Regenerate the generated doc blocks with `python scripts/gen_docs.py`,
   which runs every generator in `scripts/gen_*.py` and the support-state
   and recipe-components blocks
   (`PYTHONPATH=src python -m lakebench.config.support .`). `--check`
   writes nothing and exits 1 on a stale block, as the drift tests do.
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
10. Comparability: when two runs are comparable and when like-for-like.
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
bytes shown as GiB without the conversion), and on a percentage in the page
text that is neither derived nor a stored string.

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

## The AML pre-registration

The AML measurements are pre-registered
(`src/lakebench/spark/data/aml/aml_preregistration.json`, protocol in
[docs/internal/aml-protocol.md](internal/aml-protocol.md)). Its gated
constants are locked at prereg 3.6.0; changing one is a maintainer decision
and needs a fresh evaluation seed. Generator and silver code are not frozen:
a registered look, its calibration and its predictions name one datagen
image by digest, and their results hold for that image. A change to AML
generator output needs a new image and a `MODEL_VERSION` bump
(`datagen_rs/src/model.rs`).

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
`main`; the release workflow calls it on the tagged commit. A newer push to a
pull request cancels its older run.

| Job | What it runs |
|-----|--------------|
| Lint & Type Check | `ruff check` and `ruff format --check` on `src/ tests/ scripts/`, `mypy src/lakebench/` (Python 3.11) |
| Test (3.10, 3.13) | the unit tests in parallel, without `tests/spark` and the `slow` tests; one version failing does not cancel the other |
| Secret scan | gitleaks over the working tree with `.gitleaks.toml` |
| Rust datagen | `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test --release` in `datagen_rs/` |
| Build Package | after the four above: build the wheel and sdist, check `py.typed`, install the wheel, then `scripts/package_guard.py --dist dist` |

The Spark tier, the `slow` AML statistics tests and the full-history gitleaks
scan (`tests/test_gitleaks_history.py`) run locally before a push, not in CI.

Jobs use a fixed runner image (`ubuntu-24.04`) and every action is pinned by
commit SHA with its tag as a trailing comment.

## Further reading

- [Architecture](architecture.md) -- system layers, deploy order and the
  Spark executor profiles
- [DESIGN.md](DESIGN.md) -- the product model, invariants and extension rules
- [Operators and catalogs](operators-and-catalogs.md) -- the Spark Operator
  and the metastores in depth
- [Releasing](../RELEASING.md) -- how a release is cut
