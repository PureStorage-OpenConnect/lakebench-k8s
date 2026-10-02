# Development Guide

This guide covers setting up a local development environment for Lakebench,
running tests, and understanding the project structure.

## Prerequisites

- Python 3.10 or later
- pip
- Access to a terminal (Linux, macOS, or WSL on Windows)

A live Kubernetes cluster is **not** required for development. Unit tests mock
all external dependencies (Kubernetes API, S3, Spark Operator). You only need a
cluster for integration and end-to-end tests.

## Dev Setup

Clone the repository and install in editable mode with development dependencies:

```bash
git clone https://github.com/PureStorage-OpenConnect/lakebench-k8s.git
cd lakebench-k8s
pip install -e ".[dev]"
pre-commit install
```

This installs:

- **Runtime dependencies:** Typer, Pydantic v2, Rich, boto3, kubernetes, httpx,
  Jinja2, PyYAML
- **Dev dependencies:** pytest, pytest-cov, ruff, mypy, moto (AWS mocking),
  pre-commit
- **The `[aml]` extra:** numpy, scipy, pandas, scikit-learn, joblib and
  threadpoolctl at exactly the versions the cluster's AML reference detector
  installs (`REFERENCE_PY_DEPS` in
  `src/lakebench/modules/pipeline_engines/spark/job.py`), plus pyarrow.
  `tests/test_reference_pins.py` fails in CI if the extra and the tuple
  drift, or if the installed versions differ from them. Outside CI it reports
  a drifted install as a skip; set `LB_REQUIRE_REFERENCE_PINS=1` to make it
  fail. The pins have wheels for Python 3.10 to 3.13.

The `pre-commit install` step registers Git hooks that run the secret scan,
linting and formatting automatically before each commit. You can also set up the full
dev environment in one step with `make dev`.

## Running Tests

Before pushing, run what CI's first job runs:

```bash
make check-fast
```

That is ruff, the format check and mypy, then the unit tests in parallel
(`pytest-xdist`, one test file per worker) without the Spark tier and
without the AML statistics tests marked `slow`, which CI runs in their own
job. `make test` is the same pytest command without the lint and type
checks. To run the whole unit suite serially, slow tests included, and stop
at the first failure:

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
runtime. Tests that need Iceberg or Delta read the jars from
`LB_SPARK_TEST_JARS`, a comma-separated list of jar files for the installed
Spark line:

| Line | Jars |
|------|------|
| pyspark 4.0.1 | `iceberg-spark-runtime-4.0_2.13-1.11.0.jar`, `delta-spark_2.13-4.0.0.jar`, `delta-storage-4.0.0.jar` |
| pyspark 4.1.1 | `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, `delta-spark_4.1_2.13-4.1.0.jar`, `delta-storage-4.1.0.jar` |

These are the product defaults in `modules/pipeline_engines/spark/job.py`.
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

```bash
export LB_SPARK_TEST_JARS=/path/iceberg-spark-runtime-4.0_2.13-1.11.0.jar,/path/delta-spark_2.13-4.0.0.jar,/path/delta-storage-4.0.0.jar
LB_REQUIRE_JARS=1 pytest tests/spark -q -rs
pytest tests/spark -q --lb-reverse   # the same tests in reverse order
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
`--lb-reverse` is defined in `tests/spark/conftest.py`, so pass it with
`tests/spark` (or a file in it) on the command line.

### Test Markers

Tests are organized with pytest markers defined in `pyproject.toml`:

| Marker | Description | Requires Cluster |
|--------|-------------|------------------|
| `unit` | Fast, isolated tests with no external dependencies | No |
| `integration` | Require Kubernetes and S3 connectivity | Yes |
| `e2e` | Full deploy/run/destroy workflow | Yes |
| `slow` | Long-running tests | Varies |
| `extended` | Scale matrix tests (scales 1, 10, 50, 100) | Yes |
| `stress` | Stress tests at large scales (250, 500, 1000) | Yes |

### Coverage

Generate a coverage report with:

```bash
pytest tests/ --cov=lakebench --cov-report=term-missing --cov-report=html
```

The HTML report is written to `htmlcov/index.html`.

## Linting and Formatting

Lakebench uses [Ruff](https://docs.astral.sh/ruff/) for both linting and
formatting. The configuration lives in `pyproject.toml` and enforces:

- Python 3.10 target version
- 100-character line length
- Rule sets: pycodestyle (E, W), pyflakes (F), isort (I), flake8-bugbear (B),
  flake8-comprehensions (C4), pyupgrade (UP)

Run checks manually:

```bash
# Lint (check for errors)
ruff check src/ tests/ scripts/

# Format check, as CI runs it (drop --check to apply formatting)
ruff format --check src/ tests/ scripts/
```

Type checking uses mypy, configured in `pyproject.toml` (untyped
functions are allowed; CI runs the same command and must pass):

```bash
mypy src/lakebench/
```

## Makefile Targets

The Makefile provides shortcuts for common development tasks. Run `make help`
to see the full list.

### Setup

| Target | Description |
|--------|-------------|
| `make install` | Install the package in production mode |
| `make dev` | Install with dev dependencies and set up pre-commit hooks |

### Testing

| Target | Description |
|--------|-------------|
| `make check-fast` | Ruff, format check and mypy on `src/`, `tests/` and `scripts/`, then the unit tests in parallel without `tests/spark` and the `slow` tests (CI's first job, budget 8 minutes) |
| `make test` | The pytest command of `make check-fast` alone |
| `make test-unit` | Run every test not marked `integration` or `e2e` (`-m "not integration and not e2e"`), serially, `slow` tests included |
| `make test-integration` | Run integration tests (requires K8s and S3) |
| `make test-e2e` | Run end-to-end tests (full workflow) |
| `make test-extended` | Run scale matrix tests (scales 1, 10, 50, 100) |
| `make test-stress` | Run stress tests at large scales (250, 500, 1000) |
| `make test-cov` | Run `pytest tests/` with no marker filter and a coverage report |

### Code Quality

| Target | Description |
|--------|-------------|
| `make lint` | Run ruff linter on `src/`, `tests/` and `scripts/`, as CI does |
| `make fmt` | Format `src/`, `tests/` and `scripts/` with ruff and apply auto-fixes |
| `make typecheck` | Run mypy type checker on `src/` |

### Maintenance

| Target | Description |
|--------|-------------|
| `make clean` | Remove build artifacts, caches, and `__pycache__` directories |

## Pre-commit Hooks

When you run `pre-commit install`, the hooks in `.pre-commit-config.yaml` are
registered and run automatically on every `git commit`:

1. **gitleaks** -- Fails the commit on anything that looks like a credential.
2. **Ruff lint** -- Checks `src/` and `tests/` and applies auto-fixes where
   possible.
3. **Ruff format** -- Enforces consistent formatting on `src/` and `tests/`.
4. **cargo fmt** -- Checks formatting of the Rust datagen when a `.rs` file
   changes.

The hooks do not run the tests; run `pytest tests/ -x` yourself.

### Pre-push hook

`scripts/hooks/pre-push` refuses a push whose branch reaches a commit from
before the 2026-09-30 history rewrite, and scans the pushed commits and
their messages with gitleaks using the `.gitleaks.toml` and `.gitleaksignore`
from `origin/integrate/v1.5.0` rather than the branch's own copies. It also
scans what merge commits change and ignores inline `gitleaks:allow`
comments. Install it by copying it, not through
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
"Check fast" job runs `make check-fast` on Python 3.11 under an 8-minute
budget: `scripts/ci_budget.py` fails the step when a command runs over its
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
raised from that report, never lowered. A Spark job runs `pytest tests/spark`
with `pyspark==4.0.1` on Java 17 and checks its own floors with
`scripts/check_coverage.py --suite spark`. The "AML statistics (slow)" job runs
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
1.0.0 to 1.4.0 published) are not reported. The scan takes the config
from `origin/main` (or `origin/integrate/v1.5.0`) and the baseline from
`origin/main` (or `origin/integrate/v1.5.0` while `main` has no baseline),
not from the branch, so a branch cannot allowlist its own finding; a pull
request to `main` uses its own baseline, which the owner reviews, and CI
prints its baseline and config diff against `main`. The scan also runs with
the branch's own config, so a new rule applies at once.
`tests/test_gitleaks_baseline.py` pins the list. The package build runs
only after the lint, test, Spark, Rust and both secret-scan jobs pass; it
does not wait for the slow AML job, which most branches skip.

Every job runs on a fixed runner image (`ubuntu-24.04`, never
`ubuntu-latest`), and every action is pinned by commit SHA with its tag as
a trailing comment (`owner/repo@<sha> # vX.Y.Z`; `dtolnay/rust-toolchain`
has no release tags and names its branch and date instead). To change an action, pin
the new SHA, update its entry in `.github/action-runtimes.json` and run
`GITHUB_TOKEN=$(gh auth token) python scripts/check_action_runtimes.py
--verify`; `tests/test_workflows.py` fails on a floating runner label, an
unpinned or unmapped action, or a Node 20 or older runtime. Because
the pre-commit hooks and Makefile targets cover only `src/` and `tests/`, run
the ruff commands above before pushing a change under `scripts/`.

To run all hooks manually against the entire codebase:

```bash
pre-commit run --all-files
```

## Project Structure

The source code lives under `src/lakebench/`. Each subdirectory is a
self-contained module with a specific responsibility:

```
src/lakebench/
  aml/          Pure-Python AML helpers: reference detector, leakage gate, predictions
  benchmark/    Query engine benchmark (QpH; 8 c360 queries, 12 AML queries)
  cli/          CLI package (Typer commands, helpers, continuous pipeline)
  config/       Pydantic config schema, YAML loader, cluster autosizer
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

- `tests/` -- pytest test suite organized by module
  (`test_config.py`, `test_deploy.py`, `test_spark.py`, etc.)
- `datagen_rs/` -- Rust data generator and its container image (Parquet files to S3)
- `docs/` -- Project documentation

### Key Conventions

- **Configuration** uses Pydantic v2 models (`config/schema.py`). All user-facing
  config flows through a single YAML file parsed by `config/loader.py`.
- **Deployment** follows a component-based architecture. Each deployer
  (`deploy/postgres.py`, `deploy/hive.py`, etc.) implements deploy and destroy
  methods. The engine (`deploy/engine.py`) orchestrates them in the correct order.
- **Spark scripts** in `spark/scripts/` are standalone PySpark programs that run
  inside Spark executor pods. They are submitted via the Spark Operator.
- **All outputs** (journals, metrics, reports) go to `lakebench-output/`.

## Next Steps

- Read the [Contributing](contributing.md) guide for PR and code review guidelines.
- See [Getting Started](getting-started.md) for deploying Lakebench on a cluster.
