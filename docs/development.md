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

Run the unit test suite:

```bash
pytest tests/ -x -v --ignore=tests/test_e2e.py --ignore=tests/test_integration.py
```

The `-x` flag stops on the first failure, which is useful during development.

End-to-end and integration tests require a live Kubernetes cluster with S3
storage, a Spark Operator, and a Hive metastore. These are excluded from the
default test run. To run them when you have a cluster available:

```bash
pytest tests/ -v -m "integration"    # Integration tests (K8s + S3)
pytest tests/ -v -m "e2e"            # Full deploy/run/destroy workflow
```

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
| `make test` | Run the tests that need no cluster (excludes integration, e2e, extended and stress) |
| `make test-unit` | Run every test not marked `integration` or `e2e` (`-m "not integration and not e2e"`); unlike `make test` it does not exclude `extended` or `stress` |
| `make test-integration` | Run integration tests (requires K8s and S3) |
| `make test-e2e` | Run end-to-end tests (full workflow) |
| `make test-extended` | Run scale matrix tests (scales 1, 10, 50, 100) |
| `make test-stress` | Run stress tests at large scales (250, 500, 1000) |
| `make test-cov` | Run `pytest tests/` with no marker filter and a coverage report |

### Code Quality

| Target | Description |
|--------|-------------|
| `make lint` | Run ruff linter on `src/` and `tests/` (CI also lints `scripts/`) |
| `make fmt` | Format `src/` and `tests/` with ruff and apply auto-fixes (CI also checks `scripts/`) |
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

## What CI Runs

`.github/workflows/ci.yml` runs on every branch push and on pull requests to
`main` and `integrate/**`. A newer push to the same `lane/*` branch or pull
request cancels the older run; on every other branch and on tags each pushed
head's run finishes. The lint job runs `ruff check src/ tests/ scripts/`,
`ruff format --check src/ tests/ scripts/` and `mypy src/lakebench/` on
Python 3.11, and `scripts/check_action_runtimes.py --verify`, which reads
each action's `action.yml` at its pinned SHA and fails if
`.github/action-runtimes.json` no longer matches it. The test job runs
`pytest tests/ -rs` (excluding `tests/test_e2e.py` and
`tests/test_integration.py`) on Python 3.10 and 3.13, the oldest and newest
supported versions. It runs to the end rather than stopping at the first
failure, prints every skip reason, and a failure on one Python version does
not cancel the other (`fail-fast: false`). On 3.13 it checks per-file
coverage floors with `scripts/check_coverage.py --suite unit`. A Spark job runs `pytest tests/spark`
with `pyspark==4.0.1` on Java 17 and checks its own floors with
`scripts/check_coverage.py --suite spark`. The "AML statistics (slow)" job runs
the tests marked `slow` (the heavy fidelity-gate fits, the scale invariance
check and the Spark fidelity gate over silver) on Python 3.11 with the pinned
`[aml]` libraries, on every push to `main`, `integrate/**`, `train/*` and
tags and on every pull request to `integrate/**`; it is not run on other
branch pushes. It also reruns the D8 power simulation in a second
environment with the numpy and scipy versions the pre-registration recorded
its output hash with, since that guard skips on the `[aml]` pins.
The Rust job runs `cargo fmt
--check`, `cargo clippy --all-targets --locked -- -D warnings` and `cargo test
--release --locked` in `datagen_rs/`. A gitleaks job scans the working tree
for credentials. The package build runs only after all of these pass.

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
