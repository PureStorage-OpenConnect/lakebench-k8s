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
cd lakebench
pip install -e ".[dev]"
pre-commit install
```

This installs:

- **Runtime dependencies:** Typer, Pydantic v2, Rich, boto3, kubernetes, httpx,
  Jinja2, PyYAML
- **Dev dependencies:** pytest, pytest-cov, ruff, mypy, moto (AWS mocking),
  pre-commit

The `pre-commit install` step registers Git hooks that run linting, formatting,
and unit tests automatically before each commit. You can also set up the full
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
ruff check src/ tests/

# Format (apply consistent style)
ruff format src/ tests/
```

Type checking is available via mypy with strict settings
(`disallow_untyped_defs = true`):

```bash
mypy src/
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
| `make test` | Run all tests (excludes e2e, extended, and stress) |
| `make test-unit` | Run unit tests only (fast, no cluster needed) |
| `make test-integration` | Run integration tests (requires K8s and S3) |
| `make test-e2e` | Run end-to-end tests (full workflow) |
| `make test-extended` | Run scale matrix tests (scales 1, 10, 50, 100) |
| `make test-stress` | Run stress tests at large scales (250, 500, 1000) |
| `make test-cov` | Run unit tests with coverage report |

### Code Quality

| Target | Description |
|--------|-------------|
| `make lint` | Run ruff linter on `src/` and `tests/` |
| `make fmt` | Format code with ruff and apply auto-fixes |
| `make typecheck` | Run mypy type checker on `src/` |

### Maintenance

| Target | Description |
|--------|-------------|
| `make clean` | Remove build artifacts, caches, and `__pycache__` directories |

## Pre-commit Hooks

When you run `pre-commit install`, the following hooks are registered and run
automatically on every `git commit`:

1. **Ruff lint** -- Checks for code quality issues and applies auto-fixes
   where possible.
2. **Ruff format** -- Enforces consistent code formatting.
3. **Trailing whitespace** -- Removes trailing whitespace from all files.
4. **End-of-file fixer** -- Ensures files end with a single newline.
5. **YAML check** -- Validates YAML syntax (with support for custom tags).
6. **Large file check** -- Prevents committing files larger than 500KB.
7. **Merge conflict check** -- Catches unresolved merge conflict markers.
8. **Unit tests** -- Runs `pytest tests/ -x -q -m "not integration and not e2e"`
   to catch regressions before they reach the remote.

To run all hooks manually against the entire codebase:

```bash
pre-commit run --all-files
```

## Project Structure

The source code lives under `src/lakebench/`. Each subdirectory is a
self-contained module with a specific responsibility:

```
src/lakebench/
  benchmark/    Trino query benchmark (8-query QpH suite)
  config/       Pydantic config schema, YAML loader, cluster autosizer
  deploy/       Deployment engine and per-component deployers
                (PostgreSQL, Hive, Trino, Spark RBAC, Prometheus, Grafana)
  journal/      Event logging (session-scoped JSONL provenance logs)
  k8s/          Kubernetes client wrapper and OpenShift security (SCC/RBAC)
  metrics/      Pipeline metrics collection, aggregation, and storage
  reports/      HTML report generation from benchmark results
  s3/           S3 client (boto3 wrapper with FlashBlade compatibility)
  spark/        Spark job manager, operator integration, and PySpark scripts
  templates/    Jinja2 templates for Kubernetes manifests
```

Supporting directories:

- `tests/` -- pytest test suite organized by module
  (`test_config.py`, `test_deploy.py`, `test_spark.py`, etc.)
- `datagen/` -- Docker image for data generation (Parquet files to S3)
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
