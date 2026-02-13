# Contributing to Lakebench

Thank you for your interest in contributing to Lakebench. This guide covers
the development workflow, code standards, and how to submit changes.

## Development Setup

```bash
cd lakebench

# Install in development mode with all dev dependencies
pip install -e ".[dev]"

# Install pre-commit hooks (runs linter + unit tests on commit)
pre-commit install
```

## Code Standards

### Style

- **Formatter/Linter:** [Ruff](https://docs.astral.sh/ruff/) (configured in `pyproject.toml`)
- **Type checking:** [mypy](https://mypy-lang.org/) with strict settings (`disallow_untyped_defs = true`)
- **Line length:** 100 characters
- **Python version:** 3.10+ (use `X | Y` union syntax, not `Union[X, Y]`)

Run checks locally:

```bash
make lint        # Ruff lint + auto-fix
make fmt         # Ruff format
make typecheck   # mypy strict check
```

### Conventions

- All public functions and classes must have type annotations and docstrings.
- Use `from __future__ import annotations` in new modules.
- Pydantic v2 models for configuration; dataclasses for internal data structures.
- No `type: ignore` pragmas without a comment explaining why.

## Testing

```bash
make test           # All unit tests
make test-unit      # Unit tests only (fast, no external deps)
make test-cov       # Unit tests with coverage report
```

Tests are organized by category:

| Marker | Description | External deps |
|--------|-------------|---------------|
| `unit` | Fast, isolated tests | None |
| `integration` | Require K8s + S3 connectivity | Yes |
| `e2e` | Full deploy/run/destroy workflow | Yes |
| `slow` | Long-running tests | Varies |

See [development guide](docs/development.md) for the full testing guide.

### Writing Tests

- Place tests in `tests/test_<module>.py` matching the source module.
- Use class-based grouping (`class TestFeatureName`) for related tests.
- Mock external dependencies (K8s API, S3, Spark operator) in unit tests.
- Use fixtures from `conftest.py` for shared test configuration.

## Submitting Changes

1. **Fork and branch** from `main`.
2. **Make your changes** with tests for new functionality.
3. **Run the full check suite:**
   ```bash
   make lint && make typecheck && make test
   ```
4. **Commit** with a clear message describing the "why" not just the "what".
5. **Open a pull request** against `main`.

### Commit Messages

Write concise commit messages that describe the change and its purpose:

```
Add table name registry for dataset swapability

Extract hardcoded Iceberg table names into TableNamesConfig so
changing the dataset (e.g., Customer360 to IoT) only requires
updating config, not grepping across 8+ files.
```

### Pull Request Guidelines

- Keep PRs focused on a single concern.
- Include test coverage for new code paths.

## Architecture Notes

Key design principle: **"Port proven patterns, don't redesign them."** The pipeline
defaults have been battle-tested at 1TB+ scale. Check the engineering history for
patterns that worked before building something new.

### What NOT to do

The project has a documented history of over-abstraction leading to a full rewrite
deletion (2026-01-27). Avoid:

- Provider/plugin patterns for components that don't have a second implementation yet.
- Abstract base classes for things that only have one concrete class.
- Speculative interfaces for hypothetical future requirements.

## Questions?

Open an issue on the repository or check the existing documentation in `docs/`.
