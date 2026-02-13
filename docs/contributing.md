# Contributing

Thank you for your interest in contributing to Lakebench. Whether you are
fixing a bug, adding a feature, or improving documentation, your help is
welcome. This guide explains how to submit changes and what we expect from
contributions.

## Getting Started

1. **Fork** the repository on GitHub.
2. **Clone** your fork locally.
3. **Create a branch** from `main` for your work:
   ```bash
   git checkout -b my-feature main
   ```
4. **Set up the dev environment:**
   ```bash
   cd lakebench
   pip install -e ".[dev]"
   pre-commit install
   ```

See the [Development Guide](development.md) for full details on setup,
running tests, and using the Makefile.

## Making Changes

- Keep changes focused. One concern per pull request.
- Write clear commit messages that explain *why* the change was made, not just
  what changed:
  ```
  Add retry logic for S3 multipart upload abort

  FlashBlade sometimes returns 404 on abort calls for uploads that
  are still in-flight. Retry with backoff to handle this race.
  ```
- Follow existing code patterns. When in doubt, check how similar functionality
  is already implemented.

## Code Standards

All contributions must pass the automated checks before merging.

### Linting and Formatting

Lakebench uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting,
configured in `pyproject.toml`. Run both before submitting:

```bash
ruff check src/ tests/
ruff format src/ tests/
```

Or use the Makefile shortcuts:

```bash
make lint
make fmt
```

### Type Checking

Type annotations are required on all public functions and classes. Run mypy
with:

```bash
make typecheck
```

### Style Conventions

- **Line length:** 100 characters.
- **Python version:** 3.10+ (use `X | Y` union syntax, not `Union[X, Y]`).
- **Imports:** Use `from __future__ import annotations` in new modules.
- **Models:** Pydantic v2 for configuration, dataclasses for internal structures.
- **Docstrings:** Required on all public functions and classes.
- **Type ignores:** No `type: ignore` without a comment explaining the reason.

## Test Requirements

### Unit Tests

All new code must have corresponding unit tests. Place tests in
`tests/test_<module>.py`, matching the source module name. Use class-based
grouping (`class TestFeatureName`) for related tests.

Unit tests must run without external dependencies. Mock Kubernetes, S3, and
Spark interactions using the fixtures in `conftest.py` and the
[moto](https://github.com/getmoto/moto) library for AWS services.

Run unit tests with:

```bash
pytest tests/ -x -v --ignore=tests/test_e2e.py --ignore=tests/test_integration.py
```

### Integration and End-to-End Tests

Integration and e2e tests are optional for most contributions. They require a
live Kubernetes cluster with S3 storage and are typically run in CI or by
maintainers. If your change affects deployment or runtime behavior, mention
this in your PR description so maintainers can run the appropriate tests.

## Submitting a Pull Request

1. **Run the full check suite** before pushing:
   ```bash
   make lint && make typecheck && make test
   ```
2. **Push** your branch to your fork.
3. **Open a pull request** against `main`.
4. **Describe your changes** in the PR body:
   - What the change does and why it is needed.
   - Any testing you performed beyond unit tests.
   - Whether documentation updates are needed.
5. **Respond to review feedback.** Maintainers may request changes before
   merging.

### PR Checklist

- [ ] Ruff lint and format pass (`make lint && make fmt`)
- [ ] mypy passes (`make typecheck`)
- [ ] Unit tests pass (`make test`)
- [ ] New code has test coverage
- [ ] Commit messages are clear and descriptive

## License

Lakebench is licensed under the [Apache License 2.0](../LICENSE). By
submitting a contribution, you agree that your work will be licensed under
the same terms. You retain copyright over your contributions.

## Questions

If you are unsure about anything, open an issue on the repository. We are
happy to help you find the right approach before you invest time in
implementation.
