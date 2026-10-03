.PHONY: help install dev check-fast test-spark test test-unit test-integration test-e2e test-extended test-stress lint typecheck fmt clean

help:
	@echo "Lakebench Development Commands"
	@echo "=============================="
	@echo ""
	@echo "Setup:"
	@echo "  install          Install package in production mode"
	@echo "  dev              Install with dev deps + pre-commit hooks"
	@echo ""
	@echo "Testing:"
	@echo "  check-fast       Lint, format check, mypy and the unit tests in parallel (XDIST_WORKERS=N caps workers)"
	@echo "  test             The unit tests of check-fast, without the lint and type checks"
	@echo "  test-spark       The Spark tier on the installed pyspark, with its pinned jars"
	@echo "  test-unit        Run unit tests only, serially (slow AML statistics tests included)"
	@echo "  test-integration Run integration tests (requires K8s/S3)"
	@echo "  test-e2e         Run end-to-end tests (full workflow)"
	@echo "  test-extended    Run scale matrix tests (1, 10, 50, 100)"
	@echo "  test-stress      Run stress tests at large scales (250, 500, 1000)"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint             Run ruff linter"
	@echo "  typecheck        Run mypy type checker"
	@echo "  fmt              Format code with ruff"
	@echo ""
	@echo "Maintenance:"
	@echo "  clean            Remove build artifacts and caches"

install:
	pip install -e .

dev:
	pip install -e ".[dev]"
	pre-commit install
	@echo ""
	@echo "Dev environment ready. Pre-commit hooks installed."
	@echo "Run 'make test' to verify setup."

# The unit tier as CI's Lint job runs it: in parallel, one file per worker
# (--dist loadfile), without the AML statistics tests marked slow (CI runs
# those in the "AML statistics (slow)" job) and without the Spark tier. A -m
# here replaces pyproject.toml's, so it repeats e2e and integration.
# PYTHONPATH=src tests this checkout's code even when another checkout is
# the installed one; XDIST_WORKERS=8 caps the workers on a shared host.
PYTHON ?= python3
XDIST_WORKERS ?= auto
UNIT_PYTEST = PYTHONPATH=src$${PYTHONPATH:+:$$PYTHONPATH} $(PYTHON) -m pytest tests/ -q \
	-n $(XDIST_WORKERS) --dist loadfile -p no:cacheprovider \
	-m "not slow and not e2e and not integration" \
	--ignore=tests/spark --ignore=tests/test_e2e.py --ignore=tests/test_integration.py

check-fast:
	ruff check src/ tests/ scripts/
	ruff format --check src/ tests/ scripts/
	mypy src/lakebench/
	$(UNIT_PYTEST)

test:
	$(UNIT_PYTEST)

# The Spark tier as CI runs it: the jars pinned in
# tests/spark/jars.lock.json for the installed pyspark, every jar-reason skip
# an error, serially (the Spark sessions share the JVM's working directory).
# CI splits it into two jobs (--lb-shard 1/2 and 2/2) and also runs both
# shards with --lb-reverse.
test-spark:
	jars="$$(python scripts/fetch_test_jars.py --leg auto --print-env)" && export "$$jars" && \
	LB_REQUIRE_JARS=1 PYSPARK_PYTHON="$$(command -v python)" pytest tests/spark -q -rsxX -p no:cacheprovider

test-unit:
	pytest tests/ -v -m "not integration and not e2e"

test-integration:
	pytest tests/ -v -m "integration"

test-e2e:
	pytest tests/ -v -m "e2e"

test-extended:
	pytest tests/test_e2e.py -v -m "extended"

test-stress:
	pytest tests/test_e2e.py -v -m "stress"

test-cov:
	pytest tests/ --cov=lakebench --cov-report=term-missing --cov-report=html
	@echo ""
	@echo "Coverage report: htmlcov/index.html"

lint:
	ruff check src/ tests/ scripts/

typecheck:
	mypy src/lakebench/

fmt:
	ruff format src/ tests/ scripts/
	ruff check --fix src/ tests/ scripts/

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned build artifacts and caches."
