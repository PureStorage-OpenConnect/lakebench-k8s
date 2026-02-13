.PHONY: help install dev test test-unit test-integration test-e2e test-extended test-stress lint typecheck fmt clean

help:
	@echo "Lakebench Development Commands"
	@echo "=============================="
	@echo ""
	@echo "Setup:"
	@echo "  install          Install package in production mode"
	@echo "  dev              Install with dev deps + pre-commit hooks"
	@echo ""
	@echo "Testing:"
	@echo "  test             Run all tests"
	@echo "  test-unit        Run unit tests only (fast, no cluster needed)"
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

test:
	pytest tests/ -v -m "not e2e and not extended and not stress"

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
	ruff check src/ tests/

typecheck:
	mypy src/

fmt:
	ruff format src/ tests/
	ruff check --fix src/ tests/

clean:
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned build artifacts and caches."
