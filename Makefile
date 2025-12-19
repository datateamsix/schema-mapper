.PHONY: help install install-dev test lint format clean build upload docs

help:
	@echo "Available commands:"
	@echo "  make install      - Install package"
	@echo "  make install-dev  - Install package with dev dependencies"
	@echo "  make test         - Run tests"
	@echo "  make lint         - Run linters"
	@echo "  make format       - Format code"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make build        - Build distribution packages"
	@echo "  make upload       - Upload to PyPI"
	@echo "  make upload-test  - Upload to Test PyPI"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest -v --cov=schema_mapper --cov-report=term-missing

test-all:
	pytest -v --cov=schema_mapper --cov-report=html --cov-report=term-missing

lint:
	flake8 schema_mapper tests
	mypy schema_mapper

format:
	black schema_mapper tests
	isort schema_mapper tests

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	python -m build

upload: build
	twine upload dist/*

upload-test: build
	twine upload --repository testpypi dist/*

check:
	python setup.py check --strict --metadata

version:
	@python -c "from schema_mapper import __version__; print(__version__)"
