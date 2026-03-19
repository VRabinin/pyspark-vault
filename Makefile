.PHONY: init init-dev test build inspect-wheel

init:
	uv pip install -r pyproject.toml

init-dev:
	uv pip install -e ".[dev]"

test:
	uv run python -m pytest tests

build:
	uv build --wheel

inspect-wheel:
	unzip -l dist/*.whl
