default: test

format:
	uv run ruff format .

test:
	uv run pytest
	uv run ruff check .
	uv run ruff format --check .
	uv run ty check .

env:
	uv sync --dev
