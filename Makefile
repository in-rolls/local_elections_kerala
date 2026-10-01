.PHONY: check test to-parquet verify-data

check:
	uv sync --frozen --group dev
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest -q

test:
	uv run pytest -q

to-parquet:
	uv run python scripts/to_parquet.py

verify-data:
	uv run python scripts/to_parquet.py --check
