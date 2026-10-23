PY ?= uv run python
RUFF ?= uv run ruff

.PHONY: sync data fetch-log lint test verify data-summary check

sync:
	uv sync --frozen --group dev

data:
	$(PY) -m local_elections_kerala.parse.to_parquet
	$(PY) -m local_elections_kerala.build.fetch_log
	$(PY) -m local_elections_kerala.build.release summary

lint:
	$(RUFF) check .
	$(RUFF) format --check .

test:
	uv run pytest -q

verify:
	$(PY) -m local_elections_kerala.build.release verify
	$(PY) -m local_elections_kerala.build.fetch_log --check

data-summary:
	$(PY) -m local_elections_kerala.build.release summary

check: lint test verify
