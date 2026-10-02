PY ?= uv run python
RUFF ?= uv run ruff

.PHONY: sync data lint verify data-summary check

sync:
	uv sync --frozen --group dev

data:
	$(PY) -m local_elections_kerala.parse.to_parquet
	$(PY) -m local_elections_kerala.build.release summary

lint:
	$(RUFF) check .
	$(RUFF) format --check .

verify:
	$(PY) -m local_elections_kerala.build.release verify

data-summary:
	$(PY) -m local_elections_kerala.build.release summary

check: lint verify
