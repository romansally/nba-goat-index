.PHONY: check run validate transform

# CLAUDE.md Rule 3: the gate. Order matters — lint/format first, then tests, then
# data contract validation. Every merge to main requires this to pass locally.
check:
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest
	$(MAKE) validate

# CLAUDE.md Rule 2: no network. Runs the offline pipeline end-to-end against the
# committed seed dataset (wired up starting in T8).
run:
	@echo "make run: pipeline orchestration lands in T8 (pipeline/run.py)"

# T6 SQL transform layer: clean seed -> star schema + component-input marts
# in data/marts/ (gitignored, regenerable). T8's make run absorbs this.
transform:
	uv run python -m pipeline.clean
	uv run python -m pipeline.transform

# Pandera contract validation against the seed dataset, plus the hole check:
# every designed-bad fixture must still fail its named contract (CLAUDE.md Rule 3).
validate:
	uv run python -m pipeline.contracts
