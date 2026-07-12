.PHONY: check run validate

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

# Pandera contract validation against the seed dataset, plus the hole check:
# every designed-bad fixture must still fail its named contract (CLAUDE.md Rule 3).
validate:
	uv run python -m pipeline.contracts
