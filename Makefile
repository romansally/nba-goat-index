.PHONY: check run validate

# CLAUDE.md Rule 3: the gate. Order matters — lint/format first, then tests, then
# data contract validation. Every merge to main requires this to pass locally.
check:
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest || test $$? -eq 5  # exit 5 = no tests collected yet (pre-T5)
	$(MAKE) validate

# CLAUDE.md Rule 2: no network. Runs the offline pipeline end-to-end against the
# committed seed dataset (wired up starting in T8).
run:
	@echo "make run: pipeline orchestration lands in T8 (pipeline/run.py)"

# Pandera contract validation against the seed dataset (wired up starting in T5).
validate:
	@echo "make validate: contracts land in T5 (pipeline/contracts.py)"
