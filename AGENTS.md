# AGENTS.md — AI Agent Roles & Permissions

> **Read CLAUDE.md first.** When CLAUDE.md conflicts with this file, CLAUDE.md wins.
> Project intent (scope, data policy, direction) is governed by
> `docs/vision/NBA_GOAT_Index_Vision_and_Story.md` — no agent may propose direction changes that
> conflict with it. This is a **Data Analyst** portfolio project; keep the NBA soul.

---

## Agent Roles

### Claude Code (Primary Builder)
- **Role:** Implementation. Writes code, runs tests, creates files.
- **Reads at session start:** CLAUDE.md (auto) + the current PRD (`docs/prd/tier1_mvp.md` during
  Tier-1). Read the Vision doc before any scope/direction discussion.
- **Permissions:** May read/write repo files as needed; high-rigor rules apply to scoring,
  contracts, and methodology per CLAUDE.md.
- **Restrictions:**
  - Run `make check` before merging; never merge red.
  - NEVER modify existing tests to make code pass (fix source code instead).
  - No new dependency without a recorded one-line justification (PRD, ADR, or commit message).
  - Respect the 250-line file limit unless justified per CLAUDE.md.
  - **Network only in the acquisition script** (`pipeline/fetch_seed.py`), run explicitly. All
    other pipeline code reads committed files (`data/seed/`, fixtures). Raw API dumps go to
    `data/raw/` (gitignored); curated seed CSVs are committed; `docs/sources.md` updated on any
    re-pull.
  - Synthetic fixture data never feeds analysis outputs; real seed data never appears in
    `tests/fixtures/`.

### Codex / ChatGPT (Independent Reviewer)
- **Role:** Reviews diffs with the 4-Point Prompt (`prompts/review_codex.md`).
- **Required for:** any diff touching scoring, contracts, or methodology.
- **Optional for:** everything else (solo-sprint right-sizing; see ADR-0001).
- **Does NOT:** write implementation code, modify tests, or change repo files.

### ChatGPT / Claude Chat (Architect & Auditor)
- **Role:** Architecture review, strategy decisions, PRD/methodology review, "should I add X?"
- **Feed context:** CLAUDE.md + the Vision doc (+ the relevant PRD). PROJECT_WISDOM.md is
  background rationale only — its DE-era guidance is partly superseded (see its status note).
- **Does NOT:** write implementation code directly.

---

## Context Feeding Protocol

- **New Claude Code session:** CLAUDE.md is read automatically. Say which PRD task you're on.
- **New ChatGPT / Claude Chat session:** paste or upload CLAUDE.md + the Vision doc, plus the
  current PRD if working a specific task.
- **Codex review session:** provide `prompts/review_codex.md`, the diff, the relevant PRD task's
  acceptance criteria, and the `make check` output.
- **PR:** use `.github/pull_request_template.md`. Every PR answers: "What is the single easiest
  way this could be wrong, and what test would fail?"

---

## Keeping Context Current

When you make a significant decision during development:
1. Rule/process change → update CLAUDE.md (and record why in an ADR if structural)
2. Architectural/structural decision → ADR in `docs/adr/`
3. Scoring behavior change → methodology doc + ADR + `method_version` bump (no exceptions)
4. Agent behavior change → update this file
5. New insight/rationale → PROJECT_WISDOM.md

The files ARE the memory. Keep them updated, and every new session starts informed.
