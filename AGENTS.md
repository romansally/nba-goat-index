# AGENTS.md — AI Agent Roles & Permissions

> **Read CLAUDE.md first.** When CLAUDE.md conflicts with this file, CLAUDE.md wins.

---

## Agent Roles

### Claude Code (Primary Builder)
- **Role:** Implementation. Writes code, runs tests, creates files.
- **Reads:** CLAUDE.md (auto), PROJECT_WISDOM.md (on first session), relevant PRD
- **Permissions:** May read/write repo files as needed (including `docs/`, `.github/`, and `prompts/`); high-rigor rules apply in the scoring/contracts/pipeline zones per CLAUDE.md.
- **Restrictions:**
  - Must run `make check` frequently; at minimum before opening/updating a PR and before merge.
  - Must NOT modify existing tests to make code pass (fix source code instead)
  - Must NOT add dependencies without PRD/ADR justification
  - Must respect the 250-line file limit unless justified per CLAUDE.md (documented justification + PRD/ADR where applicable).
  - Offline-first is mandatory: default `INGEST_MODE=offline`. Online ingestion is allowed only when `INGEST_MODE=online` is explicitly set, with raw caching under `data/raw/` (gitignored) and `docs/sources.md` updated. Otherwise, no network calls are allowed in ingest/pipeline code paths.
- **On first session:** Read CLAUDE.md and PROJECT_WISDOM.md before writing any code

### Codex (Independent Reviewer)
- **Role:** Code review. Evaluates diffs against PRDs using the 4-Point Prompt.
- **Reads:** CLAUDE.md, the relevant PRD, the diff
- **Does NOT:** Write implementation code, modify tests, or change repo files
- **Review prompt:**
  1. Does this diff satisfy each PRD acceptance criterion?
  2. What are the severe bugs? (list first)
  3. Flag anything the developer would struggle to explain in an interview.
  4. Propose tests that would FAIL if the code were wrong.

### ChatGPT (Checkpoint Auditor & Architect)
- **Role:** Architecture review, strategy decisions, progress validation
- **Use for:** PRD review, methodology review, stack decisions, "should I add X?" questions
- **Does NOT:** Write implementation code directly
- **Strength:** Cross-referencing, finding gaps in plans, industry context

### Claude Chat (This Interface)
- **Role:** Deep analysis, planning, document creation
- **Use for:** Creating governance docs, analyzing approaches, writing methodology
- **Feed context:** Paste CLAUDE.md + PROJECT_WISDOM.md at session start, or upload them

---

## Context Feeding Protocol

### Starting a New Claude Code Session
Claude Code automatically reads CLAUDE.md from repo root. No action needed for the constitution.
For full context, at session start say:
> "Read PROJECT_WISDOM.md before starting any work."

### Starting a New ChatGPT Session
Paste or upload these files at the start of the conversation:
1. CLAUDE.md (required — constitution)
2. PROJECT_WISDOM.md (required — rationale and insights)
3. The specific PRD you're working on (if applicable)

### Starting a New Claude Chat Session
Same as ChatGPT. Upload or paste CLAUDE.md + PROJECT_WISDOM.md.
If the conversation is about a specific feature, also include the PRD.

### Starting a Codex Review Session
Provide:
1. Copy-paste `prompts/review_codex.md` as the prompt
2. The git diff
3. The PRD for the feature being reviewed
4. The `make check` output

### Submitting a PR
Use `.github/pull_request_template.md` — it enforces all gates as a checklist.
Every PR must answer: "What is the single easiest way this could be wrong, and what test would fail?"

---

## Keeping Context Current

When you make a significant decision during development:
1. If it's a rule change → update CLAUDE.md
2. If it's a new insight or rationale → add to PROJECT_WISDOM.md
3. If it's an architectural decision → write an ADR in `docs/adr/`
4. If it changes agent behavior → update this file (AGENTS.md)

The files ARE the memory. Keep them updated, and every new session starts informed.
