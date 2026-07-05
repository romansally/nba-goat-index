# Planning Prompt (Change / Feature)

> Use this prompt when starting a new change or feature. Paste into Claude Code or ChatGPT.

---

**IMPORTANT: Do not write implementation code. First determine the correct planning artifact under `CLAUDE.md`. Output only one of the following: (a) Full PRD content, (b) micro-PRD intent for the PR description, or (c) a no-PRD determination.**

I'm about to start work on a change for the NBA GOAT Index project.

**Read CLAUDE.md first** (it's in the repo root).

First, state which planning path applies under `CLAUDE.md`'s Planning Gate (already covered by the current phase PRD / new Full or phase PRD / micro-PRD / no PRD) and why. During Tier-1, tasks listed in `docs/prd/tier1_mvp.md` are already planned — no new PRD needed.

If `CLAUDE.md` routes this change to a Full PRD, use `docs/prd/template.md` as the structure for the planning artifact and complete each section according to the template instructions. Do not invent a reduced PRD schema here.

If the change could alter scoring behavior, methodology outputs, or data contracts, route it to the strict Full PRD path in `CLAUDE.md` and identify whether `CLAUDE.md` requires a Full PRD, ADR, and `method_version` bump.

When `CLAUDE.md` requires Plan Mode for the implementation phase, note that explicitly in the planning output.

Then audit the planning artifact, if one is required: "What is the single easiest way this change could be wrong, and what test would catch it?"

---

## Planning Protocol Decision

Before proceeding, determine which planning level this change needs:

### Complex (methodology, scoring design, new data model / table grain / schema, or user unsure about approach)
- Use the interrogation path described in `CLAUDE.md` before drafting the planning artifact.
- Do not draft the Full PRD until that interrogation is complete.
- After this planning step, `/clear` before implementation per `CLAUDE.md`.

### Standard (new feature, new module, new public interface, or multi-file change that does not alter scoring behavior, methodology outputs, or data contracts)
- Draft the Full PRD using `docs/prd/template.md`.
- After this planning step, `/clear` before implementation per `CLAUDE.md`.

### Simple (bug fix, small refactor, config change)
- Output a micro-PRD intent only (what changed, why, and what test verifies it), for use in the PR description.
- If this planning step preceded coding, `/clear` before implementation per `CLAUDE.md`.

### Trivial (formatting, typo, doc-only edit with no behavioral impact)
- State that no PRD is required under `CLAUDE.md` and stop.
