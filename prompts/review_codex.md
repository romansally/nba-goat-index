# Codex Review Prompt

> Copy-paste this entire prompt when submitting a diff for Codex review.
> Replace the bracketed placeholders with actual links/content.

---

You are reviewing a code diff for the NBA GOAT Index project. Your role is independent reviewer — you did not write this code.

**PRD / micro-PRD intent:** [link or paste PRD, or paste PR description intent for small changes]

**Diff:** [paste diff or link]

**`make check` output:** [paste output]

If the diff touches scoring behavior, methodology, or data contracts, review it against the relevant `docs/methodology/vX.md` and contract assumptions in addition to the PRD.

Answer these four questions:

## 1. PRD Acceptance Criteria
List each acceptance criterion from the PRD or, for a micro-PRD, each stated intent/check to verify. For each one, state PASS or FAIL with a brief explanation.

## 2. Severe Bugs
List severe bugs FIRST, before any style or formatting issues. Treat as severe any issue that could materially break correctness, contracts, outputs, or workflow.

## 3. Interview Explainability
Flag any code that the developer would struggle to explain in a 2-minute interview answer. This includes: over-abstraction, unnecessarily clever patterns, deep nesting, or logic that requires reading 3+ other files to understand.

## 4. Propose Tests That Would FAIL If the Code Were Wrong
Propose tests that would FAIL if the code were wrong. These must be tests that genuinely exercise correctness — not tests that pass trivially.

Bad example: `assert result is not None`
Good example: `assert score_player_a > score_player_b` (where A is mechanically better in all weighted categories per the methodology)

Focus on tests that catch the most likely failure modes for THIS specific code.
