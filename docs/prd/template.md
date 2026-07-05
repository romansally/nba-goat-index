# PRD: [Feature Name]

**Author:** [name]
**Date:** [date]
**Status:** Draft | Ready | In Progress | Complete
**Branch:** `feat/[name]`

---

## Pre-Implementation Checklist

- [ ] Planning path determined per CLAUDE.md Planning Gate (phase PRD task / Full PRD / micro-PRD / none)
- [ ] If required by CLAUDE.md (e.g., methodology/model/scoring design uncertainty): AskUserQuestion/interrogation session completed
- [ ] Planning session `/clear`ed before implementation begins
- [ ] Plan Mode used when required by CLAUDE.md (skip allowed for trivial/doc/format-only changes)

---

## 1. Objective

What this feature does and why it matters. One paragraph max.

## 2. Non-Goals

What this feature explicitly does NOT do. Prevents scope creep.

## 3. Files Affected

| Action | File Path | Description |
|--------|-----------|-------------|
| Create | `src/...` | ... |
| Modify | `src/...` | ... |

## 4. Data Contracts Impacted

Which Pandera schemas are affected? Any new era-conditional rules needed?
If no contracts are affected, state "None."

## 5. Methodology Impact

Does this change scoring behavior? If yes:
- [ ] Requires `method_version` bump (v1 → v2)
- [ ] Requires `docs/methodology/vX.md` update
- [ ] Requires golden snapshot regeneration
- [ ] Requires ADR at `docs/adr/[name].md`
- [ ] PR includes an explicit "Intentional behavior change" declaration (required if golden snapshots or outputs change)

If no methodology impact, state "None — no scoring behavior changes."

## 6. Acceptance Criteria

Numbered list of specific, testable criteria:

1. ...
2. ...
3. ...

**Reminder:** Output metadata requirements are defined in CLAUDE.md (do not restate them here).

## 7. Required Tests

### Unit tests (`tests/unit/`)
- ...

### Invariant tests (`tests/invariant/`)
- ...

### Golden snapshot artifacts impact (`tests/golden/`)
- [ ] No golden snapshot changes expected
- [ ] **OR** golden snapshots will change because: [reason + method_version bump confirmed]

> See CLAUDE.md "Golden Snapshot Guard" for full policy. Golden changes require method_version bump + PR declares "Intentional behavior change."

## 8. Dependencies

New packages needed? Must justify per complexity budget.
If none, state "No new dependencies."

## 9. Edge Cases

List edge cases this feature must handle correctly:
- ...

## 10. The Key Question

**What is the single easiest way this feature could be wrong, and what test would fail if it were?**

> (Answer required before implementation begins)
