## PR Checklist

### Feature Context
- **PRD:** `docs/prd/_____.md` (required for feature work; micro-PRD in PR description allowed for small changes; no PRD only for formatting/typo/doc-only edits with no behavioral impact)
- **ADR:** `docs/adr/_____.md` (required for structural decisions: new dependency, new storage format, pipeline architecture change, scoring framework/normalization approach)
- **Methodology / `method_version`:** `v___` (must match scoring output `method_version`)

### Gates (all must pass before merge)

- [ ] `make check` passes (paste output below)
- [ ] Acceptance criteria satisfied (PRD criteria if PRD exists; otherwise state the micro-PRD intent and how it was verified)
- [ ] Self-explanation written (5-8 sentences, as if interviewing)

### Golden Snapshot Guard

- [ ] Golden snapshot artifacts (`tests/golden/`) were NOT modified in this PR
- [ ] **OR** Golden snapshot artifacts under `tests/golden/` changed AND an explicit "Intentional behavior change" declaration is included AND `method_version` was bumped AND methodology doc updated

### Test Integrity

- [ ] No existing tests were weakened, skipped, or deleted
- [ ] No `@pytest.mark.xfail` or `@pytest.mark.skip` added
- [ ] No assertions broadened (e.g., `assertEqual` → `assertIsNotNone`)

### Intentional Behavior Changes

- [ ] This PR does NOT change scoring behavior, methodology, or data contracts
- [ ] **OR** behavior was changed intentionally AND (a) an ADR exists (when required) AND (b) `method_version` was bumped (when scoring outputs can change) AND (c) methodology doc updated when meaning changed

### Data Policy Check (CLAUDE.md Rule 2)

- [ ] This PR does NOT touch data acquisition or `data/seed/`
- [ ] **OR** acquisition/seed changed AND: raw dumps stayed gitignored (`data/raw/`), only curated seed CSVs committed, `docs/sources.md` provenance log updated
- [ ] No synthetic fixture data feeds analysis outputs; no real data added to `tests/fixtures/`

### The Key Question

**What is the single easiest way this code could be wrong, and what test would fail if it were?**

> (Write your answer here — this is required)

### `make check` Output

```
(paste full output here)
```

### Files Touched Summary

```
(paste `git diff --stat` output here)
```
