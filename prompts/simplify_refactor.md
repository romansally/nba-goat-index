# Simplify / Refactor Prompt

> Use this prompt when code feels over-engineered, when a module is hard to explain,
> or periodically as a complexity hygiene check. Paste into Claude Code or ChatGPT.

---

I need you to review the following code for unnecessary complexity.

**Read `CLAUDE.md` first** — specifically the Complexity Budget rules.

If the target code is in a high-rigor zone per `CLAUDE.md` (scoring code, Pandera contracts, scoring SQL, methodology), preserve those high-rigor expectations while simplifying. If the simplification touches methodology-coupled behavior, also review the relevant PRD and `docs/methodology/vX.md` before recommending it.

Review the code in `[file or module path]` and answer these questions:

## 1. Abstraction Audit
- Are there any abstractions that are only used in ONE place and not explicitly justified per `CLAUDE.md`? If so, recommend inlining them.
- Are there any classes that should be plain functions?
- Are there any design patterns (factory, strategy, observer, etc.) that aren't solving a concrete, current problem?

## 2. The 2-Minute Test
- Can each function/module be explained in 2 minutes to an interviewer?
- If not, which parts are hard to explain and why?
- Propose a simpler alternative for anything that fails this test.

## 3. Dependency Check
- Are there any imported packages that could be replaced with standard library or simple custom code?
- Are there any dependencies that were added "for later" but aren't used yet?

## 4. Line Count Check
- Are any implementation files (e.g., under `pipeline/` or `tests/`) over 250 lines? If so, is the length explicitly justified in a comment at the top of the file, per `CLAUDE.md`, or should it be split?

## 5. Proposed Simplifications
For each issue found, propose a specific simplification:
- What to remove or inline
- What to rename for clarity
- What to split or merge

**Goal:** Make the code as simple as possible while preserving canon-defined behavior. Remove anything that exists "just in case" or "for future flexibility."

If a proposed simplification could change behavior — or if it is unclear whether it is behavior-preserving — do not treat it as a pure refactor. Identify the `CLAUDE.md` path required for a behavior-changing change, including ADR, `method_version`, and methodology updates when required.

## 6. Regression Guard
If this refactor should NOT change behavior:
- Propose regression tests that would fail if behavior accidentally changed.
- Verify all existing golden snapshots remain identical and all relevant invariant tests still pass after the refactor.
- If any test needs to change, explain why the test was wrong (not why the behavior should change).
