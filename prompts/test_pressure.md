# Test Pressure Prompt

> Use this prompt after implementing a feature, before the review step.
> Paste into Claude Code, Codex, or ChatGPT.

---

I just implemented [feature name]. Before I submit for review, I need you to pressure-test my test suite.

**Read the relevant PRD or micro-PRD intent:** `docs/prd/<feature>.md` or PR description intent for small changes
**Look at the tests in:** `tests/unit/` (includes invariant tests during Tier-1) and `tests/golden/`
If this feature touches scoring behavior, methodology, or data contracts, also check whether invariant tests and `tests/golden/` expectations still align with the relevant `docs/methodology/vX.md` and the `method_version` / “Intentional behavior change” rules in `CLAUDE.md`.

Answer these questions:

## 1. Propose tests that would FAIL if the code were wrong
These must be tests that genuinely catch bugs — not tests that pass trivially. For each test, explain what specific bug it would catch.

## 2. Identify trivially passing tests
Are any of my current tests passing for the wrong reasons? For example:
- Assertions that are always true regardless of code behavior
- Tests that don't actually exercise the code path they claim to test
- Tests that would still pass if I replaced the function with `return None`

## 3. Check for cheating patterns
Look for any of these anti-patterns in the test suite:
- `@pytest.mark.skip` or `@pytest.mark.xfail` on previously passing tests
- Assertions that were broadened (e.g., `assertEqual` → `assertIsNotNone`)
- Deleted tests
- Coverage was reduced to pass CI

## 4. Mutation test thought experiment
"If I changed [specific line] in the source code, would any test fail?" Pick the 3 most critical lines in the implementation and answer this for each.
