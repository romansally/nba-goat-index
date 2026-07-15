# ADR-0003: Rule 1 amendment — the input-domain tightening clause

**Date:** 2026-07-13
**Status:** Accepted (owner decision, T7 Codex review round 3)
**Scope:** CLAUDE.md Rule 1 (constitutional amendment, recorded here per CLAUDE.md's own
header rule that its changes are captured in ADRs). First application: ADR-0002.

---

## Context

T7's anchor-range guard (ADR-0002) makes the engine refuse degenerate/noise-thin continuous
pools that v1.md §6 previously defined as scoring 50.0 — an observable behavior change for a
class of contract-valid inputs. The round-3 Codex review correctly objected that **an ADR
cannot override CLAUDE.md Rule 1**: Rule 1 as then written required a `method_version` bump
for any defined-behavior change, full stop, and ADR-0002's "domain restriction, not a scoring
change" category existed nowhere in the constitution. The objection was to the process
authority, not the guard itself.

The owner was given three options with tradeoffs — (a) full v2 bump, (b) amend Rule 1 to
authorize a mechanically-verified tightening category, (c) revert to §6-literal behavior
with diagnostics — and chose **(b)**: amend Rule 1 itself.

## Decision

Rule 1 gains the following bullet (verbatim from CLAUDE.md):

> **Input-domain tightening (the only bump-free path for a behavior-affecting change):** a
> change that makes the engine REFUSE (raise on) inputs it previously scored may ship without
> a `method_version` bump only when ALL of the following hold, verified mechanically and
> recorded in a dedicated ADR:
> 1. It only converts inputs from scored to refused-with-an-error. The refusal aborts the
>    entire run — it never warns, coerces, repairs, defaults, or skips/drops rows or
>    elements — and no value in any output changes for any input still accepted.
> 2. The refusal predicate is decidable from the run's inputs alone (the data, and which
>    inputs the configured scope consumes) — never from computed scores, rankings, or their
>    relationship to any target.
> 3. The committed seed dataset, fixtures, and golden snapshots pass UNCHANGED across the
>    ENTIRE change — "the change" is the full diff against the target branch's merge base,
>    never any single commit (commit boundaries are author-controlled). Artifacts existing
>    at the merge base must be byte-identical to it; an artifact CREATED within the change
>    must have no prior version and stay byte-identical from its creation commit through
>    the tip. A prior artifact edit counts as separate only if it was independently
>    reviewed and merged to the target branch before this change began. `make check` is
>    green, and every golden file remains byte-identical WITHOUT regeneration (enforced by
>    tests/unit/test_reference_artifacts.py).
> 4. The methodology doc is amended in the same change to state the new domain rule, citing
>    the ADR (that specific edit is authorized by this clause).
>
> Widening the domain back — accepting inputs that currently raise — is a behavior-changing
> scoring change and follows the full v2 path, as does any change that cannot satisfy every
> condition above.

Two clauses were tightened from the reviewed draft, both flagged to the owner: condition 3
scopes "may not be modified" to the named reference artifacts (the literal draft — "nothing
committed may be modified" — would have forbidden modifying committed source code, making
the condition unsatisfiable by any real diff, including this first application); and Rule 1's
pre-existing "typos/clarity only" sentence gained a cross-reference to condition 4, which
would otherwise contradict it.

## Why this is harmonization, not new doctrine

- **Rule 7 already contains the distinction.** Its strict path requires "methodology doc
  update, ADR, `method_version` bump **where outputs change**" — conditional, where Rule 1
  was absolute. The two rules disagreed; this amendment resolves the disagreement in Rule 7's
  direction for the one case (pure domain tightening) where outputs provably cannot change.
- **The data layer has practiced this since T5.** Pandera contract additions repeatedly
  tightened which inputs get scored (era gates, the TS-denominator rule, accolade grain),
  each with review and QA-log process but no version bump — because no accepted input's
  output changed. The engine-level guard is the same move one layer down.
- **`method_version`'s consumer promise is preserved in both directions.** "Same version ⇒
  same numbers" was never at risk; the amendment also protects "new version ⇒ something
  changed for your numbers" from dilution by numerically-identical bumps — the signal a v2
  ≡ v1 bump would have destroyed.

## Exploit resistance (why this cannot launder a real behavior change)

The category can only ever produce the **absence** of output, never a different output.
Condition 1's abort-the-entire-run requirement is load-bearing: refusal cannot silently
exclude a player or nudge a ranking, because when it fires there is no ranking — silent
manipulation requires an output to manipulate. Fixture/golden laundering is blocked by
condition 3 (reference artifacts untouched; golden byte-identical without regeneration);
result-conditioned refusal ("refuse pools where X isn't top-3") is blocked by condition 2;
the two-step attack (tighten now, widen later with new numbers) is blocked because widening
is explicitly a v2 event. The residual risk is an over-eager predicate that future
legitimate data trips — a loud denial-of-service, not a wrong number, and undoing it takes
the v2 path. Rule 8 additionally keeps every such diff in the mandatory-review zone.

## Compliance checklist — first application (ADR-0002's anchor-range guard)

| Condition | Evidence for this application |
|---|---|
| 1. Refuse-only, abort-entire-run, no accepted output changes | `_check_anchor_ranges` raises `ScoringError` before sql/05 executes — no partial output exists; no coercion/skip path in the code. Accepted-input outputs proven unchanged three ways: `tests/golden/v1_scores.json` byte-identical (`git diff` empty, regeneration reproduces identical bytes), the independent pandas oracle matches at 1e-9 on trio + seed + seven-season pools in both scopes, and the hand-worksheet constants still reproduce at ±0.01. |
| 2. Input-decidable predicate | The predicate reads only `mart_player_component_inputs` raw columns, `mart_player_award_rates.rate`, and the configured scope (to exempt the dropped Longevity input). It runs before any scaling or weighting executes, so no score or ranking exists to condition on. |
| 3. Reference artifacts unchanged, gate green, golden byte-identical | **Baseline: the merge base against `main`** (commit 05a3947 for this change — see "Baseline for condition 3" below). Relative to it, `data/seed/` and `tests/fixtures/` carry ZERO diff lines across the entire change (`git diff $(git merge-base main HEAD) -- data/seed tests/fixtures` is empty). `tests/golden/v1_scores.json` did not exist at the merge base (`main` carries only `.gitkeep`), so it takes the **created-within-the-change allowance**: no prior version existed, and its bytes are pinned from its creation commit (09fef29, round-1 reviewed) through the tip — blob OID at `09fef29:tests/golden/v1_scores.json` equals `git hash-object` of the working file; SHA-256 `50944af1412a69a69069e751659bd30d81a31bfcc333e57ae7c7fa0646698342` throughout. All of this is enforced mechanically on every gate run by `tests/unit/test_reference_artifacts.py`, which compares against the merge base — never `HEAD^` or any author-chosen commit — with the round-6 hardening: created goldens require exactly one add, zero deletes, and no pre-merge-base history for the path; every tracked file under `tests/golden/` is protected regardless of extension (`.gitkeep` metadata excepted); shallow repositories and multiple merge bases are rejected loudly. `make check` green (count in the QA log, recorded after the final gate run). Golden never regenerated to absorb drift — idempotent regeneration reproduces identical bytes. |
| 4. Methodology doc amended, citing the ADR | v1.md §6 scopes the degenerate rule to award-rate elements and §9 (retitled "Missing data and input-domain policy") states the anchor-range rule with the exact threshold, the award exact-tie carve-out, and the peak-scope exemption — both citing ADR-0002. |

### Baseline for condition 3 — what "unchanged" means and why it cannot launder

**"The change" is the entire reviewed merge diff relative to the target branch's merge
base (`git merge-base main HEAD`) — never any single commit.** An earlier revision of
this section defined the baseline as "the commit immediately before the tightening" and
argued it was "fixed by history, not chooseable." The round-5 Codex review constructed a
working exploit against that definition, and it was replaced: **commit boundaries are
author-controlled**, so a two-step attack works against any single-commit baseline —
commit A quietly modifies the seed so a future guard will not reject it (passing
contracts and the gate, touching no golden, so Rule 5 never fires), commit B introduces
the guard; "the commit immediately before the tightening" is now commit A, and a
per-commit zero-diff check passes cleanly while the seed actually moved. Merging both
together hides the whole maneuver.

The merge-base definition closes this: the same two-step attack produces a nonzero diff
under `data/seed/` between the merge base and the tip, because commits A and B are both
inside the reviewed change no matter how they are ordered or split. A prior artifact edit
counts as **separate** only if it was independently reviewed and merged to the target
branch *before this change began* — meaning it faced its own real scrutiny (Rule 5 for
goldens, visible review for seed and fixtures), not a commit-ordering dodge inside the
same branch.

**Created-within-the-change allowance (the founding case):** an artifact that does not
exist at the merge base cannot satisfy "zero diff from the merge base" — it appears as an
addition by construction. `tests/golden/v1_scores.json` is exactly this case: `main`
carries only `.gitkeep`, and the golden was born in this change at 09fef29. The allowance
is narrow and cannot launder: there is **no prior version to move** (laundering requires
a pre-existing state to shift), the artifact's bytes must stay **pinned from its creation
commit through the tip** (no post-creation adjustment can hide inside the same change —
verified by blob OID equality, 09fef29 vs working tree), and the created artifact still
faces its own independent gates (the golden must match the hand-worksheet constants typed
into `test_golden.py`, and Rule 5 governs any later change to it).

All three obligations — seed/fixtures vs merge base, merge-base goldens byte-identical
and undeletable, created goldens genuinely new and pinned — are enforced on every
`make check` by `tests/unit/test_reference_artifacts.py`, so the condition-3 evidence is
recomputed mechanically rather than asserted in prose. The round-6 review found and closed
three bypasses in the first version of that enforcement: a delete/re-add sequence reset
the "most recent add" creation pin (now: exactly one add, zero deletes, and no
pre-merge-base history for the path — a file that once existed and was removed has a
prior version to launder); non-JSON files under `tests/golden/` escaped every check (now:
all tracked golden files are protected, with only `.gitkeep` metadata exempt); and a
criss-cross history's multiple merge bases were silently reduced to one (now:
`merge-base --all` must return exactly one, and shallow repositories are rejected outright
rather than documented as unsupported). The round-7 review then defeated the enforcement
again through the INDEX: `git commit` records staged bytes, not working bytes, so staging
malicious content and restoring the working file passed every working-tree check while
the next commit would record the malicious version. Closed: every comparison now verifies
BOTH the working tree and the staged index entry — including file mode, so a
regular-file-to-symlink swap with identical apparent content is rejected as a mode change
(only plain 100644 regular files are valid reference artifacts) — and any untracked file
under a protected tree is refused outright (a stage-after-check channel otherwise). The
round-8 review found one further severe gap and one smaller one, both closed: a golden
CREATED then DELETED mid-branch was tracked at neither the merge base nor the tip, so no
candidate set ever inspected it — the created-golden candidate set is now the union of
the current index and every golden path touched anywhere in merge_base..HEAD
(NUL-delimited, rename detection disabled), so "exactly one add, zero deletes, survives
to the tip" applies to the full set of paths; and the untracked-file check dropped
--exclude-standard, so a .gitignore entry can no longer hide an untracked file under a
protected tree. The round-8 boundary note (clean filters can normalize tampered working
bytes back to the expected blob under default hash-object) was addressed rather than
merely documented: golden hashing uses --no-filters, pinning the invariant to raw
physical bytes — which is what the pipeline reads. Each closed bypass carries a
regression test that rebuilds the attack in a throwaway git history and proves the guard
refuses it.

## Alternatives considered

- **Full v2 bump:** constitutionally clean under the old Rule 1, one-time cost. Rejected:
  guardrail tightening is a demonstrably recurring pattern (three consecutive review rounds),
  so the precedent is version inflation — v2, v3… with bit-identical numbers — which turns
  `method_version` into noise and creates permanent explanation debt in the report and README.
- **Keep ADR-0002 as an implicit exception:** rejected — this was the round-3 objection. A
  subordinate document cannot override the constitution; if the constitution is wrong, amend
  the constitution and record the amendment.

## Consequences

- Future guardrail tightening has a defined, mechanically-checkable path; anything that
  cannot satisfy all four conditions takes the v2 path by default.
- Domain widening is now explicitly a v2 event — previously unstated.
- ADR-0002 is revised in the same change to invoke this clause as its authority.
