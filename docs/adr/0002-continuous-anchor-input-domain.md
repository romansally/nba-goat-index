# ADR-0002: Continuous anchor-range input domain (refuse noise-thin pools, no version bump)

**Date:** 2026-07-13 (revised same day after the round-3 review — see "Authority")
**Status:** Accepted (owner decision, T7 Codex re-review round)
**Authority:** CLAUDE.md Rule 1, input-domain tightening clause — added by ADR-0003 in the
same change. This ADR is the clause's first application and must satisfy its four conditions;
the condition-by-condition evidence is the compliance checklist in ADR-0003.
**Scope:** `pipeline/score.py::_check_anchor_ranges`, `docs/methodology/v1.md` §6/§9 amendments.

---

## Context

Min–max scaling (v1.md §6) maps a component's pool range onto 0–100. Two independent Codex
review rounds demonstrated, with reproducible numbers, that pools whose raw ranges carry no
real signal produce authoritative-looking garbage:

1. **Near-degenerate:** ws set proportional to minutes gave a WS48 pool span of 5.55e-17 —
   pure float residue — which min–max stretched into a full 0/50/75/100 spread: 13 of 20
   ranks changed, largest final score moved 11.6873.
2. **Exact-degenerate continuous:** uniformly zeroed ws + team_srs (contract-valid row by
   row) flowed 50s through the degenerate rule and changed 12 of 20 ranks.
3. **Near-zero anchors (the severe re-review finding):** per-player srs_w spanning ±1e-15
   defeated a purely *relative* threshold (the RHS shrinks with the anchors); reversing which
   player received which noise value flipped 18 of 20 ranks and moved the max final by 10.0.

v1.md §6 as originally worded promised "if `max_pool == min_pool`, every player scores 50.0
on that element" for ANY element, so an engine that refuses such pools observably deviates
from the doc's letter for a class of contract-valid inputs. The T7 re-review correctly
flagged that resolving this inside a bug fix would bypass the project's behavior-change
governance. The owner was asked to decide explicitly.

## Decision

**Refuse, document as an input-domain rule, no method_version bump.**

1. The engine refuses any pool whose **continuous** component input (peak/longevity/playoff
   raws, WS48, SRS_w, career REL_TS, career SPI) has a range
   `max − min ≤ 1e-12 + 1e-9 × max(|min|, |max|)`. The absolute floor (1e-12) covers anchors
   straddling zero; the relative term (1e-9) covers noise on large-magnitude anchors. Real
   spans sit at ≥ 0.09 absolute and ≥ 9% relative — 8+ orders above both terms.
2. **Award rates keep the §6 exact-tie → 50.0 rule** — exact pool-wide ties are legitimate
   for discrete per-award rates and are exercised by the locked worksheet (DPOY, All-Star).
   Only an unequal-yet-noise-thin award range is refused.
3. **Peak scope exempts `longevity_raw`** — §7 drops that component (zero weight, absent
   from output), so its degeneracy cannot affect any output value.
4. v1.md is amended: §6's degenerate rule is scoped to award-rate elements; §9 (retitled
   "Missing data and input-domain policy") gains the anchor-range rule. Both cite this ADR.

## Why no method_version bump — compliance with Rule 1's input-domain tightening clause

This change ships bump-free **because it satisfies all four conditions of the Rule 1
input-domain tightening clause** (evidence mapped condition-by-condition in ADR-0003's
compliance checklist), not as an exception to Rule 1:

1. **Refuse-only, whole-run abort:** the guard raises `ScoringError` before any scoring SQL
   executes; it never warns, coerces, skips rows, or emits partial output. No accepted
   input's output changes — the committed golden snapshot is byte-identical across the
   guard's introduction, the independent oracle matches at 1e-9, the worksheet constants
   still reproduce at ±0.01.
2. **Input-decidable predicate:** anchor ranges are computed from the raw component inputs
   and award rates plus the configured scope, before any scaling or weighting runs — no
   score or ranking exists to condition on.
3. **Reference artifacts untouched across the entire change:** relative to the merge base
   against `main` (05a3947), `data/seed/` and `tests/fixtures/` carry zero diff lines;
   `tests/golden/v1_scores.json` takes the created-within-the-change allowance (it did not
   exist at the merge base) with its bytes pinned from its creation commit 09fef29 through
   the tip (SHA-256 `50944af1…8342` throughout); `make check` green; golden never
   regenerated to absorb drift. Enforced mechanically by
   `tests/unit/test_reference_artifacts.py`; baseline definition, the round-5 exploit that
   forced it, and the anti-laundering rationale: ADR-0003.
4. **Methodology doc amended in the same change:** v1.md §6 (degenerate rule scoped to
   award-rate elements) and §9 (the anchor-range rule, exact threshold, exemptions), both
   citing this ADR.

Supporting context (why the narrowed §6 sentence was safe to narrow): §9's null rule and
§10.3's weight-sum errors already established fail-loud input refusal as v1-native behavior,
and no committed artifact — worksheet, golden, fixture, or real pool — ever exercised
degenerate-50 on a continuous element.

**Process history, recorded honestly:** this ADR originally shipped arguing the no-bump
outcome on its own authority ("domain restriction, not a scoring change"). The round-3 Codex
review correctly objected that an ADR cannot override Rule 1 as then written. The owner
resolved the conflict by amending Rule 1 itself (ADR-0003) rather than bumping; this ADR was
then revised to invoke that clause as its authority. Any future change that alters a score
for an in-domain input remains a full v2 event, as does widening this domain back.

## Alternatives considered

- **Preserve §6 literally for exact ties, refuse only near-ties, surface diagnostics (the
  alternative actually presented to the owner):** exact-degenerate continuous pools score
  50s per §6's letter; only unequal-but-noise-thin ranges refuse; anchor-range diagnostics
  are emitted. Rejected on two grounds: the exact-tie corruption case (all-zero ws, 12/20
  ranks changed) would still produce a plausible-looking ranking guarded only by warnings —
  which don't fail gates and had no carrier before T8's run metadata — and it creates an
  indefensible cliff where bit-identical inputs score while inputs one ULP apart refuse. A
  pure metadata-only variant (no refusal at all, every noise-thin pool scores) was rejected
  a fortiori: it additionally leaves the noise-amplification demos producing rankings.
- **Full v2 bump:** maximally strict under Rule 1 as then written, but v2 ≡ v1 numerically
  for every real input — `method_version` would stop signaling numeric change, the locked
  worksheet stays v1-labeled, and T9/T10 inherit permanent explanation debt. The owner chose
  to amend Rule 1 (ADR-0003) instead; the bump remains the default for anything that cannot
  satisfy the clause's four conditions.
- **Data-layer contract ("no constant columns") instead of an engine guard:** rejected — the
  single-player transform-test pools legitimately carry constant ws/team_srs, so the rule is
  only sound at the scoring-pool level, which is exactly where the guard sits.

## Consequences

- Corruption-shaped pools fail loudly at scoring time with the offending column named;
  scores can never be decided by floating-point residue.
- Single-player scoring pools are refused (every continuous range is zero). No PRD
  requirement scores fewer than the 15–30 player pool; pairwise needs two players.
- A hypothetical tiny custom pool with a legitimate exact continuous tie (e.g. two players
  with identical career SRS) is refused rather than scored at 50 — documented, accepted; the
  15–30 pool policy makes it unrealistic.
- Tests pin the policy: both review demos, the ±1e-15 reversal case, the peak-scope
  longevity exemption, and the career-scope counterpart.
